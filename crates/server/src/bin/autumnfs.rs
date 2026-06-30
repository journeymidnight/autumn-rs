//! `autumnfs` — standalone CLI for poking at the autumn-rs FUSE filesystem
//! without mounting it.
//!
//! Skips the `autumn-fuse` kernel mount; talks directly to the
//! autumn-rs cluster via [`autumn_client::ClusterClient`]
//! using the [`autumn_fuse::key`] + [`autumn_fuse::schema`] modules — the same
//! KV layout the fuse mount uses, so a write here is visible to a fuse mount
//! pointed at the same cluster and vice versa.
//!
//! ## Subcommands
//!
//! ```text
//! autumnfs --manager 127.0.0.1:9001 ls [PATH]
//! autumnfs --manager 127.0.0.1:9001 stat <PATH>
//! autumnfs --manager 127.0.0.1:9001 mkdir <PATH>
//! autumnfs --manager 127.0.0.1:9001 cat <PATH>
//! autumnfs --manager 127.0.0.1:9001 put <LOCAL> <REMOTE>
//! autumnfs --manager 127.0.0.1:9001 get <REMOTE> <LOCAL>
//! autumnfs --manager 127.0.0.1:9001 rm <PATH>
//! autumnfs --manager 127.0.0.1:9001 touch <PATH>
//! ```
//!
//! Paths are `/`-separated; leading `/` is optional. ROOT_INO = 1.
//!
//! The inode allocator is intentionally racy — it does a get-then-put on the
//! `next_inode` super-block counter without CAS, same as the fuse mount today.
//! Two concurrent autumnfs writers racing the counter is a known limitation;
//! the failure mode is "later writer overwrites earlier writer's inode" which
//! corrupts the dirent that pointed at it. For a one-shot CLI this is
//! acceptable; do not script `autumnfs` in parallel against the same
//! directory.

use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};

use autumn_client::ClusterClient;
use autumn_fuse::key;
use autumn_fuse::schema::{
    self, DirentValue, InodeMeta, DT_DIR, DT_LNK, DT_REG, INLINE_THRESHOLD, MAX_EXTENT, ROOT_INO,
};

#[derive(Parser, Debug)]
#[command(
    name = "autumnfs",
    about = "POSIX-ish CLI over the autumn-rs fuse KV layout (no mount needed)"
)]
struct Args {
    /// Manager address (comma-separated for HA).
    #[arg(long, default_value = "127.0.0.1:9001")]
    manager: String,

    /// Transport: tcp (default) or ucx (if built with the ucx feature).
    #[arg(long, default_value = "tcp")]
    transport: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// List a directory's entries (default `/`).
    Ls {
        #[arg(default_value = "/")]
        path: String,
        /// Long format: include mode/size/mtime.
        #[arg(short, long)]
        long: bool,
    },
    /// Show inode metadata for a path.
    Stat { path: String },
    /// Create a directory (parents must exist).
    Mkdir { path: String },
    /// Print a file's contents to stdout.
    Cat { path: String },
    /// Upload a local file to the cluster.
    Put {
        local: PathBuf,
        /// Destination path in the autumn fs. Parents must exist.
        remote: String,
    },
    /// Download a cluster file to local disk.
    Get {
        remote: String,
        /// Local destination path. Use `-` for stdout.
        local: PathBuf,
    },
    /// Remove a file or empty directory.
    Rm { path: String },
    /// Create an empty file (no-op if it already exists).
    Touch { path: String },
}

fn main() -> Result<()> {
    let args = Args::parse();
    // Stand up the same transport selector autumn-client uses everywhere.
    let kind = match args.transport.as_str() {
        "tcp" => autumn_transport::TransportKind::Tcp,
        #[cfg(feature = "ucx")]
        "ucx" => autumn_transport::TransportKind::Ucx,
        other => bail!("unknown transport: {other}"),
    };
    let _ = autumn_transport::init_with(kind);

    let rt = compio::runtime::RuntimeBuilder::new()
        .build()
        .context("create compio runtime")?;
    rt.block_on(async move {
        let cluster = ClusterClient::connect(&args.manager)
            .await
            .context("connect to manager")?;
        cluster
            .wait_for_cluster_ready(
                std::time::Duration::from_secs(20),
                std::time::Duration::from_millis(200),
            )
            .await
            .context("wait for cluster ready")?;

        match args.cmd {
            Cmd::Ls { path, long } => cmd_ls(&cluster, &path, long).await,
            Cmd::Stat { path } => cmd_stat(&cluster, &path).await,
            Cmd::Mkdir { path } => cmd_mkdir(&cluster, &path).await,
            Cmd::Cat { path } => cmd_cat(&cluster, &path).await,
            Cmd::Put { local, remote } => cmd_put(&cluster, &local, &remote).await,
            Cmd::Get { remote, local } => cmd_get(&cluster, &remote, &local).await,
            Cmd::Rm { path } => cmd_rm(&cluster, &path).await,
            Cmd::Touch { path } => cmd_touch(&cluster, &path).await,
        }
    })
}

// ─── Path resolution ─────────────────────────────────────────────────────────

/// Split a `/`-separated path into non-empty components. Leading/trailing
/// `/` and `//` runs are ignored. `""` and `"/"` both resolve to root.
fn split_path(path: &str) -> Vec<&[u8]> {
    path.split('/').filter(|c| !c.is_empty()).map(|c| c.as_bytes()).collect()
}

/// Walk dirents from `ROOT_INO` to the inode at `path`. Returns
/// `(ino, parent_ino, name_in_parent, meta)`. `parent_ino` = 0 and `name`
/// empty for the root.
async fn resolve(
    cluster: &ClusterClient,
    path: &str,
) -> Result<(u64, u64, Vec<u8>, InodeMeta)> {
    let comps = split_path(path);
    let mut ino = ROOT_INO;
    let mut parent_ino = 0u64;
    let mut name: Vec<u8> = Vec::new();
    for c in &comps {
        let dk = key::dirent_key(ino, c);
        let dv = cluster
            .get(&dk)
            .await
            .map_err(|e| anyhow!("dirent get: {e}"))?
            .ok_or_else(|| anyhow!("ENOENT: {}", String::from_utf8_lossy(c)))?;
        let dirent = schema::decode_dirent(&dv).map_err(|e| anyhow!("decode dirent: {e}"))?;
        parent_ino = ino;
        ino = dirent.child_inode;
        name = c.to_vec();
    }
    let iv = cluster
        .get(&key::inode_key(ino))
        .await
        .map_err(|e| anyhow!("inode get: {e}"))?
        .ok_or_else(|| anyhow!("ENOENT inode {ino}"))?;
    let meta = schema::decode_inode_meta(&iv).map_err(|e| anyhow!("decode inode: {e}"))?;
    Ok((ino, parent_ino, name, meta))
}

/// Resolve just the parent dir + the leaf name. Used for create / mkdir / rm
/// where we need the parent to mutate but the leaf may not exist yet.
async fn resolve_parent_leaf(
    cluster: &ClusterClient,
    path: &str,
) -> Result<(u64, InodeMeta, Vec<u8>)> {
    let comps = split_path(path);
    let (leaf, parent_comps) = comps
        .split_last()
        .ok_or_else(|| anyhow!("path must name a non-root entry"))?;
    let parent_path: Vec<u8> =
        parent_comps.iter().flat_map(|c| [b"/" as &[u8], c]).flatten().copied().collect();
    let parent_str = String::from_utf8_lossy(&parent_path).into_owned();
    let (parent_ino, _, _, parent_meta) = resolve(cluster, &parent_str).await?;
    Ok((parent_ino, parent_meta, leaf.to_vec()))
}

// ─── Inode allocation ────────────────────────────────────────────────────────

/// One-at-a-time inode allocator: get the counter, write back +1, return the
/// allocated id. NOT CAS-safe; see the module doc comment.
async fn alloc_inode(cluster: &ClusterClient) -> Result<u64> {
    let k = key::next_inode_key();
    let current = match cluster.get(&k).await.map_err(|e| anyhow!("next_inode get: {e}"))? {
        Some(v) if v.len() == 8 => u64::from_be_bytes(v[..8].try_into().unwrap()),
        _ => ROOT_INO + 1,
    };
    let next = current + 1;
    let v = next.to_be_bytes();
    cluster
        .put(&k, &v)
        .await
        .map_err(|e| anyhow!("next_inode put: {e}"))?;
    Ok(current)
}

// ─── Meta builders (re-implemented; the fuse-gated `meta::new_*_meta` would
// drag in fuser as a transitive dep). ────────────────────────────────────────

fn now_ts() -> (i64, u32) {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as i64, d.subsec_nanos())
}

fn new_file_meta() -> InodeMeta {
    let (secs, nsecs) = now_ts();
    InodeMeta {
        mode: (libc::S_IFREG | 0o644) as u32,
        uid: 0,
        gid: 0,
        size: 0,
        nlink: 1,
        atime_secs: secs,
        atime_nsecs: nsecs,
        mtime_secs: secs,
        mtime_nsecs: nsecs,
        ctime_secs: secs,
        ctime_nsecs: nsecs,
        inline_data: None,
        symlink_target: None,
    }
}

fn new_dir_meta() -> InodeMeta {
    let (secs, nsecs) = now_ts();
    InodeMeta {
        mode: (libc::S_IFDIR | 0o755) as u32,
        uid: 0,
        gid: 0,
        size: 0,
        nlink: 2,
        atime_secs: secs,
        atime_nsecs: nsecs,
        mtime_secs: secs,
        mtime_nsecs: nsecs,
        ctime_secs: secs,
        ctime_nsecs: nsecs,
        inline_data: None,
        symlink_target: None,
    }
}

// ─── ls ─────────────────────────────────────────────────────────────────────

async fn cmd_ls(cluster: &ClusterClient, path: &str, long: bool) -> Result<()> {
    let (ino, _, _, meta) = resolve(cluster, path).await?;
    if !is_dir(&meta) {
        // Single-entry ls of a regular file just prints its name.
        let name = path.rsplit('/').find(|s| !s.is_empty()).unwrap_or(path);
        if long {
            print_long_entry(name, &meta);
        } else {
            println!("{name}");
        }
        return Ok(());
    }
    // Range scan over `[0x02][parent_ino BE]`.
    let prefix = key::dirent_prefix(ino);
    let mut start: Vec<u8> = Vec::new();
    const PAGE: u32 = 256;
    loop {
        let resp = cluster
            .range(&prefix, &start, PAGE)
            .await
            .map_err(|e| anyhow!("dirent range: {e}"))?;
        let count = resp.entries.len();
        for entry in &resp.entries {
            let (parent, name_bytes) = match key::parse_dirent_key(&entry.key) {
                Some(v) => v,
                None => continue,
            };
            debug_assert_eq!(parent, ino);
            // PS `handle_range` returns only KEYS (not values) — see
            // crates/partition-server/src/rpc_handlers.rs::handle_range
            // (`value: vec![]`). The fuse-side `state.kv_range_keys` knows
            // this and does a per-key `kv_get` to fetch values; we mirror
            // that here. For one-shot CLI use, N sequential gets are fine;
            // if this ever needs to be hot we'd batch via `get_many_into`.
            let v = match cluster.get(&entry.key).await {
                Ok(Some(v)) => v,
                Ok(None) => continue, // stale dirent key with deleted value
                Err(e) => {
                    eprintln!("dirent get failed: {e}");
                    continue;
                }
            };
            let dirent = match schema::decode_dirent(&v) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let name = String::from_utf8_lossy(name_bytes).into_owned();
            if long {
                let child_meta = match cluster
                    .get(&key::inode_key(dirent.child_inode))
                    .await
                    .ok()
                    .flatten()
                    .and_then(|b| schema::decode_inode_meta(&b).ok())
                {
                    Some(m) => m,
                    None => {
                        eprintln!("(stale dirent → ino {} missing)", dirent.child_inode);
                        continue;
                    }
                };
                print_long_entry(&name, &child_meta);
            } else {
                let suffix = match dirent.file_type {
                    DT_DIR => "/",
                    DT_LNK => "@",
                    _ => "",
                };
                println!("{name}{suffix}");
            }
        }
        if count < PAGE as usize {
            break;
        }
        // Advance start cursor past the last returned key.
        let last_key = &resp.entries.last().unwrap().key;
        start = last_key.clone();
        // Bump the last byte so we don't re-emit the boundary entry.
        if let Some(b) = start.last_mut() {
            *b = b.wrapping_add(1);
        } else {
            break;
        }
    }
    Ok(())
}

fn print_long_entry(name: &str, m: &InodeMeta) {
    let kind = if m.mode & libc::S_IFMT as u32 == libc::S_IFDIR as u32 {
        'd'
    } else if m.mode & libc::S_IFMT as u32 == libc::S_IFLNK as u32 {
        'l'
    } else {
        '-'
    };
    let perm = m.mode & 0o777;
    println!(
        "{}{:03o} {:>4} {:>4} {:>10} {:>10} {}",
        kind, perm, m.uid, m.gid, m.size, m.mtime_secs, name,
    );
}

fn is_dir(m: &InodeMeta) -> bool {
    m.mode & libc::S_IFMT as u32 == libc::S_IFDIR as u32
}

fn is_reg(m: &InodeMeta) -> bool {
    m.mode & libc::S_IFMT as u32 == libc::S_IFREG as u32
}

// ─── stat ───────────────────────────────────────────────────────────────────

async fn cmd_stat(cluster: &ClusterClient, path: &str) -> Result<()> {
    let (ino, parent_ino, name, m) = resolve(cluster, path).await?;
    let kind = if is_dir(&m) {
        "directory"
    } else if is_reg(&m) {
        "regular file"
    } else {
        "other"
    };
    println!("  File: {}", path);
    println!("  Size: {}\tInode: {}\tParent ino: {}", m.size, ino, parent_ino);
    println!("  Type: {}", kind);
    println!(
        "  Mode: {:04o}  uid={} gid={} nlink={}",
        m.mode & 0o7777,
        m.uid,
        m.gid,
        m.nlink
    );
    println!(
        "  Access: {}  Modify: {}  Change: {}",
        m.atime_secs, m.mtime_secs, m.ctime_secs
    );
    if !name.is_empty() {
        println!("  Name-in-parent: {}", String::from_utf8_lossy(&name));
    }
    if let Some(ref inline) = m.inline_data {
        println!("  inline_data: {} bytes (≤ {})", inline.len(), INLINE_THRESHOLD);
    }
    Ok(())
}

// ─── mkdir ──────────────────────────────────────────────────────────────────

async fn cmd_mkdir(cluster: &ClusterClient, path: &str) -> Result<()> {
    let (parent_ino, mut parent_meta, leaf) = resolve_parent_leaf(cluster, path).await?;
    if !is_dir(&parent_meta) {
        bail!("parent is not a directory");
    }
    let dk = key::dirent_key(parent_ino, &leaf);
    if cluster.get(&dk).await.map_err(|e| anyhow!("dirent get: {e}"))?.is_some() {
        bail!("path already exists");
    }
    let new_ino = alloc_inode(cluster).await?;
    let meta = new_dir_meta();
    let dirent = DirentValue {
        child_inode: new_ino,
        file_type: DT_DIR,
    };
    // Order matters for crash recovery: inode FIRST (else a stale dirent would
    // point at a missing ino on a crash between the puts). Parent nlink bump
    // last (cheapest to leave stale — manifests only as an over-counted
    // hard-link count on the parent, observable but harmless until rmdir
    // refuses to drop it).
    cluster
        .put(&key::inode_key(new_ino), &schema::encode_inode_meta(&meta))
        .await
        .map_err(|e| anyhow!("put new dir inode: {e}"))?;
    cluster
        .put(&dk, &schema::encode_dirent(&dirent))
        .await
        .map_err(|e| anyhow!("put dirent: {e}"))?;
    parent_meta.nlink += 1; // for the new dir's ".."
    cluster
        .put(
            &key::inode_key(parent_ino),
            &schema::encode_inode_meta(&parent_meta),
        )
        .await
        .map_err(|e| anyhow!("update parent meta: {e}"))?;
    Ok(())
}

// ─── cat / get ──────────────────────────────────────────────────────────────

async fn read_file_to_writer(
    cluster: &ClusterClient,
    ino: u64,
    meta: &InodeMeta,
    out: &mut dyn Write,
) -> Result<()> {
    if !is_reg(meta) {
        bail!("not a regular file");
    }
    if let Some(ref inline) = meta.inline_data {
        out.write_all(inline).context("write inline to output")?;
        return Ok(());
    }
    // Range-scan extents under `[0x03][ino BE]`, fetch each, write in order.
    let prefix = key::extent_prefix(ino);
    let mut start = prefix.clone();
    const PAGE: u32 = 256;
    let mut written: u64 = 0;
    loop {
        let resp = cluster
            .range(&prefix, &start, PAGE)
            .await
            .map_err(|e| anyhow!("extent range: {e}"))?;
        let count = resp.entries.len();
        for entry in &resp.entries {
            let (_, off) = match key::parse_extent_key(&entry.key) {
                Some(v) => v,
                None => continue,
            };
            // PS range returns keys only — fetch each extent's value via get.
            // For multi-extent files this is N round-trips; a future caller
            // that needs speed could switch to `get_many_into` over the key
            // list. For one-shot CLI use, this is fine.
            let v = match cluster.get(&entry.key).await {
                Ok(Some(v)) => v,
                Ok(None) => continue,
                Err(e) => bail!("extent get for offset {off}: {e}"),
            };
            // Pad with zeros if there's a hole (sparse semantics).
            if off > written {
                let pad = off - written;
                std::io::copy(&mut std::io::repeat(0).take(pad), out)
                    .context("pad sparse hole")?;
                written = off;
            }
            out.write_all(&v).context("write extent to output")?;
            written += v.len() as u64;
        }
        if count < PAGE as usize {
            break;
        }
        let last_key = &resp.entries.last().unwrap().key;
        start = last_key.clone();
        if let Some(b) = start.last_mut() {
            *b = b.wrapping_add(1);
        } else {
            break;
        }
    }
    // Truncate trailing zeros if extents overshot meta.size (shouldn't happen
    // in steady state, but be defensive).
    out.flush().context("flush output")?;
    Ok(())
}

async fn cmd_cat(cluster: &ClusterClient, path: &str) -> Result<()> {
    let (ino, _, _, meta) = resolve(cluster, path).await?;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    read_file_to_writer(cluster, ino, &meta, &mut out).await
}

async fn cmd_get(cluster: &ClusterClient, remote: &str, local: &PathBuf) -> Result<()> {
    let (ino, _, _, meta) = resolve(cluster, remote).await?;
    if local.as_os_str() == "-" {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        read_file_to_writer(cluster, ino, &meta, &mut out).await
    } else {
        let mut f = std::fs::File::create(local)
            .with_context(|| format!("create local file {}", local.display()))?;
        read_file_to_writer(cluster, ino, &meta, &mut f).await
    }
}

// ─── put / touch ────────────────────────────────────────────────────────────

async fn write_file_from_reader(
    cluster: &ClusterClient,
    parent_ino: u64,
    leaf: &[u8],
    mut data_reader: impl Read,
) -> Result<u64> {
    // Read everything into RAM first. This CLI is for one-off ops on
    // small/medium files; a streaming-with-chunked-extents version is the
    // next refactor if we ever need it for multi-GiB uploads.
    let mut buf = Vec::new();
    data_reader.read_to_end(&mut buf).context("read source")?;
    let size = buf.len() as u64;

    let new_ino = alloc_inode(cluster).await?;
    let mut meta = new_file_meta();
    meta.size = size;

    if size <= INLINE_THRESHOLD as u64 {
        meta.inline_data = Some(buf);
    } else {
        // Split into ≤ MAX_EXTENT chunks at offsets 0, MAX_EXTENT, 2*MAX_EXTENT, …
        // Each gets its own extent_key. Reads stitch them together via the
        // extent_prefix range scan above.
        let mut off: u64 = 0;
        let mut rem = &buf[..];
        while !rem.is_empty() {
            let n = std::cmp::min(MAX_EXTENT, rem.len());
            let chunk = &rem[..n];
            cluster
                .put(&key::extent_key(new_ino, off), chunk)
                .await
                .map_err(|e| anyhow!("put extent at {off}: {e}"))?;
            off += n as u64;
            rem = &rem[n..];
        }
    }
    cluster
        .put(&key::inode_key(new_ino), &schema::encode_inode_meta(&meta))
        .await
        .map_err(|e| anyhow!("put inode: {e}"))?;
    let dirent = DirentValue {
        child_inode: new_ino,
        file_type: DT_REG,
    };
    cluster
        .put(&key::dirent_key(parent_ino, leaf), &schema::encode_dirent(&dirent))
        .await
        .map_err(|e| anyhow!("put dirent: {e}"))?;
    Ok(new_ino)
}

async fn cmd_put(cluster: &ClusterClient, local: &PathBuf, remote: &str) -> Result<()> {
    let (parent_ino, parent_meta, leaf) = resolve_parent_leaf(cluster, remote).await?;
    if !is_dir(&parent_meta) {
        bail!("parent is not a directory");
    }
    if cluster
        .get(&key::dirent_key(parent_ino, &leaf))
        .await
        .map_err(|e| anyhow!("dirent get: {e}"))?
        .is_some()
    {
        bail!("destination already exists");
    }
    let f = std::fs::File::open(local)
        .with_context(|| format!("open local file {}", local.display()))?;
    let ino = write_file_from_reader(cluster, parent_ino, &leaf, f).await?;
    println!("uploaded → ino {ino}");
    Ok(())
}

async fn cmd_touch(cluster: &ClusterClient, path: &str) -> Result<()> {
    let (parent_ino, parent_meta, leaf) = resolve_parent_leaf(cluster, path).await?;
    if !is_dir(&parent_meta) {
        bail!("parent is not a directory");
    }
    let dk = key::dirent_key(parent_ino, &leaf);
    if cluster.get(&dk).await.map_err(|e| anyhow!("dirent get: {e}"))?.is_some() {
        // Already exists; update mtime by re-putting the inode meta.
        let (ino, _, _, mut m) = resolve(cluster, path).await?;
        let (secs, nsecs) = now_ts();
        m.mtime_secs = secs;
        m.mtime_nsecs = nsecs;
        cluster
            .put(&key::inode_key(ino), &schema::encode_inode_meta(&m))
            .await
            .map_err(|e| anyhow!("update mtime: {e}"))?;
        return Ok(());
    }
    // Empty file.
    let ino = write_file_from_reader(cluster, parent_ino, &leaf, std::io::empty()).await?;
    println!("touched → ino {ino}");
    Ok(())
}

// ─── rm ─────────────────────────────────────────────────────────────────────

async fn cmd_rm(cluster: &ClusterClient, path: &str) -> Result<()> {
    let (ino, parent_ino, name, meta) = resolve(cluster, path).await?;
    if parent_ino == 0 {
        bail!("refusing to remove root");
    }
    if is_dir(&meta) {
        // Refuse non-empty directories.
        let prefix = key::dirent_prefix(ino);
        let resp = cluster
            .range(&prefix, &prefix, 1)
            .await
            .map_err(|e| anyhow!("dir empty check: {e}"))?;
        if !resp.entries.is_empty() {
            bail!("directory not empty");
        }
        // Delete the directory inode, the dirent in the parent, and decrement
        // parent's nlink (the dropped "..").
        cluster
            .delete(&key::dirent_key(parent_ino, &name))
            .await
            .map_err(|e| anyhow!("delete dirent: {e}"))?;
        cluster
            .delete(&key::inode_key(ino))
            .await
            .map_err(|e| anyhow!("delete inode: {e}"))?;
        // Bump parent nlink down.
        if let Some(iv) = cluster
            .get(&key::inode_key(parent_ino))
            .await
            .map_err(|e| anyhow!("parent get: {e}"))?
        {
            if let Ok(mut pm) = schema::decode_inode_meta(&iv) {
                pm.nlink = pm.nlink.saturating_sub(1);
                let _ = cluster
                    .put(
                        &key::inode_key(parent_ino),
                        &schema::encode_inode_meta(&pm),
                    )
                    .await;
            }
        }
        return Ok(());
    }
    if !is_reg(&meta) {
        bail!("unsupported file type");
    }
    // Delete extents (if any), then dirent, then inode if nlink would hit 0.
    if meta.inline_data.is_none() {
        let prefix = key::extent_prefix(ino);
        let mut start = prefix.clone();
        const PAGE: u32 = 256;
        loop {
            let resp = cluster
                .range(&prefix, &start, PAGE)
                .await
                .map_err(|e| anyhow!("extent range: {e}"))?;
            let count = resp.entries.len();
            for entry in &resp.entries {
                cluster
                    .delete(&entry.key)
                    .await
                    .map_err(|e| anyhow!("delete extent: {e}"))?;
            }
            if count < PAGE as usize {
                break;
            }
            let last_key = &resp.entries.last().unwrap().key;
            start = last_key.clone();
            if let Some(b) = start.last_mut() {
                *b = b.wrapping_add(1);
            } else {
                break;
            }
        }
    }
    cluster
        .delete(&key::dirent_key(parent_ino, &name))
        .await
        .map_err(|e| anyhow!("delete dirent: {e}"))?;
    if meta.nlink <= 1 {
        let _ = cluster
            .delete(&key::inode_key(ino))
            .await
            .map_err(|e| anyhow!("delete inode: {e}"));
    } else {
        let mut m = meta.clone();
        m.nlink -= 1;
        let _ = cluster
            .put(&key::inode_key(ino), &schema::encode_inode_meta(&m))
            .await;
    }
    Ok(())
}
