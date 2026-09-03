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
//! `autumnfs` connects SCOPED to the WHOLE `fs/` namespace
//! (no `--tenant` — Option 3 dropped it) so its keys land in the SAME keyspace a
//! fuse mount uses — a write here is visible to a mount, and vice versa. Inode
//! numbers come from the MANAGER's
//! crash-safe GLOBAL counter (`alloc_inodes`), the same source the fuse mount +
//! PyO3 `autumn.Fs` use, so autumnfs and a mount never hand out colliding inodes
//! (the pre-SD-3 racy non-CAS KV counter is gone).

use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};

use autumn_client::ClusterClient;
use autumn_fuse::key;
use autumn_fuse::schema::{
    self, DirentValue, InodeMeta, StripeLayout, DT_DIR, DT_LNK, DT_REG, INLINE_THRESHOLD,
    MAX_EXTENT, ROOT_INO,
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

    /// path to a file holding this client's authz credential
    /// (`<principal>\n<hex>`, from `autumn-op principal-create`). REQUIRED when the
    /// cluster protects the `fs/` namespace; omit on an authz-off cluster. Connects
    /// via `connect_with_credential` (principal read from the file) and FAILS FAST
    /// if it doesn't cover `fs/`. (No tenant segment — this
    /// CLI sees the WHOLE `fs/` namespace, same as a mount.)
    #[arg(long)]
    credential_file: Option<PathBuf>,

    /// Read extents STRAIGHT from the extent nodes instead of proxying every
    /// byte through the partition server (default true, same as a fuse mount).
    /// Applies per extent at or above 64 KiB; anything smaller stays on the
    /// proxy path, and ANY direct read that fails falls back to the proxy, so
    /// this is a routing choice and never a correctness one. Turn it off when
    /// the extent nodes' data ports are not reachable from this host — then
    /// each large extent would pay a pointless redirect round trip first.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    direct_read: bool,

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
        // scope the client to the WHOLE `fs/` namespace —
        // the binding prepends `fs/` to every relative fuse key (and strips it off
        // range results), so a write here is visible to a fuse mount. Every
        // `key::*`-based op below is unchanged; the client owns the prefix.
        let cluster = match &args.credential_file {
            Some(path) => {
                let (principal, secret) = autumn_client::read_credential_file(path)
                    .context("--credential-file")?;
                if principal.is_empty() {
                    bail!("--credential-file: missing principal name (expected '<principal>\\n<hex>')");
                }
                ClusterClient::connect_with_credential(
                    &args.manager,
                    "fs",
                    principal,
                    secret,
                )
                .await
                .context("connect to manager (with credential)")?
            }
            None => ClusterClient::connect(&args.manager, "fs")
                .await
                .context("connect to manager")?,
        };
        cluster
            .wait_for_cluster_ready(
                std::time::Duration::from_secs(20),
                std::time::Duration::from_millis(200),
            )
            .await
            .context("wait for cluster ready")?;

        // Bootstrap the fuse root inode (ino 1) if it's missing — a cluster that
        // has never mounted the fuse fs or used `autumn.Fs` has no root, so
        // `resolve()` from ROOT_INO would fail `ENOENT inode 1`. `autumn.Fs` /
        // the fuse mount call `ensure_root` on connect; autumnfs must too, so it
        // works standalone against a fresh cluster (e.g. uploading model weights).
        ensure_root(&cluster).await?;

        match args.cmd {
            Cmd::Ls { path, long } => cmd_ls(&cluster, &path, long).await,
            Cmd::Stat { path } => cmd_stat(&cluster, &path).await,
            Cmd::Mkdir { path } => cmd_mkdir(&cluster, &path).await,
            Cmd::Cat { path } => cmd_cat(&cluster, &path, args.direct_read).await,
            Cmd::Put { local, remote } => {
                cmd_put(&cluster, &local, &remote).await
            }
            Cmd::Get { remote, local } => {
                cmd_get(&cluster, &remote, &local, args.direct_read).await
            }
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

/// Allocate an inode number from the MANAGER's global counter — the SAME source
/// the fuse mount + PyO3 `autumn.Fs` use (M0), so autumnfs's inodes are
/// cluster-unique and never collide with a mount's. Empty volume = the single
/// global counter (SD-3 review P1-2: inodes are cluster-unique, not
/// per-volume, because the lease/fence plane keys by bare ino). Replaces the
/// pre-SD-3 racy non-CAS get/put on the `next_inode` KV counter.
async fn alloc_inode(cluster: &ClusterClient) -> Result<u64> {
    cluster
        .alloc_inodes(1, 0, b"")
        .await
        .map_err(|e| anyhow!("alloc_inodes from manager: {e}"))
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
        mode: ((libc::S_IFREG as u32) | 0o644),
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
        stripe: None,
    }
}

fn new_dir_meta() -> InodeMeta {
    let (secs, nsecs) = now_ts();
    InodeMeta {
        mode: ((libc::S_IFDIR as u32) | 0o755),
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
        stripe: None,
    }
}

/// Create the fuse root inode (ino 1) if it doesn't exist yet — the standalone
/// equivalent of `autumn_fuse::meta::ensure_root` (which needs an `FsState`).
/// Idempotent: a no-op when the root already exists.
async fn ensure_root(cluster: &ClusterClient) -> Result<()> {
    let rk = key::inode_key(ROOT_INO);
    if cluster
        .get(&rk)
        .await
        .map_err(|e| anyhow!("root inode get: {e}"))?
        .is_none()
    {
        let meta = new_dir_meta();
        cluster
            .put(&rk, &schema::encode_inode_meta(&meta))
            .await
            .map_err(|e| anyhow!("root inode put: {e}"))?;
    }
    Ok(())
}

// ─── ls ─────────────────────────────────────────────────────────────────────

/// Advance a paginated range-scan cursor: `Some(next_start)` = the last
/// returned key with its final byte bumped (so the boundary entry isn't
/// re-emitted), or `None` when the page came back short (scan complete) or
/// the last key is empty. Shared by ls / cat / rm's extent walks.
fn next_range_cursor(entries: &[autumn_client::RangeEntry], page: u32) -> Option<Vec<u8>> {
    if entries.len() < page as usize {
        return None;
    }
    let mut start = entries.last()?.key.clone();
    let b = start.last_mut()?;
    *b = b.wrapping_add(1);
    Some(start)
}

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
    // Range scan over `[0x02][parent_ino BE]`. PS `handle_range` returns KEYS
    // only (`value: vec![]`), so we batch-fetch the dirent values — and, in
    // long mode, the child inode metas — via `get_many` per page instead of a
    // sequential `get` per entry.
    let prefix = key::dirent_prefix(ino);
    let mut start: Vec<u8> = Vec::new();
    const PAGE: u32 = 256;
    loop {
        let resp = cluster
            .range(&prefix, &start, PAGE)
            .await
            .map_err(|e| anyhow!("dirent range: {e}"))?;

        // Batch-fetch this page's dirent values, then decode into (name, dirent)
        // — dropping stale keys whose value is gone.
        let dkeys: Vec<&[u8]> = resp.entries.iter().map(|e| e.key.as_slice()).collect();
        let dvals = cluster.get_many(&dkeys).await;
        let mut rows: Vec<(String, DirentValue)> = Vec::with_capacity(resp.entries.len());
        for (entry, dv) in resp.entries.iter().zip(dvals.into_iter()) {
            let (parent, name_bytes) = match key::parse_dirent_key(&entry.key) {
                Some(v) => v,
                None => continue,
            };
            debug_assert_eq!(parent, ino);
            let v = match dv {
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
            rows.push((String::from_utf8_lossy(name_bytes).into_owned(), dirent));
        }

        if long {
            // Batch-fetch the child inode metas for this page.
            let ikeys: Vec<Vec<u8>> =
                rows.iter().map(|(_, d)| key::inode_key(d.child_inode)).collect();
            let ikey_refs: Vec<&[u8]> = ikeys.iter().map(|k| k.as_slice()).collect();
            let ivals = cluster.get_many(&ikey_refs).await;
            for ((name, dirent), iv) in rows.iter().zip(ivals.into_iter()) {
                let child_meta = match iv
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
                print_long_entry(name, &child_meta);
            }
        } else {
            for (name, dirent) in &rows {
                let suffix = match dirent.file_type {
                    DT_DIR => "/",
                    DT_LNK => "@",
                    _ => "",
                };
                println!("{name}{suffix}");
            }
        }

        match next_range_cursor(&resp.entries, PAGE) {
            Some(next) => start = next,
            None => break,
        }
    }
    Ok(())
}

fn print_long_entry(name: &str, m: &InodeMeta) {
    let kind = if m.mode & (libc::S_IFMT as u32) == (libc::S_IFDIR as u32) {
        'd'
    } else if m.mode & (libc::S_IFMT as u32) == (libc::S_IFLNK as u32) {
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
    m.mode & (libc::S_IFMT as u32) == (libc::S_IFDIR as u32)
}

fn is_reg(m: &InodeMeta) -> bool {
    m.mode & (libc::S_IFMT as u32) == (libc::S_IFREG as u32)
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

/// bounded per-download read window — fetch this many extents
/// per `get_many_into` so a multi-extent download pipelines (per-op bulk fan-out
/// for the ≥ 64 KiB extents; on UCX RDMA-into-dest, on TCP recv-into-dest)
/// instead of one serial `get` per extent — and, unlike `get_many`, without
/// assembling the whole window into one giant response frame.
const GET_WINDOW_MIN_EXTENTS: usize = 8;

/// RAM ceiling for one download's reused dest buffers. The window is derived
/// from this, NOT fixed, so that shrinking `MAX_EXTENT` widens the window
/// instead of silently narrowing lane coverage.
const GET_WINDOW_MAX_BYTES: usize = 192 * 1024 * 1024;

/// How many extents to fetch per `get_many_into`, given a file's stripe width.
///
/// This has to scale with `lanes`, and the reason is not obvious. Consecutive
/// extents round-robin across lanes (`lane = (off/unit) % lanes`), while a
/// PARTITION owns a CONTIGUOUS RUN of lanes (partitions are key ranges and the
/// lane byte is high). So a window of W consecutive extents touches W
/// consecutive lanes, which is only `ceil(W / (lanes/parts))` distinct
/// partitions. With lanes == parts (the old 1:1 world) a window of 8 hit 8
/// partitions; with lanes over-provisioned to 24 over 6 partitions, that same
/// window of 8 covers 8 consecutive lanes = just 2 partitions — over-
/// provisioning would have QUIETLY COST read parallelism. Sizing the window at
/// the lane count restores full spread for every partition count that divides
/// it, without the reader needing to know how many partitions there are (which
/// is exactly the placement-derived lookup this feature removed).
fn get_window_extents(lanes: u8, unit_bytes: u32) -> usize {
    let by_ram = GET_WINDOW_MAX_BYTES / (unit_bytes.max(1) as usize);
    (lanes as usize).clamp(GET_WINDOW_MIN_EXTENTS, by_ram.max(GET_WINDOW_MIN_EXTENTS))
}

async fn read_file_to_writer(
    cluster: &ClusterClient,
    ino: u64,
    meta: &InodeMeta,
    out: &mut dyn Write,
    direct_read: bool,
) -> Result<()> {
    if !is_reg(meta) {
        bail!("not a regular file");
    }
    if let Some(ref inline) = meta.inline_data {
        out.write_all(inline).context("write inline to output")?;
        return Ok(());
    }
    // Collect every extent key (offset-sorted), then fetch values a window at a
    // time. Extent keys are ≤ 18 B and extents are ≤ 8 MiB, so the full key list
    // is tiny even for a huge file, and the extra latency-before-first-byte is
    // noise against a minutes-long download — so we pre-collect (coco P3, accepted
    // for the CLI) and bound + pipeline only the VALUE fetch, which dominates.
    let mut extents: Vec<(u64, Vec<u8>)> = Vec::new(); // (offset, extent_key)
    if let Some(s) = &meta.stripe {
        // extents live under `[0x03][lane][ino][off]`, so a range
        // scan over `[0x03][ino]` would find NOTHING. Compute the key list from
        // size + geometry. Step by the file's PERSISTED `unit_bytes` (NOT the
        // MAX_EXTENT constant — see `striped_extent_offsets`) and derive each
        // extent's lane; the window `get_many_into` below then fans the reads
        // out across the lane partitions in parallel.
        let (lanes, unit) = s.checked().map_err(|e| anyhow!("read {ino}: {e}"))?;
        // coco P3: bounded enumeration (corrupt huge size can't wrap / OOM).
        for off in
            schema::striped_extent_offsets(meta.size, unit).map_err(|e| anyhow!("read {ino}: {e}"))?
        {
            extents.push((off, key::extent_key_striped(ino, off, lanes, unit)));
        }
    } else {
        // Legacy single-partition layout: range-scan `[0x03][ino]` (PS
        // `handle_range` returns KEYS only, offset-sorted).
        let prefix = key::extent_prefix(ino);
        let mut start = prefix.clone();
        const PAGE: u32 = 256;
        loop {
            let resp = cluster
                .range(&prefix, &start, PAGE)
                .await
                .map_err(|e| anyhow!("extent range: {e}"))?;
            for entry in &resp.entries {
                if let Some((_, off)) = key::parse_extent_key(&entry.key) {
                    extents.push((off, entry.key.clone()));
                }
            }
            match next_range_cursor(&resp.entries, PAGE) {
                Some(next) => start = next,
                None => break,
            }
        }
    }

    // Dest buffers reused across windows. Each extent value is ≤ MAX_EXTENT by
    // construction (autumnfs + the fuse mount both cap extents there), so a
    // whole-value read (`offset:0, length:0`) always fits; `get_many_into`
    // returns the actual byte count. Writing at the extent's LOGICAL offset (not
    // sequentially) keeps sparse/variable-extent files correct.
    // Window sized from the file's OWN stripe width (see `get_window_extents`):
    // a legacy/unstriped file keeps the old 8, a striped one widens to its lane
    // count so the window still spans every partition the lanes are spread over.
    let window = match &meta.stripe {
        Some(s) => {
            let (lanes, unit) = s.checked().map_err(|e| anyhow!("read {ino}: {e}"))?;
            get_window_extents(lanes, unit)
        }
        None => GET_WINDOW_MIN_EXTENTS,
    };
    let mut bufs: Vec<Vec<u8>> = (0..window).map(|_| vec![0u8; MAX_EXTENT]).collect();
    let mut written: u64 = 0;
    for chunk in extents.chunks(window) {
        let mut items: Vec<autumn_client::GetManyItem> = chunk
            .iter()
            .zip(bufs.iter_mut())
            .map(|((_, k), buf)| autumn_client::GetManyItem {
                key: k.as_slice(),
                offset: 0,
                length: 0,
                dest: buf.as_mut_slice(),
            })
            .collect();
        // `get_many_direct` reads every extent at or above 64 KiB STRAIGHT from
        // an extent node, taking the partition server off the data path — the
        // whole point of a bulk download. It is the same call the fuse mount
        // makes, and it degrades the same way: sub-threshold items stay on the
        // proxy, and any direct read that fails falls back to it per item, so
        // the proxy is still the authority. Measured on a 4 GiB file over
        // loopback TCP against an EC 2+1 cluster — where there is no separate
        // NIC to take out of the path, so this is the gain at its SMALLEST:
        // 769 MiB/s off, 1552-2053 on, against a raw-KV ceiling of 2522 MB/s
        // at the same shape (1 thread, depth 8, 8 MiB values).
        //
        // Measure this with `cat > /dev/null`, never `get <local-file>`: the
        // download's own write to local disk caps the whole thing at ~800 MiB/s
        // and hides every difference above it.
        let res = if direct_read {
            cluster.get_many_direct(&mut items).await
        } else {
            cluster.get_many_into(&mut items).await
        };
        drop(items); // release the &mut borrows of `bufs` before reading them
        for ((off, _), (r, buf)) in chunk.iter().zip(res.iter().zip(bufs.iter())) {
            let n = match r {
                Ok(Some(n)) => *n,
                Ok(None) => continue,
                Err(e) => bail!("extent get for offset {off}: {e}"),
            };
            // Pad with zeros if there's a hole (sparse semantics).
            if *off > written {
                let pad = *off - written;
                std::io::copy(&mut std::io::repeat(0).take(pad), out)
                    .context("pad sparse hole")?;
                written = *off;
            }
            out.write_all(&buf[..n]).context("write extent to output")?;
            written += n as u64;
        }
    }
    out.flush().context("flush output")?;
    Ok(())
}

async fn cmd_cat(cluster: &ClusterClient, path: &str, direct_read: bool) -> Result<()> {
    let (ino, _, _, meta) = resolve(cluster, path).await?;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    read_file_to_writer(cluster, ino, &meta, &mut out, direct_read).await
}

async fn cmd_get(
    cluster: &ClusterClient,
    remote: &str,
    local: &PathBuf,
    direct_read: bool,
) -> Result<()> {
    let (ino, _, _, meta) = resolve(cluster, remote).await?;
    if local.as_os_str() == "-" {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        read_file_to_writer(cluster, ino, &meta, &mut out, direct_read).await
    } else {
        let mut f = std::fs::File::create(local)
            .with_context(|| format!("create local file {}", local.display()))?;
        read_file_to_writer(cluster, ino, &meta, &mut f, direct_read).await
    }
}

// ─── put / touch ────────────────────────────────────────────────────────────

/// Read up to `cap` bytes, looping until the buffer is full or EOF. A returned
/// length < `cap` therefore reliably means EOF — a bare `Read::read` may return
/// a short count without EOF, which would corrupt the inline-vs-extent decision
/// and the extent boundaries.
fn read_full_chunk(r: &mut impl Read, cap: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; cap];
    let n = read_full_chunk_into(r, &mut buf)?;
    buf.truncate(n);
    Ok(buf)
}

/// Fill `buf` from `r` until full or EOF; returns the filled count. The
/// into-slice core of `read_full_chunk`, used by the upload loop to read
/// straight into an `alloc_value_buf` slab (stable, recycled — on UCX
/// registered — addresses; no per-chunk 8 MiB alloc+zero).
fn read_full_chunk_into(r: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

async fn write_file_from_reader(
    cluster: &ClusterClient,
    parent_ino: u64,
    leaf: &[u8],
    data_reader: impl Read,
    stripe: Option<StripeLayout>,
) -> Result<u64> {
    let new_ino = alloc_inode(cluster).await?;
    // Extents are published BEFORE the inode+dirent (they're keyed by the new
    // ino, invisible until the dirent links it). If the upload fails partway —
    // a read-source error mid-stream, a partial `put_many`, or the inode/dirent
    // put — those extents become unreachable KV leaks (the manager-global inode
    // is never reused). Track every written key and best-effort delete them +
    // the inode key on any failure (coco P2). Harmless if a key isn't present.
    let mut written: Vec<Vec<u8>> = Vec::new();
    match publish_file(cluster, parent_ino, new_ino, leaf, data_reader, stripe, &mut written).await
    {
        Ok(()) => Ok(new_ino),
        Err(e) => {
            for k in &written {
                let _ = cluster.delete(k).await;
            }
            let _ = cluster.delete(&key::inode_key(new_ino)).await;
            Err(e)
        }
    }
}

/// Stream `data_reader` into extents for `new_ino`, then publish the inode +
/// dirent. Records every written extent key in `written` so the caller can
/// clean up orphans on failure. `stripe` selects the extent key
/// layout: `Some` → lane-striped `[0x03][lane][ino][off]` (extents spread across
/// lane partitions → parallel via batch_put's concurrent bulk fan-out); `None` →
/// legacy `[0x03][ino][off]` (single partition).
async fn publish_file(
    cluster: &ClusterClient,
    parent_ino: u64,
    new_ino: u64,
    leaf: &[u8],
    mut data_reader: impl Read,
    stripe: Option<StripeLayout>,
    written: &mut Vec<Vec<u8>>,
) -> Result<()> {
    let mut meta = new_file_meta();

    // Read the FIRST chunk to decide inline vs extent: a file that fits in one
    // sub-`INLINE_THRESHOLD` read is stored inline in the inode (no extent keys),
    // matching the fuse mount's small-file layout.
    let mut off: u64 = 0;
    let first_chunk = read_full_chunk(&mut data_reader, MAX_EXTENT).context("read source")?;
    if first_chunk.len() < MAX_EXTENT && first_chunk.len() <= INLINE_THRESHOLD {
        off = first_chunk.len() as u64;
        meta.inline_data = Some(first_chunk);
    } else {
        // (A): CONTINUOUS write pipeline. The old window-of-8 +
        // full-barrier drain kept only ~window/lanes puts in flight per lane —
        // thin group-commit batches → the striped write stalled well under the
        // cluster's capacity (profiling: nothing saturated — disks < 10% util,
        // ENs ~60% of one core — so it was purely pipeline-depth / durability
        // latency bound). Instead keep `depth` puts in flight at all times
        // (read-ahead refills as completions land, no barrier), so each lane PS
        // sees a DEEP, fat-batchable arrival stream and the durable-write latency
        // is hidden. `depth` scales with lane count (each lane wants ~PS
        // inflight-cap worth); non-striped stays shallow (one partition).
        use futures::stream::{FuturesUnordered, StreamExt};
        let depth = match &stripe {
            Some(s) => (s.lanes as usize * 12).clamp(24, 96),
            None => 8,
        };
        let mut inflight = FuturesUnordered::new();
        // chunks ride in RegPool-backed ValueBufs — `depth` slabs
        // cycle with STABLE addresses (UCX: registered → rcache hits from the
        // second round, no per-chunk ~100µs×rails re-registration; TCP: plain
        // recycling, no per-chunk 8 MiB alloc). The already-read first chunk
        // (a Vec, from the inline-vs-extent decision) is staged into a slab
        // once — every later chunk is read DIRECTLY into its slab.
        let first_len = first_chunk.len();
        let mut first_vb = autumn_client::alloc_value_buf(MAX_EXTENT);
        first_vb.as_mut_slice()[..first_len].copy_from_slice(&first_chunk);
        first_vb.truncate(first_len);
        drop(first_chunk);
        let mut pending = Some(first_vb); // the already-read first chunk
        let mut eof = false;
        // coco P1: on ANY error (source read or a put), STOP scheduling new puts
        // but keep DRAINING `inflight` to terminal state before returning — do NOT
        // just drop the FuturesUnordered. A dropped-but-already-sent MSG_PUT_BULK may
        // still commit server-side; if the caller's orphan cleanup (delete every
        // `written` key) ran before that late put landed, it would leave an
        // unreachable extent. Draining quiesces all sent puts first.
        let mut fail: Option<anyhow::Error> = None;
        loop {
            if fail.is_none() && !eof && inflight.len() < depth {
                let chunk = match pending.take() {
                    Some(c) => c,
                    None => {
                        let mut vb = autumn_client::alloc_value_buf(MAX_EXTENT);
                        match read_full_chunk_into(&mut data_reader, vb.as_mut_slice()) {
                            Ok(n) => {
                                vb.truncate(n);
                                vb
                            }
                            Err(e) => {
                                fail = Some(anyhow!("read source: {e}"));
                                eof = true;
                                continue; // stop reading; fall through to drain
                            }
                        }
                    }
                };
                let n = chunk.len();
                if n == 0 {
                    eof = true;
                } else {
                    let ek = match &stripe {
                        Some(s) => key::extent_key_striped(new_ino, off, s.lanes, s.unit_bytes),
                        None => key::extent_key(new_ino, off),
                    };
                    // Track BEFORE the RPC — a put may land then a later one fail,
                    // so orphan cleanup must cover the partial success (coco P2).
                    written.push(ek.clone());
                    let value = chunk.freeze(); // aliases the pool slab, 0-copy
                    inflight.push(async move { cluster.put_bulk(&ek, value).await });
                    off += n as u64;
                    if n < MAX_EXTENT {
                        eof = true;
                    }
                }
                continue;
            }
            match inflight.next().await {
                Some(Ok(())) => {}
                Some(Err(e)) => {
                    if fail.is_none() {
                        fail = Some(anyhow!("put extent: {e}"));
                    }
                    eof = true; // stop scheduling; keep draining the rest
                }
                None => break, // all sent puts reached terminal state
            }
        }
        if let Some(e) = fail {
            return Err(e); // caller cleans up `written` — safe now (all puts settled)
        }
    }
    meta.size = off;
    // Only stamp `stripe` for a file that actually took the extent path — an
    // inline small file has no extents to stripe (reader checks inline_data first).
    meta.stripe = if meta.inline_data.is_some() { None } else { stripe };

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
    Ok(())
}

/// the fs-wide DECLARED stripe geometry, read once per
/// invocation. This replaces `detect_stripe_lanes`, which reverse-engineered the
/// lane count from the CURRENT partition split points on every large-file
/// create. Three things went wrong with that and all three die here:
///   * a merged lane boundary silently narrowed every subsequent file
///     (BUG-FS-LANE-MERGE) — placement is not a declaration;
///   * any lookup error degraded to `lanes = 1`, so one transient manager blip
///     produced a permanently single-partition 41 GB file whose only symptom was
///     bad throughput. `read_stripe_geom` propagates instead;
///   * it hardcoded the wire prefix `b"fs/"` in a CLI, a byte string that has
///     already changed twice (tenant-first, then Option 3).
async fn declared_stripe_geom(cluster: &ClusterClient) -> Result<StripeLayout> {
    autumn_fuse::geom::read_stripe_geom(cluster).await
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
    // stripe EVERY file in a striped fs — the 64 MiB
    // threshold is gone. It protected almost nothing: with unit = MAX_EXTENT a
    // file of one extent or less has a single extent, which lands on lane 0
    // regardless, i.e. byte-for-byte the same placement as not striping; and
    // metadata (0x01/0x02) sorts below `[0x03][1]` so it never leaves lane 0
    // either. Its only real effect was to REQUIRE the final size at create
    // time — which is exactly what made streaming (fuse mount) writes
    // unstripeable, since the lane function itself is incremental.
    // `lanes < 2` is the explicit opt-OUT (`presplit --lanes 1`); everything
    // else stripes, whether or not the partitions were ever cut.
    let stripe = Some(declared_stripe_geom(cluster).await?).filter(|g| g.lanes >= 2);
    let striped_note = stripe
        .as_ref()
        .map(|s| format!(" (striped ×{} lanes)", s.lanes))
        .unwrap_or_default();
    let ino = write_file_from_reader(cluster, parent_ino, &leaf, f, stripe).await?;
    println!("uploaded → ino {ino}{striped_note}");
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
    // Empty file (never striped).
    let ino = write_file_from_reader(cluster, parent_ino, &leaf, std::io::empty(), None).await?;
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
        if let Some(s) = &meta.stripe {
            // striped extents live under `[0x03][lane][ino][off]`,
            // spread across lane partitions — a `[0x03][ino]` scan would MISS
            // them (leak). Compute + delete each key (same enumeration the read
            // path uses: stride = the file's PERSISTED unit_bytes, up to size).
            let (lanes, unit) = s.checked().map_err(|e| anyhow!("rm {ino}: {e}"))?;
            // coco P3: bounded enumeration (corrupt huge size can't wrap / OOM).
            for off in
                schema::striped_extent_offsets(meta.size, unit).map_err(|e| anyhow!("rm {ino}: {e}"))?
            {
                let ek = key::extent_key_striped(ino, off, lanes, unit);
                cluster.delete(&ek).await.map_err(|e| anyhow!("delete extent: {e}"))?;
            }
        } else {
            let prefix = key::extent_prefix(ino);
            let mut start = prefix.clone();
            const PAGE: u32 = 256;
            loop {
                let resp = cluster
                    .range(&prefix, &start, PAGE)
                    .await
                    .map_err(|e| anyhow!("extent range: {e}"))?;
                for entry in &resp.entries {
                    cluster
                        .delete(&entry.key)
                        .await
                        .map_err(|e| anyhow!("delete extent: {e}"))?;
                }
                match next_range_cursor(&resp.entries, PAGE) {
                    Some(next) => start = next,
                    None => break,
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A `Read` that hands back at most `step` bytes per call — models a socket
    /// / pipe that returns short reads WITHOUT EOF. `read_full_chunk` must keep
    /// looping until the requested `cap` is filled (or true EOF), so a short
    /// syscall never fakes an early inline-vs-extent decision or a short extent.
    struct ChoppyReader {
        inner: Cursor<Vec<u8>>,
        step: usize,
    }
    impl Read for ChoppyReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let cap = self.step.min(buf.len());
            self.inner.read(&mut buf[..cap])
        }
    }

    #[test]
    fn read_full_chunk_fills_despite_short_reads() {
        // 5904 bytes, cap 4096 ⇒ one full chunk + one short (< cap) chunk.
        let data: Vec<u8> = (0..5904u32).map(|i| i as u8).collect();
        let mut r = ChoppyReader { inner: Cursor::new(data.clone()), step: 7 };
        // First chunk: exactly `cap` bytes even though each read yields ≤ 7.
        let c1 = read_full_chunk(&mut r, 4096).unwrap();
        assert_eq!(c1.len(), 4096, "must fill cap despite 7-byte reads");
        assert_eq!(c1, &data[..4096]);
        // Second chunk: the remaining 1808 (< cap) ⇒ signals EOF via short len.
        let c2 = read_full_chunk(&mut r, 4096).unwrap();
        assert_eq!(c2.len(), 5904 - 4096, "short (< cap) len ⇒ EOF reached");
        assert_eq!(c2, &data[4096..]);
        // Third chunk: nothing left.
        let c3 = read_full_chunk(&mut r, 4096).unwrap();
        assert!(c3.is_empty(), "past EOF ⇒ empty");
    }

    #[test]
    fn read_full_chunk_exact_multiple_is_not_early_eof() {
        // cap-sized file: first read fills exactly, and only the NEXT read
        // observes EOF (empty) — so a full extent is never mistaken for the
        // last one.
        let data = vec![0xABu8; 4096];
        let mut r = ChoppyReader { inner: Cursor::new(data), step: 100 };
        let c1 = read_full_chunk(&mut r, 4096).unwrap();
        assert_eq!(c1.len(), 4096);
        let c2 = read_full_chunk(&mut r, 4096).unwrap();
        assert!(c2.is_empty());
    }
}
