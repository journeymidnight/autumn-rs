//! `autumnfs` — standalone CLI for poking at the autumn-rs FUSE filesystem
//! without mounting it.
//!
//! Skips the `autumn-fuse` kernel mount; talks directly to the
//! autumn-rs cluster via [`autumn_client::ClusterClient`]
//! using the [`autumn_fs::key`] + [`autumn_fs::schema`] modules — the same
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
//! `autumnfs` connects SCOPED to the WHOLE `fs/` namespace so its keys land in the SAME keyspace a
//! fuse mount uses — a write here is visible to a mount, and vice versa. Inode
//! numbers come from the MANAGER's
//! crash-safe GLOBAL counter (`alloc_inodes`), the same source the fuse mount +
//! PyO3 `autumn.Fs` use, so autumnfs and a mount never hand out colliding inodes
//! (the pre-SD-3 racy non-CAS KV counter is gone).

use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use autumn_params::autumnfs::AutumnfsParams;
use autumn_params::{Positional, Section};

use std::os::unix::ffi::OsStrExt;

use autumn_client::ClusterClient;
use autumn_fs::key;
use autumn_fs::state::FsState;
use autumn_fs::{dir, meta, segment};
use autumn_fs::schema::{
    self, DirentValue, InodeMeta, StripeLayout, DT_DIR, DT_LNK, DT_REG, INLINE_THRESHOLD,
    MAX_EXTENT, ROOT_INO,
};

/// autumnfs CLI parameters (`autumn_params::autumnfs`).
fn params() -> &'static AutumnfsParams {
    static PARAMS: std::sync::LazyLock<AutumnfsParams> =
        std::sync::LazyLock::new(AutumnfsParams::default);
    &PARAMS
}

const USAGE: &str = "\
usage: autumnfs [options] <COMMAND>
POSIX-ish CLI over the autumn-rs fuse KV layout (no mount needed).

commands:
  ls [-l|--long] [PATH]    list a directory (default /)
  stat <PATH>              inode metadata
  mkdir <PATH>             create a directory (parents must exist)
  cat <PATH>               print a file to stdout
  put <LOCAL> <REMOTE>     upload a local file
  get <REMOTE> <LOCAL>     download a file (LOCAL `-` = stdout)
  rm <PATH>                remove a file or empty directory
  touch <PATH>             create an empty file";

/// Global flags land in the parameter structs; the words from the command on
/// are the command.
fn parse_args() -> Cmd {
    let sections: &[&dyn Section] = &[params(), autumn_fs::params(), autumn_client::params()];
    let rest = autumn_params::parse_or_exit(USAGE, sections, Positional::Stop);
    if rest.iter().any(|w| w == "-h" || w == "--help") {
        print!("{}", autumn_params::help_text(USAGE, sections));
        std::process::exit(0);
    }
    Cmd::parse(&rest).unwrap_or_else(|e| {
        eprintln!("error: {e}\n\n{USAGE}");
        std::process::exit(2);
    })
}

#[derive(Debug, PartialEq)]
enum Cmd {
    Ls { path: String, long: bool },
    Stat { path: String },
    Mkdir { path: String },
    Cat { path: String },
    Put { local: PathBuf, remote: String },
    Get { remote: String, local: PathBuf },
    Rm { path: String },
    Touch { path: String },
}

impl Cmd {
    fn parse(words: &[String]) -> Result<Cmd, String> {
        let Some((sub, rest)) = words.split_first() else {
            return Err("a command is required".into());
        };
        let rest: Vec<&str> = rest.iter().map(String::as_str).collect();
        let one = |rest: &[&str]| match rest {
            [p] => Ok(p.to_string()),
            _ => Err(format!("{sub} takes one <PATH>")),
        };
        Ok(match sub.as_str() {
            "ls" => {
                let long = rest.iter().any(|w| matches!(*w, "-l" | "--long"));
                let paths: Vec<&str> =
                    rest.iter().copied().filter(|w| !matches!(*w, "-l" | "--long")).collect();
                let path = match paths[..] {
                    [] => "/".to_string(),
                    [p] => p.to_string(),
                    _ => return Err("ls takes at most one PATH".into()),
                };
                Cmd::Ls { path, long }
            }
            "stat" => Cmd::Stat { path: one(&rest)? },
            "mkdir" => Cmd::Mkdir { path: one(&rest)? },
            "cat" => Cmd::Cat { path: one(&rest)? },
            "rm" => Cmd::Rm { path: one(&rest)? },
            "touch" => Cmd::Touch { path: one(&rest)? },
            "put" => match rest[..] {
                [l, r] => Cmd::Put { local: l.into(), remote: r.to_string() },
                _ => return Err("put takes <LOCAL> <REMOTE>".into()),
            },
            "get" => match rest[..] {
                [r, l] => Cmd::Get { remote: r.to_string(), local: l.into() },
                _ => return Err("get takes <REMOTE> <LOCAL>".into()),
            },
            other => return Err(format!("unknown command {other:?}")),
        })
    }
}

fn main() -> Result<()> {
    let cmd = parse_args();
    let p = params();
    if !autumn_transport::set_regpool_cap_bytes(autumn_client::params().ucx_regpool_cap_bytes.get()) {
        tracing::warn!("regpool cap already set (ignored — first-call-wins)");
    }
    // Stand up the same transport selector autumn-client uses everywhere.
    let _ = autumn_transport::init_with(p.transport.get().into());

    let rt = compio::runtime::RuntimeBuilder::new()
        .build()
        .context("create compio runtime")?;
    rt.block_on(async move {
        // scope the client to the WHOLE `fs/` namespace —
        // the binding prepends `fs/` to every relative fuse key (and strips it off
        // range results), so a write here is visible to a fuse mount. Every
        // `key::*`-based op below is unchanged; the client owns the prefix.
        let cluster = match &p.credential_file.get() {
            Some(path) => {
                let (principal, secret) = autumn_client::read_credential_file(path)
                    .context("--credential-file")?;
                if principal.is_empty() {
                    bail!("--credential-file: missing principal name (expected '<principal>\\n<hex>')");
                }
                ClusterClient::connect_with_credential(
                    &p.manager.get(),
                    "fs",
                    principal,
                    secret,
                )
                .await
                .context("connect to manager (with credential)")?
            }
            None => ClusterClient::connect(&p.manager.get(), "fs")
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
        // `resolve()` from ROOT_INO would fail `ENOENT inode 1`. The core's
        // `ensure_root` also verifies (or stamps) the filesystem's schema
        // version, so this build refuses a layout it cannot read instead of
        // decoding it wrong — which the CLI's own copy of this step never did.
        let host = p.host.get().unwrap_or_else(autumn_fs::state::system_hostname);
        let mut state = FsState::from_client(cluster, format!("autumnfs-{host}"));
        meta::ensure_root(&mut state).await?;
        let cluster = state.client.clone();

        match cmd {
            Cmd::Ls { path, long } => cmd_ls(&cluster, &path, long).await,
            Cmd::Stat { path } => cmd_stat(&cluster, &path).await,
            Cmd::Mkdir { path } => cmd_mkdir(&mut state, &path).await,
            Cmd::Cat { path } => cmd_cat(&cluster, &path, p.direct_read.get()).await,
            Cmd::Put { local, remote } => {
                cmd_put(&cluster, &local, &remote).await
            }
            Cmd::Get { remote, local } => {
                cmd_get(&cluster, &remote, &local, p.direct_read.get()).await
            }
            Cmd::Rm { path } => cmd_rm(&mut state, &path).await,
            Cmd::Touch { path } => cmd_touch(&mut state, &path).await,
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
        .alloc_inodes(1, 0)
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
        generation: 1,
        segments: None,
    }
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

async fn cmd_mkdir(state: &mut FsState, path: &str) -> Result<()> {
    let (parent, name) = resolve_parent_name(state, path).await?;
    dir::mkdir(state, parent, std::ffi::OsStr::from_bytes(&name), 0o755).await?;
    Ok(())
}

/// The directory a path's last component lives in, and that component.
async fn resolve_parent_name(state: &mut FsState, path: &str) -> Result<(u64, Vec<u8>)> {
    let comps = split_path(path);
    let Some((leaf, parents)) = comps.split_last() else {
        bail!("the root has no parent");
    };
    let parent_path: Vec<String> = parents.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect();
    let parent = dir::resolve(state, &format!("/{}", parent_path.join("/")))
        .await?
        .ok_or_else(|| anyhow!("ENOENT: {path}"))?;
    if !is_dir(&meta::fetch_inode(state, parent).await?) {
        bail!("ENOTDIR: the parent of {path} is not a directory");
    }
    Ok((parent, leaf.to_vec()))
}

// ─── cat / get ──────────────────────────────────────────────────────────────

/// bounded per-download read window — fetch this many extents
/// per `get_many_into` so a multi-extent download pipelines (per-op bulk fan-out
/// for the ≥ 64 KiB extents; on UCX RDMA-into-dest, on TCP recv-into-dest)
/// instead of one serial `get` per extent — and, unlike `get_many`, without
/// assembling the whole window into one giant response frame
/// (`--get-window-min-extents`).
fn get_window_min_extents() -> usize {
    params().get_window_min_extents.get()
}

/// RAM ceiling for one download's reused dest buffers
/// (`--get-window-max-bytes`). The window is derived from this, NOT fixed, so
/// that shrinking `MAX_EXTENT` widens the window instead of silently narrowing
/// lane coverage.
fn get_window_max_bytes() -> usize {
    params().get_window_max_bytes.get()
}

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
    let min = get_window_min_extents();
    let by_ram = get_window_max_bytes() / (unit_bytes.max(1) as usize);
    (lanes as usize).clamp(min, by_ram.max(min))
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
    if let Some(map) = &meta.segments {
        return read_segmented_to_writer(cluster, ino, meta.size, map, out, direct_read).await;
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
        None => get_window_min_extents(),
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

/// A segmented file, a window at a time: plan each window from the map and
/// fetch its data extents in one batch. Mapped ranges must come back whole
/// (a data object is dense); holes are written as zeros.
async fn read_segmented_to_writer(
    cluster: &ClusterClient,
    ino: u64,
    size: u64,
    map: &schema::SegmentMap,
    out: &mut dyn Write,
    direct_read: bool,
) -> Result<()> {
    let window = (get_window_min_extents() * MAX_EXTENT) as u64;
    let mut pages = segment::PageCache::new();
    let mut buf = vec![0u8; window as usize];
    let mut pos = 0u64;
    while pos < size {
        let end = (pos + window).min(size);
        let segs = segment::load_range(cluster, &mut pages, map, pos, end).await?;
        let plan = segment::plan_reads(&segs, pos, end);
        let region = &mut buf[..(end - pos) as usize];
        region.fill(0);
        let mut rest = &mut region[..];
        let mut consumed = 0usize;
        let mut dests = Vec::with_capacity(plan.len());
        let mut sorted: Vec<&segment::DataRead> = plan.iter().collect();
        sorted.sort_by_key(|r| r.dest_offset);
        for r in &sorted {
            let (_, after) = rest.split_at_mut(r.dest_offset - consumed);
            let (head, tail) = after.split_at_mut(r.length as usize);
            dests.push(head);
            rest = tail;
            consumed = r.dest_offset + r.length as usize;
        }
        let mut items: Vec<autumn_client::GetManyItem> = sorted
            .iter()
            .zip(dests)
            .map(|(r, dest)| autumn_client::GetManyItem { key: &r.key, offset: r.offset, length: r.length, dest })
            .collect();
        let res = if direct_read {
            cluster.get_many_direct(&mut items).await
        } else {
            cluster.get_many_into(&mut items).await
        };
        drop(items);
        for (r, got) in sorted.iter().zip(res) {
            match got {
                Ok(Some(n)) if n == r.length as usize => {}
                other => bail!("read {ino}: data extent {:?} returned {other:?}", r.key),
            }
        }
        out.write_all(region).context("write to output")?;
        pos = end;
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
    // Extents and the inode are written BEFORE the dirent names them, so a
    // failure up to the dirent leaves only unreachable keys: every written
    // key and the inode key are deleted (the manager-global inode is never
    // reused).
    let mut written: Vec<Vec<u8>> = Vec::new();
    let failed = match write_file_data(cluster, new_ino, data_reader, stripe, &mut written).await {
        Err(e) => e,
        // Only if the name is still free: a name taken meanwhile is kept.
        Ok(()) => {
            let dirent = schema::encode_dirent(&DirentValue { child_inode: new_ino, file_type: DT_REG });
            match dir::compare_put_settled(cluster, &key::dirent_key(parent_ino, leaf), None, &dirent).await {
                Ok(true) => return Ok(new_ino),
                Ok(false) => anyhow!("destination already exists"),
                // A copy may still land and name the inode: its keys stay.
                Err(e) => return Err(e.context(format!("ino {new_ino} may be named; its keys are kept"))),
            }
        }
    };
    for k in written.iter().chain([&key::inode_key(new_ino)]) {
        if let Err(e) = cluster.delete(k).await {
            eprintln!("ino {new_ino}: a key of the abandoned file not deleted (leaked): {e}");
        }
    }
    Err(failed)
}

/// Stream `data_reader` into extents for `new_ino`, then write its inode.
/// Records every written extent key in `written` so the caller can clean up
/// orphans on failure. `stripe` selects the extent key
/// layout: `Some` → lane-striped `[0x03][lane][ino][off]` (extents spread across
/// lane partitions → parallel via batch_put's concurrent bulk fan-out); `None` →
/// legacy `[0x03][ino][off]` (single partition).
async fn write_file_data(
    cluster: &ClusterClient,
    new_ino: u64,
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
        let p = params();
        let depth = match &stripe {
            Some(s) => (s.lanes as usize * p.put_depth_per_lane.get())
                .clamp(p.put_depth_striped_min.get(), p.put_depth_striped_max.get()),
            None => p.put_depth.get(),
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
///   * it hardcoded the wire prefix `b"fs/"` in a CLI.
async fn declared_stripe_geom(cluster: &ClusterClient) -> Result<StripeLayout> {
    autumn_fs::geom::read_stripe_geom(cluster).await
}

async fn cmd_put(cluster: &ClusterClient, local: &PathBuf, remote: &str) -> Result<()> {
    let (parent_ino, parent_meta, leaf) = resolve_parent_leaf(cluster, remote).await?;
    if !is_dir(&parent_meta) {
        bail!("ENOTDIR: the parent of {remote} is not a directory");
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

async fn cmd_touch(state: &mut FsState, path: &str) -> Result<()> {
    let (parent, name) = resolve_parent_name(state, path).await?;
    if let Some((ino, mut m)) = dir::lookup_opt(state, parent, std::ffi::OsStr::from_bytes(&name)).await? {
        // Already exists: move its mtime (a whole-inode write keeps the
        // stored link count).
        let (secs, nsecs) = now_ts();
        m.mtime_secs = secs;
        m.mtime_nsecs = nsecs;
        meta::put_inode(state, ino, &m).await?;
        return Ok(());
    }
    let cluster = state.client.clone();
    // Empty file (never striped).
    let ino = write_file_from_reader(&cluster, parent, &name, std::io::empty(), None).await?;
    println!("touched → ino {ino}");
    Ok(())
}

// ─── rm ─────────────────────────────────────────────────────────────────────

/// Remove a file or an empty directory through the FS core, so the CLI
/// gets the same unlink as every other front-end: the tombstoned removal of
/// an unreachable inode, and the reclaim of a segmented file's data objects.
async fn cmd_rm(state: &mut FsState, path: &str) -> Result<()> {
    if split_path(path).is_empty() {
        bail!("refusing to remove root");
    }
    let (parent, leaf) = resolve_parent_name(state, path).await?;
    let name = std::ffi::OsStr::from_bytes(&leaf);
    let (_, m) = dir::lookup_opt(state, parent, name)
        .await?
        .ok_or_else(|| anyhow!("ENOENT: {path}"))?;
    if is_dir(&m) {
        dir::rmdir(state, parent, name).await
    } else if is_reg(&m) {
        dir::unlink(state, parent, name).await
    } else {
        bail!("unsupported file type")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn commands_parse_like_the_usage_says() {
        let p = |s: &str| Cmd::parse(&s.split_whitespace().map(String::from).collect::<Vec<_>>());
        assert_eq!(p("ls"), Ok(Cmd::Ls { path: "/".into(), long: false }));
        assert_eq!(p("ls -l /d"), Ok(Cmd::Ls { path: "/d".into(), long: true }));
        assert_eq!(p("ls /d --long"), Ok(Cmd::Ls { path: "/d".into(), long: true }));
        assert_eq!(p("put a /b"), Ok(Cmd::Put { local: "a".into(), remote: "/b".into() }));
        assert_eq!(p("get /b -"), Ok(Cmd::Get { remote: "/b".into(), local: "-".into() }));
        for bad in ["", "stat", "stat a b", "ls a b", "put a", "frob x"] {
            assert!(p(bad).is_err(), "{bad:?} accepted");
        }
    }

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
