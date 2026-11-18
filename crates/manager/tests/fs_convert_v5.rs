//! `convert_fs_v5` (the real binary, `target/debug/convert_fs_v5`) against an
//! in-process cluster. A v4 tree cannot be written by a v5 build, so each case
//! writes a tree, moves its metadata keys back to the v4 shape and stamps 4 —
//! the converter's exact inverse — and then checks the converted tree reads
//! back the same: names, types, link targets and bytes.

mod support;

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::time::Duration;

use autumn_client::{ClusterClient, WriteOptions};
use autumn_fs::publish::{Condition, NewFile};
use autumn_fs::schema::{DT_DIR, DT_LNK, SCHEMA_CONVERTING_TO_V5};
use autumn_fs::state::FsState;
use autumn_fs::{dir, key, meta, read};
use autumn_rpc::client::RpcClient;

use support::*;

/// path → (type, bytes or link target), over the whole tree.
type Tree = BTreeMap<String, (u8, Vec<u8>)>;

async fn snapshot(st: &mut FsState) -> Tree {
    let mut out = Tree::new();
    let mut todo = vec![(String::new(), 1u64)];
    while let Some((path, ino)) = todo.pop() {
        for e in dir::readdir(st, ino, 0).await.unwrap() {
            let name = e.name.to_string_lossy().into_owned();
            if name == "." || name == ".." {
                continue;
            }
            let p = format!("{path}/{name}");
            let body = match e.kind {
                DT_DIR => {
                    todo.push((p.clone(), e.ino));
                    Vec::new()
                }
                DT_LNK => dir::readlink(st, e.ino).await.unwrap(),
                _ => {
                    let m = meta::fetch_inode(st, e.ino).await.unwrap();
                    let mut b = Vec::new();
                    while (b.len() as u64) < m.size {
                        b.extend(read::read(st, e.ino, b.len() as i64, 4 << 20).await.unwrap());
                    }
                    b
                }
            };
            out.insert(p, (e.kind, body));
        }
    }
    out
}

async fn put(st: &mut FsState, parent: u64, name: &str, data: &[u8]) {
    let mut f = NewFile::begin(st, parent, name.as_bytes()).await.unwrap();
    for chunk in data.chunks(3 << 20) {
        f.write(st, chunk).await.unwrap();
    }
    f.finish(st).await.unwrap();
    f.publish(st, Condition::None).await.unwrap();
}

/// The v4 key of a v5 metadata key: `[0x06][h][kind][owner][name]` →
/// `[kind][owner][name]`.
fn v4_key(k: &[u8]) -> Vec<u8> {
    assert_eq!(k[0], 0x06);
    k[2..].to_vec()
}

/// Move `fraction` of the tree's metadata keys back to the v4 shape.
async fn back_to_v4(c: &ClusterClient, every_nth: usize) -> usize {
    let mut keys = Vec::new();
    let mut from = key::meta_region();
    loop {
        let page = c.range(&key::meta_region(), &from, 512).await.unwrap();
        keys.extend(page.entries.iter().map(|e| e.key.clone()));
        match page.entries.last() {
            Some(last) if page.has_more => from = [last.key.as_slice(), &[0]].concat(),
            _ => break,
        }
    }
    let mut moved = 0;
    for k in keys.iter().step_by(every_nth) {
        let v = c.get(k).await.unwrap().unwrap();
        c.put(&v4_key(k), v, WriteOptions::default()).await.unwrap();
        c.delete(k, WriteOptions::default()).await.unwrap();
        moved += 1;
    }
    moved
}

async fn stamp(c: &ClusterClient, v: u64) {
    c.put(&key::schema_version_key(), v.to_be_bytes().to_vec(), WriteOptions::default()).await.unwrap();
}

fn run_converter(mgr: std::net::SocketAddr) -> String {
    let out = std::process::Command::new(support_binary_path("convert_fs_v5"))
        .args(["--manager", &mgr.to_string()])
        .output()
        .expect("run convert_fs_v5");
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "convert_fs_v5 failed: {text}");
    text
}

/// Boot a cluster with an fs tree in it; returns the manager and the tree.
fn cluster_with_tree(base: u16) -> (std::net::SocketAddr, Tree, (tempfile::TempDir, tempfile::TempDir)) {
    let mgr = pick_addr();
    start_manager(mgr);
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (n1, n2) = (pick_addr(), pick_addr());
    start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
    start_extent_node(n2, dirs.1.path().to_path_buf(), 2);
    let tree = compio::runtime::Runtime::new().unwrap().block_on(async {
        let m = RpcClient::connect(mgr).await.unwrap();
        register_two_nodes(&m, n1, n2, base).await;
        let (log, row, meta_stream) = create_three_streams(&m).await;
        upsert_partition(&m, base as u64 * 100 + 1, log, row, meta_stream, b"", b"\xff\xff\xff\xff").await;
        start_partition_server(base as u64, mgr, pick_addr());
        compio::time::sleep(Duration::from_millis(1500)).await;
        create_namespaces(mgr, &["fs"]).await;
        let mut st = FsState::new(&mgr.to_string()).await.unwrap();
        meta::ensure_root(&mut st).await.unwrap();
        let (b, _) = dir::mkdir(&mut st, 1, OsStr::new("b"), 0o755).await.unwrap();
        let (d, _) = dir::mkdir(&mut st, b, OsStr::new("d"), 0o755).await.unwrap();
        // More than one 512-key page of inodes and of directory entries.
        for i in 0..600u32 {
            put(&mut st, d, &format!("small-{i}"), format!("inline {i}").as_bytes()).await;
        }
        let big: Vec<u8> = (0..(9u32 << 20)).map(|i| (i % 251) as u8).collect();
        put(&mut st, b, "big", &big).await;
        dir::symlink(&mut st, b, OsStr::new("link"), b"d/small-3").await.unwrap();
        snapshot(&mut st).await
    });
    assert_eq!(tree.len(), 604, "b, d, 600 small files, big, link");
    (mgr, tree, dirs)
}

#[test]
fn a_v4_tree_converts_and_reads_back_the_same() {
    let (mgr, before, _dirs) = cluster_with_tree(271);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = ClusterClient::connect(&mgr.to_string(), "fs").await.unwrap();
        let moved = back_to_v4(&c, 1).await;
        stamp(&c, 4).await;
        let mut st = FsState::new(&mgr.to_string()).await.unwrap();
        let refused = meta::ensure_root(&mut st).await.unwrap_err().to_string();
        assert!(refused.contains("schema v4") && refused.contains("convert_fs_v5"), "{refused}");

        let out = run_converter(mgr);
        // Every directory has an inode and an entry; files and the link too.
        assert!(out.contains(&format!("moved {} inodes", 1 + 604)), "{out} ({moved} keys moved back)");
        assert!(c.range(&[0x01], &[0x01], 1).await.unwrap().entries.is_empty(), "no v4 inode left");
        assert!(c.range(&[0x02], &[0x02], 1).await.unwrap().entries.is_empty(), "no v4 entry left");

        let mut st = FsState::new(&mgr.to_string()).await.unwrap();
        meta::ensure_root(&mut st).await.expect("v5 mounts");
        assert_eq!(snapshot(&mut st).await, before);
        assert!(run_converter(mgr).contains("already schema v5"), "a second run does nothing");
    });
}

/// A run stopped part way leaves the converting stamp and keys in both
/// shapes; the next run finishes it.
#[test]
fn a_conversion_stopped_part_way_is_finished_by_the_next_run() {
    let (mgr, before, _dirs) = cluster_with_tree(272);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let c = ClusterClient::connect(&mgr.to_string(), "fs").await.unwrap();
        back_to_v4(&c, 2).await;
        stamp(&c, SCHEMA_CONVERTING_TO_V5).await;
        let mut st = FsState::new(&mgr.to_string()).await.unwrap();
        let refused = meta::ensure_root(&mut st).await.unwrap_err().to_string();
        assert!(refused.contains("being converted"), "{refused}");

        run_converter(mgr);
        let mut st = FsState::new(&mgr.to_string()).await.unwrap();
        meta::ensure_root(&mut st).await.expect("v5 mounts");
        assert_eq!(snapshot(&mut st).await, before);
    });
}
