#[path = "../../manager/tests/support/mod.rs"]
mod support;

use autumn_object_store::AutumnObjectStore;
use autumn_rpc::client::RpcClient;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, *};
use support::*;

fn cluster() -> (String, Vec<tempfile::TempDir>) {
    // macOS does not provide CPU binding; leave this local correctness harness
    // unpinned. Linux validation uses the production affinity policy.
    #[cfg(target_os = "macos")]
    autumn_common::cpu_pin::set_cpu_offset(1_000_000);
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dirs = vec![tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let en1 = pick_addr();
    let en2 = pick_addr();
    start_extent_node(en1, dirs[0].path().to_path_buf(), 1);
    start_extent_node(en2, dirs[1].path().to_path_buf(), 2);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, en1, en2, 300).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"", b"").await;
    });
    let ps = pick_addr();
    start_partition_server(95, mgr_addr, ps);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        PsRouter::new(mgr_addr, ps).client_for(901).await;
    });
    (mgr_addr.to_string(), dirs)
}

#[test]
fn object_store_contract() {
    let (manager, _dirs) = cluster();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let store = AutumnObjectStore::connect(&manager, "fs/objects/contract")
            .await
            .unwrap();
        integration::put_get_delete_list(&store).await;
        integration::get_opts(&store).await;
        integration::put_opts(&store, true).await;
        integration::list_uses_directories_correctly(&store).await;
        integration::list_with_delimiter(&store).await;
        integration::rename_and_copy(&store).await;
        integration::copy_if_not_exists(&store).await;
        integration::copy_rename_nonexistent_object(&store).await;
        integration::multipart_out_of_order(&store).await;
        integration::list_with_offset_exclusivity(&store).await;
        let path = Path::from("abort-test");
        let mut upload = store.put_multipart(&path).await.unwrap();
        let pending = upload.put_part("unpolled".into());
        assert!(upload.complete().await.is_err());
        upload.abort().await.unwrap();
        pending.await.unwrap();
        assert!(upload.complete().await.is_err());
        assert!(matches!(
            store.head(&path).await,
            Err(Error::NotFound { .. })
        ));
    });
}

#[test]
fn hard_link_manifest_publication() {
    let (manager, _dirs) = cluster();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        use autumn_fuse::{dir, meta, schema::ROOT_INO, state::FsState};
        use std::ffi::OsStr;
        let mut fs = FsState::new(&manager).await.unwrap();
        meta::ensure_root(&mut fs).await.unwrap();
        let (ino, _) = dir::create(&mut fs, ROOT_INO, OsStr::new("manifest.tmp"), 0o644)
            .await
            .unwrap();
        let linked = dir::link(&mut fs, ino, ROOT_INO, OsStr::new("manifest"))
            .await
            .unwrap();
        assert_eq!(linked.nlink, 2);
        let (loser, _) = dir::create(&mut fs, ROOT_INO, OsStr::new("loser.tmp"), 0o644)
            .await
            .unwrap();
        assert!(dir::link(&mut fs, loser, ROOT_INO, OsStr::new("manifest"))
            .await
            .unwrap_err()
            .to_string()
            .contains("EEXIST"));
        assert_eq!(
            dir::lookup(&mut fs, ROOT_INO, OsStr::new("manifest"))
                .await
                .unwrap()
                .0,
            ino
        );
        dir::unlink(&mut fs, ROOT_INO, OsStr::new("manifest.tmp"))
            .await
            .unwrap();
        let (found, metadata) = dir::lookup(&mut fs, ROOT_INO, OsStr::new("manifest"))
            .await
            .unwrap();
        assert_eq!(found, ino);
        assert_eq!(metadata.nlink, 1);
        assert!(
            dir::link(&mut fs, ROOT_INO, ROOT_INO, OsStr::new("dir-link"))
                .await
                .is_err()
        );
    });
}

#[test]
fn concurrent_commits_snapshots_and_pagination() {
    let (manager, _dirs) = cluster();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let a = AutumnObjectStore::connect(&manager, "fs/objects/concurrent")
            .await
            .unwrap();
        // Independent worker + client connection, as with separate processes.
        let b = AutumnObjectStore::connect(&manager, "fs/objects/concurrent")
            .await
            .unwrap();
        let path = Path::from("table/_versions/1.manifest");
        let (left, right) = tokio::join!(
            a.put_opts(&path, "left".into(), PutMode::Create.into()),
            b.put_opts(&path, "right".into(), PutMode::Create.into())
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert!(matches!(
            left.as_ref().err().or(right.as_ref().err()),
            Some(Error::AlreadyExists { .. })
        ));
        let old = a.head(&path).await.unwrap();
        // Force the comparison to read an SST instead of only the memtable.
        let manager_for_flush = manager.clone();
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async {
                let client = autumn_client::ClusterClient::connect(
                    &manager_for_flush,
                    "fs/objects/concurrent",
                )
                .await
                .unwrap();
                client.flush(901).await.unwrap();
            })
        })
        .join()
        .unwrap();
        let update = PutMode::Update(UpdateVersion {
            e_tag: old.e_tag,
            version: None,
        });
        let (left, right) = tokio::join!(
            a.put_opts(&path, "next-left".into(), update.clone().into()),
            b.put_opts(&path, "next-right".into(), update.into())
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert!(matches!(
            left.as_ref().err().or(right.as_ref().err()),
            Some(Error::Precondition { .. })
        ));

        let large = Path::from("table/data/large.lance");
        let bytes = Bytes::from(
            (0..9 * 1024 * 1024)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>(),
        );
        a.put(&large, bytes.clone().into()).await.unwrap();
        let snapshot = a.get(&large).await.unwrap();
        let ranges = [0..0, 4_194_300..4_194_315, 8_000_000..9_000_000];
        for (value, range) in a
            .get_ranges(&large, &ranges)
            .await
            .unwrap()
            .iter()
            .zip(&ranges)
        {
            assert_eq!(
                value,
                &bytes.slice(range.start as usize..range.end as usize)
            );
        }
        b.put(&large, "replacement".into()).await.unwrap();
        b.delete(&large).await.unwrap();
        assert_eq!(snapshot.bytes().await.unwrap(), bytes);
        assert!(matches!(a.get(&large).await, Err(Error::NotFound { .. })));

        let start = std::time::Instant::now();
        futures::stream::iter(0..1100)
            .map(|i| {
                let a = a.clone();
                async move {
                    a.put(
                        &Path::from(format!("fragments/{i:04}.lance")),
                        "data".into(),
                    )
                    .await
                }
            })
            .buffer_unordered(16)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        eprintln!("1100 fragment puts: {:?}", start.elapsed());
        let mut timings = Vec::new();
        for _ in 0..20 {
            let start = std::time::Instant::now();
            let entries: Vec<_> = a
                .list(Some(&Path::from("fragments")))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(entries.len(), 1100);
            assert_eq!(
                entries
                    .iter()
                    .map(|e| &e.location)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len(),
                1100
            );
            timings.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        timings.sort_by(f64::total_cmp);
        eprintln!(
            "1100-fragment list latency ms: p50={:.3} p99={:.3}",
            timings[10], timings[19]
        );
        let freed = a.vacuum_quiescent().await.unwrap();
        assert!(freed >= 3);
        assert_eq!(
            a.get(&Path::from("fragments/1099.lance"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "data"
        );
    });
}
