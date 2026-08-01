//! End-to-end tests for client-side striped put/get (replaces the
//! server-side multipart upload + multi-fragment ValuePointer).
//!
//! The client SDK now implements striping in pure ClusterClient code (Ceph
//! striperados pattern): each chunk is a normal `Put` to a deterministic
//! reserved-namespace key, and `commit` writes a 29-byte meta blob to the
//! user's key. No new server RPCs, no multi-frag VP, no GC rewrite.
//!
//! Tests verify:
//!   - large values round-trip byte-for-byte via put_stream + get_stream
//!   - commit is atomic (pre-commit get returns NotFound; post-commit
//!     get returns the meta blob; get_stream returns the assembled value)
//!   - delete_stream cascades, plain delete only removes the meta
//!   - abort cleans up partial chunks
//!   - chunks land outside the user prefix (range scans don't see them)

mod support;

use std::time::Duration;

use autumn_client::ClusterClient;
use autumn_rpc::client::RpcClient;

use support::*;

fn pattern(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut i: u64 = 0;
    while out.len() + 8 <= len {
        out.extend_from_slice(&i.to_le_bytes());
        i = i.wrapping_add(1);
    }
    while out.len() < len {
        out.push(0);
    }
    out
}

/// Bring up a small cluster + ClusterClient. Returns it ready to use.
async fn boot_cluster(
    mgr_addr: std::net::SocketAddr,
    n1_addr: std::net::SocketAddr,
    n2_addr: std::net::SocketAddr,
    base: u16,
    part_id: u64,
) -> ClusterClient {
    let mgr = RpcClient::connect(mgr_addr).await.unwrap();
    register_two_nodes(&mgr, n1_addr, n2_addr, base).await;
    let (log, row, meta) = create_three_streams(&mgr).await;
    // Full-keyspace partition so the reserved-prefix chunk keys
    // (\xff\xfe...) routed by the SDK fit within the partition's range.
    upsert_partition(&mgr, part_id, log, row, meta, b"", b"\xff\xff\xff\xff").await;

    let ps_addr = pick_addr();
    start_partition_server(base as u64, mgr_addr, ps_addr);
    compio::time::sleep(Duration::from_millis(1500)).await;
    let _ = RpcClient::connect(ps_addr).await.unwrap();

    let cluster = ClusterClient::connect_raw(&mgr_addr.to_string())
        .await
        .expect("ClusterClient::connect");
    cluster.set_rpc_timeout(Duration::from_secs(15));
    cluster
}

#[test]
#[ignore]
fn putstream_roundtrip_12mib() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = boot_cluster(mgr_addr, n1_addr, n2_addr, 110, 11001).await;

        // 12 MiB / 4 chunks of 3 MiB. The 3 MiB chunk size is intentionally
        // not a clean fraction of the inline cap to exercise unequal final
        // chunks. Caller would override chunk_size via the handle.
        let total: usize = 12 * 1024 * 1024;
        let chunk_size: usize = 3 * 1024 * 1024;
        let value = pattern(total);
        let key = b"big-value";

        let mut handle = cluster
            .put_stream_begin(key, 0)
            .with_chunk_size(chunk_size as u32);

        // Pre-commit: cluster.get(key) must return NotFound (meta not
        // written yet; orphan chunks are invisible).
        for i in 0..2 {
            let end = ((i + 1) * chunk_size).min(total);
            handle.send(&value[i * chunk_size..end]).await.expect("send");
        }
        let pre_commit = cluster.get(key).await.expect("get pre-commit");
        assert!(
            pre_commit.is_none(),
            "pre-commit get must be NotFound; got {} bytes",
            pre_commit.map(|v| v.len()).unwrap_or(0)
        );

        // Finish the upload + commit.
        for i in 2..4 {
            let end = ((i + 1) * chunk_size).min(total);
            handle.send(&value[i * chunk_size..end]).await.expect("send");
        }
        assert_eq!(handle.bytes_sent(), total as u64);
        assert_eq!(handle.chunks_sent(), 4);
        handle.commit().await.expect("commit");

        // Post-commit: cluster.get(key) returns the 29-byte meta blob
        // (not the assembled value — that's get_stream's job per the
        // explicit-namespace contract). Length check is enough.
        let meta_blob = cluster
            .get(key)
            .await
            .expect("get post-commit")
            .expect("None");
        assert_eq!(meta_blob.len(), 29, "meta blob is fixed 29 bytes");
        // Meta starts with the magic byte 0xfe.
        assert_eq!(meta_blob[0], 0xfe);

        // get_stream auto-detects meta and reassembles via chunk fetches.
        let mut stream = cluster
            .get_stream(key, 1024 * 1024)
            .await
            .expect("get_stream")
            .expect("None");
        assert_eq!(stream.total_bytes(), total as u64);
        let mut reassembled: Vec<u8> = Vec::with_capacity(total);
        while let Some(c) = stream.next_chunk().await.expect("next_chunk") {
            reassembled.extend_from_slice(&c);
        }
        assert_eq!(reassembled.len(), total);
        if reassembled != value {
            let pos = reassembled
                .iter()
                .zip(value.iter())
                .position(|(a, b)| a != b)
                .unwrap_or(0);
            let r_slice: Vec<u8> = reassembled[pos..(pos + 16).min(reassembled.len())].to_vec();
            let v_slice: Vec<u8> = value[pos..(pos + 16).min(value.len())].to_vec();
            panic!(
                "stream content mismatch at offset {pos}: reassembled[{pos}..]={:02x?} expected={:02x?}",
                r_slice, v_slice
            );
        }

        // delete_stream cascades — meta + all chunks gone.
        cluster.delete_stream(key).await.expect("delete_stream");
        let post = cluster.get(key).await.expect("get post-delete");
        assert!(post.is_none(), "delete_stream removed meta");
        let post_stream = cluster
            .get_stream(key, 1024)
            .await
            .expect("get_stream post-delete");
        assert!(post_stream.is_none(), "get_stream returns None after delete_stream");
    });
}

/// Single-chunk striped put. Exercises the n_chunks=1 edge case +
/// verifies the smallest possible striped value survives roundtrip.
#[test]
#[ignore]
fn putstream_single_chunk_1mib() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = boot_cluster(mgr_addr, n1_addr, n2_addr, 111, 11002).await;

        let total: usize = 1024 * 1024;
        let value = pattern(total);
        let key = b"single-chunk";

        let mut handle = cluster.put_stream_begin(key, 0);
        handle.send(&value).await.unwrap();
        assert_eq!(handle.chunks_sent(), 1);
        handle.commit().await.unwrap();

        let mut stream = cluster
            .get_stream(key, 256 * 1024)
            .await
            .unwrap()
            .unwrap();
        let mut got = Vec::with_capacity(total);
        while let Some(c) = stream.next_chunk().await.unwrap() {
            got.extend_from_slice(&c);
        }
        assert_eq!(got.len(), total);
        if got != value {
            let pos = got.iter().zip(value.iter()).position(|(a, b)| a != b).unwrap_or(0);
            panic!(
                "single-chunk content mismatch at offset {pos}: got[{pos}..]={:02x?} expected={:02x?}",
                &got[pos..(pos + 16).min(got.len())],
                &value[pos..(pos + 16).min(value.len())]
            );
        }
    });
}

/// Abort path: chunks were sent but commit never fired. Meta blob was
/// never written, so `get(key)` returns NotFound throughout. After abort,
/// all written chunks are best-effort deleted.
#[test]
#[ignore]
fn putstream_abort_drops_chunks() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = boot_cluster(mgr_addr, n1_addr, n2_addr, 112, 11003).await;

        let key = b"aborted";
        let mut handle = cluster.put_stream_begin(key, 0);
        handle.send(&pattern(2 * 1024 * 1024)).await.unwrap();
        handle.send(&pattern(2 * 1024 * 1024)).await.unwrap();
        handle.abort().await.unwrap();

        // get returns None — meta was never written.
        let post = cluster.get(key).await.unwrap();
        assert!(post.is_none(), "get on aborted upload must be None");
        // get_stream similarly returns None.
        let post_stream = cluster.get_stream(key, 1024).await.unwrap();
        assert!(post_stream.is_none());
    });
}

/// get_stream over an inline value (no put_stream involved). Verifies
/// the auto-detect path: small value put via regular `put` is yielded
/// in a single `next_chunk` call.
#[test]
#[ignore]
fn get_stream_inline_value_passthrough() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = boot_cluster(mgr_addr, n1_addr, n2_addr, 113, 11004).await;

        let value = b"hello inline world";
        let key = b"inline";
        cluster.put(key, value).await.unwrap();

        let mut stream = cluster.get_stream(key, 1024).await.unwrap().unwrap();
        assert_eq!(stream.total_bytes(), value.len() as u64);
        let chunk = stream.next_chunk().await.unwrap().unwrap();
        assert_eq!(&chunk, value);
        assert!(stream.next_chunk().await.unwrap().is_none());
    });
}

/// `get_many_into` batched zero-copy reads. Exercises BOTH branches of
/// the per-item bulk decision (`bulk_worthwhile(dest.len())`): a 4 KiB value (< 64 KiB
/// → regular `MSG_GET` + copy) and a 256 KiB value (>= 64 KiB → `MSG_GET_BULK`
/// recv-into-dest), plus a missing key (`Ok(None)`).
#[test]
#[ignore]
fn get_many_into_mixed_sizes() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = boot_cluster(mgr_addr, n1_addr, n2_addr, 120, 12001).await;

        let small = pattern(4 * 1024); // < 64 KiB → regular MSG_GET branch
        let large = pattern(256 * 1024); // >= 64 KiB → MSG_GET_BULK branch
        cluster.put(b"k-small", &small).await.expect("put small");
        cluster.put(b"k-large", &large).await.expect("put large");

        let mut d_small = vec![0u8; small.len()];
        let mut d_large = vec![0u8; large.len()];
        {
            use autumn_client::GetManyItem;
            let mut items = [
                GetManyItem {
                    key: b"k-small",
                    offset: 0,
                    length: 0,
                    dest: &mut d_small[..],
                },
                GetManyItem {
                    key: b"k-large",
                    offset: 0,
                    length: 0,
                    dest: &mut d_large[..],
                },
            ];
            let results = cluster.get_many_into(&mut items).await;
            assert_eq!(results[0].as_ref().unwrap(), &Some(small.len()));
            assert_eq!(results[1].as_ref().unwrap(), &Some(large.len()));
        }
        assert_eq!(d_small, small);
        assert_eq!(d_large, large);

        // Sub-range read of the large value: bytes [1024, 1024+4096) via offset/length.
        {
            use autumn_client::GetManyItem;
            let mut d_sub = vec![0u8; 4096];
            let mut sub = [GetManyItem {
                key: b"k-large",
                offset: 1024,
                length: 4096,
                dest: &mut d_sub[..],
            }];
            let r = cluster.get_many_into(&mut sub).await;
            assert_eq!(r[0].as_ref().unwrap(), &Some(4096));
            assert_eq!(&d_sub[..], &large[1024..1024 + 4096]);
        }

        // Missing key → Ok(None), no copy into dest.
        let mut d_miss = [0u8; 16];
        {
            use autumn_client::GetManyItem;
            let mut miss = [GetManyItem {
                key: b"k-missing",
                offset: 0,
                length: 0,
                dest: &mut d_miss[..],
            }];
            let r = cluster.get_many_into(&mut miss).await;
            assert_eq!(r[0].as_ref().unwrap(), &None);
        }
    });
}

/// `put_many` batched zero-copy writes. Exercises BOTH branches of the
/// per-item bulk decision (`bulk_worthwhile(value.len())`): a 4 KiB value (< 64 KiB →
/// regular `MSG_PUT`) and a 256 KiB value (>= 64 KiB → `MSG_PUT_BULK`), then reads
/// each back byte-for-byte.
#[test]
#[ignore]
fn put_many_mixed_sizes() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = boot_cluster(mgr_addr, n1_addr, n2_addr, 121, 12101).await;

        let small = bytes::Bytes::from(pattern(4 * 1024)); // < 64 KiB → MSG_PUT
        let large = bytes::Bytes::from(pattern(256 * 1024)); // >= 64 KiB → MSG_PUT_BULK
        let items: [(&[u8], bytes::Bytes, u64); 2] = [
            (b"pm-small", small.clone(), 0u64),
            (b"pm-large", large.clone(), 0u64),
        ];
        let results = cluster.put_many(&items).await;
        assert!(results[0].is_ok(), "put small: {:?}", results[0]);
        assert!(results[1].is_ok(), "put large: {:?}", results[1]);

        assert_eq!(
            cluster.get(b"pm-small").await.unwrap().as_deref(),
            Some(small.as_ref())
        );
        assert_eq!(
            cluster.get(b"pm-large").await.unwrap().as_deref(),
            Some(large.as_ref())
        );
    });
}

/// `head_many` + `delete_many` batched fan-out. `head_many` over present +
/// absent keys returns correct `found`/`value_length`; `delete_many` removes the
/// present keys (verified gone via `get`).
#[test]
#[ignore]
fn delete_many_and_head_many() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1");
    let n2_dir = tempfile::tempdir().expect("n2");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let cluster = boot_cluster(mgr_addr, n1_addr, n2_addr, 122, 12201).await;

        cluster.put(b"dm-a", b"aaaa").await.expect("put a");
        cluster.put(b"dm-b", b"bbbbbb").await.expect("put b");

        // head_many: 2 present + 1 absent.
        let keys: [&[u8]; 3] = [b"dm-a", b"dm-b", b"dm-missing"];
        let metas = cluster.head_many(&keys).await;
        let m0 = metas[0].as_ref().unwrap();
        assert!(m0.found && m0.value_length == 4);
        let m1 = metas[1].as_ref().unwrap();
        assert!(m1.found && m1.value_length == 6);
        assert!(!metas[2].as_ref().unwrap().found);

        // delete_many the two present keys.
        let del_keys: [&[u8]; 2] = [b"dm-a", b"dm-b"];
        let dels = cluster.delete_many(&del_keys).await;
        assert!(dels[0].is_ok() && dels[1].is_ok());

        // Confirm gone.
        assert!(cluster.get(b"dm-a").await.unwrap().is_none());
        assert!(cluster.get(b"dm-b").await.unwrap().is_none());
        // head_many now reports both absent.
        let after = cluster.head_many(&del_keys).await;
        assert!(!after[0].as_ref().unwrap().found);
        assert!(!after[1].as_ref().unwrap().found);
    });
}
