//! `MSG_GET` / `MSG_BATCH_GET` answer "value or position" against a live
//! in-process cluster: asked with `direct`, a ValuePointer range of 64 KiB or
//! more comes back as its position, which an extent node serves (`MSG_READ`
//! from a client connection) byte for byte; a small value, a small range and a
//! missing key come back as themselves; without `direct` everything is the
//! value. The SDK's `get_direct` and `get_many_direct` return the same bytes as
//! `get`.

mod support;

use std::time::Duration;

use autumn_client::{ClusterClient, GetManyItem, WriteOptions};
use autumn_rpc::client::RpcClient;
use autumn_rpc::partition_rpc::{self as ps, BatchGetCtrl, BatchGetItem, BatchGetReq, GetReq, ValuePosition};
use bytes::Bytes;

use support::*;

const PART: u64 = 19101;

fn val(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

/// One raw `MSG_GET`: code, ctrl after the code, value.
async fn raw_get(ps_rpc: &RpcClient, key: &[u8], offset: u32, length: u32, direct: bool) -> (u8, Bytes, Vec<u8>) {
    let req = GetReq { part_id: PART, key: key.to_vec(), offset, length, region_epoch: 0, direct };
    let z = ps_rpc.call_into_pooled(ps::MSG_GET, ps::rkyv_encode(&req)).await.unwrap();
    (z.code, z.ctrl, z.buf.as_ref().to_vec())
}

#[test]
fn reads_answer_with_the_value_or_where_it_is() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (n1, n2) = (pick_addr(), pick_addr());
    start_extent_node(n1, dirs.0.path().to_path_buf(), 1);
    start_extent_node(n2, dirs.1.path().to_path_buf(), 2);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.unwrap();
        register_two_nodes(&mgr, n1, n2, 191).await;
        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, PART, log, row, meta, b"", b"\xff\xff\xff\xff").await;
        let ps_addr = pick_addr();
        let (ps_stop, ps_join) = start_partition_server_stoppable(191, mgr_addr, ps_addr);
        compio::time::sleep(Duration::from_millis(1500)).await;
        create_namespaces(mgr_addr, &["t"]).await;
        let c = ClusterClient::connect_raw(&mgr_addr.to_string()).await.expect("connect");
        c.set_rpc_timeout(Duration::from_secs(30));

        let big = val(1 << 20, 1);
        let small = val(1000, 2);
        c.put(b"t/big", Bytes::from(big.clone()), WriteOptions::default()).await.unwrap();
        c.put(b"t/small", Bytes::from(small.clone()), WriteOptions::default()).await.unwrap();

        let ps_rpc = RpcClient::connect(ps_addr).await.unwrap();
        // A large VP range asked for directly: its position, no value, and the
        // extent node it names serves exactly those bytes.
        let (code, ctrl, value) = raw_get(&ps_rpc, b"t/big", 4096, 200_000, true).await;
        assert_eq!(code, ps::CODE_POSITION);
        assert!(value.is_empty());
        let pos: ValuePosition = ps::rkyv_decode(&ctrl).unwrap();
        assert_eq!(pos.value_len, 200_000);
        assert_eq!(pos.ec_data_shards, 0);
        let pool = autumn_stream::ConnPool::with_role(autumn_rpc::version_hello::Role::Client);
        let read = autumn_stream::read_extent_value_direct(
            &pool,
            &pos.replica_addrs[0],
            pos.extent_id,
            pos.eversion,
            pos.value_offset,
            pos.value_len,
        )
        .await
        .unwrap();
        assert_eq!(read.as_ref(), &big[4096..204_096]);
        // Without `direct`, or for a range under 64 KiB, the value itself.
        let (code, _, value) = raw_get(&ps_rpc, b"t/big", 0, 0, false).await;
        assert_eq!((code, value == big), (ps::CODE_OK, true));
        let (code, _, value) = raw_get(&ps_rpc, b"t/big", 10, 1000, true).await;
        assert_eq!((code, value.as_slice()), (ps::CODE_OK, &big[10..1010]));
        let (code, _, value) = raw_get(&ps_rpc, b"t/small", 0, 0, true).await;
        assert_eq!((code, value == small), (ps::CODE_OK, true));
        assert_eq!(raw_get(&ps_rpc, b"t/none", 0, 0, true).await.0, ps::CODE_NOT_FOUND);

        // The batch: each item its value, its position, or not found, in order.
        let item = |k: &[u8], offset, length| BatchGetItem { key: k.to_vec(), offset, length };
        let ask = ps::rkyv_encode(&BatchGetReq {
            part_id: PART,
            region_epoch: 0,
            direct: true,
            items: vec![item(b"t/small", 0, 0), item(b"t/big", 0, 0), item(b"t/none", 0, 0), item(b"t/big", 7, 9)],
        });
        let z = ps_rpc.call_into_pooled(ps::MSG_BATCH_GET, ask).await.unwrap();
        assert_eq!(z.code, ps::CODE_OK);
        let ctrl: BatchGetCtrl = ps::rkyv_decode(&z.ctrl).unwrap();
        assert_eq!(
            ctrl.statuses,
            vec![ps::BATCH_GET_FOUND, ps::BATCH_GET_POSITION, ps::BATCH_GET_NOT_FOUND, ps::BATCH_GET_FOUND]
        );
        assert_eq!(ctrl.value_lens, vec![1000, 0, 0, 9]);
        assert_eq!(ctrl.positions.len(), 1);
        assert_eq!(ctrl.positions[0].value_len, 1 << 20);
        assert_eq!(z.buf.as_ref(), [&small[..], &big[7..16]].concat().as_slice());

        // The SDK: the same bytes as a read through the PS.
        assert_eq!(c.get_direct(b"t/big").await.unwrap().unwrap().as_ref(), &big[..]);
        assert_eq!(c.get_direct(b"t/small").await.unwrap().unwrap().as_ref(), &small[..]);
        assert!(c.get_direct(b"t/none").await.unwrap().is_none());
        let (mut d0, mut d1, mut d2, mut d3) = (vec![0u8; 1 << 20], vec![0u8; 1000], vec![0u8; 8], vec![0u8; 100_000]);
        let mut items = [
            GetManyItem { key: b"t/big", offset: 0, length: 0, dest: &mut d0 },
            GetManyItem { key: b"t/small", offset: 0, length: 0, dest: &mut d1 },
            GetManyItem { key: b"t/none", offset: 0, length: 0, dest: &mut d2 },
            GetManyItem { key: b"t/big", offset: 300, length: 100_000, dest: &mut d3 },
        ];
        let got: Vec<_> = c.get_many_direct(&mut items).await.into_iter().map(Result::unwrap).collect();
        assert_eq!(got, vec![Some(1 << 20), Some(1000), None, Some(100_000)]);
        assert_eq!(d0, big);
        assert_eq!(d1, small);
        assert_eq!(&d3[..], &big[300..100_300]);

        ps_stop.shutdown();
        ps_join.join().expect("join PS");
    });
}
