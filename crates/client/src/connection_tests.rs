use super::*;
#[path = "../../rpc/tests/support/status_peer.rs"]
mod peer;
use peer::*;

fn client(manager: String) -> ClusterClient {
    ClusterClient {
        manager_addrs: vec![manager],
        current_mgr: Cell::new(0),
        mgr_conn: Rc::new(RefCell::new(None)),
        ps_conns: RefCell::new(HashMap::new()),
        en_pool: autumn_stream::ConnPool::new(),
        regions: RefCell::new(vec![]),
        ps_details: RefCell::new(HashMap::new()),
        part_addrs: RefCell::new(HashMap::new()),
        rpc_timeout: Cell::new(Some(Duration::from_secs(2))),
        admin_token: RefCell::new(None),
        first_attempt_timeout: Cell::new(None),
        auth: RefCell::new(None),
        auth_gen: Cell::new(0),
        binding: NamespaceBinding::Raw,
    }
}

#[compio::test]
async fn client_status_errors_preserve_connections_and_transport_failures_evict() {
    let peer = Peer::start(respond).await;
    for shape in 0..4 {
        let client = client(peer.addr.clone());
        let before = peer.accepts.get();
        for code in STATUSES {
            let payload = Bytes::from(vec![code as u8]);
            let result = match shape {
                0 => {
                    client
                        .ps_call_with_timeout(&peer.addr, STATUS, payload, None)
                        .await
                }
                1 => client.ps_call(&peer.addr, STATUS, payload).await,
                2 => {
                    client
                        .ps_call_bulk_multi(
                            &peer.addr,
                            STATUS,
                            payload,
                            vec![Bytes::from_static(b"value")],
                            Some(Duration::from_secs(2)),
                        )
                        .await
                }
                _ => client.mgr_call(STATUS, payload).await,
            };
            assert!(result.is_err());
            if shape < 3 {
                assert!(
                    client.ps_conns.borrow().contains_key(&peer.addr),
                    "{shape}: {code:?} evicted"
                );
                client
                    .ps_call(&peer.addr, ECHO, Bytes::new())
                    .await
                    .unwrap();
            } else {
                assert!(client.mgr_conn.borrow().is_some());
                client.mgr_call(ECHO, Bytes::new()).await.unwrap();
            }
        }
        assert_eq!(peer.accepts.get(), before + 1);
        client.set_rpc_timeout(Duration::from_millis(50));
        for failure in [CLOSE, BAD_CRC, HANG] {
            if shape == 0 && failure == HANG {
                continue;
            }
            let result = match shape {
                0 => {
                    client
                        .ps_call_with_timeout(&peer.addr, failure, Bytes::new(), None)
                        .await
                }
                1 => client.ps_call(&peer.addr, failure, Bytes::new()).await,
                2 => {
                    client
                        .ps_call_bulk_multi(
                            &peer.addr,
                            failure,
                            Bytes::new(),
                            vec![],
                            Some(Duration::from_millis(50)),
                        )
                        .await
                }
                _ => client.mgr_call(failure, Bytes::new()).await,
            };
            assert!(result.is_err());
            let accepts = peer.accepts.get();
            if shape < 3 {
                assert!(!client.ps_conns.borrow().contains_key(&peer.addr));
                client
                    .ps_call(&peer.addr, ECHO, Bytes::new())
                    .await
                    .unwrap();
            } else {
                assert!(client.mgr_conn.borrow().is_none());
                client.mgr_call(ECHO, Bytes::new()).await.unwrap();
            }
            assert_eq!(peer.accepts.get(), accepts + 1);
        }
    }
}

fn region(epoch: u64) -> MgrRegionInfo {
    MgrRegionInfo {
        rg: Some(MgrRange {
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
        }),
        part_id: 1,
        ps_id: 1,
        log_stream: 1,
        row_stream: 2,
        meta_stream: 3,
        region_epoch: epoch,
    }
}

#[compio::test]
async fn routing_retries_keep_the_connection_for_plain_bulk_and_pooled_calls() {
    for shape in 0..3 {
        for code in [
            StatusCode::FailedPrecondition,
            StatusCode::Unavailable,
            StatusCode::NotFound,
        ] {
            let calls = Rc::new(Cell::new(0));
            let observed = calls.clone();
            let peer = Peer::start(move |f| {
                observed.set(observed.get() + 1);
                if observed.get() == 1 {
                    return Reply::Frame(autumn_rpc::Frame::error(
                        f.req_id,
                        f.msg_type,
                        RpcError::encode_status(code, "stale route"),
                    ));
                }
                if shape == 0 {
                    let req: PutReq = rkyv_decode(&f.payload).unwrap();
                    assert_eq!(req.region_epoch, 2, "retry must use refreshed epoch");
                } else if shape == 2 {
                    let req: GetReq = rkyv_decode(&f.payload).unwrap();
                    assert_eq!(req.region_epoch, 2, "retry must use refreshed epoch");
                }
                Reply::Frame(if shape == 2 {
                    autumn_rpc::Frame::response_zc(
                        f.req_id,
                        f.msg_type,
                        Bytes::from_static(&[0]),
                        Bytes::from_static(b"value"),
                    )
                } else {
                    autumn_rpc::Frame::response(
                        f.req_id,
                        f.msg_type,
                        rkyv_encode(&PutResp {
                            code: 0,
                            message: String::new(),
                            key: b"key".to_vec(),
                        }),
                    )
                })
            })
            .await;
            let ps_addr = peer.addr.clone();
            let refreshes = Rc::new(Cell::new(0));
            let count = refreshes.clone();
            let manager = Peer::start(move |f| {
                assert_eq!(f.msg_type, MSG_GET_REGIONS);
                count.set(count.get() + 1);
                Reply::Frame(autumn_rpc::Frame::response(
                    f.req_id,
                    f.msg_type,
                    rkyv_encode(&GetRegionsResp {
                        code: 0,
                        message: String::new(),
                        regions: vec![(1, region(2))],
                        ps_details: vec![],
                        part_addrs: vec![(1, ps_addr.clone())],
                    }),
                ))
            })
            .await;
            let client = client(manager.addr.clone());
            client.regions.borrow_mut().push((1, region(1)));
            client.part_addrs.borrow_mut().insert(1, peer.addr.clone());
            match shape {
                0 => client.put(b"key", b"value").await.unwrap(),
                1 => client
                    .put_bulk(b"key", Bytes::from(vec![7; 65536]))
                    .await
                    .unwrap(),
                _ => assert_eq!(client.get(b"key").await.unwrap().unwrap(), b"value"),
            }
            assert_eq!(calls.get(), 2, "one refusal then a successful retry");
            assert_eq!(refreshes.get(), 1, "the refusal must still refresh routing");
            assert_eq!(
                peer.accepts.get(),
                1,
                "{shape}: {code:?} must retry on the existing connection"
            );
        }
    }
}

#[compio::test]
async fn pooled_batch_status_keeps_connection_and_identity_change_clears_it() {
    let peer = Peer::start(respond).await;
    let client = client("127.0.0.1:1".into());
    client.regions.borrow_mut().push((1, region(1)));
    client.part_addrs.borrow_mut().insert(1, peer.addr.clone());
    let error = client
        .call_ps_for_part_pooled(
            1,
            STATUS,
            Bytes::from(vec![StatusCode::FailedPrecondition as u8]),
        )
        .await
        .err()
        .unwrap();
    assert!(matches!(error, AutumnError::PreconditionFailed(_)));
    assert!(client.ps_conns.borrow().contains_key(&peer.addr));
    client
        .ps_call(&peer.addr, ECHO, Bytes::new())
        .await
        .unwrap();
    assert_eq!(peer.accepts.get(), 1);
    client.set_principal_credential("new-identity", vec![]);
    assert!(
        client.ps_conns.borrow().is_empty(),
        "an identity change must still force reauthentication"
    );
}
