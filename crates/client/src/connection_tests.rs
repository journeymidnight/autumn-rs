use super::*;
#[path = "../../rpc/tests/support/status_peer.rs"]
mod peer;
use autumn_rpc::version_hello::Service;
use peer::*;

fn client(manager: String) -> ClusterClient {
    ClusterClient {
        role: autumn_rpc::version_hello::Role::Client,
        manager_addrs: vec![manager],
        current_mgr: Cell::new(0),
        mgr_conn: Rc::new(RefCell::new(None)),
        ps_conns: RefCell::new(HashMap::new()),
        en_pool: autumn_stream::ConnPool::with_role(autumn_rpc::version_hello::Role::Client),
        regions: RefCell::new(vec![]),
        ps_details: RefCell::new(HashMap::new()),
        part_addrs: RefCell::new(HashMap::new()),
        rpc_timeout: Cell::new(Some(Duration::from_secs(2))),
        first_attempt_timeout: Cell::new(None),
        auth: RefCell::new(None),
        auth_gen: Cell::new(0),
        wire_refused: RefCell::new(None),
        negotiated_cluster_wire: Cell::new(0),
        binding: NamespaceBinding::Raw,
    }
}

#[compio::test]
async fn wrong_shard_bulk_refusal_falls_back_to_proxy() {
    let en_calls = Rc::new(Cell::new(0));
    let count = en_calls.clone();
    let en = Peer::start_as(Service::ExtentNode, move |frame| {
        assert_eq!(
            frame.msg_type,
            autumn_stream::extent_rpc::MSG_READ_BYTES_BULK
        );
        count.set(count.get() + 1);
        Reply::Frame(autumn_rpc::Frame::error(
            frame.req_id,
            frame.msg_type,
            autumn_rpc::RpcError::encode_status(
                StatusCode::FailedPrecondition,
                "extent belongs to shard 1",
            ),
        ))
    })
    .await;
    let en_addr = en.addr.clone();
    let proxy_calls = Rc::new(Cell::new(0));
    let count = proxy_calls.clone();
    let ps = Peer::start_as(Service::PartitionServer, move |frame| {
        if frame.msg_type == MSG_GET_REDIRECT {
            Reply::Frame(autumn_rpc::Frame::response(
                frame.req_id,
                frame.msg_type,
                rkyv_encode(&GetRedirectResp {
                    code: 0,
                    message: String::new(),
                    value: vec![],
                    extent_id: 42,
                    value_offset: 0,
                    value_len: 65536,
                    eversion: 1,
                    replica_addrs: vec![en_addr.clone()],
                    ec_data_shards: 0,
                    ec_sealed_length: 0,
                }),
            ))
        } else {
            assert_eq!(frame.msg_type, MSG_GET_BULK);
            count.set(count.get() + 1);
            Reply::Frame(autumn_rpc::Frame::response_zc(
                frame.req_id,
                frame.msg_type,
                Bytes::from_static(&[0]),
                Bytes::from(vec![91; 65536]),
            ))
        }
    })
    .await;
    let client = client("127.0.0.1:1".into());
    client.regions.borrow_mut().push((1, region(1)));
    client.part_addrs.borrow_mut().insert(1, ps.addr.clone());
    let mut dest = vec![0; 65536];
    for _ in 0..2 {
        assert_eq!(
            client
                .get_range_direct_into(b"key", 0, 65536, &mut dest)
                .await
                .unwrap(),
            Some(65536)
        );
        assert!(dest.iter().all(|v| *v == 91));
    }
    assert_eq!(en_calls.get(), 2);
    assert_eq!(proxy_calls.get(), 2);
    assert_eq!(
        en.accepts.get(),
        1,
        "routing refusal must retain the healthy EN connection"
    );
}

#[compio::test]
async fn client_status_errors_preserve_connections_and_transport_failures_evict() {
    for shape in 0..4 {
        let peer = Peer::start_as(
            if shape == 3 {
                Service::Manager
            } else {
                Service::PartitionServer
            },
            respond,
        )
        .await;
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

/// The routing record a client actually holds. It used to be a
/// `MgrRegionInfo`, which forced this fixture to invent
/// `log_stream: 1, row_stream: 2, meta_stream: 3` — three numbers no client
/// code has ever read, present only because the wire form carried them. There
/// is nothing to invent now.
fn region(epoch: u64) -> ClientRegion {
    ClientRegion {
        rg: Some(MgrRange {
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
        }),
        part_id: 1,
        ps_id: 1,
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
            let peer = Peer::start_as(Service::PartitionServer, move |f| {
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
                assert_eq!(f.msg_type, MSG_GET_CLIENT_REGIONS);
                count.set(count.get() + 1);
                Reply::Frame(autumn_rpc::Frame::response(
                    f.req_id,
                    f.msg_type,
                    rkyv_encode(&ClientRegionsResp {
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
            client.mgr_client().await.unwrap();
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
    let peer = Peer::start_as(Service::PartitionServer, respond).await;
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
    // Exactly one hello, on the one connection that was opened — not one per
    // call. `say_hello` lives in `get_ps_client`, not in the call path, and
    // this is what pins that: a per-request handshake would be a round trip on
    // every data-plane op.
    assert_eq!(
        peer.hellos.get(),
        1,
        "the SDK sends VERSION_HELLO once per connection it OPENS"
    );

    // A rebuilt connection handshakes again. It has to: the server's admission
    // is per-connection, so a reconnect that skipped the hello would be judged
    // as a silent peer rather than as this client. Evicted by hand here — the
    // identity-change eviction below arms authentication, and this fixture has
    // no manager to mint a token from.
    client.ps_conns.borrow_mut().clear();
    client
        .ps_call(&peer.addr, ECHO, Bytes::new())
        .await
        .unwrap();
    assert_eq!(peer.accepts.get(), 2);
    assert_eq!(peer.hellos.get(), 2, "a reopened connection re-handshakes");

    client.set_principal_credential("new-identity", vec![]);
    assert!(
        client.ps_conns.borrow().is_empty(),
        "an identity change must still force reauthentication"
    );
}

/// A server that refuses this client's wire version must reach the CALLER with
/// the server's own words, and must not be retried.
///
/// The refusal names which way round the mismatch is — rebuild this image, or
/// deploy the cluster — and that is the only information it carries. Both
/// halves of this were wrong when the handshake first landed: `connect`
/// discarded the error and reported "cannot connect to any manager", and the
/// data path treated it as a transient connection failure and spent the full
/// `MAX_PS_REFRESHES` budget (~13 s per operation) before handing it back
/// mislabelled.
#[compio::test]
async fn a_wire_version_refusal_is_terminal_and_keeps_the_servers_words() {
    // (1) At connect. This is the entry point every wheel, fuse mount, s3
    //     gateway and autumn-op comes through, so it is where an operator
    //     actually reads the message.
    let mgr = Peer::start_refusing_hello(respond).await;
    let Err(err) = ClusterClient::connect_raw(&mgr.addr).await else {
        panic!("a refused client must not connect");
    };
    let text = format!("{err:#}");
    assert!(
        text.contains(HELLO_REFUSAL),
        "the server's own refusal must survive to the caller, got: {text}"
    );
    assert_eq!(
        mgr.accepts.get(),
        1,
        "a refusal is the same from every manager in a cluster — do not walk the list"
    );

    // (2) On the data path. A wire refusal is as terminal as PermissionDenied:
    //     the version is compiled into this binary, so no refresh can change
    //     the answer.
    let ps = Peer::start_refusing_as(Service::PartitionServer, usize::MAX, respond).await;
    let client = client("127.0.0.1:1".into());
    client.regions.borrow_mut().push((1, region(1)));
    client.part_addrs.borrow_mut().insert(1, ps.addr.clone());
    let err = client
        .get(b"k")
        .await
        .expect_err("a refused client must not read");
    assert!(
        matches!(err, AutumnError::WireVersionRefused(_)),
        "must be typed, not flattened into a connection error: {err}"
    );
    assert!(format!("{err}").contains(HELLO_REFUSAL), "{err}");
    // One attempt, not eleven. Without the short-circuit each refresh reopens
    // the connection, so the accept count is what exposes the retry storm —
    // and it does so without asserting on wall-clock time.
    assert_eq!(ps.accepts.get(), 1, "a refused client must not be retried");
}

/// A refusal must NOT latch. A client refused for running AHEAD of its cluster
/// — the routine case, since images are built from `main` — has to start
/// working by itself the moment the cluster is deployed. A fuse daemon or an
/// inference pod is not something anyone restarts to clear a flag.
#[compio::test]
async fn a_refusal_clears_when_the_cluster_catches_up() {
    let ps = Peer::start_refusing_as(Service::PartitionServer, 1, respond).await;
    let client = client("127.0.0.1:1".into());
    client.regions.borrow_mut().push((1, region(1)));
    client.part_addrs.borrow_mut().insert(1, ps.addr.clone());

    let err = client.get(b"k").await.expect_err("the first op is refused");
    assert!(matches!(err, AutumnError::WireVersionRefused(_)), "{err}");

    // Second call: a fresh connection, a fresh hello, now admitted. It must
    // reach the peer — the flag is cleared by a successful handshake, not
    // carried for the client's life.
    client
        .ps_call(&ps.addr, ECHO, Bytes::new())
        .await
        .expect("the client recovers once the cluster admits it");
    assert_eq!(ps.hellos.get(), 2, "the second connection re-handshakes");
    assert!(
        client.wire_refused.borrow().is_none(),
        "a successful handshake must clear the refusal"
    );
}

/// Each data connection negotiates independently of a successful manager
/// connection. A refused PS handshake cannot reach the Put handler.
#[compio::test]
async fn a_partition_refusal_is_checked_independently_of_the_manager() {
    let ps = Peer::start_refusing_as(Service::PartitionServer, usize::MAX, respond).await;
    let ps_addr = ps.addr.clone();
    let mgr = Peer::start(move |f| {
        assert_eq!(f.msg_type, MSG_GET_CLIENT_REGIONS);
        Reply::Frame(autumn_rpc::Frame::response(
            f.req_id,
            f.msg_type,
            rkyv_encode(&ClientRegionsResp {
                code: 0,
                message: String::new(),
                regions: vec![(1, region(1))],
                ps_details: vec![],
                part_addrs: vec![(1, ps_addr.clone())],
            }),
        ))
    })
    .await;
    let client = ClusterClient::connect_raw(&mgr.addr).await.unwrap();
    let err = client.put(b"key", b"value").await.unwrap_err();
    assert!(matches!(err, AutumnError::WireVersionRefused(_)), "{err}");
    assert!(err.to_string().contains(HELLO_REFUSAL));
    assert_eq!(ps.accepts.get(), 1);
}

/// The two-form rule, exercised from the client's side: WHICH opcode a refresh
/// sends is decided by the version the hello negotiated, and both answers are
/// correct behaviour rather than one being a degraded mode.
///
/// This is a PAIR on purpose. One fixture, one difference — the version the
/// peer reports — and opposite outcomes. Asserting only the new opcode would
/// pass just as well if the client had stopped consulting the negotiated
/// version at all and simply always sent the new one, which is precisely the
/// bug that would break every cluster older than this binary.
///
/// The fallback direction is the one that matters in the field: a wheel built
/// from `main` routinely runs ahead of a cluster nobody has upgraded yet.
#[compio::test]
async fn which_routing_opcode_is_sent_follows_the_negotiated_version() {
    use std::cell::RefCell;

    async fn opcode_seen_by_a_cluster_reporting(wire: u32) -> u8 {
        let seen: Rc<RefCell<Vec<u8>>> = Rc::new(RefCell::new(Vec::new()));
        let record = seen.clone();
        let peer = Peer::start(move |f| {
            record.borrow_mut().push(f.msg_type);
            // `connect`'s cluster-id self-check is not what this test is about;
            // closing on it is what the sibling mocks do, and the hello — which
            // is what sets the negotiated version — happens regardless.
            if f.msg_type == MSG_GET_CLUSTER_ID {
                return Reply::Close;
            }
            // Answer in whichever form was asked for; a client that sent the
            // other one would fail to decode, which is itself the assertion.
            if f.msg_type == MSG_GET_CLIENT_REGIONS {
                Reply::Frame(autumn_rpc::Frame::response(
                    f.req_id,
                    f.msg_type,
                    rkyv_encode(&ClientRegionsResp {
                        code: 0,
                        message: String::new(),
                        regions: vec![(1, region(1))],
                        ps_details: vec![],
                        part_addrs: vec![],
                    }),
                ))
            } else {
                Reply::Frame(autumn_rpc::Frame::response(
                    f.req_id,
                    f.msg_type,
                    rkyv_encode(&GetRegionsResp {
                        code: 0,
                        message: String::new(),
                        regions: vec![(
                            1,
                            MgrRegionInfo {
                                rg: Some(MgrRange {
                                    start_key: b"a".to_vec(),
                                    end_key: b"z".to_vec(),
                                }),
                                part_id: 1,
                                ps_id: 1,
                                // The three the narrow form drops. A cluster
                                // this old still sends them; the client throws
                                // them away on arrival.
                                log_stream: 1,
                                row_stream: 2,
                                meta_stream: 3,
                                region_epoch: 1,
                            },
                        )],
                        ps_details: vec![],
                        part_addrs: vec![],
                    }),
                ))
            }
        })
        .await;
        let client = ClusterClient::connect(&peer.addr, "fs")
            .await
            .expect("connect");
        seen.borrow_mut().clear();
        client.negotiated_cluster_wire.set(wire);
        client.refresh_regions().await.expect("refresh");
        let routing = seen
            .borrow()
            .iter()
            .copied()
            .find(|m| *m == MSG_GET_REGIONS || *m == MSG_GET_CLIENT_REGIONS)
            .expect("a routing request was sent");
        routing
    }

    assert_eq!(
        opcode_seen_by_a_cluster_reporting(autumn_rpc::WIRE_VERSION_WITH_CLIENT_REGIONS).await,
        MSG_GET_CLIENT_REGIONS,
        "a cluster that serves the narrow form must be asked with the new opcode"
    );
    assert_eq!(
        opcode_seen_by_a_cluster_reporting(autumn_rpc::WIRE_VERSION_WITH_CLIENT_REGIONS - 1).await,
        MSG_GET_REGIONS,
        "a cluster one version below it has no handler for the new opcode and \
         would refuse the frame outright"
    );
}

#[compio::test]
async fn a_partition_hello_does_not_change_the_managers_routing_version() {
    // A client can see a newer PS while it still uses an older manager during
    // a wire rollout. The manager's negotiated version selects routing DTOs.
    let ps = Peer::start_as(Service::PartitionServer, respond).await;
    let client = client("127.0.0.1:1".into());
    client.negotiated_cluster_wire.set(autumn_rpc::WIRE_VERSION_WITH_CLIENT_REGIONS - 1);
    client.ps_call(&ps.addr, ECHO, Bytes::new()).await.unwrap();
    assert_eq!(client.negotiated_cluster_wire.get(), autumn_rpc::WIRE_VERSION_WITH_CLIENT_REGIONS - 1);
}

/// An extent node refusing this client's credential is the answer, not a
/// failure to route around: the read returns `PermissionDenied`, with no other
/// replica tried and no proxy fallback, for the replicated and the EC
/// descriptor alike.
#[compio::test]
async fn en_credential_refusal_is_returned_without_fallback() {
    for ec_data_shards in [0u32, 2] {
        let en_calls = Rc::new(Cell::new(0));
        let mut ens = Vec::new();
        for _ in 0..2 {
            let count = en_calls.clone();
            ens.push(
                Peer::start_as(Service::ExtentNode, move |frame| {
                    count.set(count.get() + 1);
                    Reply::Frame(autumn_rpc::Frame::error(
                        frame.req_id,
                        frame.msg_type,
                        autumn_rpc::RpcError::encode_status(
                            StatusCode::PermissionDenied,
                            "direct read requires a capability token",
                        ),
                    ))
                })
                .await,
            );
        }
        let addrs: Vec<String> = ens.iter().map(|e| e.addr.clone()).collect();
        let proxy_calls = Rc::new(Cell::new(0));
        let count = proxy_calls.clone();
        let ps = Peer::start_as(Service::PartitionServer, move |frame| {
            if frame.msg_type != MSG_GET_REDIRECT {
                count.set(count.get() + 1);
            }
            Reply::Frame(autumn_rpc::Frame::response(
                frame.req_id,
                frame.msg_type,
                rkyv_encode(&GetRedirectResp {
                    code: 0,
                    message: String::new(),
                    value: vec![],
                    extent_id: 42,
                    value_offset: 0,
                    value_len: 65536,
                    eversion: 1,
                    replica_addrs: addrs.clone(),
                    ec_data_shards,
                    ec_sealed_length: 2 * 65536,
                }),
            ))
        })
        .await;
        let client = client("127.0.0.1:1".into());
        client.regions.borrow_mut().push((1, region(1)));
        client.part_addrs.borrow_mut().insert(1, ps.addr.clone());
        let mut dest = vec![0; 65536];
        let e = client
            .get_range_direct_into(b"key", 0, 65536, &mut dest)
            .await
            .expect_err("a refused credential was served");
        assert!(
            matches!(&e, AutumnError::PermissionDenied(m) if m.contains("capability token")),
            "ec_data_shards={ec_data_shards}: {e}"
        );
        assert_eq!(en_calls.get(), 1, "ec_data_shards={ec_data_shards}: another node was tried");
        assert_eq!(proxy_calls.get(), 0, "ec_data_shards={ec_data_shards}: fell back to the proxy");
    }
}
