//! The MANAGER decides admission, over a real socket, through the real decode
//! loop.
//!
//! The predicates have unit tests in `autumn-rpc`. What those cannot show is
//! that the connection loop reaches them, that the per-connection value
//! survives from the hello to the next frame, and that the msg_type scoping
//! really does leave internal peer traffic alone — the three things that are
//! the difference between this mechanism working and it being dead code with
//! green tests beside it.
//!
//! The window is SHUT today (`MIN_CLIENT_WIRE_VERSION == WIRE_VERSION`), so the
//! only client that can be out of window is one reporting a different version.
//! That is exactly the case the acceptance asks for — a connection whose
//! reported version is outside the window must be refused BY THE SERVER, not by
//! the client's own courtesy check.

mod support;

use autumn_rpc::client::RpcClient;
use autumn_rpc::client_hello::{
    admit_client, encode_hello_req, parse_hello_resp, MSG_CLIENT_HELLO,
};
use autumn_rpc::manager_rpc::{
    rkyv_encode, ClusterDfReq, GetClusterIdReq, MSG_CLUSTER_DF, MSG_GET_CLUSTER_ID,
    MSG_GET_REGIONS,
};
use autumn_rpc::{RpcError, StatusCode, WIRE_VERSION};
use bytes::Bytes;
use support::{pick_stable_port_pair, start_manager};

fn hello(version: u32) -> Bytes {
    Bytes::copy_from_slice(&encode_hello_req(version))
}

/// The status of a refusal, or `None` when the call succeeded.
fn status(r: &Result<Bytes, RpcError>) -> Option<(StatusCode, String)> {
    match r {
        Ok(_) => None,
        Err(RpcError::Status { code, message }) => Some((*code, message.clone())),
        Err(e) => panic!("expected a status frame, got {e}"),
    }
}

#[test]
fn the_manager_admits_clients_by_their_reported_wire_version() {
    let port = pick_stable_port_pair();
    let mgr_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        // (1) A client AT this version is admitted, and learns what the
        //     cluster speaks.
        let c = RpcClient::connect(mgr_addr).await.expect("connect");
        let resp = c
            .call(MSG_CLIENT_HELLO, hello(WIRE_VERSION))
            .await
            .expect("an in-window hello must be admitted");
        let (server_wire, min_client) =
            parse_hello_resp(&resp).expect("the reply is a hello response");
        assert_eq!(server_wire, WIRE_VERSION);
        assert!(
            admit_client(min_client).is_ok(),
            "the floor the cluster reports must itself be inside its window"
        );
        // …and a client-surface call on that connection goes through.
        assert!(
            c.call(MSG_CLUSTER_DF, rkyv_encode(&ClusterDfReq {}))
                .await
                .is_ok(),
            "an admitted client must be served"
        );

        // (2) A client one version AHEAD. Images are built from `main`, so a
        //     wheel running ahead of an un-upgraded cluster is the routine
        //     case, not an exotic one — and the refusal has to say so, because
        //     the fix is to deploy rather than to rebuild.
        let c = RpcClient::connect(mgr_addr).await.expect("connect");
        let r = c.call(MSG_CLIENT_HELLO, hello(WIRE_VERSION + 1)).await;
        let (code, msg) = status(&r).expect("a client above the ceiling must be refused");
        assert_eq!(code, StatusCode::FailedPrecondition, "{msg}");
        assert!(msg.contains("NEWER"), "{msg}");

        // (3) The verdict STICKS for the rest of that connection. This is the
        //     acceptance's second half: the gate is the SERVER's, so a client
        //     that ignores the refusal and issues a real request meets the same
        //     answer — it is not a one-frame courtesy.
        let r = c.call(MSG_CLUSTER_DF, rkyv_encode(&ClusterDfReq {})).await;
        let (code, msg) = status(&r).expect("the later request must be refused too");
        assert_eq!(code, StatusCode::FailedPrecondition, "{msg}");

        // (4) …but the NEGOTIATION channel stays open on that same refused
        //     connection. Gating the question on its own answer would admit
        //     nobody, and an operator staring at a refusal needs to be able to
        //     ask what the cluster actually speaks.
        assert!(
            c.call(MSG_GET_CLUSTER_ID, rkyv_encode(&GetClusterIdReq {}))
                .await
                .is_ok(),
            "MSG_GET_CLUSTER_ID must stay answerable to a refused client"
        );

        // (5) A connection that says NOTHING is served. Every client built
        //     before the hello existed is silent — and so is every partition
        //     server and extent node, which dial through `ConnPool` with no
        //     handshake of any kind. This is what makes the change inert.
        let c = RpcClient::connect(mgr_addr).await.expect("connect");
        assert!(
            c.call(MSG_CLUSTER_DF, rkyv_encode(&ClusterDfReq {}))
                .await
                .is_ok(),
            "a silent connection must still be served"
        );

        // (6) A malformed hello is InvalidArgument, never a version. Without
        //     the magic check any 8-byte payload landing on this msg_type
        //     would parse, and whatever number it yielded would decide
        //     admission.
        let r = c
            .call(MSG_CLIENT_HELLO, Bytes::from_static(&[0u8; 8]))
            .await;
        let (code, msg) = status(&r).expect("a non-hello payload must be refused");
        assert_eq!(code, StatusCode::InvalidArgument, "{msg}");
    });
}

/// `MSG_GET_REGIONS` is the one message on BOTH surfaces — an SDK routes with
/// it and so does every partition server's `sync_regions_once`. A PS sends no
/// hello, so gating it would refuse region sync for the whole fleet the moment
/// the client floor rose: the outage the msg_type scoping exists to prevent,
/// arriving through the set instead of through the connection.
///
/// Pinned here, at the level where it would actually bite, rather than only as
/// a predicate assertion. Delete this together with a way for a cluster peer to
/// identify itself — never on its own.
#[test]
fn a_refused_client_can_still_reach_the_message_partition_servers_share_with_it() {
    let port = pick_stable_port_pair();
    let mgr_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    start_manager(mgr_addr);

    compio::runtime::Runtime::new().unwrap().block_on(async move {
        let c = RpcClient::connect(mgr_addr).await.expect("connect");
        let r = c.call(MSG_CLIENT_HELLO, hello(WIRE_VERSION + 1)).await;
        assert!(r.is_err(), "precondition: this connection is refused");

        // Not asserting the RESULT of get_regions (an empty cluster answers
        // whatever it answers) — only that it is not the wire-version refusal.
        let r = c
            // GetRegions takes an empty payload — it has no request struct.
            .call(MSG_GET_REGIONS, Bytes::new())
            .await;
        if let Some((code, msg)) = status(&r) {
            assert_ne!(
                code,
                StatusCode::FailedPrecondition,
                "get_regions must not be wire-gated: {msg}"
            );
        }
    });
}
