//! The negotiation channel is pinned to exact bytes.
//!
//! `GetClusterIdReq`/`GetClusterIdResp` and `MSG_CLIENT_HELLO` are the messages
//! decoded BEFORE any compatibility decision can be made. Every other message
//! is protected by the handshake; these three are what the handshake is made
//! of, so they get the one thing a version number cannot give them — a record
//! of their bytes.
//!
//! **A comment declaring them frozen has already failed once.**
//! `GetClusterIdResp` lost its `wire_fingerprint` field in a commit that left
//! `WIRE_VERSION` unchanged on both sides (`4a9b336`, with the version sitting
//! at 36 before and after), which no per-bump review can see. That is the
//! failure this file exists to make impossible.
//!
//! ## If this test fails, do NOT update the recorded bytes
//!
//! The recorded value is not a cache of the current encoding — it is the
//! encoding already-deployed clients decode with code that cannot be changed.
//! A diff here means one of exactly two things:
//!
//! 1. **You changed a negotiation struct.** That is a client-facing break of
//!    the one surface that has no fallback: put the addition in a NEW msg_type
//!    instead. If it genuinely cannot go anywhere else, it is a
//!    `MIN_CLIENT_WIRE_VERSION` raise plus an announced window, not an edit.
//! 2. **An rkyv upgrade changed the archived format.** Also a client-wire
//!    break, with the same answer.
//!
//! Refreshing the recorded value is how the deleted schema fingerprint failed:
//! each false alarm taught the reflex of updating it without looking, and a
//! real change then went through on that reflex. (The ONE legitimate write of
//! a recorded value is the commit that first records it.)
//!
//! What this does NOT catch is a change that leaves the bytes alone and moves
//! what a field MEANS — `a857084` shipped exactly that. No test catches it;
//! it stays a review obligation.

use autumn_rpc::client_hello::{
    encode_hello_req, encode_hello_resp, parse_hello_req, parse_hello_resp,
    CLIENT_HELLO_MAGIC, MSG_CLIENT_HELLO,
};
use autumn_rpc::manager_rpc::{rkyv_decode, rkyv_encode, GetClusterIdReq, GetClusterIdResp};

/// Padding is zeroed by rkyv's writer before resolving, so the encoding is
/// deterministic and a byte-for-byte record is meaningful.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn get_cluster_id_req_is_frozen() {
    assert_eq!(hex(&rkyv_encode(&GetClusterIdReq {})), "", "read this file's header before touching this value");
}

#[test]
fn get_cluster_id_resp_is_frozen() {
    // A fixed instance, not one built from the live constants: pinning the
    // LAYOUT means the recorded bytes must not move when WIRE_VERSION does.
    let resp = GetClusterIdResp {
        code: 0,
        message: "hi".to_string(),
        cluster_id: "c".to_string(),
        wire_version_min: 0x11121314,
        wire_version_max: 0x21222324,
        cluster_version: 0x31323334,
    };
    const RECORDED: &str =
        "000000006869ffffffffffff63ffffffffffffff141312112423222134333231";
    assert_eq!(
        hex(&rkyv_encode(&resp)),
        RECORDED,
        "read this file's header before touching this value"
    );

    // The recorded string must be a real encoding of THAT struct, not a
    // plausible-looking paste. Without this a mistyped digit would be frozen
    // in as the contract and every later diff would be measured against it.
    let bytes: Vec<u8> = (0..RECORDED.len() / 2)
        .map(|i| u8::from_str_radix(&RECORDED[i * 2..i * 2 + 2], 16).unwrap())
        .collect();
    let back: GetClusterIdResp = rkyv_decode(&bytes).expect("recorded bytes decode");
    assert_eq!(back.code, resp.code);
    assert_eq!(back.message, resp.message);
    assert_eq!(back.cluster_id, resp.cluster_id);
    assert_eq!(back.wire_version_min, resp.wire_version_min);
    assert_eq!(back.wire_version_max, resp.wire_version_max);
    assert_eq!(back.cluster_version, resp.cluster_version);
}

#[test]
fn the_client_hello_is_frozen() {
    assert_eq!(MSG_CLIENT_HELLO, 0x5F, "the opcode is part of the frozen channel");
    assert_eq!(hex(&CLIENT_HELLO_MAGIC.to_le_bytes()), "41554831"); // b"AUH1"
    assert_eq!(hex(&encode_hello_req(0x11121314)), "4155483114131211");
    assert_eq!(hex(&encode_hello_resp(0x11121314, 0x21222324)), "1413121124232221");

    // And the decoders still read what that era's encoders wrote — a frozen
    // encoding whose decoder drifted is the same break in the other direction.
    let req_bytes: [u8; 8] = [0x41, 0x55, 0x48, 0x31, 0x14, 0x13, 0x12, 0x11];
    assert_eq!(parse_hello_req(&req_bytes), Some(0x11121314));
    let resp_bytes: [u8; 8] = [0x14, 0x13, 0x12, 0x11, 0x24, 0x23, 0x22, 0x21];
    assert_eq!(parse_hello_resp(&resp_bytes), Some((0x11121314, 0x21222324)));
}
