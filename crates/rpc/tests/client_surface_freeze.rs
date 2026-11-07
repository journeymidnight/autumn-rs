//! Every message an embedded client sends or decodes is pinned to exact bytes.
//!
//! `negotiation_freeze.rs` pins the three structs the handshake is MADE of.
//! This file pins everything the handshake PROTECTS: the request and response
//! forms behind each msg_type in `client_hello`'s two client-surface sets, plus
//! the capability claims a client decodes out of its own minted token.
//!
//! ## Why a freeze here and a hand-maintained version number everywhere else
//!
//! The deleted schema fingerprint hashed the SOURCE of every wire module, and
//! translating one Chinese comment split a running cluster. It covered the
//! cluster-internal schema, where editing a struct in place is the CORRECT
//! answer — you bump the version and stop the world — so
//! it fired constantly on changes whose right response was "yes, I know", and
//! that taught the reflex of refreshing the recorded value without looking.
//! A real change then went through on that reflex.
//!
//! The client surface inverts both halves. Editing one of these structs in
//! place is NEVER the correct answer: clients built from older commits are
//! live by construction, and rkyv has no field tags, so an old decoder reading
//! a changed struct can return `Ok` with its fields shifted
//! (`project_rkyv_add_field_not_always_loud`). And what is recorded is the
//! ENCODING, not the source, so a comment, a reordered `use` and a doc edit
//! move nothing. A rename does force an edit HERE — the fixtures are struct
//! literals — but to the fixture, never to a recorded value. A diff in the
//! recorded bytes is the rule speaking, not noise.
//!
//! ## If this test fails, do NOT update the recorded bytes
//!
//! The recorded value is the encoding already-deployed clients decode with code
//! that cannot be changed. Three things can move one, and only the third is an
//! edit to the table:
//!
//! 1. **You changed a client-facing struct or layout.** The fix is the
//!    two-form rule:
//!
//!    > **A new form of a client-facing message takes a NEW msg_type. The old
//!    > form keeps its opcode and its struct, untouched, until
//!    > `MIN_CLIENT_WIRE_VERSION` rises past the version that introduced its
//!    > successor.**
//!
//!    So: add `MSG_FOO_V2` and a new struct, leave the old one alone, serve
//!    both, and add the new form to the table beside the old one. A pure
//!    msg_type addition is not a wire bump (`crates/rpc/src/lib.rs`), so this
//!    costs nothing but the maintenance of one extra form — and the width of
//!    the window is then a count of forms somebody is maintaining rather than
//!    a number picked in the abstract.
//!
//!    Editing the struct in place instead is a `MIN_CLIENT_WIRE_VERSION` raise
//!    plus an announced window plus a rebuild of every image carrying an
//!    embedded client, which is the cost this whole mechanism exists to avoid
//!    paying by accident.
//!
//! 2. **EVERY value moved at once.** Then you did not touch a struct — rkyv's
//!    archived format did, through a dependency bump or a feature another
//!    crate in the graph turned on (`big_endian`, `pointer_width_*`: Cargo
//!    unifies features, and six crates here depend on rkyv independently).
//!    That is a client-facing break of every form simultaneously and the
//!    two-form rule cannot express it: there is no per-message fix. It is a
//!    `MIN_CLIENT_WIRE_VERSION` raise, an announced window, and a rebuild of
//!    every embedded client — decided deliberately, with the whole table
//!    re-recorded in that same commit. A mass red is the one shape that breeds
//!    the refresh reflex, so it is spelled out here rather than left to be
//!    discovered under pressure.
//!
//! 3. **A form was deleted** because `MIN_CLIENT_WIRE_VERSION` rose past its
//!    successor. Remove its fixture AND its row together; the tests below
//!    object to either one on its own.
//!
//! Outside those three, refreshing a recorded value is how the deleted
//! fingerprint waved a real change through.
//!
//! ## What this catches, and what it does not
//!
//! An added, removed or reordered field moves the bytes. For the rkyv forms an
//! added field ALSO fails to compile here, because those fixtures are struct
//! literals and Rust requires literals to be exhaustive — so the first signal
//! points at the field rather than at a hex string. **Never write
//! `..Default::default()` in a fixture**: several of these types derive
//! `Default`, and that spelling throws the exhaustiveness away and with it the
//! compile-time half.
//!
//! The one hand-coded form does NOT get that second signal: `bulk response
//! head` is built through its encoder, so a field added and filled INSIDE that
//! function compiles here and is caught only by the byte diff. Its layout is
//! therefore also asserted offset by offset below, which is what names the
//! field when one moves.
//!
//! Retyping a field usually moves the bytes but not always — `u32` to `i32` at
//! the same value moves nothing. And a change that leaves the bytes alone and
//! moves what a field MEANS (an earlier change shipped exactly that) is invisible
//! here. Both stay review obligations, and they are the reason §7's rule is
//! about msg_types rather than about encodings.

use autumn_rpc::cap_token::CapClaims;
use autumn_rpc::client_hello::{is_client_surface_mgr_msg, is_client_surface_ps_msg};
use autumn_rpc::error::{RpcError, StatusCode};
use autumn_rpc::extent_rpc::{self as en, ReadAmReq, ReadReq, ReadResp};
use autumn_rpc::frame;
use autumn_rpc::manager_rpc::{
    self as mgr, rkyv_decode, rkyv_encode, AcquireLeaseReq, AcquireLeaseResp, AllocInodesReq,
    AllocInodesResp, ClientRegion, ClientRegionsResp, ClusterDfReq, ClusterDfResp, DiskCapWire,
    GetRegionsResp, HeartbeatLeaseReq,
    HeartbeatLeaseResp, MgrClientId, MgrInodeLeaseInfo, MgrInvalidation, MgrPsDetail, MgrRange,
    MgrRegionInfo, MintTokenReq, MintTokenResp, NodeCapWire, PollInvalidationsReq,
    PollInvalidationsResp, ReleaseLeaseReq, ReleaseLeaseResp,
};
use autumn_rpc::partition_rpc::{
    self as ps, BatchDeleteOp, BatchDeleteReq,
    BatchDeleteResp, BatchGetCtrl, BatchGetItem, BatchGetReq, BatchPutOp, BatchPutReq,
    BatchPutResp, ClientAuthReq, ClientAuthResp, CompareWriteReq, GetAmReq, GetAmResp,
    DeleteReq, DeleteResp, GetReq, HeadReq, HeadResp, PutReq, PutResp, RangeEntry, RangeReq, RangeResp, ValuePosition,
};

// ── the recorded bytes ──────────────────────────────────────────────────────

/// `(form name, encoding)`. Read this file's header before touching a value.
const GOLDEN: &[(&str, &str)] = &[
    // partition surface
    ("PutResp", "7075742d6d6573736167657468652d6b65790000070000008b000000e8ffffffebffffff07000000"),
    ("GetAmReq", "7468652d6b6579001817161514131211f0ffffff070000002423222134333231484746454443424158575655545352516867666564636261"),
    ("GetAmResp", "6765742d616d2d6d6573736167650000070000008e000000ecffffff000000007877767574737271"),
    ("GetReq", "7468652d6b6579001817161514131211f0ffffff07000000242322213433323148474645444342410100000000000000"),
    ("CompareWriteReq", "7468652d6b65796f6c642d76616c756518171615141312112827262524232221e0ffffff0700000001000000dbffffff0900000001000000383736353433323148474645444342415857565554535251"),
    ("DeleteReq", "7468652d6b6579001817161514131211f0ffffff07000000282726252423222138373635343332314847464544434241"),
    ("DeleteResp", "64656c6574652d6d6573736167657468652d6b6579000000070000008e000000e4ffffffeaffffff07000000"),
    ("HeadReq", "7468652d6b6579001817161514131211f0ffffff070000002827262524232221"),
    ("HeadResp", "686561642d6d65737361676500000000070000008c000000ecffffff010000001817161514131211"),
    ("RangeReq", "7468652d7072656669787468652d737461727400000000001817161514131211e0ffffff0a000000e2ffffff0900000024232221000000003837363534333231"),
    ("RangeResp", "72616e67652d6d6573736167656b65792d6f6e6576616c75652d6f6e656b65792d74776f76616c75652d74776f000000ddffffff07000000dcffffff09000000ddffffff07000000dcffffff090000007468652d656e6400070000008d000000a4ffffffccffffff0200000001000000e0ffffff07000000"),
    ("BatchPutReq", "7468652d6b657900f8ffffff070000003433323100000000484746454443424158575655545352516867666564636261747372710000000018171615141312112827262524232221c0ffffff01000000"),
    ("BatchPutResp", "62617463682d7075742d6d6573736167650001020700000091000000e8fffffff1ffffff03000000"),
    ("BatchGetReq", "6b65792d6f6e656b65792d74776f0000f0ffffff070000003433323144434241e7ffffff0700000000000000000000001817161514131211282726252423222101000000ccffffff0200000000000000"),
    ("BatchGetCtrl", "62617463682d6765742d6d657373616765000102030000001413121100000000000000000000000031302e302e302e313a3731303031302e302e302e323a3731303000008d000000e4ffffff8d000000e9ffffff000000001817161514131211282726252423222138373635343332314847464544434241ccffffff02000000040000000000000058575655545352519100000070ffffff79ffffff0400000078ffffff04000000b0ffffff01000000"),
    ("BatchDeleteReq", "7468652d6b657900f8ffffff070000003837363534333231484746454443424118171615141312112827262524232221d8ffffff01000000"),
    ("BatchDeleteResp", "62617463682d64656c6574652d6d657373616765000102000700000094000000e4fffffff0ffffff03000000"),
    ("ValuePosition", "31302e302e302e313a3731303031302e302e302e323a3731303000008d000000e4ffffff8d000000e9ffffff000000001817161514131211282726252423222138373635343332314847464544434241ccffffff0200000004000000000000005857565554535251"),
    ("ClientAuthReq", "6f70617175652d6361706162696c6974792d746f6b656e00e8ffffff17000000"),
    ("ClientAuthResp", "617574682d6d657373616765070000008c000000f0ffffff"),
    ("PutReq", "7468652d6b6579001817161514131211f0ffffff0700000028272625242322213837363534333231484746454443424158575655545352516463626100000000"),
    ("bulk response head", "141312115001372100000d0000000762756c6b2d6d65737361676561b09c26"),
    // manager surface
    ("MintTokenReq", "7468652d7072696e636970616c7468652d63726564656e7469616c008d000000e4ffffffe9ffffff0e000000"),
    ("MintTokenResp", "6d696e742d6d6573736167656f70617175652d6361706162696c6974792d746f6b656e0000000000070000008c000000d4ffffffd8ffffff17000000000000001817161514131211"),
    ("CapClaims", "617574756d6e2e6361702e76317468652d6973737565727468652d61756469656e636574656e616e742f612f74656e616e742f622f000000ebffffff09000000ecffffff09000000010000008d000000b4ffffff141312118a000000b5ffffff8c000000b7ffffff282726252423222138373635343332314847464544434241b8ffffff02000000"),
    ("AllocInodesReq", "7468652d766f6c756d6500000000000014131211000000002827262524232221e0ffffff0a000000"),
    ("AllocInodesResp", "616c6c6f632d6d657373616765000000070000008d000000ecffffff000000001817161514131211"),
    ("AcquireLeaseReq", "667573652d686f73740000000000000001101112131415161718191a1b1c1d1e1f00000089000000dcffffff0000000018171615141312110201000000000000"),
    ("AcquireLeaseResp", "616371756972652d6d65737361676500070000008f000000ecffffff000000000100000000000000413100000000000026590000000000000100000018170000"),
    ("ReleaseLeaseReq", "667573652d686f73740000000000000001101112131415161718191a1b1c1d1e1f00000089000000dcffffff000000001817161514131211"),
    ("ReleaseLeaseResp", "72656c656173652d6d65737361676500070000008f000000ecffffff0000000001000000000000001817161514131211"),
    ("HeartbeatLeaseReq", "667573652d686f73740000000000000001101112131415161718191a1b1c1d1e1f00000089000000dcffffff000000001817161514131211"),
    ("HeartbeatLeaseResp", "6865617274626561742d6d657373616765000000000000000700000091000000e4ffffff000000000100000000000000413100000000000026590000000000000100000018170000"),
    ("PollInvalidationsReq", "667573652d686f737400000001101112131415161718191a1b1c1d1e1f00000089000000e0ffffff"),
    ("PollInvalidationsResp", "706f6c6c2d6d65737361676500000000181716151413121128272625242322210300000000000000070000008c000000d4ffffffdcffffff01000000"),
    ("ClusterDfReq", ""),
    ("ClusterDfResp", "64662d6d6573736167657468652d6469736b2d757569640061605f5e5d5c5b5a8d000000eaffffff71706f6e6d6c6b6a01007f7e7d7c7b7a02010f0e0d0c0b0a010001000000000021201f1e1d1c1b1a31302f2e2d2c2b2a41403f3e3d3c3b3a51504f4e4d4c4b4a01000000acffffff0100000000000000070000008a00000084ffffff000000001817161514131211282726252423222138373635343332314847464544434241585756555453525168676665646362617877767574737271080706050403020111100f0e0d0c0b0a78ffffff01000000"),
    ("GetRegionsReq (empty payload)", ""),
    ("GetClientRegionsReq (empty payload)", ""),
    ("error envelope", "03776972652d76657273696f6e206d69736d61746368"),
    ("ReadReq", "181716151413121128272625242322213837363534333231484746454443424100010000545352510100000000000000"),
    ("ReadResp", "726561642d6d65737361676574737271848382810000000068676665646362618c000000e0ffffffe4ffffff02000000"),
    ("ReadAmReq", "18171615141312112827262524232221383736353433323148474645444342410001000054535251000000000000000068676665646362617877767574737271"),
    ("GetRegionsResp", "726567696f6e732d6d6573736167657468652d73746172747468652d656e6400181716151413121101000000e3ffffff09000000e4ffffff070000000000000028272625242322213837363534333231484746454443424158575655545352516867666564636261787776757473727131302e302e302e313a37313030000000080706050403020111100f0e0d0c0b0a8d000000e0ffffff31302e302e302e323a3731303000000021201f1e1d1c1b1a8d000000e8ffffff070000008f00000044ffffff5cffffff01000000b4ffffff01000000d4ffffff01000000"),
    ("ClientRegionsResp", "636c69656e742d726567696f6e732d6d6573736167657468652d73746172747468652d656e640000181716151413121101000000e2ffffff09000000e3ffffff070000000000000028272625242322213837363534333231787776757473727131302e302e302e313a37313030000000080706050403020111100f0e0d0c0b0a8d000000e0ffffff31302e302e302e323a3731303000000021201f1e1d1c1b1a8d000000e8ffffff070000009600000054ffffff74ffffff01000000b4ffffff01000000d4ffffff01000000"),
];

// ── the fixtures ────────────────────────────────────────────────────────────

/// Which number space a msg_type belongs to. They overlap — `0x53` is
/// `MSG_BATCH_PUT` to a partition server and `MSG_ALLOC_INODES` to a manager —
/// so coverage has to be keyed by both.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Surface {
    Ps,
    Mgr,
    /// An extent node. A client reads straight from one when `--direct-read`
    /// is on, which is the default, so the read forms are client-facing even
    /// though the EN has no version concept and no admission gate
    /// (`docs/client_wire_compat_design.md` §2 and §8).
    En,
}
use Surface::{En, Mgr, Ps};

/// Which half of an exchange a form is. Coverage is per DIRECTION, not per
/// opcode: with one form enough to mark an opcode covered, deleting a response
/// fixture left the suite green — the request alone kept vouching for it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dir {
    Req,
    Resp,
    /// Neither half — it rides INSIDE another form's bytes, so it vouches for
    /// no direction. `CapClaims` is the case: no wire struct has it as a
    /// field, and the SDK decodes it out of the opaque token anyway.
    Embedded,
}
use Dir::{Embedded, Req, Resp};

struct Frozen {
    /// Every `(surface, msg_type)` this form rides on. A form reachable from
    /// two opcodes lists both, which is what lets the coverage test below
    /// prove a set member is not sitting here unfrozen.
    ///
    /// These lists are hand-maintained and nothing ties them to the
    /// dispatchers, so a wrong one can make coverage LOOK complete. What they
    /// can prove is the failure that actually happens — an opcode or a
    /// direction with nothing recorded at all.
    on: &'static [(Surface, u8)],
    dir: Dir,
    what: &'static str,
    live: String,
    /// Decode the RECORDED bytes back and re-encode them. Its job is not to
    /// catch a typo — test 1 catches every typo first, against the live
    /// fixture. It is that TODAY'S DECODER still reads what was recorded: a
    /// decoder that drifted (a stricter `bytecheck` pass, a changed validation
    /// rule) would leave the encoder agreeing with the record and still refuse
    /// bytes a deployed client sends. `None` where no decoder takes the form
    /// on its own; those have a dedicated layout test below instead.
    reencode: Option<fn(&[u8]) -> Result<String, String>>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err(format!("odd number of hex digits ({})", s.len()));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| format!("at byte {}: {e}", i / 2)))
        .collect()
}

/// Fixtures populate EVERY field with a distinct non-zero value, and every
/// `Vec`/`String`/`Option` non-empty. Two same-typed fields left at zero would
/// survive being swapped.
macro_rules! frozen {
    ($on:expr, $dir:expr, $ty:ty, $val:expr) => {
        Frozen {
            on: $on,
            dir: $dir,
            what: stringify!($ty),
            live: hex(&rkyv_encode::<$ty>(&$val)),
            reencode: Some(|data: &[u8]| -> Result<String, String> {
                let v: $ty = rkyv_decode(data)?;
                Ok(hex(&rkyv_encode(&v)))
            }),
        }
    };
}

fn a_client_id() -> MgrClientId {
    MgrClientId {
        kind: mgr::LEASE_CLIENT_KIND_FUSE,
        uuid: [
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
            0x1e, 0x1f,
        ],
        host: "fuse-host".to_string(),
    }
}

fn a_lease() -> MgrInodeLeaseInfo {
    MgrInodeLeaseInfo {
        ino: 0x3141,
        version: 0x5926,
        writer_present: true,
        ttl_secs: 0x1718,
    }
}

fn a_position() -> ValuePosition {
    ValuePosition {
        extent_id: 0x1112131415161718,
        value_offset: 0x2122232425262728,
        value_len: 0x3132333435363738,
        eversion: 0x4142434445464748,
        replica_addrs: vec!["10.0.0.1:7100".to_string(), "10.0.0.2:7100".to_string()],
        ec_data_shards: 4,
        ec_sealed_length: 0x5152535455565758,
    }
}

fn forms() -> Vec<Frozen> {
    vec![
        // ── partition surface ───────────────────────────────────────────────
        frozen!(
            &[(Ps, ps::MSG_PUT)],
            Req,
            PutReq,
            PutReq {
                part_id: 0x1112131415161718,
                key: b"the-key".to_vec(),
                expires_at: 0x2122232425262728,
                region_epoch: 0x3132333435363738,
                inode_hint: 0x4142434445464748,
                lease_epoch: 0x5152535455565758,
                evict_rank: 0x61626364,
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_COMPARE_WRITE)],
            Req,
            CompareWriteReq,
            CompareWriteReq {
                part_id: 0x1112131415161718,
                region_epoch: 0x2122232425262728,
                key: b"the-key".to_vec(),
                expected: Some(b"old-value".to_vec()),
                delete: true,
                expires_at: 0x3132333435363738,
                inode_hint: 0x4142434445464748,
                lease_epoch: 0x5152535455565758,
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_PUT), (Ps, ps::MSG_COMPARE_WRITE)],
            Resp,
            PutResp,
            PutResp {
                code: 7,
                message: "put-message".to_string(),
                key: b"the-key".to_vec(),
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_GET)],
            Req,
            GetReq,
            GetReq {
                part_id: 0x1112131415161718,
                key: b"the-key".to_vec(),
                offset: 0x21222324,
                length: 0x31323334,
                region_epoch: 0x4142434445464748,
                direct: true,
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_GET_AM)],
            Req,
            GetAmReq,
            GetAmReq {
                part_id: 0x1112131415161718,
                key: b"the-key".to_vec(),
                offset: 0x21222324,
                length: 0x31323334,
                region_epoch: 0x4142434445464748,
                tag: 0x5152535455565758,
                max_len: 0x6162636465666768,
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_GET_AM)],
            Resp,
            GetAmResp,
            GetAmResp {
                code: 7,
                message: "get-am-message".to_string(),
                value_len: 0x7172737475767778,
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_DELETE)],
            Req,
            DeleteReq,
            DeleteReq {
                part_id: 0x1112131415161718,
                key: b"the-key".to_vec(),
                region_epoch: 0x2122232425262728,
                inode_hint: 0x3132333435363738,
                lease_epoch: 0x4142434445464748,
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_DELETE)],
            Resp,
            DeleteResp,
            DeleteResp {
                code: 7,
                message: "delete-message".to_string(),
                key: b"the-key".to_vec(),
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_HEAD)],
            Req,
            HeadReq,
            HeadReq {
                part_id: 0x1112131415161718,
                key: b"the-key".to_vec(),
                region_epoch: 0x2122232425262728,
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_HEAD)],
            Resp,
            HeadResp,
            HeadResp {
                code: 7,
                message: "head-message".to_string(),
                found: true,
                value_length: 0x1112131415161718,
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_RANGE)],
            Req,
            RangeReq,
            RangeReq {
                part_id: 0x1112131415161718,
                prefix: b"the-prefix".to_vec(),
                start: b"the-start".to_vec(),
                limit: 0x21222324,
                region_epoch: 0x3132333435363738,
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_RANGE)],
            Resp,
            RangeResp,
            RangeResp {
                code: 7,
                message: "range-message".to_string(),
                entries: vec![
                    RangeEntry {
                        key: b"key-one".to_vec(),
                        value: b"value-one".to_vec(),
                    },
                    RangeEntry {
                        key: b"key-two".to_vec(),
                        value: b"value-two".to_vec(),
                    },
                ],
                has_more: true,
                cur_end_key: b"the-end".to_vec(),
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_BATCH_PUT)],
            Req,
            BatchPutReq,
            BatchPutReq {
                part_id: 0x1112131415161718,
                region_epoch: 0x2122232425262728,
                ops: vec![BatchPutOp {
                    key: b"the-key".to_vec(),
                    value_len: 0x31323334,
                    expires_at: 0x4142434445464748,
                    inode_hint: 0x5152535455565758,
                    lease_epoch: 0x6162636465666768,
                    evict_rank: 0x71727374,
                }],
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_BATCH_PUT)],
            Resp,
            BatchPutResp,
            BatchPutResp {
                code: 7,
                message: "batch-put-message".to_string(),
                statuses: vec![0, 1, 2],
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_BATCH_GET)],
            Req,
            BatchGetReq,
            BatchGetReq {
                part_id: 0x1112131415161718,
                region_epoch: 0x2122232425262728,
                direct: true,
                items: vec![
                    BatchGetItem { key: b"key-one".to_vec(), offset: 0x31323334, length: 0x41424344 },
                    BatchGetItem { key: b"key-two".to_vec(), offset: 0, length: 0 },
                ],
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_BATCH_GET)],
            Resp,
            BatchGetCtrl,
            BatchGetCtrl {
                message: "batch-get-message".to_string(),
                statuses: vec![0, 1, 2, 3],
                value_lens: vec![0x11121314, 0, 0, 0],
                positions: vec![a_position()],
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_BATCH_DELETE)],
            Req,
            BatchDeleteReq,
            BatchDeleteReq {
                part_id: 0x1112131415161718,
                region_epoch: 0x2122232425262728,
                ops: vec![BatchDeleteOp {
                    key: b"the-key".to_vec(),
                    inode_hint: 0x3132333435363738,
                    lease_epoch: 0x4142434445464748,
                }],
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_BATCH_DELETE)],
            Resp,
            BatchDeleteResp,
            BatchDeleteResp {
                code: 7,
                message: "batch-delete-message".to_string(),
                statuses: vec![0, 1, 2],
            }
        ),
        // A `MSG_GET` answered with a position: `[CODE_POSITION]` then this.
        frozen!(
            &[(Ps, ps::MSG_GET)],
            Resp,
            ValuePosition,
            a_position()
        ),
        frozen!(
            &[(Ps, ps::MSG_CLIENT_AUTH)],
            Req,
            ClientAuthReq,
            ClientAuthReq {
                token: b"opaque-capability-token".to_vec(),
            }
        ),
        frozen!(
            &[(Ps, ps::MSG_CLIENT_AUTH)],
            Resp,
            ClientAuthResp,
            ClientAuthResp {
                code: 7,
                message: "auth-message".to_string(),
            }
        ),
        // A hand-coded fixed layout, not rkyv. `reencode` is `None` because it
        // does not round-trip through a decoder on its own; it has a
        // field-by-field layout test below, which is stronger than a re-encode.
        Frozen {
            on: &[
                (Ps, ps::MSG_GET),
                (Ps, ps::MSG_BATCH_GET),
                (En, en::MSG_READ),
                (En, en::MSG_READ_AM),
            ],
            dir: Resp,
            what: "bulk response head",
            live: hex(&frame::encode_bulk_response_head(
                0x11121314, 0x50, 7, "bulk-message", 0x2122,
            )),
            reencode: None,
        },
        // The refusal envelope. `[status_code: u8][utf8 message]`, carried by
        // every `FLAG_ERROR` frame from a manager, a partition server or an
        // extent node, and decoded by every embedded client on every failure.
        // It is keyed by no msg_type — it can answer ANY of them — which is
        // why a msg_type-shaped inventory does not reach it.
        //
        // It is also what carries this mechanism's OWN refusal: a client below
        // the floor learns which way round the mismatch is by decoding these
        // bytes. A break here is a client that cannot read why it was refused.
        Frozen {
            on: &[],
            dir: Resp,
            what: "error envelope",
            live: hex(&RpcError::encode_status(
                StatusCode::FailedPrecondition,
                "wire-version mismatch",
            )),
            reencode: Some(|data: &[u8]| -> Result<String, String> {
                let (code, message) = RpcError::decode_status(data);
                Ok(hex(&RpcError::encode_status(code, &message)))
            }),
        },
        // ── extent-node surface ─────────────────────────────────────────────
        //
        // `--direct-read` is on by default, so a client reads VALUE BYTES
        // straight from an extent node: a `MSG_GET` position names the extent
        // and `read_extent_value_direct` sends `MSG_READ` to the address in
        // it. Neither surface set mentions an EN opcode, so the coverage test
        // names this one outright.
        frozen!(
            &[(En, en::MSG_READ)],
            Req,
            ReadReq,
            ReadReq {
                extent_id: 0x1112131415161718,
                eversion: 0x2122232425262728,
                offset: 0x3132333435363738,
                length: 0x4142434445464748,
                mode: en::READ_EXACT,
                payload_location: en::PAYLOAD_LOCATION_IN_SHARD_FILE,
                shard_index: 0x51525354,
                with_crcs: true,
            }
        ),
        // The read whose value comes as the active message the client tagged
        // (UCX); its reply is the `MSG_READ` reply with no value.
        frozen!(
            &[(En, en::MSG_READ_AM)],
            Req,
            ReadAmReq,
            ReadAmReq {
                read: ReadReq {
                    extent_id: 0x1112131415161718,
                    eversion: 0x2122232425262728,
                    offset: 0x3132333435363738,
                    length: 0x4142434445464748,
                    mode: en::READ_EXACT,
                    payload_location: en::PAYLOAD_LOCATION_IN_SHARD_FILE,
                    shard_index: 0x51525354,
                    with_crcs: false,
                },
                tag: 0x6162636465666768,
                max_len: 0x7172737475767778,
            }
        ),
        frozen!(
            &[(En, en::MSG_READ), (En, en::MSG_READ_AM)],
            Resp,
            ReadResp,
            ReadResp {
                end: 0x6162636465666768,
                message: "read-message".to_string(),
                block_crcs: vec![0x71727374, 0x81828384],
            }
        ),
        // ── manager surface ─────────────────────────────────────────────────
        frozen!(
            &[(Mgr, mgr::MSG_MINT_TOKEN)],
            Req,
            MintTokenReq,
            MintTokenReq {
                principal: "the-principal".to_string(),
                credential: b"the-credential".to_vec(),
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_MINT_TOKEN)],
            Resp,
            MintTokenResp,
            MintTokenResp {
                code: 7,
                message: "mint-message".to_string(),
                token: b"opaque-capability-token".to_vec(),
                exp: 0x1112131415161718,
            }
        ),
        // Not a field of any wire struct — it is rkyv-encoded into the opaque
        // token blob. It is on this surface anyway, because the SDK decodes the
        // claims out of its own minted token to check the namespace scope
        // (`crates/client/src/lib.rs`), so its layout is a client contract.
        frozen!(
            &[(Mgr, mgr::MSG_MINT_TOKEN), (Ps, ps::MSG_CLIENT_AUTH)],
            Embedded,
            CapClaims,
            CapClaims {
                ver: 1,
                typ: "autumn.cap.v1".to_string(),
                kid: 0x11121314,
                iss: "the-issuer".to_string(),
                aud: "the-audience".to_string(),
                iat: 0x2122232425262728,
                nbf: 0x3132333435363738,
                exp: 0x4142434445464748,
                allowed_prefixes: vec![b"tenant/a/".to_vec(), b"tenant/b/".to_vec()],
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_ALLOC_INODES)],
            Req,
            AllocInodesReq,
            AllocInodesReq {
                count: 0x11121314,
                floor: 0x2122232425262728,
                volume: b"the-volume".to_vec(),
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_ALLOC_INODES)],
            Resp,
            AllocInodesResp,
            AllocInodesResp {
                code: 7,
                message: "alloc-message".to_string(),
                base: 0x1112131415161718,
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_ACQUIRE_LEASE)],
            Req,
            AcquireLeaseReq,
            AcquireLeaseReq {
                client: a_client_id(),
                ino: 0x1112131415161718,
                mode: mgr::LEASE_MODE_WRITE,
                force: true,
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_ACQUIRE_LEASE)],
            Resp,
            AcquireLeaseResp,
            AcquireLeaseResp {
                code: 7,
                message: "acquire-message".to_string(),
                lease: Some(a_lease()),
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_RELEASE_LEASE)],
            Req,
            ReleaseLeaseReq,
            ReleaseLeaseReq {
                client: a_client_id(),
                ino: 0x1112131415161718,
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_RELEASE_LEASE)],
            Resp,
            ReleaseLeaseResp,
            ReleaseLeaseResp {
                code: 7,
                message: "release-message".to_string(),
                new_version: Some(0x1112131415161718),
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_HEARTBEAT_LEASE)],
            Req,
            HeartbeatLeaseReq,
            HeartbeatLeaseReq {
                client: a_client_id(),
                ino: 0x1112131415161718,
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_HEARTBEAT_LEASE)],
            Resp,
            HeartbeatLeaseResp,
            HeartbeatLeaseResp {
                code: 7,
                message: "heartbeat-message".to_string(),
                lease: Some(a_lease()),
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_POLL_INVALIDATIONS)],
            Req,
            PollInvalidationsReq,
            PollInvalidationsReq {
                client: a_client_id(),
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_POLL_INVALIDATIONS)],
            Resp,
            PollInvalidationsResp,
            PollInvalidationsResp {
                code: 7,
                message: "poll-message".to_string(),
                events: vec![MgrInvalidation {
                    ino: 0x1112131415161718,
                    version: 0x2122232425262728,
                    kind: mgr::LEASE_INVAL_META_CHANGED,
                }],
            }
        ),
        frozen!(&[(Mgr, mgr::MSG_CLUSTER_DF)], Req, ClusterDfReq, ClusterDfReq {}),
        frozen!(
            &[(Mgr, mgr::MSG_CLUSTER_DF)],
            Resp,
            ClusterDfResp,
            ClusterDfResp {
                code: 7,
                message: "df-message".to_string(),
                raw_total: 0x1112131415161718,
                raw_free: 0x2122232425262728,
                physical_used: 0x3132333435363738,
                logical_stored: 0x4142434445464748,
                logical_open_tail: 0x5152535455565758,
                logical_wal_debt: 0x6162636465666768,
                node_count: 0x7172737475767778,
                last_update_ms: 0x0102030405060708,
                logical_last_update_ms: 0x0a0b0c0d0e0f1011,
                per_node: vec![NodeCapWire {
                    node_id: 0x1a1b1c1d1e1f2021,
                    total: 0x2a2b2c2d2e2f3031,
                    free: 0x3a3b3c3d3e3f4041,
                    extent_bytes: 0x4a4b4c4d4e4f5051,
                    online: true,
                    disks: vec![DiskCapWire {
                        disk_id: 0x5a5b5c5d5e5f6061,
                        uuid: "the-disk-uuid".to_string(),
                        total: 0x6a6b6c6d6e6f7071,
                        free: 0x7a7b7c7d7e7f0001,
                        extent_bytes: 0x0a0b0c0d0e0f0102,
                        // Three adjacent bools: distinct, or swapping two of
                        // them would leave the bytes alone.
                        reported: true,
                        online: false,
                        faulted: true,
                    }],
                }],
            }
        ),
        frozen!(
            &[(Mgr, mgr::MSG_GET_REGIONS)],
            Resp,
            GetRegionsResp,
            GetRegionsResp {
                code: 7,
                message: "regions-message".to_string(),
                regions: vec![(
                    0x1112131415161718,
                    MgrRegionInfo {
                        rg: Some(MgrRange {
                            start_key: b"the-start".to_vec(),
                            end_key: b"the-end".to_vec(),
                        }),
                        part_id: 0x2122232425262728,
                        ps_id: 0x3132333435363738,
                        log_stream: 0x4142434445464748,
                        row_stream: 0x5152535455565758,
                        meta_stream: 0x6162636465666768,
                        region_epoch: 0x7172737475767778,
                    },
                )],
                ps_details: vec![(
                    0x0102030405060708,
                    MgrPsDetail {
                        ps_id: 0x0a0b0c0d0e0f1011,
                        address: "10.0.0.1:7100".to_string(),
                    },
                )],
                part_addrs: vec![(0x1a1b1c1d1e1f2021, "10.0.0.2:7100".to_string())],
            }
        ),
        // The request genuinely has no payload — no struct, zero bytes. Frozen
        // because "empty" is as much a contract as any layout: the day someone
        // gives it a body, every deployed client sends nothing and the handler
        // has to keep reading nothing.
        Frozen {
            on: &[(Mgr, mgr::MSG_GET_REGIONS)],
            dir: Req,
            what: "GetRegionsReq (empty payload)",
            live: String::new(),
            reencode: None,
        },
        // The SECOND form of the routing reply, under its own opcode. Both are
        // live at once and both are frozen: the old one because every client
        // below `WIRE_VERSION_WITH_CLIENT_REGIONS` still asks with it, the new
        // one because every client at or above it does. That is what the
        // two-form rule means in practice — the old form is not deprecated, it
        // is SERVED, until the floor passes the version that introduced this.
        frozen!(
            &[(Mgr, mgr::MSG_GET_CLIENT_REGIONS)],
            Resp,
            ClientRegionsResp,
            ClientRegionsResp {
                code: 7,
                message: "client-regions-message".to_string(),
                regions: vec![(
                    0x1112131415161718,
                    ClientRegion {
                        rg: Some(MgrRange {
                            start_key: b"the-start".to_vec(),
                            end_key: b"the-end".to_vec(),
                        }),
                        part_id: 0x2122232425262728,
                        ps_id: 0x3132333435363738,
                        region_epoch: 0x7172737475767778,
                    },
                )],
                ps_details: vec![(
                    0x0102030405060708,
                    MgrPsDetail {
                        ps_id: 0x0a0b0c0d0e0f1011,
                        address: "10.0.0.1:7100".to_string(),
                    },
                )],
                part_addrs: vec![(0x1a1b1c1d1e1f2021, "10.0.0.2:7100".to_string())],
            }
        ),
        Frozen {
            on: &[(Mgr, mgr::MSG_GET_CLIENT_REGIONS)],
            dir: Req,
            what: "GetClientRegionsReq (empty payload)",
            live: String::new(),
            reencode: None,
        },
    ]
}

// ── the tests ───────────────────────────────────────────────────────────────

#[test]
fn every_client_facing_form_is_frozen() {
    let forms = forms();
    let mut drifted = Vec::new();
    for f in &forms {
        let recorded = GOLDEN
            .iter()
            .find(|(what, _)| *what == f.what)
            .map(|(_, bytes)| *bytes);
        match recorded {
            None => drifted.push(format!(
                "  {:<32} NOT RECORDED — live encoding is {:?}",
                f.what, f.live
            )),
            Some(recorded) if recorded != f.live => drifted.push(format!(
                "  {:<32} recorded {:?}\n  {:<32}     live {:?}",
                f.what, recorded, "", f.live
            )),
            Some(_) => {}
        }
    }
    for (what, _) in GOLDEN {
        if !forms.iter().any(|f| f.what == *what) {
            drifted.push(format!(
                "  {what:<32} recorded but no fixture builds it — a form was deleted. \
                 Deleting a client-facing form is legal only once \
                 MIN_CLIENT_WIRE_VERSION has risen past its successor."
            ));
        }
    }
    assert!(
        drifted.is_empty(),
        "the client wire changed shape:\n{}\n\n\
         READ THIS FILE'S HEADER. The fix is a NEW msg_type carrying the new \
         form, with the old struct left exactly as it is — not an edit to the \
         recorded bytes.",
        drifted.join("\n")
    );
}

#[test]
fn the_recorded_bytes_decode_back_into_the_form_they_claim_to_be() {
    // Without this, a mistyped digit that happens to match a live encoding
    // once would become the contract, and every later run would agree with it.
    for f in forms() {
        let Some(reencode) = f.reencode else { continue };
        let (_, recorded) = GOLDEN
            .iter()
            .find(|(what, _)| *what == f.what)
            .unwrap_or_else(|| panic!("{} has no recorded bytes", f.what));
        let raw = unhex(recorded).unwrap_or_else(|e| panic!("{}: {e}", f.what));
        let again = reencode(&raw).unwrap_or_else(|e| {
            panic!("{}: the recorded bytes are not a valid encoding of it: {e}", f.what)
        });
        assert_eq!(
            again, *recorded,
            "{}: the recorded bytes decode, but re-encode differently",
            f.what
        );
    }
}

#[test]
fn every_client_facing_msg_type_has_a_frozen_form_in_both_directions() {
    // The mechanism that keeps the freeze from rotting. Adding a msg_type to a
    // client-surface set without recording its bytes fails here, so the set and
    // this file cannot drift apart — which is how a freeze normally dies: it
    // stays green while the surface grows past it.
    //
    // BOTH directions, because per-opcode was not enough: with one form enough
    // to mark an opcode covered, deleting `HeadResp`'s fixture and its recorded
    // row together left the whole suite green — `HeadReq` went on vouching for
    // `MSG_HEAD`, and a response is exactly the half an old client decodes.
    let forms = forms();
    let covered =
        |s: Surface, m: u8, d: Dir| forms.iter().any(|f| f.dir == d && f.on.contains(&(s, m)));

    let mut missing = Vec::new();
    let mut want = |s: Surface, m: u8, label: String| {
        for d in [Req, Resp] {
            if !covered(s, m, d) {
                missing.push(format!("  {label} has no recorded {d:?}"));
            }
        }
    };
    for m in 0..=u8::MAX {
        if is_client_surface_ps_msg(m) {
            want(Ps, m, format!("partition msg_type {m:#04x}"));
        }
        if is_client_surface_mgr_msg(m) {
            want(Mgr, m, format!("manager msg_type {m:#04x}"));
        }
    }
    // Un-gated, but on the client surface all the same: an SDK routes with it.
    // It is outside `is_client_surface_mgr_msg` only because a partition server
    // sends it too and sends no hello, which is an ADMISSION decision and says
    // nothing about whose bytes these are.
    want(Mgr, mgr::MSG_GET_REGIONS, "manager MSG_GET_REGIONS".into());
    // Its narrowed successor needs no line here: it IS inside
    // `is_client_surface_mgr_msg` (nothing but an SDK sends it), so the walk
    // above already demanded it.
    // The extent-node read a client issues directly. There is no set to walk
    // here — the EN has no gate and no version — so the one opcode a client
    // speaks to an EN is named outright. `--direct-read` is on by default, so
    // this is the common path, not a corner.
    want(En, en::MSG_READ, "extent-node MSG_READ".into());
    want(En, en::MSG_READ_AM, "extent-node MSG_READ_AM".into());
    assert!(
        missing.is_empty(),
        "these client-facing forms have no recorded encoding:\n{}\n\n\
         Add a fixture and record its bytes. A form an out-of-date client \
         sends or decodes, whose shape nothing pins, is the hole this file \
         exists to close.",
        missing.join("\n")
    );
}

#[test]
fn a_form_appears_once_and_the_table_agrees() {
    // Two fixtures under one name would both be compared against the first
    // recorded row, and the second would be frozen to nothing.
    let forms = forms();
    for (i, f) in forms.iter().enumerate() {
        assert!(
            !forms[..i].iter().any(|g| g.what == f.what),
            "{} is recorded twice; give the second form its own name",
            f.what
        );
    }
    for (i, (what, _)) in GOLDEN.iter().enumerate() {
        assert!(
            !GOLDEN[..i].iter().any(|(w, _)| w == what),
            "{what} has two rows in GOLDEN"
        );
    }
}

#[test]
fn the_numbers_a_client_interprets_are_frozen() {
    // A constant's VALUE is as much a client contract as a field's offset, and
    // nothing above pins one: the fixtures carry `code: 7` as a literal, so
    // renumbering `CODE_*` moves no recorded byte. These are the numbers an
    // embedded client turns into behavior — a status it retries, a status it
    // treats as terminal, a payload file it asks the extent node for.
    //
    // Appending is fine and is how these have always grown. Renumbering an
    // existing one is a client-facing break with no encoding to catch it.
    // By NAME, not by round trip. `from_u8(v) as u8 == v` holds for any
    // CONSISTENT renumbering, so the obvious spelling of this assertion passes
    // the very change it is here to stop — confirmed by ablation: swapping
    // `Unavailable = 5` and `AlreadyExists = 6` together with their `from_u8`
    // arms left it green. What a deployed client holds is the NAME it branches
    // on, bound to the number it was compiled with.
    assert_eq!(
        [
            StatusCode::Ok as u8,
            StatusCode::NotFound as u8,
            StatusCode::InvalidArgument as u8,
            StatusCode::FailedPrecondition as u8,
            StatusCode::Internal as u8,
            StatusCode::Unavailable as u8,
            StatusCode::AlreadyExists as u8,
            StatusCode::PermissionDenied as u8,
            StatusCode::NamespaceUnknown as u8,
        ],
        [0, 1, 2, 3, 4, 5, 6, 7, 8],
        "StatusCode numbering (crates/rpc/src/error.rs)"
    );
    // An unknown status folds to Internal rather than panicking or aliasing a
    // real one. That fold is itself a contract: it is what lets a server append
    // a status without an old client mis-branching on it.
    //
    // Pinned at 255, NOT at the next free discriminant. Pinning 9 would have
    // gone red on `Foo = 9` — an APPEND, the one change this test's own comment
    // calls legitimate — and a false red with a bare assert message is the
    // shape that teaches people to edit the test instead of reading it.
    assert_eq!(StatusCode::from_u8(255), StatusCode::Internal);
    assert_eq!(
        [
            ps::CODE_OK,
            ps::CODE_NOT_FOUND,
            ps::CODE_INVALID_ARGUMENT,
            ps::CODE_PRECONDITION,
            ps::CODE_ERROR,
            ps::CODE_VALUE_TOO_LARGE,
            ps::CODE_UNAVAILABLE,
            ps::CODE_REGION_EPOCH_STALE,
            ps::CODE_FENCED,
            ps::CODE_POSITION,
        ],
        [0, 1, 2, 3, 4, 5, 7, 8, 9, 11],
        "partition CODE_* numbering"
    );
    assert_eq!(
        [
            en::CODE_OK,
            en::CODE_NOT_FOUND,
            en::CODE_PRECONDITION,
            en::CODE_ERROR,
            en::CODE_LOCKED_BY_OTHER,
            en::CODE_EVERSION_MISMATCH,
            en::CODE_PAYLOAD_NOT_HERE,
            en::CODE_CONTENT_CORRUPT,
        ],
        [0, 1, 3, 4, 5, 6, 7, 8],
        // An embedded client interprets exactly ONE of these: `read_extent_direct`
        // branches on `!= CODE_OK` and renders the rest through
        // `code_description`. The others are pinned because the partition
        // server's `StreamClient` does branch on them and a renumbering would
        // hit both readers at once — not because the SDK reads them.
        "extent-node CODE_* numbering"
    );
    assert_eq!(
        [en::PAYLOAD_LOCATION_IN_DAT, en::PAYLOAD_LOCATION_IN_SHARD_FILE],
        [0, 1],
        "payload selector, sent by a client in every ReadReq"
    );
    assert_eq!(
        [en::READ_EXACT, en::READ_UP_TO, en::READ_TO_END],
        [0, 1, 2],
        "MSG_READ modes"
    );
    assert_eq!(
        [
            ps::BATCH_GET_FOUND,
            ps::BATCH_GET_NOT_FOUND,
            ps::BATCH_GET_ERROR,
            ps::BATCH_GET_POSITION,
            ps::BATCH_GET_DECLINED,
        ],
        [0, 1, 2, 3, 4],
        "MSG_BATCH_GET item statuses"
    );
    assert_eq!(
        [
            mgr::LEASE_CLIENT_KIND_FUSE,
            mgr::LEASE_CLIENT_KIND_IORING,
            mgr::LEASE_MODE_READ,
            mgr::LEASE_MODE_WRITE,
            mgr::LEASE_INVAL_WRITER_CLOSED,
            mgr::LEASE_INVAL_LEASE_REVOKED,
            mgr::LEASE_INVAL_META_CHANGED,
            mgr::LEASE_INVAL_WILL_REVOKE_IN,
            mgr::LEASE_MODE_STABLE,
            mgr::LEASE_MODE_REPLACE,
            mgr::LEASE_MODE_EXCLUSIVE,
        ],
        [1, 2, 1, 2, 1, 2, 3, 4, 3, 4, 5],
        "lease kinds, modes and invalidation reasons"
    );
}

#[test]
fn the_bulk_response_head_layout_is_frozen_field_by_field() {
    // The value rides as a raw tail outside the CRC, so everything a client
    // needs to split the reply from the value is in these bytes.
    let head = frame::encode_bulk_response_head(0x11121314, 0x50, 7, "bulk-message", 0x2122);
    assert_eq!(&head[0..4], &0x11121314u32.to_le_bytes(), "req_id");
    assert_eq!(head[4], 0x50, "msg_type");
    assert_eq!(head[5], frame::FLAG_RESPONSE, "flags");
    let ctrl_len = u32::from_le_bytes(head[10..14].try_into().unwrap()) as usize;
    assert_eq!(ctrl_len, 1 + "bulk-message".len(), "ctrl_len");
    let ctrl = &head[14..14 + ctrl_len];
    let (code, tail) = frame::parse_bulk_ctrl(ctrl).expect("splits");
    assert_eq!(code, 7, "code");
    assert_eq!(tail, b"bulk-message", "message");
    // `wire_len` covers the value the caller has not written yet, which is what
    // lets the receiver land it in a pooled buffer without a second read.
    let wire_len = u32::from_le_bytes(head[6..10].try_into().unwrap()) as usize;
    assert_eq!(wire_len, 4 + ctrl_len + 4 + 0x2122, "wire_len covers the value");
}
