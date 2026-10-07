//! Frozen connection bootstrap, independent of rkyv and business FrameDecoder.
//! Changing these bytes requires a separate bootstrap migration. All integers
//! are little-endian. PEER_AUTH (`peer_auth.rs`) follows it on Peer/Admin
//! connections; a client's CLIENT_AUTH follows it as a business message.
use crate::{RpcError, StatusCode};
use bytes::Bytes;
use compio::io::{AsyncReadExt, AsyncWriteExt};
use compio::BufResult;
use std::time::Duration;

pub const MSG_VERSION_HELLO: u8 = 0xF0;
// Frozen bytes; the letters predate the handshake's current name.
pub const MAGIC: [u8; 4] = *b"AUPH";
pub const BOOTSTRAP_VERSION: u16 = 1;
pub const TIMEOUT: Duration = Duration::from_secs(5);
pub const REQUEST_LEN: usize = 16;
pub const RESPONSE_PREFIX_LEN: usize = 22;
pub const MAX_MESSAGE_LEN: usize = 256;
// Frozen outer framing: req_id(4), opcode(1), flags(1), payload_len(4),
// ctrl_len(4), ctrl, crc32c(4). No value tail. Never use Frame::encode here.
const HEADER_LEN: usize = 10;
const OVERHEAD: usize = 8;
const REQUEST_ID: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Role {
    Client = 1,
    Peer = 2,
    Admin = 3,
}
impl Role {
    fn parse(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Client),
            2 => Some(Self::Peer),
            3 => Some(Self::Admin),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Service {
    Manager = 1,
    PartitionServer = 2,
    ExtentNode = 3,
}
impl Service {
    fn parse(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Manager),
            2 => Some(Self::PartitionServer),
            3 => Some(Self::ExtentNode),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Verdict {
    Ok = 0,
    WireMismatch = 1,
    ClientMismatch = 2,
    Malformed = 3,
    BootstrapMismatch = 4,
}
impl Verdict {
    fn parse(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::WireMismatch),
            2 => Some(Self::ClientMismatch),
            3 => Some(Self::Malformed),
            4 => Some(Self::BootstrapMismatch),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Hello {
    pub role: Role,
    pub wire_version: u32,
    pub client_version: u32,
}
impl Hello {
    pub fn current(role: Role) -> Self {
        Self {
            role,
            wire_version: crate::WIRE_VERSION,
            client_version: if role == Role::Client {
                crate::WIRE_VERSION
            } else {
                0
            },
        }
    }
    pub fn encode(self) -> [u8; REQUEST_LEN] {
        let mut b = [0; REQUEST_LEN];
        b[..4].copy_from_slice(&MAGIC);
        b[4..6].copy_from_slice(&BOOTSTRAP_VERSION.to_le_bytes());
        b[6] = self.role as u8;
        b[8..12].copy_from_slice(&self.wire_version.to_le_bytes());
        b[12..16].copy_from_slice(&self.client_version.to_le_bytes());
        b
    }
    fn decode(b: &[u8]) -> Result<Self, Verdict> {
        if b.len() != REQUEST_LEN || b[..4] != MAGIC || b[7] != 0 {
            return Err(Verdict::Malformed);
        }
        if u16::from_le_bytes(b[4..6].try_into().unwrap()) != BOOTSTRAP_VERSION {
            return Err(Verdict::BootstrapMismatch);
        }
        let role = Role::parse(b[6]).ok_or(Verdict::Malformed)?;
        let client_version = u32::from_le_bytes(b[12..16].try_into().unwrap());
        if role != Role::Client && client_version != 0 {
            return Err(Verdict::Malformed);
        }
        Ok(Self {
            role,
            wire_version: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            client_version,
        })
    }
    pub fn verdict(self, wire: u32, min: u32) -> Verdict {
        if self.role == Role::Client {
            if (min..=wire).contains(&self.client_version) {
                Verdict::Ok
            } else {
                Verdict::ClientMismatch
            }
        } else if self.wire_version == wire {
            Verdict::Ok
        } else {
            Verdict::WireMismatch
        }
    }
}

#[derive(Clone, Debug)]
pub struct Negotiated {
    pub role: Role,
    pub service: Service,
    pub remote_wire: u32,
    pub min_client: u32,
    pub max_client: u32,
    pub declared_version: u32,
}
impl Negotiated {
    /// Role is a protocol declaration, never an authentication credential.
    /// Existing admin/capability/identity checks still run in their handlers.
    pub fn check_opcode(&self, opcode: u8) -> Result<(), RpcError> {
        use crate::{client_hello as c, extent_rpc as e, manager_rpc as m};
        if opcode == crate::MSG_TYPE_PING {
            return Ok(());
        }
        // A connection cannot change its role or repeat either version Hello.
        if opcode == MSG_VERSION_HELLO || opcode == c::MSG_CLIENT_HELLO {
            return Err(RpcError::status(
                StatusCode::FailedPrecondition,
                "version Hello already completed",
            ));
        }
        let allowed = match (self.service, self.role) {
            (Service::Manager, Role::Client) => {
                c::is_client_surface_mgr_msg(opcode) || matches!(opcode, m::MSG_GET_CLUSTER_ID | m::MSG_GET_REGIONS)
            }
            (Service::PartitionServer, Role::Client) => c::is_client_surface_ps_msg(opcode),
            // Direct reads, plus the CLIENT_AUTH that binds a principal to them
            // when the cluster runs authz.
            (Service::ExtentNode, Role::Client) => matches!(
                opcode,
                e::MSG_READ_BYTES | e::MSG_READ_BYTES_BULK | crate::partition_rpc::MSG_CLIENT_AUTH
            ),
            (Service::Manager, Role::Peer) => {
                known_manager_opcode(opcode) && !m::is_admin_mgr_msg(opcode)
            }
            (Service::Manager, Role::Admin) => known_manager_opcode(opcode),
            (Service::PartitionServer, Role::Peer | Role::Admin) => known_ps_opcode(opcode),
            (Service::ExtentNode, Role::Peer | Role::Admin) => known_en_opcode(opcode),
        };
        if allowed {
            Ok(())
        } else {
            Err(RpcError::status(
                StatusCode::PermissionDenied,
                format!(
                    "{:?} connection cannot call {:?} opcode {opcode:#x}",
                    self.role, self.service
                ),
            ))
        }
    }
}

fn malformed(message: &str) -> RpcError {
    RpcError::status(
        StatusCode::InvalidArgument,
        format!("VERSION_HELLO: {message}"),
    )
}
fn encode_packet(ctrl: &[u8], response: bool) -> Bytes {
    encode_bootstrap(MSG_VERSION_HELLO, ctrl, response)
}
/// The frozen bootstrap framing, shared with `peer_auth`, which runs on the raw
/// stream right after this handshake.
pub(crate) fn encode_bootstrap(opcode: u8, ctrl: &[u8], response: bool) -> Bytes {
    let mut b = Vec::with_capacity(HEADER_LEN + OVERHEAD + ctrl.len());
    b.extend_from_slice(&REQUEST_ID.to_le_bytes());
    b.push(opcode);
    b.push(u8::from(response));
    b.extend_from_slice(&((ctrl.len() + OVERHEAD) as u32).to_le_bytes());
    b.extend_from_slice(&(ctrl.len() as u32).to_le_bytes());
    b.extend_from_slice(ctrl);
    b.extend_from_slice(&crc32c::crc32c(&b).to_le_bytes());
    Bytes::from(b)
}
async fn read_packet(
    reader: &mut autumn_transport::ReadHalf,
    response: bool,
    max_ctrl: usize,
) -> Result<Vec<u8>, RpcError> {
    read_bootstrap(reader, "VERSION_HELLO", MSG_VERSION_HELLO, response, max_ctrl).await
}
/// `name` only labels the error.
pub(crate) async fn read_bootstrap(
    reader: &mut autumn_transport::ReadHalf,
    name: &str,
    opcode: u8,
    response: bool,
    max_ctrl: usize,
) -> Result<Vec<u8>, RpcError> {
    let malformed = |message: &str| {
        RpcError::status(StatusCode::InvalidArgument, format!("{name}: {message}"))
    };
    let BufResult(result, header) = reader.read_exact(vec![0; HEADER_LEN]).await;
    result?;
    let length = u32::from_le_bytes(header[6..10].try_into().unwrap()) as usize;
    if u32::from_le_bytes(header[..4].try_into().unwrap()) != REQUEST_ID
        || header[4] != opcode
        || header[5] != u8::from(response)
        || length < OVERHEAD
        || length > max_ctrl + OVERHEAD
    {
        return Err(malformed("expected a bounded bootstrap frame"));
    }
    let BufResult(result, rest) = reader.read_exact(vec![0; length]).await;
    result?;
    let ctrl_len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
    if ctrl_len + OVERHEAD != length {
        return Err(malformed("invalid control length or value tail"));
    }
    let stored = u32::from_le_bytes(rest[length - 4..].try_into().unwrap());
    let mut crc_bytes = header;
    crc_bytes.extend_from_slice(&rest[..length - 4]);
    if crc32c::crc32c(&crc_bytes) != stored {
        return Err(malformed("CRC mismatch"));
    }
    Ok(rest[4..length - 4].to_vec())
}
fn encode_response(verdict: Verdict, service: Service, message: &str) -> Vec<u8> {
    let message = &message.as_bytes()[..message.len().min(MAX_MESSAGE_LEN)];
    let mut b = Vec::with_capacity(RESPONSE_PREFIX_LEN + message.len());
    b.extend_from_slice(&MAGIC);
    b.extend_from_slice(&BOOTSTRAP_VERSION.to_le_bytes());
    b.push(verdict as u8);
    b.push(service as u8);
    b.extend_from_slice(&crate::WIRE_VERSION.to_le_bytes());
    b.extend_from_slice(&crate::MIN_CLIENT_WIRE_VERSION.to_le_bytes());
    b.extend_from_slice(&crate::WIRE_VERSION.to_le_bytes());
    b.extend_from_slice(&(message.len() as u16).to_le_bytes());
    b.extend_from_slice(message);
    b
}
fn decode_response(
    b: &[u8],
    hello: Hello,
    expected: Option<Service>,
) -> Result<Negotiated, RpcError> {
    if b.len() < RESPONSE_PREFIX_LEN
        || b[..4] != MAGIC
        || u16::from_le_bytes(b[4..6].try_into().unwrap()) != BOOTSTRAP_VERSION
    {
        return Err(malformed("unknown response protocol"));
    }
    let verdict = Verdict::parse(b[6]).ok_or_else(|| malformed("unknown verdict"))?;
    let service = Service::parse(b[7]).ok_or_else(|| malformed("unknown service"))?;
    if expected.is_some_and(|s| s != service) {
        return Err(malformed("wrong target service"));
    }
    let wire = u32::from_le_bytes(b[8..12].try_into().unwrap());
    let min = u32::from_le_bytes(b[12..16].try_into().unwrap());
    let max = u32::from_le_bytes(b[16..20].try_into().unwrap());
    let n = u16::from_le_bytes(b[20..22].try_into().unwrap()) as usize;
    if n > MAX_MESSAGE_LEN || b.len() != RESPONSE_PREFIX_LEN + n || min > max || max != wire {
        return Err(malformed("invalid response bounds or length"));
    }
    if matches!(verdict, Verdict::WireMismatch | Verdict::ClientMismatch)
        || (verdict == Verdict::Ok && hello.verdict(wire, min) != Verdict::Ok)
    {
        return Err(RpcError::VersionMismatch {
            role: hello.role,
            local_wire: hello.wire_version,
            remote_wire: wire,
            client_version: hello.client_version,
            min_client: min,
            max_client: max,
            message: String::from_utf8_lossy(&b[22..]).into_owned(),
        });
    }
    if verdict != Verdict::Ok {
        return Err(malformed(&String::from_utf8_lossy(&b[22..])));
    }
    Ok(Negotiated {
        role: hello.role,
        service,
        remote_wire: wire,
        min_client: min,
        max_client: max,
        declared_version: if hello.role == Role::Client {
            hello.client_version
        } else {
            hello.wire_version
        },
    })
}

pub async fn initiate(
    reader: &mut autumn_transport::ReadHalf,
    writer: &mut autumn_transport::WriteHalf,
    hello: Hello,
    expected: Option<Service>,
) -> Result<Negotiated, RpcError> {
    compio::time::timeout(TIMEOUT, async {
        let BufResult(result, _) = writer
            .write_all(encode_packet(&hello.encode(), false))
            .await;
        result?;
        let b = read_packet(reader, true, RESPONSE_PREFIX_LEN + MAX_MESSAGE_LEN).await?;
        decode_response(&b, hello, expected)
    })
    .await
    .map_err(|_| RpcError::Timeout(TIMEOUT))?
}
/// `peer` only labels the WARN a version refusal logs, so an operator can find
/// the stale binary from the refusing side; a malformed or missing Hello (port
/// scanners, health probes) is not logged here.
pub async fn accept(
    reader: &mut autumn_transport::ReadHalf,
    writer: &mut autumn_transport::WriteHalf,
    service: Service,
    peer: &str,
) -> Result<Negotiated, RpcError> {
    compio::time::timeout(TIMEOUT, async {
        let parsed = match read_packet(reader, false, REQUEST_LEN).await {
            Ok(b) => Hello::decode(&b),
            Err(e) => {
                let BufResult(_, _) = writer
                    .write_all(encode_packet(
                        &encode_response(Verdict::Malformed, service, &e.to_string()),
                        true,
                    ))
                    .await;
                return Err(e);
            }
        };
        let verdict = match parsed {
            Ok(h) => h.verdict(crate::WIRE_VERSION, crate::MIN_CLIENT_WIRE_VERSION),
            Err(v) => v,
        };
        let message = if verdict == Verdict::Ok {
            String::new()
        } else {
            format!(
                "{verdict:?}: server wire={}, clients=[{},{}]",
                crate::WIRE_VERSION,
                crate::MIN_CLIENT_WIRE_VERSION,
                crate::WIRE_VERSION
            )
        };
        let BufResult(result, _) = writer
            .write_all(encode_packet(
                &encode_response(verdict, service, &message),
                true,
            ))
            .await;
        result?;
        if matches!(verdict, Verdict::WireMismatch | Verdict::ClientMismatch) {
            tracing::warn!(
                peer,
                ?service,
                request = ?parsed,
                server_wire = crate::WIRE_VERSION,
                min_client = crate::MIN_CLIENT_WIRE_VERSION,
                "VERSION_HELLO refused a version mismatch"
            );
        }
        if verdict != Verdict::Ok {
            return Err(RpcError::status(
                StatusCode::FailedPrecondition,
                format!("VERSION_HELLO {message}; request={parsed:?}"),
            ));
        }
        let hello = parsed.map_err(|_| malformed("invalid request"))?;
        Ok(Negotiated {
            role: hello.role,
            service,
            remote_wire: hello.wire_version,
            min_client: crate::MIN_CLIENT_WIRE_VERSION,
            max_client: crate::WIRE_VERSION,
            declared_version: if hello.role == Role::Client {
                hello.client_version
            } else {
                hello.wire_version
            },
        })
    })
    .await
    .map_err(|_| RpcError::Timeout(TIMEOUT))?
}

fn known_manager_opcode(opcode: u8) -> bool {
    use crate::manager_rpc::*;
    matches!(
        opcode,
        MSG_STATUS
            | MSG_ACQUIRE_OWNER_LOCK
            | MSG_REGISTER_NODE
            | MSG_CREATE_STREAM
            | MSG_STREAM_INFO
            | MSG_EXTENT_INFO
            | MSG_NODES_INFO
            | MSG_CHECK_COMMIT_LENGTH
            | MSG_STREAM_ALLOC_EXTENT
            | MSG_STREAM_PUNCH_HOLES
            | MSG_TRUNCATE
            | MSG_MULTI_MODIFY_SPLIT
            | MSG_REGISTER_PS
            | MSG_UPSERT_PARTITION
            | MSG_GET_REGIONS
            | MSG_GET_CLIENT_REGIONS
            | MSG_VALIDATE_RECOVERY
            | MSG_HEARTBEAT_PS
            | MSG_REGISTER_PARTITION_ADDR
            | MSG_RECONCILE_EXTENTS
            | MSG_UPDATE_STREAM_EC
            | MSG_GET_POLICY_CANDIDATES
            | MSG_REPORT_PARTITION_LOAD
            | MSG_MERGE_PARTITIONS
            | MSG_REPORT_DISK_FAILURE
            | MSG_FORCE_EC_CONVERT
            | MSG_GET_PARTITION_DETAIL
            | MSG_GET_POLICY_KIND_NAMES
            | MSG_LIST_NODE_STATES
            | MSG_EXTENT_HEALTH_REPORT
            | MSG_EXTENT_HEALTH_SUMMARY
            | MSG_REMOVE_MEMBER
            | MSG_LIST_EC_INFLIGHT_MARKERS
            | MSG_FENCE_NODE
            | MSG_SET_NODE_MAINTENANCE
            | MSG_CLEAR_NODE_OVERRIDE
            | MSG_REMOVE_NODE
            | MSG_RECOVERY_STATS
            | MSG_QUERY_AUDIT_LOG
            | MSG_GET_CLUSTER_ID
            | MSG_ACQUIRE_LEASE
            | MSG_RELEASE_LEASE
            | MSG_HEARTBEAT_LEASE
            | MSG_POLL_INVALIDATIONS
            | MSG_REPORT_CORRUPT_REPLICA
            | MSG_CLUSTER_DF
            | MSG_GET_CLUSTER_OVERVIEW
            | MSG_MINT_TOKEN
            | MSG_GET_AUTHZ_CONFIG
            | MSG_TENANT_CREATE
            | MSG_TENANT_DELETE
            | MSG_ALLOC_INODES
            | MSG_AUTOPOLICY_GET
            | MSG_AUTOPOLICY_SET
            | MSG_REBALANCE_REGIONS
            | MSG_NAMESPACE_CREATE
            | MSG_NAMESPACE_DELETE
            | MSG_NAMESPACE_LIST
            | MSG_PRINCIPAL_LIST
            | MSG_NAMESPACE_SET_PRESPLIT
            | MSG_OP_SUBMIT
            | MSG_OP_QUERY
            | MSG_OP_HISTORY
    )
}

fn known_ps_opcode(opcode: u8) -> bool {
    use crate::partition_rpc::*;
    matches!(
        opcode,
        MSG_PUT
            | MSG_DELETE
            | MSG_HEAD
            | MSG_RANGE
            | MSG_SPLIT_PART
            | MSG_MAINTENANCE
            | MSG_GET_DISCARDS
            | MSG_MERGE_PART
            | MSG_MERGE_FREEZE
            | MSG_GET_BULK
            | MSG_PUT_BULK
            | MSG_BATCH_PUT
            | MSG_BATCH_PUT_BULK
            | MSG_BATCH_GET_BULK
            | MSG_BATCH_DELETE
            | MSG_COMPARE_PUT
            | MSG_COMPARE_WRITE
            | MSG_GET_REDIRECT
            | MSG_CLIENT_AUTH
            | MSG_ROLL_TAILS
            | MSG_GET_REDIRECT_MANY
            | MSG_DIAG_TRACE_KEY
            | MSG_DIAG_PARTITION_VP
    )
}

fn known_en_opcode(opcode: u8) -> bool {
    use crate::extent_rpc::*;
    matches!(
        opcode,
        MSG_APPEND
            | MSG_READ_BYTES
            | MSG_COMMIT_LENGTH
            | MSG_ALLOC_EXTENT
            | MSG_DF
            | MSG_REQUIRE_RECOVERY
            | MSG_RE_AVALI
            | MSG_COPY_EXTENT
            | MSG_CONVERT_TO_EC
            | MSG_WRITE_SHARD
            | MSG_DELETE_EXTENT
            | MSG_PROBE_EXTENT
            | MSG_READ_BYTES_BULK
            | MSG_FENCE_EXTENT
            | MSG_SCRUB_EXTENTS
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::io::AsyncRead;

    #[test]
    fn bootstrap_bytes_are_frozen_independently_of_business_framing() {
        let hello = Hello {
            role: Role::Peer,
            wire_version: 51,
            client_version: 0,
        };
        assert_eq!(
            &encode_packet(&hello.encode(), false)[..],
            &[
                1, 0, 0, 0, 240, 0, 24, 0, 0, 0, 16, 0, 0, 0, 65, 85, 80, 72, 1, 0, 2, 0, 51, 0, 0,
                0, 0, 0, 0, 0, 32, 156, 106, 174
            ]
        );
        // Explicit numbers freeze the bootstrap independently of future wire bumps.
        let ctrl = [
            65, 85, 80, 72, 1, 0, 0, 1, 51, 0, 0, 0, 43, 0, 0, 0, 51, 0, 0, 0, 0, 0,
        ];
        assert_eq!(
            &encode_packet(&ctrl, true)[..],
            &[
                1, 0, 0, 0, 240, 1, 30, 0, 0, 0, 22, 0, 0, 0, 65, 85, 80, 72, 1, 0, 0, 1, 51, 0, 0,
                0, 43, 0, 0, 0, 51, 0, 0, 0, 0, 0, 2, 108, 241, 191
            ]
        );
    }

    #[test]
    fn client_interval_and_peer_equality_are_distinct() {
        for role in [Role::Peer, Role::Admin] {
            for wire in [42, 43, 50, 51, 52] {
                assert_eq!(
                    Hello {
                        role,
                        wire_version: wire,
                        client_version: 0
                    }
                    .verdict(51, 43),
                    if wire == 51 {
                        Verdict::Ok
                    } else {
                        Verdict::WireMismatch
                    }
                );
            }
        }
        for version in [42, 43, 47, 51, 52] {
            assert_eq!(
                Hello {
                    role: Role::Client,
                    wire_version: 51,
                    client_version: version
                }
                .verdict(51, 43),
                if (43..=51).contains(&version) {
                    Verdict::Ok
                } else {
                    Verdict::ClientMismatch
                }
            );
        }
    }

    #[test]
    fn malformed_hello_and_false_success_are_rejected() {
        let valid = Hello::current(Role::Peer).encode();
        for (offset, value, verdict) in [
            (0, 0, Verdict::Malformed),
            (4, 2, Verdict::BootstrapMismatch),
            (6, 9, Verdict::Malformed),
            (7, 1, Verdict::Malformed),
            (12, 1, Verdict::Malformed),
        ] {
            let mut b = valid;
            b[offset] = value;
            assert_eq!(Hello::decode(&b).unwrap_err(), verdict);
        }
        assert_eq!(Hello::decode(&valid[..15]).unwrap_err(), Verdict::Malformed);
        let response = encode_response(Verdict::Ok, Service::Manager, "");
        let ahead = Hello {
            role: Role::Peer,
            wire_version: crate::WIRE_VERSION + 1,
            client_version: 0,
        };
        assert!(matches!(
            decode_response(&response, ahead, None),
            Err(RpcError::VersionMismatch { .. })
        ));
        assert!(decode_response(
            &response,
            Hello::current(Role::Peer),
            Some(Service::ExtentNode)
        )
        .is_err());
    }

    #[test]
    fn role_surface_is_checked_before_payload_decoding() {
        let mut n = Negotiated {
            role: Role::Client,
            service: Service::Manager,
            remote_wire: 51,
            min_client: 43,
            max_client: 51,
            declared_version: 51,
        };
        assert!(n.check_opcode(crate::manager_rpc::MSG_REGISTER_PS).is_err());
        assert!(n
            .check_opcode(crate::manager_rpc::MSG_CREATE_STREAM)
            .is_err());
        assert!(n.check_opcode(crate::manager_rpc::MSG_GET_REGIONS).is_ok());
        n.role = Role::Peer;
        assert!(n.check_opcode(crate::manager_rpc::MSG_REGISTER_PS).is_ok());
        assert!(n
            .check_opcode(crate::manager_rpc::MSG_CREATE_STREAM)
            .is_err());
        // Account and namespace mutations are operator-only.
        for op in [
            crate::manager_rpc::MSG_TENANT_CREATE,
            crate::manager_rpc::MSG_NAMESPACE_CREATE,
            crate::manager_rpc::MSG_NAMESPACE_SET_PRESPLIT,
        ] {
            assert!(n.check_opcode(op).is_err(), "Peer sent {op:#x}");
        }
        n.role = Role::Admin;
        assert!(n
            .check_opcode(crate::manager_rpc::MSG_CREATE_STREAM)
            .is_ok());
        assert!(n
            .check_opcode(crate::manager_rpc::MSG_TENANT_CREATE)
            .is_ok());
        // The retired raw merge txn (no freeze drain) is refused even to Admin.
        assert!(n.check_opcode(0x34).is_err());

        // A Client on an EN: direct reads and the CLIENT_AUTH that binds them.
        let mut en = Negotiated {
            role: Role::Client,
            service: Service::ExtentNode,
            ..n.clone()
        };
        for op in [
            crate::extent_rpc::MSG_READ_BYTES,
            crate::extent_rpc::MSG_READ_BYTES_BULK,
            crate::partition_rpc::MSG_CLIENT_AUTH,
        ] {
            assert!(en.check_opcode(op).is_ok(), "Client refused {op:#x}");
        }
        assert!(en.check_opcode(crate::extent_rpc::MSG_APPEND).is_err());
        assert!(en.check_opcode(crate::extent_rpc::MSG_DELETE_EXTENT).is_err());
        // CLIENT_AUTH is a client message; the EN has no handler for it from a member.
        en.role = Role::Peer;
        assert!(en.check_opcode(crate::partition_rpc::MSG_CLIENT_AUTH).is_err());
        for role in [Role::Client, Role::Peer, Role::Admin] {
            n.role = role;
            assert!(n.check_opcode(MSG_VERSION_HELLO).is_err());
            assert!(n
                .check_opcode(crate::client_hello::MSG_CLIENT_HELLO)
                .is_err());
            assert!(n.check_opcode(0xfe).is_err());
            assert!(n.check_opcode(0x4a).is_err());
            assert!(n.check_opcode(0x4b).is_err());
        }
    }

    async fn socket_pair() -> (autumn_transport::Conn, autumn_transport::Conn) {
        let listener = compio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = compio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (
            autumn_transport::Conn::Tcp(client),
            autumn_transport::Conn::Tcp(server),
        )
    }

    #[compio::test]
    async fn real_sockets_admit_clients_and_refuse_mismatched_peers() {
        for (role, version) in [
            (Role::Client, crate::MIN_CLIENT_WIRE_VERSION),
            (Role::Client, crate::WIRE_VERSION),
            (Role::Client, crate::WIRE_VERSION + 1),
            (Role::Peer, crate::WIRE_VERSION - 1),
            (Role::Peer, crate::WIRE_VERSION),
            (Role::Admin, crate::WIRE_VERSION + 1),
        ] {
            let (client, server) = socket_pair().await;
            let (mut rd, mut wr) = client.into_split();
            let server = compio::runtime::spawn(async move {
                let (mut rd, mut wr) = server.into_split();
                accept(&mut rd, &mut wr, Service::ExtentNode, "test").await
            });
            let h = Hello {
                role,
                wire_version: version,
                client_version: if role == Role::Client { version } else { 0 },
            };
            let result = initiate(&mut rd, &mut wr, h, Some(Service::ExtentNode)).await;
            if h.verdict(crate::WIRE_VERSION, crate::MIN_CLIENT_WIRE_VERSION) == Verdict::Ok {
                assert!(result.is_ok());
                assert!(server.await.unwrap().is_ok());
            } else {
                assert!(
                    matches!(result, Err(RpcError::VersionMismatch { .. })),
                    "{result:?}"
                );
                assert!(server.await.unwrap().is_err());
                let BufResult(n, _) = rd.read(vec![0; 1]).await;
                assert_eq!(n.unwrap(), 0, "a refused connection is closed");
            }
        }
    }

    #[compio::test]
    async fn first_message_is_bounded_and_never_falls_back_to_business_decoding() {
        let valid = encode_packet(&Hello::current(Role::Peer).encode(), false);
        for kind in 0..6 {
            let (client, server) = socket_pair().await;
            let (mut rd, mut wr) = client.into_split();
            let task = compio::runtime::spawn(async move {
                let (mut rd, mut wr) = server.into_split();
                accept(&mut rd, &mut wr, Service::Manager, "test").await
            });
            let mut b = valid.to_vec();
            match kind {
                0 => b[4] = crate::manager_rpc::MSG_REGISTER_PS, // no Hello
                1 => b[4] = crate::client_hello::MSG_CLIENT_HELLO, // old Hello
                2 => b[33] ^= 1,                                 // bad CRC
                3 => b[6..10].copy_from_slice(&u32::MAX.to_le_bytes()), // bounds before allocation
                4 => b[10] = 17,                                 // control/value mismatch
                _ => b[5] = 1,                                   // response sent to server
            }
            wr.write_all(b).await.0.unwrap();
            let response = read_packet(&mut rd, true, RESPONSE_PREFIX_LEN + MAX_MESSAGE_LEN)
                .await
                .unwrap();
            assert_eq!(response[6], Verdict::Malformed as u8);
            assert!(task.await.unwrap().is_err());
        }
    }
}
