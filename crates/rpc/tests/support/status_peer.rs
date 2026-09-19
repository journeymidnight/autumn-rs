//! A real TCP peer for testing connection lifetime across replies and failures.
use autumn_rpc::{Frame, FrameDecoder, RpcError, StatusCode};
use bytes::Bytes;
use compio::io::{AsyncRead, AsyncWriteExt};
use std::{cell::Cell, rc::Rc};

pub const STATUS: u8 = 0xa0;
pub const ECHO: u8 = 0xa1;
pub const HANG: u8 = 0xa2;
pub const CLOSE: u8 = 0xa3;
pub const BAD_CRC: u8 = 0xa4;
/// What `start_refusing_hello` sends back. Distinctive so a test can assert
/// the SERVER's own words survived all the way to the caller — flattening them
/// is the failure this text exists to catch.
#[allow(dead_code)]
pub const HELLO_REFUSAL: &str = "wire-version mismatch: rebuild-me-from-the-cluster-commit";
pub const STATUSES: [StatusCode; 9] = [
    StatusCode::Ok,
    StatusCode::NotFound,
    StatusCode::InvalidArgument,
    StatusCode::FailedPrecondition,
    StatusCode::Internal,
    StatusCode::Unavailable,
    StatusCode::AlreadyExists,
    StatusCode::PermissionDenied,
    StatusCode::NamespaceUnknown,
];

pub struct Peer {
    pub addr: String,
    pub accepts: Rc<Cell<usize>>,
    /// How many `MSG_CLIENT_HELLO` frames this peer answered. The SDK sends
    /// one on every connection it opens, so a test can assert the handshake
    /// really happened rather than assuming it.
    ///
    /// `allow(dead_code)`: this file is `#[path]`-included as a module by
    /// several test crates, each compiling its own copy, and only
    /// `autumn-client`'s reads this one.
    #[allow(dead_code)]
    pub hellos: Rc<Cell<usize>>,
    _task: compio::runtime::JoinHandle<()>,
}

pub enum Reply {
    Frame(Frame),
    Hang,
    Close,
    BadCrc(Frame),
}

pub fn respond(frame: Frame) -> Reply {
    match frame.msg_type {
        STATUS => Reply::Frame(Frame::error(
            frame.req_id,
            frame.msg_type,
            RpcError::encode_status(StatusCode::from_u8(frame.payload[0]), "refused"),
        )),
        HANG => Reply::Hang,
        CLOSE => Reply::Close,
        BAD_CRC => Reply::BadCrc(Frame::response(frame.req_id, frame.msg_type, Bytes::new())),
        _ => Reply::Frame(Frame::response(
            frame.req_id,
            frame.msg_type,
            Bytes::from_static(b"ok"),
        )),
    }
}

impl Peer {
    pub async fn start(handler: impl Fn(Frame) -> Reply + 'static) -> Self {
        Self::start_inner(handler, 0).await
    }

    /// A peer that REFUSES every `MSG_CLIENT_HELLO` with the status a real
    /// server uses for an out-of-window client. There is no way to build a
    /// client that reports a wrong version — the constant is compiled in — so
    /// the refusal has to come from the peer.
    #[allow(dead_code)] // only autumn-client's copy of this module uses it
    pub async fn start_refusing_hello(handler: impl Fn(Frame) -> Reply + 'static) -> Self {
        Self::start_inner(handler, usize::MAX).await
    }

    /// Refuses the first `n` hellos, then admits — a cluster being upgraded
    /// past a client that was running ahead of it. A refusal must not latch.
    #[allow(dead_code)]
    pub async fn start_refusing_first_hellos(
        n: usize,
        handler: impl Fn(Frame) -> Reply + 'static,
    ) -> Self {
        Self::start_inner(handler, n).await
    }

    async fn start_inner(
        handler: impl Fn(Frame) -> Reply + 'static,
        refuse_hellos: usize,
    ) -> Self {
        let listener = compio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepts = Rc::new(Cell::new(0));
        let count = accepts.clone();
        let hellos = Rc::new(Cell::new(0));
        let hello_count = hellos.clone();
        let handler = Rc::new(handler);
        let task = compio::runtime::spawn(async move {
            let mut connections = Vec::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                count.set(count.get() + 1);
                let handler = handler.clone();
                let hello_count = hello_count.clone();
                let refuse_hellos = refuse_hellos;
                connections.push(compio::runtime::spawn(async move {
                    let mut decoder = FrameDecoder::new();
                    loop {
                        let compio::BufResult(n, buf) = socket.read(vec![0; 8192]).await;
                        let Ok(n) = n else { return };
                        if n == 0 {
                            return;
                        }
                        decoder.feed(&buf[..n]);
                        while let Some(frame) = decoder.try_decode().unwrap() {
                            // Answered HERE, ahead of the handler, so every
                            // mock keeps its strict assertions about the first
                            // frame it cares about. A real server gates this
                            // in its own connection layer for the same reason:
                            // the handshake is not part of any service.
                            if frame.msg_type == autumn_rpc::client_hello::MSG_CLIENT_HELLO {
                                hello_count.set(hello_count.get() + 1);
                                let resp = if hello_count.get() <= refuse_hellos {
                                    Frame::error(
                                        frame.req_id,
                                        frame.msg_type,
                                        RpcError::encode_status(
                                            StatusCode::FailedPrecondition,
                                            HELLO_REFUSAL,
                                        ),
                                    )
                                    .encode()
                                } else {
                                    Frame::response(
                                    frame.req_id,
                                    frame.msg_type,
                                    Bytes::copy_from_slice(
                                        &autumn_rpc::client_hello::encode_hello_resp(
                                            autumn_rpc::WIRE_VERSION,
                                            autumn_rpc::MIN_CLIENT_WIRE_VERSION,
                                        ),
                                    ),
                                )
                                .encode()
                                };
                                if socket.write_all(resp).await.0.is_err() {
                                    return;
                                }
                                continue;
                            }
                            let bytes = match handler(frame) {
                                Reply::Close => return,
                                Reply::Hang => continue,
                                Reply::Frame(f) => f.encode(),
                                Reply::BadCrc(f) => {
                                    let mut b = f.encode().to_vec();
                                    // CRC follows the header, ctrl length and ctrl bytes.
                                    let crc = autumn_rpc::frame::HEADER_LEN + 4 + f.payload.len();
                                    b[crc] ^= 1;
                                    Bytes::from(b)
                                }
                            };
                            if socket.write_all(bytes).await.0.is_err() {
                                return;
                            }
                        }
                    }
                }));
            }
        });
        Self {
            addr,
            accepts,
            hellos,
            _task: task,
        }
    }
}
