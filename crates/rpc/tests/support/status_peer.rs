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
        let listener = compio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepts = Rc::new(Cell::new(0));
        let count = accepts.clone();
        let handler = Rc::new(handler);
        let task = compio::runtime::spawn(async move {
            let mut connections = Vec::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                count.set(count.get() + 1);
                let handler = handler.clone();
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
            _task: task,
        }
    }
}
