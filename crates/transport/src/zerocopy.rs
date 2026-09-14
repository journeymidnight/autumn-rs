//! Owned TCP sends: send completion and buffer release are separate events.
use bytes::{Buf, Bytes};
use compio::io::{AsyncWrite, AsyncWriteExt, AsyncWriteZerocopy};
use compio::BufResult;
use std::io;

pub(crate) async fn write_all<W>(writer: &mut W, mut bufs: Vec<Bytes>) -> io::Result<()>
where
    W: AsyncWrite + AsyncWriteZerocopy,
{
    bufs.retain(|buf| !buf.is_empty());
    while !bufs.is_empty() {
        let BufResult(result, ready) = writer.write_zerocopy_vectored(bufs).await;
        // Even an error may leave a completion notification outstanding. Do not
        // inspect/reuse/drop the owned buffers until the kernel releases them.
        // If this future is cancelled, compio's operation owns them until its
        // final CQE; no borrowed pointers or early pool returns are involved.
        bufs = ready.await;
        match result {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(mut written) => {
                let mut consumed = 0;
                for buf in &mut bufs {
                    if written < buf.len() {
                        buf.advance(written);
                        written = 0;
                        break;
                    }
                    written -= buf.len();
                    consumed += 1;
                }
                debug_assert_eq!(written, 0);
                bufs.drain(..consumed);
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) if unsupported(&e) => {
                // Only retry the unsent suffix, after buffer release. Ordinary
                // connection errors must reach the RPC writer and close it.
                return writer.write_vectored_all(bufs).await.0;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn unsupported(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::Unsupported {
        return true;
    }
    #[cfg(target_os = "linux")]
    if matches!(
        error.raw_os_error(),
        Some(libc::EOPNOTSUPP | libc::ENOSYS | libc::ENOBUFS)
    ) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::buf::{IoBuf, IoVectoredBuf};
    use futures::{channel::oneshot, FutureExt};
    use std::{cell::Cell, collections::VecDeque, future::Future, pin::Pin, rc::Rc};

    struct Writer {
        outcomes: VecDeque<io::Result<usize>>,
        sent: Vec<u8>,
        ordinary: usize,
        release: Option<oneshot::Receiver<()>>,
        released: Rc<Cell<bool>>,
    }

    impl Writer {
        fn new(outcomes: Vec<io::Result<usize>>) -> Self {
            Self {
                outcomes: outcomes.into(),
                sent: vec![],
                ordinary: 0,
                release: None,
                released: Rc::new(Cell::new(true)),
            }
        }
    }

    impl AsyncWrite for Writer {
        async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
            assert!(self.released.get(), "fallback ran before buffer release");
            self.ordinary += 1;
            self.sent.extend_from_slice(buf.as_init());
            BufResult(Ok(buf.buf_len()), buf)
        }
        async fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        async fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl AsyncWriteZerocopy for Writer {
        type BufferReadyFuture<T: IoBuf> = std::future::Ready<T>;
        type VectoredBufferReadyFuture<T: IoVectoredBuf> = Pin<Box<dyn Future<Output = T>>>;
        async fn write_zerocopy<T: IoBuf>(
            &mut self,
            _: T,
        ) -> BufResult<usize, Self::BufferReadyFuture<T>> {
            unreachable!()
        }
        async fn write_zerocopy_vectored<T: IoVectoredBuf>(
            &mut self,
            buf: T,
        ) -> BufResult<usize, Self::VectoredBufferReadyFuture<T>> {
            assert!(self.released.get(), "next send ran before buffer release");
            let result = self.outcomes.pop_front().expect("unexpected send");
            if let Ok(mut n) = result {
                for part in buf.iter_slice() {
                    let bytes = part.as_init();
                    let take = n.min(bytes.len());
                    self.sent.extend_from_slice(&bytes[..take]);
                    n -= take;
                }
                assert_eq!(n, 0);
            }
            let release = self.release.take();
            let released = self.released.clone();
            released.set(false);
            let ready: Self::VectoredBufferReadyFuture<T> = Box::pin(async move {
                if let Some(rx) = release {
                    rx.await.unwrap();
                }
                released.set(true);
                buf
            });
            BufResult(result, ready)
        }
    }

    fn parts() -> Vec<Bytes> {
        vec![
            Bytes::new(),
            Bytes::from_static(b"header"),
            Bytes::from_static(b"value"),
            Bytes::new(),
        ]
    }

    #[test]
    fn partial_send_and_fallback_preserve_exact_suffix() {
        futures::executor::block_on(async {
            let mut writer =
                Writer::new(vec![Ok(2), Ok(5), Err(io::ErrorKind::Unsupported.into())]);
            write_all(&mut writer, parts()).await.unwrap();
            assert_eq!(writer.sent, b"headervalue");
            assert!(writer.ordinary > 0);
        });
    }

    #[test]
    fn completion_is_required_before_fallback() {
        futures::executor::block_on(async {
            let mut writer = Writer::new(vec![Err(io::ErrorKind::Unsupported.into())]);
            let (tx, rx) = oneshot::channel();
            writer.release = Some(rx);
            let mut send = Box::pin(write_all(&mut writer, parts()));
            assert!(send.as_mut().now_or_never().is_none());
            tx.send(()).unwrap();
            send.await.unwrap();
            assert_eq!(writer.sent, b"headervalue");
        });
    }

    #[test]
    fn zero_write_and_connection_errors_do_not_retry() {
        futures::executor::block_on(async {
            for (outcome, expected) in [
                (Ok(0), io::ErrorKind::WriteZero),
                (
                    Err(io::ErrorKind::BrokenPipe.into()),
                    io::ErrorKind::BrokenPipe,
                ),
            ] {
                let mut writer = Writer::new(vec![Ok(6), outcome]);
                assert_eq!(
                    write_all(&mut writer, parts()).await.unwrap_err().kind(),
                    expected
                );
                assert_eq!(writer.sent, b"header");
                assert_eq!(writer.ordinary, 0);
            }
        });
    }
}
