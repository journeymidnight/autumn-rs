//! Append writes that bypass the page cache (on by default in the
//! `autumn-extent-node` binary; `--no-direct-io` turns it off). Linux only: the
//! module is compiled only there, and a node configured for it elsewhere refuses
//! to start.
//!
//! An extent's length after a restart is the `.dat` file's size, and the
//! commit protocol takes that length as the node's committed end — so the
//! file can never be padded to an O_DIRECT boundary: padding would be read
//! back as data and silently diverge the replicas. The aligned part of a
//! burst goes around the page cache instead, and only the sub-block tail
//! goes through it:
//!
//! ```text
//!   a0 = floor(start)          b = floor(end)
//!   |  head  |  payload ......................  | tail |
//!   [a0,start) read back from the page cache, copied in front of the payload
//!   [a0, b)    one aligned O_DIRECT write (in DIRECT_IO_CHUNK_BYTES pieces)
//!   [b, end)   buffered write, issued only after the direct write completed
//! ```
//!
//! followed by the burst's usual single `sync_data`. Two orders are wrong and
//! stay wrong: a buffered head with an O_DIRECT body (the direct write can
//! reach the disk while the head is still dirty, so a crash leaves zeros
//! inside the file size) and the tail before the body (the file size covers a
//! range the body has not written yet).
//!
//! The head bytes are already durable — the previous burst's `sync_data`
//! covered them — so rewriting them with the same content is harmless.
//!
//! Bursts below `DIRECT_IO_MIN_BYTES` stay buffered: the tail rewrite is a
//! second serial write, and small bursts have no copy cost worth saving.
//! Measured trade-offs (a large CPU saving, a small write-throughput loss and
//! slower read-after-write in a cluster) are in the crate CLAUDE.md, "Direct
//! I/O for large bursts".

use bytes::Bytes;
use compio::buf::IoBuf;
use compio::fs::{File as CompioFile, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio::BufResult;
use std::cell::RefCell;

/// Offset, length and memory alignment of every direct write. 4 KiB covers
/// both 512 B and 4 KiB logical-block devices.
pub(crate) const DIRECT_IO_ALIGN: u64 = 4096;

/// Smallest burst that takes the direct path.
pub(crate) const DIRECT_IO_MIN_BYTES: u64 = 1 << 20;

/// Largest single direct write. A burst can hold many appends, so the bounce
/// buffer is filled and written in pieces of this size instead of being sized
/// to the burst.
const DIRECT_IO_CHUNK_BYTES: usize = 8 << 20;

/// Bounce buffers kept per shard thread for reuse; more concurrent bursts
/// than this allocate and free their own.
const POOLED_CHUNKS: usize = 4;

thread_local! {
    static CHUNK_POOL: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
thread_local! {
    /// Bytes this thread wrote through an O_DIRECT descriptor, so a test can
    /// tell the direct path from a buffered write with the same result.
    pub(crate) static DIRECT_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// A `DIRECT_IO_ALIGN`-aligned window of a heap buffer, handed to the kernel
/// as the write source. The alignment comes from offsetting into an
/// over-allocated `Vec`, whose pointer never moves while it is owned.
struct AlignedChunk {
    raw: Vec<u8>,
    start: usize,
    len: usize,
}

impl AlignedChunk {
    fn take() -> Self {
        let raw = CHUNK_POOL
            .with(|p| p.borrow_mut().pop())
            .unwrap_or_else(|| vec![0u8; DIRECT_IO_CHUNK_BYTES + DIRECT_IO_ALIGN as usize]);
        let start = raw.as_ptr().align_offset(DIRECT_IO_ALIGN as usize);
        Self { raw, start, len: 0 }
    }

    fn window(&mut self, n: usize) -> &mut [u8] {
        self.len = n;
        &mut self.raw[self.start..self.start + n]
    }

    fn give_back(self) {
        CHUNK_POOL.with(|p| {
            let mut p = p.borrow_mut();
            if p.len() < POOLED_CHUNKS {
                p.push(self.raw);
            }
        });
    }
}

impl IoBuf for AlignedChunk {
    fn as_init(&self) -> &[u8] {
        &self.raw[self.start..self.start + self.len]
    }
}

/// Walks a burst's payload buffers in order.
struct PayloadCursor {
    bufs: Vec<Bytes>,
    idx: usize,
    off: usize,
}

impl PayloadCursor {
    fn copy_into(&mut self, dst: &mut [u8]) {
        let mut at = 0;
        while at < dst.len() {
            let src = &self.bufs[self.idx][self.off..];
            let n = src.len().min(dst.len() - at);
            dst[at..at + n].copy_from_slice(&src[..n]);
            at += n;
            self.off += n;
            if self.off == self.bufs[self.idx].len() {
                self.idx += 1;
                self.off = 0;
            }
        }
    }

    fn rest(self) -> Vec<Bytes> {
        let mut out = Vec::new();
        for (i, b) in self.bufs.into_iter().enumerate().skip(self.idx) {
            let from = if i == self.idx { self.off } else { 0 };
            if from < b.len() {
                out.push(Bytes::slice(&b, from..));
            }
        }
        out
    }
}

/// A second descriptor for the SAME inode as `f`, opened O_DIRECT. Going
/// through `/proc/self/fd` rather than the path means it can never name a
/// different file than the one the caller holds, whatever renamed over the
/// path in between.
async fn reopen_direct(f: &CompioFile) -> std::io::Result<CompioFile> {
    use std::os::fd::AsRawFd;
    OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(format!("/proc/self/fd/{}", f.as_raw_fd()))
        .await
}

/// Write one burst (`bufs` concatenated) at `at`: aligned body O_DIRECT, head
/// and tail as described in the module docs. The caller still owns the
/// `sync_data`. `_all` semantics: every byte lands or an error is returned.
pub(crate) async fn write_burst_direct(
    f: &CompioFile,
    bufs: Vec<Bytes>,
    at: u64,
) -> std::io::Result<()> {
    let total: u64 = bufs.iter().map(|b| b.len() as u64).sum();
    let end = at + total;
    let a0 = at / DIRECT_IO_ALIGN * DIRECT_IO_ALIGN;
    let b = end / DIRECT_IO_ALIGN * DIRECT_IO_ALIGN;
    let mut src = PayloadCursor {
        bufs,
        idx: 0,
        off: 0,
    };
    if b > a0 {
        let dio = reopen_direct(f).await?;
        let mut head = (at - a0) as usize;
        let mut w: &CompioFile = &dio;
        let mut chunk = AlignedChunk::take();
        let mut off = a0;
        while off < b {
            let n = (b - off).min(DIRECT_IO_CHUNK_BYTES as u64) as usize;
            let mut filled = 0;
            if head > 0 {
                let BufResult(r, prefix) = f.read_exact_at(vec![0u8; head], a0).await;
                r?;
                chunk.window(n)[..head].copy_from_slice(&prefix);
                filled = head;
                head = 0;
            }
            src.copy_into(&mut chunk.window(n)[filled..]);
            let BufResult(r, back) = w.write_all_at(chunk, off).await;
            chunk = back;
            r?;
            #[cfg(test)]
            DIRECT_BYTES.with(|c| c.set(c.get() + n as u64));
            off += n as u64;
        }
        chunk.give_back();
    }
    // A burst inside one block never took the direct path, so all of it is
    // "tail" and starts at `at`.
    let tail_at = if b > a0 { b } else { at };
    let tail = src.rest();
    if !tail.is_empty() {
        crate::extent_node::write_vectored_all_at_chunked(f, tail, tail_at).await?;
    }
    Ok(())
}

/// Startup check for one data directory: open its `disk_id` sentinel (written
/// by `autumn-op format`, read by every EN start) read-only with O_DIRECT. A
/// filesystem without O_DIRECT refuses the open with EINVAL (tmpfs before
/// Linux 6.6). Nothing is written, so a full or failing disk cannot be
/// mistaken for missing O_DIRECT. Not checked: the 4 KiB write alignment (only
/// a device with logical blocks above 4 KiB would refuse it, and it would do
/// so as an EINVAL on the first large append, never as bad bytes), and a
/// filesystem that takes O_DIRECT but quietly buffers it.
pub(crate) async fn check(dir: &std::path::Path) -> std::io::Result<()> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(dir.join("disk_id"))
        .await
        .map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(seed: u64, n: usize) -> Bytes {
        let mut v = Vec::with_capacity(n);
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            v.push((x >> 56) as u8);
        }
        Bytes::from(v)
    }

    /// Buffered write of `prefix`, then one direct burst of `parts` at its end;
    /// the file must hold exactly `prefix ++ parts` and be exactly that long.
    async fn check(prefix: usize, parts: &[usize]) {
        // Not tempfile's default: /tmp may be tmpfs, which refuses O_DIRECT.
        let dir = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        let path = dir.path().join("x.dat");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await
            .unwrap();
        let mut want = pattern(1, prefix).to_vec();
        let mut w: &CompioFile = &f;
        w.write_all_at(Bytes::from(want.clone()), 0)
            .await
            .0
            .unwrap();
        f.sync_data().await.unwrap();
        let bufs: Vec<Bytes> = parts
            .iter()
            .enumerate()
            .map(|(i, &n)| pattern(i as u64 + 2, n))
            .collect();
        for b in &bufs {
            want.extend_from_slice(b);
        }
        write_burst_direct(&f, bufs, prefix as u64).await.unwrap();
        f.sync_data().await.unwrap();
        drop(f);
        let got = std::fs::read(&path).unwrap();
        assert_eq!(
            got.len(),
            want.len(),
            "file size, prefix={prefix} parts={parts:?}"
        );
        let first_bad = got.iter().zip(&want).position(|(a, b)| a != b);
        assert_eq!(first_bad, None, "content, prefix={prefix} parts={parts:?}");
    }

    #[compio::test]
    async fn aligned_start_and_end() {
        check(0, &[1 << 20]).await;
        check(8192, &[1 << 20]).await;
    }

    #[compio::test]
    async fn unaligned_head_is_preserved() {
        check(4095, &[1 << 20]).await;
        check(4097, &[(1 << 20) + 57]).await;
        check(13, &[1 << 20, 3, 70_000]).await;
    }

    #[compio::test]
    async fn sub_block_tail_lands_after_the_body() {
        check(0, &[(1 << 20) + 1]).await;
        check(100, &[(1 << 20) + 4095]).await;
    }

    #[compio::test]
    async fn burst_spanning_several_chunks() {
        check(
            777,
            &[DIRECT_IO_CHUNK_BYTES - 5, 9, DIRECT_IO_CHUNK_BYTES + 123, 1],
        )
        .await;
    }

    #[compio::test]
    async fn burst_inside_one_block_stays_buffered() {
        check(10, &[4000]).await;
        check(4096, &[1]).await;
    }

    #[compio::test]
    async fn check_opens_the_sentinel_with_o_direct() {
        let dir = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        assert!(
            super::check(dir.path()).await.is_err(),
            "no disk_id must not pass"
        );
        std::fs::write(dir.path().join("disk_id"), b"1").unwrap();
        super::check(dir.path()).await.unwrap();
    }
}
