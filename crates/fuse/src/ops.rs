//! fuser::Filesystem trait implementation.
//!
//! Each callback runs on a fuser thread, sends an FsRequest over the bridge
//! channel, and blocks waiting for the reply from the compio thread.

use std::ffi::OsStr;
use std::time::Duration;

use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};

use crate::bridge::*;

/// FUSE attr/entry cache TTL (from 3FS: 30s).
const TTL: Duration = Duration::from_secs(30);

/// The FUSE filesystem implementation. Lives on fuser threads.
/// Sends all requests to the compio thread via the bridge channel.
pub struct AutumnFs {
    tx: futures::channel::mpsc::UnboundedSender<FsRequest>,
}

impl AutumnFs {
    pub fn new(tx: futures::channel::mpsc::UnboundedSender<FsRequest>) -> Self {
        Self { tx }
    }

    fn send<T>(&self, make_req: impl FnOnce(Reply<T>) -> FsRequest) -> anyhow::Result<T> {
        let (reply_tx, reply_rx) = reply_channel();
        let req = make_req(reply_tx);
        call_sync(&self.tx, req, reply_rx)
    }
}

impl Filesystem for AutumnFs {
    fn init(
        &mut self,
        _req: &Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        // Ask for big requests, so a userspace 8 MiB read or write arrives as
        // few FUSE calls rather than a stream of 128 KiB ones.
        //
        // `set_max_write` does nothing without the `abi-7-28` feature on the
        // `fuser` dependency: `FUSE_MAX_PAGES` and the INIT reply's `max_pages`
        // field are both compiled out below it, and without them the kernel
        // clamps EVERY request to its 32-page default — 128 KiB. Built against
        // `abi-7-12` this line was inert for as long as it had existed; a 4 GiB
        // write arrived as 32768 calls of 128 KiB and reads were chopped the
        // same way, worth 45% of the read path (898 -> 1621 MiB/s on a 4 GiB
        // file, alternating A/B, when the feature was enabled).
        //
        // 1 MiB is not a tuning choice, it is the ceiling: the kernel clamps
        // `max_pages` to FUSE_MAX_MAX_PAGES (256), so asking for 4 or 8 MiB
        // changes nothing — measured, the request count stays at exactly 4096
        // for 4 GiB either way. (Linux 6.10+ makes that clamp a sysctl, so it
        // is the ceiling on this kernel, not a constant of the protocol.)
        //
        // `set_max_readahead` is a DIFFERENT story and is inert either way:
        // `KernelConfig::new` caps it at whatever the kernel offered in its own
        // INIT, and the setter returns `Err` above that — swallowed here. It is
        // kept because it costs nothing and becomes real if a mount ever
        // negotiates a larger readahead, not because it does anything today.
        let _ = config.set_max_readahead(16 * 1024 * 1024);
        let _ = config.set_max_write(1024 * 1024);
        match self.send(|reply| FsRequest::Init { reply }) {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!(error = %e, "init failed");
                Err(libc::EIO)
            }
        }
    }

    fn destroy(&mut self) {
        let _ = self.tx.unbounded_send(FsRequest::Destroy);
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.send(|r| FsRequest::Lookup {
            parent,
            name: name.to_owned(),
            reply: r,
        }) {
            Ok((attr, _ino)) => reply.entry(&TTL, &attr, 0),
            Err(_) => reply.error(libc::ENOENT),
        }
    }

    fn forget(&mut self, _req: &Request<'_>, ino: u64, nlookup: u64) {
        let _ = self.tx.unbounded_send(FsRequest::Forget { ino, nlookup });
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.send(|r| FsRequest::GetAttr { ino, reply: r }) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(_) => reply.error(libc::ENOENT),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        match self.send(|r| FsRequest::SetAttr {
            ino,
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            reply: r,
        }) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        match self.send(|r| FsRequest::Mkdir {
            parent,
            name: name.to_owned(),
            mode,
            reply: r,
        }) {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => {
                let code = err_to_errno(&e);
                reply.error(code);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.send(|r| FsRequest::Rmdir {
            parent,
            name: name.to_owned(),
            reply: r,
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.send(|r| FsRequest::Unlink {
            parent,
            name: name.to_owned(),
            reply: r,
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        old_parent: u64,
        old_name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        // `renameat2` flags are NOT supported, and saying so is load-bearing:
        // the rename below is a plain POSIX clobbering rename, so honouring a
        // flag we cannot implement by ignoring it DESTROYS data. Reproduced
        // with `RENAME_EXCHANGE` (2) on a mount: the call returned SUCCESS and
        // performed a one-way rename, leaving the destination holding the
        // source's bytes and the destination's own content gone.
        //
        // This became reachable when the `fuser` dependency moved to
        // `abi-7-28`: below `abi-7-23` the `FUSE_RENAME2` opcode is not parsed,
        // so the kernel got ENOSYS, set `no_rename2` and answered the caller
        // EINVAL itself. The flags argument existed before that and was
        // ignored safely only because nothing could ever set it.
        //
        // `RENAME_NOREPLACE` (1) does not reach us at all — the VFS answers
        // EEXIST before dispatching — but it is refused here too rather than
        // relying on that.
        if flags != 0 {
            reply.error(libc::EINVAL);
            return;
        }
        match self.send(|r| FsRequest::Rename {
            old_parent,
            old_name: old_name.to_owned(),
            new_parent,
            new_name: new_name.to_owned(),
            reply: r,
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        match self.send(|r| FsRequest::Create {
            parent,
            name: name.to_owned(),
            mode,
            flags,
            reply: r,
        }) {
            Ok((attr, fh)) => reply.created(&TTL, &attr, 0, fh, 0),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        match self.send(|r| FsRequest::Open {
            ino,
            flags,
            reply: r,
        }) {
            // FOPEN_DIRECT_IO (= 1) — bypass kernel page cache so every
            // user-space `read()` reaches our dispatcher. Without this,
            // pages are cached after the first access and ~99% of reads
            // never round-trip to autumn-fuse, masking any improvement
            // from concurrent dispatch / parallel chunk fetch.
            //
            // Trade-off: removes the kernel page cache as a free
            // accelerator for repeat reads. For workloads that benefit
            // from page caching (e.g. hot key reread loops), this hurts.
            // The right long-term tuning is to expose this as a mount
            // option (`--direct-io`) so users can pick. For now we
            // default to direct_io because measurement-without-it gives
            // misleading "free" throughput numbers.
            Ok(fh) => reply.opened(fh, 1),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        // Async-reply read (autumn-fuse perf fix #1): hand `reply`
        // straight to compio thread and return immediately. The fuser
        // single-threaded dispatch loop is then free to read the next
        // /dev/fuse request while this read's parallel chunks are in
        // flight on compio. Without this fix, fuser blocked here on the
        // call_sync std::mpsc reply, capping aggregate FUSE read
        // throughput at ~13 k ops/s regardless of client/dispatcher
        // concurrency.
        if let Err(e) = self.tx.unbounded_send(FsRequest::Read {
            ino,
            offset,
            size,
            fuse_reply: reply,
        }) {
            // Channel closed — the bridge is gone. Recover `reply` out of the
            // rejected message and answer EIO.
            //
            // This used to just log and return, on the reasoning that "fuser
            // times out". It does not: FUSE has no timeout, so a request the
            // daemon never answers leaves that caller blocked in
            // uninterruptible sleep FOREVER, holding whatever locks it held.
            // Every other op here is safe because `call_sync`'s timeout still
            // ends with `reply.error(EIO)`; Read was the one path that could
            // strand the kernel, precisely because it hands `reply` away.
            tracing::error!("fuse Read: bridge channel closed — replying EIO");
            if let FsRequest::Read { fuse_reply, .. } = e.into_inner() {
                fuse_reply.error(libc::EIO);
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.send(|r| FsRequest::Write {
            ino,
            offset,
            data: data.to_vec(),
            reply: r,
        }) {
            Ok(written) => reply.written(written),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        match self.send(|r| FsRequest::Flush { ino, reply: r }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.send(|r| FsRequest::Release {
            ino,
            flush,
            reply: r,
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn fsync(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, datasync: bool, reply: ReplyEmpty) {
        match self.send(|r| FsRequest::Fsync {
            ino,
            datasync,
            reply: r,
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        match self.send(|r| FsRequest::Readdir {
            ino,
            offset,
            reply: r,
        }) {
            Ok(entries) => {
                for e in entries {
                    // M1: core entries carry a DT_* byte;
                    // convert to fuser::FileType at the reply boundary.
                    if reply.add(e.ino, e.offset, crate::attr::dt_to_filetype(e.kind), &e.name) {
                        break; // buffer full
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(err_to_errno(&e)),
        }
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        match self.send(|r| FsRequest::Statfs { reply: r }) {
            Ok(s) => reply.statfs(
                s.blocks, s.bfree, s.bavail, s.files, s.ffree, s.bsize, s.namelen, 0,
            ),
            Err(_) => {
                // Return reasonable defaults
                reply.statfs(
                    1 << 30, // blocks
                    1 << 29, // bfree
                    1 << 29, // bavail
                    1 << 20, // files
                    1 << 19, // ffree
                    4096,    // bsize
                    255,     // namelen
                    0,       // frsize
                );
            }
        }
    }
}

use std::time::SystemTime;

/// Map error message to errno.
fn err_to_errno(e: &anyhow::Error) -> i32 {
    let msg = e.to_string();
    if msg.contains("ENOENT") || msg.contains("not found") {
        libc::ENOENT
    } else if msg.contains("EEXIST") {
        libc::EEXIST
    } else if msg.contains("ENOTDIR") {
        libc::ENOTDIR
    } else if msg.contains("ENOTEMPTY") {
        libc::ENOTEMPTY
    } else if msg.contains("EISDIR") {
        libc::EISDIR
    } else if msg.contains("EBUSY") || msg.contains("lease mode mismatch") {
        // coco P2 #4: writer-lease conflicts and
        // in-mount mode mismatches now surface as EBUSY so apps
        // can distinguish "someone else holds the file" from real
        // I/O failure. Without this mapping the lease conflict
        // looked like an EIO and was indistinguishable from a
        // storage outage.
        libc::EBUSY
    } else {
        // Catch-all EIO. Log the unmapped error at WARN so operators can see
        // what actually failed — pre-this the app just got a bare EIO with no
        // cause (a real I/O failure, a fence rejection, an ENOSPC, a UCX/routing
        // error all looked identical). Only fires on the unexpected fallthrough,
        // not on the mapped ENOENT/EEXIST/... cases, so it stays low-noise.
        tracing::warn!("fuse op → EIO (unmapped error): {msg}");
        libc::EIO
    }
}
