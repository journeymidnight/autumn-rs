//! Python binding for the shared FUSE filesystem core (`autumn.Fs`).
//!
//! M2. A SYNCHRONOUS, blocking façade over the fuser-FREE FS core
//! (`autumn_fuse` built with `--features core`): meta/dir/extent/read/write ops
//! on the shared inode layout (`0x01` inode / `0x02` dirent / `0x03` extent),
//! plus the reusable `autumn_client::lease` primitives. This is the SAME Rust
//! core the `autumn-fuse` kernel mount runs on — the logic is NOT reimplemented
//! in Python, so a file created through `Fs` is byte-identical through the mount
//! (and vice versa), with no drift.
//!
//! Threading mirrors `Client`/`Memory`: a dedicated compio worker thread owns
//! the `!Send` `FsState`; each method ships a job to it and blocks (GIL
//! released) for the result. Sync is what the M3 fsspec facade wants — fsspec's
//! `AbstractFileSystem` is a synchronous API.
//!
//! Scope boundary (M2 = binding + headless correctness): the lease methods are
//! thin wrappers; the open→acquire→heartbeat→release facade logic + fencing
//! tests land in M4, and the fsspec rewrite in M3.
//!
//! CROSS-CLIENT COHERENCE IS M4, NOT M2. This binding reuses the shared core's
//! `FsState.inodes` cache (populated by `write`/`truncate`), so a *long-lived*
//! `Fs` whose cache holds an inode can serve a stale size / extent plan after
//! ANOTHER client (a second `Fs` or a fuse mount) modifies + flushes that
//! inode — there is no invalidation poll here yet. The design's Q1 answer
//! (write-only leases; reads coherent via `poll_invalidations`/`cache_is_stale`)
//! is wired in M4. Until then, coherence holds for: a client reading its OWN
//! writes, and any FRESH `Fs` (empty cache → reads authoritative KV) — which is
//! what the M2 e2e's cross-instance check exercises.

use std::sync::Mutex;

use autumn_client::lease::{self, AcquireResult, HeartbeatResult};
use autumn_fuse::meta::{S_IFDIR, S_IFLNK, S_IFMT};
use autumn_fuse::schema::{DT_DIR, DT_LNK, DT_REG};
use autumn_fuse::state::{FsState, FuseLease};
use autumn_rpc::manager_rpc::{LEASE_MODE_READ, LEASE_MODE_WRITE};
use futures::channel::mpsc::{unbounded, UnboundedSender};
use futures::future::LocalBoxFuture;
use futures::StreamExt;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

/// A unit of work for the fs worker: given `&mut FsState`, produce a future
/// that computes a result and sends it back over the job's own channel. The
/// `for<'a>` bound lets the future borrow the worker-owned state across its
/// awaits (the state is never sent — only the boxed FnOnce crosses the channel,
/// hence `+ Send`; the future it builds is driven entirely on the worker).
type FsJob = Box<dyn for<'a> FnOnce(&'a mut FsState) -> LocalBoxFuture<'a, ()> + Send>;

#[pyclass]
pub struct Fs {
    tx: Mutex<Option<UnboundedSender<FsJob>>>,
}

/// Plain (Send) snapshot of an inode's metadata, marshaled back from the
/// worker before the GIL-holding method turns it into a Python dict.
struct InodeInfo {
    ino: u64,
    size: u64,
    mode: u32,
    nlink: u32,
    uid: u32,
    gid: u32,
    atime: f64,
    mtime: f64,
    ctime: f64,
}

/// fsspec-style type string for an inode `mode`.
fn type_str(mode: u32) -> &'static str {
    match mode & S_IFMT {
        m if m == S_IFDIR => "directory",
        m if m == S_IFLNK => "link",
        _ => "file",
    }
}

/// `DT_*` readdir byte for an inode `mode` (lookup returns meta, not a dirent).
fn kind_of(mode: u32) -> u8 {
    match mode & S_IFMT {
        m if m == S_IFDIR => DT_DIR,
        m if m == S_IFLNK => DT_LNK,
        _ => DT_REG,
    }
}

fn ts_to_f64(secs: i64, nsecs: u32) -> f64 {
    secs as f64 + (nsecs as f64) / 1e9
}

/// True iff a lease entry for `ino` is held AND server-side revoked. A persist
/// op on a revoked lease must fast-fail (M4 fence — the stale writer's bytes
/// must not land; the PS would fence them anyway, this just fails early). A
/// MISSING entry is NOT revoked — anonymous writes without a held lease stay
/// allowed (M2 semantics); the fsspec facade holds a lease + a heartbeat gate.
/// The borrow is scoped so callers never hold it across an await.
fn lease_revoked(st: &FsState, ino: u64) -> bool {
    st.held_leases
        .borrow()
        .get(&ino)
        .map(|l| l.revoked)
        .unwrap_or(false)
}

/// Macro: build a job that runs `$body` (a `Result<$ret, String>` expression
/// that may `.await`) against `$st: &mut FsState`, ship it to the worker, and
/// block (GIL released) for the result. `tx`/`rx`/`job` are macro-hygienic.
macro_rules! fs_blocking {
    ($self:ident, $py:ident, $ret:ty, |$st:ident| $body:block) => {{
        let (tx, rx) = std::sync::mpsc::channel::<Result<$ret, String>>();
        let job: FsJob = Box::new(move |$st: &mut FsState| Box::pin(async move {
            let _ = tx.send($body);
        }));
        $self.submit(job)?;
        $self.recv_block($py, rx)
    }};
}

#[pymethods]
impl Fs {
    /// Connect an `Fs` against `manager`. Blocks until the worker has connected
    /// (or raises). `host` seeds the daemon lease identity
    /// (`DaemonClientId::new_fuse`); defaults to a stable label.
    ///
    /// `direct_read=True` makes whole-value reads (≥ 64 KiB)
    /// bypass the PS and read straight from an extent node (`get_many_direct`) —
    /// a cross-host throughput win for fsspec model/dataset serving. TOPOLOGY-
    /// DEPENDENT (this host must reach EN data ports), default False; each read
    /// falls back to the PS proxy on any direct-read failure.
    ///
    /// `principal=` + `credential=` (both or neither) attach an
    /// authz identity, exactly like `Client`/`BatchClient` and the native
    /// `--credential-file` clients. REQUIRED once the deploy protects `fs/` —
    /// authz gates READS on protected prefixes too, so a credential-less `Fs`
    /// (e.g. the vLLM weight loader) would fail with PermissionDenied on every
    /// read. `credential` is RAW bytes (hex-decode the `autumn-op principal-create`
    /// output first — see `autumn_kvcache._identity.read_credential_file`).
    /// `principal` is the credential owner (from the credential file's name line).
    /// no tenant — `autumn.Fs` scopes to the WHOLE `fs/`
    /// namespace (one global tree; see docs/key_namespace_split_design.md §8).
    #[staticmethod]
    #[pyo3(signature = (manager, host=None, principal=None, credential=None, direct_read=false))]
    fn connect(
        py: Python<'_>,
        manager: String,
        host: Option<String>,
        principal: Option<String>,
        credential: Option<Vec<u8>>,
        direct_read: bool,
    ) -> PyResult<Self> {
        let host = host.unwrap_or_else(|| "autumn-fs-py".to_string());
        // Both-or-neither, checked HERE (config error) rather than surfacing as a
        // confusing mid-read PermissionDenied.
        if principal.is_some() != credential.is_some() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "principal= and credential= must be passed together (both or neither)",
            ));
        }
        let (job_tx, mut job_rx) = unbounded::<FsJob>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

        std::thread::Builder::new()
            .name("autumn-fs".into())
            .spawn(move || {
                let rt = match compio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = ready_tx.send(Err(format!("compio runtime: {e}")));
                        return;
                    }
                };
                rt.block_on(async move {
                    // credential path when one was supplied —
                    // `connect_with_credential` FAILS FAST at connect if the
                    // credential doesn't cover `fs/`.
                    let opened = match credential {
                        Some(cred) => {
                            let who = principal.unwrap_or_default();
                            FsState::new_with_host_credential(&manager, host, &who, cred).await
                        }
                        None => FsState::new_with_host(&manager, host).await,
                    };
                    let mut state = match opened {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = ready_tx.send(Err(e.to_string()));
                            return;
                        }
                    };
                    state.direct_read = direct_read;
                    // Ensure the shared root inode exists (the fuse mount does
                    // this in its Init; the binding is a co-equal front-end).
                    if let Err(e) = autumn_fuse::meta::ensure_root(&mut state).await {
                        let _ = ready_tx.send(Err(format!("ensure_root: {e}")));
                        return;
                    }
                    // M4: the same per-session lease background tasks
                    // the fuse mount runs (headless — no kernel invalidator):
                    // heartbeat held write leases so a long write doesn't lose
                    // its lease, and poll invalidations so a preempted lease is
                    // marked revoked. Spawned on this worker's compio runtime.
                    autumn_fuse::lease_tasks::spawn_lease_background_tasks(&state, None);
                    let _ = ready_tx.send(Ok(()));
                    // Sequential job processing: one `&mut FsState` borrow at a
                    // time (no concurrent borrow), so core ops that hold `&mut`
                    // across awaits are safe.
                    while let Some(job) = job_rx.next().await {
                        job(&mut state).await;
                    }
                });
            })
            .map_err(|e| PyRuntimeError::new_err(format!("spawn worker: {e}")))?;

        match py.allow_threads(move || ready_rx.recv()) {
            Ok(Ok(())) => Ok(Fs {
                tx: Mutex::new(Some(job_tx)),
            }),
            Ok(Err(e)) => Err(PyRuntimeError::new_err(e)),
            Err(_) => Err(PyRuntimeError::new_err("fs worker died during connect")),
        }
    }

    /// Tear down the worker (drops the sender → worker drains + exits). Any
    /// un-`flush`ed write buffers are discarded — flush before close.
    fn close(&self) {
        *self.tx.lock().unwrap() = None;
    }

    // ── path resolution + metadata ──────────────────────────────────────────

    /// Resolve an absolute path to its inode number, or `None` if absent.
    fn resolve(&self, py: Python<'_>, path: String) -> PyResult<PyObject> {
        let r: Option<u64> = fs_blocking!(self, py, Option<u64>, |st| {
            autumn_fuse::dir::resolve(st, &path)
                .await
                .map_err(|e| e.to_string())
        })?;
        Ok(match r {
            Some(ino) => ino.into_pyobject(py)?.into_any().unbind(),
            None => py.None(),
        })
    }

    /// Inode attributes as a dict: `{ino, size, type, mode, nlink, uid, gid,
    /// atime, mtime, ctime}` (`type` = "file"/"directory"/"link"; times are
    /// float seconds). Raises on a missing/unreadable inode.
    fn getattr(&self, py: Python<'_>, ino: u64) -> PyResult<PyObject> {
        let info: InodeInfo = fs_blocking!(self, py, InodeInfo, |st| {
            autumn_fuse::meta::get_inode(st, ino)
                .await
                .map(|m| InodeInfo {
                    ino,
                    size: m.size,
                    mode: m.mode,
                    nlink: m.nlink,
                    uid: m.uid,
                    gid: m.gid,
                    atime: ts_to_f64(m.atime_secs, m.atime_nsecs),
                    mtime: ts_to_f64(m.mtime_secs, m.mtime_nsecs),
                    ctime: ts_to_f64(m.ctime_secs, m.ctime_nsecs),
                })
                .map_err(|e| e.to_string())
        })?;
        let d = PyDict::new(py);
        d.set_item("ino", info.ino)?;
        d.set_item("size", info.size)?;
        d.set_item("type", type_str(info.mode))?;
        d.set_item("mode", info.mode)?;
        d.set_item("nlink", info.nlink)?;
        d.set_item("uid", info.uid)?;
        d.set_item("gid", info.gid)?;
        d.set_item("atime", info.atime)?;
        d.set_item("mtime", info.mtime)?;
        d.set_item("ctime", info.ctime)?;
        Ok(d.into_any().unbind())
    }

    /// Directory entries as `[(name, ino, kind)]` (`kind` = `DT_*` byte;
    /// `DT_DIR`=4, `DT_REG`=8, `DT_LNK`=10). `.`/`..` are excluded.
    fn readdir(&self, py: Python<'_>, ino: u64) -> PyResult<PyObject> {
        let entries: Vec<(String, u64, u8)> = fs_blocking!(self, py, Vec<(String, u64, u8)>, |st| {
            autumn_fuse::dir::readdir(st, ino, 0)
                .await
                .map(|es| {
                    es.into_iter()
                        .map(|e| (e.name.to_string_lossy().into_owned(), e.ino, e.kind))
                        .filter(|(name, _, _)| name != "." && name != "..")
                        .collect()
                })
                .map_err(|e| e.to_string())
        })?;
        let objs: PyResult<Vec<Bound<'_, PyAny>>> = entries
            .into_iter()
            .map(|(name, cino, kind)| {
                PyTuple::new(
                    py,
                    &[
                        name.into_pyobject(py)?.into_any(),
                        cino.into_pyobject(py)?.into_any(),
                        kind.into_pyobject(py)?.into_any(),
                    ],
                )
                .map(|t| t.into_any())
            })
            .collect();
        Ok(PyList::new(py, objs?)?.into_any().unbind())
    }

    /// Look up `name` under `parent` → `(ino, kind)` or `None` if absent. A hard
    /// KV/decode error raises (via `lookup_opt`'s barrier) — a transient failure
    /// is NOT reported as a miss.
    fn lookup(&self, py: Python<'_>, parent: u64, name: String) -> PyResult<PyObject> {
        let r: Option<(u64, u8)> = fs_blocking!(self, py, Option<(u64, u8)>, |st| {
            let osname = std::ffi::OsStr::new(&name);
            autumn_fuse::dir::lookup_opt(st, parent, osname)
                .await
                .map(|opt| opt.map(|(ino, meta)| (ino, kind_of(meta.mode))))
                .map_err(|e| e.to_string())
        })?;
        Ok(match r {
            Some((ino, kind)) => PyTuple::new(
                py,
                &[
                    ino.into_pyobject(py)?.into_any(),
                    kind.into_pyobject(py)?.into_any(),
                ],
            )?
            .into_any()
            .unbind(),
            None => py.None(),
        })
    }

    // ── mutations ───────────────────────────────────────────────────────────

    /// Create a subdirectory under `parent`; returns the new inode.
    #[pyo3(signature = (parent, name, mode=0o755))]
    fn mkdir(&self, py: Python<'_>, parent: u64, name: String, mode: u32) -> PyResult<u64> {
        fs_blocking!(self, py, u64, |st| {
            let osname = std::ffi::OsStr::new(&name);
            autumn_fuse::dir::mkdir(st, parent, osname, mode)
                .await
                .map(|(ino, _)| ino)
                .map_err(|e| e.to_string())
        })
    }

    /// Create an empty regular file under `parent`; returns the new inode.
    #[pyo3(signature = (parent, name, mode=0o644))]
    fn create(&self, py: Python<'_>, parent: u64, name: String, mode: u32) -> PyResult<u64> {
        fs_blocking!(self, py, u64, |st| {
            let osname = std::ffi::OsStr::new(&name);
            autumn_fuse::dir::create(st, parent, osname, mode)
                .await
                .map(|(ino, _)| ino)
                .map_err(|e| e.to_string())
        })
    }

    /// Unlink a regular file `name` under `parent`.
    fn unlink(&self, py: Python<'_>, parent: u64, name: String) -> PyResult<()> {
        fs_blocking!(self, py, (), |st| {
            let osname = std::ffi::OsStr::new(&name);
            autumn_fuse::dir::unlink(st, parent, osname)
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Remove an empty directory `name` under `parent`.
    fn rmdir(&self, py: Python<'_>, parent: u64, name: String) -> PyResult<()> {
        fs_blocking!(self, py, (), |st| {
            let osname = std::ffi::OsStr::new(&name);
            autumn_fuse::dir::rmdir(st, parent, osname)
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Rename `old_name`@`old_parent` → `new_name`@`new_parent`.
    fn rename(
        &self,
        py: Python<'_>,
        old_parent: u64,
        old_name: String,
        new_parent: u64,
        new_name: String,
    ) -> PyResult<()> {
        fs_blocking!(self, py, (), |st| {
            let on = std::ffi::OsStr::new(&old_name);
            let nn = std::ffi::OsStr::new(&new_name);
            autumn_fuse::dir::rename(st, old_parent, on, new_parent, nn)
                .await
                .map_err(|e| e.to_string())
        })
    }

    // ── data ────────────────────────────────────────────────────────────────

    /// Read up to `size` bytes at `offset` from inode `ino`.
    fn read(&self, py: Python<'_>, ino: u64, offset: i64, size: u32) -> PyResult<PyObject> {
        let v: Vec<u8> = fs_blocking!(self, py, Vec<u8>, |st| {
            autumn_fuse::read::read(st, ino, offset, size)
                .await
                .map_err(|e| e.to_string())
        })?;
        Ok(PyBytes::new(py, &v).into_any().unbind())
    }

    /// Zero-copy read into a caller buffer. `buf` is a writable,
    /// C-contiguous Python buffer-protocol object (e.g. a CUDA-pinned host tensor
    /// `torch.empty(n, dtype=torch.uint8, pin_memory=True)`); the extent data is
    /// written STRAIGHT into it — no intermediate `bytes` — so a model loader can
    /// overlap the storage read with the async H2D copy (the Run:ai-Model-Streamer
    /// pattern). Reads up to `len(buf)` bytes from `offset` (capped at 1 GiB per
    /// call — the `GetReq` size is u32; loop, advancing `offset`, to fill a bigger
    /// buffer) and returns the number of bytes written into `buf[0..n]`. Honors the
    /// connection's `direct_read` (bypasses the PS, reading straight from an EN).
    fn read_into(&self, py: Python<'_>, ino: u64, offset: i64, buf: PyBuffer<u8>) -> PyResult<usize> {
        if buf.readonly() {
            return Err(PyValueError::new_err("buf must be writable"));
        }
        if !buf.is_c_contiguous() {
            return Err(PyValueError::new_err("buf must be C-contiguous"));
        }
        let dest_ptr = buf.buf_ptr() as usize;
        let dest_len = buf.item_count();
        // This method BLOCKS (recv_block) until the worker finishes, so `buf` — a
        // parameter on this stack frame — outlives the worker's write into
        // `dest_ptr`, and the buffer protocol pins the memory. Only the Send
        // `usize` ptr/len cross to the worker (never the `!Send`-in-spirit view).
        fs_blocking!(self, py, usize, |st| {
            // SAFETY: dest_ptr/dest_len come from `buf`, alive on the caller's
            // stack for this whole blocking call; the worker holds the only
            // `&mut` to that memory while the caller is parked in `recv_block`.
            let dest = unsafe { std::slice::from_raw_parts_mut(dest_ptr as *mut u8, dest_len) };
            autumn_fuse::read::read_into(st, ino, offset, dest)
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Write `data` at `offset` to inode `ino`; returns bytes written. Data is
    /// buffered — call `flush` (or `truncate`) to persist it durably.
    fn write(&self, py: Python<'_>, ino: u64, offset: i64, data: Vec<u8>) -> PyResult<u32> {
        fs_blocking!(self, py, u32, |st| {
            if lease_revoked(st, ino) {
                Err(format!("write lease for ino {ino} was revoked"))
            } else {
                autumn_fuse::write::write(st, ino, offset, &data)
                    .await
                    .map_err(|e| e.to_string())
            }
        })
    }

    /// Drop `ino` from this session's inode cache so the next read reloads
    /// authoritative metadata + extents from the cluster. The facade calls this
    /// after releasing a write lease, giving cross-client close-to-open
    /// coherence (M2's cache is populated only by writes, so a never-written
    /// inode already reads fresh).
    fn forget(&self, py: Python<'_>, ino: u64) -> PyResult<()> {
        fs_blocking!(self, py, (), |st| {
            st.inodes.remove(&ino);
            Ok(())
        })
    }

    /// Flush inode `ino`'s buffered writes + metadata to the KV store.
    fn flush(&self, py: Python<'_>, ino: u64) -> PyResult<()> {
        fs_blocking!(self, py, (), |st| {
            if lease_revoked(st, ino) {
                Err(format!("write lease for ino {ino} was revoked"))
            } else {
                autumn_fuse::write::flush_inode(st, ino)
                    .await
                    .map_err(|e| e.to_string())
            }
        })
    }

    /// Truncate (grow or shrink) inode `ino` to `size` bytes.
    fn truncate(&self, py: Python<'_>, ino: u64, size: u64) -> PyResult<()> {
        fs_blocking!(self, py, (), |st| {
            if lease_revoked(st, ino) {
                Err(format!("write lease for ino {ino} was revoked"))
            } else {
                autumn_fuse::write::truncate(st, ino, size)
                    .await
                    .map_err(|e| e.to_string())
            }
        })
    }

    // ── leases (M2: thin wrappers; the facade drives them in M4) ─────────────

    /// Acquire a lease on `ino`. `mode` is "r"/"read" or "w"/"write". Returns
    /// the granted `lease_epoch`; raises on conflict.
    #[pyo3(signature = (ino, mode="w"))]
    fn acquire(&self, py: Python<'_>, ino: u64, mode: &str) -> PyResult<u64> {
        let req_mode = match mode.chars().next() {
            Some('w') | Some('W') => LEASE_MODE_WRITE,
            Some('r') | Some('R') => LEASE_MODE_READ,
            _ => return Err(PyRuntimeError::new_err(format!("bad lease mode: {mode}"))),
        };
        fs_blocking!(self, py, u64, |st| {
            let cluster = st.client.clone();
            let id = st.client_id.clone();
            match lease::acquire(&cluster, &id, ino, req_mode).await {
                Ok(AcquireResult::Granted(info)) => {
                    let mut held = st.held_leases.borrow_mut();
                    let e = held.entry(ino).or_insert(FuseLease {
                        mode: req_mode,
                        refcount: 0,
                        lease_epoch: info.version,
                        revoked: false,
                    });
                    e.refcount += 1;
                    e.mode = req_mode;
                    e.lease_epoch = info.version;
                    e.revoked = false;
                    Ok(info.version)
                }
                Ok(AcquireResult::Conflict { manager_message }) => {
                    Err(format!("lease conflict on ino {ino}: {manager_message}"))
                }
                Ok(AcquireResult::RevokePending { .. }) => {
                    Err(format!("lease revoke pending on ino {ino}"))
                }
                Err(e) => Err(format!("acquire ino {ino}: {e}")),
            }
        })
    }

    /// Renew `ino`'s lease. Returns `True` if renewed, `False` if the manager
    /// has revoked/expired it (`NotHeld`).
    fn heartbeat(&self, py: Python<'_>, ino: u64) -> PyResult<bool> {
        fs_blocking!(self, py, bool, |st| {
            let cluster = st.client.clone();
            let id = st.client_id.clone();
            match lease::heartbeat(&cluster, &id, ino).await {
                Ok(HeartbeatResult::Renewed(_)) => Ok(true),
                Ok(HeartbeatResult::NotHeld) => {
                    st.held_leases.borrow_mut().remove(&ino);
                    Ok(false)
                }
                Err(e) => Err(format!("heartbeat ino {ino}: {e}")),
            }
        })
    }

    /// Release one `acquire` on `ino`. Refcount-aware (mirrors the FUSE dispatch
    /// release): only the final 1→0 release fires the manager `ReleaseLease` +
    /// drops the local entry, so balanced acquire/release nesting keeps the
    /// write fence intact instead of dropping it on the first release.
    fn release(&self, py: Python<'_>, ino: u64) -> PyResult<()> {
        fs_blocking!(self, py, (), |st| {
            // Decide under a brief borrow (dropped before any await).
            let fire = {
                let mut held = st.held_leases.borrow_mut();
                match held.get_mut(&ino) {
                    Some(l) if l.refcount > 1 => {
                        l.refcount -= 1;
                        false
                    }
                    Some(_) => {
                        held.remove(&ino);
                        true
                    }
                    // No local record — still release at the manager (idempotent).
                    None => true,
                }
            };
            if fire {
                let cluster = st.client.clone();
                let id = st.client_id.clone();
                lease::release(&cluster, &id, ino)
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("release ino {ino}: {e}"))
            } else {
                Ok(())
            }
        })
    }
}

impl Fs {
    /// Ship a job to the worker (non-generic; keeps `FsJob` monomorphic).
    fn submit(&self, job: FsJob) -> PyResult<()> {
        let guard = self.tx.lock().unwrap();
        match guard.as_ref() {
            Some(tx) => tx
                .unbounded_send(job)
                .map_err(|_| PyRuntimeError::new_err("fs worker channel closed")),
            None => Err(PyRuntimeError::new_err("fs is closed")),
        }
    }

    /// Block (GIL released) for a job's result over its own channel.
    fn recv_block<T: Send + 'static>(
        &self,
        py: Python<'_>,
        rx: std::sync::mpsc::Receiver<Result<T, String>>,
    ) -> PyResult<T> {
        match py.allow_threads(move || rx.recv()) {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(PyRuntimeError::new_err(e)),
            Err(_) => Err(PyRuntimeError::new_err("fs worker gone")),
        }
    }
}
