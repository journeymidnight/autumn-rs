// asyncio-native bindings.
//
// Architecture:
//   - One dedicated OS thread runs a compio runtime hosting a single
//     `ClusterClient`. All RPCs live on that thread (compio is
//     thread-per-core / !Send by design).
//   - Python-side methods are sync functions that allocate an
//     `asyncio.Future`, push an `Op` carrying a `PyHandle {loop, fut}`
//     to the compio thread via an unbounded mpsc, and return the future.
//   - When the compio future resolves, the worker re-acquires the GIL
//     and calls `loop.call_soon_threadsafe(fut.set_result|set_exception, …)`
//     so the asyncio loop wakes the awaiting coroutine on its own thread.
//
// `Client.connect` is an async classmethod: it spawns the worker thread,
// performs `ClusterClient::connect` inside the runtime, then returns a
// fully-constructed `Client` Python object via the same future-bridge.
// The worker thread is detached; on close (or sender drop) the event
// loop returns and the runtime tears itself down.

use std::sync::Mutex;

use autumn_client::{AutumnError, ClusterClient};
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::StreamExt;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

// ── PyHandle: asyncio.Future bridge ─────────────────────────────────────────

struct PyHandle {
    loop_obj: Py<PyAny>,
    fut: Py<PyAny>,
}

impl PyHandle {
    fn new(loop_obj: &Bound<'_, PyAny>, fut: &Bound<'_, PyAny>) -> Self {
        Self {
            loop_obj: loop_obj.clone().unbind(),
            fut: fut.clone().unbind(),
        }
    }

    /// Schedule `fut.set_result(value)` on the asyncio loop.
    /// `build` constructs the result value while holding the GIL.
    fn resolve<F>(self, build: F)
    where
        F: FnOnce(Python<'_>) -> PyResult<PyObject>,
    {
        Python::with_gil(|py| match build(py) {
            Ok(obj) => {
                let _ = self.dispatch(py, "set_result", obj);
            }
            Err(err) => {
                let exc: PyObject = err.into_value(py).into_any();
                let _ = self.dispatch(py, "set_exception", exc);
            }
        });
    }

    /// Schedule `fut.set_exception(RuntimeError(msg))` on the asyncio loop.
    fn reject(self, msg: String) {
        Python::with_gil(|py| {
            let exc: PyObject = PyRuntimeError::new_err(msg).into_value(py).into_any();
            let _ = self.dispatch(py, "set_exception", exc);
        });
    }

    fn dispatch(&self, py: Python<'_>, method: &str, arg: PyObject) -> PyResult<()> {
        let setter = self.fut.bind(py).getattr(method)?;
        self.loop_obj
            .bind(py)
            .call_method1("call_soon_threadsafe", (setter, arg))?;
        Ok(())
    }
}

/// Allocate `(handle, future)` from the currently-running asyncio loop.
fn make_handle<'py>(py: Python<'py>) -> PyResult<(PyHandle, Bound<'py, PyAny>)> {
    let asyncio = py.import("asyncio")?;
    let loop_obj = asyncio.call_method0("get_running_loop")?;
    let fut = loop_obj.call_method0("create_future")?;
    let handle = PyHandle::new(&loop_obj, &fut);
    Ok((handle, fut))
}

// ── Op: requests dispatched onto the compio thread ──────────────────────────

enum Op {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        handle: PyHandle,
    },
    Get {
        key: Vec<u8>,
        handle: PyHandle,
    },
    Delete {
        key: Vec<u8>,
        handle: PyHandle,
    },
    Range {
        prefix: Vec<u8>,
        start: Vec<u8>,
        limit: u32,
        handle: PyHandle,
    },
    BatchDelete {
        prefix: Vec<u8>,
        handle: PyHandle,
    },
    Head {
        key: Vec<u8>,
        handle: PyHandle,
    },
    BatchHead {
        keys: Vec<Vec<u8>>,
        handle: PyHandle,
    },
    PutFrom {
        key: Vec<u8>,
        buf: PyBuffer<u8>,
        handle: PyHandle,
    },
    GetInto {
        key: Vec<u8>,
        dest_ptr: usize,
        dest_len: usize,
        _buf_keepalive: PyBuffer<u8>,
        handle: PyHandle,
    },
    BatchPutFrom {
        keys: Vec<Vec<u8>>,
        bufs: Vec<PyBuffer<u8>>,
        handle: PyHandle,
    },
    BatchGetInto {
        keys: Vec<Vec<u8>>,
        dest_ptrs: Vec<usize>,
        dest_lens: Vec<usize>,
        _bufs_keepalive: Vec<PyBuffer<u8>>,
        handle: PyHandle,
    },
    Close {
        handle: PyHandle,
    },
}

// PyBuffer<u8> is Send+Sync by virtue of the buffer protocol's pinning
// guarantee (the underlying memory does not move during the view's lifetime).
// `Drop` for PyBuffer acquires the GIL internally, so handing one off to the
// compio worker thread is safe even though no GIL is held there.
//
// The raw `dest_ptr` (usize-cast pointer) in GetInto is only dereferenced from
// the compio worker thread, and only for the duration of one in-flight op.
// `_buf_keepalive` holds the underlying Python buffer view alive across that
// window, guaranteeing the pointed-at memory is valid and won't be remapped.

// ── compio worker loop ─────────────────────────────────────────────────────

async fn event_loop(client: ClusterClient, mut rx: UnboundedReceiver<Op>) {
    // All ClusterClient data ops take `&self` and scope every RefCell borrow
    // across `.await` (see crates/client/src/lib.rs:162), so a shared `&client`
    // is safe to hand to several concurrent in-flight RPCs — which is exactly
    // what the batch ops below do via `join_all`.
    while let Some(op) = rx.next().await {
        match op {
            Op::Close { handle } => {
                handle.resolve(|py| Ok(py.None()));
                break;
            }
            other => handle_op(&client, other).await,
        }
    }
}

async fn handle_op(client: &ClusterClient, op: Op) {
    match op {
        Op::Put { key, value, handle } => {
            match client.put(&key, &value).await {
                Ok(()) => handle.resolve(|py| Ok(py.None())),
                Err(e) => handle.reject(e.to_string()),
            }
        }
        Op::Get { key, handle } => {
            match client.get(&key).await {
                Ok(Some(v)) => handle.resolve(|py| Ok(PyBytes::new(py, &v).into_any().unbind())),
                Ok(None) => handle.resolve(|py| Ok(py.None())),
                Err(e) => handle.reject(e.to_string()),
            }
        }
        Op::Delete { key, handle } => {
            match client.delete(&key).await {
                Ok(()) => handle.resolve(|py| Ok(py.None())),
                Err(e) => handle.reject(e.to_string()),
            }
        }
        Op::Range {
            prefix,
            start,
            limit,
            handle,
        } => match client.range(&prefix, &start, limit).await {
            Ok(result) => handle.resolve(move |py| {
                let entries = result.entries;
                let py_list = pyo3::types::PyList::empty(py);
                for e in entries {
                    let k = PyBytes::new(py, &e.key);
                    let v = PyBytes::new(py, &e.value);
                    let tup = pyo3::types::PyTuple::new(py, &[k.into_any(), v.into_any()])?;
                    py_list.append(tup)?;
                }
                Ok(py_list.into_any().unbind())
            }),
            Err(e) => handle.reject(e.to_string()),
        },
        Op::BatchDelete { prefix, handle } => {
            match do_batch_delete(client, &prefix).await {
                Ok(n) => handle.resolve(move |py| Ok(n.into_pyobject(py)?.into_any().unbind())),
                Err(msg) => handle.reject(msg),
            }
        }
        Op::Head { key, handle } => {
            match client.head(&key).await {
                Ok(meta) => handle.resolve(move |py| {
                    Ok(meta.found.into_pyobject(py)?.to_owned().into_any().unbind())
                }),
                Err(e) => handle.reject(e.to_string()),
            }
        }
        Op::BatchHead { keys, handle } => {
            // Pipeline all existence probes concurrently. Returns list[bool]
            // (found per key); the caller derives the contiguous-prefix length.
            // RPC error on a key surfaces as False (treated as "not present").
            let futs = keys.iter().map(|k| client.head(k));
            let results = futures::future::join_all(futs).await;
            let founds: Vec<bool> = results
                .into_iter()
                .map(|r| r.map(|m| m.found).unwrap_or(false))
                .collect();
            handle.resolve(move |py| Ok(pyo3::types::PyList::new(py, founds)?.into_any().unbind()));
        }
        Op::PutFrom { key, buf, handle } => {
            // SAFETY: buf is held by this Op until handle_op returns; PyBuffer's
            // pinning contract guarantees the pointed-at memory is stable.
            let value = unsafe {
                std::slice::from_raw_parts(buf.buf_ptr() as *const u8, buf.item_count())
            };
            match client.put(&key, value).await {
                Ok(()) => handle.resolve(|py| Ok(py.None())),
                Err(e) => handle.reject(e.to_string()),
            }
            drop(buf); // explicit; PyBuffer::drop reacquires the GIL itself
        }
        Op::GetInto {
            key,
            dest_ptr,
            dest_len,
            _buf_keepalive,
            handle,
        } => {
            match client.get(&key).await {
                Ok(Some(v)) => {
                    if v.len() == dest_len {
                        // SAFETY: dest_ptr / dest_len come from a PyBuffer that
                        // `_buf_keepalive` keeps alive for the duration of this
                        // block. We have exclusive use of the view; the buffer
                        // protocol guarantees pinned memory.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                v.as_ptr(),
                                dest_ptr as *mut u8,
                                dest_len,
                            );
                        }
                        handle.resolve(|py| {
                            Ok(true.into_pyobject(py)?.to_owned().into_any().unbind())
                        });
                    } else {
                        // Size mismatch — surface as False so the sglang backend can
                        // treat it as a cache miss without raising.
                        handle.resolve(|py| {
                            Ok(false.into_pyobject(py)?.to_owned().into_any().unbind())
                        });
                    }
                }
                Ok(None) => {
                    handle.resolve(|py| {
                        Ok(false.into_pyobject(py)?.to_owned().into_any().unbind())
                    });
                }
                Err(e) => handle.reject(e.to_string()),
            }
            drop(_buf_keepalive);
        }
        Op::BatchPutFrom { keys, bufs, handle } => {
            // Pipeline all puts concurrently on this single compio thread. The
            // partition server's group-commit (F099-D) coalesces them at the
            // WAL level — same pattern perf-check uses, just driven from one
            // batch op instead of N Python round-trips.
            let futs = keys.iter().zip(bufs.iter()).map(|(k, buf)| {
                // SAFETY: each buf is held in `bufs` for the whole join_all;
                // PyBuffer pins the underlying memory.
                let value =
                    unsafe { std::slice::from_raw_parts(buf.buf_ptr() as *const u8, buf.item_count()) };
                client.put(k, value)
            });
            let results = futures::future::join_all(futs).await;
            let oks: Vec<bool> = results.iter().map(|r| r.is_ok()).collect();
            handle.resolve(move |py| {
                Ok(pyo3::types::PyList::new(py, oks)?.into_any().unbind())
            });
            drop(bufs);
        }
        Op::BatchGetInto {
            keys,
            dest_ptrs,
            dest_lens,
            _bufs_keepalive,
            handle,
        } => {
            let futs = keys.iter().map(|k| client.get(k));
            let results = futures::future::join_all(futs).await;
            // Any RPC error surfaces as False for that key (cache miss), never
            // an exception — sglang's prefetch budget must not be blown by a
            // raise. A whole-batch transport failure still resolves per-key.
            let mut oks: Vec<bool> = Vec::with_capacity(results.len());
            for (i, res) in results.into_iter().enumerate() {
                let ok = match res {
                    Ok(Some(v)) if v.len() == dest_lens[i] => {
                        // SAFETY: dest_ptrs[i]/dest_lens[i] come from a PyBuffer
                        // kept alive in `_bufs_keepalive` for this whole block.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                v.as_ptr(),
                                dest_ptrs[i] as *mut u8,
                                dest_lens[i],
                            );
                        }
                        true
                    }
                    _ => false,
                };
                oks.push(ok);
            }
            handle.resolve(move |py| {
                Ok(pyo3::types::PyList::new(py, oks)?.into_any().unbind())
            });
            drop(_bufs_keepalive);
        }
        Op::Close { .. } => unreachable!("Close handled in event_loop"),
    }
}

async fn do_batch_delete(client: &ClusterClient, prefix: &[u8]) -> Result<u64, String> {
    let res = client
        .range(prefix, &[], u32::MAX)
        .await
        .map_err(|e| e.to_string())?;
    let mut count = 0u64;
    for entry in &res.entries {
        match client.delete(&entry.key).await {
            Ok(()) => count += 1,
            Err(AutumnError::NotFound) => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(count)
}

// ── Python Client class ─────────────────────────────────────────────────────

#[pyclass]
struct Client {
    tx: Mutex<Option<UnboundedSender<Op>>>,
}

#[pymethods]
impl Client {
    /// Async classmethod: `await Client.connect("127.0.0.1:9001")`.
    /// Spawns the compio worker thread, performs the cluster handshake,
    /// then resolves the returned future with a connected `Client` instance.
    #[staticmethod]
    fn connect<'py>(py: Python<'py>, manager: String) -> PyResult<Bound<'py, PyAny>> {
        let (handle, fut) = make_handle(py)?;
        let (tx, rx) = unbounded::<Op>();

        std::thread::Builder::new()
            .name("autumn-py-compio".into())
            .spawn(move || {
                let rt = match compio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        handle.reject(format!("compio runtime: {e}"));
                        return;
                    }
                };
                rt.block_on(async move {
                    match ClusterClient::connect(&manager).await {
                        Ok(client) => {
                            // Move `tx` into the Python Client object so the
                            // caller can submit further ops; resolve future.
                            handle.resolve(move |py| {
                                let py_client = Client {
                                    tx: Mutex::new(Some(tx)),
                                };
                                Py::new(py, py_client).map(|p| p.into_any())
                            });
                            event_loop(client, rx).await;
                        }
                        Err(e) => handle.reject(format!("connect failed: {e}")),
                    }
                });
            })
            .map_err(|e| PyRuntimeError::new_err(format!("spawn worker: {e}")))?;

        Ok(fut)
    }

    fn put<'py>(
        &self,
        py: Python<'py>,
        key: &[u8],
        value: &[u8],
    ) -> PyResult<Bound<'py, PyAny>> {
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            handle,
        })?;
        Ok(fut)
    }

    fn get<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::Get {
            key: key.to_vec(),
            handle,
        })?;
        Ok(fut)
    }

    fn delete<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::Delete {
            key: key.to_vec(),
            handle,
        })?;
        Ok(fut)
    }

    /// Range scan across all partitions. Returns list of `(key, value)` tuples.
    #[pyo3(signature = (prefix, start=vec![], limit=100))]
    fn range<'py>(
        &self,
        py: Python<'py>,
        prefix: Vec<u8>,
        start: Vec<u8>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::Range {
            prefix,
            start,
            limit,
            handle,
        })?;
        Ok(fut)
    }

    /// Delete all keys with `prefix`. Returns number of keys deleted.
    fn batch_delete<'py>(
        &self,
        py: Python<'py>,
        prefix: &[u8],
    ) -> PyResult<Bound<'py, PyAny>> {
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::BatchDelete {
            prefix: prefix.to_vec(),
            handle,
        })?;
        Ok(fut)
    }

    /// Existence check — returns True if the key is present, False otherwise.
    /// Does NOT transfer the value, so it's the right primitive for sglang
    /// HiCache's `batch_exists` admission probe.
    fn head<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::Head {
            key: key.to_vec(),
            handle,
        })?;
        Ok(fut)
    }

    /// Batched existence probe. Pipelines all `head` RPCs concurrently and
    /// resolves to a `list[bool]` (found per key, in input order). This is the
    /// throughput path for sglang HiCache `batch_exists`, which probes up to
    /// `storage_batch_size` keys before each prefetch.
    fn batch_head<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::BatchHead { keys, handle })?;
        Ok(fut)
    }

    /// Zero-copy put. `buf` is any Python buffer-protocol object (numpy array,
    /// torch tensor, memoryview, etc.) holding the value bytes. The buffer's
    /// memory is read directly when the put RPC is encoded — no intermediate
    /// Python `bytes` is allocated.
    ///
    /// Requires `buf` to be C-contiguous. Returns an awaitable that resolves
    /// to None on success and raises on RPC error.
    fn put_from<'py>(
        &self,
        py: Python<'py>,
        key: &[u8],
        buf: PyBuffer<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if !buf.is_c_contiguous() {
            return Err(PyValueError::new_err("buf must be C-contiguous"));
        }
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::PutFrom {
            key: key.to_vec(),
            buf,
            handle,
        })?;
        Ok(fut)
    }

    /// Zero-copy get. `buf` is a writable Python buffer-protocol object whose
    /// length matches the stored value's length. The value is written directly
    /// into `buf` without going through a Python `bytes`.
    ///
    /// Returns an awaitable that resolves to:
    ///   - `True`  — value found and successfully copied into `buf`.
    ///   - `False` — key missing OR stored value size != buf length (treated
    ///               as a cache miss for the sglang HiCache backend).
    /// Raises on RPC error.
    fn get_into<'py>(
        &self,
        py: Python<'py>,
        key: &[u8],
        buf: PyBuffer<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if buf.readonly() {
            return Err(PyValueError::new_err("buf must be writable"));
        }
        if !buf.is_c_contiguous() {
            return Err(PyValueError::new_err("buf must be C-contiguous"));
        }
        let dest_ptr = buf.buf_ptr() as usize;
        let dest_len = buf.item_count();
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::GetInto {
            key: key.to_vec(),
            dest_ptr,
            dest_len,
            _buf_keepalive: buf,
            handle,
        })?;
        Ok(fut)
    }

    /// Batched zero-copy put. `keys` and `bufs` are parallel lists; all puts
    /// are pipelined concurrently on the worker thread (the partition server
    /// group-commits them). One awaitable resolves to a `list[bool]`, where
    /// element i is True iff `keys[i]` was stored successfully.
    ///
    /// This is the throughput path for sglang HiCache `batch_set_v1`: a single
    /// op carrying N pages instead of N separate round-trips.
    fn batch_put_from<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<Vec<u8>>,
        bufs: Vec<PyBuffer<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if keys.len() != bufs.len() {
            return Err(PyValueError::new_err("keys and bufs must have equal length"));
        }
        for buf in &bufs {
            if !buf.is_c_contiguous() {
                return Err(PyValueError::new_err("every buf must be C-contiguous"));
            }
        }
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::BatchPutFrom { keys, bufs, handle })?;
        Ok(fut)
    }

    /// Batched zero-copy get. `keys` and `bufs` are parallel lists; all gets
    /// are pipelined concurrently. One awaitable resolves to a `list[bool]`,
    /// where element i is True iff `keys[i]` was found AND its stored size
    /// matched `bufs[i]` length (value copied directly into `bufs[i]`).
    ///
    /// This is the throughput path for sglang HiCache `batch_get_v1`.
    fn batch_get_into<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<Vec<u8>>,
        bufs: Vec<PyBuffer<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if keys.len() != bufs.len() {
            return Err(PyValueError::new_err("keys and bufs must have equal length"));
        }
        let mut dest_ptrs = Vec::with_capacity(bufs.len());
        let mut dest_lens = Vec::with_capacity(bufs.len());
        for buf in &bufs {
            if buf.readonly() {
                return Err(PyValueError::new_err("every buf must be writable"));
            }
            if !buf.is_c_contiguous() {
                return Err(PyValueError::new_err("every buf must be C-contiguous"));
            }
            dest_ptrs.push(buf.buf_ptr() as usize);
            dest_lens.push(buf.item_count());
        }
        let (handle, fut) = make_handle(py)?;
        self.dispatch(Op::BatchGetInto {
            keys,
            dest_ptrs,
            dest_lens,
            _bufs_keepalive: bufs,
            handle,
        })?;
        Ok(fut)
    }

    /// Close the client. Idempotent. Returns an awaitable that resolves
    /// once the worker thread has stopped accepting new ops.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (handle, fut) = make_handle(py)?;
        let tx = self.tx.lock().unwrap().take();
        match tx {
            Some(tx) => {
                if tx.unbounded_send(Op::Close { handle }).is_err() {
                    // Worker already exited; drop the inner handle and
                    // resolve immediately on next-best-effort. We need a
                    // fresh handle here because the original was consumed.
                    let (handle2, fut2) = make_handle(py)?;
                    handle2.resolve(|py| Ok(py.None()));
                    return Ok(fut2);
                }
                drop(tx); // releases our Sender; worker drains then exits
                Ok(fut)
            }
            None => {
                handle.resolve(|py| Ok(py.None()));
                Ok(fut)
            }
        }
    }
}

impl Client {
    fn dispatch(&self, op: Op) -> PyResult<()> {
        let guard = self.tx.lock().unwrap();
        let tx = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("client is closed"))?;
        tx.unbounded_send(op)
            .map_err(|_| PyRuntimeError::new_err("worker thread died"))
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Drop the sender; the worker observes channel disconnect and
        // exits its event loop. Thread is detached — no join.
        let _ = self.tx.lock().unwrap().take();
    }
}

// ── Transport selection ──────────────────────────────────────────────────────

/// F216-E: tracks whether the process transport is UCX, so the batch path can
/// default to zero-copy ("ucx ⟹ zerocopy") with no explicit flag. Set by
/// `set_transport`; read at `BatchClient` construction. `autumn_transport`'s
/// `is_ucx` lives on `ReadHalf` (per-conn), not globally, so we mirror it here.
static IS_UCX: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Select the process-global transport. Must be called BEFORE the first
/// `Client.connect`. Idempotent (first call wins, per
/// `autumn_transport::init_with`). `"tcp"` (default) or `"ucx"`.
///
/// `"ucx"` requires the wheel to be built with `maturin build --features ucx`;
/// otherwise it raises (the underlying transport panics on a UCX request in a
/// non-UCX build, which we surface as a Python error here).
#[pyfunction]
fn set_transport(kind: &str) -> PyResult<()> {
    match kind {
        "tcp" => {
            autumn_transport::init_with(autumn_transport::TransportKind::Tcp);
            IS_UCX.store(false, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        "ucx" => {
            #[cfg(feature = "ucx")]
            {
                // F216-E: this process becomes an RDMA endpoint — UCX rc_mlx5
                // pins registered send/recv buffers (ibv_reg_mr) against
                // RLIMIT_MEMLOCK. The default soft limit (often 8 MiB) faults
                // libibverbs on the first large transfer (observed: client
                // SIGSEGV in ucp_worker_progress under 256 KiB+ pages). Raise to
                // INFINITY (falls back to soft-up-to-hard if INFINITY is refused;
                // raising the HARD limit needs CAP_SYS_RESOURCE, so the launcher /
                // systemd `LimitMEMLOCK=infinity` is the production backstop).
                #[cfg(unix)]
                unsafe {
                    let inf = libc::rlimit {
                        rlim_cur: libc::RLIM_INFINITY,
                        rlim_max: libc::RLIM_INFINITY,
                    };
                    if libc::setrlimit(libc::RLIMIT_MEMLOCK, &inf) != 0 {
                        let mut ml = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
                        if libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut ml) == 0
                            && ml.rlim_cur < ml.rlim_max
                        {
                            ml.rlim_cur = ml.rlim_max;
                            libc::setrlimit(libc::RLIMIT_MEMLOCK, &ml);
                        }
                    }
                }
                autumn_transport::init_with(autumn_transport::TransportKind::Ucx);
                IS_UCX.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
            #[cfg(not(feature = "ucx"))]
            {
                Err(PyRuntimeError::new_err(
                    "autumn was built without the `ucx` feature; \
                     rebuild with `maturin build --features ucx`",
                ))
            }
        }
        other => Err(PyValueError::new_err(format!(
            "transport must be 'tcp' or 'ucx', got {other:?}"
        ))),
    }
}

// ── BatchClient: GIL-releasing multi-worker batch engine ─────────────────────
//
// The async `Client` above pays per-op asyncio + GIL overhead: each get/put is
// a Python future resolved via `call_soon_threadsafe`. For a 128-page batch
// that's 128 GIL-serialized round-trips → ~480 MB/s ceiling regardless of how
// many client workers you stack (the GIL serializes the marshaling).
//
// `BatchClient` removes that: ONE PyO3 call extracts all N (key, dest-ptr,
// len) under the GIL, then `py.allow_threads` releases it while K dedicated
// worker threads — each with its own compio runtime + ClusterClient + UCX
// worker — pipeline the transfers and memcpy directly into the pinned dest
// pages in Rust. No per-op Python, no asyncio. Concurrency per worker is
// capped (`buffered`) to stay under the UCX single-worker rendezvous cliff.

use std::sync::mpsc as smpsc;

#[derive(Clone, Copy)]
enum BatchOp {
    Get,
    Put,
}

/// One page of work. `ptr`/`len` address a pinned host page kept alive by the
/// PyBuffer held in the calling PyO3 method across `allow_threads`.
struct WorkItem {
    key: Vec<u8>,
    ptr: usize,
    len: usize,
}
// SAFETY: `ptr` is a raw address into a Python buffer the caller pins for the
// duration of the batch; only the owning worker thread dereferences it, and
// only while the PyO3 method is blocked in `allow_threads` (no concurrent
// Python access).
unsafe impl Send for WorkItem {}

struct BatchJob {
    op: BatchOp,
    items: Vec<WorkItem>,
    done: smpsc::Sender<Vec<bool>>,
}

struct BatchWorker {
    job_tx: UnboundedSender<BatchJob>,
}

#[pyclass]
struct BatchClient {
    workers: Vec<BatchWorker>,
    per_worker_cap: usize,
    /// F216-E: captured from the process transport at construction. True ⟹
    /// zero-copy data path (writes always; reads above UCX_ZC_READ_MIN_BYTES).
    is_ucx: bool,
}

async fn run_job(client: &ClusterClient, op: BatchOp, items: Vec<WorkItem>, cap: usize) -> Vec<bool> {
    let cap = cap.max(1);
    match op {
        BatchOp::Get => {
            // F244-D: consolidate onto `ClusterClient::get_many_into` — the ONE
            // batch-read fan-out primitive (also driving the e2e-tested SDK path).
            // Drops the hand-rolled `buffered` loop + the per-item `zc_worthwhile`
            // decision (which used to drift across callers — see F234); the ZC rule
            // now lives once, inside `get_many_into`. `cap` (per_worker_cap) is the
            // concurrency window.
            // SAFETY: each `it.ptr/len` addresses a pinned dest page the caller keeps
            // alive for the whole batch; disjoint pages → disjoint `&mut` slices;
            // single-threaded compio (no parallel aliasing).
            let mut gitems: Vec<autumn_client::GetManyItem> = items
                .iter()
                .map(|it| autumn_client::GetManyItem {
                    key: &it.key,
                    offset: 0,
                    length: 0,
                    dest: unsafe { std::slice::from_raw_parts_mut(it.ptr as *mut u8, it.len) },
                    reg: None,
                })
                .collect();
            let results = client.get_many_into(&mut gitems, cap).await;
            drop(gitems);
            // Success = the value was found AND its length matches the page size
            // the caller reserved (same contract as the pre-F244 per-item path).
            results
                .iter()
                .zip(items.iter())
                .map(|(r, it)| matches!(r, Ok(Some(n)) if *n == it.len))
                .collect()
        }
        BatchOp::Put => {
            // F246: consolidate onto `ClusterClient::put_many` (fan_out) — the write
            // mirror of the Get arm. The ZC decision (`zc_worthwhile`) + per-item
            // put_zc/put dispatch now live once inside `put_many`. The value is a
            // `Bytes::from_static` view aliasing the pinned source page.
            // SAFETY: each `it.ptr/len` addresses a pinned source page the caller
            // keeps alive for the whole batch (until `allow_threads` returns); the
            // `Bytes::from_static` view is consumed within the call and its Drop is a
            // no-op (never frees the page).
            let pitems: Vec<(&[u8], bytes::Bytes)> = items
                .iter()
                .map(|it| {
                    let page: &'static [u8] =
                        unsafe { std::slice::from_raw_parts(it.ptr as *const u8, it.len) };
                    (it.key.as_slice(), bytes::Bytes::from_static(page))
                })
                .collect();
            let results = client.put_many(&pitems, cap).await;
            drop(pitems);
            results.iter().map(|r| r.is_ok()).collect()
        }
    }
}

#[pymethods]
impl BatchClient {
    /// Blocking constructor: spawn `n_workers` threads, each connecting its own
    /// ClusterClient, and wait until all are ready. `per_worker_cap` bounds the
    /// in-flight pipeline depth per worker (keep ≤ ~16-32 on UCX to dodge the
    /// rendezvous cliff).
    #[new]
    #[pyo3(signature = (manager, n_workers=4, per_worker_cap=16))]
    fn new(py: Python<'_>, manager: String, n_workers: usize, per_worker_cap: usize) -> PyResult<Self> {
        let n_workers = n_workers.max(1);
        // F216-E "ucx ⟹ zerocopy": derive the data path from the process
        // transport (set via set_transport BEFORE construction) — no explicit
        // zc flag. UCX → zero-copy (writes always; reads size-guarded).
        let is_ucx = IS_UCX.load(std::sync::atomic::Ordering::SeqCst);
        let result: Result<Vec<BatchWorker>, String> = py.allow_threads(|| {
            let mut workers = Vec::with_capacity(n_workers);
            for i in 0..n_workers {
                let (job_tx, mut job_rx) = unbounded::<BatchJob>();
                let (ready_tx, ready_rx) = smpsc::channel::<Result<(), String>>();
                let endpoint = manager.clone();
                std::thread::Builder::new()
                    .name(format!("autumn-batch-{i}"))
                    .spawn(move || {
                        let rt = match compio::runtime::Runtime::new() {
                            Ok(rt) => rt,
                            Err(e) => {
                                let _ = ready_tx.send(Err(format!("compio runtime: {e}")));
                                return;
                            }
                        };
                        rt.block_on(async move {
                            let client = match ClusterClient::connect(&endpoint).await {
                                Ok(c) => {
                                    let _ = ready_tx.send(Ok(()));
                                    c
                                }
                                Err(e) => {
                                    let _ = ready_tx.send(Err(e.to_string()));
                                    return;
                                }
                            };
                            while let Some(job) = job_rx.next().await {
                                let res = run_job(&client, job.op, job.items, per_worker_cap).await;
                                let _ = job.done.send(res);
                            }
                        });
                    })
                    .map_err(|e| format!("spawn batch worker: {e}"))?;
                ready_rx
                    .recv()
                    .map_err(|_| "batch worker died during connect".to_string())??;
                workers.push(BatchWorker { job_tx });
            }
            Ok(workers)
        });
        let workers = result.map_err(PyRuntimeError::new_err)?;
        Ok(BatchClient { workers, per_worker_cap, is_ucx })
    }

    /// Whether this client uses the F216-E zero-copy data path. This is now
    /// derived from the process transport (True ⟺ `set_transport("ucx")` was
    /// called before construction): writes are zero-copy (MSG_PUT_ZC) always,
    /// reads (MSG_GET_ZC) for values ≥ the size threshold. No explicit flag.
    fn zc(&self) -> bool {
        self.is_ucx
    }

    /// Batched zero-copy get. Returns list[bool] aligned to `keys`: True iff the
    /// key was found AND its stored size == the buffer length (value copied into
    /// the buffer). Fans across worker threads with the GIL released.
    fn get_into(&self, py: Python<'_>, keys: Vec<Vec<u8>>, bufs: Vec<PyBuffer<u8>>) -> PyResult<Vec<bool>> {
        self.run_batch(py, BatchOp::Get, keys, bufs, true)
    }

    /// Batched zero-copy put. Returns list[bool] aligned to `keys`.
    fn put_from(&self, py: Python<'_>, keys: Vec<Vec<u8>>, bufs: Vec<PyBuffer<u8>>) -> PyResult<Vec<bool>> {
        self.run_batch(py, BatchOp::Put, keys, bufs, false)
    }

    fn n_workers(&self) -> usize {
        self.workers.len()
    }
}

impl BatchClient {
    fn run_batch(
        &self,
        py: Python<'_>,
        op: BatchOp,
        keys: Vec<Vec<u8>>,
        bufs: Vec<PyBuffer<u8>>,
        writable: bool,
    ) -> PyResult<Vec<bool>> {
        let n = keys.len();
        if bufs.len() != n {
            return Err(PyValueError::new_err("keys and bufs must have equal length"));
        }
        // Extract raw (ptr, len) under the GIL; validate buffers.
        let mut items: Vec<WorkItem> = Vec::with_capacity(n);
        for (k, buf) in keys.into_iter().zip(bufs.iter()) {
            if writable && buf.readonly() {
                return Err(PyValueError::new_err("every buf must be writable"));
            }
            if !buf.is_c_contiguous() {
                return Err(PyValueError::new_err("every buf must be C-contiguous"));
            }
            items.push(WorkItem {
                key: k,
                ptr: buf.buf_ptr() as usize,
                len: buf.item_count(),
            });
        }

        let k = self.workers.len();
        // Split into k contiguous shards (preserves order on reassembly).
        let per = n.div_ceil(k).max(1);
        let mut shards: Vec<Vec<WorkItem>> = Vec::with_capacity(k);
        let mut it = items.into_iter();
        for _ in 0..k {
            let chunk: Vec<WorkItem> = it.by_ref().take(per).collect();
            if chunk.is_empty() {
                break;
            }
            shards.push(chunk);
        }

        let results = py.allow_threads(|| {
            let mut dones: Vec<(smpsc::Receiver<Vec<bool>>, usize)> = Vec::with_capacity(shards.len());
            for (wi, chunk) in shards.into_iter().enumerate() {
                let chunk_len = chunk.len();
                let (done_tx, done_rx) = smpsc::channel::<Vec<bool>>();
                let job = BatchJob { op, items: chunk, done: done_tx };
                if self.workers[wi].job_tx.unbounded_send(job).is_err() {
                    // Worker dead: synthesize a failed chunk so output stays aligned.
                    let (tx, rx) = smpsc::channel::<Vec<bool>>();
                    let _ = tx.send(vec![false; chunk_len]);
                    dones.push((rx, chunk_len));
                } else {
                    dones.push((done_rx, chunk_len));
                }
            }
            let mut out: Vec<bool> = Vec::with_capacity(n);
            for (rx, chunk_len) in dones {
                match rx.recv() {
                    Ok(part) => out.extend(part),
                    Err(_) => out.extend(std::iter::repeat(false).take(chunk_len)),
                }
            }
            out
        });
        drop(bufs); // release pinned buffers only after all workers finished
        Ok(results)
    }
}

// ── Python module ───────────────────────────────────────────────────────────

#[pymodule]
fn autumn(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<BatchClient>()?;
    m.add_function(wrap_pyfunction!(set_transport, m)?)?;
    Ok(())
}
