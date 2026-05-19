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

async fn event_loop(mut client: ClusterClient, mut rx: UnboundedReceiver<Op>) {
    while let Some(op) = rx.next().await {
        match op {
            Op::Close { handle } => {
                handle.resolve(|py| Ok(py.None()));
                break;
            }
            other => handle_op(&mut client, other).await,
        }
    }
}

async fn handle_op(client: &mut ClusterClient, op: Op) {
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
        Op::Close { .. } => unreachable!("Close handled in event_loop"),
    }
}

async fn do_batch_delete(client: &mut ClusterClient, prefix: &[u8]) -> Result<u64, String> {
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

// ── Python module ───────────────────────────────────────────────────────────

#[pymodule]
fn autumn(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    Ok(())
}
