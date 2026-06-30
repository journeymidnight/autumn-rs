//! Python binding for `autumn_memory::MemoryStore`.
//!
//! A SYNCHRONOUS, blocking façade (unlike the async `Client`): each method
//! ships a job to a dedicated compio worker thread that owns the `!Send`
//! `MemoryStore` and blocks for the result with the GIL released. Sync is what
//! the agent-framework adapters want — Hermes `MemoryProvider.sync_turn` /
//! `prefetch`, LangGraph `BaseStore` — they call into this from their own
//! (daemon) threads.

use std::rc::Rc;
use std::sync::mpsc::Sender;
use std::sync::Mutex;

use autumn_memory::{MemoryStore, ReconcileReport, ScoredDoc};
use futures::channel::mpsc::{unbounded, UnboundedSender};
use futures::future::LocalBoxFuture;
use futures::StreamExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

/// A unit of work for the memory worker: given the store, produce a future that
/// computes a result and sends it back over the job's own result channel.
type MemJob = Box<dyn FnOnce(Rc<MemoryStore>) -> LocalBoxFuture<'static, ()> + Send>;

#[pyclass]
pub struct Memory {
    tx: Mutex<Option<UnboundedSender<MemJob>>>,
}

#[pymethods]
impl Memory {
    /// Connect a store for `(tenant, agent)` against `manager`. Blocks until the
    /// worker has connected (or raises on failure).
    #[staticmethod]
    #[pyo3(signature = (manager, tenant, agent, default_ttl=0))]
    fn connect(
        py: Python<'_>,
        manager: String,
        tenant: String,
        agent: String,
        default_ttl: u64,
    ) -> PyResult<Self> {
        let (job_tx, mut job_rx) = unbounded::<MemJob>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

        std::thread::Builder::new()
            .name("autumn-memory".into())
            .spawn(move || {
                let rt = match compio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = ready_tx.send(Err(format!("compio runtime: {e}")));
                        return;
                    }
                };
                rt.block_on(async move {
                    let store = match MemoryStore::connect(&manager, tenant, agent).await {
                        Ok(s) => {
                            let s = if default_ttl > 0 {
                                s.with_default_ttl(default_ttl)
                            } else {
                                s
                            };
                            Rc::new(s)
                        }
                        Err(e) => {
                            let _ = ready_tx.send(Err(e.to_string()));
                            return;
                        }
                    };
                    let _ = ready_tx.send(Ok(()));
                    while let Some(job) = job_rx.next().await {
                        job(store.clone()).await;
                    }
                });
            })
            .map_err(|e| PyRuntimeError::new_err(format!("spawn worker: {e}")))?;

        match py.allow_threads(move || ready_rx.recv()) {
            Ok(Ok(())) => Ok(Memory {
                tx: Mutex::new(Some(job_tx)),
            }),
            Ok(Err(e)) => Err(PyRuntimeError::new_err(e)),
            Err(_) => Err(PyRuntimeError::new_err("memory worker died during connect")),
        }
    }

    fn close(&self) {
        *self.tx.lock().unwrap() = None;
    }

    // -- episodic ------------------------------------------------------------

    #[pyo3(signature = (session, event, ttl=None))]
    fn append_event(
        &self,
        py: Python<'_>,
        session: String,
        event: Vec<u8>,
        ttl: Option<u64>,
    ) -> PyResult<u64> {
        self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.append_event(&session, &event, ttl)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })
    }

    fn recent_events(&self, py: Python<'_>, session: String, limit: usize) -> PyResult<PyObject> {
        let r: Vec<Vec<u8>> = self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.recent_events(&session, limit)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })?;
        bytes_list(py, r)
    }

    #[pyo3(signature = (session, limit=None))]
    fn replay_session(
        &self,
        py: Python<'_>,
        session: String,
        limit: Option<usize>,
    ) -> PyResult<PyObject> {
        let r: Vec<Vec<u8>> = self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.replay_session(&session, limit)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })?;
        bytes_list(py, r)
    }

    // -- facts ---------------------------------------------------------------

    #[pyo3(signature = (namespace, key, value, ttl=None))]
    fn put_fact(
        &self,
        py: Python<'_>,
        namespace: String,
        key: String,
        value: Vec<u8>,
        ttl: Option<u64>,
    ) -> PyResult<()> {
        self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.put_fact(&namespace, &key, &value, ttl)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })
    }

    fn get_fact(&self, py: Python<'_>, namespace: String, key: String) -> PyResult<PyObject> {
        let r: Option<Vec<u8>> = self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.get_fact(&namespace, &key)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })?;
        Ok(match r {
            Some(v) => PyBytes::new(py, &v).into_any().unbind(),
            None => py.None(),
        })
    }

    fn delete_fact(&self, py: Python<'_>, namespace: String, key: String) -> PyResult<()> {
        self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.delete_fact(&namespace, &key)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })
    }

    #[pyo3(signature = (namespace, limit=None))]
    fn list_facts(
        &self,
        py: Python<'_>,
        namespace: String,
        limit: Option<usize>,
    ) -> PyResult<PyObject> {
        let r: Vec<(String, Vec<u8>)> = self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.list_facts(&namespace, limit)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })?;
        let items: PyResult<Vec<Bound<'_, PyAny>>> = r
            .into_iter()
            .map(|(k, v)| {
                PyTuple::new(
                    py,
                    &[
                        k.into_pyobject(py)?.into_any(),
                        PyBytes::new(py, &v).into_any(),
                    ],
                )
                .map(|t| t.into_any())
            })
            .collect();
        Ok(PyList::new(py, items?)?.into_any().unbind())
    }

    // -- BM25 lexical --------------------------------------------------------

    #[pyo3(signature = (doc_id, text, meta=Vec::new(), ttl=None))]
    fn index_memory(
        &self,
        py: Python<'_>,
        doc_id: String,
        text: String,
        meta: Vec<u8>,
        ttl: Option<u64>,
    ) -> PyResult<()> {
        self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.index_memory(&doc_id, &text, &meta, ttl)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })
    }

    fn delete_memory(&self, py: Python<'_>, doc_id: String) -> PyResult<()> {
        self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(s.delete_memory(&doc_id).await.map_err(|e| e.to_string()));
            })
        })
    }

    /// Fetch one indexed memory by id → `(text, meta)` tuple, or `None`.
    fn get_memory(&self, py: Python<'_>, doc_id: String) -> PyResult<PyObject> {
        let r: Option<(String, Vec<u8>)> = self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(s.get_memory(&doc_id).await.map_err(|e| e.to_string()));
            })
        })?;
        Ok(match r {
            Some((text, meta)) => PyTuple::new(
                py,
                &[
                    text.into_pyobject(py)?.into_any(),
                    PyBytes::new(py, &meta).into_any(),
                ],
            )?
            .into_any()
            .unbind(),
            None => py.None(),
        })
    }

    /// Returns a list of `(doc_id, text, meta, score)` tuples.
    fn search_lexical(&self, py: Python<'_>, query: String, top_k: usize) -> PyResult<PyObject> {
        let r: Vec<ScoredDoc> = self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.search_lexical(&query, top_k)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })?;
        scored_list(py, r)
    }

    // -- IVF vector ----------------------------------------------------------

    #[pyo3(signature = (doc_id, vector, ttl=None))]
    fn index_vector(
        &self,
        py: Python<'_>,
        doc_id: String,
        vector: Vec<f32>,
        ttl: Option<u64>,
    ) -> PyResult<()> {
        self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.index_vector(&doc_id, &vector, ttl)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })
    }

    #[pyo3(signature = (nlist, max_iter=25, seed=1))]
    fn train_centroids(
        &self,
        py: Python<'_>,
        nlist: usize,
        max_iter: usize,
        seed: u64,
    ) -> PyResult<usize> {
        self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.train_centroids(nlist, max_iter, seed)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })
    }

    /// Returns a list of `(doc_id, score)` tuples.
    #[pyo3(signature = (query, top_k, nprobe=8))]
    fn search_vector(
        &self,
        py: Python<'_>,
        query: Vec<f32>,
        top_k: usize,
        nprobe: usize,
    ) -> PyResult<PyObject> {
        let r: Vec<(String, f32)> = self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.search_vector(&query, top_k, nprobe)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })?;
        id_score_list(py, r)
    }

    /// Hybrid (BM25 + IVF) fused by RRF. Returns `(doc_id, fused_score)` tuples.
    #[pyo3(signature = (query_text, query_vec, top_k, nprobe=8))]
    fn search_hybrid(
        &self,
        py: Python<'_>,
        query_text: String,
        query_vec: Vec<f32>,
        top_k: usize,
        nprobe: usize,
    ) -> PyResult<PyObject> {
        let r: Vec<(String, f32)> = self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(
                    s.search_hybrid(&query_text, &query_vec, top_k, nprobe)
                        .await
                        .map_err(|e| e.to_string()),
                );
            })
        })?;
        id_score_list(py, r)
    }

    // -- index reconcile / repair (ops) --------------------------------------

    /// Audit index integrity. Returns a dict of the `ReconcileReport` fields
    /// plus `stats_consistent` / `is_clean`.
    fn reconcile(&self, py: Python<'_>) -> PyResult<PyObject> {
        let r: ReconcileReport = self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(s.reconcile().await.map_err(|e| e.to_string()));
            })
        })?;
        let d = PyDict::new(py);
        d.set_item("docs", r.docs)?;
        d.set_item("sum_doc_len", r.sum_doc_len)?;
        d.set_item("stats_docs", r.stats_docs)?;
        d.set_item("stats_sum_doc_len", r.stats_sum_doc_len)?;
        d.set_item("ivf_postings", r.ivf_postings)?;
        d.set_item("vptrs", r.vptrs)?;
        d.set_item("orphan_ivf", r.orphan_ivf)?;
        d.set_item("duplicate_ivf", r.duplicate_ivf)?;
        d.set_item("dangling_vptr", r.dangling_vptr)?;
        d.set_item("malformed_vptr", r.malformed_vptr)?;
        d.set_item("stats_consistent", r.stats_consistent())?;
        d.set_item("is_clean", r.is_clean())?;
        Ok(d.into_any().unbind())
    }

    /// Rewrite `meta/stats` from a fresh doc recount. MUST run with no
    /// concurrent writer for this (tenant, agent). Returns `(n_docs, sum_len)`.
    fn repair_stats(&self, py: Python<'_>) -> PyResult<(u64, u64)> {
        self.run(py, move |s, tx| {
            Box::pin(async move {
                let _ = tx.send(s.repair_stats().await.map_err(|e| e.to_string()));
            })
        })
    }
}

impl Memory {
    /// Ship a job to the worker and block (GIL released) for its result.
    fn run<T: Send + 'static>(
        &self,
        py: Python<'_>,
        make: impl FnOnce(Rc<MemoryStore>, Sender<Result<T, String>>) -> LocalBoxFuture<'static, ()>
            + Send
            + 'static,
    ) -> PyResult<T> {
        let (rtx, rrx) = std::sync::mpsc::channel::<Result<T, String>>();
        let job: MemJob = Box::new(move |s| make(s, rtx));
        {
            let guard = self.tx.lock().unwrap();
            match guard.as_ref() {
                Some(tx) => tx
                    .unbounded_send(job)
                    .map_err(|_| PyRuntimeError::new_err("memory worker channel closed"))?,
                None => return Err(PyRuntimeError::new_err("memory store is closed")),
            }
        }
        match py.allow_threads(move || rrx.recv()) {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(PyRuntimeError::new_err(e)),
            Err(_) => Err(PyRuntimeError::new_err("memory worker gone")),
        }
    }
}

fn bytes_list(py: Python<'_>, items: Vec<Vec<u8>>) -> PyResult<PyObject> {
    let objs: Vec<Bound<'_, PyAny>> = items
        .iter()
        .map(|v| PyBytes::new(py, v).into_any())
        .collect();
    Ok(PyList::new(py, objs)?.into_any().unbind())
}

fn id_score_list(py: Python<'_>, items: Vec<(String, f32)>) -> PyResult<PyObject> {
    let objs: PyResult<Vec<Bound<'_, PyAny>>> = items
        .into_iter()
        .map(|(id, score)| {
            PyTuple::new(
                py,
                &[id.into_pyobject(py)?.into_any(), score.into_pyobject(py)?.into_any()],
            )
            .map(|t| t.into_any())
        })
        .collect();
    Ok(PyList::new(py, objs?)?.into_any().unbind())
}

fn scored_list(py: Python<'_>, items: Vec<ScoredDoc>) -> PyResult<PyObject> {
    let objs: PyResult<Vec<Bound<'_, PyAny>>> = items
        .into_iter()
        .map(|d| {
            PyTuple::new(
                py,
                &[
                    d.id.into_pyobject(py)?.into_any(),
                    d.text.into_pyobject(py)?.into_any(),
                    PyBytes::new(py, &d.meta).into_any(),
                    d.score.into_pyobject(py)?.into_any(),
                ],
            )
            .map(|t| t.into_any())
        })
        .collect();
    Ok(PyList::new(py, objs?)?.into_any().unbind())
}
