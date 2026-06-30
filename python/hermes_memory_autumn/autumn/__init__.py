"""autumn — a Hermes Agent `MemoryProvider` backed by autumn-memory.

A user-installed Hermes memory plugin (ships to ``$HERMES_HOME/plugins/autumn/``;
see the README). It gives a Hermes agent cross-session recall on the autumn-rs
cluster with no extra daemon — every hook delegates to
``autumn_memory.AutumnMemory`` (the ergonomic layer over the Rust core).

Wiring (Hermes calls these — see agent/memory_provider.py):
  initialize        connect AutumnMemory, derive the (tenant, agent) namespace
                    from the Hermes identity, start a background writer
  sync_turn         append the turn to the episodic log + index it for recall
                    (non-blocking: queued to the background worker)
  queue_prefetch /  recall relevant memory for the next turn (background) and
  prefetch          return the cached block (fast)
  get_tool_schemas  expose `memory_search` / `memory_store` to the model
  handle_tool_call  dispatch those tools
  on_memory_write   mirror Hermes's built-in MEMORY.md / USER.md writes as facts
  shutdown          drain the queue + close

Config: the cluster manager address comes from ``AUTUMN_MEMORY_MANAGER`` (env)
or, if present, the Hermes config key ``memory.autumn.manager``. The (tenant,
agent) namespace is derived from the identity kwargs Hermes passes to
``initialize`` (``user_id`` / ``agent_identity`` / ``agent_workspace``). An
optional embedder (``AUTUMN_MEMORY_EMBED_URL`` + ``_MODEL``) enables the
vector / hybrid recall leg; without it, recall is lexical (BM25).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

from agent.memory_provider import MemoryProvider

logger = logging.getLogger(__name__)

MEMORY_SEARCH_SCHEMA = {
    "name": "memory_search",
    "description": (
        "Search your persistent cross-session memory for relevant past facts, "
        "turns, or notes. Use before answering questions about the user or "
        "prior work."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "What to recall."},
            "k": {"type": "integer", "description": "Max results (default 5).", "default": 5},
        },
        "required": ["query"],
    },
}

MEMORY_STORE_SCHEMA = {
    "name": "memory_store",
    "description": "Store a durable memory the user would expect you to remember later.",
    "parameters": {
        "type": "object",
        "properties": {
            "text": {"type": "string", "description": "The memory to store."},
            "id": {"type": "string", "description": "Optional stable id (else content-derived)."},
        },
        "required": ["text"],
    },
}


def _cfg_manager() -> str:
    addr = os.environ.get("AUTUMN_MEMORY_MANAGER")
    if addr:
        return addr
    try:  # best-effort: pick up the Hermes config key if running inside Hermes
        from hermes_cli.config import cfg_get, load_config  # type: ignore

        return cfg_get(load_config(), "memory", "autumn", "manager") or ""
    except Exception:
        return ""


def _embedder():
    url = os.environ.get("AUTUMN_MEMORY_EMBED_URL")
    model = os.environ.get("AUTUMN_MEMORY_EMBED_MODEL")
    if url and model:
        from autumn_memory import http_embedder

        return http_embedder(url, model)
    return None


class AutumnMemoryProvider(MemoryProvider):
    @property
    def name(self) -> str:
        return "autumn"

    # -- core lifecycle ------------------------------------------------------

    def is_available(self) -> bool:
        # config + deps only, NO network (per the ABC contract).
        try:
            import autumn_memory  # noqa: F401
        except Exception:
            return False
        return bool(_cfg_manager())

    def initialize(self, session_id: str, **kwargs) -> None:
        from autumn_memory import AutumnMemory

        self._session_id = session_id or "default"
        # namespace from the Hermes identity; tenant groups an org/user, agent
        # the profile. (Prefix isolation only — real multi-tenant authz is
        # server-side, plan §9.5.)
        self._tenant = str(kwargs.get("user_id") or kwargs.get("agent_workspace") or "hermes")
        self._agent = str(kwargs.get("agent_identity") or "default")
        # skip writes for non-primary contexts (cron/subagent/flush) — their
        # system prompts would corrupt the user's memory.
        self._write_enabled = kwargs.get("agent_context", "primary") in (None, "primary")
        self._mem = AutumnMemory(_cfg_manager(), self._tenant, self._agent, embed=_embedder())

        self._q: "queue.Queue" = queue.Queue()
        self._prefetched: Dict[str, str] = {}
        self._stop = False
        self._worker = threading.Thread(
            target=self._run_worker, name="autumn-mem-worker", daemon=True
        )
        self._worker.start()

    def _run_worker(self) -> None:
        while not self._stop:
            try:
                job = self._q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                job()
            except Exception:
                # best-effort (don't kill the agent loop), but NEVER silent —
                # a swallowed write is lost memory the user can't diagnose.
                logger.exception("autumn memory worker job failed")
            finally:
                self._q.task_done()

    def flush(self, timeout: float = 10.0) -> None:
        """Block until queued writes/recalls drain (used by tests + session end).

        Waits on ``unfinished_tasks`` (decremented by the worker's ``task_done``)
        so a job already taken off the queue but still EXECUTING is also waited
        for — ``Queue.empty()`` alone would return as soon as the last job is
        dequeued, before its append/remember actually lands."""
        deadline = time.monotonic() + timeout
        while self._q.unfinished_tasks > 0 and time.monotonic() < deadline:
            time.sleep(0.02)
        if self._q.unfinished_tasks > 0:
            logger.warning("autumn memory flush timed out with %d job(s) in flight",
                           self._q.unfinished_tasks)

    # -- recall --------------------------------------------------------------

    def _hit_text(self, hit: Dict[str, Any]) -> Optional[str]:
        # lexical hits carry text; vector/hybrid hits carry only id+score, so
        # resolve via the authoritative doc record. Tolerate a single bad hit.
        text = hit.get("text")
        if text is None:
            try:
                got = self._mem.get(hit["id"])
            except Exception:
                logger.exception("autumn memory: resolve failed for %s", hit.get("id"))
                return None
            text = got["text"] if got else None
        return text

    def _recall(self, query: str, k: int = 5) -> str:
        if not query.strip():
            return ""
        try:
            hits = self._mem.search(query, k, mode="auto")
        except Exception:
            logger.exception("autumn memory: recall search failed")
            return ""
        lines = [f"- {t}" for t in (self._hit_text(h) for h in hits) if t]
        return "## Relevant memory (autumn)\n" + "\n".join(lines) if lines else ""

    def queue_prefetch(self, query: str, *, session_id: str = "") -> None:
        sid = session_id or self._session_id
        if not query.strip():
            return
        self._q.put(lambda: self._prefetched.__setitem__(sid, self._recall(query)))

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        sid = session_id or self._session_id
        cached = self._prefetched.pop(sid, None)
        if cached is not None:
            return cached
        return self._recall(query)  # no prior queue_prefetch — recall directly

    # -- writes (non-blocking: queued) ---------------------------------------

    def sync_turn(
        self,
        user_content: str,
        assistant_content: str,
        *,
        session_id: str = "",
        messages: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        if not self._write_enabled:
            return
        sid = session_id or self._session_id
        # ms timestamp + a uuid suffix so two turns in the same ms (fast/replay/
        # concurrent gateway sessions) don't collide and overwrite each other.
        doc_id = f"turn:{sid}:{int(time.time() * 1000)}:{uuid.uuid4().hex[:8]}"

        def _job():
            self._mem.append(sid, {"role": "user", "content": user_content})
            self._mem.append(sid, {"role": "assistant", "content": assistant_content})
            self._mem.remember(
                doc_id,
                f"{user_content}\n{assistant_content}".strip(),
                {"session": sid, "kind": "turn"},
            )

        self._q.put(_job)

    def on_memory_write(
        self,
        action: str,
        target: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        # mirror the built-in MEMORY.md / USER.md writes into autumn (best-effort
        # mirror, not a replacement — plan §13).
        if not self._write_enabled or not content:
            return
        ns = f"builtin:{target}"
        key = hashlib.sha1(content.encode("utf-8")).hexdigest()[:16]
        # on replace, Hermes passes the prior text in metadata["old_text"]; reap
        # its fact so the mirror doesn't accumulate superseded entries.
        old_text = (metadata or {}).get("old_text") if action == "replace" else None
        old_key = hashlib.sha1(old_text.encode("utf-8")).hexdigest()[:16] if old_text else None

        def _job():
            if action == "remove":
                self._mem.delete_fact(ns, key)
            else:  # add / replace
                if old_key and old_key != key:
                    self._mem.delete_fact(ns, old_key)
                self._mem.put_fact(ns, key, {"content": content, "metadata": metadata})

        self._q.put(_job)

    def on_session_end(self, messages: List[Dict[str, Any]]) -> None:
        # turns are already persisted per-turn; just make sure the queue drains.
        self.flush()

    # -- tools ---------------------------------------------------------------

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        return [MEMORY_SEARCH_SCHEMA, MEMORY_STORE_SCHEMA]

    def handle_tool_call(self, tool_name: str, args: Dict[str, Any], **kwargs) -> str:
        if tool_name == "memory_search":
            hits = self._mem.search(str(args.get("query", "")), int(args.get("k", 5)), mode="auto")
            results = []
            for h in hits:
                # backfill text for vector/hybrid hits, else the tool returns
                # bare id+score and is useless to the model.
                text = self._hit_text(h)
                item = {"id": h["id"], "score": h.get("score")}
                if text is not None:
                    item["text"] = text
                results.append(item)
            return json.dumps({"results": results})
        if tool_name == "memory_store":
            text = str(args.get("text", ""))
            doc_id = args.get("id") or "mem:" + hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]
            self._mem.remember(doc_id, text, args.get("metadata"))
            return json.dumps({"id": doc_id, "stored": True})
        return json.dumps({"error": f"unknown tool {tool_name}"})

    def system_prompt_block(self) -> str:
        return (
            "You have persistent cross-session memory (autumn). Relevant past memory is "
            "auto-injected each turn; use `memory_search` to recall more and `memory_store` "
            "to save durable facts."
        )

    def shutdown(self) -> None:
        self.flush(timeout=3.0)
        self._stop = True
        try:
            self._worker.join(timeout=2.0)
        except Exception:
            pass
        try:
            self._mem.close()
        except Exception:
            pass


def register(ctx) -> None:
    """Hermes plugin entry point — register the provider."""
    ctx.register_memory_provider(AutumnMemoryProvider())
