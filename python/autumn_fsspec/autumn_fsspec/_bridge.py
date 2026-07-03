"""Sync ↔ async bridge for the autumn PyO3 client.

The `autumn` PyO3 module exposes async methods (each returns an awaitable that
resolves when the compio worker thread completes the RPC). fsspec's API is
synchronous, so — exactly like `autumn_kvcache._bridge` — we own one dedicated
asyncio loop on a daemon thread and dispatch coroutines onto it, blocking the
caller until the result.

Why a *thunk* (zero-arg callable returning an awaitable) rather than a bare
coroutine: autumn's PyO3 methods call `asyncio.get_running_loop()` at the
instant they are invoked, to bind the resolve-future to the loop that will
receive the completion callback. Invoking them on the caller's (loop-less)
thread raises `RuntimeError: no running event loop`. By deferring the call into
`_wrap`, the method runs on the worker loop and `get_running_loop()` succeeds.
"""

from __future__ import annotations

import asyncio
import threading

_LOOP: "asyncio.AbstractEventLoop | None" = None
_LOOP_LOCK = threading.Lock()


def get_loop() -> "asyncio.AbstractEventLoop":
    global _LOOP
    with _LOOP_LOCK:
        if _LOOP is None or not _LOOP.is_running():
            loop = asyncio.new_event_loop()
            threading.Thread(
                target=loop.run_forever, name="autumn-fsspec-loop", daemon=True
            ).start()
            _LOOP = loop
        return _LOOP


def run(thunk):
    """Submit `thunk` to the worker loop and block until its result/exception."""

    async def _wrap():
        return await thunk()

    return asyncio.run_coroutine_threadsafe(_wrap(), get_loop()).result()
