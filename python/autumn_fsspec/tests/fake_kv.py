"""In-memory stand-in for the `autumn.Client` async surface used by the fsspec
adapter. Lets the whole filesystem be exercised without a live cluster.

Implements exactly the async methods `AutumnFileSystem` calls, with the same
contracts the real Rust client has:
  * `range(prefix, start, limit)` — keys that start with `prefix` and are >=
    `start` (or `prefix` if empty), in byte order, capped at `limit`.
  * `batch_get_into(keys, bufs)` — copies each value into its buffer iff the
    value exists AND its length equals the buffer length; returns list[bool].
    A size mismatch is a False (miss), buffer left untouched.
"""

from __future__ import annotations


class FakeKV:
    def __init__(self):
        self.store: dict[bytes, bytes] = {}

    async def get(self, key):
        return self.store.get(bytes(key))

    async def put(self, key, value, ttl_secs=0):
        self.store[bytes(key)] = bytes(value)

    async def delete(self, key):
        self.store.pop(bytes(key), None)

    async def range(self, prefix, start, limit):
        # The real PS range is a KEYS-ONLY scan — RangeEntry.value is always
        # empty (rpc_handlers.rs range_scan_sst_merge). Mirror that so the
        # adapter's listing path is exercised the same way offline as live.
        prefix, start = bytes(prefix), bytes(start)
        cursor = start if start else prefix
        keys = sorted(k for k in self.store if k.startswith(prefix) and k >= cursor)
        return [(k, b"") for k in keys[:limit]]

    async def batch_delete(self, prefix):
        prefix = bytes(prefix)
        dead = [k for k in self.store if k.startswith(prefix)]
        for k in dead:
            del self.store[k]
        return len(dead)

    async def batch_get_into(self, keys, bufs):
        oks = []
        for k, buf in zip(keys, bufs):
            v = self.store.get(bytes(k))
            if v is not None and len(v) == len(buf):
                buf[:] = v
                oks.append(True)
            else:
                oks.append(False)
        return oks
