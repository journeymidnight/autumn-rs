"""Functional test for the LangGraph `AutumnStore` against an isolated cluster.

Drives the public `BaseStore` surface (put/get/search/delete/list_namespaces),
which dispatches through `batch`, so it exercises the whole adapter. Mirrors the
behaviors LangGraph's own InMemoryStore guarantees.

Run via tests/run_store_test.sh. Standalone:
    AUTUMN_MEMORY_MANAGER=127.0.0.1:19001 python tests/test_store_inproc.py
"""

import os
import time
import uuid

from autumn_memory_langgraph import AutumnStore


def main():
    manager = os.environ.get("AUTUMN_MEMORY_MANAGER", "127.0.0.1:19001")
    agent = "lg-" + uuid.uuid4().hex[:8]
    store = AutumnStore.connect(manager, "__am_lg", agent)

    users = ("users", "alice")
    # put + get round-trip (dict value)
    store.put(users, "profile", {"name": "Alice", "lang": "rust", "city": "berlin"})
    it = store.get(users, "profile")
    assert it is not None and it.value["name"] == "Alice", it
    assert it.namespace == users and it.key == "profile", it
    created0 = it.created_at

    # missing key → None
    assert store.get(users, "nope") is None

    # update preserves created_at, bumps updated_at
    store.put(users, "profile", {"name": "Alice", "lang": "python", "city": "berlin"})
    it2 = store.get(users, "profile")
    assert it2.value["lang"] == "python", it2
    assert it2.created_at == created0, (it2.created_at, created0)
    assert it2.updated_at >= it2.created_at

    # more items across namespaces
    store.put(("users", "bob"), "profile", {"name": "Bob", "lang": "go", "city": "berlin"})
    store.put(("users", "alice", "prefs"), "ui", {"theme": "dark", "city": "berlin"})

    # search by namespace prefix
    hits = store.search(("users",))
    keys = {(h.namespace, h.key) for h in hits}
    assert (("users", "alice"), "profile") in keys, keys
    assert (("users", "bob"), "profile") in keys, keys
    assert (("users", "alice", "prefs"), "ui") in keys, keys

    # filter: exact-match on a value field
    berlin = store.search(("users",), filter={"city": "berlin"})
    assert len(berlin) == 3, [h.key for h in berlin]
    pythonistas = store.search(("users",), filter={"lang": "python"})
    assert [(h.namespace, h.key) for h in pythonistas] == [(("users", "alice"), "profile")], pythonistas

    # query: lexical ranking (the 'go' profile should top a 'go' query)
    ranked = store.search(("users",), query="go bob")
    assert ranked[0].namespace == ("users", "bob"), [(h.namespace, h.score) for h in ranked]
    assert ranked[0].score is not None and ranked[0].score > 0

    # filter operators (coco P2#1): $gt / $ne / $in, not just equality
    store.put(("nums",), "a", {"score": 20})
    store.put(("nums",), "b", {"score": 5})
    assert [h.key for h in store.search(("nums",), filter={"score": {"$gt": 10}})] == ["a"]
    assert [h.key for h in store.search(("nums",), filter={"score": {"$ne": 5}})] == ["a"]
    assert {h.key for h in store.search(("nums",), filter={"score": {"$in": [5, 20]}})} == {"a", "b"}
    assert [h.key for h in store.search(("nums",), filter={"score": {"$lte": 5}})] == ["b"]

    # limit / offset
    page = store.search(("users",), limit=1)
    assert len(page) == 1, page

    # list_namespaces: prefix + max_depth (dedups truncated namespaces)
    nss = store.list_namespaces(prefix=("users",))
    assert ("users", "alice") in nss and ("users", "bob") in nss, nss
    assert ("users", "alice", "prefs") in nss, nss
    shallow = store.list_namespaces(prefix=("users",), max_depth=2)
    assert ("users", "alice") in shallow, shallow
    assert ("users", "alice", "prefs") not in shallow, shallow  # truncated to depth 2

    # suffix match
    prefs = store.list_namespaces(suffix=("prefs",))
    assert prefs == [("users", "alice", "prefs")], prefs

    # delete → gone, and an emptied namespace drops out of list_namespaces
    store.delete(("users", "bob"), "profile")
    assert store.get(("users", "bob"), "profile") is None
    nss2 = store.list_namespaces(prefix=("users",))
    assert ("users", "bob") not in nss2, nss2
    assert ("users", "alice") in nss2, nss2  # still has items

    # ttl path doesn't error (60 min)
    store.put(("eph",), "k", {"x": 1}, ttl=60)
    assert store.get(("eph",), "k").value["x"] == 1

    # TTL expiry (coco P2#2): after the only item expires, the namespace must
    # NOT linger in list_namespaces and its registry entry is reaped. ttl is in
    # MINUTES → 0.05 min ≈ 3s.
    store.put(("eph2",), "k", {"x": 1}, ttl=0.05)
    assert ("eph2",) in store.list_namespaces(prefix=("eph2",))
    time.sleep(5)
    assert store.get(("eph2",), "k") is None, "ttl item should have expired"
    assert store.list_namespaces(prefix=("eph2",)) == [], "expired namespace must be reaped"

    store.close()
    print("LANGGRAPH STORE OK: BaseStore surface (get/put/search/filter-ops/query/list_namespaces/delete/ttl-expiry-reap)")


if __name__ == "__main__":
    main()
