"""In-process test for the autumn Hermes `MemoryProvider` against the REAL
Hermes ABC + an isolated autumn cluster.

It loads the actual `agent.memory_provider.MemoryProvider` from a Hermes
checkout (HERMES_AGENT_PATH, default /data/dongmao_dev/hermes-agent), loads our
plugin via importlib (under a non-`autumn` module name so it can't shadow the
`autumn` PyO3 binding), and drives the provider's lifecycle: register, init,
sync_turn → prefetch recall, the memory_search/memory_store tools, and the
built-in-write mirror.

Run via tests/run_hermes_test.sh.
"""

import importlib.util
import json
import os
import sys
import uuid

HERMES = os.environ.get("HERMES_AGENT_PATH", "/data/dongmao_dev/hermes-agent")
sys.path.insert(0, HERMES)  # for `agent.memory_provider`

from agent.memory_provider import MemoryProvider  # noqa: E402  (real Hermes ABC)


def load_plugin():
    # load autumn/__init__.py under a NON-`autumn` name so it doesn't shadow the
    # `autumn` binding (Hermes itself loads it as `_hermes_user_memory.autumn`).
    here = os.path.dirname(os.path.abspath(__file__))
    init = os.path.join(here, "..", "autumn", "__init__.py")
    spec = importlib.util.spec_from_file_location("autumn_hermes_plugin", init)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class Collector:
    """Mimics Hermes's plugin context (captures register_memory_provider)."""

    def __init__(self):
        self.provider = None

    def register_memory_provider(self, p):
        self.provider = p


def main():
    manager = os.environ.get("AUTUMN_MEMORY_MANAGER", "127.0.0.1:19001")
    os.environ["AUTUMN_MEMORY_MANAGER"] = manager
    plugin = load_plugin()

    # the provider IS the real MemoryProvider ABC, and register() wires it
    assert issubclass(plugin.AutumnMemoryProvider, MemoryProvider)
    col = Collector()
    plugin.register(col)
    p = col.provider
    assert isinstance(p, MemoryProvider), p

    # is_available: config + deps present (no network)
    assert p.is_available(), "provider should be available (manager + autumn_memory)"

    agent = "coder-" + uuid.uuid4().hex[:8]
    p.initialize("sess-1", user_id="u1", agent_identity=agent, agent_context="primary")

    # static surface
    assert p.name == "autumn"
    tools = {t["name"] for t in p.get_tool_schemas()}
    assert tools == {"memory_search", "memory_store"}, tools
    assert "memory_search" in p.system_prompt_block()

    # sync_turn persists + indexes; prefetch recalls it
    p.sync_turn("what's my favorite language?", "You like Rust and write autumn-rs.", session_id="sess-1")
    p.flush()
    ctx = p.prefetch("favorite programming language", session_id="sess-1")
    assert "Rust" in ctx, f"prefetch should recall the turn: {ctx!r}"

    # queue_prefetch (background) then prefetch returns the cached block
    p.queue_prefetch("autumn-rs project", session_id="sess-1")
    p.flush()
    ctx2 = p.prefetch("autumn-rs project", session_id="sess-1")
    assert "autumn-rs" in ctx2, f"queued prefetch should be cached: {ctx2!r}"

    # tools: store then search
    r = json.loads(p.handle_tool_call("memory_store", {"text": "the deploy command is make ship"}))
    assert r["stored"] and r["id"], r
    sr = json.loads(p.handle_tool_call("memory_search", {"query": "deploy command", "k": 5}))
    assert any("make ship" in (h.get("text") or "") for h in sr["results"]), sr

    # built-in-write mirror -> a fact under builtin:user
    p.on_memory_write("add", "user", "The user's name is Alice and she likes Rust.")
    p.flush()
    facts = p._mem.list_facts("builtin:user")
    assert any("Alice" in f[1].get("content", "") for f in facts), facts

    # non-primary context skips writes (use a FRESH agent so the skip is observable)
    cron_agent = "cron-" + uuid.uuid4().hex[:8]
    p2 = plugin.AutumnMemoryProvider()
    p2.initialize("sess-cron", user_id="u1", agent_identity=cron_agent, agent_context="cron")
    assert p2._write_enabled is False, "cron context must disable writes"
    p2.sync_turn("remember this cron secret", "ok", session_id="sess-cron")
    p2.on_memory_write("add", "user", "cron should not write this")
    p2.flush()
    assert p2._mem.search("cron", 5, mode="lexical") == [], "cron writes must be skipped"
    assert p2._mem.list_facts("builtin:user") == [], "cron memory-write must be skipped"
    p2.shutdown()

    p.shutdown()
    print("HERMES PROVIDER OK: real MemoryProvider ABC (register/init/sync_turn/prefetch/tools/mirror/contexts)")


if __name__ == "__main__":
    main()
