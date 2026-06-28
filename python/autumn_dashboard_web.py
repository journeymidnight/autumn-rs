#!/usr/bin/env python3
"""autumn-rs web dashboard.

A browser UI over the same `autumn-op --json` control plane that
`autumn_dashboard.py` (console) and `node_policy.py` use. Renders the
cluster as a PS -> Partition -> Extents hierarchy and (in "danger" mode)
lets an operator actuate policy ops (split / merge / gc / compact / flush /
force-ec-convert) per target, apply individual advisories, or run the
auto-policy controller on a tick loop.

Design notes:
  * Zero third-party deps — stdlib `http.server` + a single self-contained
    HTML page (vanilla JS, no build step), matching the no-extra-deps
    convention of the Python ops tooling.
  * The autumn-op subprocess boundary, snapshot gather, and the
    candidate -> command + cooldown decision logic are REUSED verbatim from
    `autumn_dashboard.py` (import) — this file only adds the HTTP layer,
    the PS->Partition->Extents hierarchy assembly, and the auto-policy
    background thread.
  * Cluster-mutating actions are gated TWICE: the UI hides the buttons
    until the operator flips the "danger" toggle AND confirms each action,
    and the backend independently whitelists the allowed autumn-op verbs +
    validates args are integers (never trust the client for a mutation).

Usage:
    python3 python/autumn_dashboard_web.py [--manager H:P] [--bin PATH]
                                           [--host 127.0.0.1] [--port 8799]

Then open http://127.0.0.1:8799 .
"""
from __future__ import annotations

import argparse
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Tuple

# Reuse the console dashboard's I/O boundary + decision logic verbatim.
from autumn_dashboard import (  # noqa: E402
    ACTIONABLE_KINDS,
    OpError,
    candidate_to_cmd,
    cooldown_key,
    decide_actions,
    describe_candidate,
    make_op,
)

DEFAULT_MANAGER = os.environ.get("AUTUMN_MANAGER", "127.0.0.1:9001")

# Whitelist of autumn-op verbs the web layer is allowed to actuate. Anything
# not here is refused at the backend regardless of what the client POSTs.
# (The same set the auto-controller / candidate_to_cmd can emit, plus the
# manual per-target ops the UI exposes.)
# NOTE: these are exactly the autumn-op mutation subcommands (no `flush` —
# autumn-op doesn't expose one; FLUSH is a ClusterClient-only maintenance op).
ALLOWED_VERBS = {
    "split",            # split <part_id>
    "merge",            # merge <survivor> <victim>
    "gc",               # gc <part_id>
    "forcegc",          # forcegc <part_id> <ext_id>...
    "compact",          # compact <part_id>
    "force-ec-convert",  # force-ec-convert --extent <ext_id>
}


# ── binary resolution (repo-relative fallback) ───────────────────────────────
def resolve_bin(explicit: Optional[str]) -> str:
    """Find the autumn-op binary. Precedence: --bin > $AUTUMN_OP_BIN >
    ./target/release/autumn-op > ./target/debug/autumn-op (relative to the
    repo root, i.e. this file's parent's parent) > bare "autumn-op" ($PATH).

    Added because the bare-name default kept failing when target/ wasn't on
    PATH — the exact "could not find autumn-op" papercut from the console
    tool."""
    if explicit:
        return explicit
    env = os.environ.get("AUTUMN_OP_BIN")
    if env:
        return env
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for rel in ("target/release/autumn-op", "target/debug/autumn-op"):
        cand = os.path.join(repo_root, rel)
        if os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
    return "autumn-op"


# ── snapshot assembly (scalable: compact overview + lazy per-partition) ───────
#
# Big-cluster strategy (数百 PS / 数千 partition / 数万 extent):
#   * The OVERVIEW (`build_overview`) sends only the bounded, compact partition
#     list (id / ps / range / size / extent-count / stream ids) — range-sorted.
#     The giant `extents` array from `info` is STRIPPED from the browser payload
#     but cached server-side (`OverviewCache`) so the lazy endpoint can resolve a
#     partition's extents without another RPC. No per-partition `--detail` loop.
#   * Per-partition DETAIL (`partition_detail`) is fetched on demand when the
#     operator expands one partition: a single `info --part <id> --detail` (load
#     metrics) plus that partition's extents resolved from the cache.
# Inherent residual: the `info` RPC itself still serialises all extents
# (autumn-op CLI shape); a manager-side "partitions-only" RPC would remove even
# that, but that's a Rust change. Parsing one JSON server-side ≪ rendering 数万
# DOM nodes or firing N per-partition RPCs, so this keeps the browser + refresh
# cost bounded by PARTITION count, not extent count.

def _range_key(p):
    s = p.get("range_start", "")
    return (s != "", s)  # "" (−∞) sorts first


def build_overview(op: Callable[..., Any]) -> Dict[str, Any]:
    """Cheap cluster overview backed by the SCALABLE `autumn-op info` (compact:
    per-partition rollup + per-node extent counts, NO per-extent array). Merges
    node health (list-nodes) + capacity (df.per_node) + extent-shard counts
    (info.nodes) into self-contained node rows for the node-detail drawer."""
    errors: List[str] = []

    def safe(label: str, fn):
        try:
            return fn()
        except OpError as e:
            errors.append(f"{label}: {e}")
            return None

    df = safe("df", lambda: op(["df"])) or {}
    info = safe("info", lambda: op(["info"])) or {}     # compact (default)
    cands = safe("policy-candidates", lambda: op(["policy-candidates"])) or []
    node_states = safe("list-nodes", lambda: op(["list-nodes"])) or []

    partitions = list(info.get("partitions", []))
    partitions.sort(key=_range_key)

    # Merge the three node sources into one row per node: state (list-nodes) +
    # capacity (df.per_node) + extent-shard count (info.nodes).
    df_by_node = {n.get("node_id"): n for n in (df.get("per_node") or [])}
    ext_by_node = {n.get("node_id"): n.get("extent_count", 0) for n in info.get("nodes", [])}
    nodes: List[Dict[str, Any]] = []
    for n in node_states:
        nid = n.get("node_id")
        dn = df_by_node.get(nid, {})
        nodes.append({
            **n,
            "free": dn.get("free"), "total": dn.get("total"),
            "extent_bytes": dn.get("extent_bytes"),
            "extent_count": ext_by_node.get(nid, 0),
            "online": dn.get("online", n.get("auto_state") == "Online"),
        })

    # Roll up by PS INSTANCE (ps_id), not per-partition ps_addr (F099-K gives
    # each partition its own listener, so addr-grouping over-counts PS).
    ps_roll: Dict[int, Dict[str, Any]] = {}
    for p in partitions:
        psid = p.get("ps_id", 0)
        r = ps_roll.setdefault(psid, {"ps_id": psid, "addr": p.get("ps_addr", ""), "n": 0, "size": 0})
        r["n"] += 1
        r["size"] += p.get("live_size", 0)

    advisories: List[Dict[str, Any]] = []
    for c in cands:
        advisories.append({
            "kind": c.get("kind"),
            "primary_part_id": c.get("primary_part_id", 0),
            "secondary_part_id": c.get("secondary_part_id", 0),
            "reason": c.get("reason", ""),
            "desc": describe_candidate(c),
            "cmd": candidate_to_cmd(c),
            "key": cooldown_key(c),
        })

    return {
        "ts": time.time(),
        "df": df,
        "nodes": nodes,
        "partitions": partitions,        # range-sorted, compact
        "ps_roll": sorted(ps_roll.values(), key=lambda x: x["ps_id"]),
        "part_count": info.get("part_count", len(partitions)),
        "ps_count": info.get("ps_count", len(ps_roll)),
        "total_req_per_sec": info.get("total_req_per_sec", 0),
        "total_write_bytes_per_sec": info.get("total_write_bytes_per_sec", 0),
        "total_read_bytes_per_sec": info.get("total_read_bytes_per_sec", 0),
        "advisories": advisories,
        "errors": errors,
    }


def partition_detail(op: Callable[..., Any], pid: int) -> Dict[str, Any]:
    """Lazy per-partition detail: a SCOPED `info --part <id>` (the partition's
    extents — bounded by THIS partition, never a full-cluster pull) + an
    `info --part <id> --detail` (load metrics)."""
    errors: List[str] = []

    def safe(label, fn, default):
        try:
            return fn()
        except OpError as e:
            errors.append(f"{label}: {e}")
            return default

    pinfo = safe(f"info --part {pid}", lambda: op(["info", "--part", str(pid)]), {}) or {}
    d = safe(f"info --part {pid} --detail", lambda: op(["info", "--part", str(pid), "--detail"]), {}) or {}

    return {
        "part_id": pid,
        "ps_addr": pinfo.get("ps_addr", ""),
        "range_start": pinfo.get("range_start", ""),
        "range_end": pinfo.get("range_end", ""),
        "req_per_sec": d.get("req_per_sec", 0),
        "write_bytes_per_sec": d.get("write_bytes_per_sec", 0),
        "read_bytes_per_sec": d.get("read_bytes_per_sec", 0),
        "p99_us": d.get("p99_us", 0),
        "size_bytes": d.get("size_bytes") or pinfo.get("live_size", 0),
        "gc_debt_bytes": d.get("gc_debt_bytes", 0),
        "pending_compaction_bytes": d.get("pending_compaction_bytes", 0),
        "gc_inflight": bool(d.get("gc_inflight")),
        "compact_inflight": bool(d.get("compact_inflight")),
        "sealed_log_extent_count": d.get("sealed_log_extent_count", 0),
        "extents": pinfo.get("extents", []),
        "errors": errors,
    }


# ── policies ─────────────────────────────────────────────────────────────────
# A policy is a named set of toggles. The UI exposes 5 friendly switches
# (split / ec / compact / gc / merge); a switch maps to one or more
# `ACTIONABLE_KINDS` (compact == major + minor). The active policy, when the
# controller is ON, drives the tick loop.
SWITCH_TO_KINDS: Dict[str, Tuple[str, ...]] = {
    "split": ("split",),
    "ec": ("ec",),
    "compact": ("major", "minor"),
    "gc": ("gc",),
    "merge": ("merge",),
}
SWITCH_ORDER = ("split", "ec", "compact", "gc", "merge")


def kinds_from_switches(switches: Dict[str, bool]) -> Tuple[str, ...]:
    out: List[str] = []
    for sw in SWITCH_ORDER:
        if switches.get(sw):
            out.extend(SWITCH_TO_KINDS[sw])
    return tuple(out)


def _policy(name: str, desc: str, switches: Dict[str, bool],
            interval: float, cooldown: float, max_actions: int, builtin: bool = True) -> Dict[str, Any]:
    sw = {s: bool(switches.get(s)) for s in SWITCH_ORDER}
    return {
        "name": name, "desc": desc, "switches": sw,
        "interval": interval, "cooldown": cooldown, "max_actions": max_actions,
        "builtin": builtin,
    }


# Preset policies, ordered safest -> most aggressive.
PRESET_POLICIES: List[Dict[str, Any]] = [
    _policy("gc-only", "Reclaim space only (GC)",
            {"gc": True}, 30, 120, 2),
    _policy("maintenance", "GC + compaction, no topology change",
            {"gc": True, "compact": True}, 30, 180, 2),
    _policy("space-reclaim", "GC + auto-EC, space-first",
            {"gc": True, "ec": True}, 20, 120, 3),
    _policy("balanced", "GC + compaction + EC (recommended steady-state)",
            {"gc": True, "compact": True, "ec": True}, 30, 240, 2),
    _policy("aggressive", "Full auto: incl. split / merge topology changes",
            {"split": True, "ec": True, "compact": True, "gc": True, "merge": True}, 20, 180, 3),
]


# ── auto-policy controller (background thread) ───────────────────────────────
class AutoPolicy:
    """Tick-loop that gathers candidates and actuates them per the ACTIVE
    policy, mirroring `autumn_dashboard.py control --apply`. Holds a registry
    of preset + user-created policies; the operator picks which is active and
    flips the master enable. Off by default. All state behind a lock (HTTP
    handler threads read/write it)."""

    def __init__(self, op: Callable[..., Any], allow_mutations: bool = False):
        self._op = op
        self._allow = allow_mutations
        self._lock = threading.Lock()
        self.enabled = False
        self.active: Optional[str] = None
        # name -> policy dict (presets first, then custom)
        self.policies: Dict[str, Dict[str, Any]] = {p["name"]: dict(p) for p in PRESET_POLICIES}
        self._cooldowns: Dict[str, float] = {}
        self.log: List[Dict[str, Any]] = []  # recent actuations (newest first)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": self.enabled,
                "active": self.active,
                "allow_mutations": self._allow,
                "policies": list(self.policies.values()),
                "switch_order": list(SWITCH_ORDER),
                "log": list(self.log[:50]),
            }

    def upsert_policy(self, body: Dict[str, Any]) -> Dict[str, Any]:
        name = (body.get("name") or "").strip()
        if not name:
            return {"error": "policy name required"}
        with self._lock:
            existing = self.policies.get(name)
            if existing and existing.get("builtin"):
                return {"error": f"'{name}' is a built-in preset; pick a different name"}
            sw = body.get("switches") or {}
            self.policies[name] = _policy(
                name,
                body.get("desc", "custom policy"),
                {s: bool(sw.get(s)) for s in SWITCH_ORDER},
                max(2.0, float(body.get("interval", 30))),
                max(0.0, float(body.get("cooldown", 180))),
                max(1, int(body.get("max_actions", 2))),
                builtin=False,
            )
        return self.state()

    def delete_policy(self, name: str) -> Dict[str, Any]:
        with self._lock:
            p = self.policies.get(name)
            if not p:
                return {"error": "no such policy"}
            if p.get("builtin"):
                return {"error": "cannot delete a built-in preset"}
            del self.policies[name]
            if self.active == name:
                self.active = None
                self.enabled = False
        return self.state()

    def activate(self, body: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            if "active" in body:
                name = body["active"]
                if name is not None and name not in self.policies:
                    return {"error": "no such policy"}
                self.active = name
            if "enabled" in body:
                want = bool(body["enabled"]) and self.active is not None
                if want and not self._allow:
                    return {"error": "server is read-only; relaunch with --allow-mutations to arm the controller",
                            **self.state()}
                self.enabled = want
        return self.state()

    def _record(self, entry: Dict[str, Any]) -> None:
        entry["ts"] = time.time()
        self.log.insert(0, entry)
        del self.log[100:]

    def _run(self) -> None:
        while True:
            with self._lock:
                enabled = self.enabled
                pol = self.policies.get(self.active) if self.active else None
            if not enabled or not pol:
                time.sleep(1.0)
                continue
            kinds = kinds_from_switches(pol["switches"])
            interval, cooldown, max_actions = pol["interval"], pol["cooldown"], pol["max_actions"]
            if not kinds:
                time.sleep(interval)
                continue
            try:
                cands = self._op(["policy-candidates"])
            except OpError as e:
                self._record({"level": "error", "msg": f"policy-candidates: {e}"})
                time.sleep(interval)
                continue
            now = time.time()
            with self._lock:
                actions = decide_actions(cands or [], self._cooldowns, kinds, now, cooldown, max_actions)
            for c, cmd, key in actions:
                try:
                    self._op(cmd, json_out=False)
                    with self._lock:
                        self._cooldowns[key] = now
                    self._record({"level": "issued", "msg": f"autumn-op {' '.join(cmd)}",
                                  "desc": describe_candidate(c), "policy": pol["name"]})
                except OpError as e:
                    self._record({"level": "refused", "msg": f"autumn-op {' '.join(cmd)}: {e}"})
            time.sleep(interval)


# ── action validation (backend trust boundary) ──────────────────────────────
def validate_action(cmd: List[str]) -> Optional[str]:
    """Return an error string if `cmd` is not an allowed mutation, else None.
    Never trust the client: the verb must be whitelisted and every remaining
    token must be an integer or a known flag form."""
    if not cmd or not isinstance(cmd, list):
        return "empty command"
    verb = cmd[0]
    if verb not in ALLOWED_VERBS:
        return f"verb '{verb}' not allowed"
    for tok in cmd[1:]:
        if tok == "--extent":
            continue
        if not (isinstance(tok, str) and tok.lstrip("-").isdigit()):
            return f"bad argument '{tok}' (expected integer)"
    return None


# ── HTTP server ──────────────────────────────────────────────────────────────
class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, addr, op: Callable[..., Any], with_detail: bool, allow_mutations: bool):
        self.op = op
        self.with_detail = with_detail
        # SERVER-SIDE mutation gate. The client-side "Danger" toggle +
        # confirm() dialog are NOT sufficient protection (a headless browser
        # auto-accepts confirm(); a human can misclick). Unless the operator
        # explicitly launches with --allow-mutations, the backend REFUSES
        # every /api/action and controller-enable, so the dashboard is a
        # pure read-only viewer no matter what the page does.
        self.allow_mutations = allow_mutations
        self.auto = AutoPolicy(op, allow_mutations)
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "autumn_dashboard_web.html"), encoding="utf-8") as f:
            self.page = f.read()
        super().__init__(addr, DashboardHandler)


class DashboardHandler(BaseHTTPRequestHandler):
    server: DashboardServer  # type: ignore[assignment]

    def log_message(self, *_a):  # silence per-request stderr noise
        pass

    def _send_json(self, obj: Any, code: int = 200) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> Dict[str, Any]:
        n = int(self.headers.get("Content-Length", 0) or 0)
        if n <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode() or "{}")
        except json.JSONDecodeError:
            return {}

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/":
            self._send_html(self.server.page)
        elif path == "/api/overview":
            try:
                self._send_json(build_overview(self.server.op))
            except Exception as e:  # never 500 the poller into a blank page
                self._send_json({"ts": time.time(), "partitions": [], "errors": [f"overview: {e}"]})
        elif path.startswith("/api/partition/"):
            try:
                pid = int(path.rsplit("/", 1)[1])
            except ValueError:
                self._send_json({"error": "bad partition id"}, 400)
                return
            try:
                self._send_json(partition_detail(self.server.op, pid))
            except Exception as e:
                self._send_json({"part_id": pid, "extents": [], "errors": [f"detail: {e}"]})
        elif path == "/api/policies":
            self._send_json(self.server.auto.state())
        else:
            self._send_json({"error": "not found"}, 404)

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        body = self._read_body()
        auto = self.server.auto
        if path == "/api/action":
            if not self.server.allow_mutations:
                self._send_json({"ok": False, "error": "server is read-only; relaunch with --allow-mutations"}, 403)
                return
            cmd = body.get("cmd")
            err = validate_action(cmd)
            if err:
                self._send_json({"ok": False, "error": err}, 400)
                return
            try:
                out = self.server.op(cmd, json_out=False)
                self._send_json({"ok": True, "output": out, "cmd": cmd})
            except OpError as e:
                self._send_json({"ok": False, "error": str(e), "cmd": cmd})
        elif path == "/api/policies/upsert":
            self._send_json(auto.upsert_policy(body))
        elif path == "/api/policies/activate":
            self._send_json(auto.activate(body))
        elif path == "/api/policies/delete":
            self._send_json(auto.delete_policy(body.get("name", "")))
        else:
            self._send_json({"error": "not found"}, 404)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="autumn-rs web dashboard")
    ap.add_argument("--manager", default=DEFAULT_MANAGER, help=f"manager addr (default {DEFAULT_MANAGER})")
    ap.add_argument("--bin", default=None, help="autumn-op binary (default: $AUTUMN_OP_BIN or target/{release,debug})")
    ap.add_argument("--host", default="127.0.0.1", help="bind host (default 127.0.0.1)")
    ap.add_argument("--port", type=int, default=8799, help="bind port (default 8799)")
    ap.add_argument("--no-detail", action="store_true", help="skip per-partition detail RPCs (topology only)")
    ap.add_argument("--allow-mutations", action="store_true",
                    help="ARM cluster-mutating actions (split/merge/gc/compact/ec + auto-controller). "
                         "Default OFF: the server is a read-only viewer and refuses every action at the backend.")
    args = ap.parse_args(argv)

    bin_path = resolve_bin(args.bin)
    op = make_op(args.manager, bin_path)
    server = DashboardServer((args.host, args.port), op,
                             with_detail=not args.no_detail, allow_mutations=args.allow_mutations)
    url = f"http://{args.host}:{args.port}"
    print(f"autumn-rs web dashboard → {url}")
    print(f"  manager={args.manager}  autumn-op={bin_path}")
    print(f"  mutations={'ARMED (--allow-mutations)' if args.allow_mutations else 'READ-ONLY (relaunch with --allow-mutations to enable actions)'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
