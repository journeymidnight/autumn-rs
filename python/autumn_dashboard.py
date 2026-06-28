#!/usr/bin/env python3
"""autumn_dashboard.py — cluster dashboard + auto-policy controller for autumn-rs.

Like `node_policy.py`, this shells out to the `autumn-op --json` Rust binary
rather than re-implementing the rkyv wire codec in Python — the wire schema
stays in exactly one place (`crates/rpc/src/manager_rpc.rs`) and upgrades are
"bump the binary". Two modes:

    # Live cluster dashboard (capacity, node health, per-partition IOPS /
    # throughput / size / debt, pending policy advisories). Ctrl-C to exit.
    autumn_dashboard.py dashboard [--interval 2] [--once]

    # Auto-policy controller. Polls the manager's advisory engine
    # (`policy-candidates`) and actuates ec / split / merge / gc / compact via
    # autumn-op. SAFE BY DEFAULT: prints intended actions unless --apply is
    # given. This is the external policy loop the manager (F203 pure-mechanism)
    # was deliberately designed for — the manager only EMITS advisories; an
    # operator-owned controller like this one DECIDES and ACTUATES.
    autumn_dashboard.py control [--interval 30] [--apply]
                               [--enable ec,split,merge,gc,major,minor]
                               [--cooldown 300] [--max-actions 2]

Design note: every function that touches the cluster takes an injected `op`
callable so the parse + decide logic is unit-testable without a live cluster
(see test_autumn_dashboard.py). The manager's own ops are crash-safe and
idempotent-on-retry (F149 fence, F207 inflight ledger, F185 merge freeze), so
this controller only needs client-side rate-limiting + cooldown + a "refuse =
already in flight, retry next tick" posture, NOT its own 2PC.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

DEFAULT_BIN = os.environ.get("AUTUMN_OP_BIN", "autumn-op")
DEFAULT_MANAGER = os.environ.get("AUTUMN_MANAGER", "127.0.0.1:9001")

# Policy candidate kinds → how to actuate via autumn-op. `hotcold` is a
# placement hint with no direct mutation, so it is advisory-only here.
ACTIONABLE_KINDS = ("ec", "split", "merge", "gc", "major", "minor")


class OpError(RuntimeError):
    pass


# ── autumn-op subprocess wrapper (I/O boundary) ──────────────────────────────
def make_op(manager: str, bin_path: str) -> Callable[[List[str], bool], Any]:
    """Return an `op(args, json_out=True)` closure bound to a manager + binary.

    Isolating the subprocess call behind a closure lets the gather / decide
    logic be driven by a fake `op` in tests.
    """

    def op(args: List[str], json_out: bool = True) -> Any:
        cmd = [bin_path, "--manager", manager]
        if json_out:
            cmd.append("--json")
        cmd.extend(args)
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        except FileNotFoundError as e:
            raise OpError(
                f"could not find `{bin_path}`; set AUTUMN_OP_BIN or build "
                f"`cargo build --release -p autumn-server --bin autumn-op`"
            ) from e
        if result.returncode != 0:
            raise OpError(
                f"`autumn-op {' '.join(args)}` exit={result.returncode}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
        out = result.stdout.strip()
        if not json_out:
            return out
        try:
            return json.loads(out) if out else None
        except json.JSONDecodeError as e:
            raise OpError(f"could not parse autumn-op output as JSON: {e}\n{out}") from e

    return op


# ── formatting helpers (pure) ────────────────────────────────────────────────
def fmt_bytes(n: Optional[int]) -> str:
    """Human-readable bytes (binary units)."""
    if n is None:
        return "-"
    f = float(n)
    for unit in ("B", "K", "M", "G", "T", "P"):
        if abs(f) < 1024.0 or unit == "P":
            return f"{f:.0f}{unit}" if unit == "B" else f"{f:.1f}{unit}"
        f /= 1024.0
    return f"{f:.1f}P"


def pct(used: Optional[int], total: Optional[int]) -> str:
    if not total:
        return "-"
    return f"{100.0 * (used or 0) / total:.0f}%"


# ── gather (state collection; takes injected `op` for testability) ───────────
def gather(op: Callable[..., Any], with_partition_detail: bool = True) -> Dict[str, Any]:
    """Collect a full cluster snapshot via autumn-op JSON commands.

    Returns a dict {df, info, partitions(list of detail dicts), candidates,
    errors}. Per-partition detail is one `info --part P --detail` RPC each; on
    a cluster with many partitions this is the dashboard's dominant cost, so
    `with_partition_detail=False` skips it (topology-only).
    """
    state: Dict[str, Any] = {"errors": []}

    def safe(label: str, fn):
        try:
            return fn()
        except OpError as e:
            state["errors"].append(f"{label}: {e}")
            return None

    state["df"] = safe("df", lambda: op(["df"]))
    info = safe("info", lambda: op(["info"]))
    state["info"] = info
    state["candidates"] = safe("policy-candidates", lambda: op(["policy-candidates"])) or []

    parts: List[Dict[str, Any]] = []
    part_ids = [p.get("part_id") for p in (info or {}).get("partitions", []) if p.get("part_id") is not None]
    if with_partition_detail:
        for pid in part_ids:
            d = safe(f"info --part {pid}", lambda pid=pid: op(["info", "--part", str(pid), "--detail"]))
            if d:
                # carry the topology row (range / ps) alongside the load detail
                topo = next((p for p in info.get("partitions", []) if p.get("part_id") == pid), {})
                d["_topo"] = topo
                parts.append(d)
    state["partitions"] = parts
    return state


# ── dashboard rendering (pure: state dict → string) ──────────────────────────
def render_dashboard(state: Dict[str, Any], now: Optional[float] = None) -> str:
    lines: List[str] = []
    ts = time.strftime("%H:%M:%S", time.localtime(now))
    lines.append(f"═══ autumn-rs cluster dashboard ═══  {ts}")

    df = state.get("df") or {}
    if df:
        raw_total = df.get("raw_total", 0)
        raw_used = df.get("raw_used", 0)
        raw_free = df.get("raw_free", 0)
        amp = df.get("amplification") or df.get("amp") or 0
        # amp==0 is the manager's "no sealed logical data measured yet"
        # sentinel (df sets it when logical_stored==0); show n/a like the
        # autumn-op text path instead of a misleading "0.00x".
        amp_str = f"{amp:.2f}x" if isinstance(amp, (int, float)) and amp > 0 else "n/a"
        lines.append(
            f"capacity: raw {fmt_bytes(raw_used)}/{fmt_bytes(raw_total)} ({pct(raw_used, raw_total)}) used"
            f"  free {fmt_bytes(raw_free)}"
            f"  physical {fmt_bytes(df.get('physical_used'))}"
            f"  logical {fmt_bytes(df.get('logical_stored_sealed'))}"
            f"  amp {amp_str}"
        )
        per_node = df.get("per_node") or []
        if per_node:
            lines.append("  nodes: " + "  ".join(
                f"n{n.get('node_id')}[{'on' if n.get('online') else 'OFF'} "
                f"{fmt_bytes(n.get('free'))}/{fmt_bytes(n.get('total'))} "
                f"ext={fmt_bytes(n.get('extent_bytes'))}]"
                for n in per_node
            ))

    # Per-partition table, sorted by IOPS (req_per_sec) desc — hottest first.
    parts = sorted(
        state.get("partitions", []),
        key=lambda p: p.get("req_per_sec", 0),
        reverse=True,
    )
    if parts:
        lines.append("")
        lines.append(
            f"{'PART':>7} {'PS_ADDR':<16} {'IOPS':>8} {'p99us':>7} {'SIZE':>8} "
            f"{'GCdebt':>8} {'COMPACT':>8} {'INFL':>5} {'SEALEXT':>7}"
        )
        for p in parts:
            topo = p.get("_topo", {})
            # The `info` partitions array identifies the serving PS by its
            # per-partition listener ADDRESS (`ps_addr`, F099-K), not an
            # integer id — ps_id/ps are accepted as forward-compat fallbacks.
            ps = topo.get("ps_addr") or topo.get("ps_id") or topo.get("ps") or "-"
            # The F203 PartitionLoad `size_bytes` is SST-flushed bytes and reads
            # 0 until the first flush; the topology row's `live_size` is the
            # authoritative total (log_stream + SSTs), so fall back to it.
            size_b = p.get("size_bytes") or topo.get("live_size") or 0
            infl = ("g" if p.get("gc_inflight") else "-") + ("c" if p.get("compact_inflight") else "-")
            lines.append(
                f"{p.get('part_id'):>7} {str(ps):<16} {p.get('req_per_sec', 0):>8} "
                f"{p.get('p99_us', 0):>7} {fmt_bytes(size_b):>8} "
                f"{fmt_bytes(p.get('gc_debt_bytes')):>8} "
                f"{fmt_bytes(p.get('pending_compaction_bytes')):>8} {infl:>5} "
                f"{p.get('sealed_log_extent_count', 0):>7}"
            )

    cands = state.get("candidates") or []
    lines.append("")
    if cands:
        lines.append(f"policy advisories ({len(cands)}):")
        for c in cands:
            lines.append("  " + describe_candidate(c))
    else:
        lines.append("policy advisories: (none)")

    for err in state.get("errors", []):
        lines.append(f"  ! {err}")
    return "\n".join(lines)


def describe_candidate(c: Dict[str, Any]) -> str:
    kind = c.get("kind", "?")
    prim = c.get("primary_part_id", 0)
    sec = c.get("secondary_part_id", 0)
    reason = c.get("reason", "")
    if kind == "ec":
        tgt = f"extent {sec}"
    elif kind == "merge":
        tgt = f"part {prim}<-{sec}"
    else:
        tgt = f"part {prim}"
    return f"{kind:<6} {tgt:<18} {reason}"


# ── auto-policy controller (pure decision + I/O apply) ───────────────────────
def candidate_to_cmd(c: Dict[str, Any]) -> Optional[List[str]]:
    """Map a policy candidate to its autumn-op actuation command (or None if
    not actionable). EC carries the extent in `secondary_part_id` (primary=0);
    split/gc/compact use `primary_part_id`; merge uses primary=survivor,
    secondary=victim."""
    kind = c.get("kind")
    prim = c.get("primary_part_id", 0)
    sec = c.get("secondary_part_id", 0)
    if kind == "ec":
        if not sec:
            return None
        return ["force-ec-convert", "--extent", str(sec)]
    if kind == "split":
        return ["split", str(prim)]
    if kind == "merge":
        if not sec:
            return None
        return ["merge", str(prim), str(sec)]
    if kind == "gc":
        return ["gc", str(prim)]
    if kind in ("major", "minor"):
        # both map to compact; the PS picks major vs minor by its own state
        return ["compact", str(prim)]
    return None  # hotcold / unknown → advisory only


def cooldown_key(c: Dict[str, Any]) -> str:
    """A stable per-(kind, target) key for client-side cooldown tracking."""
    kind = c.get("kind")
    if kind == "ec":
        return f"ec:{c.get('secondary_part_id', 0)}"
    if kind == "merge":
        return f"merge:{c.get('primary_part_id', 0)}:{c.get('secondary_part_id', 0)}"
    return f"{kind}:{c.get('primary_part_id', 0)}"


def decide_actions(
    candidates: List[Dict[str, Any]],
    cooldowns: Dict[str, float],
    enabled: Tuple[str, ...],
    now: float,
    cooldown_secs: float,
    max_actions: int,
) -> List[Tuple[Dict[str, Any], List[str], str]]:
    """Pure decision: pick which candidates to actuate this tick.

    Filters by `enabled` kinds, drops anything still inside its client-side
    cooldown window, maps to an autumn-op command, and caps at `max_actions`.
    Ordering prioritises relief-valve ops first (split before merge — splitting
    a hot partition relieves load, whereas merge concentrates it; then the
    cheap maintenance ops). Returns (candidate, cmd, cooldown_key) triples.
    """
    # Priority: split (relief) > gc/compact (cheap upkeep) > ec (space) > merge
    # (concentrates load — least urgent). Mirrors feedback: auto-split before
    # auto-merge.
    order = {"split": 0, "gc": 1, "minor": 2, "major": 3, "ec": 4, "merge": 5}
    picked: List[Tuple[Dict[str, Any], List[str], str]] = []
    seen_keys: set = set()
    for c in sorted(candidates, key=lambda c: order.get(c.get("kind"), 9)):
        if len(picked) >= max_actions:
            break
        kind = c.get("kind")
        if kind not in enabled:
            continue
        cmd = candidate_to_cmd(c)
        if cmd is None:
            continue
        key = cooldown_key(c)
        if key in seen_keys:
            continue
        last = cooldowns.get(key, 0.0)
        if now - last < cooldown_secs:
            continue  # still cooling down from a recent actuation
        seen_keys.add(key)
        picked.append((c, cmd, key))
    return picked


def apply_actions(
    actions: List[Tuple[Dict[str, Any], List[str], str]],
    op: Callable[..., Any],
    cooldowns: Dict[str, float],
    now: float,
    dry_run: bool,
) -> int:
    """Actuate (or, in dry_run, just print) the decided actions. Records the
    cooldown stamp on a real (non-dry-run) issue. A manager refusal
    (precondition: already in flight) is logged, not fatal — the next tick
    retries. Returns the number of actions actually issued."""
    issued = 0
    for c, cmd, key in actions:
        desc = describe_candidate(c)
        if dry_run:
            print(f"  [dry-run] would: autumn-op {' '.join(cmd)}   ({desc})")
            continue
        try:
            op(cmd, json_out=False)
            cooldowns[key] = now
            issued += 1
            print(f"  issued: autumn-op {' '.join(cmd)}   ({desc})")
        except OpError as e:
            # Manager refuses (e.g. inflight / precondition) → benign, retry next tick.
            print(f"  refused (retry next tick): autumn-op {' '.join(cmd)}: {e}")
    return issued


# ── CLI commands ─────────────────────────────────────────────────────────────
def cmd_dashboard(args: argparse.Namespace) -> int:
    op = make_op(args.manager, args.bin)
    try:
        while True:
            state = gather(op, with_partition_detail=not args.no_detail)
            out = render_dashboard(state)
            if not args.once:
                sys.stdout.write("\033[2J\033[H")  # clear + home
            print(out)
            if args.once:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


def cmd_control(args: argparse.Namespace) -> int:
    op = make_op(args.manager, args.bin)
    enabled = tuple(k.strip() for k in args.enable.split(",") if k.strip())
    bad = [k for k in enabled if k not in ACTIONABLE_KINDS]
    if bad:
        print(f"error: unknown --enable kind(s): {bad}; valid: {ACTIONABLE_KINDS}", file=sys.stderr)
        return 2
    cooldowns: Dict[str, float] = {}
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"autumn auto-policy controller [{mode}] manager={args.manager} "
        f"enable={','.join(enabled)} interval={args.interval}s cooldown={args.cooldown}s "
        f"max-actions/tick={args.max_actions}"
    )
    try:
        while True:
            now = time.time()
            try:
                candidates = op(["policy-candidates"]) or []
            except OpError as e:
                print(f"[{time.strftime('%H:%M:%S')}] poll failed: {e}")
                candidates = []
            actions = decide_actions(
                candidates, cooldowns, enabled, now, args.cooldown, args.max_actions
            )
            n_cand = len(candidates)
            if actions:
                print(f"[{time.strftime('%H:%M:%S')}] {n_cand} advisories, acting on {len(actions)}:")
                apply_actions(actions, op, cooldowns, now, dry_run=not args.apply)
            else:
                print(f"[{time.strftime('%H:%M:%S')}] {n_cand} advisories, none actionable this tick")
            if args.once:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="autumn_dashboard.py",
        description="autumn-rs cluster dashboard + auto-policy controller (shells out to autumn-op)",
    )
    p.add_argument("--manager", default=DEFAULT_MANAGER, help=f"manager addr (default {DEFAULT_MANAGER})")
    p.add_argument("--bin", default=DEFAULT_BIN, help=f"autumn-op binary (default {DEFAULT_BIN})")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("dashboard", help="live cluster state view")
    d.add_argument("--interval", type=float, default=2.0, help="refresh seconds (default 2)")
    d.add_argument("--once", action="store_true", help="print once and exit (no live refresh)")
    d.add_argument("--no-detail", action="store_true", help="skip per-partition detail RPCs (topology only)")
    d.set_defaults(func=cmd_dashboard)

    c = sub.add_parser("control", help="auto-policy controller (ec/split/merge/gc/compact)")
    c.add_argument("--interval", type=float, default=30.0, help="poll seconds (default 30)")
    c.add_argument("--apply", action="store_true", help="ACTUALLY actuate (default: dry-run print only)")
    c.add_argument("--enable", default=",".join(ACTIONABLE_KINDS),
                   help=f"comma-list of kinds to act on (default all: {','.join(ACTIONABLE_KINDS)})")
    c.add_argument("--cooldown", type=float, default=300.0,
                   help="client-side per-(kind,target) cooldown seconds (default 300)")
    c.add_argument("--max-actions", type=int, default=2, help="max actuations per tick (default 2)")
    c.add_argument("--once", action="store_true", help="one poll+act cycle then exit")
    c.set_defaults(func=cmd_control)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
