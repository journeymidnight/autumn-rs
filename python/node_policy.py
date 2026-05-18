#!/usr/bin/env python3
"""F211-G: Operator policy script for autumn-rs node lifecycle.

Implements the HDFS-decommission-style operator runbook. Periodically
pulls health facts from the manager (via the `autumn-op` companion CLI)
and, optionally, takes safe automated actions or surfaces decisions
for human confirmation. Designed for cron / k8s-CronJob deployment.

Examples:

    # One-shot read-only views
    node_policy.py list
    node_policy.py inspect 7
    node_policy.py inspect-extent 12345
    node_policy.py list-stale-markers

    # Decision actions (interactive confirm unless --yes)
    node_policy.py fence 7 --reason "host hardware failure"
    node_policy.py maintenance 7 --reason "planned reboot" --expire-in 30m
    node_policy.py unfence 7
    node_policy.py remove 7

    # Integrated lifecycle (HDFS dfsadmin -decommission analogue)
    node_policy.py decommission 7

    # Half-auto: scan ec_convert_advisory and reissue ready EC converts
    node_policy.py auto-reissue --dry-run

The script intentionally shells out to the `autumn-op` Rust binary
rather than re-implementing rkyv encode in Python. This keeps the
wire definitions in exactly one place (`crates/rpc/src/manager_rpc.rs`)
and makes upgrades trivial — bump the binary; the script keeps
working.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_MANAGER = os.environ.get("AUTUMN_MANAGER", "127.0.0.1:9001")
DEFAULT_BIN = os.environ.get("AUTUMN_OP_BIN", "autumn-op")


# ── autumn-op subprocess wrapper ─────────────────────────────────────────────

class OpError(RuntimeError):
    pass


def _op(args: List[str], json_out: bool = True) -> Any:
    """Call `autumn-op <args>` and return parsed JSON (or raw stdout)."""
    cmd = [DEFAULT_BIN, "--manager", DEFAULT_MANAGER]
    if json_out:
        cmd.append("--json")
    cmd.extend(args)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError as e:
        raise OpError(
            f"could not find `{DEFAULT_BIN}`; set AUTUMN_OP_BIN or build "
            f"crates/server/src/bin/autumn_op.rs first"
        ) from e
    if result.returncode != 0:
        raise OpError(
            f"`autumn-op {' '.join(args)}` exit={result.returncode}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    out = result.stdout.strip()
    if not json_out:
        return out
    if not out:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError as e:
        raise OpError(f"could not parse autumn-op output as JSON: {e}\n{out}") from e


# ── Time / parsing helpers ───────────────────────────────────────────────────

def parse_duration(s: str) -> int:
    """Parse `30m`, `1h`, `7d`, `90s`, or a raw integer (seconds)."""
    s = s.strip().lower()
    m = re.fullmatch(r"(\d+)([smhd]?)", s)
    if not m:
        raise ValueError(f"invalid duration {s!r}")
    n = int(m.group(1))
    unit = m.group(2)
    return n * {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


def confirm(prompt: str, yes: bool) -> bool:
    if yes:
        return True
    sys.stdout.write(f"{prompt} [y/N]: ")
    sys.stdout.flush()
    return sys.stdin.readline().strip().lower() in ("y", "yes")


def whoami() -> str:
    return os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"


# ── Read-only views ──────────────────────────────────────────────────────────

def cmd_list(args: argparse.Namespace) -> int:
    """`list` — overview of every EN's auto state + override."""
    nodes = _op(["list-nodes"])
    if not nodes:
        print("(no nodes registered)")
        return 0
    print(
        f"{'ID':<5} {'ADDRESS':<22} {'AUTO':<10} {'HB_AGO':<10} "
        f"{'OVERRIDE':<14} REASON"
    )
    for n in nodes:
        hb = n["last_heartbeat_secs_ago"]
        hb_str = "never" if hb >= 2**63 else f"{hb}s"
        print(
            f"{n['node_id']:<5} {n['address']:<22} "
            f"{n['auto_state']:<10} {hb_str:<10} "
            f"{n['override_kind']:<14} {n['override_reason']}"
        )
    return 0


def cmd_inspect(args: argparse.Namespace) -> int:
    """`inspect <node>` — detail dump including extents the node touches."""
    nid = args.node
    nodes = _op(["list-nodes"])
    me: Optional[dict] = next((n for n in nodes if n["node_id"] == nid), None)
    if not me:
        print(f"node {nid} not registered", file=sys.stderr)
        return 1
    print(f"node {nid}  address={me['address']}")
    print(f"  auto_state    = {me['auto_state']}")
    print(f"  last_hb_ago_s = {me['last_heartbeat_secs_ago']}")
    print(f"  suspected_age = {me['suspected_age_secs']}s")
    print(
        f"  override      = {me['override_kind']}  by={me['override_set_by']!r}  "
        f"reason={me['override_reason']!r}"
    )
    print()
    # Show extents that have any slot on this node.
    ext = _op(["extent-health", "--node", str(nid), "--all"])
    if not ext:
        print("(no extents reference this node)")
        return 0
    print(f"extents referencing node {nid}: {len(ext)}")
    for e in ext:
        flag = "  UNHEALTHY" if e["unhealthy"] else ""
        print(
            f"  extent {e['extent_id']}  ev={e['eversion']}  "
            f"sealed={e['sealed_length']}  ec={e['ec_converted']}{flag}"
        )
        for s in e["slots"]:
            if s["node_id"] == nid:
                print(
                    f"    slot[{s['slot']}] avali={s['avali']} "
                    f"auto={s['auto_state']} override={s['override']}"
                )
    # EC markers where this node is the coord.
    markers = _op(["list-ec-markers"]) or []
    coord_markers = [m for m in markers if m.get("coord_node_id") == nid]
    if coord_markers:
        print()
        print(f"inflight EC convert markers where node {nid} is coord:")
        for m in coord_markers:
            print(
                f"  extent {m['extent_id']}  age={m['age_secs']}s  "
                f"coord_auto={m['coord_auto_state']} override={m['coord_override']}"
            )
    return 0


def cmd_inspect_extent(args: argparse.Namespace) -> int:
    ext = _op(["extent-health", "--all"])
    matches = [e for e in ext if e["extent_id"] == args.extent_id]
    if not matches:
        print(f"extent {args.extent_id} not found", file=sys.stderr)
        return 1
    e = matches[0]
    print(
        f"extent {e['extent_id']}  ev={e['eversion']} sealed={e['sealed_length']} "
        f"ec={e['ec_converted']} unhealthy={e['unhealthy']}"
    )
    for s in e["slots"]:
        print(
            f"  slot[{s['slot']}] node={s['node_id']:<4} "
            f"avali={s['avali']} auto={s['auto_state']} override={s['override']}"
        )
    return 0


def cmd_list_stale_markers(args: argparse.Namespace) -> int:
    """List ConvertToEc markers where the coord is Suspected or Fenced.

    These are exactly the markers that the OP would normally examine and
    decide whether to `fence-node` + later `reissue-ec`.
    """
    markers = _op(["list-ec-markers"]) or []
    stale = [m for m in markers if m["coord_auto_state"] != "Online" or m["coord_override"] != "-"]
    if not stale:
        print("(no stale markers)")
        return 0
    print(f"stale markers (coord not Online): {len(stale)}")
    for m in stale:
        print(
            f"  extent {m['extent_id']}  coord={m['coord_node_id']} "
            f"({m['coord_auto_state']}/{m['coord_override']})  "
            f"age={m['age_secs']}s targets={m['target_nodes']}"
        )
    return 0


# ── Admin actions ────────────────────────────────────────────────────────────

def _admin(args: List[str]) -> Any:
    return _op(args)


def cmd_fence(args: argparse.Namespace) -> int:
    nodes = _op(["list-nodes"])
    me = next((n for n in nodes if n["node_id"] == args.node), None)
    if not me:
        print(f"node {args.node} not registered", file=sys.stderr)
        return 1
    print("about to fence:")
    print(f"  node_id = {args.node}")
    print(f"  address = {me['address']}")
    print(f"  auto    = {me['auto_state']} (hb_ago={me['last_heartbeat_secs_ago']}s)")
    print(f"  reason  = {args.reason!r}")
    if not confirm("proceed?", args.yes):
        print("aborted")
        return 1
    cmd = [
        "fence-node",
        str(args.node),
        "--reason",
        args.reason,
        "--by",
        args.by or whoami(),
    ]
    if args.force:
        cmd.append("--force")
    out = _admin(cmd)
    print(json.dumps(out, indent=2))
    return 0


def cmd_maintenance(args: argparse.Namespace) -> int:
    cmd = [
        "maintenance",
        str(args.node),
        "--reason",
        args.reason,
        "--by",
        args.by or whoami(),
    ]
    if args.expire_in:
        secs = parse_duration(args.expire_in)
        cmd.extend(["--expire", str(int(time.time()) + secs)])
    elif args.expire_at:
        cmd.extend(["--expire", str(args.expire_at)])
    out = _admin(cmd)
    print(json.dumps(out, indent=2))
    return 0


def cmd_unfence(args: argparse.Namespace) -> int:
    out = _admin(["unfence", str(args.node), "--by", args.by or whoami()])
    print(json.dumps(out, indent=2))
    return 0


def cmd_remove(args: argparse.Namespace) -> int:
    print(f"about to REMOVE node {args.node}. This is unrecoverable.")
    if not confirm("proceed?", args.yes):
        print("aborted")
        return 1
    out = _admin(["remove", str(args.node), "--by", args.by or whoami()])
    print(json.dumps(out, indent=2))
    if out.get("code", 0) != 0:
        return 2
    return 0


# ── Integrated decommission flow ─────────────────────────────────────────────

def cmd_decommission(args: argparse.Namespace) -> int:
    """fence → poll until recovery drains → remove.

    Equivalent to `hdfs dfsadmin -decommission`. Polls `extent-health
    --node N` until no extents reference the node, then `remove`.
    """
    nid = args.node

    print(f"step 1/3 — fence node {nid}")
    rc = cmd_fence(args)
    if rc != 0:
        return rc

    print(f"step 2/3 — wait for recovery to drain (poll every {args.poll}s)")
    deadline = time.time() + args.timeout if args.timeout else None
    while True:
        ext = _op(["extent-health", "--node", str(nid), "--all"]) or []
        remaining = [e for e in ext if any(s["node_id"] == nid for s in e["slots"])]
        if not remaining:
            break
        print(f"  still {len(remaining)} extent(s) reference node {nid}; sleeping {args.poll}s")
        if deadline and time.time() > deadline:
            print(f"timeout after {args.timeout}s — {len(remaining)} extents still pending")
            return 3
        time.sleep(args.poll)

    print(f"step 3/3 — remove node {nid}")
    rc = cmd_remove(args)
    return rc


# ── Half-auto: reissue EC converts after recovery ────────────────────────────

def cmd_auto_reissue(args: argparse.Namespace) -> int:
    """Scan extent health and reissue `force_ec_convert` for sealed
    extents that are not yet EC-encoded and whose all slots are Online.

    F213: `force-ec-convert` now lives on `autumn-op`, same binary this
    script already shells to — so when `--dry-run=false`, drive the
    reissue from here instead of asking the operator to type it.
    `--dry-run` (default) just prints the candidate list.
    """
    # The advisory etcd prefix is not directly exposed via a manager RPC;
    # the closest signal is "list-ec-markers" which now also reflects
    # markers fence-abandoned (they're gone from the ledger but the
    # ec_convert_advisory/<id> entry persists). For Stage 1 we report
    # any sealed extents that are NOT ec_converted and whose all slots
    # are Online — these are candidates for force_ec_convert. The
    # operator decides.
    ext = _op(["extent-health", "--all"]) or []
    cand = []
    for e in ext:
        if e["ec_converted"] or e["sealed_length"] == 0:
            continue
        ok = all(s["auto_state"] == "Online" and s["override"] == "-" for s in e["slots"])
        if ok:
            cand.append(e)
    if not cand:
        print("(no candidates for EC reissue)")
        return 0
    print(f"candidates ready for force_ec_convert: {len(cand)}")
    for e in cand:
        print(f"  extent {e['extent_id']}  sealed={e['sealed_length']}")
    if not args.dry_run:
        print()
        print(f"issuing force-ec-convert for {len(cand)} extent(s) via autumn-op:")
        failures: List[Tuple[int, str]] = []
        for e in cand:
            eid = e["extent_id"]
            try:
                resp = _op(["force-ec-convert", "--extent", str(eid)])
                msg = resp.get("message", "") if isinstance(resp, dict) else ""
                print(f"  extent {eid}: ok {msg}")
            except OpError as oe:
                failures.append((eid, str(oe)))
                print(f"  extent {eid}: FAILED {oe}", file=sys.stderr)
        if failures:
            print(f"\n{len(failures)} of {len(cand)} reissue(s) failed", file=sys.stderr)
            return 2
    return 0


# ── argparse ─────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="node_policy.py",
        description="F211-G operator policy script for autumn-rs node lifecycle",
    )
    p.add_argument("--manager", help="manager address (default: $AUTUMN_MANAGER or 127.0.0.1:9001)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="list every EN's state").set_defaults(func=cmd_list)

    s = sub.add_parser("inspect", help="show a node's state + referenced extents")
    s.add_argument("node", type=int)
    s.set_defaults(func=cmd_inspect)

    s = sub.add_parser("inspect-extent", help="show an extent's per-slot health")
    s.add_argument("extent_id", type=int)
    s.set_defaults(func=cmd_inspect_extent)

    sub.add_parser("list-stale-markers", help="ConvertToEc markers with non-Online coord").set_defaults(
        func=cmd_list_stale_markers
    )

    s = sub.add_parser("fence", help="confirm a node dead and trigger cleanup")
    s.add_argument("node", type=int)
    s.add_argument("--reason", required=True)
    s.add_argument("--by", help="operator name (default: $USER)")
    s.add_argument("--force", action="store_true", help="skip capacity precheck")
    s.add_argument("--yes", action="store_true", help="skip interactive confirm")
    s.set_defaults(func=cmd_fence)

    s = sub.add_parser("maintenance", help="soft-pause a node without recovery")
    s.add_argument("node", type=int)
    s.add_argument("--reason", required=True)
    s.add_argument("--by", help="operator name (default: $USER)")
    s.add_argument("--expire-in", help="auto-clear after duration (e.g. 30m, 1h, 1d)")
    s.add_argument("--expire-at", type=int, help="auto-clear at unix-epoch seconds")
    s.set_defaults(func=cmd_maintenance)

    s = sub.add_parser("unfence", help="clear a fence/maintenance override")
    s.add_argument("node", type=int)
    s.add_argument("--by", help="operator name (default: $USER)")
    s.set_defaults(func=cmd_unfence)

    s = sub.add_parser("remove", help="hard delete a Fenced node (after drain)")
    s.add_argument("node", type=int)
    s.add_argument("--by", help="operator name (default: $USER)")
    s.add_argument("--yes", action="store_true", help="skip interactive confirm")
    s.set_defaults(func=cmd_remove)

    s = sub.add_parser(
        "decommission",
        help="HDFS-style: fence → poll until drained → remove",
    )
    s.add_argument("node", type=int)
    s.add_argument("--reason", required=True)
    s.add_argument("--by", help="operator name (default: $USER)")
    s.add_argument("--force", action="store_true")
    s.add_argument("--yes", action="store_true")
    s.add_argument("--poll", type=int, default=30, help="seconds between polls (default: 30)")
    s.add_argument(
        "--timeout",
        type=int,
        default=0,
        help="abandon after N seconds (0 = wait forever)",
    )
    s.set_defaults(func=cmd_decommission)

    s = sub.add_parser(
        "auto-reissue",
        help="list extents ready for EC convert reissue (advisory)",
    )
    s.add_argument("--dry-run", action="store_true", default=True)
    s.set_defaults(func=cmd_auto_reissue)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.manager:
        os.environ["AUTUMN_MANAGER"] = args.manager
    global DEFAULT_MANAGER
    DEFAULT_MANAGER = os.environ.get("AUTUMN_MANAGER", DEFAULT_MANAGER)
    try:
        return args.func(args)
    except OpError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
