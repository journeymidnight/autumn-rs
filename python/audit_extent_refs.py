#!/usr/bin/env python3
"""Audit extent reference-count consistency against actual stream membership.

The manager's `MgrExtentInfo.refs` is meant to equal the number of times an
extent appears across all live streams' `extent_ids` (CoW split increments it,
GC punch_holes / truncate decrements it). When those drift apart you get two
dangerous, normally-invisible states:

  * ORPHAN  — `refs > 0` but the extent is in ZERO live streams. It can never
    reach `refs == 0` (no stream to punch it out of), so it is NEVER physically
    reclaimed, AND it is invisible to `autumn-op info` (which only renders
    extents that some live stream lists). Pure silent disk leak.
  * REFS-MISMATCH — a visible extent whose `refs` != its real membership count
    (the same leak, caught before it becomes a full orphan).
  * DUP-MEMBERSHIP — an extent listed more than once inside a single stream's
    `extent_ids` (a CoW-shared extent spliced into a stream that already had
    it, e.g. a merge that concatenates without dedup).

Data sources (ops-tool convention: shell out to existing tools, add no RPC):
  * `etcdctl get --prefix extents/ --keys-only`  — the FULL extent id set
    (the only source that includes orphans; `info` hides them by construction).
  * `autumn-op --json info`                      — live stream `extent_ids`
    + the `refs` of every stream-member extent.

NOTE: `vp_table_refs` is NOT exposed by either tool, so this audit only checks
the `refs` (stream-membership) lifetime. An extent flagged here may ALSO still
be legitimately held by `vp_table_refs > 0` (live SSTable ValuePointers) — DO
NOT delete a flagged extent's files until you have confirmed via the manager /
etcd that `vp_table_refs == 0`, or you will lose data still referenced by live
SSTables.

Usage:
    python3 python/audit_extent_refs.py
    python3 python/audit_extent_refs.py --manager 127.0.0.1:9001 \\
        --etcd 127.0.0.1:2379 --op ./target/release/autumn-op
"""
import argparse
import json
import os
import subprocess
import sys
from collections import Counter


def sh(args):
    """Run a command, return stdout as text; raise with stderr on failure."""
    p = subprocess.run(args, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed (rc={p.returncode}): {p.stderr.strip()}")
    return p.stdout


def etcd_extent_ids(etcdctl, endpoint):
    """Full set of extent ids from the etcd `extents/<id>` keys (incl. orphans)."""
    out = sh([etcdctl, "--endpoints=" + endpoint, "get", "--prefix", "extents/", "--keys-only"])
    ids = set()
    for line in out.splitlines():
        line = line.strip()
        # Defensive: skip etcdctl warnings / blank lines; keep only extents/<int>.
        if not line.startswith("extents/"):
            continue
        tail = line[len("extents/"):]
        if tail.isdigit():
            ids.add(int(tail))
    return ids


def op_info(op_bin, manager):
    """`autumn-op --json info` → (streams list, {eid: extent-dict}).

    Post-MERGE-REFS-LEAK the manager's full dump includes orphan (non-member)
    extents, each carrying `refs` + `vp_table_refs`, so we can classify orphans
    as retained-by-live-data vs reclaimable-dead without extra RPCs.
    """
    out = sh([op_bin, "--manager", manager, "--json", "info"])
    doc = json.loads(out)
    streams = doc.get("streams") or []
    extents = {e["extent_id"]: e for e in (doc.get("extents") or [])}
    return streams, extents


def main(argv=None):
    ap = argparse.ArgumentParser(description="Audit extent refs vs stream membership")
    ap.add_argument("--manager", default=os.environ.get("AUTUMN_MANAGER", "127.0.0.1:9001"))
    ap.add_argument("--etcd", default=os.environ.get("AUTUMN_ETCD", "127.0.0.1:2379"))
    ap.add_argument("--op", default=os.environ.get("AUTUMN_OP_BIN", "./target/release/autumn-op"))
    ap.add_argument("--etcdctl", default=os.environ.get("ETCDCTL_BIN", "etcdctl"))
    args = ap.parse_args(argv)

    all_ids = etcd_extent_ids(args.etcdctl, args.etcd)
    streams, extents = op_info(args.op, args.manager)

    # listings[eid] = total occurrences across all streams' extent_ids.
    # per_stream_dups: (stream_id, eid) listed more than once in that one stream.
    listings = Counter()
    per_stream_dups = []
    for s in streams:
        sid = s["stream_id"]
        c = Counter(s["extent_ids"])
        for eid, n in c.items():
            listings[eid] += n
            if n > 1:
                per_stream_dups.append((sid, eid, n))

    dead_orphans = []      # 0 streams AND vp_table_refs==0 -> reclaimable leak
    retained_orphans = []  # 0 streams BUT vp_table_refs>0 -> live data, OK
    mismatches = []        # visible member, refs != listing count
    for eid in sorted(all_ids):
        listed = listings.get(eid, 0)
        e = extents.get(eid)
        refs = e["refs"] if e else None
        vp = e.get("vp_table_refs", 0) if e else 0
        if listed == 0:
            (retained_orphans if vp > 0 else dead_orphans).append((eid, refs, vp))
        elif refs is not None and refs != listed:
            mismatches.append((eid, refs, listed))

    print(f"manager={args.manager} etcd={args.etcd}")
    print(f"extents in etcd: {len(all_ids)} | in info: {len(extents)} | streams: {len(streams)}")
    print()

    if dead_orphans:
        print(f"!! DEAD ORPHANS ({len(dead_orphans)}) — 0 streams AND vp_table_refs==0.")
        print("   Un-reclaimable leaks (refs can't reach 0 via GC — no stream to punch).")
        print("   Safe to reap: delete etcd extents/<id> + unlink files (python/reclaim ...).")
        for eid, refs, _vp in dead_orphans:
            print(f"     extent {eid}: streams=0, refs={refs}, vp_table_refs=0")
        print()

    if mismatches:
        print(f"!! REFS MISMATCH ({len(mismatches)}) — refs != actual stream membership.")
        print("   Leaked refcount; fix with python/patch_extent_refs.py (manager stopped).")
        for eid, refs, listed in mismatches:
            print(f"     extent {eid}: refs={refs}, actual_listings={listed} "
                  f"(leaked {refs - listed:+d})")
        print()

    if per_stream_dups:
        print(f"!! DUPLICATE MEMBERSHIP ({len(per_stream_dups)}) — extent listed >1x in one stream.")
        for sid, eid, n in per_stream_dups:
            print(f"     stream {sid}: extent {eid} listed {n}x")
        print()

    # Retained orphans are informational, NOT a problem: a log extent GC-punched
    # out of its stream but still holding live values referenced by SSTs. Correct
    # state; reclaims naturally once those keys die + compact (vp_table_refs->0,
    # then it becomes a DEAD ORPHAN). refs should equal 0 here (matches 0 streams).
    if retained_orphans:
        print(f"ok: {len(retained_orphans)} retained orphan(s) (no stream, held by live "
              f"vp_table_refs — serving data, not a leak):")
        for eid, refs, vp in retained_orphans:
            note = "" if refs == 0 else f"  [refs={refs} should be 0 — leak]"
            print(f"     extent {eid}: streams=0, vp_table_refs={vp}{note}")
        print()

    problems = bool(dead_orphans or mismatches or per_stream_dups)
    # A retained orphan with refs != 0 is also a leak worth flagging.
    if any(refs != 0 for _e, refs, _v in retained_orphans if refs is not None):
        problems = True
    if not problems:
        print("OK: every extent's refs matches its live stream membership.")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
