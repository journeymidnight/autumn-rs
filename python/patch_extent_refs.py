#!/usr/bin/env python3
"""Repair leaked `MgrExtentInfo.refs` in etcd to match true stream membership.

One-off operator repair for the MERGE-REFS-LEAK residue: a few extents carry a
`refs` higher than the number of streams that actually list them (the manager's
pre-fix merge over-counted). `refs` is a plain `u64` field of the rkyv-archived
`MgrExtentInfo` stored at etcd key `extents/<id>`; patching it in place is a
fixed-offset scalar edit — no structural change, so the record still decodes.

`ArchivedMgrExtentInfo` is a fixed 88 bytes (the two `Vec` fields are 8-byte
out-of-line pointers regardless of content), and rkyv places the root struct at
the END of the buffer. So:  root_offset = len - 88,  refs is at root + 32
(extent_id@+0, replicates@+8, parity@+16, eversion@+24, refs@+32). The script
verifies extent_id@root matches and refs@+32 equals the expected current value
before writing — a mismatch aborts (guards against a layout/codec change).

SAFETY:
  * The manager MUST be stopped first — it holds extents in memory and would
    overwrite this etcd patch on its next mutation; stop it so the corrected
    value is loaded fresh on restart (`cluster.sh stop-manager` / `restart-manager`).
  * Original values are backed up (base64) to --backup before any write.
  * Uses the etcd v3 REST gateway (base64 — binary-clean, unlike `etcdctl put`).
  * Dry-run by default; pass --apply to write.

Usage (stop the manager first):
    python3 python/patch_extent_refs.py 10:0 33:1                 # dry-run
    python3 python/patch_extent_refs.py 10:0 33:1 --apply
"""
import argparse
import base64
import json
import sys
import urllib.request

ARCHIVED_SIZE = 88   # sizeof(ArchivedMgrExtentInfo)
REFS_OFF = 32        # refs offset within the root struct
U64 = 8


def rpc(endpoint, path, body):
    req = urllib.request.Request(
        f"http://{endpoint}/v3/kv/{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def get(endpoint, key):
    resp = rpc(endpoint, "range", {"key": base64.b64encode(key.encode()).decode()})
    kvs = resp.get("kvs")
    if not kvs:
        return None
    return base64.b64decode(kvs[0]["value"])


def put(endpoint, key, value):
    rpc(endpoint, "put", {
        "key": base64.b64encode(key.encode()).decode(),
        "value": base64.b64encode(value).decode(),
    })


def locate(val, eid):
    """Return (root_off, refs_off, current_refs); raise on any layout mismatch."""
    if len(val) < ARCHIVED_SIZE:
        raise ValueError(f"value too short ({len(val)} < {ARCHIVED_SIZE})")
    root = len(val) - ARCHIVED_SIZE
    got_eid = int.from_bytes(val[root:root + U64], "little")
    if got_eid != eid:
        raise ValueError(f"extent_id@root = {got_eid}, expected {eid} (layout changed?)")
    refs_off = root + REFS_OFF
    cur = int.from_bytes(val[refs_off:refs_off + U64], "little")
    return root, refs_off, cur


def main(argv=None):
    ap = argparse.ArgumentParser(description="Patch MgrExtentInfo.refs in etcd")
    ap.add_argument("targets", nargs="+", metavar="EID:REFS",
                    help="extent_id:new_refs (e.g. 10:0 33:1)")
    ap.add_argument("--etcd", default="127.0.0.1:2379")
    ap.add_argument("--expect-current", type=int, default=2,
                    help="abort unless the extent's current refs equals this (default 2)")
    ap.add_argument("--backup", default="/tmp/extent_refs_backup.json")
    ap.add_argument("--apply", action="store_true", help="write (default: dry-run)")
    args = ap.parse_args(argv)

    plans, backup = [], {}
    for t in args.targets:
        eid_s, refs_s = t.split(":")
        eid, new_refs = int(eid_s), int(refs_s)
        val = get(args.etcd, f"extents/{eid}")
        if val is None:
            print(f"extent {eid}: NOT FOUND in etcd — skip")
            continue
        root, refs_off, cur = locate(val, eid)
        backup[str(eid)] = base64.b64encode(val).decode()
        if cur != args.expect_current:
            print(f"extent {eid}: current refs={cur} != --expect-current {args.expect_current} "
                  f"— ABORT (re-check before forcing)")
            return 2
        patched = bytearray(val)
        patched[refs_off:refs_off + U64] = new_refs.to_bytes(U64, "little")
        plans.append((eid, bytes(patched), cur, new_refs))
        print(f"extent {eid}: refs {cur} -> {new_refs}  (root@{root}, refs@{refs_off}, len={len(val)})")

    if not plans:
        print("nothing to do")
        return 0
    if not args.apply:
        print("\nDRY-RUN (no writes). Stop the manager, then re-run with --apply.")
        return 0

    with open(args.backup, "w") as f:
        json.dump(backup, f, indent=1)
    print(f"\nbacked up {len(backup)} original value(s) to {args.backup}")
    for eid, patched, cur, new_refs in plans:
        put(args.etcd, f"extents/{eid}", patched)
        rb = get(args.etcd, f"extents/{eid}")
        _, refs_off, now = locate(rb, eid)
        status = "OK" if now == new_refs else "FAILED"
        print(f"extent {eid}: wrote refs={new_refs}, readback={now} [{status}]")
        if now != new_refs:
            return 3
    print("done — restart the manager so it replays the corrected refs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
