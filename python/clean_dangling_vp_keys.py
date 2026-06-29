#!/usr/bin/env python3
"""Find + delete keys whose value is UNREADABLE because its ValuePointer
references a deleted/lost log_stream extent ("extent N not found").

This is the operator cleanup after a dangling-VP data-loss event (e.g. the F1
GC punch-before-checkpoint class): the metadata + key survive but the value
bytes are gone, so `get` fails. This tool enumerates every value-bearing key,
test-reads it, and for each broken one deletes the key plus its derived
metadata (`.meta/<k>/*`, `.hls/<k>/*`, `.thumb/<k>`).

Ops-tool convention: shells out to the existing `autumn-client` CLI, adds no new
RPC. Bounded worker pool (default 6) + retries to avoid macOS ephemeral-port
exhaustion (see python/del_prefix.py sibling).

Usage:
    AC=./target/release/autumn-client python3 python/clean_dangling_vp_keys.py            # dry-run
    AC=./target/release/autumn-client python3 python/clean_dangling_vp_keys.py --apply     # delete
"""
import os, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor

MGR = os.environ.get("AUTUMN_MANAGER", "127.0.0.1:9001")
BIN = os.environ.get("AC") or os.environ.get("AUTUMN_CLIENT_BIN", "autumn-client")
WORKERS = int(os.environ.get("WORKERS", "6"))
DRY = "--apply" not in sys.argv


def cli(args, **kw):
    return subprocess.run([BIN, "--manager", MGR] + args, capture_output=True, text=True, **kw)


def list_all():
    keys, start = [], ""
    while True:
        a = ["ls", "--limit", "1000"] + (["--start", start] if start else [])
        page = [l for l in cli(a).stdout.split("\n") if l.strip() != ""]
        if start and page and page[0] == start:
            page = page[1:]  # --start is inclusive
        if not page:
            break
        keys += page
        start = page[-1]
        if len(page) < 999:
            break
    return keys


def is_broken(k):
    r = subprocess.run([BIN, "--manager", MGR, "get", k], stdout=subprocess.DEVNULL,
                       stderr=subprocess.PIPE, stdin=subprocess.DEVNULL)
    return k, (r.returncode != 0 and b"not found" in r.stderr)


def main():
    print("enumerating keys...")
    keys = list_all()
    meta = set(k for k in keys if k.startswith((".meta/", ".hls/", ".thumb/")))
    value_keys = [k for k in keys if k not in meta]
    print(f"total {len(keys)}  value {len(value_keys)}  meta/hls/thumb {len(meta)}")

    broken = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for k, bad in ex.map(is_broken, value_keys):
            if bad:
                broken.append(k)
    print(f"BROKEN value keys: {len(broken)}")

    todel = set(broken)
    for b in broken:
        for pfx in (f".meta/{b}/", f".hls/{b}/", f".thumb/{b}"):
            todel |= set(k for k in keys if k.startswith(pfx) or k == pfx.rstrip("/"))
    print(f"keys to delete (broken + their meta/hls/thumb): {len(todel)}")

    if DRY:
        print("\n[DRY-RUN] nothing deleted. pass --apply to delete.")
        return

    def dele(k):
        for _ in range(3):
            if cli(["del", k]).returncode == 0:
                return True
            time.sleep(0.2)
        return False

    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        done = sum(1 for r in ex.map(dele, sorted(todel)) if r)
    print(f"deleted {done}/{len(todel)}")


if __name__ == "__main__":
    main()
