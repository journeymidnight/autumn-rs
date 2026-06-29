#!/usr/bin/env python3
"""Bulk-delete every key under a prefix via the autumn-client data-plane CLI.

The store has no prefix/range delete (only single-key `del`), so this pages
`autumn-client ls --prefix P` and fans `autumn-client del` out across a thread
pool. Ops-tool convention: shells out to the existing CLI, adds no new RPC.

Usage:
    python3 python/del_prefix.py --prefix '!bench'
    AUTUMN_CLIENT_BIN=./target/release/autumn-client \\
        python3 python/del_prefix.py --prefix '!bench' --workers 32

    --dry-run   enumerate + count only, delete nothing
"""
import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

DEFAULT_MANAGER = os.environ.get("AUTUMN_MANAGER", "127.0.0.1:9001")
DEFAULT_BIN = os.environ.get("AUTUMN_CLIENT_BIN", "autumn-client")


def run_client(bin_path, manager, args):
    cmd = [bin_path, "--manager", manager] + args
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        sys.exit(
            f"could not find `{bin_path}`; set AUTUMN_CLIENT_BIN or pass "
            f"--client-bin (e.g. ./target/release/autumn-client)"
        )


def scan_prefix(bin_path, manager, prefix, batch):
    """Page through `ls --prefix` (note: --start is INCLUSIVE) → list of keys."""
    keys = []
    start = prefix          # begin the scan at the prefix
    prev_last = None
    while True:
        r = run_client(bin_path, manager,
                       ["ls", "--prefix", prefix, "--start", start,
                        "--limit", str(batch)])
        if r.returncode != 0:
            sys.exit(f"ls failed: {r.stderr.strip() or r.stdout.strip()}")
        raw = [ln for ln in r.stdout.splitlines() if ln.startswith(prefix)]
        if not raw:
            break
        page = raw
        # --start is inclusive: the first key of every page after the first
        # repeats the previous page's last key — drop it.
        if prev_last is not None and page and page[0] == prev_last:
            page = page[1:]
        keys.extend(page)
        prev_last = raw[-1]
        if len(raw) < batch:    # short page → no more results
            break
        start = prev_last
    return keys


def main(argv=None):
    p = argparse.ArgumentParser(description="bulk-delete keys under a prefix")
    p.add_argument("--prefix", required=True, help="key prefix to delete")
    p.add_argument("--manager", default=DEFAULT_MANAGER,
                   help=f"manager address (default {DEFAULT_MANAGER})")
    p.add_argument("--client-bin", default=DEFAULT_BIN,
                   help=f"autumn-client path (default {DEFAULT_BIN})")
    p.add_argument("--workers", type=int, default=8, help="parallel del workers")
    p.add_argument("--batch", type=int, default=5000, help="ls page size")
    p.add_argument("--retries", type=int, default=8,
                   help="per-key retries on transient failure (connect churn)")
    p.add_argument("--dry-run", action="store_true", help="count only, no delete")
    args = p.parse_args(argv)

    print(f"scanning prefix {args.prefix!r} ...", flush=True)
    keys = scan_prefix(args.client_bin, args.manager, args.prefix, args.batch)
    total = len(keys)
    print(f"found {total} key(s) under {args.prefix!r}")
    if total == 0:
        return 0
    if args.dry_run:
        print("dry-run: nothing deleted")
        return 0

    failures = []
    done = 0

    def delete(k):
        # Retry with backoff: tens of thousands of short-lived cold
        # connections churn ephemeral ports / TIME_WAIT, so a `cannot
        # connect to any manager` is transient — the port pressure eases
        # within a second or two. Retrying per-key converges in one run.
        last = ""
        for attempt in range(args.retries):
            r = run_client(args.client_bin, args.manager, ["del", k])
            if r.returncode == 0:
                return k, 0, ""
            last = r.stderr.strip() or r.stdout.strip()
            time.sleep(min(0.2 * (2 ** attempt), 3.0))
        return k, 1, last

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for k, rc, msg in ex.map(delete, keys):
            done += 1
            if rc != 0:
                failures.append((k, msg))
            if done % 5000 == 0 or done == total:
                print(f"  deleted {done}/{total}"
                      f" ({len(failures)} failed)", flush=True)

    if failures:
        print(f"\n{len(failures)} delete(s) FAILED; first few:")
        for k, msg in failures[:10]:
            print(f"  {k}: {msg}")
        return 1
    print(f"done: {total} key(s) deleted under {args.prefix!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
