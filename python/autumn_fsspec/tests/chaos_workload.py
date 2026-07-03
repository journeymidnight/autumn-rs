"""fsspec-interface chaos workload (mirrors autumn_kvcache/tests/chaos_workload.py).

Runs a continuous write → readback-verify loop through `AutumnFileSystem`
(pipe_file / cat_file / ls / rm — the full fsspec → SDK → PS → EN path,
chunked layout included) against an externally-running cluster, while the
bash harness (`scripts/fsspec_chaos.sh`) kills PSes / the manager underneath.

Protocol (stdout, line-oriented — the harness greps these):
    OK <r>          round r stored AND a random prior file read back byte-exact
    ERR <r> <what>  transient failure (expected during failover windows)
    MISMATCH <r>    CORRUPTION — a successfully-stored file came back wrong
    DONE total=<n> verified=<m> mismatches=<k>

Uncertainty rule ([[feedback_chaos_timeout_uncertain]]): an op that RAISED is
uncertain — the file is dropped from the manifest (it may or may not have
landed); only files whose write returned SUCCESS are integrity-checked.

File r content: deterministic from the round number (byte i == (r*7+i) & 0xff)
so `--verify <manifest>` can re-derive expectations in a fresh process.

Stop: create the file given in $CHAOS_STOP_FILE.
"""

from __future__ import annotations

import hashlib
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from autumn_fsspec import AutumnFileSystem  # noqa: E402

MANAGER = os.environ.get("AUTUMN_MANAGER", "127.0.0.1:9001")
ROOT = os.environ.get("CHAOS_ROOT", "fsspec_chaos")
CHUNK = 256 * 1024  # multi-chunk kicks in at 512 KiB; ZC-eligible ≥ 64 KiB


def content_for(r: int, size: int) -> bytes:
    return bytes((r * 7 + i) & 0xFF for i in range(size))


def size_for(r: int) -> int:
    # 16 KiB .. ~1.5 MiB → 1-6 chunks at 256 KiB; every 7th is sub-chunk small
    rnd = random.Random(r)
    if r % 7 == 0:
        return rnd.randint(1, 4000)
    return rnd.randint(16 * 1024, 1536 * 1024)


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def connect() -> AutumnFileSystem:
    return AutumnFileSystem(
        manager=MANAGER, root=ROOT, chunk_size=CHUNK, skip_instance_cache=True
    )


def run_workload(manifest_path: str, stop_file: str) -> int:
    fs = connect()
    manifest: dict[str, tuple[int, str]] = {}  # path -> (size, sha)
    total = verified = mismatches = 0
    r = 0
    while not os.path.exists(stop_file):
        r += 1
        path = f"d{r % 4}/f-{r}.bin"
        size = size_for(r)
        data = content_for(r, size)

        # write; an exception ⇒ UNCERTAIN ⇒ not manifested
        try:
            fs.pipe_file(path, data)
        except Exception as e:  # noqa: BLE001
            print(f"ERR {r} write:{type(e).__name__}", flush=True)
            # the client may hold a poisoned conn during failover; reconnect
            try:
                fs = connect()
            except Exception:
                time.sleep(0.5)
            continue
        manifest[path] = (size, sha(data))
        total += 1

        # occasionally overwrite an old file (exercises stale-chunk reap)
        if r % 11 == 0 and manifest:
            op = random.Random(r ^ 0xBEEF).choice(sorted(manifest))
            osize = size_for(r ^ 0xBEEF) // 3 + 1
            odata = content_for(r ^ 0xBEEF, osize)
            try:
                fs.pipe_file(op, odata)
                manifest[op] = (osize, sha(odata))
            except Exception as e:  # noqa: BLE001
                del manifest[op]  # uncertain either way now
                print(f"ERR {r} overwrite:{type(e).__name__}", flush=True)

        # occasionally delete (exception ⇒ uncertain ⇒ drop from manifest)
        if r % 13 == 0 and manifest:
            dp = random.Random(r ^ 0xF00D).choice(sorted(manifest))
            try:
                fs.rm_file(dp)
            except Exception as e:  # noqa: BLE001
                print(f"ERR {r} rm:{type(e).__name__}", flush=True)
            del manifest[dp]

        # verify a random prior manifested file byte-exact
        if manifest:
            vp = random.Random(r).choice(sorted(manifest))
            vsize, vsha = manifest[vp]
            try:
                got = fs.cat_file(vp)
            except Exception as e:  # noqa: BLE001
                print(f"ERR {r} read:{type(e).__name__}", flush=True)
                continue
            if len(got) != vsize or sha(bytes(got)) != vsha:
                mismatches += 1
                print(f"MISMATCH {r} {vp} want={vsize} got={len(got)}", flush=True)
            else:
                verified += 1
                print(f"OK {r}", flush=True)

    with open(manifest_path, "w") as f:
        for p, (size_, sha_) in sorted(manifest.items()):
            f.write(f"{p}\t{size_}\t{sha_}\n")
    print(f"DONE total={total} verified={verified} mismatches={mismatches}", flush=True)
    return 1 if mismatches else 0


def run_verify(manifest_path: str) -> int:
    """Fresh process + fresh client: EVERY manifested file must read back
    byte-exact, appear in `find`, and report the right size via `info`."""
    fs = connect()
    entries = []
    with open(manifest_path) as f:
        for line in f:
            p, size_, sha_ = line.rstrip("\n").split("\t")
            entries.append((p, int(size_), sha_))
    found = set(fs.find(""))
    bad = 0
    for p, size_, sha_ in entries:
        try:
            got = fs.cat_file(p)
        except Exception as e:  # noqa: BLE001
            print(f"VERIFY-FAIL {p} read:{type(e).__name__}:{e}", flush=True)
            bad += 1
            continue
        if len(got) != size_ or sha(bytes(got)) != sha_:
            print(f"VERIFY-FAIL {p} content want={size_} got={len(got)}", flush=True)
            bad += 1
            continue
        if fs.info(p)["size"] != size_:
            print(f"VERIFY-FAIL {p} info-size", flush=True)
            bad += 1
            continue
        if p not in found:
            print(f"VERIFY-FAIL {p} missing-from-find", flush=True)
            bad += 1
    print(f"VERIFY done files={len(entries)} bad={bad}", flush=True)
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--verify":
        sys.exit(run_verify(sys.argv[2]))
    manifest = sys.argv[1] if len(sys.argv) > 1 else "/tmp/fsspec_chaos_manifest.txt"
    stop = os.environ.get("CHAOS_STOP_FILE", "/tmp/fsspec_chaos_stop")
    sys.exit(run_workload(manifest, stop))
