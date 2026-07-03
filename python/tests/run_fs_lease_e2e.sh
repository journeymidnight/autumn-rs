#!/usr/bin/env bash
# F-FS-UNIFY M4 — lease fencing + cross-client coherence for autumn.Fs.
#
#   1. build the wheel (with Fs) into a throwaway venv,
#   2. bring up an ISOLATED minimal cluster (memory-mode manager, 1 EN, 1 PS),
#   3. drive TWO Fs clients (distinct DaemonClientId) and assert:
#      - write-lease XOR: A holds WRITE ⇒ B's acquire(WRITE) conflicts;
#        A releases ⇒ B acquires (release-unblocks).
#      - coherence: write via A, read via B sees it; A overwrites, B sees the
#        new bytes (fresh reads).
#      - forget(): after B writes+releases (caching the inode), an A overwrite
#        is still seen by B's next read (B evicted its cache on release).
#   4. tear down.
# Does NOT touch any system venv or any other cluster.
#
#   cargo build --workspace
#   bash python/tests/run_fs_lease_e2e.sh
set -u
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AFSL_WORK:-/tmp/afsl-pye2e}"
VENV="${AFSL_VENV:-/tmp/afsl-venv}"
MGR="127.0.0.1:19601"
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[fs-lease-e2e] build autumn wheel into venv"
rm -rf "$VENV"; python3 -m venv "$VENV"
"$VENV/bin/pip" install -q maturin >"$WORK/pip.log" 2>&1 || { echo "FAIL pip"; tail -8 "$WORK/pip.log"; exit 1; }
# shellcheck disable=SC1091
source "$VENV/bin/activate"
( cd python && maturin develop 2>&1 | tail -2 ) || { echo "FAIL maturin"; exit 1; }

echo "[fs-lease-e2e] cluster bring-up"
"$BIN/autumn-manager-server" --port 19601 --listen 127.0.0.1 >"$WORK/mgr.log" 2>&1 & PIDS+=($!)
wait_port 19601 20 || { echo FAIL mgr; tail -6 "$WORK/mgr.log"; exit 1; }
"$BIN/autumn-op" --manager "$MGR" format --listen :19611 --advertise 127.0.0.1:19611 "$WORK/en0" >"$WORK/fmt.log" 2>&1 || { echo FAIL fmt; cat "$WORK/fmt.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WORK/en0" --port 19611 --manager "$MGR" --cpuset 0 --listen 127.0.0.1 >"$WORK/en0.log" 2>&1 & PIDS+=($!)
wait_port 19611 20 || { echo FAIL en; tail -6 "$WORK/en0.log"; exit 1; }
sleep 3
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 >"$WORK/bs.log" 2>&1 || { echo FAIL bootstrap; cat "$WORK/bs.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port 19621 --manager "$MGR" --data "$WORK/ps1" --listen 127.0.0.1 --advertise 127.0.0.1:19621 >"$WORK/ps1.log" 2>&1 & PIDS+=($!)
wait_port 19621 20 || { echo FAIL ps; tail -6 "$WORK/ps1.log"; exit 1; }
sleep 4

echo "[fs-lease-e2e] two-client fencing + coherence via autumn.Fs"
AUTUMN_MANAGER="$MGR" python - <<'PY'
import os, autumn
MGR = os.environ["AUTUMN_MANAGER"]
ROOT = 1

fsA = autumn.Fs.connect(MGR, host="clientA")
fsB = autumn.Fs.connect(MGR, host="clientB")

ino = fsA.create(ROOT, "fenced.bin")

# ── write-lease XOR + release-unblocks ───────────────────────────────────
epA = fsA.acquire(ino, "w")
assert isinstance(epA, int), epA
try:
    fsB.acquire(ino, "w")
    raise AssertionError("B acquired a WRITE lease while A holds it")
except RuntimeError as e:
    assert "conflict" in str(e).lower(), f"unexpected error: {e}"
fsA.release(ino)                 # A releases → B can now take it
epB = fsB.acquire(ino, "w")
assert isinstance(epB, int), epB
fsB.release(ino)
print("PY M4 fencing OK (write-lease XOR + release-unblocks)")

def write_via(fs, data):
    fs.acquire(ino, "w")
    fs.truncate(ino, 0)
    fs.write(ino, 0, data)
    fs.flush(ino)
    fs.release(ino)
    fs.forget(ino)

# ── coherence: B (never cached ino) always reads the latest ─────────────
write_via(fsA, b"v1-content")
assert fsB.read(ino, 0, 100) == b"v1-content"
write_via(fsA, b"v2-much-longer-content")
assert fsB.read(ino, 0, 100) == b"v2-much-longer-content"
assert fsB.getattr(ino)["size"] == len(b"v2-much-longer-content")

# ── forget(): B caches ino by writing v3, then A overwrites v4 → B sees v4
write_via(fsB, b"v3-from-B")     # B now cached ino, then forgot on release
write_via(fsA, b"v4-from-A")
assert fsB.read(ino, 0, 100) == b"v4-from-A", "forget() failed: B served a stale cache"
print("PY M4 coherence OK (cross-client reads see latest; forget evicts)")

fsA.close(); fsB.close()
PY
RC=$?; echo "===== fs-lease-e2e exit: $RC ====="; exit $RC
