#!/usr/bin/env bash
# F-FS-UNIFY M3 — live fsspec e2e over the shared inode layout (autumn.Fs).
#
#   1. build + install the `autumn` wheel (with Fs) + fsspec/datasets/pytest
#      into a throwaway venv,
#   2. bring up an ISOLATED minimal cluster (memory-only manager, 1 EN, 1 PS,
#      loopback, no etcd) from this tree's debug binaries,
#   3. run the live fsspec suite (tests/test_e2e_cluster.py) against it —
#      chunk round-trips, ranged reads, ls/find/rm, overwrite-shrink, append,
#      exclusive create, and a HuggingFace `datasets` round-trip,
#   4. tear everything down.
# Does NOT touch any system venv or any other cluster.
#
#   cargo build --workspace          # debug binaries first
#   bash python/autumn_fsspec/tests/run_fsspec_e2e.sh
set -u
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AFSSPEC_WORK:-/tmp/afsspec-pye2e}"
VENV="${AFSSPEC_VENV:-/tmp/afsspec-venv}"
MGR="127.0.0.1:19501"
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[fsspec-e2e] build autumn wheel (with Fs) + deps into venv"
rm -rf "$VENV"; python3 -m venv "$VENV"
"$VENV/bin/pip" install -q maturin pytest fsspec datasets >"$WORK/pip.log" 2>&1 \
    || { echo "FAIL pip install"; tail -8 "$WORK/pip.log"; exit 1; }
# shellcheck disable=SC1091
source "$VENV/bin/activate"
( cd python && maturin develop 2>&1 | tail -2 ) || { echo "FAIL maturin"; exit 1; }

echo "[fsspec-e2e] cluster bring-up"
"$BIN/autumn-manager-server" --port 19501 --listen 127.0.0.1 >"$WORK/mgr.log" 2>&1 & PIDS+=($!)
wait_port 19501 20 || { echo FAIL mgr; tail -6 "$WORK/mgr.log"; exit 1; }
"$BIN/autumn-op" --manager "$MGR" format --listen :19511 --advertise 127.0.0.1:19511 "$WORK/en0" >"$WORK/fmt.log" 2>&1 || { echo FAIL fmt; cat "$WORK/fmt.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WORK/en0" --port 19511 --manager "$MGR" --cpuset 0 --listen 127.0.0.1 >"$WORK/en0.log" 2>&1 & PIDS+=($!)
wait_port 19511 20 || { echo FAIL en; tail -6 "$WORK/en0.log"; exit 1; }
sleep 3
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 >"$WORK/bs.log" 2>&1 || { echo FAIL bootstrap; cat "$WORK/bs.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port 19521 --manager "$MGR" --data "$WORK/ps1" --listen 127.0.0.1 --advertise 127.0.0.1:19521 >"$WORK/ps1.log" 2>&1 & PIDS+=($!)
wait_port 19521 20 || { echo FAIL ps; tail -6 "$WORK/ps1.log"; exit 1; }
sleep 4

echo "[fsspec-e2e] live fsspec suite (autumn.Fs backing)"
AUTUMN_MANAGER="$MGR" python -m pytest python/autumn_fsspec/tests/test_e2e_cluster.py -q 2>&1 | tail -25
RC=${PIPESTATUS[0]}; echo "===== fsspec-e2e exit: $RC ====="; exit $RC
