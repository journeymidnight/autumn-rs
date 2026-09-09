#!/usr/bin/env bash
# Self-contained Python e2e for the `autumn.Memory` PyO3 binding:
#   1. build + install the `autumn` wheel (with Memory) into a throwaway venv,
#   2. bring up an ISOLATED minimal cluster (memory-only manager, 1 EN, 1 PS,
#      loopback, no etcd) from this tree's debug binaries,
#   3. drive the full Memory surface from Python against it,
#   4. tear everything down.
# Does NOT touch any system venv or any other cluster.
#
#   cargo build --workspace          # debug binaries first
#   bash python/tests/run_memory_e2e.sh
set -u
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AM_WORK:-/tmp/am-pye2e}"
VENV="${AM_VENV:-/tmp/am-venv}"
MGR="127.0.0.1:19001"
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[py-e2e] build autumn wheel (with Memory) into venv"
rm -rf "$VENV"; python3 -m venv "$VENV"
"$VENV/bin/pip" install -q maturin 2>&1 | tail -1
# shellcheck disable=SC1091
source "$VENV/bin/activate"
( cd python && maturin develop 2>&1 | tail -2 ) || { echo "FAIL maturin"; exit 1; }

echo "[py-e2e] cluster bring-up"
"$BIN/autumn-manager-server" --port 19001 --listen 127.0.0.1 >"$WORK/mgr.log" 2>&1 & PIDS+=($!)
wait_port 19001 20 || { echo FAIL mgr; tail -6 "$WORK/mgr.log"; exit 1; }
"$BIN/autumn-op" --manager "$MGR" format "$WORK/en0" >"$WORK/fmt.log" 2>&1 || { echo FAIL fmt; cat "$WORK/fmt.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WORK/en0" --port 19101 --manager "$MGR" --cpuset 0 --advertise 127.0.0.1:19101 --listen 127.0.0.1 >"$WORK/en0.log" 2>&1 & PIDS+=($!)
wait_port 19101 20 || { echo FAIL en; tail -6 "$WORK/en0.log"; exit 1; }
sleep 3
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 >"$WORK/bs.log" 2>&1 || { echo FAIL bootstrap; cat "$WORK/bs.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port 19201 --manager "$MGR" --data "$WORK/ps1" --listen 127.0.0.1 --advertise 127.0.0.1:19201 >"$WORK/ps1.log" 2>&1 & PIDS+=($!)
wait_port 19201 20 || { echo FAIL ps; tail -6 "$WORK/ps1.log"; exit 1; }
sleep 4

echo "[py-e2e] python smoke via autumn.Memory"
python - <<'PY'
import autumn
m = autumn.Memory.connect("127.0.0.1:19001", "__am_py", "agent-1")
m.append_event("s1", b'{"role":"user","i":0}')
m.append_event("s1", b'{"role":"asst","i":1}')
r = m.replay_session("s1"); assert len(r) == 2 and b'"i":0' in r[0], r
assert b'"i":1' in m.recent_events("s1", 1)[0]
m.put_fact("p", "name", b"Alice", 0)
assert m.get_fact("p", "name") == b"Alice" and m.get_fact("p", "x") is None
assert len(m.list_facts("p")) == 1
m.delete_fact("p", "name"); assert m.get_fact("p", "name") is None
m.index_memory("d1", "the cat sat on the mat")
m.index_memory("d2", "a cat and a dog play")
m.index_memory("d3", "quantum error codes")
ids = [h[0] for h in m.search_lexical("cat", 10)]
assert "d1" in ids and "d2" in ids and "d3" not in ids, ids
m.index_vector("d1", [1.0,0.0,0.0]); m.index_vector("d2", [0.0,1.0,0.0]); m.index_vector("d3", [0.0,0.0,1.0])
assert m.search_vector([0.9,0.1,0.0], 1, 4)[0][0] == "d1"
assert m.train_centroids(3, 25, 7) >= 1
assert m.search_vector([0.1,0.95,0.0], 1, 3)[0][0] == "d2"
assert any(x[0] == "d1" for x in m.search_hybrid("cat", [0.9,0.1,0.0], 3, 3))
for d in ("d1","d2","d3"): m.delete_memory(d)
m.close()
print("PY-E2E OK: autumn.Memory full surface")
PY
RC=$?; echo "===== py-e2e exit: $RC ====="; exit $RC
