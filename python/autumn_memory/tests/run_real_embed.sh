#!/usr/bin/env bash
# Real-model semantic e2e: start a local sglang embedding server + an isolated
# autumn cluster, drive AutumnMemory's vector/hybrid legs (and the Hermes
# provider's vector tool path) against the REAL embeddings endpoint, tear down.
#
# Everything lives within THIS one process (sglang is a child) so it doesn't
# need to survive across calls. Reuses an existing venv with `autumn` +
# `autumn_memory` (build one first, e.g. via run_hermes_test.sh).
#
#   EMBED_MODEL=Alibaba-NLP/gte-Qwen2-1.5B-instruct EMBED_GPU=7 \
#     bash python/autumn_memory/tests/run_real_embed.sh
set -uo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AM_WORK:-/tmp/am-real}"
VENV="${AM_VENV:-/tmp/am-hermes-venv}"
MGR="127.0.0.1:19001"
EMBED_MODEL="${EMBED_MODEL:-Alibaba-NLP/gte-Qwen2-1.5B-instruct}"
EMBED_GPU="${EMBED_GPU:-7}"
EMBED_PORT="${EMBED_PORT:-30000}"
HERMES_AGENT_PATH="${HERMES_AGENT_PATH:-/data/dongmao_dev/hermes-agent}"
case "$WORK" in /tmp/*) ;; *) echo "refusing rm -rf AM_WORK=$WORK (must be /tmp/*)"; exit 2;; esac
[ -x "$VENV/bin/python" ] || { echo "FAIL: no venv at $VENV (run run_hermes_test.sh first to build one)"; exit 2; }
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=(); SGLANG_PID=""
cleanup() { [ -n "$SGLANG_PID" ] && kill "$SGLANG_PID" 2>/dev/null; for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[real] starting sglang embedding server ($EMBED_MODEL on GPU$EMBED_GPU :$EMBED_PORT)"
# sglang lives in the BASE python (not the isolated venv); the autumn tests run
# in the venv. SGLANG_PY overrides the interpreter if needed.
FLASHINFER_DISABLE_VERSION_CHECK=1 CUDA_VISIBLE_DEVICES="$EMBED_GPU" \
  "${SGLANG_PY:-python3}" -m sglang.launch_server --model-path "$EMBED_MODEL" \
  --is-embedding --host 127.0.0.1 --port "$EMBED_PORT" --mem-fraction-static 0.12 \
  >"$WORK/sglang.log" 2>&1 &
SGLANG_PID=$!
echo "[real] waiting for embeddings endpoint (model load ~1-2min)"
ready=0
for _ in $(seq 1 180); do
  kill -0 "$SGLANG_PID" 2>/dev/null || { echo "FAIL: sglang exited"; tail -25 "$WORK/sglang.log"; exit 1; }
  if curl -s -m 3 -X POST "http://127.0.0.1:$EMBED_PORT/v1/embeddings" \
       -H 'Content-Type: application/json' -d "{\"model\":\"$EMBED_MODEL\",\"input\":\"ping\"}" \
       2>/dev/null | grep -q '"embedding"'; then ready=1; break; fi
  sleep 2
done
[ "$ready" = 1 ] || { echo "FAIL: embeddings endpoint not ready"; tail -25 "$WORK/sglang.log"; exit 1; }
echo "[real] embeddings endpoint ready"

echo "[real] autumn cluster bring-up"
"$BIN/autumn-manager-server" --port 19001 --listen 127.0.0.1 >"$WORK/mgr.log" 2>&1 & PIDS+=($!)
wait_port 19001 20 || { echo FAIL mgr; tail -6 "$WORK/mgr.log"; exit 1; }
"$BIN/autumn-op" --manager "$MGR" format --listen :19101 --advertise 127.0.0.1:19101 "$WORK/en0" >"$WORK/fmt.log" 2>&1 || { echo FAIL fmt; cat "$WORK/fmt.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WORK/en0" --port 19101 --manager "$MGR" --cpuset 0 --listen 127.0.0.1 >"$WORK/en0.log" 2>&1 & PIDS+=($!)
wait_port 19101 20 || { echo FAIL en; tail -6 "$WORK/en0.log"; exit 1; }
sleep 3
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 >"$WORK/bs.log" 2>&1 || { echo FAIL bootstrap; cat "$WORK/bs.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port 19201 --manager "$MGR" --data "$WORK/ps1" --listen 127.0.0.1 --advertise 127.0.0.1:19201 >"$WORK/ps1.log" 2>&1 & PIDS+=($!)
wait_port 19201 20 || { echo FAIL ps; tail -6 "$WORK/ps1.log"; exit 1; }
sleep 4

export AUTUMN_MEMORY_MANAGER="$MGR"
export AUTUMN_MEMORY_EMBED_URL="http://127.0.0.1:$EMBED_PORT/v1"
export AUTUMN_MEMORY_EMBED_MODEL="$EMBED_MODEL"
export HERMES_AGENT_PATH

echo "[real] semantic e2e (AutumnMemory vector/hybrid)"
"$VENV/bin/python" python/autumn_memory/tests/test_real_embed.py || exit 1
echo "[real] Hermes provider with the real embedder (vector tool path, coco P1#1)"
"$VENV/bin/python" python/hermes_memory_autumn/tests/test_provider_inproc.py || exit 1
echo "===== real-embed-test exit: 0 ====="
