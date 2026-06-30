#!/usr/bin/env bash
# Self-contained test for the OpenAI-compatible embedder client. No cluster
# needed — only a venv with the `autumn` binding + `autumn_memory` (the embedder
# ships in that package). Spins up a mock /embeddings server in-process.
#
#   cargo build --workspace        # (for the autumn binding)
#   bash python/autumn_memory/tests/run_embedder_test.sh
set -uo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
VENV="${AM_VENV:-/tmp/am-embed-venv}"
case "$VENV" in /tmp/*) ;; *) echo "refusing to rm -rf AM_VENV=$VENV (must be under /tmp/)"; exit 2;; esac

echo "[embed] venv + autumn wheel + autumn_memory"
rm -rf "$VENV"; python3 -m venv "$VENV"
"$VENV/bin/pip" install -q maturin 2>&1 | tail -1 || { echo FAIL maturin-install; exit 1; }
# shellcheck disable=SC1091
source "$VENV/bin/activate"
( cd python && maturin develop 2>&1 | tail -2 ) || { echo FAIL maturin; exit 1; }
pip install -q -e python/autumn_memory 2>&1 | tail -1 || { echo FAIL pip autumn_memory; exit 1; }

echo "[embed] embedder mock-server test"
python python/autumn_memory/tests/test_embedder.py
RC=$?; echo "===== embedder-test exit: $RC ====="; exit $RC
