#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

OPS="${APPEND_BENCH_OPS:-20000}"
PAYLOAD="${APPEND_BENCH_PAYLOAD:-4096}"
WARMUP="${APPEND_BENCH_WARMUP:-1000}"
echo "[append-bench] ops=$OPS payload=$PAYLOAD warmup=$WARMUP batch=16 extent=3GiB"
APPEND_BENCH_OPS="$OPS" \
APPEND_BENCH_PAYLOAD="$PAYLOAD" \
APPEND_BENCH_WARMUP="$WARMUP" \
  cargo test -p autumn-manager --test append_benchmark benchmark_append_stream_throughput -- --ignored --nocapture
