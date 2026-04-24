#!/usr/bin/env bash
# perf_check.sh — build release, start fresh 3-replica cluster, run perf-check
#
# Usage:
#   ./perf_check.sh                       # default disk (/tmp/autumn-rs on overlay)
#   ./perf_check.sh --shm                 # RAM tmpfs (/dev/shm/autumn-rs)
#   ./perf_check.sh --update-baseline     # create / overwrite baseline
#   ./perf_check.sh --shm --update-baseline
#
# --shm is useful for isolating the RPC / partition / stream layers from the
# underlying filesystem (extent storage lives in RAM, fsync is a no-op).
# Separate baseline file is used so disk vs RAM numbers don't collide.

set -euo pipefail

# macOS default is 256 open files — far too few for 256-thread benchmarks
ulimit -n 65536 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AC="$SCRIPT_DIR/target/release/autumn-client"

USE_SHM=0
UPDATE_BASELINE=""
PARTITIONS=1
PIPELINE_DEPTH=1
SKIP_CLUSTER=0
TRANSPORT="tcp"           # F100-UCX: --ucx forces UCX, --transport-both runs both
RUN_BOTH=0
while (( $# > 0 )); do
    case "$1" in
        --shm)              USE_SHM=1 ;;
        --update-baseline)  UPDATE_BASELINE="--update-baseline" ;;
        --skip-cluster)     SKIP_CLUSTER=1 ;;
        --ucx)              TRANSPORT="ucx" ;;
        --tcp)              TRANSPORT="tcp" ;;
        --transport-both)   RUN_BOTH=1 ;;
        --partitions)
            shift
            PARTITIONS="${1:-}"
            [[ "$PARTITIONS" =~ ^[0-9]+$ ]] && (( PARTITIONS >= 1 )) \
                || { echo "--partitions must be a positive integer" >&2; exit 1; }
            ;;
        --pipeline-depth)
            shift
            PIPELINE_DEPTH="${1:-}"
            [[ "$PIPELINE_DEPTH" =~ ^[0-9]+$ ]] && (( PIPELINE_DEPTH >= 1 && PIPELINE_DEPTH <= 256 )) \
                || { echo "--pipeline-depth must be an integer in [1, 256]" >&2; exit 1; }
            ;;
        -h|--help)
            sed -n '2,11p' "$0"
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            exit 1
            ;;
    esac
    shift
done

# Emit AUTUMN_BOOTSTRAP_PRESPLIT=N:hexstring when N > 1.
# The bootstrap handler expands "hexstring" into N uniform 8-char hex midpoints
# via hex_split_ranges(); no need to compute midpoints explicitly here.
if (( PARTITIONS > 1 )); then
    export AUTUMN_BOOTSTRAP_PRESPLIT="${PARTITIONS}:hexstring"
    echo "[perf-check] presplit: $AUTUMN_BOOTSTRAP_PRESPLIT"
fi

if (( USE_SHM )); then
    export AUTUMN_DATA_ROOT="/dev/shm/autumn-rs"
    STORAGE_LABEL="RAM tmpfs (/dev/shm)"
    BASELINE_TCP="$SCRIPT_DIR/perf_baseline_shm.json"
    BASELINE_UCX="$SCRIPT_DIR/perf_baseline_shm_ucx.json"
else
    export AUTUMN_DATA_ROOT="/tmp/autumn-rs"
    STORAGE_LABEL="disk ($AUTUMN_DATA_ROOT)"
    BASELINE_TCP="$SCRIPT_DIR/perf_baseline.json"
    BASELINE_UCX="$SCRIPT_DIR/perf_baseline_ucx_cluster.json"
fi

# F100-UCX: build with the ucx feature when any UCX run is requested.
# Without it, AUTUMN_TRANSPORT=ucx in the cluster would panic at init().
NEED_UCX_FEATURE=0
if [[ "$TRANSPORT" == "ucx" || $RUN_BOTH -eq 1 ]]; then
    NEED_UCX_FEATURE=1
fi
echo "[perf-check] building release binaries$([ $NEED_UCX_FEATURE -eq 1 ] && echo " (with --features autumn-transport/ucx)")..."
cd "$SCRIPT_DIR"
if (( NEED_UCX_FEATURE )); then
    cargo build --workspace --release --exclude autumn-fuse \
        --features autumn-transport/ucx 2>&1 \
        | grep -E "^(Compiling|Finished|error)" || true
else
    cargo build --workspace --release --exclude autumn-fuse 2>&1 \
        | grep -E "^(Compiling|Finished|error)" || true
fi

# Inner runner: starts cluster under given AUTUMN_TRANSPORT, runs perf-check
# against its baseline, leaves cluster running for caller to clean up.
run_perf() {
    local mode="$1"
    local baseline="$2"
    echo
    echo "============================================================"
    echo "[perf-check] mode=$mode storage=$STORAGE_LABEL baseline=$baseline"
    echo "============================================================"
    if (( SKIP_CLUSTER == 0 )); then
        bash "$SCRIPT_DIR/cluster.sh" clean
        AUTUMN_TRANSPORT="$mode" bash "$SCRIPT_DIR/cluster.sh" start 3
    else
        echo "[perf-check] --skip-cluster: assuming cluster is already running in $mode mode"
    fi
    AUTUMN_TRANSPORT="$mode" "$AC" --manager 127.0.0.1:9001 \
        perf-check \
        --nosync \
        --threads 256 \
        --duration 10 \
        --size 4096 \
        --partitions "$PARTITIONS" \
        --pipeline-depth "$PIPELINE_DEPTH" \
        --baseline "$baseline" \
        $UPDATE_BASELINE
}

if (( RUN_BOTH )); then
    run_perf tcp "$BASELINE_TCP"
    run_perf ucx "$BASELINE_UCX"
else
    if [[ "$TRANSPORT" == "ucx" ]]; then
        run_perf ucx "$BASELINE_UCX"
    else
        run_perf tcp "$BASELINE_TCP"
    fi
fi
