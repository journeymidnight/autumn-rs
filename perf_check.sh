#!/usr/bin/env bash
# perf_check.sh — build release, start fresh 3-replica cluster, run perf-check
#
# Default: runs the 2×2×1×2 = 8-run matrix
#   transports     = {tcp, ucx}
#   partitions     = {1, 8}
#   pipeline-depth = {8}          (client-side only; d=8 is the throughput point)
#   value size     = {4K, 8M}
# → 4 cluster restarts (size is client-side only; inner-loop).
#
# Client concurrency: `--threads 16` by default (override with --threads N).
# Total in-flight = threads × pipeline-depth. Keep threads low (≤ ~32) and
# scale via pipeline-depth — this is thread-per-core-correct on the client
# side AND keeps each partition's single-threaded UCX worker on the PS
# in its supported region (see "UCX cliff" note below).
# At 16t × d=8 = 128 in-flight the 2×2×2×2 matrix reaches:
#   TCP p=8 × 16t × d=8 × 4 KB → 142 k write / 1.11 M read
#   UCX p=8 × 16t × d=8 × 4 KB → 129 k write / 764 k read
# 8 MB payload is where UCX rc_mlx5 zero-copy starts beating TCP loopback
# memcpy — the rndv-get-zcopy handshake gets amortized over a much larger
# DMA; at 4 KB it's pure overhead (see F100-UCX §12).
#
# UCX cliff (post fix(ucx): drop UcxEp close-on-Drop, 2026-04-29). Each
# PS partition runs a single-threaded UCX worker. The cliff is set by
# *EPs per partition's worker*, not by aggregate in-flight ops:
#   - perf-check read keeps a per-thread HashMap<ps_addr, RpcClient> →
#     each thread eventually opens one EP to every partition →
#     EPs / partition = client_threads.
#   - perf-check write pins each thread to one partition (tid % parts) →
#     EPs / partition = client_threads ÷ partitions.
# In-flight is symmetric (FuturesUnordered cap is per-thread =
# pipeline_depth) and = client_threads × pipeline_depth ÷ partitions for
# both phases. The reason read collapses before write at the same thread
# count is the EP-count axis (8× more EPs/partition for read at p=8).
# Empirical at p=8 d=16 4 KB:
#   --threads 16  → 16 EPs/p → write 104 k · read 970 k · p99 0.46 ms ✓
#   --threads 32  → 32 EPs/p → write  80 k · read 610 k · p99 1.16 ms (degrading)
#   --threads 64  → 64 EPs/p → write 14 k  · read 105 k · p99 18 ms ✗ cliff
#   --threads 256 → 256 EPs/p → write ~0   · read   0   · ✗ hard fail
# Rule of thumb: keep client_threads ÷ partitions ≲ 32 (read EPs per
# partition's worker). Need more total client concurrency? Add
# partitions, not threads — see README "UCX scaling and limits" for
# the full discussion. Numbers above `--threads 32` at
# `--pipeline-depth 16 --partitions 8` are outside the UCX supported
# region and should not be used as performance signal.
#
# Usage:
#   ./perf_check.sh                       # default 2×2×2×2 matrix on disk
#   ./perf_check.sh --shm                 # matrix on RAM tmpfs
#   ./perf_check.sh --tcp                 # tcp only (still all inner axes)
#   ./perf_check.sh --ucx                 # ucx only
#   ./perf_check.sh --partitions 8        # both transports, partitions=8 only
#   ./perf_check.sh --pipeline-depth 8    # pipeline-depth=8 only
#   ./perf_check.sh --size 8m             # 8 MB only (or e.g. --size 4k, --size 1048576)
#   ./perf_check.sh --threads 32          # override client thread count
#   ./perf_check.sh --tcp --partitions 1 --pipeline-depth 1 --size 4k  # one combo
#   ./perf_check.sh --update-baseline     # create / overwrite per-combo baselines
#
# --shm is useful for isolating the RPC / partition / stream layers from the
# underlying filesystem (extent storage lives in RAM, fsync is a no-op).
# Separate baseline files per (transport, partitions, storage) combination.

set -uo pipefail   # NOTE: no -e — we want the matrix to keep going past a failure

# macOS default is 256 open files — far too few for 256-thread benchmarks
ulimit -n 65536 2>/dev/null || true

# RDMA pins memory via ibv_reg_mr. Default RLIMIT_MEMLOCK (often 8 MB) is
# too small for the 8 MB payload matrix (16 threads × 8 MB = 128 MB
# concurrent pinned). Child processes (cluster.sh → manager/node/ps
# daemons) inherit this limit, so set it here to cover everything.
ulimit -l unlimited 2>/dev/null || true

# UCX workaround: this environment blocks BOTH the SysV and POSIX
# shared-memory transports on the > eager-threshold path:
#   sysv:  `mm_sysv.c:59  shmat(shmid=...) failed: Invalid argument`
#          (IPC namespace denies shmat)
#   posix: `mm_posix.c:233 open(file_name=/proc/<peer_pid>/fd/<N>) failed:
#           No such file or directory`
#          (peer-fd visibility through /proc restricted)
# Either one being chosen by UCX for an 8 MB rendezvous causes the send
# to wedge for tens of seconds. Excluding both lets UCX fall back to
# `cma` (zero-copy syscall, 17+ GB/s in ucx_perftest) for intra-host
# bulk + `tcp` for control. Respects caller-provided UCX_TLS.
: "${UCX_TLS:=^sysv,^posix}"
export UCX_TLS

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AC="$SCRIPT_DIR/target/release/autumn-client"

USE_SHM=0
UPDATE_BASELINE=""
SKIP_CLUSTER=0
TRANSPORT_LIST="tcp ucx"          # default: both transports
PARTITIONS_LIST="1 8"             # default: both partition counts
PIPELINE_DEPTH_LIST="8"           # depth is client-side only; d=8 is the representative throughput point
SIZES_LIST="4096 8388608"         # default: 4 KB (small-msg) + 8 MB (rndv-zcopy)
THREADS=16                        # default: 16 client OS threads (see header)

# Map a byte size to a short label used in baseline filenames:
# 4096 → "4k", 8388608 → "8m", other → "<N>b" / "<N>k" / "<N>m".
fmt_size_label() {
    local n="$1"
    if (( n >= 1048576 )) && (( n % 1048576 == 0 )); then
        echo "$(( n / 1048576 ))m"
    elif (( n >= 1024 )) && (( n % 1024 == 0 )); then
        echo "$(( n / 1024 ))k"
    else
        echo "${n}b"
    fi
}
# Parse a user-facing size arg: accepts "4096", "4k", "8m", etc.
parse_size() {
    local s="$1"
    case "$s" in
        *[mM])    echo $(( ${s%[mM]} * 1048576 )) ;;
        *[kK])    echo $(( ${s%[kK]} * 1024 )) ;;
        *[0-9])   echo "$s" ;;
        *) echo "__ERR__" ;;
    esac
}
while (( $# > 0 )); do
    case "$1" in
        --shm)              USE_SHM=1 ;;
        --update-baseline)  UPDATE_BASELINE="--update-baseline" ;;
        --skip-cluster)     SKIP_CLUSTER=1 ;;
        --ucx)              TRANSPORT_LIST="ucx" ;;
        --tcp)              TRANSPORT_LIST="tcp" ;;
        --transport-both)   TRANSPORT_LIST="tcp ucx" ;;   # back-compat no-op (already default)
        --partitions)
            shift
            v="${1:-}"
            [[ "$v" =~ ^[0-9]+$ ]] && (( v >= 1 )) \
                || { echo "--partitions must be a positive integer" >&2; exit 1; }
            PARTITIONS_LIST="$v"
            ;;
        --pipeline-depth)
            shift
            v="${1:-}"
            [[ "$v" =~ ^[0-9]+$ ]] && (( v >= 1 && v <= 256 )) \
                || { echo "--pipeline-depth must be an integer in [1, 256]" >&2; exit 1; }
            PIPELINE_DEPTH_LIST="$v"
            ;;
        --size)
            shift
            bytes="$(parse_size "${1:-}")"
            [[ "$bytes" =~ ^[0-9]+$ ]] && (( bytes >= 1 )) \
                || { echo "--size must be bytes, or Nk / Nm (e.g. 4096, 4k, 8m)" >&2; exit 1; }
            SIZES_LIST="$bytes"
            ;;
        --threads)
            shift
            v="${1:-}"
            [[ "$v" =~ ^[0-9]+$ ]] && (( v >= 1 )) \
                || { echo "--threads must be a positive integer" >&2; exit 1; }
            THREADS="$v"
            ;;
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            exit 1
            ;;
    esac
    shift
done

if (( USE_SHM )); then
    export AUTUMN_DATA_ROOT="/dev/shm/autumn-rs"
    STORAGE_LABEL="RAM tmpfs (/dev/shm)"
    STORAGE_SUFFIX="_shm"
else
    export AUTUMN_DATA_ROOT="/tmp/autumn-rs"
    STORAGE_LABEL="disk ($AUTUMN_DATA_ROOT)"
    STORAGE_SUFFIX=""
fi

# F100-UCX: build with the ucx feature when any UCX run is requested.
NEED_UCX_FEATURE=0
for t in $TRANSPORT_LIST; do
    [[ "$t" == "ucx" ]] && NEED_UCX_FEATURE=1
done
echo "[perf-check] building release binaries$([ $NEED_UCX_FEATURE -eq 1 ] && echo " (with --features autumn-server/ucx)")..."
cd "$SCRIPT_DIR"
if (( NEED_UCX_FEATURE )); then
    cargo build --workspace --release --exclude autumn-fuse \
        --features autumn-server/ucx 2>&1 \
        | grep -E "^(Compiling|Finished|error)" || true
else
    cargo build --workspace --release --exclude autumn-fuse 2>&1 \
        | grep -E "^(Compiling|Finished|error)" || true
fi

# Wait until extent-node ports (9101..9103) have no lingering sockets in
# either direction (server-side LISTEN/TIME_WAIT *or* client-side TIME_WAIT
# with peer=:910x). UCX's ucp_listener_create empirically refuses to bind
# while client-side TIME_WAITs targeting the same port still exist, so we
# must wait for both columns to clear. Bounded so a stuck socket can't
# stall the matrix forever.
await_ports_clear() {
    # 180s cap — TCP runs can pile up many client-side TIME_WAITs that need
    # to age out before UCX's ucp_listener_create (no SO_REUSEADDR) succeeds.
    local deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
        if ! ss -tan 2>/dev/null \
                | awk 'NR>1 {print $4; print $5}' \
                | grep -qE ':(9101|9102|9103)$'; then
            return 0
        fi
        sleep 5
    done
    echo "[perf-check] WARN: 9101..9103 still have lingering sockets after 180s"
}

# Inner runner: starts cluster under given AUTUMN_TRANSPORT + presplit, runs
# perf-check at the requested pipeline-depth and value size. The cluster is
# restarted per (mode, parts) but reused across pipeline-depth and size
# values for that pair — both are purely client-side knobs. Saves many
# cluster restarts (~25 s each) when the full matrix runs.
run_perf() {
    local mode="$1"
    local parts="$2"
    local depth="$3"
    local size="$4"
    local size_label
    size_label="$(fmt_size_label "$size")"
    local baseline="$SCRIPT_DIR/perf_baseline_${mode}_p${parts}_d${depth}_s${size_label}${STORAGE_SUFFIX}.json"

    echo
    echo "============================================================"
    echo "[perf-check] mode=$mode partitions=$parts pipeline-depth=$depth size=$size_label ($size B) storage=$STORAGE_LABEL"
    echo "[perf-check] baseline=$(basename "$baseline")"
    echo "============================================================"
    "$AC" --manager "${AUTUMN_BIND_HOST:-127.0.0.1}:9001" --transport "$mode" \
        perf-check \
        --nosync \
        --threads "$THREADS" \
        --duration 10 \
        --size "$size" \
        --partitions "$parts" \
        --pipeline-depth "$depth" \
        --baseline "$baseline" \
        $UPDATE_BASELINE \
        || echo "[perf-check] perf-check exited non-zero (mode=$mode parts=$parts depth=$depth size=$size_label)"
}

start_cluster_for() {
    local mode="$1"
    local parts="$2"
    if (( parts > 1 )); then
        export AUTUMN_BOOTSTRAP_PRESPLIT="${parts}:hexstring"
    else
        unset AUTUMN_BOOTSTRAP_PRESPLIT
    fi
    if (( SKIP_CLUSTER == 0 )); then
        bash "$SCRIPT_DIR/cluster.sh" clean
        await_ports_clear
        AUTUMN_TRANSPORT="$mode" bash "$SCRIPT_DIR/cluster.sh" start 3 \
            || { echo "[perf-check] FAILED to start cluster (mode=$mode parts=$parts)"; return 1; }
    else
        echo "[perf-check] --skip-cluster: assuming cluster is already running in $mode mode"
    fi
}

OVERALL_RC=0
for mode in $TRANSPORT_LIST; do
    for parts in $PARTITIONS_LIST; do
        if ! start_cluster_for "$mode" "$parts"; then
            OVERALL_RC=1
            continue
        fi
        for depth in $PIPELINE_DEPTH_LIST; do
            for size in $SIZES_LIST; do
                run_perf "$mode" "$parts" "$depth" "$size" || OVERALL_RC=1
            done
        done
    done
done

# Final cluster cleanup so the matrix leaves no dangling processes.
bash "$SCRIPT_DIR/cluster.sh" clean >/dev/null 2>&1 || true

exit $OVERALL_RC
