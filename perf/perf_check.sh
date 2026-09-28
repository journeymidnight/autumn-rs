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
# UCX legs need AUTUMN_BIND_HOST set to a RoCE NIC IP (see the UCX block
# below); on 127.0.0.1 the script refuses them, so run `--tcp` there. The
# committed baselines come from two invocations: `--tcp` on 127.0.0.1 and
# `--ucx` with the RoCE bind — docs/ops.md, next to the perf matrix.
#
# Client concurrency: `--threads 16` by default (override with --threads N).
# Total in-flight = threads × pipeline-depth. Keep threads low (≤ ~32) and
# scale via pipeline-depth — this is thread-per-core-correct on the client
# side AND keeps each partition's single-threaded UCX worker on the PS
# in its supported region (see "UCX cliff" note below).
# At 16t × d=8 = 128 in-flight, 3 disks, 8 partitions (baselines of
# 2026-09-28): TCP 4 KB 55 k write / 583 k read ops/s, 8 MB 2.4 / 7.5 GB/s;
# UCX over RoCE 4 KB 18 k / 964 k, 8 MB 2.2 / 5.0 GB/s. UCX 4 KB writes pay
# an rc round trip per small append.
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
#   ./perf_check.sh                       # default matrix on disk (UCX legs need AUTUMN_BIND_HOST)
#   ./perf_check.sh --shm                 # matrix on RAM tmpfs
#   ./perf_check.sh --tcp                 # tcp only (still all inner axes)
#   ./perf_check.sh --ucx                 # ucx only
#   ./perf_check.sh --partitions 8        # both transports, partitions=8 only
#   ./perf_check.sh --pipeline-depth 8    # pipeline-depth=8 only
#   ./perf_check.sh --size 8m             # 8 MB only (or e.g. --size 4k, --size 1048576)
#   ./perf_check.sh --threads 32          # override client thread count
#   ./perf_check.sh --tcp --partitions 1 --pipeline-depth 1 --size 4k  # one combo
#   ./perf_check.sh --update-baseline     # create / overwrite per-combo baselines
#   ./perf_check.sh --3disk               # spread 3 replicas across /data03,
#                                         #   /data05, /data08 (3 independent
#                                         #   NVMes) — fsync parallelises across
#                                         #   hardware instead of all 3 replicas
#                                         #   queueing on a single disk
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# perf/ layout: baselines live beside this script (SCRIPT_DIR); the repo
# root (cluster.sh, target/) is one level up.
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
AC="$ROOT_DIR/target/release/autumn-client"

# AC_PREFIX is a hook for ad-hoc client wrapping (numactl, taskset, perf
# stat, strace ...). Empty by default — the benchmark should reflect the
# production network path where client runs on a separate host and the
# NIC's NUMA locality (not the client thread's) is what matters. Pinning
# the local bench client via `numactl --cpunodebind=0 --membind=0` does
# improve loopback read throughput ~20% by removing cross-NUMA sk_buff
# memcpy, but that win does not exist over real NICs and would inflate
# the baseline above any production system can reach. Keep the hook so
# experiments that explicitly want to isolate the bench harness can
# still set `AC_PREFIX="numactl ..." ./perf_check.sh ...`.
: "${AC_PREFIX:=}"
if [[ -n "$AC_PREFIX" ]]; then
    echo "[perf-check] AC_PREFIX (override): $AC_PREFIX"
fi

USE_SHM=0
USE_3DISK=0
UPDATE_BASELINE=""
SKIP_CLUSTER=0
TRANSPORT_LIST="tcp ucx"          # default: both transports
PARTITIONS_LIST="1 8"             # default: both partition counts
PIPELINE_DEPTH_LIST="8"           # depth is client-side only; d=8 is the representative throughput point
SIZES_LIST="4096 8388608"         # default: 4 KB (small-msg) + 8 MB (rndv-zcopy)
THREADS=16                        # default: 16 client OS threads (see header)
DURATION=10                       # default: 10 s (baseline window). Use --duration 120
                                  # to exercise compact/gc paths (D-r7-recal).

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
        --3disk)            USE_3DISK=1 ;;
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
        --duration)
            shift
            v="${1:-}"
            [[ "$v" =~ ^[0-9]+$ ]] || { echo "--duration needs a positive integer" >&2; exit 1; }
            DURATION="$v"
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
    # tmpfs refuses O_DIRECT before Linux 6.6 (and buffers it after), so
    # the ENs run without direct I/O here.
    export AUTUMN_EXTENT_DIRECT_IO=0
    STORAGE_LABEL="RAM tmpfs (/dev/shm)"
    STORAGE_SUFFIX="_shm"
else
    # Honor a pre-set AUTUMN_DATA_ROOT so callers can target a specific
    # disk (e.g. AUTUMN_DATA_ROOT=/data03/autumn-rs to escape overlayfs
    # in containerized environments where /tmp may not be a real ext4
    # mount). Default stays /tmp/autumn-rs to match historical baselines.
    export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/tmp/autumn-rs}"
    STORAGE_LABEL="disk ($AUTUMN_DATA_ROOT)"
    STORAGE_SUFFIX=""
fi

# UCX: ONE rule, single host or multi-host (the same one cluster.sh's
# apply_ucx_env_defaults applies) — bind a RoCE NIC IP and run
# UCX_TLS=rc_mlx5,ud_mlx5,tcp,self with a pinned UCX_NET_DEVICES, on the
# cluster AND this bench client. There is no loopback UCX configuration:
# 127.0.0.1 has no RoCE GID, and the posix shm path it falls back to stalls
# concurrent >=64K transfers (8 MiB writes at ~130 MB/s, p50 4 s). POSITIVE
# lists only (a leading ^ negates the whole list, a later ^x is ignored).
# On this box: AUTUMN_BIND_HOST='[fdbd:dc62:3:300::14]' (eth1 = mlx5_2, the
# storage NIC; mlx5_1 is the GPU's). Unlike cluster.sh (fixed mlx5_1:1
# default), an unset UCX_NET_DEVICES is derived here from the bind IP and
# exported, so the cluster inherits it. TCP legs in the same invocation use
# the same bind address.
if [[ " $TRANSPORT_LIST " == *" ucx "* ]]; then
    ucx_host="${AUTUMN_BIND_HOST:-127.0.0.1}"
    ucx_host="${ucx_host//[\[\]]/}"
    if [[ "$ucx_host" == "127.0.0.1" || "$ucx_host" == "::1" || "$ucx_host" == "localhost" ]]; then
        echo "[perf-check] UCX needs a RoCE NIC IP, not $ucx_host: set AUTUMN_BIND_HOST" \
             "(e.g. '[fdbd:dc62:3:300::14]' with UCX_NET_DEVICES=mlx5_2:1 on this box)," \
             "or run --tcp only" >&2
        exit 2
    fi
    export UCX_TLS="${UCX_TLS:-rc_mlx5,ud_mlx5,tcp,self}"
    # Default the device to the RDMA port whose netdev carries the bind IP,
    # so the bench measures the NIC it binds.
    if [[ -z "${UCX_NET_DEVICES:-}" ]]; then
        for ib in /sys/class/infiniband/*; do
            for nd in "$ib"/device/net/*; do
                [[ -e "$nd" ]] || continue
                if ip -o addr show dev "$(basename "$nd")" 2>/dev/null | grep -qF " $ucx_host/"; then
                    UCX_NET_DEVICES="$(basename "$ib"):1"
                fi
            done
        done
        [[ -n "${UCX_NET_DEVICES:-}" ]] || {
            echo "[perf-check] no RDMA device carries $ucx_host; set UCX_NET_DEVICES" >&2
            exit 2
        }
    fi
    export UCX_NET_DEVICES
    echo "[perf-check] ucx env: UCX_TLS=$UCX_TLS UCX_NET_DEVICES=$UCX_NET_DEVICES bind=$AUTUMN_BIND_HOST"
fi

# build with the ucx feature when any UCX run is requested.
NEED_UCX_FEATURE=0
for t in $TRANSPORT_LIST; do
    [[ "$t" == "ucx" ]] && NEED_UCX_FEATURE=1
done
echo "[perf-check] building release binaries$([ $NEED_UCX_FEATURE -eq 1 ] && echo " (with --features autumn-server/ucx)")..."
cd "$ROOT_DIR"
if (( NEED_UCX_FEATURE )); then
    cargo build --workspace --release --exclude autumn-fuse \
        --features autumn-server/ucx 2>&1 \
        | grep -E "^(Compiling|Finished|error)" || true
else
    cargo build --workspace --release --exclude autumn-fuse 2>&1 \
        | grep -E "^(Compiling|Finished|error)" || true
fi

# Wait until the cluster's fixed ports — the manager (9001), the extent-node
# grid (AUTUMN_EXTENT_BASE_PORT + node + shard*stride, 9101..9173 with the
# defaults and 8 shards) and the PS listeners (AUTUMN_PS_BASE_PORT onwards)
# — have no lingering sockets in either direction (server-side LISTEN/TIME_WAIT *or* client-side TIME_WAIT with
# the port as peer). UCX's ucp_listener_create empirically refuses to bind
# while client-side TIME_WAITs targeting the same port still exist, and the
# TIME_WAITs of UCX-accepted sockets (no SO_REUSEADDR) make even a TCP EN's
# SO_REUSEADDR bind fail with EADDRINUSE — watching only the shard-0 ports
# let a TCP cluster start after a UCX one while shard 7's port was still
# held, and that EN fail-stopped; a UCX manager started over 9001's
# TIME_WAITs outlived cluster.sh's start timeout the same way. Bounded so a
# stuck socket can't stall the matrix forever.
await_ports_clear() {
    # 180s cap — TCP runs can pile up many client-side TIME_WAITs that need
    # to age out before UCX's ucp_listener_create (no SO_REUSEADDR) succeeds.
    local en_base="${AUTUMN_EXTENT_BASE_PORT:-9100}"
    local shards="${AUTUMN_EXTENT_SHARDS:-8}" stride="${AUTUMN_EXTENT_SHARD_STRIDE:-10}"
    local en_lo=$(( en_base + 1 )) en_hi=$(( en_base + 3 + (shards - 1) * stride ))
    local ps_lo="${AUTUMN_PS_BASE_PORT:-9301}"
    local ps_hi=$(( ps_lo + 64 ))
    local deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
        if ! ss -tan 2>/dev/null | awk -v el="$en_lo" -v eh="$en_hi" -v pl="$ps_lo" -v ph="$ps_hi" '
                NR > 1 { for (f = 4; f <= 5; f++) { n = split($f, a, ":"); p = a[n] + 0
                         if (p == 9001 || (p >= el && p <= eh) || (p >= pl && p <= ph)) found = 1 } }
                END { exit !found }'; then
            return 0
        fi
        sleep 5
    done
    echo "[perf-check] WARN: ports 9001, $en_lo-$en_hi, $ps_lo-$ps_hi still have lingering sockets after 180s"
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
    ${AC_PREFIX:-} "$AC" --manager "${AUTUMN_BIND_HOST:-127.0.0.1}:9001" --transport "$mode" \
        perf-check \
        --threads "$THREADS" \
        --duration "$DURATION" \
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
    # AUTUMN_BOOTSTRAP_PRESPLIT now means "presplit the BENCH namespace into N
    # partitions" — cluster.sh applies it AFTER bootstrap via
    # `presplit --namespace bench --tenant perf --count N`. The old meaning
    # (`bootstrap --presplit N:hexstring`) cut the RAW keyspace at points no
    # `bench/perf/…` key ever reaches, so `bench_user_starts` filtered them all
    # out and every N measured ONE partition (BUG-BENCH-NS-UNREGISTERED).
    if (( parts > 1 )); then
        export AUTUMN_BOOTSTRAP_PRESPLIT="${parts}:hexstring"
    else
        unset AUTUMN_BOOTSTRAP_PRESPLIT
    fi
    if (( SKIP_CLUSTER == 0 )); then
        bash "$ROOT_DIR/cluster.sh" clean
        await_ports_clear
        # fix: default to AUTUMN_EXTENT_SHARDS=4 so each EN process has 4
        # cores serving extent traffic (single-shard mode put all 3-replica
        # writes through 3 cores total — EN became the wall at >100k ops/s).
        # Caller can override via env. cluster.sh's auto layout slices the
        # launcher's allowed cores (EN_i from index (i-1)*SHARDS, PS from
        # REPLICAS*SHARDS): 4 shards × 3 replicas = 12 EN cores, then the PS
        # takes 2N cores after — fits any reasonably-sized host's cpuset.
        # --3disk: spread the 3 replicas across /data03, /data05, /data08
        # (three independent NVMes) instead of one disk → fsync work
        # parallelises across hardware. Without this flag, all 3 replicas
        # land on whatever single disk AUTUMN_DATA_ROOT points at.
        local cluster_3disk=()
        if (( USE_3DISK )); then
            cluster_3disk=(--3disk)
        fi
        # cluster.sh default PS cpuset budget = PS_PARTS_HINT (8).
        # If this bench asks for more partitions, the PS would silently
        # skip openings past the budget. Bump the hint to match the
        # bench's partition count so all partitions actually open.
        local ps_parts_hint_for_bench="${AUTUMN_PS_PARTS_HINT:-}"
        if [[ -z "$ps_parts_hint_for_bench" ]] && (( parts > 8 )); then
            ps_parts_hint_for_bench="$parts"
        fi
        # followup (2026-05-13): bumped 4 → 8 after 120 s test
        # showed SHARDS=8 gives read +9 % / read-p99 −11 % at no
        # write cost (write is fsync-bound, the row_stream tail extent
        # serialises on its single shard regardless of total shard count).
        AUTUMN_EXTENT_SHARDS="${AUTUMN_EXTENT_SHARDS:-8}" \
        AUTUMN_PS_PARTS_HINT="${ps_parts_hint_for_bench:-8}" \
        AUTUMN_TRANSPORT="$mode" bash "$ROOT_DIR/cluster.sh" start 3 \
            "${cluster_3disk[@]}" \
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
bash "$ROOT_DIR/cluster.sh" clean >/dev/null 2>&1 || true

exit $OVERALL_RC
