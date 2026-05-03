#!/usr/bin/env bash
# cluster.sh — dev cluster management for autumn-rs
#
# Usage:
#   ./cluster.sh start [N]      # start N extent-node cluster (default 1, e.g. 3, 4, 5)
#   ./cluster.sh stop           # kill all cluster processes (data preserved)
#   ./cluster.sh restart [N]    # stop + start (data preserved)
#   ./cluster.sh clean          # stop + wipe data dirs
#   ./cluster.sh reset [N]      # clean + start (fresh cluster, all data wiped)
#   ./cluster.sh status         # show running processes
#   ./cluster.sh logs           # tail log files (Ctrl-C to exit)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$SCRIPT_DIR/target/release"
LOG_DIR="/tmp/autumn-rs-logs"
DATA_ROOT="${AUTUMN_DATA_ROOT:-/tmp/autumn-rs}"
# AUTUMN_BIND_HOST overrides the bind/advertise host. Default is IPv4 loopback.
# For UCX/RoCE perf, set to a RoCE-attached IP, e.g.:
#   AUTUMN_BIND_HOST="[fdbd:dc62:3:302::14]" AUTUMN_TRANSPORT=ucx bash cluster.sh reset 4
BIND_HOST="${AUTUMN_BIND_HOST:-127.0.0.1}"
MANAGER_ADDR="${BIND_HOST}:9001"
# AUTUMN_TRANSPORT selects tcp (default) or ucx. Consumed here and passed to
# binaries as --transport; binaries never read AUTUMN_TRANSPORT themselves.
TRANSPORT="${AUTUMN_TRANSPORT:-tcp}"
case "$TRANSPORT" in
    tcp|ucx) ;;
    *) echo "AUTUMN_TRANSPORT must be 'tcp' or 'ucx', got '$TRANSPORT'" >&2; exit 2 ;;
esac
unset AUTUMN_TRANSPORT  # don't leak to child processes
ETCD_DIR="$DATA_ROOT/etcd"

MANAGER="$BIN/autumn-manager-server"
NODE="$BIN/autumn-extent-node"
PS="$BIN/autumn-ps"
AC="$BIN/autumn-client"

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

die() { echo "ERROR: $*" >&2; exit 1; }

need_bin() {
    local b="$1"
    [[ -x "$b" ]] || die "Binary not found: $b — run: cargo build --release --workspace"
}

pid_file() { echo "$DATA_ROOT/pids/$1.pid"; }

save_pid() {
    local name="$1" pid="$2"
    mkdir -p "$DATA_ROOT/pids"
    echo "$pid" > "$(pid_file "$name")"
}

start_proc() {
    local name="$1"; shift
    local log="$LOG_DIR/${name}.log"
    mkdir -p "$LOG_DIR"
    echo "[cluster] starting $name → $log"
    "$@" >"$log" 2>&1 &
    save_pid "$name" $!
}

kill_proc() {
    local name="$1"
    local pf; pf="$(pid_file "$name")"
    if [[ -f "$pf" ]]; then
        local pid; pid="$(cat "$pf")"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            # F120-C: wait up to 60s for process to exit gracefully — autumn-ps
            # now drains active+imm to row_stream on SIGTERM (PartitionServer::
            # shutdown), which can take seconds per partition with EC bulk
            # uploads. SIGKILL if still stuck after the deadline (replay on
            # restart covers any unflushed data).
            # NOTE: use pre-increment `((++i))` — `((i++))` returns the OLD value,
            # which is 0 on the first iteration and trips `set -e`.
            local i=0
            while kill -0 "$pid" 2>/dev/null && (( i < 600 )); do
                sleep 0.1
                (( ++i ))
            done
            if kill -0 "$pid" 2>/dev/null; then
                echo "[cluster] $name (pid $pid) did not exit within 60s; SIGKILL"
                kill -9 "$pid" 2>/dev/null || true
                sleep 0.2
            fi
            echo "[cluster] stopped $name (pid $pid)"
        fi
        rm -f "$pf"
    fi
}

wait_port() {
    local port="$1" name="$2" retries="${3:-20}"
    # 4th arg overrides the host (default = BIND_HOST). etcd binds on
    # 127.0.0.1 only — pass that explicitly when waiting for it.
    local host="${4:-${BIND_HOST//[\[\]]/}}"
    echo -n "[cluster] waiting for $name on :$port (host=$host)..."
    if [[ "$TRANSPORT" == "ucx" ]]; then
        local log=""
        local pattern=""
        case "$name" in
            manager) log="$LOG_DIR/manager.log"; pattern="manager listening" ;;
            node*) log="$LOG_DIR/${name}.log"; pattern="autumn-extent-node listening|extent-node shard listening" ;;
            partition*) log="$LOG_DIR/ps.log"; pattern="partition listener bound" ;;
            ps) log="$LOG_DIR/ps.log"; pattern="partition server serving" ;;
        esac
        if [[ -n "$log" ]]; then
            for _ in $(seq 1 "$retries"); do
                if [[ -f "$log" ]] && grep -Eq "$pattern" "$log"; then echo " ok"; return 0; fi
                sleep 0.5
            done
            echo " TIMEOUT"
            die "$name did not start in time (port $port)"
        fi
    fi
    for _ in $(seq 1 $retries); do
        if nc -z "$host" "$port" 2>/dev/null; then echo " ok"; return 0; fi
        sleep 0.5
    done
    echo " TIMEOUT"
    die "$name did not start in time (port $port)"
}

# ---------------------------------------------------------------------------
# disk helpers
# ---------------------------------------------------------------------------

# Return the --data argument for extent node $1 (1-indexed) given $CLUSTER_MODE.
disk_args_for_node() {
    local i="$1"
    case "${CLUSTER_MODE:-default}" in
        3disk)
            case "$i" in
                1) echo "/data03/autumn-rs/d1" ;;
                2) echo "/data05/autumn-rs/d2" ;;
                3) echo "/data08/autumn-rs/d3" ;;
                *) die "--3disk supports exactly 3 nodes; got i=$i" ;;
            esac
            ;;
        multidisk-1node)
            [[ "$i" == "1" ]] || die "--multidisk-1node supports exactly 1 node"
            echo "/data03/autumn-rs/d1,/data05/autumn-rs/d2,/data08/autumn-rs/d3"
            ;;
        *)
            echo "$DATA_ROOT/d$i"
            ;;
    esac
}

# ---------------------------------------------------------------------------
# F102: per-process helpers + saved config
# ---------------------------------------------------------------------------
#
# Used by:
#   - do_start (full bring-up): writes the config snapshot once everything
#     is staged, then re-uses launch_extent_node / register_extent_node /
#     launch_ps so the per-process subcommands take exactly the same code
#     path as the bulk start.
#   - do_start_node / do_stop_node / do_start_ps / do_stop_ps: load the
#     snapshot so a recovery test can `stop-node 2 && start-node 2`
#     without re-typing flags or remembering env vars.

CONFIG_FILE="$DATA_ROOT/cluster_config"

# Populate $SHARDS / $SHARD_STRIDE from the env. Both helpers (do_start
# and the post-load subcommands) call this BEFORE launching any node.
compute_shard_config() {
    SHARDS="${AUTUMN_EXTENT_SHARDS:-1}"
    [[ "$SHARDS" =~ ^[0-9]+$ ]] && (( SHARDS >= 1 )) || SHARDS=1
    SHARD_STRIDE="${AUTUMN_EXTENT_SHARD_STRIDE:-10}"
    [[ "$SHARD_STRIDE" =~ ^[0-9]+$ ]] && (( SHARD_STRIDE >= 1 )) || SHARD_STRIDE=10
}

# Launch one extent-node ($1 = 1-indexed). Picks --shards/--data based on
# the current CLUSTER_MODE + AUTUMN_EXTENT_SHARDS env (compute_shard_config
# must have run first). Creates data dirs if missing. Waits for the primary
# port before returning.
launch_extent_node() {
    local i="$1"
    local port=$(( 9100 + i ))
    local disk_arg
    disk_arg=$(disk_args_for_node "$i")
    # F122-fix: each node owns SHARDS cores starting at (i-1)*SHARDS.
    # PS gets cores starting after all extent-node ranges (see launch_ps).
    local cpu_start=$(( (i - 1) * SHARDS ))
    # shellcheck disable=SC2046  # intentional word splitting on commas
    mkdir -p $(echo "$disk_arg" | tr ',' ' ')
    if [[ "$disk_arg" == *,* ]]; then
        if (( SHARDS > 1 )); then
            start_proc "node$i" \
                "$NODE" --port "$port" --data "$disk_arg" --manager "$MANAGER_ADDR" \
                --listen "$BIND_HOST" --transport "$TRANSPORT" \
                --shards "$SHARDS" --shard-stride "$SHARD_STRIDE" \
                --cpu-start "$cpu_start"
        else
            start_proc "node$i" \
                "$NODE" --port "$port" --data "$disk_arg" --manager "$MANAGER_ADDR" \
                --listen "$BIND_HOST" --transport "$TRANSPORT" \
                --cpu-start "$cpu_start"
        fi
    else
        if (( SHARDS > 1 )); then
            start_proc "node$i" \
                "$NODE" --port "$port" --disk-id "$i" --data "$disk_arg" --manager "$MANAGER_ADDR" \
                --listen "$BIND_HOST" --transport "$TRANSPORT" \
                --shards "$SHARDS" --shard-stride "$SHARD_STRIDE" \
                --cpu-start "$cpu_start"
        else
            start_proc "node$i" \
                "$NODE" --port "$port" --disk-id "$i" --data "$disk_arg" --manager "$MANAGER_ADDR" \
                --listen "$BIND_HOST" --transport "$TRANSPORT" \
                --cpu-start "$cpu_start"
        fi
    fi
    wait_port "$port" "node$i"
}

# Register one extent-node ($1 = 1-indexed) with the manager. No-op for
# multidisk-1node — the `format` step already registered.
register_extent_node() {
    local i="$1"
    local port=$(( 9100 + i ))
    if (( SHARDS > 1 )); then
        local shard_ports_csv=""
        for (( s=0; s<SHARDS; s++ )); do
            local sp=$(( port + s * SHARD_STRIDE ))
            if [[ -z "$shard_ports_csv" ]]; then
                shard_ports_csv="$sp"
            else
                shard_ports_csv="${shard_ports_csv},${sp}"
            fi
        done
        "$AC" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" register-node \
            --addr "${BIND_HOST}:$port" --disk "disk-$i" \
            --shard-ports "$shard_ports_csv"
    else
        "$AC" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" register-node --addr "${BIND_HOST}:$port" --disk "disk-$i"
    fi
}

# Launch the partition server. F099-K: the per-partition listeners (9201,
# 9202, ...) come up only after partitions open, so we don't wait_port
# here — bootstrap or region-sync handles readiness.
launch_ps() {
    # F122-fix: PS partitions pin starting AFTER all extent-node core ranges
    # (each EN owns SHARDS cores, REPLICAS nodes total → PS starts at
    # REPLICAS*SHARDS). Disjoint ranges prevent ord=0 collision on core 0.
    local ps_cpu_start=$(( REPLICAS * SHARDS ))
    start_proc ps \
        "$PS" \
        --psid 1 --port 9201 \
        --manager "$MANAGER_ADDR" \
        --listen "$BIND_HOST" \
        --advertise "${BIND_HOST}:9201" \
        --transport "$TRANSPORT" \
        --cpu-start "$ps_cpu_start"
    echo "[cluster] PS launched (F099-K: per-partition listeners bind on partition open; cpu-start=$ps_cpu_start)"
}

# Snapshot the launch parameters so `start-node` / `start-ps` (run later)
# can reproduce the exact env. Captures REPLICAS + CLUSTER_MODE explicitly,
# then dumps every AUTUMN_* env var (quoted via %q so values with spaces
# or special chars survive a `source`). Called once at the end of do_start
# after all envs that matter have been read.
save_cluster_config() {
    mkdir -p "$DATA_ROOT"
    {
        printf 'REPLICAS=%s\n' "$REPLICAS"
        printf 'CLUSTER_MODE=%s\n' "$MODE"
        env | grep -E '^AUTUMN_' | sort | while IFS='=' read -r k v; do
            printf 'export %s=%q\n' "$k" "$v"
        done
    } > "$CONFIG_FILE"
}

# Replay the snapshot. Errors with a clear hint if the cluster was never
# started in this $DATA_ROOT.
load_cluster_config() {
    [[ -f "$CONFIG_FILE" ]] || die "no cluster config at $CONFIG_FILE — run '$0 start [N]' first"
    # shellcheck disable=SC1090
    source "$CONFIG_FILE"
    MODE="${CLUSTER_MODE:-default}"
    export CLUSTER_MODE="$MODE"
    BIND_HOST="${AUTUMN_BIND_HOST:-127.0.0.1}"
    MANAGER_ADDR="${BIND_HOST}:9001"
    compute_shard_config
}

# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------

do_start() {
    local replicas="${1:-1}"
    [[ "$replicas" =~ ^[0-9]+$ ]] && (( replicas >= 1 )) || die "replicas must be a positive integer"

    # Stop any leftover processes from a previous run to avoid port conflicts
    do_stop

    need_bin "$MANAGER"
    need_bin "$NODE"
    need_bin "$PS"
    need_bin "$AC"

    # Create data dirs for etcd and ps; node dirs are created per-node below.
    mkdir -p "$DATA_ROOT/etcd" "$DATA_ROOT/ps"

    # etcd
    start_proc etcd \
        etcd \
        --data-dir "$ETCD_DIR" \
        --listen-client-urls http://127.0.0.1:2379 \
        --advertise-client-urls http://127.0.0.1:2379
    wait_port 2379 etcd 20 127.0.0.1

    # Clean etcd data on fresh start (no bootstrap marker = fresh cluster)
    local bootstrap_marker="$DATA_ROOT/bootstrapped"
    if [[ ! -f "$bootstrap_marker" ]]; then
        echo "[cluster] cleaning etcd (fresh start)"
        etcdctl del "" --prefix >/dev/null 2>&1 || true
    fi

    # manager
    start_proc manager \
        "$MANAGER" --port 9001 --etcd 127.0.0.1:2379 --listen "$BIND_HOST" \
        --transport "$TRANSPORT"
    wait_port 9001 manager

    # Extent node(s): node1=9101, node2=9102, ...
    # Pre-create all data directories so both single-disk and multi-disk paths can rely on them.
    for (( i=1; i<=replicas; i++ )); do
        local disk_arg
        disk_arg=$(disk_args_for_node "$i")
        mkdir -p $(echo "$disk_arg" | tr ',' ' ')
    done

    # In --multidisk-1node mode, `autumn-client format` must run BEFORE the
    # extent-node starts. Format registers the node with the manager and
    # writes the `disk_id` file in every data directory — ExtentNodeConfig::
    # new_multi requires those files on open. The format call also replaces
    # register-node for this mode.
    if [[ "${CLUSTER_MODE:-default}" == "multidisk-1node" ]]; then
        local disk_arg
        disk_arg=$(disk_args_for_node 1)
        # shellcheck disable=SC2086  # intentional word splitting for positional args
        "$AC" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" format \
            --listen ":9101" \
            --advertise "${BIND_HOST}:9101" \
            $(echo "$disk_arg" | tr ',' ' ')
    fi

    # F099-M: when AUTUMN_EXTENT_SHARDS is set, launch each extent-node
    # with K shards (see compute_shard_config + launch_extent_node above).
    compute_shard_config
    if (( SHARDS > 1 )); then
        echo "[cluster] F099-M: extent-node shards=$SHARDS stride=$SHARD_STRIDE"
    fi

    # Launch extent-node processes.
    for (( i=1; i<=replicas; i++ )); do
        launch_extent_node "$i"
    done

    # register extent node(s) — skip for multidisk-1node (format already registered).
    if [[ "${CLUSTER_MODE:-default}" != "multidisk-1node" ]]; then
        for (( i=1; i<=replicas; i++ )); do
            register_extent_node "$i"
        done
        echo "[cluster] extent node(s) registered"
    else
        echo "[cluster] extent node registered via format (multidisk-1node)"
    fi

    if [[ -n "${AUTUMN_GROUP_COMMIT_CAP:-}" ]]; then
        echo "[cluster] AUTUMN_GROUP_COMMIT_CAP=$AUTUMN_GROUP_COMMIT_CAP (forwarding to PS)"
    fi

    launch_ps

    # bootstrap (create streams + partitions) — only on a fresh data dir
    if [[ -f "$bootstrap_marker" ]]; then
        echo "[cluster] skipping bootstrap (already done — use 'restart' for a fresh cluster)"
        # Give PS a moment to sync regions and re-bind listeners on restart.
        wait_port 9201 ps 60
    else
        # Auto-select EC shape (FOPS-02).
        #
        # `replicas` here is the number of EXTENT NODES in the cluster,
        # NOT the replication factor. Streams are described by the
        # triple `(replicates, ec_data, ec_parity)` (see MgrStreamInfo):
        #   - replicates: open-extent replica count, fixed at 3 (or N
        #     when N<3 for tiny dev clusters). All open extents land
        #     on this many nodes regardless of EC config.
        #   - ec_data (K): post-seal data-shard count. Independent of
        #     replicates — a 3-replica stream can be EC-encoded into
        #     4+1 or 7+1 on seal.
        #   - ec_parity (M): post-seal parity-shard count.
        #
        # EC default (applied to both log_stream and row_stream):
        #   N<=3 → no EC (no parity headroom)
        #   N=4 → 3+1   (K=3, M=1)
        #   N>=5 → 4+1  (K=4, M=1; K capped at 4 to bound RS decode cost)
        #
        # log_stream EC is supported via ec_subrange_read's generalised
        # N-shard parallel sub-range read (handles VP reads of any width
        # safely; replaced the buggy two-adjacent-shard-only fast path).
        # See crates/stream/CLAUDE.md note 6 + the bug-history block.
        #
        # Env overrides: AUTUMN_EC_LOG / AUTUMN_EC_ROW
        #   "off"  → disable EC for that stream (pure replication)
        #   "K+M"  → use that explicit EC shape (open replicates is
        #            still the meta replication factor; only the
        #            post-seal EC encoding shape changes)
        local log_ec_default row_ec_default meta_repl
        if (( replicas >= 3 )); then meta_repl=3; else meta_repl=$replicas; fi
        local _ec_default=""
        if (( replicas == 4 )); then
            _ec_default="3+1"
        elif (( replicas >= 5 )); then
            _ec_default="4+1"
        fi
        log_ec_default="$_ec_default"
        row_ec_default="$_ec_default"

        local log_ec row_ec
        case "${AUTUMN_EC_LOG:-}" in
            off) log_ec="" ;;
            ?*)  log_ec="$AUTUMN_EC_LOG" ;;
            *)   log_ec="$log_ec_default" ;;
        esac
        case "${AUTUMN_EC_ROW:-}" in
            off) row_ec="" ;;
            ?*)  row_ec="$AUTUMN_EC_ROW" ;;
            *)   row_ec="$row_ec_default" ;;
        esac

        # Validate K+M <= replicas to catch misconfiguration early.
        for ec in "$log_ec" "$row_ec"; do
            if [[ -n "$ec" ]]; then
                [[ "$ec" =~ ^([0-9]+)\+([0-9]+)$ ]] || die "invalid EC shape '$ec' (expected K+M e.g. 3+1)"
                local ec_k="${BASH_REMATCH[1]}" ec_m="${BASH_REMATCH[2]}"
                (( ec_k >= 2 && ec_m >= 1 )) || die "EC shape '$ec' invalid: need K>=2 and M>=1"
                (( ec_k + ec_m <= replicas )) || die "EC shape '$ec' needs K+M=$((ec_k+ec_m)) nodes, only have $replicas"
            fi
        done

        local bootstrap_args=( --replication "${meta_repl}+0" )
        [[ -n "$log_ec" ]] && bootstrap_args+=( --log-ec "$log_ec" )
        [[ -n "$row_ec" ]] && bootstrap_args+=( --row-ec "$row_ec" )

        sleep 2  # give PS a moment to register with manager
        # AUTUMN_BOOTSTRAP_PRESPLIT: e.g. "4:3fffffff,7ffffffe,bffffffd"
        # The literal split points in the env var are documentation only;
        # autumn-client's `--presplit N:hexstring` calls hex_split_ranges(N)
        # internally and produces the same 0x3FFF.../0x7FFF.../0xBFFF... split
        # points. Forward just the partition count with `hexstring` kind.
        if [[ -n "${AUTUMN_BOOTSTRAP_PRESPLIT:-}" ]]; then
            local n_parts_arg="${AUTUMN_BOOTSTRAP_PRESPLIT%%:*}"
            [[ "$n_parts_arg" =~ ^[0-9]+$ ]] || n_parts_arg=1
            bootstrap_args+=( --presplit "${n_parts_arg}:hexstring" )
        fi
        "$AC" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" bootstrap "${bootstrap_args[@]}"
        touch "$bootstrap_marker"
        # Wait for PS to pick up the new partition(s) and finish opening them.
        # Each partition's open() runs stream commit_length calls serially against
        # the server-level stream_client, so total time scales with partition count.
        # Budget ~3s per partition (empirically sufficient at bootstrap time).
        local n_parts=1
        if [[ -n "${AUTUMN_BOOTSTRAP_PRESPLIT:-}" ]]; then
            n_parts="${AUTUMN_BOOTSTRAP_PRESPLIT%%:*}"
            [[ "$n_parts" =~ ^[0-9]+$ ]] || n_parts=1
        fi
        local wait_secs=$(( 3 * n_parts ))
        (( wait_secs < 3 )) && wait_secs=3
        echo "[cluster] waiting ${wait_secs}s for PS to open ${n_parts} partition(s)..."
        sleep "$wait_secs"
        # F099-K: confirm the first partition's listener is actually up.
        # Under F099-K the per-partition listener on :9201 only exists
        # once partition 0 has been opened and registered with the mgr.
        wait_port 9201 "partition 0 listener" 60
    fi

    # F102: snapshot launch params so per-process subcommands can replay
    # them later without the user re-typing flags or env vars.
    save_cluster_config

    echo ""
    echo "[cluster] ✓ cluster ready (replicas=$replicas)"
    echo "[cluster]   manager  : $MANAGER_ADDR"
    echo "[cluster]   partition: ${BIND_HOST}:9201"
    echo "[cluster]   logs     : $LOG_DIR"
    echo ""
    echo "  AC=(\"$AC\" --manager \"$MANAGER_ADDR\")"
    echo "  \"\${AC[@]}\" info"
    echo "  echo hello | \"\${AC[@]}\" put mykey /dev/stdin"
    echo "  \"\${AC[@]}\" get mykey"
    echo "  \"\${AC[@]}\" ls"
}

# ---------------------------------------------------------------------------
# F102: per-process subcommands (recovery testing)
# ---------------------------------------------------------------------------

do_start_node() {
    local i="$1"
    [[ "$i" =~ ^[0-9]+$ ]] && (( i >= 1 )) || die "usage: $0 start-node <N>  (positive integer)"
    load_cluster_config
    (( i <= REPLICAS )) || die "node$i exceeds REPLICAS=$REPLICAS in saved config; re-run '$0 start $i' to extend"
    need_bin "$NODE"
    need_bin "$AC"
    local pf; pf="$(pid_file "node$i")"
    if [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null; then
        die "node$i already running (pid $(cat "$pf"))"
    fi
    launch_extent_node "$i"
    if [[ "$MODE" != "multidisk-1node" ]]; then
        # register-node is now idempotent on duplicate addr (manager
        # rpc_handlers.rs handle_register_node) — safe to call on every
        # restart so a manager that lost state re-learns the node.
        register_extent_node "$i"
        echo "[cluster] node$i registered"
    fi
}

do_stop_node() {
    local i="$1"
    [[ "$i" =~ ^[0-9]+$ ]] && (( i >= 1 )) || die "usage: $0 stop-node <N>  (positive integer)"
    kill_proc "node$i"
}

do_start_ps() {
    load_cluster_config
    need_bin "$PS"
    local pf; pf="$(pid_file "ps")"
    if [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null; then
        die "ps already running (pid $(cat "$pf"))"
    fi
    launch_ps
    # Per-partition listener on :9201 only comes up after partition 0
    # opens (F099-K). Wait for it so a recovery test can `start-ps` and
    # immediately drive traffic.
    wait_port 9201 ps 60
}

do_stop_ps() {
    kill_proc ps
}

# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------

do_stop() {
    kill_proc ps
    # Stop any node1..nodeN by scanning pid files
    for pf in "$DATA_ROOT"/pids/node*.pid; do
        [[ -f "$pf" ]] || continue
        local name; name="$(basename "$pf" .pid)"
        kill_proc "$name"
    done
    kill_proc manager
    kill_proc etcd
    # Safety net: kill by binary name if pid files were missing (e.g. switching
    # between --shm and disk mode changes DATA_ROOT, so the old pids dir is
    # invisible to kill_proc).
    pkill -9 -f "$BIN/autumn-manager-server" 2>/dev/null || true
    pkill -9 -f "$BIN/autumn-extent-node" 2>/dev/null || true
    pkill -9 -f "$BIN/autumn-ps " 2>/dev/null || true
    # Match stray etcd only when its --data-dir is inside an autumn-rs tree.
    # Avoids killing unrelated etcd instances on the host.
    pkill -9 -f 'etcd --data-dir [^ ]*autumn-rs' 2>/dev/null || true
    echo "[cluster] all processes stopped"
}

# ---------------------------------------------------------------------------
# clean
# ---------------------------------------------------------------------------

do_clean() {
    do_stop
    rm -rf "$DATA_ROOT" "$LOG_DIR"
    # Also wipe alternate-disk data if this run used --3disk or --multidisk-1node.
    rm -rf /data03/autumn-rs /data05/autumn-rs /data08/autumn-rs 2>/dev/null || true
    echo "[cluster] data dirs wiped"
}

# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

do_status() {
    local names=(etcd manager)
    for pf in "$DATA_ROOT"/pids/node*.pid; do
        [[ -f "$pf" ]] && names+=("$(basename "$pf" .pid)")
    done
    names+=(ps)
    for name in "${names[@]}"; do
        local pf; pf="$(pid_file "$name")"
        if [[ -f "$pf" ]]; then
            local pid; pid="$(cat "$pf")"
            if kill -0 "$pid" 2>/dev/null; then
                echo "  $name  (pid $pid)  RUNNING"
            else
                echo "  $name  (pid $pid)  DEAD (stale pid file)"
            fi
        else
            echo "  $name  NOT STARTED"
        fi
    done
}

# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------

do_logs() {
    local logs=()
    for log in "$LOG_DIR"/*.log; do
        [[ -f "$log" ]] && logs+=("$log")
    done
    if [[ ${#logs[@]} -eq 0 ]]; then
        echo "[cluster] no log files found (cluster not started?)"
        exit 1
    fi
    tail -f "${logs[@]}"
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

CMD="${1:-help}"
shift || true

REPLICAS=1
MODE="default"  # default | 3disk | multidisk-1node
ARG_INT_PROVIDED=0  # F102: distinguishes "explicit N" from "default 1" so per-process subcommands can require an explicit index.
if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
    REPLICAS="$1"
    ARG_INT_PROVIDED=1
    shift
fi
while (( $# > 0 )); do
    case "$1" in
        --3disk)             MODE="3disk" ;;
        --multidisk-1node)   MODE="multidisk-1node" ;;
        *) die "unknown cluster.sh flag: $1" ;;
    esac
    shift
done

# Mode validation (only enforced for full-cluster commands; per-process
# subcommands inherit MODE from the saved config via load_cluster_config).
if [[ "$CMD" == "start" || "$CMD" == "restart" || "$CMD" == "reset" ]]; then
    if [[ "$MODE" == "3disk" ]]; then
        (( REPLICAS == 3 )) || die "--3disk requires replicas=3 (got $REPLICAS)"
    elif [[ "$MODE" == "multidisk-1node" ]]; then
        (( REPLICAS == 1 )) || die "--multidisk-1node requires replicas=1 (got $REPLICAS)"
    fi
fi
export CLUSTER_MODE="$MODE"

case "$CMD" in
    start)        do_start "$REPLICAS" ;;
    stop)         do_stop ;;
    restart)      do_stop; do_start "$REPLICAS" ;;
    clean)        do_clean ;;
    reset)        do_clean; do_start "$REPLICAS" ;;
    status)       do_status ;;
    logs)         do_logs ;;
    # F102: per-process control for recovery testing. Args inherit the
    # ARG_INT_PROVIDED gate above so a typo doesn't silently default N=1.
    start-node)
        (( ARG_INT_PROVIDED )) || die "usage: $0 start-node <N>"
        do_start_node "$REPLICAS"
        ;;
    stop-node)
        (( ARG_INT_PROVIDED )) || die "usage: $0 stop-node <N>"
        do_stop_node "$REPLICAS"
        ;;
    restart-node)
        (( ARG_INT_PROVIDED )) || die "usage: $0 restart-node <N>"
        do_stop_node "$REPLICAS"; do_start_node "$REPLICAS"
        ;;
    start-ps)     do_start_ps ;;
    stop-ps)      do_stop_ps ;;
    restart-ps)   do_stop_ps; do_start_ps ;;
    *)
        echo "Usage: $0 <command> [args]"
        echo ""
        echo "  Cluster:"
        echo "    start [N]          start full cluster (etcd + manager + N extent-nodes + ps)"
        echo "    stop               kill all cluster processes"
        echo "    restart [N]        stop + start (data preserved)"
        echo "    clean              stop + wipe data dirs"
        echo "    reset [N]          clean + start (fresh cluster)"
        echo "    status             show running processes"
        echo "    logs               tail all log files (Ctrl-C to exit)"
        echo ""
        echo "  Per-process (recovery testing — requires a prior 'start' to snapshot launch params):"
        echo "    start-node <N>     start extent-node N (1-indexed)"
        echo "    stop-node <N>      stop extent-node N"
        echo "    restart-node <N>   stop-node + start-node"
        echo "    start-ps           start partition server"
        echo "    stop-ps            stop partition server"
        echo "    restart-ps         stop-ps + start-ps"
        echo ""
        echo "  Mode flag (only with start/restart/reset):"
        echo "    --3disk            replicas=3, nodes on /data03,/data05,/data08"
        echo "    --multidisk-1node  replicas=1, one node spans all three NVMes"
        exit 1
        ;;
esac
