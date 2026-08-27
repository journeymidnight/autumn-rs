#!/usr/bin/env bash
# cluster.sh — dev/chaos/perf TEST harness for autumn-rs.
#
# NOT a deployment tool. For deploying autumn (single-host or multi-host,
# physical servers) use deploy/baremetal/autumn-deploy; for Kubernetes use
# deploy/k8s/. cluster.sh is kept because the chaos/perf suites (scripts/*.sh,
# the perf-check skill) depend on its RAW process kill/restart semantics for
# fault injection — the deploy path deliberately hides processes behind
# systemd/pidfiles, which is wrong for chaos testing. Keep them separate.
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

# RDMA (UCX rc_mlx5) pins every registered send/recv buffer against
# RLIMIT_MEMLOCK via ibv_reg_mr. The default soft limit (often 8 MiB) faults
# libibverbs on large (e.g. 8 MiB) value transfers — observed as a PS SIGSEGV
# in ucp_stream_send_nbx -> rcache -> ibv_reg_mr_iova2 under concurrent 8 MiB
# reads. Raise to unlimited here so every child (manager/extent-node/ps)
# inherits it; raising the HARD limit needs CAP_SYS_RESOURCE which the binaries
# don't have once launched, so it must happen in this launcher. Best-effort:
# under an unprivileged shell this is a no-op and the binaries still raise the
# soft limit up to the hard limit themselves. In production use a systemd unit
# with `LimitMEMLOCK=infinity`.
ulimit -l unlimited 2>/dev/null || true

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

# UCX env defaults (2026-07-03; the UCX C library reads UCX_* directly, so the
# launcher is the right layer — binaries stay env-free). ONE rule (user
# decision 2026-07-03): UCX clusters bind a RoCE-attached IP and get
#   UCX_TLS=rc_mlx5,ud_mlx5,tcp,self  +  a pinned UCX_NET_DEVICES.
# That single POSITIVE list (never `^` negation) serves BOTH intra-host
# (rc loopback in the HCA — 8M read 8 GB/s, zero stalls) and cross-host
# traffic. Do NOT add posix/cma: the posix large-message path stalls
# concurrent ≥64K transfers on the PS→EN hop (3 s timeout storms, apparent
# write wedges; rndv/zcopy knobs don't help). NET_DEVICES must be pinned
# (10-device auto-select hangs); mlx5_1:1 is this box's bench convention —
# production should pin a NON-GPU NIC.
#
# Binding a UCX cluster to loopback is NOT SUPPORTED by default (no RoCE GID
# on lo → rc unusable; the posix fallback is known-broken for ≥64K). It
# fails loudly unless the caller EXPLICITLY sets UCX_TLS (legacy escape
# hatch — the loopback chaos/perf harnesses set posix,cma,tcp,self
# themselves and keep working). Callers' explicit env always wins.
apply_ucx_env_defaults() {
    [[ "$TRANSPORT" == "ucx" ]] || return 0
    local plain="${BIND_HOST//[\[\]]/}"
    if [[ "$plain" == "127.0.0.1" || "$plain" == "::1" || "$plain" == "localhost" ]]; then
        if [[ -z "${UCX_TLS:-}" ]]; then
            die "UCX cluster bound to loopback: 127.0.0.1 is not an RDMA \
device address — bind a RoCE NIC IP (AUTUMN_BIND_HOST='[fdbd:dc62:3:302::14]' \
on this box). Legacy shm-only loopback mode requires an explicit \
UCX_TLS=posix,cma,tcp,self (≥64K transfers are known-broken there)"
        fi
        export UCX_TLS
    else
        export UCX_TLS="${UCX_TLS:-rc_mlx5,ud_mlx5,tcp,self}"
        export UCX_NET_DEVICES="${UCX_NET_DEVICES:-mlx5_1:1}"
    fi
    echo "[cluster] ucx env: UCX_TLS=$UCX_TLS${UCX_NET_DEVICES:+ UCX_NET_DEVICES=$UCX_NET_DEVICES}"
}
# Invoked from do_start and load_cluster_config (NOT here) so it always sees
# the FINAL BIND_HOST — including the one re-derived from the saved config by
# per-process subcommands (start-ps / start-node after a RoCE start).
# base port for the PS per-partition listeners (binds
# PS_BASE_PORT + ordinal). Kept clear of the extent-node shard port grid
# (9100+i + s*SHARD_STRIDE, climbs to ~9253 at 16 shards) so partition
# listeners don't lose their first bind to an EN shard and force a
# region-sync reopen — which also climbs the monotonic partition ord and
# overflows the PS cpuset (the cpu_pin "stays unpinned" warning). 9301
# leaves ~48 ports of headroom; override via AUTUMN_PS_BASE_PORT. Clients
# resolve partition addrs from the manager's GetRegions, so they follow
# automatically. (Also re-derived in load_cluster_config for start-ps.)
PS_BASE_PORT="${AUTUMN_PS_BASE_PORT:-9301}"
ETCD_DIR="$DATA_ROOT/etcd"

MANAGER="$BIN/autumn-manager-server"
NODE="$BIN/autumn-extent-node"
PS="$BIN/autumn-ps"
AC="$BIN/autumn-client"
# all cluster/partition op commands (bootstrap / register-node /
# format / split / merge / compact / gc / info / ...) live on autumn-op.
# autumn-client is data-plane only.
AO="$BIN/autumn-op"

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

# True iff $1 is a live, schedulable process — alive AND not a zombie.
#
# A daemon that cluster.sh backgrounds outlives the launching script: once
# `do_start` exits, the daemon reparents to pid 1. In a normal host pid 1
# (systemd/init) reaps it on exit. In a container whose pid 1 is NOT a
# reaper (here pid 1 is literally `sleep`), an exited daemon becomes an
# unwaitable ZOMBIE (`STAT Z`): the PID slot lingers forever holding only
# the exit status — all real resources (memory, sockets, ports) are already
# released. Crucially `kill -0 <zombie>` still SUCCEEDS and `kill -9` is a
# no-op, so the pre-fix `while kill -0` loop below would burn its full 60s
# SIGKILL deadline on every orphaned corpse (UCX daemons hit this constantly
# — they exit/crash more readily and nothing reaps them → "stop hangs").
# Treat state Z as "gone" so stop is instant against corpses while still
# giving a genuinely-alive PS its 60s graceful-drain window.
proc_alive() {
    local pid="$1"
    kill -0 "$pid" 2>/dev/null || return 1
    local st; st="$(ps -o stat= -p "$pid" 2>/dev/null | tr -d '[:space:]')"
    [[ "$st" == Z* ]] && return 1
    return 0
}

kill_proc() {
    local name="$1"
    local pf; pf="$(pid_file "$name")"
    if [[ -f "$pf" ]]; then
        local pid; pid="$(cat "$pf")"
        if proc_alive "$pid"; then
            kill "$pid" 2>/dev/null || true
            # wait up to 60s for process to exit gracefully — autumn-ps
            # now drains active+imm to row_stream on SIGTERM (PartitionServer::
            # shutdown), which can take seconds per partition with EC bulk
            # uploads. SIGKILL if still stuck after the deadline (replay on
            # restart covers any unflushed data). The loop breaks the instant
            # the process exits — including when it exits into an unreaped
            # zombie (proc_alive returns false on Z), so a daemon that drained
            # in 1ms doesn't cost 60s just because pid 1 won't bury the corpse.
            # NOTE: use pre-increment `((++i))` — `((i++))` returns the OLD value,
            # which is 0 on the first iteration and trips `set -e`.
            local i=0
            while proc_alive "$pid" && (( i < 600 )); do
                sleep 0.1
                (( ++i ))
            done
            if proc_alive "$pid"; then
                echo "[cluster] $name (pid $pid) did not exit within 60s; SIGKILL"
                kill -9 "$pid" 2>/dev/null || true
                sleep 0.2
            fi
            echo "[cluster] stopped $name (pid $pid)"
        elif kill -0 "$pid" 2>/dev/null; then
            # Corpse: real process already dead, just not reaped (non-reaping
            # pid 1). Its ports/sockets are free, so a fresh start can rebind.
            echo "[cluster] $name (pid $pid) already exited (unreaped zombie — pid 1 is not a reaper)"
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
    sleep 3
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
# per-process helpers + saved config
# ---------------------------------------------------------------------------
#
# Used by:
#   - do_start (full bring-up): writes the config snapshot once everything
#     is staged, then re-uses launch_extent_node / format_extent_node /
#     launch_ps so the per-process subcommands take exactly the same code
#     path as the bulk start. (format_extent_node replaces the
#     former register_extent_node — see its doc above.)
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

# Detect the available CPU count in a portable way (Linux + macOS).
# Returns 0 if no probe succeeded — caller should treat 0 as "unknown,
# don't bother with affinity".
detect_cpu_count() {
    local n
    n="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
    if [[ -z "$n" ]]; then n="$(nproc 2>/dev/null || true)"; fi
    if [[ -z "$n" ]]; then n="$(sysctl -n hw.ncpu 2>/dev/null || true)"; fi
    [[ "$n" =~ ^[0-9]+$ ]] || n=0
    echo "$n"
}

# Decide whether to pass --cpu-start to extent-nodes / PS. Sets the global
# AFFINITY_ENABLED=1|0 and (when enabled) leaves the calculated cpu_start
# values to the launchers. Compatible across macOS and Linux:
#   - macOS: core_affinity in our Rust code is best-effort; passing
#     --cpu-start is harmless. We still gate by detected core count.
#   - Linux with taskset: nproc honors the cpuset, so the gate triggers
#     correctly under restricted cgroups.
# Heuristic: need REPLICAS*SHARDS cores for EN side + 2*PS_PARTS_HINT for
# PS (P-log + P-sst per partition). PS_PARTS_HINT defaults to 8 (matches
# perf_check.sh default partition count); override via
# AUTUMN_PS_PARTS_HINT for clusters with more partitions.
compute_affinity_decision() {
    local detected required ps_hint
    detected="$(detect_cpu_count)"
    ps_hint="${AUTUMN_PS_PARTS_HINT:-8}"
    [[ "$ps_hint" =~ ^[0-9]+$ ]] && (( ps_hint >= 1 )) || ps_hint=8
    required=$(( REPLICAS * SHARDS + 2 * ps_hint ))
    if (( detected == 0 )); then
        AFFINITY_ENABLED=0
        echo "[cluster] affinity: disabled (could not detect CPU count)"
    elif (( detected < required )); then
        AFFINITY_ENABLED=0
        echo "[cluster] affinity: disabled (detected $detected cores < required $required = ${REPLICAS}*${SHARDS} EN + 2*${ps_hint} PS)"
    else
        AFFINITY_ENABLED=1
        echo "[cluster] affinity: enabled (detected $detected cores >= required $required)"
    fi
}

# Launch one extent-node ($1 = 1-indexed). Picks --shards/--data based on
# the current CLUSTER_MODE + AUTUMN_EXTENT_SHARDS env (compute_shard_config
# must have run first). Creates data dirs if missing. Waits for the primary
# port before returning.
launch_extent_node() {
    local i="$1"
    local port=$(( ${AUTUMN_EXTENT_BASE_PORT:-9100} + i ))
    local disk_arg
    disk_arg=$(disk_args_for_node "$i")
    # EN has no --shards flag — shard count == cpuset_len. Each
    # node gets its own slice of cores, computed from SHARDS env (which
    # now means "shards per EN, also = cores per EN"). Operator can
    # override per node via AUTUMN_EN${i}_CPUSET (taskset syntax).
    # PS gets the remaining cores after all EN ranges (see launch_ps).
    local cpu_start=$(( (i - 1) * SHARDS ))
    local cpu_end=$(( cpu_start + SHARDS - 1 ))
    local -a cpu_args=()
    local en_cpuset_var="AUTUMN_EN${i}_CPUSET"
    if [[ -n "${!en_cpuset_var:-}" ]]; then
        cpu_args=(--cpuset "${!en_cpuset_var}")
        # If the operator-supplied cpuset has a different cardinality from
        # the global SHARDS variable, the manager would format with the
        # wrong shard_ports list — every commit_length probe would land
        # on shard 0 and the EN would refuse with "extent N belongs to
        # shard M not shard 0". Fail loudly here instead of letting the
        # cluster come up in a wedged state. (Trivial taskset arithmetic;
        # exact pattern doesn't matter — count the comma + range-expanded
        # entries.)
        local _spec="${!en_cpuset_var}"
        local _count=0
        local _seg
        for _seg in ${_spec//,/ }; do
            if [[ "$_seg" =~ ^([0-9]+)-([0-9]+)$ ]]; then
                _count=$(( _count + ${BASH_REMATCH[2]} - ${BASH_REMATCH[1]} + 1 ))
            elif [[ "$_seg" =~ ^[0-9]+$ ]]; then
                _count=$(( _count + 1 ))
            fi
        done
        if (( _count != SHARDS )); then
            die "$en_cpuset_var='$_spec' has $_count cores but AUTUMN_EXTENT_SHARDS=$SHARDS — they MUST match (EN shards = cpuset_len)"
        fi
    elif (( ${AFFINITY_ENABLED:-0} == 1 )); then
        cpu_args=(--cpuset "${cpu_start}-${cpu_end}")
    fi
    # Without --cpuset the EN auto-detects all cores via core_affinity,
    # which on a dev box would spawn many shards — for the SHARDS=1
    # legacy default we still want one shard. Bound it with taskset.
    if [[ ${#cpu_args[@]} -eq 0 ]] && (( SHARDS == 1 )); then
        cpu_args=(--cpuset "0")
    fi
    # shellcheck disable=SC2046  # intentional word splitting on commas
    mkdir -p $(echo "$disk_arg" | tr ',' ' ')
    local -a stride_args=()
    (( SHARDS > 1 )) && stride_args=(--shard-stride "$SHARD_STRIDE")
    # --disk-id was removed. Single-disk and multi-disk paths
    # are now uniform — `autumn-op format` writes the disk_id sentinel
    # file per dir, which the EN reads on startup.
    local -a metrics_args=()
    if [[ "${AUTUMN_METRICS:-0}" == "1" ]]; then
        metrics_args=(--metrics-port "$(( 9600 + i ))")
    fi
    start_proc "node$i" \
        "$NODE" --port "$port" --data "$disk_arg" --manager "$MANAGER_ADDR" \
        --listen "$BIND_HOST" --transport "$TRANSPORT" \
        --advertise "${BIND_HOST}:$port" \
        ${stride_args[@]:+"${stride_args[@]}"} \
        ${metrics_args[@]:+"${metrics_args[@]}"} \
        ${cpu_args[@]:+"${cpu_args[@]}"}
    wait_port "$port" "node$i"
}

# Format one extent-node's data dir(s) ($1 = 1-indexed). Calls
# `autumn-op format` which queries the manager's cluster_id, allocates
# a fresh disk_uuid per dir, registers the node, and stamps the
# cluster_id / disk_uuid / node_id / disk_id sentinel files. Must run
# AFTER the manager is up and BEFORE the EN binary starts (the EN
# refuses to start without the sentinel files).
#
# unification: this replaces the former fork between
# `format` (used only for multidisk-1node) and `register-node`
# (used everywhere else with the `--disk-id N` flag). Both paths
# now go through `autumn-op format`, which handles single-disk
# and multi-disk uniformly.
format_extent_node() {
    local i="$1"
    local disk_arg
    disk_arg=$(disk_args_for_node "$i")
    # Ensure each dir exists before format touches it. `format` would
    # also create them, but cluster.sh has historically pre-created so
    # the launch path can rely on them.
    # shellcheck disable=SC2046  # intentional word splitting on commas
    mkdir -p $(echo "$disk_arg" | tr ',' ' ')
    # M1c: format is IDENTITY-ONLY — it takes no
    # --listen/--advertise/--shard-ports. launch_extent_node's own
    # --advertise self-registers the live address + shard ports at every
    # EN startup (M1a/M1b), so format never needs to know them.
    # shellcheck disable=SC2086  # intentional word splitting for positional args
    "$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" format \
        $(echo "$disk_arg" | tr ',' ' ')
}

# Launch the partition server. The per-partition listeners (PS_BASE_PORT+1,
# 9202, ...) come up only after partitions open, so we don't wait_port
# here — bootstrap or region-sync handles readiness.
launch_ps() {
    # even-distribution layout (single-host, multi-process cluster):
    #   EN i ∈ [1..REPLICAS]  → cores [(i-1)*SHARDS .. i*SHARDS-1]
    #   PS                    → cores [REPLICAS*SHARDS .. REPLICAS*SHARDS + 2*PS_PARTS_HINT - 1]
    # Each PS partition reserves 2 cores (P-log + P-sst). PS_PARTS_HINT
    # (defaults to 8, matches perf_check.sh) sets how many partitions
    # this PS has room for. compute_affinity_decision() guarantees
    # AFFINITY_ENABLED=1 only when total cores >= REPLICAS*SHARDS +
    # 2*PS_PARTS_HINT, so the EN ranges and the PS range are disjoint
    # by construction.
    #
    # AUTUMN_PS_CPUSET (taskset syntax, e.g. "8-15") overrides this auto
    # layout — useful for cross-host deployments where PS owns whole
    # ranges.
    local ps_hint="${AUTUMN_PS_PARTS_HINT:-8}"
    [[ "$ps_hint" =~ ^[0-9]+$ ]] && (( ps_hint >= 1 )) || ps_hint=8
    local ps_cpu_start=$(( REPLICAS * SHARDS ))
    local ps_cpu_end=$(( ps_cpu_start + 2 * ps_hint - 1 ))
    local -a cpu_args=()
    local affinity_msg="affinity=off"
    if [[ -n "${AUTUMN_PS_CPUSET:-}" ]]; then
        cpu_args=(--cpuset "$AUTUMN_PS_CPUSET")
        affinity_msg="cpuset=${AUTUMN_PS_CPUSET}"
    elif (( ${AFFINITY_ENABLED:-0} == 1 )); then
        cpu_args=(--cpuset "${ps_cpu_start}-${ps_cpu_end}")
        affinity_msg="cpuset=${ps_cpu_start}-${ps_cpu_end} (max_parts=$ps_hint)"
    fi
    # operator-set AUTUMN_* env vars are translated explicitly to
    # per-binary CLI flags here — Rust libraries no longer read env
    # vars. Each is opt-in: only emit the flag if the env is set.
    local -a tunable_args=()
    if [[ -n "${AUTUMN_GROUP_COMMIT_CAP:-}" ]]; then
        tunable_args+=(--group-commit-cap "$AUTUMN_GROUP_COMMIT_CAP")
    fi
    if [[ -n "${AUTUMN_PS_INFLIGHT_CAP:-}" ]]; then
        tunable_args+=(--ps-inflight-cap "$AUTUMN_PS_INFLIGHT_CAP")
    fi
    if [[ -n "${AUTUMN_PS_FLUSH_INFLIGHT_CAP:-}" ]]; then
        tunable_args+=(--flush-inflight-cap "$AUTUMN_PS_FLUSH_INFLIGHT_CAP")
    fi
    if [[ -n "${AUTUMN_PS_SST_INFLIGHT_CAP:-}" ]]; then
        tunable_args+=(--ps-bulk-inflight-cap "$AUTUMN_PS_SST_INFLIGHT_CAP")
    fi
    if [[ -n "${AUTUMN_PS_MAX_IMM_DEPTH:-}" ]]; then
        tunable_args+=(--max-imm-depth "$AUTUMN_PS_MAX_IMM_DEPTH")
    fi
    if [[ -n "${AUTUMN_PS_MAX_WAL_GAP:-}" ]]; then
        tunable_args+=(--max-wal-gap "$AUTUMN_PS_MAX_WAL_GAP")
    fi
    if [[ -n "${AUTUMN_PS_SHUTDOWN_TIMEOUT_MS:-}" ]]; then
        tunable_args+=(--shutdown-timeout-ms "$AUTUMN_PS_SHUTDOWN_TIMEOUT_MS")
    fi
    if [[ -n "${AUTUMN_PS_MAJOR_COMPACT_PARALLELISM:-}" ]]; then
        tunable_args+=(--major-compact-parallelism "$AUTUMN_PS_MAJOR_COMPACT_PARALLELISM")
    fi
    if [[ -n "${AUTUMN_PS_GC_PARALLELISM:-}" ]]; then
        tunable_args+=(--gc-parallelism "$AUTUMN_PS_GC_PARALLELISM")
    fi
    if [[ -n "${AUTUMN_PS_CONN_INFLIGHT_CAP:-}" ]]; then
        tunable_args+=(--conn-inflight-cap "$AUTUMN_PS_CONN_INFLIGHT_CAP")
    fi
    if [[ -n "${AUTUMN_PS_FG_RATE_BYTES_PER_SEC:-}" ]]; then
        tunable_args+=(--fg-rate-bytes-per-sec "$AUTUMN_PS_FG_RATE_BYTES_PER_SEC")
    fi
    if [[ -n "${AUTUMN_PS_FG_IOPS_PER_SEC:-}" ]]; then
        tunable_args+=(--fg-iops-per-sec "$AUTUMN_PS_FG_IOPS_PER_SEC")
    fi
    # D-r7: --bg-rate-bytes-per-sec was split into D-r7's admission
    # compact + gc rate caps. Note: the `--gc-rate-bytes-per-sec` flag is
    # a SEPARATE per-partition limiter wired via
    # AUTUMN_PS_GC_RATE_BYTES_PER_SEC and stays as-is.
    if [[ -n "${AUTUMN_PS_ADMISSION_COMPACT_RATE_BYTES_PER_SEC:-}" ]]; then
        tunable_args+=(--admission-compact-rate-bytes-per-sec \
            "$AUTUMN_PS_ADMISSION_COMPACT_RATE_BYTES_PER_SEC")
    fi
    if [[ -n "${AUTUMN_PS_ADMISSION_GC_RATE_BYTES_PER_SEC:-}" ]]; then
        tunable_args+=(--admission-gc-rate-bytes-per-sec \
            "$AUTUMN_PS_ADMISSION_GC_RATE_BYTES_PER_SEC")
    fi
    if [[ -n "${AUTUMN_PS_FG_SATURATED_THRESHOLD:-}" ]]; then
        tunable_args+=(--fg-saturated-threshold "$AUTUMN_PS_FG_SATURATED_THRESHOLD")
    fi
    if [[ -n "${AUTUMN_PS_FG_QPS_QUOTA:-}" ]]; then
        tunable_args+=(--fg-qps-quota "$AUTUMN_PS_FG_QPS_QUOTA")
    fi
    if [[ -n "${AUTUMN_PS_GC_DEBT_HIGH_BYTES:-}" ]]; then
        tunable_args+=(--gc-debt-high-bytes "$AUTUMN_PS_GC_DEBT_HIGH_BYTES")
    fi
    if [[ -n "${AUTUMN_PS_COMPACT_PENDING_HIGH_BYTES:-}" ]]; then
        tunable_args+=(--compact-pending-high-bytes "$AUTUMN_PS_COMPACT_PENDING_HIGH_BYTES")
    fi
    if [[ -n "${AUTUMN_PS_GC_COOLDOWN_SECS:-}" ]]; then
        tunable_args+=(--gc-cooldown-secs "$AUTUMN_PS_GC_COOLDOWN_SECS")
    fi
    if [[ -n "${AUTUMN_PS_COMPACT_COOLDOWN_SECS:-}" ]]; then
        tunable_args+=(--compact-cooldown-secs "$AUTUMN_PS_COMPACT_COOLDOWN_SECS")
    fi
    if [[ -n "${AUTUMN_PS_MIN_BATCH:-}" ]]; then
        tunable_args+=(--min-pipeline-batch "$AUTUMN_PS_MIN_BATCH")
    fi
    if [[ -n "${AUTUMN_READ_HEDGE_MS:-}" ]]; then
        tunable_args+=(--read-hedge-ms "$AUTUMN_READ_HEDGE_MS")
    fi
    if [[ -n "${AUTUMN_APPEND_CHAIN_MIN_BYTES:-}" ]]; then
        tunable_args+=(--append-chain-min-bytes "$AUTUMN_APPEND_CHAIN_MIN_BYTES")
    fi
    if [[ -n "${AUTUMN_SST_BLOCK_CACHE_BYTES:-}" ]]; then
        tunable_args+=(--sst-block-cache-bytes "$AUTUMN_SST_BLOCK_CACHE_BYTES")
    fi
    if [[ -n "${AUTUMN_PS_GC_READ_CHUNK_BYTES:-}" ]]; then
        tunable_args+=(--gc-read-chunk-bytes "$AUTUMN_PS_GC_READ_CHUNK_BYTES")
    fi
    if [[ -n "${AUTUMN_PS_GC_BATCH_RECORDS:-}" ]]; then
        tunable_args+=(--gc-batch-records "$AUTUMN_PS_GC_BATCH_RECORDS")
    fi
    if [[ -n "${AUTUMN_PS_GC_BATCH_BYTES:-}" ]]; then
        tunable_args+=(--gc-batch-bytes "$AUTUMN_PS_GC_BATCH_BYTES")
    fi
    if [[ -n "${AUTUMN_PS_GC_RATE_BYTES_PER_SEC:-}" ]]; then
        tunable_args+=(--gc-rate-bytes-per-sec "$AUTUMN_PS_GC_RATE_BYTES_PER_SEC")
    fi
    if [[ -n "${AUTUMN_UCX_REGPOOL_CAP_BYTES:-}" ]]; then
        tunable_args+=(--ucx-regpool-cap-bytes "$AUTUMN_UCX_REGPOOL_CAP_BYTES")
    fi
    if [[ -n "${AUTUMN_REGPOOL_LOG_INTERVAL_SECS:-}" ]]; then
        tunable_args+=(--regpool-log-interval-secs "$AUTUMN_REGPOOL_LOG_INTERVAL_SECS")
    fi
    if [[ "${AUTUMN_METRICS:-0}" == "1" ]]; then
        tunable_args+=(--metrics-port 9701)
    fi
    start_proc ps \
        "$PS" \
        --psid 1 --port "$PS_BASE_PORT" \
        --manager "$MANAGER_ADDR" \
        --listen "$BIND_HOST" \
        --advertise "${BIND_HOST}:${PS_BASE_PORT}" \
        --transport "$TRANSPORT" \
        ${cpu_args[@]:+"${cpu_args[@]}"} \
        ${tunable_args[@]:+"${tunable_args[@]}"}
    echo "[cluster] PS launched (per-partition listeners bind on partition open; $affinity_msg)"
}

# Derive the etcd client endpoint list from AUTUMN_ETCD_CLUSTER without
# starting anything — used by `start-manager` (R0 rolling restart), where
# etcd is already running and only the manager needs relaunching. Must
# mirror the port grid in do_start's etcd section (2379 + m*10).
compute_etcd_endpoints() {
    local etcd_n="${AUTUMN_ETCD_CLUSTER:-1}"
    [[ "$etcd_n" =~ ^[0-9]+$ ]] && (( etcd_n >= 1 )) || etcd_n=1
    if (( etcd_n == 1 )); then
        ETCD_ENDPOINTS="127.0.0.1:2379"
    else
        ETCD_ENDPOINTS=""
        local m
        for (( m=0; m<etcd_n; m++ )); do
            ETCD_ENDPOINTS+="${ETCD_ENDPOINTS:+,}127.0.0.1:$(( 2379 + m * 10 ))"
        done
    fi
}

# Launch the manager. Reads the global ETCD_ENDPOINTS (set by do_start's
# etcd bring-up, or by compute_etcd_endpoints for a standalone
# start-manager). AUTUMN_POLICY_FAST_MODE=1 enables fast-mode
# advisory thresholds (1 MiB GC / 4 MiB compact / 5s tick / 1 bucket /
# 30s cooldown) so load tests surface advisories within seconds instead
# of the production 5-min sustained window.
launch_manager() {
    local mgr_extra=""
    if [[ "${AUTUMN_POLICY_FAST_MODE:-0}" == "1" ]]; then
        mgr_extra="--policy-fast-mode"
    fi
    # Observability batch 1: AUTUMN_METRICS=1 wires Prometheus /metrics
    # endpoints on every role (manager 9591, EN 960<i>, PS 9701 — all
    # below the 10000 ephemeral floor, see BUG#3).
    if [[ "${AUTUMN_METRICS:-0}" == "1" ]]; then
        mgr_extra="$mgr_extra --metrics-port 9591"
    fi
    # The web dashboard is a separate app now (examples/dashboard → the
    # autumn-dashboard binary); cluster.sh does not start it. Run it by hand
    # against this cluster's manager + admin token when you want the UI.
    # unlike the deploy layer (entrypoint / autumn-deploy
    # default `balanced`), cluster.sh leaves the controller OFF by default so
    # chaos / perf / dev are unaffected. Set AUTUMN_AUTO_POLICY_DEFAULT=<preset> to
    # opt in for testing.
    [[ -n "${AUTUMN_AUTO_POLICY_DEFAULT:-}" ]] \
        && mgr_extra="$mgr_extra --auto-policy-default $AUTUMN_AUTO_POLICY_DEFAULT"
    if [[ -n "${AUTUMN_MGR_MIN_ALLOC_FREE_BYTES:-}" ]]; then
        [[ "$AUTUMN_MGR_MIN_ALLOC_FREE_BYTES" =~ ^[0-9]+$ ]] \
            || die "AUTUMN_MGR_MIN_ALLOC_FREE_BYTES must be a non-negative integer (got '$AUTUMN_MGR_MIN_ALLOC_FREE_BYTES')"
        mgr_extra="$mgr_extra --min-alloc-free-bytes $AUTUMN_MGR_MIN_ALLOC_FREE_BYTES"
    fi
    # Sticky authz: once provisioned, authz stays ON across `restart`/`start` even
    # without AUTUMN_AUTH=1 — if a signing key already exists on disk (from a prior
    # bring-up) and the operator didn't explicitly pass AUTUMN_AUTH=0, re-enable it
    # so a plain `restart` never SILENTLY drops token enforcement. `reset` wipes
    # all of $DATA_ROOT (incl authz/), so a fresh cluster starts clean unless
    # AUTUMN_AUTH=1 is set.
    if [[ "${AUTUMN_AUTH:-}" != "0" && -s "$DATA_ROOT/authz/signing.key" ]]; then
        AUTUMN_AUTH=1
    fi
    # Turnkey authz: AUTUMN_AUTH=1 auto-provisions a signing key + admin token and
    # protects mem/ & gallery/, feeding the AUTUMN_AUTH_* block below (per-example
    # tenant credentials are minted post-bootstrap). Files live under
    # $DATA_ROOT/authz/ and survive restart; `reset` wipes + regenerates them.
    # The ADMIN TOKEN is independent of data-plane authz and is now provisioned
    # UNCONDITIONALLY. They gate different planes — the admin token gates
    # control-plane admin RPCs (namespace-create, principal-create, …), the
    # signing key gates data-plane authz — but the token used to live inside the
    # signing-key branch, so a cluster without authz had NO admin token, and
    # `namespace-create` answered "admin RPCs disabled". That made it impossible
    # to register a namespace on a default cluster, which is exactly what the
    # benches need (BUG-BENCH-NS-UNREGISTERED). Configuring the token is safe:
    # it only ever ENABLES admin RPCs that were refused outright before.
    local _az="$DATA_ROOT/authz"
    mkdir -p "$_az"
    if [[ -z "${AUTUMN_ADMIN_TOKEN_FILE:-}" ]]; then
        [[ -s "$_az/admin.token" ]] \
            || head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$_az/admin.token"
        AUTUMN_ADMIN_TOKEN_FILE="$_az/admin.token"
    fi
    if [[ "${AUTUMN_AUTH:-0}" == "1" ]]; then
        if [[ -z "${AUTUMN_AUTH_SIGNING_KEY_FILE:-}" ]]; then
            [[ -s "$_az/signing.key" ]] \
                || "$AO" gen-signing-key > "$_az/signing.key" 2>/dev/null \
                || die "gen-signing-key failed (build autumn-op first)"
            AUTUMN_AUTH_SIGNING_KEY_FILE="$_az/signing.key"
        fi
        # authz protects EVERYTHING when a signing key is configured
        # no per-prefix list needed.
    fi
    # Pass the admin token whether or not a signing key exists (the signing-key
    # block below adds the authz flags on top when authz is on).
    if [[ -n "${AUTUMN_ADMIN_TOKEN_FILE:-}" && -r "${AUTUMN_ADMIN_TOKEN_FILE}" ]]; then
        mgr_extra="$mgr_extra --admin-token-file $AUTUMN_ADMIN_TOKEN_FILE"
    fi
    # data-plane authz (docs/data_plane_authz_design.md). OPT-IN —
    # with no env set, no flags are passed and authz stays OFF (zero impact on
    # every existing flow: perf_check, chaos, fuse, kvcache). To enable:
    #   AUTUMN_AUTH_SIGNING_KEY_FILE=/path/key   (generate: autumn-op gen-signing-key)
    #   AUTUMN_ADMIN_TOKEN_FILE=/path/token      (gates tenant-create/delete; file,
    #                                             not argv — /proc leak)
    #   AUTUMN_AUTH_PROTECTED_PREFIXES=mem/      (comma-separated; default mem/)
    #   AUTUMN_AUTH_TOKEN_TTL_SECS=3600          (optional)
    if [[ -n "${AUTUMN_AUTH_SIGNING_KEY_FILE:-}" ]]; then
        [[ -r "$AUTUMN_AUTH_SIGNING_KEY_FILE" ]] \
            || die "AUTUMN_AUTH_SIGNING_KEY_FILE '$AUTUMN_AUTH_SIGNING_KEY_FILE' is not readable"
        mgr_extra="$mgr_extra --auth-signing-key-file $AUTUMN_AUTH_SIGNING_KEY_FILE"
        # (--admin-token-file is added unconditionally above — it gates the
        # control plane and is not part of data-plane authz.)
        if [[ -n "${AUTUMN_AUTH_PROTECTED_PREFIXES:-}" ]]; then
            local _pfx
            for _pfx in ${AUTUMN_AUTH_PROTECTED_PREFIXES//,/ }; do
                mgr_extra="$mgr_extra --auth-protected-prefix $_pfx"
            done
        fi
        if [[ -n "${AUTUMN_AUTH_TOKEN_TTL_SECS:-}" ]]; then
            [[ "$AUTUMN_AUTH_TOKEN_TTL_SECS" =~ ^[0-9]+$ ]] \
                || die "AUTUMN_AUTH_TOKEN_TTL_SECS must be a non-negative integer (got '$AUTUMN_AUTH_TOKEN_TTL_SECS')"
            mgr_extra="$mgr_extra --auth-token-ttl-secs $AUTUMN_AUTH_TOKEN_TTL_SECS"
        fi
    elif [[ -n "${AUTUMN_AUTH_PROTECTED_PREFIXES:-}" ]]; then
        # NOTE: AUTUMN_ADMIN_TOKEN_FILE deliberately NOT in this guard any more.
        # The admin token gates CONTROL-plane admin RPCs and is now provisioned on
        # every cluster; only the protected-prefix list is meaningless without a
        # signing key (it configures DATA-plane authz).
        die "AUTUMN_AUTH_PROTECTED_PREFIXES set without AUTUMN_AUTH_SIGNING_KEY_FILE — data-plane authz needs a signing key (autumn-op gen-signing-key)"
    fi
    start_proc manager \
        "$MANAGER" --port 9001 --etcd "$ETCD_ENDPOINTS" --listen "$BIND_HOST" \
        --transport "$TRANSPORT" $mgr_extra
    wait_port 9001 manager
    # Wait for LEADERSHIP, not just the listener: a restart leaves the
    # previous leader's 10 s etcd lease behind, so the fresh manager can
    # spend ~10 s answering NOT_LEADER — `autumn-op format` / bootstrap
    # right after this point would fail and abort do_start under set -e
    # (flaky `cluster.sh restart` with preserved etcd; reset never hit it
    # because it wipes the lease). `info` is leader-gated → a clean probe.
    echo -n "[cluster] waiting for manager leadership..."
    local _t
    for _t in $(seq 1 60); do
        if "$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" info >/dev/null 2>&1; then
            echo " ok"
            return 0
        fi
        sleep 0.5
    done
    echo " TIMEOUT (continuing — early RPCs may transiently fail)"
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
        # Persist the EFFECTIVE transport: AUTUMN_TRANSPORT is unset early (so
        # children never read it), which used to silently drop `ucx` from the
        # snapshot — a later bare `start-ps`/`start-node`/`start-manager` then
        # relaunched that component as TCP against a UCX cluster (coco P1,
        # 2026-07-03). load_cluster_config restores it.
        printf 'TRANSPORT=%q\n' "$TRANSPORT"
        # `|| true`: with zero matching vars in the env, grep exits 1 and
        # pipefail+set -e would abort do_start right here (config written
        # truncated, "cluster ready" banner never printed) — only ever
        # surfaced on a bare `bash cluster.sh restart N` with a clean env.
        # UCX_* is persisted alongside AUTUMN_* so subcommands relaunch with
        # the exact same (explicit or derived) UCX env.
        env | { grep -E '^(AUTUMN_|UCX_)' || true; } | sort | while IFS='=' read -r k v; do
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
    # re-derive after sourcing so a persisted AUTUMN_PS_BASE_PORT
    # (saved by save_cluster_config) takes effect for start-ps/stop-ps.
    PS_BASE_PORT="${AUTUMN_PS_BASE_PORT:-9301}"
    compute_shard_config
    compute_affinity_decision
    apply_ucx_env_defaults
}

# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------

do_start() {
    local replicas="${1:-1}"
    # fresh=1 ONLY from `reset` — the explicit "wipe + fresh cluster" path.
    # `start` / `restart` pass 0 and MUST NEVER clean etcd (data-safety).
    local fresh="${2:-0}"
    [[ "$replicas" =~ ^[0-9]+$ ]] && (( replicas >= 1 )) || die "replicas must be a positive integer"
    # Sync the global to the count we're ACTUALLY launching. The dispatch may pass
    # a reused/persisted count that differs from the CLI-parsed global REPLICAS
    # (e.g. a bare `restart` reusing the snapshot). Without this, compute_affinity
    # and — critically — save_cluster_config would use the stale CLI default (1),
    # clobbering the persisted topology to 1 even though N ENs are running.
    REPLICAS="$replicas"
    apply_ucx_env_defaults

    # Stop any leftover processes from a previous run to avoid port conflicts
    do_stop

    need_bin "$MANAGER"
    need_bin "$NODE"
    need_bin "$PS"
    need_bin "$AC"
    need_bin "$AO"

    # Create data dirs for etcd and ps; node dirs are created per-node below.
    mkdir -p "$DATA_ROOT/etcd" "$DATA_ROOT/ps"

    # etcd — AUTUMN_ETCD_CLUSTER=N (default 1) starts an N-member etcd
    # cluster on client ports 2379/2389/2399... (peer ports +1). The
    # manager gets the full endpoint list; autumn-etcd's reconnect_shared
    # round-robins across members on failure (validated by
    # scripts/etcd_chaos.sh D0 member-kill).
    local etcd_n="${AUTUMN_ETCD_CLUSTER:-1}"
    [[ "$etcd_n" =~ ^[0-9]+$ ]] && (( etcd_n >= 1 )) || die "AUTUMN_ETCD_CLUSTER must be a positive integer"
    ETCD_ENDPOINTS=""
    if (( etcd_n == 1 )); then
        start_proc etcd \
            etcd \
            --data-dir "$ETCD_DIR" \
            --listen-client-urls http://127.0.0.1:2379 \
            --advertise-client-urls http://127.0.0.1:2379
        wait_port 2379 etcd 20 127.0.0.1
        ETCD_ENDPOINTS="127.0.0.1:2379"
    else
        local ic="" m cport pport
        for (( m=0; m<etcd_n; m++ )); do
            pport=$(( 2380 + m * 10 ))
            ic+="${ic:+,}etcd$m=http://127.0.0.1:$pport"
        done
        for (( m=0; m<etcd_n; m++ )); do
            cport=$(( 2379 + m * 10 ))
            pport=$(( 2380 + m * 10 ))
            mkdir -p "$ETCD_DIR-$m"
            start_proc "etcd$m" \
                etcd \
                --name "etcd$m" \
                --data-dir "$ETCD_DIR-$m" \
                --listen-client-urls "http://127.0.0.1:$cport" \
                --advertise-client-urls "http://127.0.0.1:$cport" \
                --listen-peer-urls "http://127.0.0.1:$pport" \
                --initial-advertise-peer-urls "http://127.0.0.1:$pport" \
                --initial-cluster "$ic" \
                --initial-cluster-state new
            ETCD_ENDPOINTS+="${ETCD_ENDPOINTS:+,}127.0.0.1:$cport"
        done
        for (( m=0; m<etcd_n; m++ )); do
            wait_port $(( 2379 + m * 10 )) "etcd$m" 30 127.0.0.1
        done
        echo "[cluster] etcd cluster: $etcd_n members ($ETCD_ENDPOINTS)"
    fi

    # DATA-SAFETY: clean etcd ONLY on an explicit `reset` (fresh=1). `start` /
    # `restart` MUST NEVER wipe etcd.
    #
    # History (the bug this fixes): the wipe used to be gated on the presence of
    # the LOCAL `$DATA_ROOT/bootstrapped` marker file — "no marker = fresh
    # cluster = clean etcd". But `restart` also routes through `do_start`, so any
    # time that marker was missing or desynced from etcd (tmpfs cleared on
    # reboot, a prior `clean`, a DATA_ROOT mismatch, a manual rm), a bare
    # `restart` SILENTLY deleted all cluster data. `restart` must be a pure
    # stop+start; only `reset`/`clean` may destroy data, and they say so.
    local bootstrap_marker="$DATA_ROOT/bootstrapped"
    if [[ "$fresh" == "1" ]]; then
        echo "[cluster] cleaning etcd (reset — fresh cluster, all data wiped)"
        etcdctl del "" --prefix >/dev/null 2>&1 || true
    fi

    launch_manager

    # Extent node(s): node1=9101, node2=9102, ...
    # Pre-create all data directories so both single-disk and multi-disk paths can rely on them.
    for (( i=1; i<=replicas; i++ )); do
        local disk_arg
        disk_arg=$(disk_args_for_node "$i")
        mkdir -p $(echo "$disk_arg" | tr ',' ' ')
    done

    # auto-size the PS partition budget from the presplit count when the
    # operator didn't set AUTUMN_PS_PARTS_HINT. Each partition needs 2 PS cores
    # (P-log + P-sst); the PS refuses partitions past cpuset_len/2, and
    # cluster.sh sizes the PS cpuset from AUTUMN_PS_PARTS_HINT (default 8). So
    # presplitting >8 partitions without bumping the hint silently strands the
    # surplus ("core budget exhausted"). Raise the hint to the presplit
    # count so a fresh start "just works". Only ever RAISES above the default 8
    # (≤8 keeps the default); an explicit AUTUMN_PS_PARTS_HINT always wins. If
    # the box lacks REPLICAS*SHARDS + 2*hint cores, compute_affinity_decision
    # disables pinning and the PS falls back to all-core auto-detect (a wide
    # budget), so this never over-constrains. NOTE: only fires when
    # AUTUMN_BOOTSTRAP_PRESPLIT is in the env (fresh start/reset); a bare
    # `restart` without it needs AUTUMN_PS_PARTS_HINT (or re-pass presplit).
    if [[ -z "${AUTUMN_PS_PARTS_HINT:-}" && -n "${AUTUMN_BOOTSTRAP_PRESPLIT:-}" ]]; then
        local _presplit_n="${AUTUMN_BOOTSTRAP_PRESPLIT%%:*}"
        if [[ "$_presplit_n" =~ ^[0-9]+$ ]] && (( _presplit_n > 8 )); then
            export AUTUMN_PS_PARTS_HINT="$_presplit_n"
            echo "[cluster] auto PS_PARTS_HINT=$_presplit_n (from presplit; PS budget = $(( _presplit_n * 2 )) cores)"
        fi
    fi

    # when AUTUMN_EXTENT_SHARDS is set, launch each extent-node
    # with K shards (see compute_shard_config + launch_extent_node above).
    compute_shard_config
    compute_affinity_decision
    if (( SHARDS > 1 )); then
        echo "[cluster] extent-node shards=$SHARDS stride=$SHARD_STRIDE"
    fi

    # format each EN's data dirs BEFORE launching the EN
    # binary. `autumn-op format` queries the manager's cluster_id,
    # registers the node, and stamps the per-dir sentinel files; the
    # EN binary refuses to start without them. The previous separate
    # `register-node` post-launch loop and the `multidisk-1node`
    # special case are both subsumed into this single per-node call.
    for (( i=1; i<=replicas; i++ )); do
        format_extent_node "$i"
        launch_extent_node "$i"
    done
    echo "[cluster] extent node(s) formatted + launched"

    if [[ -n "${AUTUMN_GROUP_COMMIT_CAP:-}" ]]; then
        echo "[cluster] AUTUMN_GROUP_COMMIT_CAP=$AUTUMN_GROUP_COMMIT_CAP (forwarding to PS)"
    fi

    launch_ps

    # bootstrap (create streams + partitions) — only when the cluster has never
    # been bootstrapped. AUTHORITATIVE check = does etcd already hold cluster
    # metadata (`streams/` keys, written by bootstrap). The local marker file
    # alone is unreliable: a `restart` whose marker was lost must NOT
    # re-bootstrap on top of live etcd data (that would create duplicate
    # streams/partitions over the preserved data). etcd is the source of truth;
    # the marker is just a fast-path hint we self-heal here.
    local etcd_bootstrapped=0
    if etcdctl get "streams/" --prefix --keys-only 2>/dev/null | grep -q .; then
        etcd_bootstrapped=1
    fi
    if [[ -f "$bootstrap_marker" || "$etcd_bootstrapped" == "1" ]]; then
        if [[ ! -f "$bootstrap_marker" ]]; then
            echo "[cluster] etcd already bootstrapped (marker was missing) — preserving data, re-creating marker"
            mkdir -p "$DATA_ROOT" && touch "$bootstrap_marker"
        else
            echo "[cluster] skipping bootstrap (already done — preserving data; use 'reset' for a fresh cluster)"
        fi
        # Give PS a moment to sync regions and re-bind listeners on restart.
        wait_port "$PS_BASE_PORT" ps 60
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

        # UCX warmup race: wait for `replicas` EN df-health Online states
        # before bootstrap (otherwise allocate_extent → "no healthy node").
        # Same as the cross-host UCX bring-up path documented in
        # docs/perf_tcp_vs_ucx_xhost.md.
        for _i in $(seq 1 24); do
            sleep 5
            local _n
            _n=$("$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" list-nodes 2>/dev/null | grep -c Online || true)
            if [[ "${_n:-0}" -ge "${replicas}" ]]; then
                echo "[cluster] ${_n} node(s) Online after ${_i}×5s"
                break
            fi
            echo "[cluster] waiting for Online nodes (round ${_i})..."
        done
        # `bootstrap --presplit` is RETIRED (it cut the RAW
        # keyspace, which misses every `{ns}/`-prefixed key). AUTUMN_BOOTSTRAP_PRESPLIT
        # is now interpreted as "presplit the BENCH namespace into N partitions",
        # applied after bootstrap + namespace registration below.
        # the manager now always has an admin token (provisioned
        # above), and bootstrap sends CREATE_STREAM / UPSERT_PARTITION, which the
        # manager gates. Pass the token so bring-up is authorized.
        local _boot_admin=()
        [[ -r "${AUTUMN_ADMIN_TOKEN_FILE:-}" ]] && _boot_admin=( --admin-token-file "$AUTUMN_ADMIN_TOKEN_FILE" )
        "$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" "${_boot_admin[@]}" bootstrap "${bootstrap_args[@]}"
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
        # confirm the first partition's listener is actually up.
        # The per-partition listener on :$PS_BASE_PORT only exists
        # once partition 0 has been opened and registered with the mgr.
        wait_port "$PS_BASE_PORT" "partition 0 listener" 60
    fi

    # Turnkey authz (AUTUMN_AUTH=1): register the gallery namespace + mint the
    # per-example tenant credentials into $DATA_ROOT/authz/. Idempotent — a
    # tenant whose .cred already exists is left alone so restarts keep working.
    # The examples read their cred via AUTUMN_CREDENTIAL_FILE / --credential-file:
    #   memory-mcp --credential-file $DATA_ROOT/authz/memory.cred
    #   AUTUMN_CREDENTIAL_FILE=$DATA_ROOT/authz/gallery.cred gallery <mgr>
    # BUG-BENCH-NS-UNREGISTERED: register `bench` on EVERY cluster, not just an
    # authz one. Layer-A is active whenever the namespace registry is non-empty —
    # and bootstrap always seeds fs/kvc/mem — so it runs even with authz OFF and
    # rejects any write whose first key segment is unregistered. perf-check and
    # ycsb bind the scope `bench/perf`, so without this every bench write dies
    # with NamespaceUnknown. Idempotent (already-exists is a no-op).
    if [[ -r "$DATA_ROOT/authz/admin.token" ]]; then
        local _btok; _btok="$(cat "$DATA_ROOT/authz/admin.token")"
        "$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" \
            namespace-create --name bench --admin-token "$_btok" >/dev/null 2>&1 || true
        # Presplit the bench namespace RELATIVE to `bench/perf/` so the cut points
        # are where the bench keys actually are. The retired bootstrap presplit cut
        # raw hex points that `bench_user_starts` filtered out entirely, so every
        # `--partitions N` run silently measured one partition.
        local _bp="${AUTUMN_BOOTSTRAP_PRESPLIT:-}"; local _bparts="${_bp%%:*}"
        if [[ "$_bparts" =~ ^[0-9]+$ ]] && (( _bparts > 1 )); then
            "$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" \
                presplit --namespace bench --tenant perf --count "$_bparts" \
                --admin-token "$_btok" \
                || echo "[cluster] warning: bench presplit into $_bparts failed (bench will use 1 partition)"
        fi
    fi

    if [[ "${AUTUMN_AUTH:-0}" == "1" ]]; then
        local _az="$DATA_ROOT/authz"
        local _atok; _atok="$(cat "$_az/admin.token")"
        local _ao=( "$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" )
        "${_ao[@]}" namespace-create --name gallery --admin-token "$_atok" >/dev/null 2>&1 || true
        local _spec _p _grant _out
        # (§8.8): per-family principals (NS-FIRST keys, no
        # tenant segment). Each app uses its own credential = least privilege by
        # default (no all-ns master key). Cred file = two-line `principal:`/
        # `credential:` form (read_credential_file carries the name).
        #   fs/  → fuse mount / autumnfs        kvc/ → kvcache loader
        #   gallery/ → gallery example          mem/memory/ → memory-mcp
        for _spec in "fs fs/ $_az/fs.cred" \
                     "kvc kvc/ $_az/kvc.cred" \
                     "gallery gallery/ $_az/gallery.cred" \
                     "memory mem/memory/ $_az/memory.cred"; do
            read -r _p _grant _out <<< "$_spec"
            [[ -s "$_out" ]] && continue
            "${_ao[@]}" --json principal-create --principal "$_p" --grant "$_grant" --admin-token "$_atok" \
                | python3 -c 'import sys,json;d=json.load(sys.stdin);print("principal: "+d["principal"]);print("credential: "+d["credential"])' > "$_out" \
                || die "principal-create $_p failed"
            [[ -s "$_out" ]] || die "principal-create $_p produced no credential"
        done
        echo "[cluster] authz ON: signing key + admin token + per-family creds in $_az/"
        echo "[cluster]   fuse/autumnfs: --credential-file $_az/fs.cred"
        echo "[cluster]   memory-mcp: --credential-file $_az/memory.cred"
        echo "[cluster]   gallery: AUTUMN_CREDENTIAL_FILE=$_az/gallery.cred"
    fi

    # snapshot launch params so per-process subcommands can replay
    # them later without the user re-typing flags or env vars.
    save_cluster_config

    echo ""
    echo "[cluster] ✓ cluster ready (replicas=$replicas)"
    echo "[cluster]   manager  : $MANAGER_ADDR"
    echo "[cluster]   partition: ${BIND_HOST}:${PS_BASE_PORT}"
    echo "[cluster]   logs     : $LOG_DIR"
    echo ""
    echo "  AC=(\"$AC\" --manager \"$MANAGER_ADDR\")     # data plane (put/get/ls/bench)"
    echo "  AO=(\"$AO\" --manager \"$MANAGER_ADDR\")     # op plane (info/split/merge/...)"
    echo "  \"\${AO[@]}\" info"
    echo "  echo hello | \"\${AC[@]}\" put mykey /dev/stdin"
    echo "  \"\${AC[@]}\" get mykey"
    echo "  \"\${AC[@]}\" ls"
}

# ---------------------------------------------------------------------------
# per-process subcommands (recovery testing)
# ---------------------------------------------------------------------------

do_start_node() {
    local i="$1"
    [[ "$i" =~ ^[0-9]+$ ]] && (( i >= 1 )) || die "usage: $0 start-node <N>  (positive integer)"
    load_cluster_config
    (( i <= REPLICAS )) || die "node$i exceeds REPLICAS=$REPLICAS in saved config; re-run '$0 start $i' to extend"
    need_bin "$NODE"
    need_bin "$AC"
    need_bin "$AO"
    local pf; pf="$(pid_file "node$i")"
    if [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null; then
        die "node$i already running (pid $(cat "$pf"))"
    fi
    # idempotent re-format. The dir's existing cluster_id /
    # disk_uuid sentinel files match the manager → format reuses the
    # existing disk_uuid and re-calls register-node. This is the safety
    # net for "manager lost in-memory state (no etcd mode) after a
    # restart" — the deposed manager's node table is gone, but format
    # re-imprints it. With etcd, this call is a no-op aside from
    # bumping the heartbeat timestamp.
    format_extent_node "$i"
    launch_extent_node "$i"
    echo "[cluster] node$i formatted + launched"
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
    # Per-partition listener on :$PS_BASE_PORT only comes up after partition 0
    # opens. Wait for it so a recovery test can `start-ps` and
    # immediately drive traffic.
    wait_port "$PS_BASE_PORT" ps 60
}

do_stop_ps() {
    kill_proc ps
}

# R0 rolling restart: the manager was the one role without per-process
# subcommands (its launch was inlined in do_start). With etcd it replays
# all state on start, so stop-manager + start-manager is a safe rolling
# step — EN/PS retry their manager RPCs through the gap.
do_start_manager() {
    load_cluster_config
    need_bin "$MANAGER"
    local pf; pf="$(pid_file manager)"
    if [[ -f "$pf" ]] && proc_alive "$(cat "$pf")"; then
        die "manager already running (pid $(cat "$pf"))"
    fi
    compute_etcd_endpoints
    launch_manager
}

do_stop_manager() {
    kill_proc manager
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
    # Multi-member etcd (AUTUMN_ETCD_CLUSTER>1): etcd0.pid, etcd1.pid, ...
    # (coco P2: the node*.pid loop above doesn't match them, and the
    # pkill net below only covers autumn-rs data-dir paths).
    for pf in "$DATA_ROOT"/pids/etcd*.pid; do
        [[ -f "$pf" ]] || continue
        local name; name="$(basename "$pf" .pid)"
        kill_proc "$name"
    done
    # Safety net: kill by binary name if pid files were missing (e.g. switching
    # between --shm and disk mode changes DATA_ROOT, so the old pids dir is
    # invisible to kill_proc).
    pkill -9 -f "$BIN/autumn-manager-server" 2>/dev/null || true
    pkill -9 -f "$BIN/autumn-extent-node" 2>/dev/null || true
    pkill -9 -f "$BIN/autumn-ps " 2>/dev/null || true
    # Match stray etcd only when its --data-dir is inside an autumn-rs tree.
    # Avoids killing unrelated etcd instances on the host.
    pkill -9 -f 'etcd .*--data-dir [^ ]*autumn-rs' 2>/dev/null || true
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
ARG_INT_PROVIDED=0  # distinguishes "explicit N" from "default 1" so per-process subcommands can require an explicit index.
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

# For the etcd-REUSING bring-ups (`start`/`restart`, fresh=0), resolve the EN
# count into $EFF_REPLICAS. The persisted snapshot = the topology etcd's
# partitions are placed on, so:
#   - no N (bare `start`/`restart`) → re-launch exactly the persisted count (a
#     bare bring-up must not shrink to the default 1).
#   - N >= persisted (`restart 8` on a 5-EN cluster) → GROW: launch N ENs; the
#     first `persisted` reuse their formatted disks, the extra N-persisted format
#     fresh. Adding capacity is safe. do_start re-persists N.
#   - N <  persisted (`restart 3` on a 5-EN cluster) → REFUSE: partitions placed
#     across `persisted` nodes would be stranded on absent extents (PS loops on
#     `commit_length`, never binds its listener). Use `reset N` to re-place.
# MUST run in the MAIN shell BEFORE do_stop — a `die` inside a `$(...)` only kills
# the subshell, which would leave the cluster stopped on a rejected shrink.
resolve_replicas() {
    local snap="$REPLICAS"
    if [[ -f "$CONFIG_FILE" ]]; then
        local s
        s="$(sed -n 's/^REPLICAS=//p' "$CONFIG_FILE" | head -1)"
        [[ "$s" =~ ^[0-9]+$ ]] && snap="$s"
    fi
    if (( ARG_INT_PROVIDED )); then
        if (( REPLICAS < snap )); then
            die "$CMD $REPLICAS would SHRINK below the persisted $snap ENs — partitions placed across $snap nodes would be stranded on absent extents. Keep N >= $snap to grow/same, or run 'reset $REPLICAS' to re-place from a clean etcd."
        fi
        (( REPLICAS > snap )) && echo "[cluster] $CMD: growing $snap → $REPLICAS ENs (+$((REPLICAS - snap)); existing extents preserved)." >&2
        EFF_REPLICAS="$REPLICAS"
    else
        EFF_REPLICAS="$snap"
    fi
}

case "$CMD" in
    start)        resolve_replicas; do_start "$EFF_REPLICAS" 0 ;;
    stop)         do_stop ;;
    restart)      resolve_replicas; do_stop; do_start "$EFF_REPLICAS" 0 ;;
    clean)        do_clean ;;
    reset)        do_clean; do_start "$REPLICAS" 1 ;;
    status)       do_status ;;
    logs)         do_logs ;;
    # per-process control for recovery testing. Args inherit the
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
    start-manager)   do_start_manager ;;
    stop-manager)    do_stop_manager ;;
    restart-manager) do_stop_manager; do_start_manager ;;
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
        echo "    start-manager      start manager (etcd must already be up)"
        echo "    stop-manager       stop manager"
        echo "    restart-manager    stop-manager + start-manager (etcd replay)"
        echo ""
        echo "  Mode flag (only with start/restart/reset):"
        echo "    --3disk            replicas=3, nodes on /data03,/data05,/data08"
        echo "    --multidisk-1node  replicas=1, one node spans all three NVMes"
        exit 1
        ;;
esac
