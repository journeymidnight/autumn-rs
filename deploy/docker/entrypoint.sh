#!/usr/bin/env bash
# entrypoint.sh — role dispatcher for the autumn-rs container image.
#
#   entrypoint.sh manager | extent-node | ps | bootstrap
#   entrypoint.sh <anything else>   → exec'd verbatim (client tooling, debug)
#
# Everything k8s-specific lives here, not in the Rust binaries (which stay
# env-free and take only CLI flags):
#   - DNS→IP resolution: the binaries parse addresses with SocketAddr::parse
#     (IP literals ONLY, no DNS anywhere — crates/transport/src/lib.rs
#     format_listen_addr, crates/etcd/src/transport.rs). Service names from
#     env are resolved to ClusterIPs here, then passed as flags.
#   - env→flag translation, mirroring cluster.sh's AUTUMN_* conventions.
#   - bring-up ordering guards (wait-for-leader, idempotent format,
#     streams-guarded bootstrap) — the same protocol as cluster.sh.
#
# Addressing model (see docs/k8s_deploy.md):
#   extent-node  advertises its per-pod Service ClusterIP (Service name ==
#                pod name). The manager keys EN identity on the advertise
#                address and an EN never re-registers a new address, so the
#                address must survive pod rescheduling — a ClusterIP does,
#                a pod IP does not.
#   ps           advertises its POD IP. Safe because every partition open
#                re-registers its address with the manager, so a rescheduled
#                PS heals its routing automatically. Per-partition listener
#                ports are dynamic (base+ord, monotonic) — flat pod-to-pod
#                networking carries them without Service port enumeration.

set -euo pipefail

log() { echo "[autumn-entrypoint] $*" >&2; }
die() { log "ERROR: $*"; exit 1; }

TRANSPORT="${AUTUMN_TRANSPORT:-tcp}"

# ---------------------------------------------------------------------------
# address helpers
# ---------------------------------------------------------------------------

# Wrap bare IPv6 literals in brackets so "host:port" strings survive
# SocketAddr::parse on the Rust side.
bracket_host() {
    local ip="$1"
    if [[ "$ip" == *:* && "$ip" != \[* ]]; then
        echo "[$ip]"
    else
        echo "$ip"
    fi
}

# Resolve a DNS name (or pass through an IP literal) to one IP, retrying
# until kube-dns catches up. Prints the bracketed-if-IPv6 IP.
resolve_ip() {
    local name="$1" tries="${2:-60}"
    name="${name#[}"; name="${name%]}"
    local i ip
    for (( i = 0; i < tries; i++ )); do
        ip="$(getent hosts "$name" 2>/dev/null | awk 'NR==1 {print $1}')" || true
        if [[ -n "${ip:-}" ]]; then
            bracket_host "$ip"
            return 0
        fi
        (( i == 0 )) && log "waiting for DNS: $name"
        sleep 2
    done
    die "cannot resolve '$name' after $tries attempts"
}

# Resolve every "name:port" in a comma-separated list to "IP:port".
resolve_hostport_list() {
    local list="$1" out="" item name port
    for item in ${list//,/ }; do
        # split on the LAST ':' so bracketed IPv6 names survive
        port="${item##*:}"
        name="${item%:*}"
        [[ "$port" =~ ^[0-9]+$ ]] || die "expected host:port, got '$item'"
        out+="${out:+,}$(resolve_ip "$name"):$port"
    done
    echo "$out"
}

# Leader-gated readiness: `autumn-op info` only succeeds against the elected
# leader, so this doubles as "control plane actually up", not just TCP-open.
wait_for_manager() {
    local mgr="$1" tries="${2:-150}"
    local i
    for (( i = 0; i < tries; i++ )); do
        if autumn-op --manager "$mgr" --transport "$TRANSPORT" info >/dev/null 2>&1; then
            return 0
        fi
        (( i == 0 )) && log "waiting for manager leader at $mgr"
        sleep 2
    done
    die "manager at $mgr not answering leader-gated info after $tries attempts"
}

# ---------------------------------------------------------------------------
# roles
# ---------------------------------------------------------------------------

run_manager() {
    local port="${AUTUMN_MANAGER_PORT:-9001}"
    local etcd
    etcd="$(resolve_hostport_list "${AUTUMN_ETCD_ENDPOINTS:-etcd:2379}")"
    local -a args=(
        --port "$port" --etcd "$etcd" --listen 0.0.0.0 --transport "$TRANSPORT"
    )
    [[ "${AUTUMN_POLICY_FAST_MODE:-0}" == "1" ]] && args+=(--policy-fast-mode)
    [[ "${AUTUMN_METRICS:-0}" == "1" ]] && args+=(--metrics-port 9591)
    # F-DASH-IN-MGR: embedded web dashboard + auto-policy controller, ON by
    # default (read-only viewer, bound to --listen). Set AUTUMN_DASHBOARD=0 to
    # disable; AUTUMN_DASHBOARD_ALLOW_MUTATIONS=1 arms manual actions + the
    # controller (autoPolicy runs ONLY on the leader).
    if [[ "${AUTUMN_DASHBOARD:-1}" != "0" ]]; then
        args+=(--dashboard-port "${AUTUMN_DASHBOARD_PORT:-8799}")
        [[ "${AUTUMN_DASHBOARD_ALLOW_MUTATIONS:-0}" == "1" ]] && args+=(--dashboard-allow-mutations)
    fi
    [[ -n "${AUTUMN_MGR_MIN_ALLOC_FREE_BYTES:-}" ]] \
        && args+=(--min-alloc-free-bytes "$AUTUMN_MGR_MIN_ALLOC_FREE_BYTES")
    # F-AUTHZ-1 (opt-in, same contract as cluster.sh: no key file = authz off)
    if [[ -n "${AUTUMN_AUTH_SIGNING_KEY_FILE:-}" ]]; then
        args+=(--auth-signing-key-file "$AUTUMN_AUTH_SIGNING_KEY_FILE")
        [[ -n "${AUTUMN_ADMIN_TOKEN_FILE:-}" ]] \
            && args+=(--admin-token-file "$AUTUMN_ADMIN_TOKEN_FILE")
        local pfx
        for pfx in ${AUTUMN_AUTH_PROTECTED_PREFIXES:-}; do
            args+=(--auth-protected-prefix "$pfx")
        done
        [[ -n "${AUTUMN_AUTH_TOKEN_TTL_SECS:-}" ]] \
            && args+=(--auth-token-ttl-secs "$AUTUMN_AUTH_TOKEN_TTL_SECS")
    fi
    log "exec autumn-manager-server ${args[*]}"
    exec autumn-manager-server "${args[@]}"
}

run_extent_node() {
    local port="${AUTUMN_EXTENT_PORT:-9101}"
    local data="${AUTUMN_EXTENT_DATA:-/data/autumn}"
    local mgr
    mgr="$(resolve_hostport_list "${AUTUMN_MANAGER:-autumn-manager:9001}")"
    [[ "$mgr" == *,* ]] && die "extent-node takes a single manager address, got '$mgr'"

    # Advertise the per-pod Service ClusterIP. Convention: the Service is
    # named after the pod (autumn-en-0, autumn-en-1, ...), so the default
    # needs no per-pod config at all.
    local adv
    adv="$(resolve_ip "${AUTUMN_ADVERTISE_NAME:-$HOSTNAME}")"

    # Sharding (AUTUMN_EXTENT_SHARDS, default 1). Shard i binds its data
    # listener at `port + i*stride` and its control listener at
    # `port+1000 + i*stride` (EN default stride 10 — keep it in sync here).
    # The manager routes an extent to shard `extent_id % shards` and dials
    # `advertise_host:shard_port`, so:
    #   - `format --shard-ports <csv>` MUST register every shard's DATA port,
    #     or the manager sends all extents to the base port (shard 0 only);
    #   - the per-pod Service MUST expose every shard's data AND control port.
    # shards=1 keeps the original single-shard behaviour (no --shard-ports).
    local shards="${AUTUMN_EXTENT_SHARDS:-1}"
    [[ "$shards" =~ ^[0-9]+$ && "$shards" -ge 1 ]] \
        || die "AUTUMN_EXTENT_SHARDS must be a positive integer"
    local stride=10
    local cpuset="${AUTUMN_EXTENT_CPUSET:-}"
    if [[ -z "$cpuset" ]]; then
        (( shards == 1 )) && cpuset="0" || cpuset="0-$((shards - 1))"
    fi
    local -a shard_ports_args=()
    if (( shards > 1 )); then
        local csv="" s
        for (( s = 0; s < shards; s++ )); do
            csv+="${csv:+,}$(( port + s * stride ))"
        done
        shard_ports_args=(--shard-ports "$csv")
    fi

    wait_for_manager "$mgr"

    # Idempotent (F214-C): re-running on a formatted dir reuses the stamped
    # disk_uuid and re-registers as the same node. Registration is keyed on
    # the advertise address — stable here by construction (ClusterIP).
    local -a dirs=()
    local d
    for d in ${data//,/ }; do
        mkdir -p "$d"
        dirs+=("$d")
    done
    local i
    for (( i = 0; i < 30; i++ )); do
        if autumn-op --manager "$mgr" --transport "$TRANSPORT" format \
            --listen ":$port" --advertise "${adv}:${port}" \
            "${shard_ports_args[@]}" "${dirs[@]}"; then
            break
        fi
        (( i == 29 )) && die "autumn-op format failed after 30 attempts"
        log "format attempt $((i + 1)) failed; retrying"
        sleep 2
    done

    local -a args=(
        --port "$port" --data "$data" --manager "$mgr"
        --listen 0.0.0.0 --transport "$TRANSPORT"
        # Shard count == cpuset length (EN F196). Default cpuset "0" = single
        # shard; AUTUMN_EXTENT_SHARDS=N gives cores 0..N-1. See the sharding
        # block above and docs/k8s_deploy.md "Multi-shard extent nodes".
        --cpuset "$cpuset"
    )
    [[ "${AUTUMN_METRICS:-0}" == "1" ]] && args+=(--metrics-port 9601)
    log "exec autumn-extent-node ${args[*]}"
    exec autumn-extent-node "${args[@]}"
}

# cluster.sh AUTUMN_* env → autumn-ps flag table (subset that makes sense
# off-host; UCX knobs excluded until a ucx-enabled image exists).
PS_TUNABLES=(
    "AUTUMN_GROUP_COMMIT_CAP:--group-commit-cap"
    "AUTUMN_PS_INFLIGHT_CAP:--ps-inflight-cap"
    "AUTUMN_PS_FLUSH_INFLIGHT_CAP:--flush-inflight-cap"
    "AUTUMN_PS_BULK_INFLIGHT_CAP:--ps-bulk-inflight-cap"
    "AUTUMN_PS_MAX_IMM_DEPTH:--max-imm-depth"
    "AUTUMN_PS_MAX_WAL_GAP:--max-wal-gap"
    "AUTUMN_PS_SHUTDOWN_TIMEOUT_MS:--shutdown-timeout-ms"
    "AUTUMN_PS_MAJOR_COMPACT_PARALLELISM:--major-compact-parallelism"
    "AUTUMN_PS_GC_PARALLELISM:--gc-parallelism"
    "AUTUMN_PS_CONN_INFLIGHT_CAP:--conn-inflight-cap"
    "AUTUMN_PS_FG_RATE_BYTES_PER_SEC:--fg-rate-bytes-per-sec"
    "AUTUMN_PS_FG_IOPS_PER_SEC:--fg-iops-per-sec"
    "AUTUMN_PS_ADMISSION_COMPACT_RATE_BYTES_PER_SEC:--admission-compact-rate-bytes-per-sec"
    "AUTUMN_PS_ADMISSION_GC_RATE_BYTES_PER_SEC:--admission-gc-rate-bytes-per-sec"
    "AUTUMN_PS_FG_SATURATED_THRESHOLD:--fg-saturated-threshold"
    "AUTUMN_PS_FG_QPS_QUOTA:--fg-qps-quota"
    "AUTUMN_PS_GC_DEBT_HIGH_BYTES:--gc-debt-high-bytes"
    "AUTUMN_PS_COMPACT_PENDING_HIGH_BYTES:--compact-pending-high-bytes"
    "AUTUMN_PS_GC_COOLDOWN_SECS:--gc-cooldown-secs"
    "AUTUMN_PS_COMPACT_COOLDOWN_SECS:--compact-cooldown-secs"
    "AUTUMN_READ_HEDGE_MS:--read-hedge-ms"
    "AUTUMN_APPEND_CHAIN_MIN_BYTES:--append-chain-min-bytes"
    "AUTUMN_SST_BLOCK_CACHE_BYTES:--sst-block-cache-bytes"
    "AUTUMN_PS_GC_READ_CHUNK_BYTES:--gc-read-chunk-bytes"
    "AUTUMN_PS_GC_BATCH_RECORDS:--gc-batch-records"
    "AUTUMN_PS_GC_BATCH_BYTES:--gc-batch-bytes"
    "AUTUMN_PS_GC_RATE_BYTES_PER_SEC:--gc-rate-bytes-per-sec"
)

run_ps() {
    local port="${AUTUMN_PS_PORT:-9301}"
    local mgr
    mgr="$(resolve_hostport_list "${AUTUMN_MANAGER:-autumn-manager:9001}")"

    # psid must be unique and non-zero: StatefulSet ordinal + 1.
    local psid="${AUTUMN_PSID:-}"
    if [[ -z "$psid" ]]; then
        local ord="${HOSTNAME##*-}"
        [[ "$ord" =~ ^[0-9]+$ ]] \
            || die "cannot derive psid from HOSTNAME='$HOSTNAME'; set AUTUMN_PSID"
        psid=$(( ord + 1 ))
    fi

    [[ -n "${POD_IP:-}" ]] || die "POD_IP not set (downward API status.podIP required)"
    local adv
    adv="$(bracket_host "$POD_IP")"

    wait_for_manager "$mgr"

    local -a args=(
        --psid "$psid" --port "$port" --manager "$mgr"
        --listen 0.0.0.0 --advertise "${adv}:${port}" --transport "$TRANSPORT"
    )
    [[ -n "${AUTUMN_PS_CPUSET:-}" ]] && args+=(--cpuset "$AUTUMN_PS_CPUSET")
    [[ "${AUTUMN_METRICS:-0}" == "1" ]] && args+=(--metrics-port 9701)
    local pair env_name flag
    for pair in "${PS_TUNABLES[@]}"; do
        env_name="${pair%%:*}"; flag="${pair#*:}"
        [[ -n "${!env_name:-}" ]] && args+=("$flag" "${!env_name}")
    done
    log "exec autumn-ps ${args[*]}"
    exec autumn-ps "${args[@]}"
}

run_bootstrap() {
    local expect="${AUTUMN_EXPECT_NODES:-3}"
    [[ "$expect" =~ ^[0-9]+$ && "$expect" -ge 1 ]] \
        || die "AUTUMN_EXPECT_NODES must be a positive integer"
    local mgr
    mgr="$(resolve_hostport_list "${AUTUMN_MANAGER:-autumn-manager:9001}")"
    wait_for_manager "$mgr"

    # Guard: `bootstrap` is NOT idempotent (a second run creates duplicate
    # streams/partitions). Mirror cluster.sh's etcd streams/ check via the
    # leader-gated info snapshot: any stream ⇒ already bootstrapped. Only
    # `info --full` emits the streams array ("stream_id") + partitions ("part_id").
    local info
    info="$(autumn-op --manager "$mgr" --transport "$TRANSPORT" --json info --full 2>/dev/null || true)"
    if grep -q '"stream_id"' <<<"$info"; then
        if grep -q '"part_id"' <<<"$info"; then
            log "cluster already bootstrapped (streams + partition) — nothing to do"
        else
            # Streams but no partition = a bootstrap that died mid-way. Do NOT
            # re-run (would duplicate streams); surface it loudly. The Job
            # completes so it stops retrying; an operator must inspect/repair.
            log "WARN: streams exist but NO partition — cluster looks HALF-bootstrapped; not re-running (would duplicate streams). Inspect 'autumn-op info --full'."
        fi
        exit 0
    fi

    log "waiting for $expect extent node(s) Online"
    local i n
    for (( i = 0; i < 60; i++ )); do
        n="$(autumn-op --manager "$mgr" --transport "$TRANSPORT" list-nodes 2>/dev/null \
            | grep -c Online || true)"
        (( n >= expect )) && break
        sleep 5
    done
    (( n >= expect )) || die "only $n/$expect extent node(s) Online"

    # EC + replication defaults follow cluster.sh: replication min(3,N);
    # N==4 → 3+1, N>=5 → 4+1, N<=3 → no EC. AUTUMN_EC_LOG/ROW: "off" or "K+M".
    local meta_repl=$(( expect >= 3 ? 3 : expect ))
    local ec_default=""
    (( expect == 4 )) && ec_default="3+1"
    (( expect >= 5 )) && ec_default="4+1"
    local log_ec row_ec
    case "${AUTUMN_EC_LOG:-}" in off) log_ec="" ;; ?*) log_ec="$AUTUMN_EC_LOG" ;; *) log_ec="$ec_default" ;; esac
    case "${AUTUMN_EC_ROW:-}" in off) row_ec="" ;; ?*) row_ec="$AUTUMN_EC_ROW" ;; *) row_ec="$ec_default" ;; esac

    local -a args=(--replication "${AUTUMN_REPLICATION:-${meta_repl}+0}")
    [[ -n "$log_ec" ]] && args+=(--log-ec "$log_ec")
    [[ -n "$row_ec" ]] && args+=(--row-ec "$row_ec")
    if [[ -n "${AUTUMN_BOOTSTRAP_PRESPLIT:-}" ]]; then
        local n_parts="${AUTUMN_BOOTSTRAP_PRESPLIT%%:*}"
        [[ "$n_parts" =~ ^[0-9]+$ ]] || die "AUTUMN_BOOTSTRAP_PRESPLIT must start with a partition count"
        args+=(--presplit "${n_parts}:hexstring")
    fi
    log "autumn-op bootstrap ${args[*]}"
    autumn-op --manager "$mgr" --transport "$TRANSPORT" bootstrap "${args[@]}"
    log "bootstrap complete"
}

# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------

case "${1:-}" in
    manager)      run_manager ;;
    extent-node)  run_extent_node ;;
    ps)           run_ps ;;
    bootstrap)    run_bootstrap ;;
    "")           die "usage: entrypoint.sh manager|extent-node|ps|bootstrap|<command...>" ;;
    *)            exec "$@" ;;
esac
