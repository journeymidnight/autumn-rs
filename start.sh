#!/usr/bin/env bash
# start.sh — single-host autumn-rs cluster launcher
#
# Brings up: etcd + manager + N extent-nodes (one per /dataK NVMe) + 1 PS,
# then bootstraps an empty cluster.
#
# Edit DATA_DIRS to match your local NVMe layout. Each present dir = one EN.
# Use ./stop.sh (or `pkill -f autumn-` + `pkill etcd`) to tear down.

set -euo pipefail

# ============================================================
# Config
# ============================================================

# Per-EN data dirs. Each present dir → one EN. Missing dirs are skipped.
DATA_DIRS=(
    /data03/autumn-rs
    /data05/autumn-rs
    /data06/autumn-rs
    /data07/autumn-rs
    /data08/autumn-rs
)

REPO="${REPO:-$(cd "$(dirname "$0")" && pwd)}"
BIN="${BIN:-$REPO/target/release}"
WORK="${WORK:-/var/lib/autumn-rs}"            # etcd data + PS local state + logs
LOG_DIR="${LOG_DIR:-$WORK/logs}"
BIND_HOST="${BIND_HOST:-127.0.0.1}"
MANAGER_ADDR="$BIND_HOST:9001"
PS_PORT="${PS_PORT:-9301}"
EN_BASE_PORT="${EN_BASE_PORT:-18101}"          # EN i listens on EN_BASE_PORT+i-1
# autumn-op format hardcodes control_addr = data_port + 1000, so the EN
# data and control ports MUST stay 1000 apart. Pick a base whose +1000
# range is also free. On this host Ray's IDLE workers claim 10101-10110,
# which is autumn's historical default (9101+1000). Moved both ranges
# above that to 18101-18105 (data) + 19101-19105 (control). Bump
# EN_BASE_PORT if 18101+ is also in use on your host.
EN_CPU_BASE="${EN_CPU_BASE:-0}"                # EN i pinned to cpu (EN_CPU_BASE+i-1)
TRANSPORT="${TRANSPORT:-tcp}"                  # tcp | ucx

MANAGER="$BIN/autumn-manager-server"
NODE="$BIN/autumn-extent-node"
PS="$BIN/autumn-ps"
AO="$BIN/autumn-op"

# ============================================================
# Helpers
# ============================================================

log()  { printf '[start] %s\n' "$*" >&2; }
die()  { printf '[start] error: %s\n' "$*" >&2; exit 1; }

wait_port() {
    # wait_port <port> <name> [host] [tries]
    # host defaults to $BIND_HOST; pass an explicit host for services that
    # bind elsewhere (etcd is hardcoded to 127.0.0.1, NOT $BIND_HOST).
    local port="$1" name="$2"
    local host="${3:-$BIND_HOST}"
    local tries="${4:-30}"
    # bash /dev/tcp does NOT accept bracketed IPv6 (`[fdbd::14]`) — strip the
    # brackets so the bare address reaches getaddrinfo. Without this, any
    # non-loopback BIND_HOST makes every probe falsely time out even though
    # the service is up.
    host="${host#[}"; host="${host%]}"
    local i  # CRITICAL: must be local, else clobbers the outer for-loop's $i
    for ((i=0; i<tries; i++)); do
        if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
            exec 3>&- 3<&-
            return 0
        fi
        sleep 0.5
    done
    die "$name did not open port $port within $((tries/2))s"
}

start_proc() {
    # start_proc <name> <cmd...>  — daemonize via setsid + nohup, log to LOG_DIR
    local name="$1"; shift
    local log="$LOG_DIR/$name.log"
    log "starting $name → $log"
    nohup setsid "$@" >"$log" 2>&1 &
    sleep 0.1
}

# ============================================================
# UCX transport env  (only when TRANSPORT=ucx)
# ============================================================
# Exported here so the manager/EN/PS children (started via start_proc →
# setsid/nohup) inherit it. UCX_* are read by the UCX C library directly,
# not by autumn rust code — consistent with "config via flags not env in
# rust": these configure a third-party lib, so the script is the right place.
if [[ "$TRANSPORT" == "ucx" ]]; then
    # POSITIVE list only — NEVER prefix an entry with `^` (a leading `^`
    # negates the WHOLE list; a non-leading `^x` is silently ignored).
    # Union: rc_mlx5/ud_mlx5 (cross-host RoCE) + posix/cma (same-host shm:
    # posix = short msgs, cma = large rendezvous bulk) + tcp (fallback) + self.
    # UCX scores per-connection and auto-picks shm for a same-host pair,
    # rc_mlx5 for a cross-host pair — so ONE cluster serves both local & remote.
    # Order is irrelevant (UCX_TLS is an allow-set, not a priority list).
    # sysv is deliberately excluded (buggy on this stack).
    export UCX_TLS="${UCX_TLS:-rc_mlx5,ud_mlx5,posix,cma,tcp,self}"
    # This host has 10 RoCE devices → UCX auto-select HANGS cross-host; pin the
    # one on your routable subnet (here mlx5_1 carries the ::14/::15 subnet).
    # Verify per host:  scripts/check_roce.sh --listen-candidates
    #   (or: ls /sys/class/net/<iface>/device/infiniband/)
    # Harmless for loopback — shm TLs (posix/cma/self) ignore net devices.
    export UCX_NET_DEVICES="${UCX_NET_DEVICES:-mlx5_1:1}"
    # RDMA pins memory via ibv_reg_mr; default 8 MiB memlock faults on >=256K pages.
    ulimit -l unlimited 2>/dev/null || true
    log "ucx env: UCX_TLS=$UCX_TLS  UCX_NET_DEVICES=$UCX_NET_DEVICES  memlock=$(ulimit -l)"
    # NOTE: to actually serve cross-host, launch with a ROUTABLE bind address,
    # e.g.  BIND_HOST='[fdbd:dc62:3:302::14]' TRANSPORT=ucx ./start.sh
    # (default BIND_HOST=127.0.0.1 is loopback-only — remote clients can't reach it).
fi

# ============================================================
# Preflight
# ============================================================

[[ -x "$MANAGER" ]] || die "manager binary missing: $MANAGER (run: cargo build --release -p autumn-server)"
[[ -x "$NODE"    ]] || die "extent-node binary missing: $NODE"
[[ -x "$PS"      ]] || die "ps binary missing: $PS"
[[ -x "$AO"      ]] || die "autumn-op binary missing: $AO"
command -v etcd >/dev/null || die "etcd not in PATH — install from github.com/etcd-io/etcd/releases"

# Filter to present disks
present_disks=()
for d in "${DATA_DIRS[@]}"; do
    parent="$(dirname "$d")"
    if [[ -d "$parent" ]]; then
        mkdir -p "$d"
        present_disks+=("$d")
    else
        log "skip $d — parent $parent does not exist"
    fi
done
(( ${#present_disks[@]} >= 3 )) || die "need at least 3 present data dirs (RF=3); have ${#present_disks[@]}"
N_EN=${#present_disks[@]}

mkdir -p "$WORK" "$LOG_DIR" "$WORK/ps"

# ----- etcd state: interactive wipe-vs-preserve -----
#
# bootstrap accumulates partition/extent IDs in etcd; a stale leftover from
# a wedged stop / SIGKILL'd etcd / surviving bind-mount can make the next
# bootstrap mint new ids on top of dangling references → PS opens the
# partition, commit_length finds 0/3 EN replicas have the extent, retries
# forever (witnessed: `part_id=17 stream_id=11 extent 12 0/3 committed
# members reachable`).
#
# But the OPPOSITE failure mode is what just bit us: wiping etcd while the
# data dirs still hold the previous cluster's `cluster_id` sentinel makes
# `autumn-op format` bail with "already formatted for cluster X". So we
# don't auto-wipe anymore — the operator decides per-run. Default = preserve.
#
# Stdin not a tty (CI / piped) → read returns empty → default N → preserve.
# To wipe non-interactively: `echo y | ./start.sh`.

etcd_is_fresh=1
if [[ -d "$WORK/etcd" ]] && [[ -n "$(ls -A "$WORK/etcd" 2>/dev/null)" ]]; then
    echo "[start] found existing etcd data at $WORK/etcd"
    echo "[start]   preserve → reuse cluster_id; existing data dirs keep working"
    echo "[start]   wipe     → fresh cluster (use after a wedged stop / SIGKILL'd etcd)"
    read -r -p "[start] wipe etcd? [y/N]: " ans
    case "${ans,,}" in
        y|yes)
            log "wiping $WORK/etcd"
            rm -rf "$WORK/etcd"
            ;;
        *)
            log "preserving etcd data"
            etcd_is_fresh=0
            ;;
    esac
fi
mkdir -p "$WORK/etcd"

# ----- data-dir state: prompt only when etcd is fresh and dirs are stale -----
#
# If etcd is fresh (just wiped or never existed) AND any data dir has a
# `cluster_id` sentinel from a previous cluster, autumn-op format will
# refuse on cluster_id mismatch. Surface that BEFORE format runs so the
# operator isn't left staring at a generic "format node1 failed".
if (( etcd_is_fresh == 1 )); then
    stale_dirs=()
    for d in "${present_disks[@]}"; do
        [[ -f "$d/cluster_id" ]] && stale_dirs+=("$d")
    done
    if (( ${#stale_dirs[@]} > 0 )); then
        echo "[start] etcd is empty but these data dirs carry sentinels from a previous cluster:"
        for d in "${stale_dirs[@]}"; do
            echo "[start]   $d (cluster_id=$(cat "$d/cluster_id"))"
        done
        echo "[start] autumn-op format will refuse on cluster_id mismatch."
        read -r -p "[start] wipe these data dirs to start fresh? THIS DESTROYS DATA. [y/N]: " ans
        case "${ans,,}" in
            y|yes)
                for d in "${stale_dirs[@]}"; do
                    log "wiping $d"
                    rm -rf "$d"
                    mkdir -p "$d"
                done
                ;;
            *)
                die "data dirs hold a stale cluster_id; format will fail. \
re-run and choose wipe, or 'preserve' etcd instead so cluster_id stays stable."
                ;;
        esac
    fi
fi

# ----- bootstrap presplit: how many partitions, which shape -----
#
# `autumn-op bootstrap --presplit N:<shape>` creates N partitions up
# front. Each partition gets its own PS thread (P-log + P-bulk), so
# more partitions = more parallel write/read capacity without waiting
# for auto-split's QPS threshold (~15K, see project_partition_qps_ceiling).
#
# Two shapes:
#   hexstring — N equal slices of the ASCII hex u32 keyspace
#               ([\"40000000\", \"80000000\", ...]). Good for raw KV /
#               kvcache / mixed workloads — splits bytes 0x30-0x66.
#   fuse      — splits on BOTTOM byte of `ino` in the [0x03][ino BE]
#               file-extent keyspace, so sequential fuse inodes
#               (1, 2, 3, ...) round-robin across N partitions from
#               the very first file. Pick this when fuse is the
#               dominant workload.
#
# Only ask when bootstrap is about to run (etcd was just wiped OR
# never existed); on a preserved-etcd restart there's nothing to
# bootstrap and the presplit choice is already baked in.

PRESPLIT_COUNT=1
PRESPLIT_SHAPE=""
if (( etcd_is_fresh == 1 )); then
    echo "[start] partition presplit:"
    echo "[start]   1 → single partition; auto-split kicks in when QPS > ~15K"
    echo "[start]   N → pre-split N ways (each partition = its own PS thread)"
    read -r -p "[start] partition count [default 1, max 256]: " ans
    ans="${ans:-1}"
    if ! [[ "$ans" =~ ^[0-9]+$ ]] || (( ans < 1 || ans > 256 )); then
        die "invalid partition count '$ans' — must be an integer in [1, 256]"
    fi
    PRESPLIT_COUNT=$ans

    if (( PRESPLIT_COUNT > 1 )); then
        echo "[start] presplit shape:"
        echo "[start]   hexstring → general workloads (raw KV / kvcache / mixed)"
        echo "[start]   fuse      → fuse-dominant (round-robin by inode bottom byte)"
        read -r -p "[start] use fuse-aware split? [y/N]: " ans
        case "${ans,,}" in
            y|yes)
                PRESPLIT_SHAPE="fuse"
                ;;
            *)
                PRESPLIT_SHAPE="hexstring"
                ;;
        esac
    fi
fi

# ============================================================
# 1. etcd
# ============================================================

log "1/5  etcd"
start_proc etcd etcd \
    --data-dir "$WORK/etcd" \
    --listen-client-urls "http://127.0.0.1:2379" \
    --advertise-client-urls "http://127.0.0.1:2379" \
    --listen-peer-urls "http://127.0.0.1:2380" \
    --initial-advertise-peer-urls "http://127.0.0.1:2380" \
    --initial-cluster "default=http://127.0.0.1:2380"
# etcd binds 127.0.0.1 above (local-only; only the same-host manager talks to
# it), so probe 127.0.0.1 — NOT $BIND_HOST, which may be a RoCE address etcd
# isn't listening on.
wait_port 2379 etcd 127.0.0.1

# ============================================================
# 2. manager
# ============================================================

log "2/5  manager on $MANAGER_ADDR"
# --transport MUST match the EN/PS/client transport. The manager is pure
# control-plane (small RPCs, no RDMA benefit), BUT the transport is
# process-global / single-pick: a `--transport ucx` client uses UCX for ALL
# connections including the manager link, so a UCX client can only reach a UCX
# manager. Omitting this (pre-fix bug) left the manager on default TCP while
# ucx clients failed with "cannot connect to any manager". cluster.sh passes
# it; start.sh now matches.
start_proc manager "$MANAGER" \
    --port 9001 \
    --etcd 127.0.0.1:2379 \
    --listen "$BIND_HOST" \
    --transport "$TRANSPORT"
wait_port 9001 manager

# ============================================================
# 3. extent-nodes (format + launch)
# ============================================================

# Wait until the manager actually wins etcd leadership before proceeding.
# A blind `sleep 10` races the election: not-leader-yet → bootstrap later
# fails with `create stream: not leader`; won-early → wasted time.
# `policy-candidates` is leader-gated (CODE_NOT_LEADER on a follower) and is
# only answered AFTER replay_from_etcd completes, so exit 0 == "leader, ready".
printf '[start] waiting for manager leadership'
for ((i=1; i<=60; i++)); do
    if "$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" policy-candidates >/dev/null 2>&1; then
        echo " — ready (${i}s)"
        break
    fi
    printf '.'
    sleep 1
    (( i == 60 )) && die "manager did not become leader within 60s — check $LOG_DIR/manager.log and that etcd is listening on :2379"
done

log "3/5  $N_EN extent-nodes"
for ((i=0; i<N_EN; i++)); do
    idx=$((i + 1))
    port=$((EN_BASE_PORT + i))
    disk="${present_disks[$i]}"
    log "  node$idx port=$port disk=$disk — format"
    "$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" format \
        --listen ":$port" --advertise "$BIND_HOST:$port" "$disk" \
        >"$LOG_DIR/node$idx-format.log" 2>&1 \
        || die "format node$idx failed — see $LOG_DIR/node$idx-format.log"
    # autumn-op format briefly binds $port and $(port+1000) to register with
    # the manager. The sockets may sit in TIME_WAIT for up to ~60s after
    # format exits — without a wait, the EN binary's listener bind raises
    # "Address already in use" on the control port. 2s is enough on Linux
    # with default tcp_tw_recycle behavior; tune if your kernel is stricter.
    sleep 2
    log "  node$idx — launch"
    # CRITICAL: --cpuset limits the shard count (F196: shards == cpuset_len).
    # Without it the EN auto-detects all CPU cores and tries to bind one
    # control port per shard at `port+1000 + shard_idx*shard_stride`. With 5
    # ENs × 192 shards each, the per-EN port ranges overlap massively and
    # most control listeners die with `Address already in use` → manager's
    # df probe can't reach them → all disks show `online=false`. One shard
    # per EN (cpuset of size 1) is plenty for dev. Use a distinct core per
    # EN so the kernel scheduler doesn't pile them on the same core.
    en_cpu=$((EN_CPU_BASE + i))
    start_proc "node$idx" "$NODE" \
        --port "$port" --data "$disk" \
        --manager "$MANAGER_ADDR" \
        --listen "$BIND_HOST" \
        --transport "$TRANSPORT" \
        --cpuset "$en_cpu"
    wait_port "$port" "node$idx"
done

# ============================================================
# 4. partition-server
# ============================================================

# Wait for manager to see all ENs as healthy (df health check fires ~1Hz).
# Without this, bootstrap can pick a "registered but no df yet" EN and
# allocate extents on it before it's truly serving — PS later opens the
# partition, commit_length probes the assigned EN, gets `0/3 committed
# members reachable`, and retries forever.
# Wait until the manager reports all ENs as df-online (NodeAutoState::Online),
# not merely registered. A freshly-registered EN sits in `Suspend` until its
# first successful df probe; bootstrapping against a not-yet-df'd EN makes the
# PS later wedge on `0/3 committed members reachable`. (Blind `sleep 10` raced
# this — list-nodes col 3 is the AUTO state.)
log "  waiting for $N_EN EN(s) to be df-online in manager..."
for ((i=1; i<=60; i++)); do
    online=$("$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" list-nodes 2>/dev/null \
                 | awk '$3=="Online"{c++} END{print c+0}')
    (( online >= N_EN )) && { log "  $online/$N_EN EN(s) df-online (${i}s)"; break; }
    sleep 1
    (( i == 60 )) && die "only ${online:-0}/$N_EN EN(s) df-online after 60s — check $LOG_DIR/node*.log + manager df health"
done

log "4/5  partition-server on $BIND_HOST:$PS_PORT"
start_proc ps "$PS" \
    --psid 1 \
    --port "$PS_PORT" \
    --manager "$MANAGER_ADDR" \
    --listen "$BIND_HOST" \
    --advertise "$BIND_HOST:$PS_PORT" \
    --transport "$TRANSPORT"
sleep 3  # let PS register before bootstrap

# ============================================================
# 5. bootstrap
# ============================================================

if (( PRESPLIT_COUNT > 1 )); then
    log "5/5  bootstrap (empty cluster, presplit ${PRESPLIT_COUNT}:${PRESPLIT_SHAPE})"
    "$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" bootstrap \
        --presplit "${PRESPLIT_COUNT}:${PRESPLIT_SHAPE}" \
        >"$LOG_DIR/bootstrap.log" 2>&1 \
        || die "bootstrap failed — see $LOG_DIR/bootstrap.log"
else
    log "5/5  bootstrap (empty cluster, single partition)"
    "$AO" --manager "$MANAGER_ADDR" --transport "$TRANSPORT" bootstrap \
        >"$LOG_DIR/bootstrap.log" 2>&1 \
        || die "bootstrap failed — see $LOG_DIR/bootstrap.log"
fi

# ============================================================
# Summary
# ============================================================

cat <<EOF

[start] ✓ cluster ready
  manager  : $MANAGER_ADDR
  ps       : $BIND_HOST:$PS_PORT
  ens      : ${N_EN} nodes  ($(IFS=,; echo "${present_disks[*]}"))
  etcd     : 127.0.0.1:2379  (data: $WORK/etcd)
  logs     : $LOG_DIR

Use:  (--transport MUST match the cluster; clients default to tcp and a tcp
       client cannot reach a ucx manager)
  AC="$BIN/autumn-client --manager $MANAGER_ADDR --transport $TRANSPORT"
  AO="$BIN/autumn-op --manager $MANAGER_ADDR --transport $TRANSPORT"
  \$AO info
  echo hello | \$AC put mykey /dev/stdin
  \$AC get mykey

Stop: pkill -f autumn-  ;  pkill etcd
EOF
