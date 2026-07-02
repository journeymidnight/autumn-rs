#!/usr/bin/env bash
# fuse_start.sh — mount the autumn-fuse filesystem against a RUNNING cluster.
#
# This is a CONSUMER-side tool, NOT a cluster launcher. autumn-fuse is one of the
# three client interfaces (fuse / kvcache / client) built on top of the partition
# layer; it runs where the APPLICATION runs (e.g. the sglang/vLLM inference / GPU
# nodes), mounts a POSIX filesystem, and reads/writes through the cluster's
# manager. The storage cluster is deployed separately — bare-metal via
# deploy/baremetal/autumn-deploy, Kubernetes via deploy/k8s/. (Inside k8s, mount
# by running autumn-fuse as a privileged per-node DaemonSet; this script is the
# bare-metal equivalent.)
#
# Knobs — all standalone, no coupling to any cluster-launch script:
#   MANAGER    manager address        default 127.0.0.1:9001 (host:port; bracket IPv6)
#   TRANSPORT  tcp | ucx — MUST match the cluster's transport. The fuse daemon is
#              a DATA-PLANE client (ClusterClient reads/writes extents over this
#              transport), process-global, so a tcp fuse can't reach a ucx cluster.
#   MOUNTPOINT mount path             default /mnt/dongmao-share
#   LOG_DIR    daemon log dir         default /tmp/autumn-fuse
#   BIN        binary dir             default ./target/release
#   UCX_NET_DEVICES  RoCE device (ucx only)  default mlx5_1:1
#                    (verify with scripts/check_roce.sh --listen-candidates)
#
# Examples:
#   ./fuse_start.sh                                                    # local tcp cluster
#   MANAGER='[fdbd:dc62:3:302::14]:9001' TRANSPORT=ucx ./fuse_start.sh # remote ucx cluster
set -euo pipefail

MANAGER="${MANAGER:-127.0.0.1:9001}"
TRANSPORT="${TRANSPORT:-tcp}"
MOUNTPOINT="${MOUNTPOINT:-/mnt/dongmao-share}"
LOG_DIR="${LOG_DIR:-/tmp/autumn-fuse}"
BIN="${BIN:-./target/release}"

# UCX env — the UCX C library reads UCX_* directly (not autumn rust), so setting
# them here is the right layer (config via flags, not env, in rust). Positive TLS
# list (cross-host RoCE + same-host shm + tcp fallback; NEVER `^` negation),
# pinned RoCE device (auto-select hangs with many devices), raised memlock for
# ibv_reg_mr.
if [[ "$TRANSPORT" == "ucx" ]]; then
    export UCX_TLS="${UCX_TLS:-rc_mlx5,ud_mlx5,posix,cma,tcp,self}"
    export UCX_NET_DEVICES="${UCX_NET_DEVICES:-mlx5_1:1}"
    ulimit -l unlimited 2>/dev/null || true
fi

if [[ ! -x "$BIN/autumn-fuse" ]]; then
    echo "fuse_start: $BIN/autumn-fuse missing — build it first:" >&2
    if [[ "$TRANSPORT" == "ucx" ]]; then
        echo "  cargo build --release -p autumn-fuse --features ucx" >&2
    else
        echo "  cargo build --release -p autumn-fuse" >&2
    fi
    exit 1
fi
# Refuse if a healthy fuse is already mounted here; clear a STALE one (a dead/
# killed daemon leaves "Transport endpoint is not connected", and the new mount
# — even the mkdir below — fails on it). Detect stale via /proc/mounts (it's
# still listed even when dead) OR an ENOTCONN on `ls` (don't use `[[ -e ]]` /
# `stat`: those THEMSELVES error with ENOTCONN on a stale fuse, so they never
# trigger the cleanup — the bug that left mkdir failing).
if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
    echo "fuse_start: $MOUNTPOINT is already mounted — unmount first (umount -l $MOUNTPOINT)" >&2
    exit 1
fi
if grep -qF " $MOUNTPOINT " /proc/mounts 2>/dev/null || ! ls "$MOUNTPOINT" >/dev/null 2>&1; then
    echo "fuse_start: clearing stale mount at $MOUNTPOINT"
    umount -l "$MOUNTPOINT" 2>/dev/null || fusermount3 -u "$MOUNTPOINT" 2>/dev/null || true
fi
mkdir -p "$MOUNTPOINT" "$LOG_DIR"

echo "fuse_start: mounting $MOUNTPOINT (manager=$MANAGER transport=$TRANSPORT) → $LOG_DIR/fuse.log"
nohup setsid "$BIN/autumn-fuse" \
     --manager "$MANAGER" \
     --mountpoint "$MOUNTPOINT" \
     --transport "$TRANSPORT" \
     > "$LOG_DIR/fuse.log" 2>&1 &
PID=$!

# Verify the daemon ACTUALLY mounted. A manager-unreachable / transport-mismatch
# (e.g. tcp client vs ucx cluster) / stale-mountpoint failure makes autumn-fuse
# exit within ~1 s; without this check the script would falsely report success.
for _ in $(seq 1 20); do
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "fuse_start: ERROR — autumn-fuse exited during mount (check transport / manager addr). Last log:" >&2
        tail -15 "$LOG_DIR/fuse.log" >&2
        exit 1
    fi
    if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
        echo "fuse_start: ✓ mounted (pid $PID).  Unmount: umount -l $MOUNTPOINT  (or pkill -f autumn-fuse)"
        exit 0
    fi
    sleep 0.5
done
echo "fuse_start: ERROR — $MOUNTPOINT not mounted within 10 s (daemon pid $PID alive but no mount). Last log:" >&2
tail -15 "$LOG_DIR/fuse.log" >&2
exit 1
