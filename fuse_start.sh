#!/usr/bin/env bash
# fuse_start.sh — mount the autumn-fuse filesystem against a running cluster.
#
# Env (all overridable):
#   MANAGER     manager addr (default 127.0.0.1:9001;
#               cross-host / RoCE: MANAGER='[fdbd:dc62:3:302::14]:9001')
#   TRANSPORT   tcp | ucx  — MUST match the cluster's transport. The fuse daemon
#               is a DATA-PLANE client (ClusterClient reads/writes file extents
#               over this transport), and the transport is process-global, so a
#               tcp fuse cannot reach a ucx cluster (and vice-versa).
#   MOUNTPOINT  default /mnt/dongmao-share
#   LOG_DIR     default /var/lib/autumn-rs/logs   (match the cluster's WORK/logs)
#   BIN         default ./target/release
#   UCX_NET_DEVICES  default mlx5_1:1 (only when ucx; verify per host with
#                    scripts/check_roce.sh --listen-candidates)
#
# Examples:
#   ./fuse_start.sh                                            # local tcp cluster
#   MANAGER='[fdbd:dc62:3:302::14]:9001' TRANSPORT=ucx \
#     LOG_DIR=/var/lib/autumn-rs-d02/logs ./fuse_start.sh      # cross-host ucx
set -euo pipefail

MANAGER="${MANAGER:-127.0.0.1:9001}"
TRANSPORT="${TRANSPORT:-tcp}"
MOUNTPOINT="${MOUNTPOINT:-/mnt/dongmao-share}"
LOG_DIR="${LOG_DIR:-/var/lib/autumn-rs/logs}"
BIN="${BIN:-./target/release}"

# UCX env — same rationale as start.sh: positive TLS list (union of cross-host
# RoCE + same-host shm + tcp fallback; NEVER use `^` negation), pinned RoCE
# device (auto-select hangs with many devices), raised memlock for ibv_reg_mr.
# UCX_* are read by the UCX C library directly (not autumn rust) — script is the
# right place per the "config via flags not env in rust" rule.
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
mkdir -p "$MOUNTPOINT" "$LOG_DIR"

echo "fuse_start: mounting $MOUNTPOINT (manager=$MANAGER transport=$TRANSPORT) → $LOG_DIR/fuse.log"
nohup setsid "$BIN/autumn-fuse" \
     --manager "$MANAGER" \
     --mountpoint "$MOUNTPOINT" \
     --transport "$TRANSPORT" \
     > "$LOG_DIR/fuse.log" 2>&1 &
echo "fuse_start: started (pid $!).  Unmount: fusermount -u $MOUNTPOINT  (or pkill -f autumn-fuse)"
