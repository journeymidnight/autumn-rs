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
# Refuse if a healthy fuse is already mounted here; clear a STALE one (a
# previously-killed daemon leaves "Transport endpoint is not connected", and
# the new mount fails on it — `ls` on the dir errors when it's stale).
if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
    echo "fuse_start: $MOUNTPOINT is already mounted — unmount first (umount -l $MOUNTPOINT)" >&2
    exit 1
elif ! ls "$MOUNTPOINT" >/dev/null 2>&1 && [[ -e "$MOUNTPOINT" ]]; then
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
