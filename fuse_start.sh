#!/usr/bin/env bash
# fuse_start.sh — mount the autumn-fuse filesystem against a running cluster.
#
# Accepts the SAME env vars as start.sh, so one set drives both the cluster and
# the mount:
#   BIND_HOST  → MANAGER  ($BIND_HOST:9001)   default 127.0.0.1
#   WORK       → LOG_DIR  ($WORK/logs)        default /var/lib/autumn-rs
#   TRANSPORT  tcp | ucx — MUST match the cluster's transport. The fuse daemon is
#              a DATA-PLANE client (ClusterClient reads/writes extents over this
#              transport), process-global, so a tcp fuse can't reach a ucx cluster.
# Explicit overrides still win:
#   MANAGER    full manager addr (overrides BIND_HOST-derived)
#   LOG_DIR    full log dir       (overrides WORK-derived)
#   MOUNTPOINT default /mnt/dongmao-share
#   BIN        default ./target/release
#   UCX_NET_DEVICES  default mlx5_1:1 (only when ucx; verify with
#                    scripts/check_roce.sh --listen-candidates)
#
# Examples:
#   ./fuse_start.sh                                            # local tcp cluster
#   WORK=/var/lib/autumn-rs-d02 BIND_HOST='[fdbd:dc62:3:302::14]' \
#     TRANSPORT=ucx ./fuse_start.sh                            # matches the same
#                                                              # WORK/BIND_HOST/TRANSPORT
#                                                              # you gave start.sh
set -euo pipefail

BIND_HOST="${BIND_HOST:-127.0.0.1}"
WORK="${WORK:-/var/lib/autumn-rs}"
MANAGER="${MANAGER:-${BIND_HOST}:9001}"
TRANSPORT="${TRANSPORT:-tcp}"
MOUNTPOINT="${MOUNTPOINT:-/mnt/dongmao-share}"
LOG_DIR="${LOG_DIR:-${WORK}/logs}"
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
