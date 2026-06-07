#!/usr/bin/env bash
# stop.sh — graceful shutdown for start.sh's cluster.
#
# Reverse order: PS → ENs → manager → etcd.
# SIGTERM first (autumn-ps drains imm to row_stream — can take seconds),
# SIGKILL after STOP_TIMEOUT_S seconds.
#
# Usage:
#   ./stop.sh             # stop processes only
#   ./stop.sh --wipe      # also delete $WORK (etcd data + logs) AND DATA_DIRS

set -euo pipefail

# ============================================================
# Config — keep in sync with start.sh
# ============================================================

DATA_DIRS=(
    /data03/autumn-rs
    /data05/autumn-rs
    /data06/autumn-rs
    /data07/autumn-rs
    /data08/autumn-rs
)

REPO="${REPO:-$(cd "$(dirname "$0")" && pwd)}"
BIN="${BIN:-$REPO/target/release}"
WORK="${WORK:-/var/lib/autumn-rs}"
STOP_TIMEOUT_S="${STOP_TIMEOUT_S:-60}"

# ============================================================
# Helpers
# ============================================================

log() { printf '[stop] %s\n' "$*" >&2; }

# Kill all processes matching the given pattern; wait up to STOP_TIMEOUT_S
# for them to exit on SIGTERM, then SIGKILL stragglers.
kill_pattern() {
    local name="$1" pattern="$2"
    mapfile -t pids < <(pgrep -f "$pattern" || true)
    if (( ${#pids[@]} == 0 )); then
        return 0
    fi
    log "stopping $name (pids: ${pids[*]})"
    kill "${pids[@]}" 2>/dev/null || true
    local deadline=$(( $(date +%s) + STOP_TIMEOUT_S ))
    while (( $(date +%s) < deadline )); do
        mapfile -t alive < <(pgrep -f "$pattern" || true)
        (( ${#alive[@]} == 0 )) && { log "stopped $name"; return 0; }
        sleep 0.2
    done
    log "$name did not exit in ${STOP_TIMEOUT_S}s; SIGKILL"
    pkill -9 -f "$pattern" 2>/dev/null || true
    sleep 0.3
}

# ============================================================
# Args
# ============================================================

WIPE=0
for arg in "$@"; do
    case "$arg" in
        --wipe|-w) WIPE=1 ;;
        -h|--help)
            sed -n '2,12p' "$0"; exit 0 ;;
        *) log "unknown arg: $arg"; exit 2 ;;
    esac
done

# ============================================================
# Stop in reverse order
# ============================================================

# Scope each pattern to our $BIN path so we never touch unrelated processes
# on the host (e.g. another tenant's etcd).
kill_pattern "ps"      "$BIN/autumn-ps( |$)"
kill_pattern "ens"     "$BIN/autumn-extent-node( |$)"
kill_pattern "manager" "$BIN/autumn-manager-server( |$)"
kill_pattern "etcd"    "etcd --data-dir $WORK/etcd"

# Final sweep: anything left from this $BIN tree.
pkill -9 -f "$BIN/autumn-" 2>/dev/null || true

log "all processes stopped"

# ============================================================
# Optional wipe
# ============================================================

if (( WIPE )); then
    log "wiping $WORK"
    rm -rf "$WORK"
    for d in "${DATA_DIRS[@]}"; do
        if [[ -d "$d" ]]; then
            log "wiping $d"
            rm -rf "$d"
        fi
    done
    log "wipe done"
fi
