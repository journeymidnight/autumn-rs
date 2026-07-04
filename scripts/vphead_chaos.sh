#!/usr/bin/env bash
# vphead_chaos.sh — multi-seed chaos over the vp_head correctness thread.
#
# Drives the in-process `system_chaos` harness (real subprocess ENs + etcd +
# toxiproxy, real kill/fence/split/merge/compact/GC) across SEVERAL random seeds,
# with the nemesis action set focused on the ops that stress the SSTable
# `vp_head` (recovery replay-start) — the F-COMPACT/FLUSH-VPHEAD fixes:
#
#   split   — CoW-shares log extents across children; recovery must replay each
#             from the correct first-occurrence vp_head.
#   merge   — splices log extents; vp_head ordering is load-bearing.
#   compact — stamps output vp_head = MAX(input vp_heads) (must be a true content
#             boundary, else recovery skips the un-flushed tail).
#   forcegc — bypasses the discard-ratio gate and asks the PS to punch SPECIFIC
#             sealed extents; a wrong vp_head lets it punch a live one -> loss.
#             The replay-floor guard MUST relocate live VPs + skip protected
#             extents. This is the maximal stress on the whole thread.
#   +gc/flush/kill — auto GC, flush, and EN kill/restart (recovery is where the
#             recovery TAIL-seed matters).
#
# After each seed the harness verifies EVERY acked put byte-exact
# (verify_per_key), per-partition range order/phantom (verify_per_partition_range),
# post-settle write-liveness, and manager extent-accounting. ANY data loss on ANY
# seed fails the whole run.
#
# Usage:
#   ./scripts/vphead_chaos.sh                    # 6 default seeds
#   VPHEAD_SEEDS="1 2 42 777" ./scripts/vphead_chaos.sh
#   AUTUMN_CHAOS_ACTIONS=split,merge,compact,forcegc \
#     AUTUMN_CHAOS_DURATION_SECS=60 ./scripts/vphead_chaos.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

SEEDS="${VPHEAD_SEEDS:-1 7 42 137 2024 88888}"
# The vp_head-stressing set + realistic fault injection. Override to bisect.
export AUTUMN_CHAOS_ACTIONS="${AUTUMN_CHAOS_ACTIONS:-split,merge,compact,forcegc,gc,flush,kill}"
export AUTUMN_CHAOS_DURATION_SECS="${AUTUMN_CHAOS_DURATION_SECS:-45}"
export AUTUMN_CHAOS_NEMESIS_INTERVAL_MS="${AUTUMN_CHAOS_NEMESIS_INTERVAL_MS:-2000}"
export AUTUMN_CHAOS_NUM_ENS="${AUTUMN_CHAOS_NUM_ENS:-5}"

say()  { echo "[vphead $(date +%H:%M:%S)] $*"; }

command -v etcd >/dev/null            || { say "FATAL: etcd not on PATH"; exit 2; }
command -v toxiproxy-server >/dev/null || { say "FATAL: toxiproxy-server not on PATH"; exit 2; }

# `system_chaos::binary_path` resolves target/debug — build the workspace so the
# EN / autumn-op subprocesses are the CURRENT code (a stale build silently tests
# old behavior).
say "building workspace (debug) so subprocess binaries match this tree"
cargo build --workspace 2>&1 | tail -2 || { say "FATAL: build failed"; exit 2; }
# Compile the chaos test binary once up front (kept warm across seeds).
cargo test -p autumn-manager --test system_chaos --no-run 2>&1 | tail -1 || {
    say "FATAL: chaos test compile failed"; exit 2; }

drain_ports() {
    # Ephemeral-port TIME_WAIT piles up across seeds (each seed opens hundreds of
    # loopback sockets via toxiproxy). Gate the next seed on it draining, or a
    # late seed sees spurious not_found from bind/connect failures.
    for _ in $(seq 1 30); do
        tw=$(ss -tan state time-wait 2>/dev/null | grep -c '127.0.0.1') || tw=0
        [ "${tw:-0}" -lt 4000 ] && return 0
        sleep 2
    done
    say "WARN: TIME_WAIT still high (${tw}) — proceeding anyway"
}

LOG_DIR="$(mktemp -d /tmp/vphead_chaos.XXXXXX)"
say "actions=$AUTUMN_CHAOS_ACTIONS duration=${AUTUMN_CHAOS_DURATION_SECS}s seeds='$SEEDS' logs=$LOG_DIR"

FAILED_SEEDS=""
PASSED=0
for seed in $SEEDS; do
    drain_ports
    say "=== seed $seed ==="
    log="$LOG_DIR/seed_$seed.log"
    if AUTUMN_CHAOS_SEED="$seed" \
       cargo test -p autumn-manager --test system_chaos \
           chaos_real_kill_split_merge_ec_fence_no_data_loss \
           -- --ignored --nocapture --exact >"$log" 2>&1; then
        # Surface what actually got exercised (esp. forcegc) + the acked count.
        acked=$(grep -oE 'acked=[0-9]+|writes_acked[^0-9]*[0-9]+' "$log" | tail -1)
        fg=$(grep -c 'ForceGc OK' "$log" 2>/dev/null || echo 0)
        sp=$(grep -c 'Split OK' "$log" 2>/dev/null || echo 0)
        mg=$(grep -c 'Merge OK' "$log" 2>/dev/null || echo 0)
        cp=$(grep -c 'Compact OK' "$log" 2>/dev/null || echo 0)
        say "seed $seed PASS  (forcegc=$fg split=$sp merge=$mg compact=$cp $acked)"
        PASSED=$((PASSED + 1))
    else
        say "seed $seed FAIL — see $log"
        grep -iE 'MISMATCH|not_found|WRITE WEDGE|ORPHAN|UNDER-COUNT|OVER-COUNT|data.loss|panicked' "$log" | head -8
        FAILED_SEEDS="$FAILED_SEEDS $seed"
    fi
done

echo
if [ -n "$FAILED_SEEDS" ]; then
    say "RESULT: FAIL — seeds failed:$FAILED_SEEDS (passed $PASSED). Logs: $LOG_DIR"
    exit 1
fi
say "RESULT: PASS — all seeds green ($PASSED). No data loss across split/merge/compact/forcegc. Logs: $LOG_DIR"
