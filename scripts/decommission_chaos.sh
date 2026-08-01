#!/usr/bin/env bash
# decommission_chaos.sh — full-coverage chaos + terminal node DECOMMISSION.
#
# Two gaps this closes over vphead_chaos.sh (which runs a focused vp_head subset):
#
#  1. FULL nemesis action set. vphead runs split,merge,compact,forcegc,gc,flush,kill.
#     This adds the ones the scripts never exercised even though the harness has
#     always supported them:
#       fence      — MSG_FENCE_NODE / clear (owner_epoch + drain)
#       killfence  — SIGKILL then fence the dead node (recovery dispatch path)
#       ec         — MSG_UPDATE_STREAM_EC convert-under-load
#       partition  — toxiproxy link disable (network partition)
#       latency    — toxiproxy latency toxic (slow-link timeouts)
#
#  2. Node DECOMMISSION (`AUTUMN_CHAOS_DECOMMISSION=1`). After the nemesis loop
#     stops and the cluster heals, ONE extent-node is permanently removed the
#     HDFS way: fence -> wait for the drain + fenced_only recovery to
#     relocate every extent off it -> MSG_REMOVE_NODE (refuses until fully
#     drained, tombstones the address). The existing per-key / range / accounting
#     verify then proves NO DATA LOSS with the node gone. This is the first chaos
#     coverage of the MSG_REMOVE_NODE decommission primitive.
#
# Non-reversible removal is a TERMINAL one-shot (see run_terminal_decommission),
# NOT a per-cycle nemesis action — every nemesis action must be reversible.
#
# ┌─ KNOWN OPEN BUG — live-node decommission STALLS (found by THIS scenario) ────┐
# │ Decommissioning a LIVE fenced node HANGS: `AUTUMN_CHAOS_DECOMMISSION=1` (the │
# │ default here) is EXPECTED TO FAIL "DECOMMISSION FAILED … did NOT drain".     │
# │ Root cause (EN-instrumented): the fenced-slot recovery dispatch fires + holds │
# │ a Recovery marker, but the target EN's `run_recovery_task` HANGS in the copy  │
# │ path (a compiled-in 30 s read timeout NEVER fires → the EN's single-threaded  │
# │ compio runtime is starved). Frozen marker → re-dispatch blocked → `remove`   │
# │ never unblocks. Four correct latent-bug fixes landed (recovery read timeout, │
# │ sealed-empty short-circuit, 120 s recovery-marker release, NOT_LEADER retry)  │
# │ but NONE resolve the hang — it needs focused recovery-runtime investigation.  │
# │ CONTRAST: AUTUMN_CHAOS_DECOMMISSION_KILL=1 (stop the node first) drains fast. │
# │ Tracked in feature_list.md (passes:false / OPEN).                             │
# └─────────────────────────────────────────────────────────────────────────────┘
#
# Usage:
#   ./scripts/decommission_chaos.sh                              # live-node decommission
#   AUTUMN_CHAOS_DECOMMISSION_KILL=1 ./scripts/decommission_chaos.sh  # stop-then-remove
#   AUTUMN_CHAOS_DECOMMISSION=0 ./scripts/decommission_chaos.sh   # full nemesis set, no remove
#   DECOMMISSION_SEEDS="1 2 42" ./scripts/decommission_chaos.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

SEEDS="${DECOMMISSION_SEEDS:-1 42 2024}"
# FULL action set — every nemesis the harness supports (ALL_ACTIONS).
export AUTUMN_CHAOS_ACTIONS="${AUTUMN_CHAOS_ACTIONS:-split,merge,ec,fence,flush,compact,gc,forcegc,kill,killfence,partition,latency}"
export AUTUMN_CHAOS_DECOMMISSION="${AUTUMN_CHAOS_DECOMMISSION:-1}"
# 1 = SIGKILL the fenced node before draining (stop-then-remove flow). Default 0
# runs the live-node path (now drains, see note above).
export AUTUMN_CHAOS_DECOMMISSION_KILL="${AUTUMN_CHAOS_DECOMMISSION_KILL:-0}"
# release a stuck Recovery marker fast so a live-fenced-node
# decommission re-dispatches + drains within the test window (product default is
# 120 s; 60 s here just speeds up CI — the recovery-specific threshold picks the
# min of this and AUTUMN_MGR_RECOVERY_INFLIGHT_STALE_SECS).
export AUTUMN_MGR_INFLIGHT_STALE_THRESHOLD_SECS="${AUTUMN_MGR_INFLIGHT_STALE_THRESHOLD_SECS:-60}"
export AUTUMN_MGR_INFLIGHT_SWEEP_INTERVAL_SECS="${AUTUMN_MGR_INFLIGHT_SWEEP_INTERVAL_SECS:-5}"
export AUTUMN_CHAOS_DURATION_SECS="${AUTUMN_CHAOS_DURATION_SECS:-45}"
export AUTUMN_CHAOS_NEMESIS_INTERVAL_MS="${AUTUMN_CHAOS_NEMESIS_INTERVAL_MS:-2000}"
# 6 ENs: nemesis kills/fences need healthy_count > K+M; the terminal decommission
# removes one and must still leave >= K+M for the read verify.
export AUTUMN_CHAOS_NUM_ENS="${AUTUMN_CHAOS_NUM_ENS:-6}"

say()  { echo "[decommission $(date +%H:%M:%S)] $*"; }

command -v etcd >/dev/null             || { say "FATAL: etcd not on PATH"; exit 2; }
command -v toxiproxy-server >/dev/null || { say "FATAL: toxiproxy-server not on PATH"; exit 2; }

# `system_chaos::binary_path` resolves target/debug — build the workspace so the
# EN / autumn-op subprocesses are the CURRENT code (a stale build silently tests
# old behavior).
say "building workspace (debug) so subprocess binaries match this tree"
cargo build --workspace 2>&1 | tail -2 || { say "FATAL: build failed"; exit 2; }
cargo test -p autumn-manager --test system_chaos --no-run 2>&1 | tail -1 || {
    say "FATAL: chaos test compile failed"; exit 2; }

drain_ports() {
    for _ in $(seq 1 30); do
        tw=$(ss -tan state time-wait 2>/dev/null | grep -c '127.0.0.1') || tw=0
        [ "${tw:-0}" -lt 4000 ] && return 0
        sleep 2
    done
    say "WARN: TIME_WAIT still high (${tw}) — proceeding anyway"
}

LOG_DIR="$(mktemp -d /tmp/decommission_chaos.XXXXXX)"
say "actions=$AUTUMN_CHAOS_ACTIONS decommission=$AUTUMN_CHAOS_DECOMMISSION num_ens=$AUTUMN_CHAOS_NUM_ENS duration=${AUTUMN_CHAOS_DURATION_SECS}s seeds='$SEEDS' logs=$LOG_DIR"

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
        acked=$(grep -oE 'acked=[0-9]+|writes_acked[^0-9]*[0-9]+' "$log" | tail -1)
        dec=$(grep -E 'chaos: decommission —' "$log" | tail -1 | sed 's/.*decommission —/decommission:/')
        fn=$(grep -c 'FenceUnfence OK' "$log" 2>/dev/null || echo 0)
        kf=$(grep -c 'KillThenFence OK' "$log" 2>/dev/null || echo 0)
        np=$(grep -c 'NetworkPartition OK' "$log" 2>/dev/null || echo 0)
        say "seed $seed PASS  (fence=$fn killfence=$kf partition=$np $acked; ${dec:-decommission: (off)})"
        PASSED=$((PASSED + 1))
    else
        say "seed $seed FAIL — see $log"
        grep -iE 'MISMATCH|not_found|WRITE WEDGE|ORPHAN|UNDER-COUNT|OVER-COUNT|data.loss|panicked|DECOMMISSION FAILED' "$log" | head -8
        FAILED_SEEDS="$FAILED_SEEDS $seed"
    fi
done

echo
if [ -n "$FAILED_SEEDS" ]; then
    say "RESULT: FAIL — seeds failed:$FAILED_SEEDS (passed $PASSED). Logs: $LOG_DIR"
    exit 1
fi
say "RESULT: PASS — all seeds green ($PASSED). Full nemesis set + node decommission, no data loss. Logs: $LOG_DIR"
