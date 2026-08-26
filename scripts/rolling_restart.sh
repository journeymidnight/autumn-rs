#!/usr/bin/env bash
# rolling_restart.sh — SAME-BINARY rolling restart.
#
# autumn does NOT support rolling UPGRADES: a version change is stop-the-world
# (full stop, swap binaries, full start). This script restarts a cluster that
# is already running one binary — for config changes, host maintenance, or
# clearing a wedged process.
#
# Programmatic rolling restart of a running autumn-rs cluster, one process
# at a time, with a convergence gate + write-liveness probe between every
# step. Fail-stop: the first gate that does not converge within its budget
# aborts the whole procedure, leaving the cluster in its current (still
# serving) state for the operator to inspect.
#
# Order (most-depended-on end first):
#   1. extent-nodes, one by one   (EN gate: node Online + recovery quiet)
#   2. partition server           (PS gate: every partition routed)
#   3. manager                    (gate: leader back + all nodes Online)
#
# This is a SAME-BINARY rolling restart (config change / machine move /
# kernel upgrade). Version-skew rolling upgrades additionally need the
# R1+ cluster_version machinery; this script is their orchestration
# skeleton.
#
# Usage:
#   bash scripts/rolling_restart.sh            # full sequence
#
# Env knobs:
#   AUTUMN_DATA_ROOT        same value the cluster was started with
#   AUTUMN_TRANSPORT        tcp (default) | ucx — forwarded to cluster.sh
#   ROLL_GATE_TIMEOUT       per-gate convergence budget, secs (default 180)
#   ROLL_HB_FRESH_SECS      max heartbeat age to call a node "back" (default 10)
#   ROLL_LIVENESS_TRIES     put retries per liveness key, 2s apart (default 30)
set -euo pipefail
# Byte-wise string comparison everywhere — probe-prefix derivation proves
# range membership in BYTE order (the partition sort order), which locale
# collation would silently break.
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER="$REPO_DIR/cluster.sh"
BIN="$REPO_DIR/target/release"

DATA_ROOT="${AUTUMN_DATA_ROOT:-/tmp/autumn-rs}"
CONFIG_FILE="$DATA_ROOT/cluster_config"

GATE_TIMEOUT="${ROLL_GATE_TIMEOUT:-180}"
HB_FRESH="${ROLL_HB_FRESH_SECS:-10}"
LIVENESS_TRIES="${ROLL_LIVENESS_TRIES:-30}"

say()  { echo "[roll] $*"; }
die()  { echo "[roll] FAIL: $*" >&2; exit 1; }

[[ -f "$CONFIG_FILE" ]] || die "no cluster config at $CONFIG_FILE — is the cluster running? (AUTUMN_DATA_ROOT mismatch?)"
# shellcheck disable=SC1090
source "$CONFIG_FILE"   # REPLICAS, CLUSTER_MODE, exported AUTUMN_* vars

BIND_HOST="${AUTUMN_BIND_HOST:-127.0.0.1}"
MANAGER_ADDR="${BIND_HOST}:9001"
TRANSPORT="${AUTUMN_TRANSPORT:-tcp}"
EXTENT_BASE_PORT="${AUTUMN_EXTENT_BASE_PORT:-9100}"
PS_BASE_PORT="${AUTUMN_PS_BASE_PORT:-9301}"

AC=("$BIN/autumn-client" --manager "$MANAGER_ADDR" --transport "$TRANSPORT")
AO=("$BIN/autumn-op"     --manager "$MANAGER_ADDR" --transport "$TRANSPORT")

[[ -x "${AC[0]}" && -x "${AO[0]}" ]] || die "binaries missing under $BIN — cargo build --release --workspace"

# Exactly one rolling restart at a time per cluster — two concurrent rolls
# would restart two processes simultaneously and void the one-at-a-time
# availability guarantee (coco P2).
LOCK_FILE="$DATA_ROOT/rolling_restart.lock"
exec 9>"$LOCK_FILE" || die "cannot open lock file $LOCK_FILE"
flock -n 9 || die "another rolling restart is already running (lock: $LOCK_FILE)"

# Probe keys are namespaced per run and deleted on exit (best-effort), so
# repeated rolls neither overwrite business keys nor accumulate garbage
# (coco P2). Every written key is recorded in $WORK/probe_keys.txt.
RUN_ID="$(date +%s)-$$"

WORK="$(mktemp -d /tmp/autumn-roll.XXXXXX)"
cleanup() {
    if [[ -s "$WORK/probe_keys.txt" ]]; then
        while IFS= read -r k; do
            "${AC[@]}" del "$k" >/dev/null 2>&1 || true
        done < "$WORK/probe_keys.txt"
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# gates — each returns 0 once converged, non-zero otherwise; wait_for polls
# ---------------------------------------------------------------------------

wait_for() { # wait_for <desc> <fn> [args...]
    local desc="$1"; shift
    local deadline=$(( SECONDS + GATE_TIMEOUT ))
    while (( SECONDS < deadline )); do
        if "$@" 2>/dev/null; then say "gate OK: $desc"; return 0; fi
        sleep 2
    done
    die "gate TIMEOUT (${GATE_TIMEOUT}s): $desc"
}

# Node $1 (addr) reports Online with a fresh heartbeat in list-nodes.
node_online() {
    local addr="$1" hb
    hb="$("${AO[@]}" list-nodes 2>/dev/null \
        | awk -v a="$addr" '$2 == a && $3 == "Online" { print $4 }')"
    [[ -n "$hb" && "$hb" != "never" ]] || return 1
    [[ "${hb%s}" =~ ^[0-9]+$ ]] || return 1
    (( ${hb%s} <= HB_FRESH ))
}

# Re-replication triggered by the downtime has fully drained: no inflight
# recovery copies and no extents parked in retry-backoff.
recovery_quiet() {
    local out
    out="$("${AO[@]}" recovery-stats 2>/dev/null)" || return 1
    grep -q '^global: 0/' <<<"$out" && grep -q 'backoff_entries=0' <<<"$out"
}

# Every partition the manager knows about is routed to a live PS address.
partitions_routed() {
    local out n_parts
    out="$("${AO[@]}" info 2>/dev/null)" || return 1
    n_parts="$(grep -c '^  part ' <<<"$out" || true)"
    (( n_parts >= 1 )) || return 1
    ! grep -q 'ps=unknown' <<<"$out"
}

# Manager answers info at all (leader elected + routable).
manager_up() {
    "${AO[@]}" info >/dev/null 2>&1
}

all_nodes_online() {
    local i
    for (( i=1; i<=REPLICAS; i++ )); do
        node_online "${BIND_HOST}:$(( EXTENT_BASE_PORT + i ))" || return 1
    done
}

# ---------------------------------------------------------------------------
# per-partition liveness + seed verification
# ---------------------------------------------------------------------------
#
# Partition ranges are discovered from `autumn-op info`. For each [start,end)
# a probe PREFIX is derived that PROVABLY sorts inside the range (coco P1:
# naively appending a suffix to start routes the empty-start partition's
# probe into the LAST partition — every key extends "" — so the first
# partition would never actually be probed while the gate claims coverage):
#
#   - start not a prefix of end (incl. end=∞): prefix = start. Any extension
#     of start stays >= start, and < end because start vs end already differ
#     at a byte inside start.
#   - start IS a prefix of end (incl. start=""): prefix = start + d where d
#     is a printable byte strictly below end's next byte — extensions then
#     differ from end at that position and stay < end.
#
# Partitions whose range gives no such printable prefix (binary keys from a
# runtime auto-split, or end's next byte too low) are SKIPPED with a loud
# per-partition warning — the gate reports probed/skipped counts honestly;
# exhaustive binary-key liveness is the chaos harness's job (it probes
# in-process, without shell-printability limits).

PART_PREFIXES=()
derive_probe_prefix() { # <start> <end-or-∞> → echoes prefix, rc=1 if impossible
    local start="$1" end="$2"
    if [[ "$end" == "∞" || -z "$end" ]]; then
        echo "$start"; return 0
    fi
    if [[ "${end:0:${#start}}" != "$start" ]]; then
        echo "$start"; return 0
    fi
    local next="${end:${#start}:1}" d
    for d in 0 1 3 7 A K U e o z; do
        if [[ "$d" < "$next" ]]; then echo "${start}${d}"; return 0; fi
    done
    return 1
}

discover_partitions() {
    PART_PREFIXES=()
    local out line start end prefix skipped=0
    out="$("${AO[@]}" info 2>/dev/null)" || die "autumn-op info failed during partition discovery"
    # Delimiter is '|' (excluded from the printable-key charset below),
    # NOT a tab: tab is whitespace-class IFS, so `read` SWALLOWS a leading
    # empty field — the empty-start first partition came back as
    # start="<end>", end="" and its probe silently targeted the wrong
    # partition (found by the 1-partition regression run).
    while IFS='|' read -r start end; do
        if [[ ! "$start" =~ ^[0-9a-zA-Z_-]*$ || ! ( "$end" =~ ^[0-9a-zA-Z_-]*$ || "$end" == "∞" ) ]]; then
            say "WARN: partition [$start..$end) has non-printable bounds — probe SKIPPED"
            (( ++skipped )); continue
        fi
        if prefix="$(derive_probe_prefix "$start" "$end")"; then
            PART_PREFIXES+=("$prefix")
            say "partition [${start}..${end:-∞}) -> probe prefix '$prefix'"
        else
            say "WARN: partition [$start..$end) too narrow for a printable probe key — SKIPPED"
            (( ++skipped ))
        fi
    done < <(sed -n 's/^  part [0-9]*: ps=[^,]*, range=\[\(.*\)\.\.\(.*\))$/\1|\2/p' <<<"$out")
    (( ${#PART_PREFIXES[@]} >= 1 )) || die "no probeable partitions discovered from autumn-op info"
    say "partitions discovered: ${#PART_PREFIXES[@]} probed, $skipped skipped"
}

probe_key() { # <prefix> <name> — namespaced per run, recorded for cleanup
    local k="$1__autumn-roll-${RUN_ID}-$2"
    echo "$k" >> "$WORK/probe_keys.txt"
    echo "$k"
}

# Write-liveness: one in-range key per probeable partition must accept a PUT
# and read back identically within the retry budget. This is the
# authoritative per-partition readiness gate — `partitions_routed` above is
# only a cheap pre-check (autumn-op info falls back to the PS base address
# while a partition listener is still coming up, coco P2).
write_liveness() {
    local tag="$1" prefix k try ok
    for prefix in "${PART_PREFIXES[@]}"; do
        k="$(probe_key "$prefix" "live-$tag")"
        printf 'live-%s-%s\n' "$tag" "$prefix" > "$WORK/live.in"
        ok=0
        for (( try=1; try<=LIVENESS_TRIES; try++ )); do
            if "${AC[@]}" put "$k" "$WORK/live.in" >/dev/null 2>&1; then ok=1; break; fi
            sleep 2
        done
        (( ok )) || die "[$tag] write liveness WEDGED on key '$k' (${LIVENESS_TRIES}x2s)"
        "${AC[@]}" get "$k" 2>/dev/null | cmp -s - "$WORK/live.in" \
            || die "[$tag] liveness read-back mismatch on key '$k'"
    done
    say "[$tag] write liveness OK (${#PART_PREFIXES[@]} partitions)"
}

# Seed: per-partition 1 KiB value + one 12 MiB striped value (VP / large-
# value path). Written once before the roll, content-verified at the end —
# proves the restarts lost nothing that was ACKed beforehand.
BIGSTRIPE_KEY=""
seed_put() {
    mkdir -p "$WORK/seed"
    local prefix k
    for prefix in "${PART_PREFIXES[@]}"; do
        k="$(probe_key "$prefix" "seed")"
        head -c 1024 /dev/urandom > "$WORK/seed/$(echo -n "$k" | md5sum | cut -d' ' -f1)"
        "${AC[@]}" put "$k" "$WORK/seed/$(echo -n "$k" | md5sum | cut -d' ' -f1)" >/dev/null \
            || die "seed put failed on '$k' — cluster unhealthy before roll, aborting"
    done
    BIGSTRIPE_KEY="$(probe_key "" "bigstripe")"
    head -c $(( 12 * 1024 * 1024 )) /dev/urandom > "$WORK/seed/bigstripe"
    "${AC[@]}" put-stream "$BIGSTRIPE_KEY" "$WORK/seed/bigstripe" >/dev/null \
        || die "seed put-stream failed — cluster unhealthy before roll, aborting"
    say "seeded ${#PART_PREFIXES[@]} keys + 12MiB stripe"
}

seed_verify() {
    local prefix k bad=0
    for prefix in "${PART_PREFIXES[@]}"; do
        k="${prefix}__autumn-roll-${RUN_ID}-seed"
        if ! "${AC[@]}" get "$k" 2>/dev/null \
                | cmp -s - "$WORK/seed/$(echo -n "$k" | md5sum | cut -d' ' -f1)"; then
            echo "[roll] seed key '$k' mismatch/missing" >&2; bad=1
        fi
    done
    rm -f "$WORK/big.out"
    if ! "${AC[@]}" get-stream --out "$WORK/big.out" "$BIGSTRIPE_KEY" >/dev/null 2>&1 \
            || ! cmp -s "$WORK/big.out" "$WORK/seed/bigstripe"; then
        echo "[roll] bigstripe mismatch/missing" >&2; bad=1
    fi
    (( bad == 0 )) || die "seed verification FAILED — data lost across the roll"
    say "seed verify OK (${#PART_PREFIXES[@]} keys + 12MiB stripe)"
}

# ---------------------------------------------------------------------------
# main sequence
# ---------------------------------------------------------------------------

say "=== pre-flight (cluster must be healthy before rolling) ==="
wait_for "manager answers info" manager_up
wait_for "all $REPLICAS extent-node(s) Online" all_nodes_online
wait_for "recovery quiet" recovery_quiet
wait_for "all partitions routed" partitions_routed
discover_partitions
seed_put
write_liveness "preflight"

for (( i=1; i<=REPLICAS; i++ )); do
    addr="${BIND_HOST}:$(( EXTENT_BASE_PORT + i ))"
    say "=== rolling extent-node $i/$REPLICAS ($addr) ==="
    bash "$CLUSTER" restart-node "$i" || die "cluster.sh restart-node $i failed"
    wait_for "node$i Online again" node_online "$addr"
    wait_for "recovery quiet after node$i" recovery_quiet
    write_liveness "en$i"
done

say "=== rolling partition server ==="
bash "$CLUSTER" restart-ps || die "cluster.sh restart-ps failed"
wait_for "all partitions routed after PS restart" partitions_routed
write_liveness "ps"

say "=== rolling manager ==="
bash "$CLUSTER" restart-manager || die "cluster.sh restart-manager failed"
wait_for "manager leader back" manager_up
wait_for "all nodes Online under new manager" all_nodes_online
wait_for "all partitions routed under new manager" partitions_routed
write_liveness "manager"

say "=== final verification ==="
seed_verify

say "ROLLING RESTART COMPLETE: $REPLICAS EN + PS + manager, all gates passed, zero loss"
