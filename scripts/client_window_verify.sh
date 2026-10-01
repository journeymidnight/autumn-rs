#!/bin/bash
# Prove the client wire window: a client built below the ceiling (as low in the
# window as a hello-capable build exists) still works against a cluster at its
# CEILING, and stops working the moment the floor is raised past it.
#
# This is the acceptance for F-CLIENT-WIRE-COMPAT, and it is a script rather
# than a note because a one-shot manual run proves nothing the next time
# somebody moves a version number. Nothing else in the tree can check it: it
# needs TWO builds of the client at DIFFERENT wire versions, which a cargo test
# cannot produce.
#
#   scripts/client_window_verify.sh [OLD_COMMIT]
#
# OLD_COMMIT defaults to the commit with the LOWEST wire version in
# [MIN_CLIENT_WIRE_VERSION, WIRE_VERSION) that already speaks PROTOCOL_HELLO —
# a real binary from as low in the window as one exists, not a forged version
# number. That distinction is the point: the ledger row asks for a client
# "不是伪造区间". A client built before PROTOCOL_HELLO is refused whatever its
# number, so it cannot stand for the window. When no such commit exists yet
# (every hello-capable build is at the ceiling), there is nothing to prove and
# the script says so and exits 0.
#
# Leaves nothing behind: the worktree, the venv, the cluster and its data are
# all removed on exit, including on failure.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${TMPDIR:-/tmp}/autumn-window-verify.$$"
MGR_PORT=${MGR_PORT:-19801}
EN_PORT=${EN_PORT:-19901}
PS_PORT=${PS_PORT:-20001}
M=127.0.0.1:$MGR_PORT
NS=winverify
PIDS=()
RC=0
# The control step EDITS a tracked source file. Its backup therefore lives
# OUTSIDE $WORK — the exit trap deletes $WORK, so a backup in there is gone
# exactly when an interrupted run needs it, and the tree would be left with the
# floor raised and two `const` guards commented out. Rule 9: no destructive
# half-finished state.
GUARD="$REPO/crates/rpc/src/lib.rs"
GUARD_BAK="$REPO/.client_window_verify.lib.rs.bak"


say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
ok()  { printf '  \033[32mOK\033[0m   %s\n' "$*"; }
bad() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; RC=1; }

cleanup() {
    # Source first, before anything that could itself fail.
    if [ -f "$GUARD_BAK" ]; then
        cp "$GUARD_BAK" "$GUARD" && rm -f "$GUARD_BAK"
        echo "  restored $GUARD"
    fi
    for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
    sleep 1
    for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
    git -C "$REPO" worktree remove --force "$WORK/old" 2>/dev/null
    git -C "$REPO" worktree prune 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

constant() { grep -oP "pub const $1: u32 = \K[0-9]+" "$2/crates/rpc/src/lib.rs" | head -1; }

if [ -f "$GUARD_BAK" ]; then
    echo "a previous run left $GUARD_BAK — it holds the original lib.rs."
    echo "Restore it (cp it back over $GUARD) and delete it before re-running."
    exit 1
fi

CEILING=$(constant WIRE_VERSION "$REPO")
FLOOR=$(constant MIN_CLIENT_WIRE_VERSION "$REPO")
say "this tree: window [$FLOOR, $CEILING]"
if [ "$FLOOR" = "$CEILING" ]; then
    echo "  the window is SHUT — there is no older client to test with."
    echo "  Not a failure of this script; there is simply nothing to prove."
    exit 0
fi

# ── find a real hello-capable commit below the ceiling ──────────────────────
in_window() { [ -n "$1" ] && [ "$1" -ge "$FLOOR" ] && [ "$1" -lt "$CEILING" ]; }
OLD=${1:-}
if [ -z "$OLD" ]; then
    say "looking for the lowest hello-capable wire in [$FLOOR, $CEILING)"
    best=
    for c in $(git -C "$REPO" log --format=%h -n 400 -- crates/rpc/src/lib.rs); do
        git -C "$REPO" cat-file -e "$c:crates/rpc/src/protocol_hello.rs" 2>/dev/null || break
        v=$(git -C "$REPO" show "$c:crates/rpc/src/lib.rs" \
            | grep -oP 'pub const WIRE_VERSION: u32 = \K[0-9]+' | head -1)
        if in_window "$v" && { [ -z "$best" ] || [ "$v" -le "$best" ]; }; then
            OLD=$c; best=$v
        fi
    done
    if [ -z "$OLD" ]; then
        echo "  every hello-capable build is at wire $CEILING: no client below the"
        echo "  ceiling exists yet. Not a failure of this script; nothing to prove."
        exit 0
    fi
fi
echo "  using $OLD ($(git -C "$REPO" log -1 --format=%s "$OLD"))"

mkdir -p "$WORK"
git -C "$REPO" worktree add -q "$WORK/old" "$OLD" || exit 1
# A client built before mandatory PROTOCOL_HELLO is refused whatever its
# number, so it cannot stand for the window.
[ -f "$WORK/old/crates/rpc/src/protocol_hello.rs" ] \
    || { echo "$OLD predates PROTOCOL_HELLO; pass a hello-capable commit"; exit 1; }
OLDVER=$(constant WIRE_VERSION "$WORK/old")
in_window "$OLDVER" || { echo "$OLD is at wire $OLDVER, outside [$FLOOR, $CEILING)"; exit 1; }

say "building the CEILING cluster ($CEILING) and the old client ($OLDVER)"
( cd "$REPO" && cargo build -q --bins ) || exit 1
( cd "$WORK/old" && cargo build -q --bin autumn-client ) || exit 1
OLDC="$WORK/old/target/debug/autumn-client --manager $M --namespace $NS"
B="$REPO/target/debug"

# ── cluster ─────────────────────────────────────────────────────────────────
say "starting a cluster at wire $CEILING"
mkdir -p "$WORK/en0" "$WORK/ps1"
"$B/autumn-manager-server" --listen 127.0.0.1 --port "$MGR_PORT" >"$WORK/mgr.log" 2>&1 &
PIDS+=($!); sleep 4
"$B/autumn-op" --manager "$M" format "$WORK/en0" >/dev/null || exit 1
# One shard: the EN otherwise binds one data port per core and fail-stops on
# the first collision, which on a shared box is immediate.
"$B/autumn-extent-node" --data "$WORK/en0" --port "$EN_PORT" --manager "$M" \
    --advertise "127.0.0.1:$EN_PORT" --cpuset 0 >"$WORK/en.log" 2>&1 &
PIDS+=($!); sleep 8
"$B/autumn-op" --manager "$M" bootstrap --replication 1+0 >/dev/null || exit 1
"$B/autumn-ps" --psid 1 --port "$PS_PORT" --manager "$M" --data "$WORK/ps1" \
    >"$WORK/ps.log" 2>&1 &
PIDS+=($!); sleep 5
"$B/autumn-op" --manager "$M" namespace-create --name "$NS" >/dev/null 2>&1
echo "  compiled server wire=$CEILING, client window=[$FLOOR,$CEILING]; client connections verify it via PROTOCOL_HELLO"

# ── the data plane, from the old client ─────────────────────────────
say "wire-$OLDVER client against the wire-$CEILING cluster"
head -c 2048 /dev/urandom >"$WORK/small"
head -c 9000000 /dev/urandom >"$WORK/big"

$OLDC put wk/small "$WORK/small" >/dev/null 2>&1 \
    && $OLDC get wk/small >"$WORK/small.back" 2>/dev/null \
    && cmp -s "$WORK/small" "$WORK/small.back" \
    && ok "put + get byte-exact (2 KiB)" || bad "put + get (2 KiB)"

$OLDC put wk/big "$WORK/big" >/dev/null 2>&1 \
    && $OLDC get wk/big >"$WORK/big.back" 2>/dev/null \
    && cmp -s "$WORK/big" "$WORK/big.back" \
    && ok "put + get byte-exact (9 MB, bulk path)" || bad "put + get (9 MB)"

$OLDC direct-get wk/big >"$WORK/big.direct" 2>/dev/null \
    && cmp -s "$WORK/big" "$WORK/big.direct" \
    && ok "EN-direct read byte-exact" || bad "EN-direct read"

$OLDC head wk/small 2>/dev/null | grep -q 2048 \
    && ok "head" || bad "head"
[ "$($OLDC ls 2>/dev/null | grep -c '^wk/')" = 2 ] \
    && ok "range" || bad "range"

# The batch family: MSG_BATCH_PUT_BULK / MSG_BATCH_GET_BULK. perf-check --bulk N
# drives one put_many/get_many_into per round, which is the only client entry
# point that reaches them.
$OLDC perf-check --threads 2 --size 4k --bulk 32 --partitions 1 >"$WORK/batch.log" 2>&1
if grep -q 'Ops/sec' "$WORK/batch.log"; then
    ok "batch put_many/get_many_into ($(grep -oP 'Complete ops *: \K[0-9]+' "$WORK/batch.log" | tail -1) ops)"
else
    bad "batch"
fi

$OLDC del wk/small >/dev/null 2>&1 && $OLDC del wk/big >/dev/null 2>&1 \
    && ok "delete" || bad "delete"

# ── the control: close the window, same binary must be refused ──────────────
say "control: raise the floor to $CEILING and re-run the same binary"
cp "$GUARD" "$GUARD_BAK"
python3 - "$GUARD" "$CEILING" <<'PY'
import sys, re
p, ceiling = sys.argv[1], sys.argv[2]
s = open(p).read()
s = re.sub(r'pub const MIN_CLIENT_WIRE_VERSION: u32 = \d+;',
           f'pub const MIN_CLIENT_WIRE_VERSION: u32 = {ceiling};', s, count=1)
# The const guards deliberately forbid this; the control is the one place it
# is legitimate, so they are bypassed for the rebuild and restored after.
s = re.sub(r'^const _: \(\) = assert!\(MIN_CLIENT_WIRE_VERSION.*$',
           '// bypassed by client_window_verify.sh', s, flags=re.M)
open(p, 'w').write(s)
PY
( cd "$REPO" && cargo build -q --bin autumn-manager-server ) || RC=1
kill "${PIDS[0]}" 2>/dev/null; sleep 2
"$B/autumn-manager-server" --listen 127.0.0.1 --port "$MGR_PORT" >"$WORK/mgr2.log" 2>&1 &
PIDS+=($!); sleep 5
echo "  rebuilt manager client window=[$CEILING,$CEILING]; verify admission below"

# Captured first, then matched. A refused client exits non-zero, and under
# `pipefail` that sinks the whole pipeline even when grep matched — which reads
# as "not refused" and is exactly backwards. This check cost one false FAIL
# before it was written this way.
$OLDC put wk/small "$WORK/small" >"$WORK/refusal" 2>&1
if grep -Eq 'PROTOCOL_HELLO.*version mismatch|wire-version mismatch' "$WORK/refusal"; then
    ok "the same binary is now REFUSED, and the message says which way round"
    grep -Eo 'PROTOCOL_HELLO.*version mismatch.*|wire-version mismatch.*' "$WORK/refusal" | sed 's/^/       /'
else
    bad "a below-floor client was NOT refused — the window is not enforced"
    sed 's/^/       /' "$WORK/refusal"
fi

cp "$GUARD_BAK" "$GUARD" && rm -f "$GUARD_BAK"
( cd "$REPO" && cargo build -q --bins ) || RC=1
say "restored: window [$(constant MIN_CLIENT_WIRE_VERSION "$REPO"), $(constant WIRE_VERSION "$REPO")]"

[ $RC = 0 ] && echo "ALL CHECKS PASSED" || echo "FAILURES ABOVE"
exit $RC
