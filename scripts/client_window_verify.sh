#!/bin/bash
# Prove the client wire window: a client built at the window's FLOOR still
# works against a cluster at its CEILING, and stops working the moment the
# floor is raised past it.
#
# This is the acceptance for F-CLIENT-WIRE-COMPAT, and it is a script rather
# than a note because a one-shot manual run proves nothing the next time
# somebody moves a version number. Nothing else in the tree can check it: it
# needs TWO builds of the client at DIFFERENT wire versions, which a cargo test
# cannot produce.
#
#   scripts/client_window_verify.sh [OLD_COMMIT]
#
# OLD_COMMIT defaults to the newest commit whose WIRE_VERSION equals this
# tree's MIN_CLIENT_WIRE_VERSION — i.e. a real binary from the floor of the
# window, not a forged version number. That distinction is the point: the
# ledger row asks for a client "不是伪造区间".
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

# ── find a real commit at the floor ─────────────────────────────────────────
OLD=${1:-}
if [ -z "$OLD" ]; then
    say "looking for the newest commit whose WIRE_VERSION is $FLOOR"
    for c in $(git -C "$REPO" log --format=%h -n 400 -- crates/rpc/src/lib.rs); do
        v=$(git -C "$REPO" show "$c:crates/rpc/src/lib.rs" 2>/dev/null \
            | grep -oP 'pub const WIRE_VERSION(_MAX)?: u32 = \K[0-9]+' | head -1)
        if [ "$v" = "$FLOOR" ]; then OLD=$c; break; fi
    done
fi
[ -n "$OLD" ] || { echo "no commit found at wire $FLOOR — pass one explicitly"; exit 1; }
echo "  using $OLD ($(git -C "$REPO" log -1 --format=%s "$OLD"))"

mkdir -p "$WORK"
git -C "$REPO" worktree add -q "$WORK/old" "$OLD" || exit 1
OLDVER=$(constant WIRE_VERSION "$WORK/old")
[ "$OLDVER" = "$FLOOR" ] || { echo "worktree is at wire $OLDVER, expected $FLOOR"; exit 1; }

say "building the CEILING cluster ($CEILING) and the FLOOR client ($FLOOR)"
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
"$B/autumn-op" --manager "$M" cluster-version | sed 's/^/  /'

# ── the data plane, from the floor-built client ─────────────────────────────
say "wire-$FLOOR client against the wire-$CEILING cluster"
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
python3 - "$REPO/crates/rpc/src/lib.rs" "$CEILING" <<'PY'
import sys, re
p, ceiling = sys.argv[1], sys.argv[2]
s = open(p).read()
s = re.sub(r'pub const MIN_CLIENT_WIRE_VERSION: u32 = \d+;',
           f'pub const MIN_CLIENT_WIRE_VERSION: u32 = {ceiling};', s, count=1)
# The two const guards deliberately forbid this; the control is the one place
# it is legitimate, so they are bypassed for the rebuild and restored after.
s = re.sub(r'^const _: \(\) = assert!\(MIN_CLIENT_WIRE_VERSION.*$',
           '// bypassed by client_window_verify.sh', s, flags=re.M)
open(p, 'w').write(s)
PY
( cd "$REPO" && cargo build -q --bin autumn-manager-server ) || RC=1
kill "${PIDS[0]}" 2>/dev/null; sleep 2
"$B/autumn-manager-server" --listen 127.0.0.1 --port "$MGR_PORT" >"$WORK/mgr2.log" 2>&1 &
PIDS+=($!); sleep 5
"$B/autumn-op" --manager "$M" cluster-version | grep -q 'window shut' \
    && ok "window reports shut" || bad "window did not close"

# Captured first, then matched. A refused client exits non-zero, and under
# `pipefail` that sinks the whole pipeline even when grep matched — which reads
# as "not refused" and is exactly backwards. This check cost one false FAIL
# before it was written this way.
$OLDC put wk/small "$WORK/small" >"$WORK/refusal" 2>&1
if grep -q 'wire-version mismatch' "$WORK/refusal"; then
    ok "the same binary is now REFUSED, and the message says which way round"
    grep -o 'wire-version mismatch.*' "$WORK/refusal" | sed 's/^/       /'
else
    bad "a below-floor client was NOT refused — the window is not enforced"
    sed 's/^/       /' "$WORK/refusal"
fi

cp "$GUARD_BAK" "$GUARD" && rm -f "$GUARD_BAK"
( cd "$REPO" && cargo build -q --bins ) || RC=1
say "restored: window [$(constant MIN_CLIENT_WIRE_VERSION "$REPO"), $(constant WIRE_VERSION "$REPO")]"

[ $RC = 0 ] && echo "ALL CHECKS PASSED" || echo "FAILURES ABOVE"
exit $RC
