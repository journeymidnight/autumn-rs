#!/usr/bin/env bash
# fuse_inval_deadlock.sh — a kernel cache invalidation must not wedge the mount.
#
# `inval_inode` is a synchronous write(2) to /dev/fuse, and the kernel serves
# it by locking every cached page of the inode (invalidate_inode_pages2_range).
# A page under readahead stays locked until its FUSE_READ is answered. If the
# thread that issues the invalidation is also the one that must prepare that
# FUSE_READ, neither can move: the reader sits in D state and the mount answers
# nothing again (FUSE has no timeout).
#
# Opens are answered with FOPEN_DIRECT_IO, so plain read(2) never puts pages in
# the cache. What does is a MAP_PRIVATE mmap (the kernel allows it on a
# direct-io file and pages it through the cache) — the reader here is exactly
# that: re-fault a 64 MiB private mapping in a loop, dropping the cache between
# passes so every pass goes through readahead.
#
# The invalidations come from a SECOND mount that opens the same file for
# write and closes it, over and over: every close releases the write lease,
# the manager sends the reader mount a WriterClosed event, and the reader
# mount's lease poll loop calls `inval_inode` for it.
#
# Invariants:
#   REACHES: before the race, one WriterClosed empties the reader mount's page
#           cache for the file (mincore over a private mapping), proof that the
#           notifies land on the right inode, which no log line can show.
#   LIVE:   the reader's pass counter never stops advancing for STALL_SECS.
#           This is the check that discriminates: a notify stuck on a page
#           keeps that page locked, and the reader's next pass faults on it.
#   INTACT: every pass hashes to the seeded file's sha256 (the writer never
#           writes bytes, so this catches corruption, not stale reads).
# A stall dumps the reader mount's threads (state + wchan) and the reader's
# kernel stack before failing.
#
# Teardown aborts each mount's FUSE connection (fusectl) BEFORE killing its
# daemon. A daemon SIGKILLed while a notify waits on a readahead page can never
# exit: nobody is left to answer that read, the waiting thread is
# uninterruptible, and the /dev/fuse fd — whose release is what would abort the
# connection — lives until every thread is gone. Aborting fails the pending
# reads, the page unlocks, the notify returns, and the kill completes.
#
# Stops only this tree's cluster (cluster.sh pid files), its own two mounts,
# reader and writer; wipes AUTUMN_DATA_ROOT.
#
# Usage: AUTUMN_DATA_ROOT=/data05/autumn-inval ./scripts/fuse_inval_deadlock.sh
#   DURATION=60 STALL_SECS=15 FILE_MIB=64 READ_IO_THREADS=4 (0 = reads on the
#   dispatcher, the shape most likely to wedge)
#   FUSE_BIN=<path> runs another autumn-fuse build (A/B against an older one)
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$ROOT/target/release"; FUSE_BIN="${FUSE_BIN:-$BIN/autumn-fuse}"
MGR="127.0.0.1:9001"
MNT_R="${MNT_R:-/mnt/autumn-fuse-inval-r}"; MNT_W="${MNT_W:-/mnt/autumn-fuse-inval-w}"
DURATION="${DURATION:-60}"; STALL_SECS="${STALL_SECS:-15}"; FILE_MIB="${FILE_MIB:-64}"
READ_IO_THREADS="${READ_IO_THREADS:-4}"; MIN_EVENTS="${MIN_EVENTS:-1000}"
WORK="$(mktemp -d /tmp/fuse_inval.XXXXXX)"; FAIL=0
FUSE_R=""; FUSE_W=""; CONN_R=""; CONN_W=""; READER=""; WRITER=""; PROBE_PID=""
FUSECTL=/sys/fs/fuse/connections
export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-inval}"
say(){ echo "[inval $(date +%H:%M:%S)] $*"; }
fail(){ echo "[inval $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }
umnt(){ local m="$1" i; for i in 1 2 3 4 5 6; do grep -q " $m " /proc/mounts || return 0; umount -l "$m" 2>/dev/null; sleep 0.2; done; }
# The FUSE connection number of a mount = the minor of its device. Read from
# mountinfo, never by stat: a stat of a wedged mount blocks.
conn_of(){ awk -v m="$1" '$5 == m { split($3, d, ":"); print d[2] }' /proc/self/mountinfo | tail -1; }
cleanup(){
  local p c i; for p in $WRITER $READER $PROBE_PID; do kill -9 "$p" 2>/dev/null; done
  mountpoint -q "$FUSECTL" || mount -t fusectl none "$FUSECTL" 2>/dev/null
  # Only while the mount still names that connection: once it is gone the
  # number may already belong to someone else's mount.
  [ -n "$CONN_R" ] && [ "$(conn_of "$MNT_R")" = "$CONN_R" ] && echo 1 > "$FUSECTL/$CONN_R/abort"
  [ -n "$CONN_W" ] && [ "$(conn_of "$MNT_W")" = "$CONN_W" ] && echo 1 > "$FUSECTL/$CONN_W/abort"
  for p in $FUSE_R $FUSE_W; do kill -9 "$p" 2>/dev/null; done
  for p in $FUSE_R $FUSE_W; do
    for i in $(seq 1 20); do kill -0 "$p" 2>/dev/null || break; sleep 0.25; done
    kill -0 "$p" 2>/dev/null && echo "[inval] WARNING: daemon $p did not exit (connection not aborted?)"
  done
  umnt "$MNT_R"; umnt "$MNT_W"; bash "$ROOT/cluster.sh" stop > /dev/null 2>&1
}
trap cleanup EXIT

mount_fuse(){ local mnt="$1" tag="$2"; shift 2
  mkdir -p "$mnt"
  RUST_LOG=info setsid nohup "$FUSE_BIN" --manager "$MGR" --mountpoint "$mnt" --transport tcp "$@" \
    > "$WORK/fuse_$tag.log" 2>&1 </dev/null &
  local pid=$! i
  for i in $(seq 1 30); do mountpoint -q "$mnt" && { echo "$pid"; return 0; }; sleep 1; done
  return 1
}

say "starting cluster (3 EN; work=$WORK)"
umnt "$MNT_R"; umnt "$MNT_W"; rm -rf "$AUTUMN_DATA_ROOT"
env AUTUMN_EXTENT_BASE_PORT=20000 AUTUMN_TRANSPORT=tcp bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
grep -q "bootstrap succeeded" "$WORK/cluster.log" || { echo "cluster start failed"; tail -20 "$WORK/cluster.log"; exit 1; }
sleep 3
FUSE_R=$(mount_fuse "$MNT_R" r --read-io-threads "$READ_IO_THREADS") || { fail "mount reader"; exit 1; }
FUSE_W=$(mount_fuse "$MNT_W" w) || { fail "mount writer"; exit 1; }
CONN_R=$(conn_of "$MNT_R"); CONN_W=$(conn_of "$MNT_W")
[ -n "$CONN_R" ] && [ -n "$CONN_W" ] || { fail "no FUSE connection id for the mounts"; exit 1; }
say "reader mount pid=$FUSE_R (--read-io-threads $READ_IO_THREADS), writer mount pid=$FUSE_W"

head -c $((FILE_MIB << 20)) /dev/urandom > "$WORK/src.bin"
cp "$WORK/src.bin" "$MNT_R/f.bin"; sync
WANT=$(sha256sum < "$WORK/src.bin" | cut -d' ' -f1)
[ "$(sha256sum < "$MNT_R/f.bin" | cut -d' ' -f1)" = "$WANT" ] || { fail "seeded file reads back wrong"; exit 1; }
say "seeded f.bin ($FILE_MIB MiB)"

# Probe first: one WriterClosed must actually evict the reader mount's cached
# pages. fuser reports a notify for an inode the kernel does not know as
# success, so a notify aimed at the wrong place logs nothing; only the page
# cache shows it. Residency is read with mincore(2) over a private mapping.
python3 - "$MNT_R/f.bin" "$WORK/probe" > "$WORK/probe.log" 2>&1 <<'PROBE' &
import ctypes, os, sys, time
path, flag = sys.argv[1:3]
libc = ctypes.CDLL(None, use_errno=True)
libc.mmap.restype = ctypes.c_void_p
libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
PROT_READ, MAP_PRIVATE, PAGE = 1, 2, os.sysconf("SC_PAGE_SIZE")
fd = os.open(path, os.O_RDONLY)
size = os.fstat(fd).st_size
addr = libc.mmap(None, size, PROT_READ, MAP_PRIVATE, fd, 0)
if addr in (None, ctypes.c_void_p(-1).value):
    sys.exit(f"mmap: errno {ctypes.get_errno()}")
pages = (size + PAGE - 1) // PAGE
vec = (ctypes.c_ubyte * pages)()
def resident():
    if libc.mincore(ctypes.c_void_p(addr), ctypes.c_size_t(size), vec) != 0:
        sys.exit(f"mincore: errno {ctypes.get_errno()}")
    return sum(b & 1 for b in vec) * 100 // pages
for off in range(0, size, PAGE):
    ctypes.string_at(addr + off, 1)
before = resident()
open(flag + ".ready", "w").close()
give_up = time.time() + 60
while not os.path.exists(flag + ".closed"):
    if time.time() > give_up:
        sys.exit("the writer close never came")
    time.sleep(0.05)
deadline = time.time() + 5
after = resident()
while after > 10 and time.time() < deadline:
    time.sleep(0.1)
    after = resident()
print(f"resident before={before}% after={after}%")
sys.exit(0 if before >= 90 and after <= 10 else 1)
PROBE
PROBE_PID=$!
for i in $(seq 1 300); do [ -e "$WORK/probe.ready" ] && break; kill -0 "$PROBE_PID" 2>/dev/null || break; sleep 0.1; done
# Notifying before every page is in would race the probe's own readahead.
[ -e "$WORK/probe.ready" ] || { fail "probe never became ready: $(cat "$WORK/probe.log")"; exit 1; }
timeout 30 python3 -c 'import os, sys; os.close(os.open(sys.argv[1], os.O_WRONLY))' "$MNT_W/f.bin" 2> "$WORK/probe_writer.err" \
  || { fail "probe: writer open/close failed: $(cat "$WORK/probe_writer.err")"; exit 1; }
touch "$WORK/probe.closed"
if ! wait "$PROBE_PID"; then fail "probe: the invalidation did not reach the kernel page cache ($(cat "$WORK/probe.log"))"; exit 1; fi
PROBE_PID=""
# A poll error or overflow also invalidates every held inode, with no WriterClosed.
if grep -q -e 'poll failed' -e 'overflow sentinel' "$WORK/fuse_r.log"; then
  fail "probe: the eviction may come from a poll error, not the WriterClosed: $(grep -m1 -e 'poll failed' -e 'overflow sentinel' "$WORK/fuse_r.log")"; exit 1
fi
say "probe: one WriterClosed evicted the cached pages ($(cat "$WORK/probe.log"))"

python3 - "$MNT_R/f.bin" "$WANT" "$WORK/reader.progress" "$WORK/reader.bad" > "$WORK/reader.log" 2>&1 <<'EOF' &
import hashlib, mmap, os, sys
path, want, progress, bad = sys.argv[1:5]
fd = os.open(path, os.O_RDONLY)
size = os.fstat(fd).st_size
mm = mmap.mmap(fd, size, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ)
n = 0
while True:
    # Unmap our view and drop the clean cache pages so the next pass faults
    # every page back in through readahead.
    mm.madvise(mmap.MADV_DONTNEED)
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    got = hashlib.sha256(mm).hexdigest()
    n += 1
    if got != want:
        with open(bad, "a") as f:
            f.write(f"pass {n}: {got}\n")
    with open(progress + ".tmp", "w") as f:
        f.write(str(n))
    os.replace(progress + ".tmp", progress)
EOF
READER=$!; disown "$READER"

python3 - "$MNT_W/f.bin" "$WORK/writer.progress" > "$WORK/writer.log" 2>&1 <<'EOF' &
import os, sys
path, progress = sys.argv[1:3]
n = 0
while True:
    os.close(os.open(path, os.O_WRONLY))
    n += 1
    if n % 20 == 0:
        with open(progress + ".tmp", "w") as f:
            f.write(str(n))
        os.replace(progress + ".tmp", progress)
EOF
WRITER=$!; disown "$WRITER"
say "reader pid=$READER (private mmap re-fault loop), writer pid=$WRITER (open O_WRONLY + close loop)"

dump_stall(){
  say "reader mount threads (state, wchan):"
  local t; for t in /proc/"$FUSE_R"/task/*; do
    printf '  %-6s %-22s %s %s\n' "${t##*/}" "$(cat "$t/comm" 2>/dev/null)" \
      "$(awk '{print $3}' "$t/stat" 2>/dev/null)" "$(cat "$t/wchan" 2>/dev/null)"
  done
  say "reader process: state=$(awk '{print $3}' /proc/"$READER"/stat 2>/dev/null) wchan=$(cat /proc/"$READER"/wchan 2>/dev/null)"
  cat /proc/"$READER"/stack 2>/dev/null | sed 's/^/  /'
}

last=-1; still=0; wlast=""; wstill=0; t=0
while [ "$t" -lt "$DURATION" ]; do
  sleep 1; t=$((t + 1))
  kill -0 "$READER" 2>/dev/null || { fail "reader exited: $(tail -3 "$WORK/reader.log")"; break; }
  kill -0 "$WRITER" 2>/dev/null || { fail "writer exited: $(tail -3 "$WORK/writer.log")"; break; }
  cur=$(cat "$WORK/reader.progress" 2>/dev/null || echo 0)
  # The writer is what produces the events; a wedged writer mount would leave
  # the reader nothing to race against.
  wcur=$(cat "$WORK/writer.progress" 2>/dev/null || echo 0)
  if [ "$wcur" = "${wlast:-}" ]; then wstill=$((wstill + 1)); else wstill=0; wlast="$wcur"; fi
  [ "$wstill" -ge "$STALL_SECS" ] && { fail "writer made no progress for ${STALL_SECS}s at close $wcur"; break; }
  if [ "$cur" = "$last" ]; then still=$((still + 1)); else still=0; last="$cur"; fi
  if [ "$still" -ge "$STALL_SECS" ]; then
    fail "reader made no progress for ${STALL_SECS}s at pass $cur"
    dump_stall
    break
  fi
done

passes=$(cat "$WORK/reader.progress" 2>/dev/null || echo 0)
closes=$(cat "$WORK/writer.progress" 2>/dev/null || echo 0)
# The poll loop's per-event line (queued for the kernel, not proof of delivery;
# LIVE is what proves the notifies kept draining).
events=$(grep -c 'lease_tasks.* invalidation ' "$WORK/fuse_r.log" 2>/dev/null); events=${events:-0}
say "reader passes=$passes writer closes=$closes invalidation events at the reader mount=$events"
[ -s "$WORK/reader.bad" ] && fail "reader saw wrong bytes: $(head -3 "$WORK/reader.bad")"
# Without a steady stream of events, and notifies that actually landed, the run
# proves nothing about the invalidation path.
[ "$events" -ge "$MIN_EVENTS" ] || fail "only $events invalidation events at the reader mount (want >= $MIN_EVENTS)"
notify_errs=$(grep -c -e 'notify_inval_inode failed' -e 'invalidation thread is gone' "$WORK/fuse_r.log" 2>/dev/null)
[ "${notify_errs:-0}" -eq 0 ] || fail "$notify_errs kernel notifies did not land: $(grep -m1 -e 'notify_inval_inode failed' -e 'invalidation thread is gone' "$WORK/fuse_r.log")"
[ "$passes" -gt 0 ] || fail "the reader never finished a pass"
[ $FAIL -eq 0 ] && say "PASS ($WORK)" || say "FAILED ($WORK)"
exit $FAIL
