#!/usr/bin/env bash
# fuse_page_cache.sh — the mount serves reads through the kernel page cache
# and keeps it across opens (FOPEN_KEEP_CACHE); no reader may see stale bytes.
#
# Two mounts of one cluster: A reads, B writes. Every rewrite is the SAME size,
# so only the content generation (not size) tells old from new.
#
#   SHARED: a MAP_SHARED mapping works (a direct_io mount refused it: ENODEV).
#   KEEP:   pages read through A survive A's close and reopen (mincore right
#           after reopening, before touching anything).
#   REOPEN: B rewrites while A has the file closed — A holds no lease then, so
#           no invalidation reaches it; A's next open must still read the new
#           bytes (the generation check at open drops the cache).
#   HELD:   A keeps an fd open (and its lease) with the old bytes cached; B
#           rewrites and closes; A's same fd must see the new bytes within
#           HELD_SECS (the WriterClosed invalidation).
#   TAIL:   A keeps an fd open and has read to EOF; B appends and closes; A's
#           same fd must read the appended bytes past the old EOF within
#           HELD_SECS (the size A's kernel asks for after the invalidation has
#           to be the new one).
#   PREFETCH: A reads the first 16 MiB of a 64 MiB file in sequence, so the
#           daemon fetches blocks ahead of it; B rewrites the file and closes;
#           within 3 s (under the prefetch cache's 5 s idle expiry, so expiry
#           cannot pass it) A's same fd must read the NEW bytes past 16 MiB, not
#           the blocks fetched before the rewrite.
#   LOCAL:  on A alone, bytes written through one fd are what another fd and a
#           shared mapping read.
#   MMAPW:  on A, a store through a writable shared mapping, then munmap and
#           close with no msync, is what B reads afterwards.
#
# Usage: AUTUMN_DATA_ROOT=/data05/autumn-pc ./scripts/fuse_page_cache.sh
#   FILE_MIB=16 HELD_SECS=5 FUSE_BIN=<path> (run another autumn-fuse build;
#   the REOPEN check fails on a build that keeps the cache without checking the
#   generation)
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$ROOT/target/release"; FUSE_BIN="${FUSE_BIN:-$BIN/autumn-fuse}"
MGR="127.0.0.1:9001"
MNT_A="${MNT_A:-/mnt/autumn-fuse-pc-a}"; MNT_B="${MNT_B:-/mnt/autumn-fuse-pc-b}"
FILE_MIB="${FILE_MIB:-16}"; HELD_SECS="${HELD_SECS:-5}"
WORK="$(mktemp -d /tmp/fuse_pc.XXXXXX)"; FAIL=0
FUSE_A=""; FUSE_B=""; HOLDER=""
export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-pc}"
say(){ echo "[page-cache $(date +%H:%M:%S)] $*"; }
fail(){ echo "[page-cache $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }
umnt(){ local m="$1" i; for i in 1 2 3 4 5 6; do grep -q " $m " /proc/mounts || return 0; umount -l "$m" 2>/dev/null; sleep 0.2; done; }
cleanup(){
  local p; for p in $HOLDER $FUSE_A $FUSE_B; do kill -9 "$p" 2>/dev/null; done
  umnt "$MNT_A"; umnt "$MNT_B"; bash "$ROOT/cluster.sh" stop > /dev/null 2>&1
}
trap cleanup EXIT

mount_fuse(){ local mnt="$1" tag="$2"
  mkdir -p "$mnt"
  RUST_LOG=info setsid nohup "$FUSE_BIN" --manager "$MGR" --mountpoint "$mnt" --transport tcp \
    > "$WORK/fuse_$tag.log" 2>&1 </dev/null &
  local pid=$! i
  for i in $(seq 1 30); do grep -q " $mnt " /proc/mounts && { echo "$pid"; return 0; }; sleep 1; done
  return 1
}
sha(){ sha256sum < "$1" | cut -d' ' -f1; }
# Same-size rewrite of $1 with fresh random bytes, through one open/close.
rewrite(){ python3 -c '
import os, sys
fd = os.open(sys.argv[1], os.O_WRONLY)
data = open(sys.argv[2], "rb").read()
os.pwrite(fd, data, 0); os.close(fd)' "$1" "$2"; }
# Percent of $1 resident in the page cache, measured through a fresh open.
resident(){ python3 - "$1" <<'EOF'
import ctypes, mmap, os, sys
libc = ctypes.CDLL(None, use_errno=True)
libc.mmap.restype = ctypes.c_void_p
libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
fd = os.open(sys.argv[1], os.O_RDONLY)
size = os.fstat(fd).st_size
pages = (size + mmap.PAGESIZE - 1) // mmap.PAGESIZE
vec = (ctypes.c_ubyte * pages)()
addr = libc.mmap(None, size, mmap.PROT_READ, mmap.MAP_SHARED, fd, 0)
if addr in (None, ctypes.c_void_p(-1).value):
    sys.exit(f"mmap errno {ctypes.get_errno()}")
if libc.mincore(ctypes.c_void_p(addr), ctypes.c_size_t(size), vec) != 0:
    sys.exit(f"mincore errno {ctypes.get_errno()}")
print(sum(b & 1 for b in vec) * 100 // pages)
EOF
}

say "starting cluster (3 EN; work=$WORK)"
umnt "$MNT_A"; umnt "$MNT_B"; rm -rf "$AUTUMN_DATA_ROOT"
env AUTUMN_EXTENT_BASE_PORT=20000 AUTUMN_TRANSPORT=tcp bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
grep -q "bootstrap succeeded" "$WORK/cluster.log" || { echo "cluster start failed"; tail -20 "$WORK/cluster.log"; exit 1; }
sleep 3
FUSE_A=$(mount_fuse "$MNT_A" a) || { fail "mount A"; exit 1; }
FUSE_B=$(mount_fuse "$MNT_B" b) || { fail "mount B"; exit 1; }
say "mount A pid=$FUSE_A, mount B pid=$FUSE_B, fuse=$FUSE_BIN"

for v in v1 v2 v3 v4; do head -c $((FILE_MIB << 20)) /dev/urandom > "$WORK/$v"; done
cp "$WORK/v1" "$MNT_B/f.bin"; sync
[ "$(sha "$MNT_A/f.bin")" = "$(sha "$WORK/v1")" ] || { fail "seeded file reads back wrong on A"; exit 1; }

# SHARED
python3 -c 'import mmap, os, sys
fd = os.open(sys.argv[1], os.O_RDONLY); m = mmap.mmap(fd, 0, prot=mmap.PROT_READ); m[:16]' "$MNT_A/f.bin" \
  2> "$WORK/shared.err" && say "SHARED ok" || fail "SHARED: MAP_SHARED mmap failed: $(cat "$WORK/shared.err")"

# KEEP
cat "$MNT_A/f.bin" > /dev/null
r=$(resident "$MNT_A/f.bin")
[ "${r:-0}" -ge 90 ] && say "KEEP ok ($r% resident after reopen)" || fail "KEEP: only ${r}% resident after reopen"

# REOPEN
rewrite "$MNT_B/f.bin" "$WORK/v2"
got=$(sha "$MNT_A/f.bin")
[ "$got" = "$(sha "$WORK/v2")" ] && say "REOPEN ok" \
  || fail "REOPEN: A read $( [ "$got" = "$(sha "$WORK/v1")" ] && echo 'the OLD bytes' || echo 'unknown bytes') after B's rewrite"

# HELD
python3 - "$MNT_A/f.bin" "$(sha "$WORK/v3")" "$WORK/held" "$HELD_SECS" > "$WORK/held.log" 2>&1 <<'EOF' &
import hashlib, os, sys, time
path, want, flag, secs = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
fd = os.open(path, os.O_RDONLY); size = os.fstat(fd).st_size
def h(): return hashlib.sha256(os.pread(fd, size, 0)).hexdigest()
h(); open(flag + ".ready", "w").close()
while not os.path.exists(flag + ".written"): time.sleep(0.05)
deadline = time.time() + secs
while time.time() < deadline:
    if h() == want:
        print(f"new bytes after {secs - (deadline - time.time()):.2f}s"); sys.exit(0)
    time.sleep(0.1)
print("still old bytes"); sys.exit(1)
EOF
HOLDER=$!
for i in $(seq 1 100); do [ -e "$WORK/held.ready" ] && break; sleep 0.1; done
rewrite "$MNT_B/f.bin" "$WORK/v3"; touch "$WORK/held.written"
wait "$HOLDER" && say "HELD ok ($(cat "$WORK/held.log"))" || fail "HELD: $(cat "$WORK/held.log")"
HOLDER=""

# TAIL
head -c 1048576 /dev/urandom > "$WORK/tail"
python3 - "$MNT_A/f.bin" "$WORK/tail" "$WORK/tailflag" "$HELD_SECS" > "$WORK/tail.log" 2>&1 <<'EOF' &
import os, sys, time
path, src, flag, secs = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
want = open(src, "rb").read()
fd = os.open(path, os.O_RDONLY); size = os.fstat(fd).st_size
os.pread(fd, size, 0); assert os.pread(fd, 4096, size) == b"", "read past EOF before the append"
open(flag + ".ready", "w").close()
while not os.path.exists(flag + ".written"): time.sleep(0.05)
deadline = time.time() + secs
while time.time() < deadline:
    if os.pread(fd, len(want), size) == want:
        print(f"appended bytes after {secs - (deadline - time.time()):.2f}s"); sys.exit(0)
    time.sleep(0.1)
print(f"still EOF at {size} (fstat now says {os.fstat(fd).st_size})"); sys.exit(1)
EOF
HOLDER=$!
for i in $(seq 1 100); do [ -e "$WORK/tailflag.ready" ] && break; sleep 0.1; done
cat "$WORK/tail" >> "$MNT_B/f.bin"; touch "$WORK/tailflag.written"
wait "$HOLDER" && say "TAIL ok ($(cat "$WORK/tail.log"))" || fail "TAIL: $(cat "$WORK/tail.log")"
HOLDER=""

# PREFETCH
head -c $((64 << 20)) /dev/urandom > "$WORK/p1"; head -c $((64 << 20)) /dev/urandom > "$WORK/p2"
cp "$WORK/p1" "$MNT_B/p.bin"; sync
python3 - "$MNT_A/p.bin" "$WORK/p2" "$WORK/pf" > "$WORK/pf.log" 2>&1 <<'EOF' &
import os, sys, time
path, newf, flag = sys.argv[1:4]
new = open(newf, "rb").read()
fd = os.open(path, os.O_RDONLY)
for off in range(0, 16 << 20, 1 << 20):
    os.pread(fd, 1 << 20, off)
time.sleep(1)  # the blocks ahead land
open(flag + ".ready", "w").close()
while not os.path.exists(flag + ".written"): time.sleep(0.05)
deadline = time.time() + 3
while time.time() < deadline:
    if os.pread(fd, 32 << 20, 16 << 20) == new[16 << 20:48 << 20]:
        print("new bytes past the prefetch front"); sys.exit(0)
    time.sleep(0.1)
print("still old bytes past the prefetch front"); sys.exit(1)
EOF
HOLDER=$!
for i in $(seq 1 100); do [ -e "$WORK/pf.ready" ] && break; sleep 0.1; done
rewrite "$MNT_B/p.bin" "$WORK/p2"; touch "$WORK/pf.written"
wait "$HOLDER" && say "PREFETCH ok ($(cat "$WORK/pf.log"))" || fail "PREFETCH: $(cat "$WORK/pf.log")"
HOLDER=""

# LOCAL
python3 - "$MNT_A/f.bin" <<'EOF' > "$WORK/local.log" 2>&1 && say "LOCAL ok" || fail "LOCAL: $(cat "$WORK/local.log")"
import mmap, os, sys
p = sys.argv[1]
w = os.open(p, os.O_RDWR); r = os.open(p, os.O_RDONLY)
m = mmap.mmap(r, 0, prot=mmap.PROT_READ)
old = os.pread(r, 4096, 8192)
new = bytes(b ^ 0xFF for b in old)
os.pwrite(w, new, 8192)
assert os.pread(r, 4096, 8192) == new, "another fd read the old bytes"
assert m[8192:12288] == new, "the shared mapping holds the old bytes"
os.close(w)
EOF

# MMAPW
python3 - "$MNT_A/f.bin" "$WORK/v4" <<'EOF' > "$WORK/mmapw.log" 2>&1 || fail "MMAPW (store): $(cat "$WORK/mmapw.log")"
import mmap, os, sys
p, src = sys.argv[1], sys.argv[2]
fd = os.open(p, os.O_RDWR)
m = mmap.mmap(fd, 0)  # MAP_SHARED, read/write
data = open(src, "rb").read()
m[:len(data)] = data
m.close(); os.close(fd)  # no msync
EOF
got=$(head -c $((FILE_MIB << 20)) "$MNT_B/f.bin" | sha256sum | cut -d' ' -f1)
[ "$got" = "$(sha "$WORK/v4")" ] && say "MMAPW ok" || fail "MMAPW: B did not read the bytes stored through A's shared mapping"

if [ "$FAIL" = 0 ]; then say "PASS"; else say "FAILED (logs in $WORK)"; fi
exit "$FAIL"
