#!/usr/bin/env bash
# F-FS-UNIFY M4 — REAL cross-surface interop: an autumn-fuse kernel MOUNT and
# the autumn_fsspec Python facade on ONE cluster, one shared inode layout.
#
#   1. build the wheel (with Fs) + server + autumn-fuse binaries,
#   2. bring up an ISOLATED cluster (memory-mode manager, 1 EN, 1 PS),
#   3. MOUNT autumn-fuse at $WORK/mnt (needs /dev/fuse + fusermount3),
#   4. write a file THROUGH THE MOUNT (POSIX open/write/close) → read it
#      byte-exact via fsspec; and write via fsspec → read it byte-exact through
#      the mount — both a tiny + a 10 MiB (multi-extent) file each way,
#   5. unmount + tear down.
# Requires FUSE (skips cleanly if /dev/fuse or fusermount3 is absent).
#
#   cargo build --workspace
#   bash python/autumn_fsspec/tests/run_mount_fsspec_interop.sh
set -u
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2

[ -e /dev/fuse ] || { echo "SKIP: no /dev/fuse (FUSE unavailable)"; exit 0; }
command -v fusermount3 >/dev/null || command -v fusermount >/dev/null || { echo "SKIP: no fusermount(3)"; exit 0; }
UMOUNT=$(command -v fusermount3 || command -v fusermount)

BIN=target/debug
WORK="${AMI_WORK:-/tmp/ami-interop}"
VENV="${AMI_VENV:-/tmp/ami-venv}"
MNT="$WORK/mnt"
MGR="127.0.0.1:19701"
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps autumn-fuse; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1" "$MNT"
PIDS=()
cleanup() { "$UMOUNT" -u "$MNT" 2>/dev/null; for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[interop] build autumn wheel into venv"
rm -rf "$VENV"; python3 -m venv "$VENV"
"$VENV/bin/pip" install -q maturin fsspec -e python/autumn_fsspec >"$WORK/pip.log" 2>&1 || { echo "FAIL pip"; tail -8 "$WORK/pip.log"; exit 1; }
# shellcheck disable=SC1091
source "$VENV/bin/activate"
( cd python && maturin develop 2>&1 | tail -2 ) || { echo "FAIL maturin"; exit 1; }

echo "[interop] cluster bring-up"
"$BIN/autumn-manager-server" --port 19701 --listen 127.0.0.1 >"$WORK/mgr.log" 2>&1 & PIDS+=($!)
wait_port 19701 20 || { echo FAIL mgr; tail -6 "$WORK/mgr.log"; exit 1; }
"$BIN/autumn-op" --manager "$MGR" format --listen :19711 --advertise 127.0.0.1:19711 "$WORK/en0" >"$WORK/fmt.log" 2>&1 || { echo FAIL fmt; cat "$WORK/fmt.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WORK/en0" --port 19711 --manager "$MGR" --cpuset 0 --listen 127.0.0.1 >"$WORK/en0.log" 2>&1 & PIDS+=($!)
wait_port 19711 20 || { echo FAIL en; tail -6 "$WORK/en0.log"; exit 1; }
sleep 3
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 >"$WORK/bs.log" 2>&1 || { echo FAIL bootstrap; cat "$WORK/bs.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port 19721 --manager "$MGR" --data "$WORK/ps1" --listen 127.0.0.1 --advertise 127.0.0.1:19721 >"$WORK/ps1.log" 2>&1 & PIDS+=($!)
wait_port 19721 20 || { echo FAIL ps; tail -6 "$WORK/ps1.log"; exit 1; }
sleep 4

echo "[interop] mount autumn-fuse at $MNT"
setsid "$BIN/autumn-fuse" --manager "$MGR" --mountpoint "$MNT" --transport tcp >"$WORK/fuse.log" 2>&1 < /dev/null & PIDS+=($!)
for _ in $(seq 1 20); do grep -q " $MNT fuse" /proc/mounts && break; sleep 1; done
grep -q " $MNT fuse" /proc/mounts || { echo "FAIL: autumn-fuse not mounted"; tail -20 "$WORK/fuse.log"; exit 1; }
sleep 2  # let Init (root inode create) settle before fsspec connects

echo "[interop] cross-surface byte-exact (mount ↔ fsspec)"
AUTUMN_MANAGER="$MGR" AUTUMN_MNT="$MNT" python - <<'PY'
import os, hashlib
from autumn_fsspec import AutumnFileSystem
MGR = os.environ["AUTUMN_MANAGER"]; MNT = os.environ["AUTUMN_MNT"]
fs = AutumnFileSystem(manager=MGR, skip_instance_cache=True)  # root="" → shares ROOT_INO with the mount
sha = lambda b: hashlib.sha256(b).hexdigest()

for name, size in [("small", 37), ("big", 10 * 1024 * 1024)]:
    # ── direction 1: write THROUGH THE MOUNT, read via fsspec ──
    payload = os.urandom(size)
    with open(os.path.join(MNT, f"m2f_{name}.bin"), "wb") as f:
        f.write(payload); f.flush(); os.fsync(f.fileno())
    got = fs.cat_file(f"m2f_{name}.bin")
    assert got == payload, f"mount→fsspec MISMATCH {name}: {len(got)} vs {size}"
    assert fs.info(f"m2f_{name}.bin")["size"] == size

    # ── direction 2: write via fsspec, read THROUGH THE MOUNT ──
    payload2 = os.urandom(size)
    fs.pipe_file(f"f2m_{name}.bin", payload2)
    with open(os.path.join(MNT, f"f2m_{name}.bin"), "rb") as f:
        got2 = f.read()
    assert got2 == payload2, f"fsspec→mount MISMATCH {name}: {len(got2)} vs {size}"
    print(f"  {name} ({size}B): mount→fsspec OK, fsspec→mount OK  (sha {sha(payload)[:8]} / {sha(payload2)[:8]})")

# a directory made via the mount is a real dir to fsspec, and vice versa
os.mkdir(os.path.join(MNT, "mdir"))
with open(os.path.join(MNT, "mdir/inner.txt"), "wb") as f:
    f.write(b"via-mount")
assert fs.info("mdir")["type"] == "directory"
assert fs.cat_file("mdir/inner.txt") == b"via-mount"
assert "mdir/inner.txt" in set(fs.find("mdir"))

fs.makedirs("fdir/sub")
fs.pipe_file("fdir/sub/x.bin", b"via-fsspec")
assert os.path.isdir(os.path.join(MNT, "fdir/sub"))
with open(os.path.join(MNT, "fdir/sub/x.bin"), "rb") as f:
    assert f.read() == b"via-fsspec"

print("PY INTEROP OK: fuse mount + fsspec are one filesystem (byte-exact both ways)")
PY
RC=$?; echo "===== interop exit: $RC ====="; exit $RC
