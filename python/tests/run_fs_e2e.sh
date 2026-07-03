#!/usr/bin/env bash
# F-FS-UNIFY M2 — headless e2e for the `autumn.Fs` PyO3 binding.
#
#   1. build + install the `autumn` wheel (with Fs) into a throwaway venv,
#   2. bring up an ISOLATED minimal cluster (memory-only manager, 1 EN, 1 PS,
#      loopback, no etcd) from this tree's debug binaries,
#   3. drive the Fs surface from Python — create/write/flush/read byte-exact
#      (inline + multi-extent + ranged), readdir/lookup/resolve, mkdir/rename/
#      unlink/truncate, a lease acquire→heartbeat→release smoke, and a
#      CROSS-INSTANCE read (write via one Fs, read byte-exact via a second),
#   4. tear everything down.
# Does NOT touch any system venv or any other cluster.
#
#   cargo build --workspace          # debug binaries first
#   bash python/tests/run_fs_e2e.sh
set -u
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AFS_WORK:-/tmp/afs-pye2e}"
VENV="${AFS_VENV:-/tmp/afs-venv}"
MGR="127.0.0.1:19401"
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[fs-e2e] build autumn wheel (with Fs) into venv"
rm -rf "$VENV"; python3 -m venv "$VENV"
"$VENV/bin/pip" install -q maturin 2>&1 | tail -1
# shellcheck disable=SC1091
source "$VENV/bin/activate"
( cd python && maturin develop 2>&1 | tail -2 ) || { echo "FAIL maturin"; exit 1; }

echo "[fs-e2e] cluster bring-up"
"$BIN/autumn-manager-server" --port 19401 --listen 127.0.0.1 >"$WORK/mgr.log" 2>&1 & PIDS+=($!)
wait_port 19401 20 || { echo FAIL mgr; tail -6 "$WORK/mgr.log"; exit 1; }
"$BIN/autumn-op" --manager "$MGR" format --listen :19411 --advertise 127.0.0.1:19411 "$WORK/en0" >"$WORK/fmt.log" 2>&1 || { echo FAIL fmt; cat "$WORK/fmt.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WORK/en0" --port 19411 --manager "$MGR" --cpuset 0 --listen 127.0.0.1 >"$WORK/en0.log" 2>&1 & PIDS+=($!)
wait_port 19411 20 || { echo FAIL en; tail -6 "$WORK/en0.log"; exit 1; }
sleep 3
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 >"$WORK/bs.log" 2>&1 || { echo FAIL bootstrap; cat "$WORK/bs.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port 19421 --manager "$MGR" --data "$WORK/ps1" --listen 127.0.0.1 --advertise 127.0.0.1:19421 >"$WORK/ps1.log" 2>&1 & PIDS+=($!)
wait_port 19421 20 || { echo FAIL ps; tail -6 "$WORK/ps1.log"; exit 1; }
sleep 4

echo "[fs-e2e] python headless correctness via autumn.Fs"
AUTUMN_MANAGER="$MGR" python - <<'PY'
import os, autumn
MGR = os.environ["AUTUMN_MANAGER"]
ROOT = 1  # ROOT_INO (FUSE_ROOT_ID)
DT_DIR, DT_REG = 4, 8

fs = autumn.Fs.connect(MGR)

# ── small (single-extent) file: create → write → flush → read byte-exact ──
ino = fs.create(ROOT, "hello.txt")
assert isinstance(ino, int) and ino > ROOT, ino
small = b"hello autumn.Fs\n" * 100          # 1600 B
assert fs.write(ino, 0, small) == len(small)
fs.flush(ino)
assert fs.read(ino, 0, len(small)) == small
# read past EOF clamps to file bytes
assert fs.read(ino, 0, len(small) + 4096) == small

# ── getattr / resolve / readdir / lookup ─────────────────────────────────
info = fs.getattr(ino)
assert info["size"] == len(small) and info["type"] == "file", info
assert fs.resolve("/hello.txt") == ino
assert fs.resolve("hello.txt") == ino
assert fs.resolve("/") == ROOT
assert fs.resolve("/does-not-exist") is None
ents = {name: (cino, kind) for (name, cino, kind) in fs.readdir(ROOT)}
assert "hello.txt" in ents and ents["hello.txt"] == (ino, DT_REG), ents
assert "." not in ents and ".." not in ents
lk = fs.lookup(ROOT, "hello.txt")
assert lk == (ino, DT_REG), lk
assert fs.lookup(ROOT, "nope") is None

# ── directory + multi-extent (10 MiB > 8 MiB MAX_EXTENT) file ─────────────
d = fs.mkdir(ROOT, "sub")
assert fs.getattr(d)["type"] == "directory"
f2 = fs.create(d, "inner.bin")
big = bytes(range(256)) * (40 * 1024)       # 10 MiB, spans multiple extents
assert fs.write(f2, 0, big) == len(big)
fs.flush(f2)
assert fs.read(f2, 0, len(big)) == big
# ranged read straddling an 8 MiB extent boundary
B = 8 * 1024 * 1024
assert fs.read(f2, B - 10, 20) == big[B - 10 : B + 10]
assert fs.resolve("/sub/inner.bin") == f2

# ── rename (across dirs) preserves content ───────────────────────────────
fs.rename(ROOT, "hello.txt", d, "renamed.txt")
assert fs.resolve("/hello.txt") is None
assert fs.resolve("/sub/renamed.txt") == ino
assert fs.read(ino, 0, len(small)) == small

# ── truncate (shrink) ────────────────────────────────────────────────────
fs.truncate(f2, 100)
assert fs.getattr(f2)["size"] == 100
assert fs.read(f2, 0, 4096) == big[:100]

# ── unlink ───────────────────────────────────────────────────────────────
fs.unlink(d, "renamed.txt")
assert fs.resolve("/sub/renamed.txt") is None

# ── negative write offset is rejected (coco P1: `offset as u64` overflow) ─
try:
    fs.write(ino, -1, b"x")
    raise AssertionError("negative offset must be rejected")
except RuntimeError:
    pass

# ── lease acquire → heartbeat → release smoke (M2 thin wrappers) ─────────
lf = fs.create(ROOT, "leased.bin")
ep = fs.acquire(lf, "w")
assert isinstance(ep, int), ep
assert fs.heartbeat(lf) is True
fs.release(lf)

# ── lease refcount: nested acquire needs matching releases (coco P2#4) ────
lf2 = fs.create(ROOT, "leased2.bin")
fs.acquire(lf2, "w")
fs.acquire(lf2, "w")           # refcount → 2
fs.release(lf2)                # refcount → 1: lease MUST stay held
assert fs.heartbeat(lf2) is True, "lease released too early (refcount ignored)"
fs.release(lf2)                # refcount → 0: manager release fires

print("PY M2 single-instance surface OK")

# ── CROSS-INSTANCE: write via `fs`, read byte-exact via a fresh `fs2` ─────
xino = fs.create(ROOT, "cross.bin")
payload = os.urandom(3 * 1024 * 1024)       # 3 MiB
assert fs.write(xino, 0, payload) == len(payload)
fs.flush(xino)

fs2 = autumn.Fs.connect(MGR)
assert fs2.resolve("/cross.bin") == xino
assert fs2.getattr(xino)["size"] == len(payload)
assert fs2.read(xino, 0, len(payload)) == payload
# the shrink `fs` committed above is visible to a second client
assert fs2.getattr(f2)["size"] == 100
assert fs2.read(f2, 0, 4096) == big[:100]
fs2.close()

fs.close()
print("PY M2 CROSS-INSTANCE byte-exact OK")
PY
RC=$?; echo "===== fs-e2e exit: $RC ====="; exit $RC
