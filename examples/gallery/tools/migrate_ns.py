#!/usr/bin/env python3
"""One-time migration: move existing gallery keys under the new `gallery/` root.

The gallery example used to write bare keys (`<file>`, `.thumb/…`, `.hls/…`,
`.meta/…`) at the root of the shared keyspace, so its whole-store list scan
walked EVERY key in the cluster — and stalled once another app (autumn-memory
under `mem/`, kvcache under `kvc/`) had written a lot. The example now namespaces
everything under `gallery/`. This script renames the old keys so an existing
gallery keeps working after the upgrade.

For each old gallery key it does get → put(`gallery/`+key) → delete, via the
`autumn-client` CLI (sequential — one key at a time — to avoid the ephemeral-port
exhaustion of process-per-key fan-out).

What migrates:
  * inline files      — bare `<name>` with a NON-video extension (images/pdf/text)
  * `.thumb/…`, `.hls/…`, `.meta/…` — thumbnails / HLS output / per-file meta

What is SKIPPED (and why):
  * `gallery/…`       — already migrated
  * `mem/…`, `kvc/…`  — other apps' namespaces, not ours
  * bare `<name>` with a VIDEO extension — a striped-upload original whose chunks
    live in the SDK's reserved namespace and can't be renamed by a plain get/put.
    A FINISHED video's playable form is its `.hls/…` (which DOES migrate); only an
    original still mid-transcode is lost — re-upload it.
  * binary / undecodable keys (e.g. the striped chunk namespace)

Usage:
    python3 examples/gallery/tools/migrate_ns.py --manager 127.0.0.1:9001 [--dry-run]

Requires a FRESH `autumn-client` on PATH or at target/debug/autumn-client (rebuild
it if you hit a wire-version mismatch). Stop the gallery server first.
"""
import argparse
import subprocess
import sys
import tempfile

ROOT = "gallery/"
DERIVED_PREFIXES = (".thumb/", ".hls/", ".meta/")
VIDEO_EXTS = {"mp4", "webm", "ogg", "mov", "m4v"}
# Whitelist of gallery inline-file extensions. Bare keys are ONLY migrated when
# they match this — so foreign bare keys (e.g. autumn-memory's percent-encoded
# `crates%2F…%3A%3A…` symbol keys, which happen to sit at the root) are never
# touched. Gallery stores raw filenames, so a bare key containing '%' or '/' is
# by construction NOT gallery's and is skipped.
GALLERY_EXTS = {
    "jpg", "jpeg", "png", "gif", "webp", "bmp", "svg",  # images
    "pdf", "txt", "md", "csv", "json",                   # docs
    "mp4", "webm", "ogg", "mov", "m4v",                  # videos (skipped below)
}


def ext_of(name: str) -> str:
    return name.rsplit(".", 1)[-1].lower() if "." in name else ""


def classify(key: str) -> str:
    """Return 'migrate', 'skip-video', 'skip-foreign', or 'skip'."""
    if key.startswith(ROOT):
        return "skip"  # already migrated
    if key.startswith(DERIVED_PREFIXES):
        return "migrate"  # .thumb/.hls/.meta are gallery-exclusive prefixes
    # A gallery inline file is a bare filename: no '/', no percent-encoding, and
    # a recognized media/doc extension. Anything else at the root belongs to
    # another app (or is junk) — do NOT touch it.
    if "/" in key or "%" in key:
        return "skip-foreign"
    ext = ext_of(key)
    if ext in VIDEO_EXTS:
        return "skip-video"
    if ext in GALLERY_EXTS:
        return "migrate"
    return "skip-foreign"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manager", default="127.0.0.1:9001")
    ap.add_argument("--bin", default="target/debug/autumn-client")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=5_000_000, help="max keys to scan")
    args = ap.parse_args()

    base = [args.bin, "--manager", args.manager]

    def run(cmd, **kw):
        return subprocess.run(base + cmd, capture_output=True, **kw)

    # Enumerate the whole keyspace once (bare gallery originals share no prefix,
    # so a full scan is unavoidable — but it's one-off).
    print("scanning keyspace …", file=sys.stderr)
    r = run(["ls", "--prefix", "", "--limit", str(args.limit)], text=True)
    if r.returncode != 0:
        sys.exit(f"ls failed: {r.stderr.strip()}")
    keys = [ln for ln in r.stdout.splitlines() if ln and not ln.startswith("(")]

    migrated = skipped_video = skipped_foreign = failed = 0
    for key in keys:
        kind = classify(key)
        if kind == "skip":
            continue
        if kind == "skip-foreign":
            skipped_foreign += 1
            continue
        if kind == "skip-video":
            skipped_video += 1
            print(f"  skip (striped video original, re-upload if needed): {key}", file=sys.stderr)
            continue

        newkey = ROOT + key
        if args.dry_run:
            print(f"  would migrate: {key} -> {newkey}")
            migrated += 1
            continue

        # get → put(new) → del(old), value through a temp file (handles binary).
        with tempfile.NamedTemporaryFile() as tmp:
            g = run(["get", key])
            if g.returncode != 0:
                print(f"  FAIL get {key}: {g.stderr.decode(errors='replace').strip()}", file=sys.stderr)
                failed += 1
                continue
            tmp.write(g.stdout)
            tmp.flush()
            p = run(["put", newkey, tmp.name])
            if p.returncode != 0:
                print(f"  FAIL put {newkey}: {p.stderr.decode(errors='replace').strip()}", file=sys.stderr)
                failed += 1
                continue
            run(["del", key])  # best-effort; new key already written
            migrated += 1
            if migrated % 100 == 0:
                print(f"  … {migrated} migrated", file=sys.stderr)

    print(f"\ndone: migrated={migrated} skipped_video={skipped_video} "
          f"skipped_foreign={skipped_foreign} failed={failed}"
          f"{' (dry-run)' if args.dry_run else ''}", file=sys.stderr)


if __name__ == "__main__":
    main()
