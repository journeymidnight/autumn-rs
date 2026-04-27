# Gallery Example

A small Axum-based web gallery on top of `autumn-client`. Serves images, PDFs,
text, and **HLS-encoded video** from the distributed KV store.

## Prerequisites

- A running autumn-rs cluster (manager + extent nodes + ps). The simplest path
  is `./cluster.sh start 3` from the repo root (after `cargo build --release
  --workspace`).
- `ffmpeg` 4.x or newer in `$PATH`. Required for video transcoding and image
  thumbnail extraction. On macOS: `brew install ffmpeg`.

## Run

```bash
cargo run --release -p gallery -- 127.0.0.1:9001
# then open http://localhost:5001
```

`127.0.0.1:9001` is the manager address. Override with the first CLI arg
if your cluster runs elsewhere.

## Storage Layout

Files live in the cluster's KV store under these key conventions:

| Key pattern | Holds |
|---|---|
| `<filename>` | Original image / PDF / text upload (and a *transient* video original — see below) |
| `.thumb/320/<filename>` | Cached 320 px-wide JPEG thumbnail |
| `.hls/<filename>/index.m3u8` | HLS playlist for a transcoded video |
| `.hls/<filename>/seg000.ts` … | HLS media segments |

Thumbnails and HLS segments are served by dedicated routes
(`/thumb/<name>`, `/hls/<name>/<segment>`) and hidden from `/list/`.

## Video Pipeline

When a video (`mp4` / `webm` / `ogg` / `mov` / `m4v`) is uploaded:

1. The original bytes are written to `<filename>` so ffmpeg can range-read
   them via the gallery's own `/get/` route.
2. A background `compio::runtime::spawn` task kicks off ffmpeg in
   `spawn_blocking`:
   - `libx264 / aac, CRF 23, 4-second segments, single bitrate`.
   - Same pass extracts a 0.5 s thumbnail keyframe.
3. All produced files are written to `.hls/<filename>/...` and
   `.thumb/320/<filename>`.
4. The original `<filename>` key is deleted to reclaim space.

Status is exposed at `GET /transcode-status/<filename>`:

```json
{ "status": "queued"  | "transcoding" | "done" | "failed", "error": "…" }
```

The frontend polls this once per cell on render, then with 2 / 5 / 10 second
backoff while in `queued` / `transcoding`. Failed jobs leave the original in
place so you can re-upload after fixing the issue.

## Playback

The frontend uses [hls.js](https://github.com/video-dev/hls.js/) loaded from
jsDelivr. Safari (and other browsers with native HLS) gets `<video src=…>`
directly; everything else attaches `Hls` and feeds segments via MSE.

## Server Restart

On startup the gallery scans the KV store and re-enqueues any video whose
original key is still present but whose `.hls/<filename>/index.m3u8` is
missing. This recovers in-flight transcodes from a process crash.

## Manual Verification

```bash
# 1. Bring up cluster + gallery
./cluster.sh start 3
cargo run --release -p gallery -- 127.0.0.1:9001 &

# 2. Open the UI
open http://localhost:5001

# 3. Upload a sample video
ffmpeg -y -f lavfi -i testsrc=duration=8:size=320x240:rate=30 \
       -f lavfi -i sine=frequency=440:duration=8 \
       -c:v libx264 -preset ultrafast -pix_fmt yuv420p -c:a aac \
       /tmp/sample.mp4
curl -sS -F "file=@/tmp/sample.mp4" http://127.0.0.1:5001/put/

# 4. Watch the status flip
for i in 1 2 3 4 5 6 7 8 9 10; do
  curl -sS http://127.0.0.1:5001/transcode-status/sample.mp4
  echo
  sleep 1
done

# 5. Confirm HLS artifacts
curl -sS http://127.0.0.1:5001/hls/sample.mp4/index.m3u8 | head
curl -sSI http://127.0.0.1:5001/hls/sample.mp4/seg000.ts | grep -i content-type

# 6. Confirm the original was reaped
curl -sS -o /dev/null -w "%{http_code}\n" http://127.0.0.1:5001/get/sample.mp4   # expect 404

# 7. Tear down
./cluster.sh stop
```

In-browser verification: the file appears with a "转码中…" placeholder while
ffmpeg runs, swaps to a thumbnail with a play glyph on completion, and plays
in both Chrome (hls.js) and Safari (native HLS).
