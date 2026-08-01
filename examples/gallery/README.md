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

## Range Reads

`GET /get/{name}` parses RFC 7233 byte ranges (`bytes=N-`, `bytes=N-M`,
`bytes=-N`) and streams the response in 4 MiB chunks back to the client.
`/get/` serves the **inline** uploads (images / PDFs / text); chunked
streaming keeps resident memory O(chunk) regardless of file size. Videos are
not served here — they're stored striped and the transcoder reads the source
back over the SDK's `get_stream` (see "Large Videos" / "Video Pipeline").

## Storage Layout

Files live in the cluster's KV store under these key conventions:

| Key pattern | Holds |
|---|---|
| `<filename>` | Original image / PDF / text upload (inline value); for a *transient* video original, the 28-byte stripe-meta blob — see below |
| `\xff\xfe…` (reserved) | Striped video chunks (`autumn-client` `put_stream` namespace; one 4 MiB chunk per key) |
| `.thumb/320/<filename>` | Cached 320 px-wide JPEG thumbnail |
| `.hls/<filename>/index.m3u8` | HLS playlist for a transcoded video |
| `.hls/<filename>/seg000.ts` … | HLS media segments |

Thumbnails, HLS segments, and the reserved stripe-chunk namespace are hidden
from `/list/`; thumbnails/HLS are served by dedicated routes (`/thumb/<name>`,
`/hls/<name>/<segment>`).

## Large Videos: Streaming (striped) Upload

Images, PDFs, and text are small, so they're stored as a single inline KV
value and byte-range-served via `/get/`. **Videos** are different: they can be
arbitrarily large and are never range-served (the frontend plays them through
HLS), so the upload handler detects them by extension and streams them with
`autumn-client`'s striped API:

- `put_stream_begin` opens a handle; the multipart field's network-sized
  chunks are coalesced into 4 MiB (`STRIPE_CHUNK_SIZE`) pieces and `send`-ed
  one at a time, then `commit` writes the meta blob (the atomic
  linearisation point). Resident memory stays O(chunk) and no single KV value
  has to hold the whole file — that's what makes multi-GiB videos uploadable.
- The transcoder reads the source back with `get_stream` + `next_chunk`
  (sequential — exactly the full-file access a transcode download needs).
- Deleting a video uses `delete_stream`, which cascades through the striped
  chunks plus the meta blob.

## Video Pipeline

When a video (`mp4` / `webm` / `ogg` / `mov` / `m4v`) is uploaded:

1. The original bytes are streamed into a striped value (see "Large Videos"
   above) keyed by `<filename>` — bounded memory, no single-value ceiling.
2. A background `compio::runtime::spawn` task downloads the source to a temp
   file with `get_stream` + `next_chunk`, then kicks off ffmpeg in
   `spawn_blocking`:
  - If the source is already compatible with the gallery's MPEG-TS HLS
    output (`h264` video plus copy-safe audio such as `aac` / `mp3` /
    `ac3` / `eac3`), it first tries a lossless `-c copy` passthrough.
  - Otherwise it falls back to `libx264 / aac, CRF 23, single bitrate`.
  - **Segment duration is adaptive, not a fixed 20 s.** Each `.ts` becomes one
    KV value, so it must stay under the partition server's 64 MiB inline-`put`
    cap (a fixed 20 s segment on a high-bitrate clip hit ~106 MiB and the
    upload was rejected with `CODE_VALUE_TOO_LARGE`). The pipeline picks
    `-hls_time` from the source byte-rate so an average segment targets ~48 MiB
    (clamped to 2–20 s). `-c copy` can only cut on the source's own keyframes,
    so if a segment *still* overshoots the cap (sparse keyframes) the pipeline
    re-encodes with `-force_key_frames` at the segment boundary, which bounds
    the segment size reliably.
   - Same pass extracts a 0.5 s thumbnail keyframe.
3. All produced files are written to `.hls/<filename>/...` and
   `.thumb/320/<filename>`.
4. The original is reaped with `delete_stream` (striped chunks + meta blob)
   to reclaim space.

Status is exposed at `GET /transcode-status/<filename>`:

```json
{ "status": "queued"  | "transcoding" | "done" | "failed", "error": "…" }
```

An in-progress (or failed) video is **not** in `/list/` yet — it has no HLS
playlist — so it would be invisible in the main grid. Instead the set of
in-progress transcodes is exposed at `GET /transcoding/`:

```json
[ { "name": "clip.mp4", "status": "queued" | "transcoding" | "failed" }, … ]
```

The frontend shows these in a dedicated **Transcoding** card (hidden when
empty), polling `/transcoding/` every 2 s. When a name drops out of the set it
finished, so the main file list is refreshed to reveal the new video. Failed
jobs stay in the card with a ⚠ and a delete button (they leave the original in
place, so you can clear it or re-upload after fixing the issue).

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

# 4b. While it's running, the in-progress list surfaces it (the main /list/
#     does not until the HLS playlist exists)
curl -sS http://127.0.0.1:5001/transcoding/   # e.g. [{"name":"sample.mp4","status":"transcoding"}]

# 5. Confirm HLS artifacts
curl -sS http://127.0.0.1:5001/hls/sample.mp4/index.m3u8 | head
curl -sSI http://127.0.0.1:5001/hls/sample.mp4/seg000.ts | grep -i content-type

# 5b. Every segment must be under the 64 MiB inline cap (adaptive -hls_time).
#     For a high-bitrate source, segments are shorter than 20 s.
for seg in $(curl -sS http://127.0.0.1:5001/hls/sample.mp4/index.m3u8 | grep '\.ts$'); do
  curl -sSI "http://127.0.0.1:5001/hls/sample.mp4/$seg" | grep -i content-length
done

# 6. Confirm the original was reaped
curl -sS -o /dev/null -w "%{http_code}\n" http://127.0.0.1:5001/get/sample.mp4   # expect 404

# 7. Tear down
./cluster.sh stop
```

In-browser verification: while ffmpeg runs the video shows in the separate
**Transcoding** card with a "转码中…" spinner; on completion it leaves that card
and appears in the main Files grid as a thumbnail with a play glyph, and plays
in both Chrome (hls.js) and Safari (native HLS).
