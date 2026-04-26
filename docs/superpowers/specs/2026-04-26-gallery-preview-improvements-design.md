# Gallery Preview Improvements — Design

**Date:** 2026-04-26
**Component:** `examples/gallery` (single-page demo for autumn-rs)
**Status:** Approved (conversation 2026-04-26)

## Goals

Improve the visual and interactive quality of `examples/gallery`'s media preview, while also making autumn-rs's storage characteristics more visible to viewers. Mix of three intents:

- **A. Showcase autumn-rs** → make storage performance visible.
- **B. Daily-use image host** → grid view, fast scanning, lightbox strip nav.
- **C. Polished example** → smooth transitions, color-aware backdrop, motion polish.

## Non-Goals

- Authentication, share-link signing, multi-user permissions.
- Video thumbnails (would require ffmpeg).
- AVIF / HEIC support.
- Mobile-first redesign (best-effort responsive only).
- Server-side image format conversion beyond thumbnails.

## Architecture Overview

```
┌──────────────────────────── Frontend (index.html) ────────────────────────────┐
│  List / Grid view toggle ── thumbnails via /thumb/<name>                       │
│  Lightbox: 2-layer image crossfade + 2-layer backdrop + thumbnail strip        │
│  HUD: PerformanceObserver → latency / throughput overlay                       │
└────────────────────────────────────┬───────────────────────────────────────────┘
                                     │
                ┌────────────────────┼─────────────────────┐
                │ /get /thumb /ls /del /put                │
                ▼                                          ▼
┌─────────────── gallery server (main.rs) ──────────────────────────────────────┐
│  GET  /thumb/<name>  →  cache hit:  get .thumb/320/<name>.webp                 │
│                          cache miss: get original → image::resize → put cache  │
│  DELETE /del/<name>  →  delete original + delete .thumb/320/<name>.webp        │
│  POST   /put/<name>  →  delete .thumb/320/<name>.webp (invalidate)             │
│  GET    /ls          →  filter out keys starting with ".thumb/"                │
└────────────────────────────────────┬───────────────────────────────────────────┘
                                     ▼
                          ┌─── ClusterClient ───┐
                          │  autumn KV          │
                          └─────────────────────┘
```

**New dependency** (gallery only): `image = { version = "0.25", default-features = false, features = ["jpeg", "png", "webp", "gif", "bmp"] }`. No AVIF, no system deps.

**Cache key naming**: `.thumb/320/<original-name>.webp`. Single fixed size (320 px). CSS scales 320 → 96 px for the lightbox strip; downscale-in-browser is good enough.

## Component Specs

### 1. Server-side thumbnails (B1)

**New handler** `GET /thumb/{name}`:

1. If extension is video → respond `404` (frontend handles fallback icon).
2. If extension is `svg` → proxy original `/get/<name>` bytes with svg mime.
3. `client.get(thumb_key)` → if found, return with `image/webp` mime + `Cache-Control: public, max-age=86400`.
4. Cache miss: `client.get(original)` → `image::load_from_memory` → `.thumbnail(320, 320)` → encode as WebP → `client.put(thumb_key, bytes)` → return.
5. Decode/encode error → log, fall back to serving the original (graceful degradation).

**Modified `delete_handler_inner`**:

- Delete original first; on success, fire-and-forget `client.delete(thumb_key)`. NotFound is ignored.

**Modified `put_handler_inner`** (re-upload invalidation):

- After putting the original, fire-and-forget `client.delete(thumb_key)`. Next `/thumb/` request regenerates.

**Modified `ls_handler`**:

- Filter out keys starting with `.thumb/` before returning.

**Concurrency**: simultaneous misses both compute and `put`; second wins. Acceptable for a demo. No per-key dedup needed (YAGNI).

**Threading**: `image::load_from_memory` is CPU-bound. For a single-user demo on compio thread-per-core this is fine inline.

**Acceptance:**

- `curl -I /thumb/foo.jpg` returns webp mime, second call is sub-millisecond.
- `DELETE /del/foo.jpg` followed by `GET /thumb/foo.jpg` returns 404 (or regenerates if foo.jpg also re-uploaded).
- `GET /ls` does not list `.thumb/...` entries.

### 2. Grid view toggle (B2)

- Add a 2-button toggle in the file-list header: `≡ List` / `▦ Grid`.
- Persist to `localStorage.galleryViewMode` (default: `list` — preserves current first-load behavior).
- Grid CSS: `display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr))`, ~140×140 cells; thumb on top, filename truncated below.
- Cell click = select; double-click or `⛶` button = lightbox.
- Videos in grid: dark cell + ▶ glyph + filename (no thumbnail generated).
- Non-media files (txt/pdf/etc.): ext label cell, mirrors current `.file-ext` style.

**Acceptance:**

- Toggle persists across reloads.
- Switching modes does not lose selection.
- Video cells render the play glyph (no broken-image icon).

### 3. Lightbox thumbnail strip + ←/→ keys (B3)

- New `<div class="lb-strip">` between image and caption, ~80 px tall.
- Horizontal scroll, snap to thumb, current thumb highlighted with accent border.
- Click thumb → navigate via the same code path as ←/→ keys.
- Auto-scroll active thumb into view (smooth) on navigation.
- Strip click does **not** close lightbox (handler stops propagation).
- Add `ArrowLeft`/`ArrowRight` as aliases for `ArrowUp`/`ArrowDown` when lightbox is open. Up/Down still work for list-view nav as today.
- Strip only renders media files (images + videos, same set as nav indices).

**Acceptance:**

- Opening lightbox shows strip with current thumb centered.
- ← and → navigate; up/down still work too.
- Clicking outside the strip + outside the image still closes the lightbox.

### 4. Image crossfade + direction-aware slide (C2)

- Lightbox now has **two stacked `<img>` layers** (`lbImageA`, `lbImageB`) and **two backdrops** (`lbBackdropA`, `lbBackdropB`). The "active" one sits on top.
- On navigate:
  1. Determine direction (prev/next).
  2. Preload new image via `Image.decode()` (existing logic — re-used).
  3. Set the *inactive* image and backdrop to the new url.
  4. Trigger transition: inactive layer slides in from right (next) or left (prev) **+** fades 0→1 over 220 ms; active layer fades 1→0 over the same window.
  5. Swap "active" pointer (`activeLayer = 'A' | 'B'`).
- `prefers-reduced-motion: reduce` → disable slide; keep crossfade only.

**Acceptance:**

- Pressing → 5 times shows 5 smooth slide-in transitions, no flash.
- With `prefers-reduced-motion` set in browser, transitions become a plain crossfade.

### 5. Dominant color backdrop (C3)

- After a new lightbox image decodes, draw it onto an offscreen 8×8 canvas, `getImageData`, average RGB ignoring near-black/near-white pixels (so a black corner doesn't dominate).
- Set CSS var on lightbox root: `lightbox.style.setProperty('--lb-dominant', 'rgb(r,g,b)')`.
- Lightbox base layer: `background: var(--lb-dominant, rgba(5,5,6,0.6))`, `transition: background-color 300ms`.
- Blurred image stays on top (current `.lb-backdrop` look) but its opacity drops to ~0.85 so the dominant color shows at edges where blur fades out.

**Acceptance:**

- Opening a sunset photo tints the lightbox edges orange; opening a forest photo tints them green.
- Switching between two images visibly transitions the backdrop color.

### 6. Performance HUD (A)

- Fixed-position panel (top-right), semi-transparent. Press **`** (backtick) to toggle. Default off.
- `PerformanceObserver({ type: 'resource', buffered: true })`.
- For each entry whose URL matches `/get/` or `/thumb/`, capture: filename, transferSize, duration, `responseStart - requestStart` (TTFB).
- HUD displays:
  - **Last request**: filename · size · TTFB · total · MB/s
  - **Session totals**: requests · bytes · avg MB/s
- Per-request "range used" detection is dropped (PerformanceResourceTiming doesn't expose status code reliably). Instead, when a video is open the caption shows a small `range` badge (static, based on file ext).

**Acceptance:**

- Toggling backtick shows/hides the HUD.
- Loading a 4 K image updates "Last request" with sane TTFB (< 50 ms locally) and MB/s figures.
- Session totals accumulate across multiple lightbox opens.

### 7. Typography / motion polish (C7)

- Token cleanup in `:root`: a single `--ease`, `--dur-fast`, `--dur-slow`; replace per-rule magic numbers.
- `font-feature-settings: 'tnum' 1` on numeric containers (file sizes, HUD).
- `:focus-visible` ring on all interactive elements (currently sparse).
- Wrap motion in `@media (prefers-reduced-motion: no-preference)` where applicable.
- Replace `loading.gif` (54 KB) with a CSS conic-gradient spinner. Remove the route + asset + `include_bytes!`.
- Tighten spacing scale: 4 / 8 / 12 / 16 / 24 / 32. Audit existing rules.

**Acceptance:**

- `loading.gif` is gone from disk and from `main.rs` `include_bytes!`.
- Spinner is visible during slow loads (throttle network in devtools to verify).
- Tab key cycles through buttons with a visible focus ring.

## Build Order

Each step is independently shippable, testable, and gets its own commit.

| # | Step | Surface |
|---|------|---------|
| 1 | server-thumbnails | `gallery/Cargo.toml`, `gallery/src/main.rs` |
| 2 | grid-view | `gallery/static/index.html` (CSS + JS) |
| 3 | lb-strip | `gallery/static/index.html` |
| 4 | dominant-color | `gallery/static/index.html` |
| 5 | crossfade | `gallery/static/index.html` |
| 6 | polish | `gallery/static/index.html`, `gallery/src/main.rs`, delete `gallery/static/loading.gif` |
| 7 | hud | `gallery/static/index.html` |

## Test Plan

Static demo — verification is largely manual.

- `cargo build -p gallery` after each backend-touching step.
- After step 1: hit `/thumb/<name>` twice, observe second call latency drop; `DELETE` then `GET /thumb` returns 404.
- After steps 2–7: launch the binary, exercise grid view, lightbox nav, dominant color, transitions, HUD toggle.
- `examples/gallery/README.md` (or root `README.md` gallery section) gets a "manual verification" subsection with curl + UI steps.

## Risks / Open Questions

- **GIF decoding**: `image` crate decodes the first frame only — animated GIF thumbnails are still images. Original GIF still served via `/get/`. Acceptable.
- **WebP encoding** with `default-features = false` requires explicit `webp` feature. Verified in cargo docs.
- **Compile time** increase: ~15 s on first build. Acceptable for a demo.
- **HUD accuracy** depends on browser exposing `transferSize` for cross-origin/disk-cached responses. Same-origin only here, so reliable.
