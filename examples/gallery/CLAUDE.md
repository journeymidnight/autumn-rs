# Gallery architecture

`main.rs` owns the single-threaded compio application, scoped Autumn client,
uploads, image thumbnail generation, and the striped-video-to-HLS pipeline.
The browser reads inline originals and HLS artifacts; transient video originals
are private to the transcoder and `/get/` rejects them with 404.

`cache.rs` owns HTTP cache policy and conditional responses. The served HTML,
CSS and JS use `no-cache` and independently compute SHA-256 ETags on every
request. The source HTML keeps CSS/JS inline for maintainability; `app_assets`
extracts them and emits references to their separate routes. Mutable media use
`no-cache` and `Last-Modified` from `uploaded_at` or `transcoded_at`. API results
and errors use `no-store`, including router/extractor errors through the outer
response middleware. Never issue a 304 before confirming a resource exists.
Replacement uploads synchronously remove old timestamp validators and the old
thumbnail before writing new bytes; an absent timestamp safely means 200.

Original-file reads use `head` plus the small `uploaded_at` metadata KV, then
stream a requested range in 4 MiB chunks. HLS and thumbnails use full-value
reads and their relevant metadata time. Striped videos keep bounded streaming
for uploads and transcode downloads and never enter the HTTP `/get/` path.

Validation stays proportionate for this example: compile it, run its existing
tests, and use the direct HTTP checks in `docs/ops.md`.
