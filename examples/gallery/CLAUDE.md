# Gallery architecture

`main.rs` owns the single-threaded compio application, scoped Autumn client,
uploads, image thumbnail generation, and the striped-video-to-HLS pipeline.
The browser reads inline originals and HLS artifacts; transient video originals
are private to the transcoder and `/get/` rejects them with 404.

`cache.rs` owns HTTP cache policy and conditional responses. Media and the
embedded page use `no-cache` and SHA-256 ETags derived from the actual response
bytes. API results and errors use `no-store`, including router/extractor errors
through the outer response middleware. Never issue a 304 before confirming a
resource still exists. Never use length, filename or an independently written
sidecar as a strong validator: same-name overwrites can invalidate those claims.

Original-file reads use `get_pooled().freeze()` and byte slices, retaining the
pooled response buffer without copying. This reads and hashes a full inline
value even for HEAD/304/ranges, because the storage head API has no version.
Memory is O(value size), replacing the former 4 MiB range-streaming path.
HLS and thumbnails already used full-value reads. Striped videos keep bounded
streaming for uploads and transcode downloads and never enter this HTTP path.

Validation: `cargo test -p gallery` covers validators, same-size replacement,
Range/If-Range, HEAD, empty values, and router-level cache policy.
