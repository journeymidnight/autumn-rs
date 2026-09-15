# Receive-side copies per link, 2026-09-15

## Question and boundary

Is data copied more than necessary on the receiving side? Locate, separately for
the three links — write Client→PS, write PS→EN, read EN/PS→Client — the
application-layer copies, the TCP kernel copy and the UCX Stream unpack copy, and
remove the copies that are not required. Boundary: receive side of large values
(64 KiB, 1 MiB, 8 MiB), wire and storage formats unchanged, all-replica
durability and CRC coverage unchanged. Acceptance: each copy attributed to a code
location by measurement, avoidable application copies removed with byte-exact
tests passing, untraced throughput/CPU compared against the baseline in
interleaved runs, and remaining unavoidable copies explained.

## Method

H200-1 `dongmao-autumn`, Linux 6.1, UCX 1.16.0, Rust 1.95, branch
`compio-upgrade` at `ae06c1c` (baseline) and the same tree plus this change
(final). RF3 on /data03, /data05, /data08, one partition, client depth 8,
`controlled_path` with `write` (`put_bulk`), `read` (`get_pooled`, PS proxy) and
the new `direct` (`get_direct`, EN descriptor read) modes. Same-host TCP loopback
and same-host RoCE (`rc_mlx5,ud_mlx5,tcp,self`, `mlx5_1:1`); not cross-host.

`perf/receive_copies` (see its README and docs/ops.md) counts, per process and
fixed-work window:

- TCP kernel receive copy: `skb_copy_datagram_iter` bytes.
- UCX Stream unpack: memcpy/memmove calls returning into libucp/libuct.
- Application copies: memcpy/memmove ≥ 1 KiB returning into an autumn binary,
  resolved to file:line with `addr2line` (ASLR off for the traced processes;
  the uprobe sits on glibc's resolved memcpy IFUNC target).

Values below are bytes copied divided by logical bytes moved: 1.00x = one full
copy of every value on that process. The traced pass measures bytes only;
throughput and CPU come from separate untraced windows.

UCX Stream source (openucx v1.16.0 `src/ucp/stream/stream_recv.c`,
`stream_send.c`): Stream has no rendezvous protocol. Sends use eager AM (bcopy,
or zcopy for large fragments); a receive unpacks each arrived AM fragment into
the posted buffer (`ucp_stream_rdata_unpack`), and returns one fragment's worth
per call. Passing a registered `memh` does not change that.

## Baseline findings (before the change)

Attributed call sites, 8 MiB unless stated:

| Link | Receiver | Application copies found |
|---|---|---|
| Client→PS write | PS | `FrameDecoder::feed` of the 64 KiB read + `drain_bulk_writes` `drain_into` of the same prefix: 2.0x for a 64 KiB value, 0.016x at 8 MiB. The value tail is received straight into the pooled buffer. |
| PS→EN append | each EN | `FrameDecoder::feed` from the 512 KiB scratch `Vec` (`extent_node.rs` `handle_connection`): 1.0x on every replica, plus jemalloc regrowth under `try_decode` (0.02–0.4x on UCX). Appends carry the payload inside the CRC'd ctrl, so there was no pooled path. |
| EN→PS / PS→client / EN→client read | PS, client | `read_loop` feed + drain of the 64 KiB prefix: 2.0x for 64 KiB values on TCP, 0.016x at 8 MiB. Tail received into the pooled buffer. |

Transport copies: exactly 1.00x per receiving process — TCP kernel copy on TCP,
UCX Stream unpack on UCX (including receives into registered pooled slabs). The
PS send side, the EN decode → owner mailbox → pwritev chain and the read-response
sends (head + value iovecs) do not copy the value.

## Change

Receive loops read into the `FrameDecoder`'s own spare capacity
(`FrameDecoder::read_window` / `finish_read`, a `compio::buf::Slice<BytesMut>`)
instead of a scratch `Vec` followed by `feed`. Applied to the EN connection loop,
the PS connection loop and `RpcClient::read_loop` (client→PS, client→EN and
PS→EN reads). The decoder reuses an allocation while a partially buffered frame's
rest fits; a frame in progress receives its remainder in place (EN window =
`max(512 KiB, rest of front frame)`); PS/client bulk frames keep a fixed 64 KiB
window so their value still goes to the pooled buffer. Comments that described a
registered UCX Stream receive as RDMA zero-copy were corrected.

Rejected variant: sizing each boundary window on the EN to the previous frame
removed the remaining prefix copy (TCP 1 MiB appends ≈0.45x → ≈0.002x per
replica) but
reserves the next buffer while the previous append still shares the old one.
EN page faults rose from tens to 18–28 K per 2 GiB window, and UCX 8 MiB writes
were ~2% slower (3 interleaved runs each: 459–464 MiB/s with it, 470–477 without,
base 472–475). It was removed.

## Result: copies per link

TCP (kernel copy), 2 trials per build:

| Link | Receiver | Value | Transport copy | App copy base | App copy final |
|---|---|---:|---:|---:|---:|
| Client→PS write | PS | 64 KiB | 1.00x | 2.148x | 1.148x |
| Client→PS write | PS | 1 MiB | 1.00x | 0.134x | 0.072x |
| Client→PS write | PS | 8 MiB | 1.00x | 0.017x | 0.009x |
| PS→EN append | 3 ENs summed | 64 KiB | 3.01x | 3.111x | 0.092x |
| PS→EN append | 3 ENs summed | 1 MiB | 3.00x | 3.042x | 1.365x |
| PS→EN append | 3 ENs summed | 8 MiB | 3.00x | 3.050x | 0.186x |
| EN→PS proxy read | PS | 64 KiB | 1.00x | 2.049x | 1.049x |
| EN→PS proxy read | PS | 1 MiB | 1.00x | 0.128x | 0.066x |
| EN→PS proxy read | PS | 8 MiB | 1.00x | 0.016x | 0.008x |
| PS→client proxy read | client | 64 KiB | 1.00x | 2.174x | 1.174x |
| PS→client proxy read | client | 1 MiB | 1.00x | 0.136x | 0.073x |
| PS→client proxy read | client | 8 MiB | 1.00x | 0.017x | 0.009x |
| EN→client direct read | client | 64 KiB | 1.00x | 2.174x | 1.174x |
| EN→client direct read | client | 1 MiB | 1.00x | 0.136x | 0.073x |
| EN→client direct read | client | 8 MiB | 1.00x | 0.017x | 0.009x |

UCX (Stream unpack), 2 trials per build:

| Link | Receiver | Value | Transport copy | App copy base | App copy final |
|---|---|---:|---:|---:|---:|
| Client→PS write | PS | 64 KiB | 1.00x | 2.146x | 1.147x |
| Client→PS write | PS | 1 MiB | 1.00x | 0.133x | 0.071x |
| Client→PS write | PS | 8 MiB | 1.00x | 0.016x | 0.009x |
| PS→EN append | 3 ENs summed | 64 KiB | 3.00x | 3.033x | 0.035x |
| PS→EN append | 3 ENs summed | 1 MiB | 3.00x | 3.003x | 0.565x |
| PS→EN append | 3 ENs summed | 8 MiB | 3.00x | 3.019x | 0.048x |
| EN→PS proxy read | PS | 64 KiB | 1.00x | 0.630x | 0.292x |
| EN→PS proxy read | PS | 1 MiB | 1.00x | 0.028x | 0.013x |
| EN→PS proxy read | PS | 8 MiB | 1.00x | 0.004x | 0.002x |
| PS→client proxy read | client | 64 KiB | 1.00x | 0.321x | 0.223x |
| PS→client proxy read | client | 1 MiB | 1.00x | 0.025x | 0.019x |
| PS→client proxy read | client | 8 MiB | 1.00x | 0.003x | 0.002x |
| EN→client direct read | client | 64 KiB | 1.00x | 0.282x | 0.217x |
| EN→client direct read | client | 1 MiB | 1.00x | 0.030x | 0.018x |
| EN→client direct read | client | 8 MiB | 1.00x | 0.005x | 0.003x |

The EN rows sum the three replicas (3.00x transport copy = one per replica). The
remaining EN copy is `try_decode` reserving an append whose beginning arrived in
the previous 512 KiB window (1 MiB appends: ~0.45x per replica on TCP, ~0.19x on
UCX, where reads return AM fragments). On the client, ~0.14x of a 64 KiB read is
the benchmark's own buffer handling (`controlled_path.rs`) and appears equally in
both builds; on the PS, ~0.15x of a 64 KiB write is request-struct moves in
`partition_loop` (≥ 1 KiB memmoves of in-flight state, not value bytes), also in
both builds. Base TCP figures differ slightly from the
first TCP runs because this table uses the 1 KiB attribution threshold for both
transports.

## Result: untraced throughput and process CPU

Untraced, several-second fixed-work windows. Base: 6 trials (4 earlier + 2 interleaved with final); final: 4 trials. Median MiB/s with [min–max]; process CPU seconds per GiB (user+system from /proc, 10 ms ticks; the EN column sums the three extent-node processes); user-mode cycles per GiB summed over client, PS and ENs (host perf). Only the last two base runs were interleaved with the final runs; the other four base runs came from the earlier ABBA matrix on the same host.

| Transport | Value | Mode | MiB/s base | MiB/s final | EN CPU s/GiB | PS CPU s/GiB | user Gcycles/GiB |
|---|---:|---|---:|---:|---|---|---|
| TCP | 64 KiB | write | 121 [109–122] | 141 [136–149] | 11.46 → 10.25 | 3.73 → 2.51 | 11.53 → 7.41 |
| TCP | 1 MiB | write | 610 [597–620] | 657 [639–670] | 4.55 → 4.27 | 1.19 → 1.12 | 4.32 → 3.85 |
| TCP | 8 MiB | write | 809 [777–820] | 811 [804–821] | 3.93 → 3.87 | 1.13 → 1.11 | 3.72 → 3.19 |
| TCP | 64 KiB | read | 1743 [1704–1761] | 1850 [1831–1862] | 0.40 → 0.42 | 0.56 → 0.53 | 0.89 → 0.72 |
| TCP | 1 MiB | read | 3090 [3032–3101] | 3125 [3088–3128] | 0.30 → 0.29 | 0.30 → 0.30 | 0.12 → 0.11 |
| TCP | 8 MiB | read | 2808 [2736–2877] | 2834 [2807–2882] | 0.41 → 0.40 | 0.33 → 0.32 | 0.06 → 0.06 |
| TCP | 64 KiB | direct | 2320 [2268–2340] | 2546 [2484–2557] | 0.40 → 0.40 | 0.23 → 0.23 | 0.79 → 0.70 |
| TCP | 1 MiB | direct | 3697 [3159–3760] | 3759 [3730–3779] | 0.28 → 0.27 | 0.01 → 0.01 | 0.09 → 0.08 |
| TCP | 8 MiB | direct | 7610 [7295–7827] | 7614 [7599–7685] | 0.37 → 0.37 | 0.00 → 0.00 | 0.02 → 0.02 |
| UCX | 64 KiB | write | 133 [129–137] | 162 [152–166] | 8.40 → 7.14 | 2.35 → 1.61 | 9.16 → 7.09 |
| UCX | 1 MiB | write | 447 [414–464] | 467 [463–590] | 3.92 → 3.76 | 0.73 → 0.70 | 5.54 → 5.07 |
| UCX | 8 MiB | write | 469 [466–478] | 483 [478–485] | 4.00 → 3.89 | 0.70 → 0.69 | 5.90 → 5.55 |
| UCX | 64 KiB | read | 2665 [2634–2746] | 2847 [2765–2898] | 0.39 → 0.39 | 0.34 → 0.33 | 1.39 → 1.31 |
| UCX | 1 MiB | read | 4758 [4642–4817] | 4718 [4672–4809] | 0.31 → 0.31 | 0.17 → 0.17 | 0.87 → 0.87 |
| UCX | 8 MiB | read | 3561 [3530–3586] | 3562 [3535–3588] | 0.31 → 0.31 | 0.21 → 0.21 | 1.14 → 1.13 |
| UCX | 64 KiB | direct | 2780 [2736–2814] | 2834 [2740–2908] | 0.27 → 0.28 | 0.13 → 0.14 | 0.93 → 0.91 |
| UCX | 1 MiB | direct | 4705 [4483–4985] | 4573 [4481–4762] | 0.20 → 0.19 | 0.01 → 0.01 | 0.39 → 0.39 |
| UCX | 8 MiB | direct | 6588 [6267–6762] | 6549 [6495–6595] | 0.41 → 0.41 | 0.00 → 0.00 | 0.53 → 0.53 |

Small-value paths gain the most: at 64 KiB the removed copy is a full value on
the PS (write) or client (read), and on every EN replica. Large writes are bound
by kernel copies and fsync, so removing one user-space copy per replica shows as
fewer user cycles per GiB (TCP 8 MiB 3.72 → 3.19 G) rather than throughput, except
UCX 8 MiB writes (+3%). Large reads had only the 64 KiB prefix to remove and stay
within run-to-run ranges. /proc CPU uses 10 ms ticks; treat sub-0.05 s/GiB
differences as noise. Shared host (other tenants' sglang/python work ran during
the windows); same-host transports only.

## Remaining copies and follow-ups

- One transport copy per receiving process remains: the TCP kernel copy, or the
  UCX Stream unpack. Removing the UCX one would need a different UCX API (AM
  rendezvous receiving into the destination, or tag/RMA); it is not pursued.
- Bulk paths copy the value prefix that shares the 64 KiB read with the frame
  prologue (1.0x of a 64 KiB value, ~0.06x at 1 MiB).
- The EN copies the part of a large append that arrived before its header was
  parsed (largest for 1 MiB appends); see the rejected variant.
- `ClusterClient::get` returns an owned `Vec`, which is its one application
  copy; `get_pooled` / `get_into` avoid or place it (next section).

## Plain GET on the bulk read

`ClusterClient::get` / `get_range` used the rkyv `MSG_GET`. Its callers are not
only Python (`Client.get` / `get_into` / `batch_get_into`): the `autumn-client
get` CLI, autumn-memory, FUSE/autumnfs metadata reads, the gallery example,
striped-stream chunk reads and SDK small-item fallbacks all went through it.
Per 8 MiB value the PS copied it four times (`GetResp` conversion of the pooled
`Bytes`, twice in rkyv encode, `Frame::encode`) and the client twice (aligned
decode, deserialize).

`get` / `get_range` now share the pooled `MSG_GET_BULK` core with `get_pooled`
and return `to_vec()` of the pooled value. The choice between decoding a small
reply and receiving a large one into the pool stays automatic at the receiver,
on the reply's actual size (`read_loop`: TCP < 64 KiB decode + memcpy, otherwise
pooled receive); the caller of `get` has no size to decide with in advance.
Python's `get`, `get_into` and `batch_get_into` use `get_pooled` directly, and
`get_many_into` / `get_range_direct_into` small items use `get_range_into`.

Fixed on the way: a bulk reply wrote the handler's `StatusCode` into its
`CODE_*` byte with `as u8`. The spaces diverge above 3, so a read refused while GC
held the extent's writer pin (`Unavailable`, 5) reached `get_pooled`/`get_into`
callers as `CODE_VALUE_TOO_LARGE` — a terminal error instead of a retry.
`partition_rpc::code_for_status` translates it; the pooled core's retry
classification now matches the one `get` always had (NotFound = miss, authz
terminal, everything else refresh + retry, first-attempt timeout).

Copies (base = `f1ec10e`, 2 traced trials per build, 1 KiB attribution threshold):

TCP, 2 trials per build (application copies per logical byte; transport copy on the receiver):

| Call | Value | PS app base | PS app new | client app base | client app new | client transport copy new |
|---|---:|---:|---:|---:|---:|---:|
| `get` | 4 KiB | 6.80x | 1.79x | 4.82x | 5.30x | 1.00x |
| `get` | 64 KiB | 5.26x | 1.05x | 2.99x | 2.21x | 1.00x |
| `get` | 1 MiB | 4.54x | 0.07x | 2.07x | 1.07x | 1.00x |
| `get` | 8 MiB | 4.01x | 0.01x | 2.01x | 1.01x | 1.00x |
| `get_pooled` | 64 KiB | 1.05x | 1.05x | 1.18x | 1.18x | 1.00x |
| `get_pooled` | 8 MiB | 0.01x | 0.01x | 0.01x | 0.01x | 1.00x |

UCX, 2 trials per build (application copies per logical byte; transport copy on the receiver):

| Call | Value | PS app base | PS app new | client app base | client app new | client transport copy new |
|---|---:|---:|---:|---:|---:|---:|
| `get` | 4 KiB | 6.80x | 1.79x | 4.82x | 5.06x | 1.00x |
| `get` | 64 KiB | 4.51x | 0.29x | 2.30x | 1.26x | 1.00x |
| `get` | 1 MiB | 4.45x | 0.01x | 2.07x | 1.02x | 1.00x |
| `get` | 8 MiB | 4.29x | 0.00x | 2.01x | 1.00x | 1.00x |
| `get_pooled` | 64 KiB | 0.29x | 0.28x | 0.23x | 0.24x | 1.00x |
| `get_pooled` | 8 MiB | 0.00x | 0.00x | 0.00x | 0.00x | 1.00x |

At 4 KiB the client's two value copies are unchanged (pool, then `Vec`); the rest
of the client figure there is benchmark and future-state moves ≥ 1 KiB present in
both builds. Untraced fixed-work windows, 3 interleaved runs per build:

| Transport | Call | Value | MiB/s base [min–max] | MiB/s new [min–max] | p99 ms base → new | PS CPU s/GiB | client CPU s/GiB |
|---|---|---:|---:|---:|---|---|---|
| TCP | `get` | 4 KiB | 438 [437–438] | 468 [467–469] | 0.08 → 0.08 | 2.32 → 2.13 | 2.34 → 2.19 |
| TCP | `get` | 64 KiB | 1087 [1051–1107] | 1799 [1740–1834] | 0.58 → 0.39 | 0.90 → 0.53 | 0.56 → 0.34 |
| TCP | `get` | 1 MiB | 822 [814–826] | 3170 [3107–3169] | 10.01 → 3.63 | 0.77 → 0.29 | 1.24 → 0.19 |
| TCP | `get` | 8 MiB | 639 [612–642] | 2625 [2464–2650] | 122.77 → 35.43 | 0.89 → 0.34 | 1.60 → 0.21 |
| TCP | `get_pooled` | 64 KiB | 1829 [1802–1857] | 1821 [1764–1862] | 0.39 → 0.38 | 0.53 → 0.53 | 0.31 → 0.31 |
| TCP | `get_pooled` | 8 MiB | 2699 [2513–2742] | 2714 [2533–2740] | 34.04 → 33.99 | 0.33 → 0.32 | 0.16 → 0.16 |
| UCX | `get` | 4 KiB | 387 [379–387] | 515 [498–518] | 0.10 → 0.08 | 2.00 → 1.70 | 1.83 → 1.46 |
| UCX | `get` | 64 KiB | 1409 [1390–1412] | 2583 [2575–2819] | 0.51 → 0.30 | 0.70 → 0.34 | 0.45 → 0.22 |
| UCX | `get` | 1 MiB | 1117 [1111–1221] | 4509 [4392–4509] | 12.53 → 3.05 | 0.67 → 0.17 | 0.77 → 0.17 |
| UCX | `get` | 8 MiB | 864 [744–981] | 3244 [3215–3356] | 168.23 → 30.84 | 0.87 → 0.21 | 1.10 → 0.25 |
| UCX | `get_pooled` | 64 KiB | 2689 [2508–2807] | 2665 [2638–2699] | 0.30 → 0.30 | 0.34 → 0.34 | 0.19 → 0.19 |
| UCX | `get_pooled` | 8 MiB | 3447 [3401–3456] | 3358 [3347–3473] | 31.86 → 31.94 | 0.20 → 0.21 | 0.15 → 0.15 |

`get` is faster at every size, most at 1–8 MiB where the removed copies dominate,
and its CPU per GiB falls on both roles; `get_pooled` is unchanged within
run-to-run ranges. A size-based fallback to `MSG_GET` for small values would not
help: 4 KiB is also faster on the bulk read. With no caller left, `MSG_GET` was
retired in wire v41 (0x41 reserved); raw-frame tests and `ps_bench` read through
`MSG_GET_BULK`.

## Evidence

Results, hashes and patches: `/data08/autumn-receive-copies` (`results/`,
`binaries-final.sha256`, `final-build.patch`, `summary.json`, `cpu*.json`);
committed summaries in `perf/receive_copies/results/`.
