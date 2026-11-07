# ExtentNode Benchmark Results

## Linux x86-64

Platform: Linux 6.1 x86-64
Runtime: compio single-threaded, loopback TCP
Payload: 4KB per op

### Write (single connection, varying pipeline depth)

| depth | ops/sec  | throughput  | avg latency |
|-------|----------|------------|------------|
| 1     | 54,417   | 212.6 MB/s | 18.4 µs    |
| 2     | 56,659   | 221.3 MB/s | 17.6 µs    |
| 4     | 85,312   | 333.2 MB/s | 11.7 µs    |
| 8     | 95,979   | 374.9 MB/s | 10.4 µs    |
| 16    | 136,454  | 533.0 MB/s | 7.3 µs     |
| 32    | 192,500  | 752.0 MB/s | 5.2 µs     |
| 64    | 207,889  | 812.1 MB/s | 4.8 µs     |

### Read (varying tasks × pipeline depth)

| tasks | depth | ops/sec  | throughput  | avg latency |
|-------|-------|----------|------------|------------|
| 1     | 1     | 99,258   | 387.7 MB/s | 10.1 µs    |
| 1     | 16    | 176,918  | 691.1 MB/s | 5.7 µs     |
| 1     | 64    | 186,304  | 727.8 MB/s | 5.4 µs     |
| 32    | 1     | 112,356  | 438.9 MB/s | 8.9 µs     |
| 32    | 16    | 170,489  | 666.0 MB/s | 5.9 µs     |
| 32    | 64    | 160,434  | 626.7 MB/s | 6.2 µs     |

### Mixed R/W (same extent)

| config                           | write ops/s | read ops/s | total ops/s |
|----------------------------------|-------------|-----------|-------------|
| 1 writer(d=32) + 1 reader(d=16)  | 86,377      | 86,377    | 172,755     |
| 1 writer(d=32) + 4 readers(d=16) | 35,348      | 141,393   | 176,741     |
| 1 writer(d=32) + 16 readers(d=16)| 10,212      | 163,398   | 173,610     |

### Optimizations exercised

1. **Server: pwritev batch** — consecutive MSG_APPEND frames coalesced into one `write_vectored_at` syscall
2. **Server: pread batch** — consecutive MSG_READ_BYTES processed sequentially (no spawn), responses collected
3. **Server: write_vectored_all** — ALL responses from one TCP read batch written in one `write_vectored_all` syscall
4. **Client: pipeline depth** — sliding window sends N requests ahead, hiding RTT
