# Receive-copy accounting

Attributes every receive-side copy of a value on the three links — client→PS
write, PS→EN replication, EN/PS→client read — to one of:

- **TCP kernel copy**: `skb_copy_datagram_iter` bytes per process (kprobe).
- **UCX Stream unpack**: memcpy whose return address lies in libucp/libuct.
- **Application copy**: memcpy/memmove ≥ 1 KiB whose return address lies in an
  autumn binary, resolved with `addr2line` to the Rust call site.

Each byte figure is divided by the logical bytes the window moved (`_x`), so 1.0
means one full copy of every value on that process.

Run it only in the H200-1 `dongmao-autumn` test container. It reuses
`perf/controlled_validation/run.py` for the cluster layout (RF3 on /data03,
/data05, /data08; ENs on CPUs 0–11, PS 12–19, client 40) with its own root
`/data08/autumn-receive-copies`, one partition, depth 8.

## Inputs

- `bin/base` and `bin/new` under the root: `autumn-{manager-server,extent-node,
  ps,op,client}` and the `controlled_path` bench, each built with
  `cargo +1.95.0 build --release -p autumn-server --features ucx --bins --bench controlled_path`
  from the tree being compared. Record `sha256sum` of both directories.
- Mounted tracefs (`mount -t tracefs nodev /sys/kernel/tracing` inside the
  container if absent; unmount at the end of the task).

## Measurement

```sh
python3 perf/receive_copies/copytrace.py --version base --transport tcp --repeat 1
python3 perf/receive_copies/analyze.py /data08/autumn-receive-copies/results > summary.json
```

A trial loads 64 keys of 64 KiB, 1 MiB and 8 MiB, then runs fixed-work windows
twice: untraced (MiB/s, p99, process CPU through host perf) and traced with
`copies.bt`. The traced pass exists for byte accounting only: a uprobe fires on
every memcpy call, so its throughput is not a performance result. Modes are
`write` (`put_bulk`), `read` (`get_pooled`, PS proxy) and `direct` (`get_direct`,
EN descriptor read).

The driver disables ASLR for itself and every process it starts (the flag is
inherited across exec). Return addresses then resolve after the processes
exit: services from their `/proc/PID/maps`, the benchmark from its fixed PIE
base and `LD_TRACE_LOADED_OBJECTS`. The uprobe offset is glibc's resolved
memcpy/memmove IFUNC target, found by `dlsym` in the driver.

Frame-pointer stacks from a probe at memcpy's entry skip the immediate caller,
which is why the return address at `[sp]` is keyed separately. Copies under
1 KiB are only counted (`small_memcpy_calls`): comparing their count explains
traced-throughput differences between builds. The threshold must stay below a
UCX AM fragment: Stream receives return one fragment per call, so a higher
threshold hides both the unpack and any application copy of those reads.

A successful trial copies results to `results/`, stops the services and removes
its marked data directories. A failed trial keeps them for diagnosis; move its
trial directory aside before rerunning the same label.
