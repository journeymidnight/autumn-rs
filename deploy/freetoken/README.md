# freetoken-autumn

FreeToken (edge-native MoE serving) with its model weights read from an autumn
FUSE mount, plus the autumn PyO3 client and the `autumn_kvcache` adapters.

## What this image is for

FreeToken runs a large MoE model on a single consumer GPU by keeping the routed
experts in **host RAM** and computing them partly on the CPU (`--moe-backend
hybrid`). That makes host memory, not VRAM, the size limit — and it makes the
weights a cold-start streaming problem rather than a resident one, which is why
they can live in autumn and be read through a FUSE mount.

## Hard constraints, all verified

| Constraint | Consequence |
|---|---|
| Driver **r580+ / CUDA 13** (`docs/install.md`) | On the VKE cluster only `192.168.3.2` (8× RTX 4090, driver 580.105.08) qualifies. `192.168.0.102` (4090D) is on 550 and **cannot run this image**. |
| nvcc needed **at runtime** | Kernels JIT-compile on first use and `kernel/_toolchain.py` refuses a CUDA-major mismatch — hence a `-devel` CUDA base, not a slim runtime. |
| **No tensor parallelism** (`server/args.py:640` errors outright) | One GPU per replica. Multi-GPU is data parallel only: N replicas, each with its **own full copy** of the expert banks in host RAM. |
| **No authentication** in `ft serve` | No API key, no bearer, anywhere in `server/`. ClusterIP only; never a LoadBalancer. |
| Ports 1919 (API) and 1920 (distributed init) | `--host 0.0.0.0` is mandatory; the default binds 127.0.0.1. |
| Embeds the autumn PyO3 client | **WIRE lockstep**: build at the same commit as the cluster core, and point `AUTUMN_IMAGE` at that commit's tag. |

## Quantization: why NVFP4, and why `--nvfp4-backend triton`

A 4090 is Ada (sm_89) and has **no FP4 tensor cores** — FP4 hardware starts at
Blackwell. FreeToken still runs NVFP4 there, as W4A16 (4-bit weights,
dequantized in-kernel), via one of two paths (`moe/nvfp4_backends.py`):

- **marlin**, sm_80–99 — the fast one, but it borrows vLLM's AOT wheel, and vLLM
  pins `transformers>=4.56,<5` against FreeToken's `transformers>=5.5`. Upstream
  documents the conflict in `pyproject.toml` and leaves vLLM out of the
  resolution. This image follows that: no vLLM.
- **triton** — FreeToken's own kernels, any architecture, no extra dependencies.
  Start here. Revisit only if expert GEMM measures as the bottleneck.

NVFP4 is not merely convenient, it is close to required for hybrid. The CPU-side
expert kernel accepts only `{bf16, nvfp4, mxfp4_triton, ds_fp4, q4_0}`
(`moe/cpu_executor.py:71`) — **`fp8_block` is absent**. Feed it an FP8 checkpoint
and it raises, telling you to use `--moe-backend offload` instead: the experts
all stream over PCIe and the CPU half of "hybrid" is gone. bf16 would fit the
whitelist but at 2 bytes/param the host-RAM budget stops being comfortable.

## Reading weights from the mount

FreeToken's FTW reader probes `O_DIRECT` once. On success it streams with
chunked multi-threaded `preadv`; on failure it falls back to a whole-shard
`mmap.mmap(fd, 0)` — and Python's mmap defaults to `MAP_SHARED`, which the kernel
refuses on a FUSE file opened `FOPEN_DIRECT_IO` (`ENODEV`). autumn-fuse sets that
flag unconditionally, so a failed probe would kill **both** load paths.

Measured on the live cluster (2026-09-01, kernel 5.15): `O_DIRECT` reads succeed
at 4 KiB and at 8 MiB, so the fast path holds and the mmap trap is unreachable.

**Do not use `--expert-load parallel` on a mount.** It opens with a
no-fallback `O_RDONLY|O_DIRECT` and `load_expert_banks` only catches
`NotImplementedError`, so any `OSError` aborts startup. `serial` is the safe
setting; pre-converting to FTW (`ft checkpoint`) is better still.

## Build

Built by the Volcengine CP pipeline, not locally (this repo's images are; see
`deploy/docker/Dockerfile` for the core one). Build context is the **repo root**:

```bash
docker build -f deploy/freetoken/Dockerfile \
  --build-arg BASE_REGISTRY=docker.m.daocloud.io/ \
  --build-arg PIP_INDEX_URL=https://mirrors.ivolces.com/pypi/simple/ \
  --build-arg RUSTUP_DIST_SERVER=https://rsproxy.cn \
  --build-arg RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup \
  --build-arg CARGO_MIRROR=sparse+https://rsproxy.cn/index/ \
  --build-arg AUTUMN_IMAGE=<cr>/autumn-rs:<same-commit-sha> \
  -t <cr>/freetoken-autumn:<same-commit-sha> .
```

`PIP_INDEX_URL` is not optional on the VKE agents: the default index resolves
pypi.org but pulls wheels from files.pythonhosted.org, which stalls there — the
build hangs with no output rather than failing.

Build args worth knowing: `FREETOKEN_SPEC` (pinned to `freetoken[accel]==0.1.2`;
accepts a git spec to test an unreleased engine), `TORCH_INDEX_URL` (cu130 wheels
are not on PyPI proper), `CUDA_IMAGE`, `AUTUMN_IMAGE`.

## Running

No ENTRYPOINT — the pod supplies the command, because two things must happen
first: resolve the manager's ClusterIP (autumn's Rust client parses a
`SocketAddr`, so IP literals only, no DNS), and mount the weights.

```bash
M="$(getent hosts autumn-manager | awk '{print $1}'):9001"
autumn-fuse --manager "$M" --mountpoint /mnt/autumn \
    --credential-file /etc/autumn/cred/fs.cred --transport tcp &
# wait for the mount, then:
exec ft serve --model /mnt/autumn/models/<name> \
    --host 0.0.0.0 --port 1919 \
    --moe-backend hybrid --nvfp4-backend triton \
    --page-size 64 --expert-load serial \
    --tool-call-parser <explicit> --reasoning-parser <explicit>
```

Set the parsers explicitly: `_infer_tool_call_parser` matches on substrings of
the **model path**, so a renamed directory silently selects the wrong one.

`--page-size` defaults to **1**. Leave it there and any future L3 KV cache keys
one hash per token, which is pathological; 64 is a sane starting point for a
model that does not force its own page size.

Run `ft bench bw` once on the node before relying on `--moe-backend auto`: the
calibration is cached per **GPU UUID** under `$XDG_CACHE_HOME/freetoken/benchbw/`
and does not transfer between machines. Without it, `auto` never upgrades to
`hybrid`. Passing `--moe-backend hybrid` explicitly sidesteps the question.

Health: `GET /health`. Treat a 503 naming `maintenance_state == "failed"` as
**fatal** in the liveness probe — a failed `/v1/cache/rebuild` latches the server
into that state and only a restart clears it.
