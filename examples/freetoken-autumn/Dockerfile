# FreeToken + autumn image — MoE serving with weights read from an autumn FUSE mount.
#
# Build context = autumn-rs REPO ROOT (needs the Rust workspace to build the wheel):
#   docker build -f examples/freetoken-autumn/Dockerfile \
#     --build-arg BASE_REGISTRY=docker.m.daocloud.io/ \
#     --build-arg RUSTUP_DIST_SERVER=https://rsproxy.cn \
#     --build-arg RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup \
#     --build-arg CARGO_MIRROR=sparse+https://rsproxy.cn/index/ \
#     --build-arg PIP_INDEX_URL=https://mirrors.ivolces.com/pypi/simple/ \
#     --build-arg AUTUMN_IMAGE=<cr>/autumn-rs:<same-commit> \
#     -t <cr>/freetoken-autumn:<tag> .
#
# ── Why a -devel CUDA base and not a slim runtime ──────────────────────────────
# FreeToken JIT-compiles CUDA kernels on first use and `freetoken/kernel/
# _toolchain.py:check_nvcc_matches_torch()` REFUSES to build when nvcc's CUDA
# major differs from torch's. So nvcc must be on PATH at RUNTIME, not just at
# build time. docs/install.md states the floor plainly: "Linux x86_64, NVIDIA
# GPU, driver r580+ (CUDA 13)". The 4090 node (192.168.3.2) runs 580.105.08;
# the 4090D node (192.168.0.102) is on 550 and CANNOT run this image.
#
# ── WIRE lockstep ─────────────────────────────────────────────────────────────
# This image embeds the autumn PyO3 client, so it is bound by the same rule as
# vllm-autumn and hermes-autumn: build it at the SAME commit as the cluster core.
# rkyv has no cross-version compatibility and the handshake refuses a mismatch
# (and, worse, a matching WIRE fingerprint across a layout drift decodes garbage
# silently). AUTUMN_IMAGE below should name that same commit's tag.

ARG BASE_REGISTRY=
# CUDA 13 + nvcc. Overridable because the exact patch tag moves; anything
# 13.x -devel with a glibc >= 2.28 userland works (FreeToken's CI wheels are
# manylinux_2_28, cp310-cp313; Ubuntu 22.04 ships cp310).
ARG CUDA_IMAGE=nvidia/cuda:13.0.1-devel-ubuntu22.04
# The autumn-rs image to lift `autumn-fuse` out of — MUST be the same commit.
# Deliberately NOT prefixed with BASE_REGISTRY: that knob exists to route the
# PUBLIC base images through a mirror, and this one is a full private-registry
# reference already (`<host>.cr.volces.com/<ns>/autumn-rs:<sha>`). Prefixing it
# would produce `docker.m.daocloud.io/<host>.cr.volces.com/...` and fail to pull.
ARG AUTUMN_IMAGE=autumn-rs:latest

FROM ${AUTUMN_IMAGE} AS autumncore

FROM ${BASE_REGISTRY}${CUDA_IMAGE}

# ── Mirror knobs (override for China / air-gapped builds) ─────────────────────
#   RUSTUP_DIST_SERVER / RUSTUP_UPDATE_ROOT : rustup TOOLCHAIN download mirror.
#   CARGO_MIRROR   : crates.io registry mirror for the wheel build.
#   PIP_INDEX_URL  : PyPI mirror. On the VKE nodes the DEFAULT index resolves
#     pypi.org fine but downloads wheels from files.pythonhosted.org, which is
#     slow/blocked there — a build sits with no output for many minutes. Use
#     https://mirrors.ivolces.com/pypi/simple/.
#   APT_MIRROR     : Ubuntu apt mirror BASE URL (e.g. https://mirrors.volces.com).
ARG RUSTUP_DIST_SERVER=https://static.rust-lang.org
ARG RUSTUP_UPDATE_ROOT=https://static.rust-lang.org/rustup
ARG CARGO_MIRROR=
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG APT_MIRROR=
# torch for CUDA 13 is not on PyPI proper; it comes from PyTorch's own index.
# Kept separate from PIP_INDEX_URL so a mirror can serve everything else.
ARG TORCH_INDEX_URL=https://download.pytorch.org/whl/cu130

ENV CARGO_HOME=/root/.cargo \
    PATH=/root/.cargo/bin:/usr/local/cuda/bin:$PATH \
    RUSTUP_DIST_SERVER=${RUSTUP_DIST_SERVER} \
    RUSTUP_UPDATE_ROOT=${RUSTUP_UPDATE_ROOT} \
    PIP_INDEX_URL=${PIP_INDEX_URL} \
    DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

RUN if [ -n "$APT_MIRROR" ]; then \
      find /etc/apt \( -name '*.list' -o -name '*.sources' \) -print | while read -r f; do \
        sed -i -E "s#https?://(archive\.ubuntu\.com|security\.ubuntu\.com|ports\.ubuntu\.com)#${APT_MIRROR}#g" "$f"; \
      done; \
    fi \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 python3-pip python3-dev python3-venv \
      curl ca-certificates git build-essential pkg-config libssl-dev \
      libfuse3-3 fuse3 numactl \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal \
 && python3 -m pip install --no-cache-dir --upgrade pip "maturin>=1,<2"

# `autumn-fuse` mounts the fs/ namespace so FreeToken can read weights as files.
# Lifted from the core image rather than rebuilt: one Rust build, one binary,
# and it cannot drift from the cluster it talks to.
COPY --from=autumncore /usr/local/bin/autumn-fuse /usr/local/bin/autumn-fuse
COPY --from=autumncore /usr/local/bin/autumnfs     /usr/local/bin/autumnfs
COPY --from=autumncore /usr/local/bin/autumn-op    /usr/local/bin/autumn-op

# ── torch first, pinned to the CUDA-13 build ──────────────────────────────────
# FreeToken requires torch>=2.11,<2.12 and its extensions compile against it, so
# torch must be present and correct BEFORE freetoken is built/installed.
RUN python3 -m pip install --no-cache-dir --index-url ${TORCH_INDEX_URL} "torch>=2.11,<2.12"

# ── FreeToken ─────────────────────────────────────────────────────────────────
# A pip spec, so this can point at a released wheel or a git ref:
#   --build-arg FREETOKEN_SPEC='freetoken[accel]==<version>'
#   --build-arg FREETOKEN_SPEC='freetoken[accel] @ git+https://github.com/FlashML-org/FreeToken@<sha>'
# NOTE: `[accel]` pulls flashinfer + sglang-kernel. It does NOT pull vLLM, and
# that is deliberate — vLLM supplies the Marlin NVFP4 expert-GEMM path for
# sm_80-99 (which is what a 4090 needs for the fastest NVFP4 kernels), but it
# pins transformers>=4.56,<5 against FreeToken's own transformers>=5.5. Upstream
# calls this out in pyproject.toml and leaves it out of the resolution. Serve
# with `--nvfp4-backend triton` instead; revisit only if expert GEMM measures as
# the bottleneck, and then in a dedicated image.
# Pinned, not floating: an engine upgrade should be a deliberate edit here, not
# something that happens because a rebuild ran on a different day.
# 0.1.2 was checked against the source this deployment was designed from
# (git e05cff8, 18 commits ahead): moe/nvfp4_backends.py, moe/cpu_executor.py and
# kvcache/base.py are all present, and the CPU-MoE format whitelist
# `_WFMT_IDS = {bf16, nvfp4, mxfp4_triton, ds_fp4, q4_0}` is identical — which is
# the fact the model choice rests on (fp8 experts cannot run on the CPU side, so
# an FP8 checkpoint silently loses hybrid and falls back to pure offload).
ARG FREETOKEN_SPEC=freetoken[accel]==0.1.2
RUN FREETOKEN_ALLOW_CUDA_MISMATCH=0 \
    python3 -m pip install --no-cache-dir "${FREETOKEN_SPEC}"

# ── the autumn client + its kvcache adapters ──────────────────────────────────
WORKDIR /src
COPY . .
RUN if [ -n "$CARGO_MIRROR" ]; then mkdir -p "$CARGO_HOME"; \
      printf '[source.crates-io]\nreplace-with="m"\n[source.m]\nregistry="%s"\n' "$CARGO_MIRROR" >> "$CARGO_HOME/config.toml"; fi

# The native `autumn` PyO3 client, then the pure-python adapter package. The
# kvcache adapter carries `AutumnKVCacheStorage`, the sglang-HiCache-shaped L3
# backend that a HiCache-enabled FreeToken loads via
#   --hicache-storage-backend dynamic
#   --hicache-storage-backend-extra-config '{"module_path":"autumn_kvcache.sglang_backend",
#     "class_name":"AutumnKVCacheStorage","interface_v1":1,...}'
# Installing it now costs nothing and means the image is ready the moment the
# engine side lands. `autumn_vllm_loader` is deliberately NOT installed: weights
# come from the FUSE mount here, not from `--load-format autumn`.
RUN cd python \
 && maturin build --release --interpreter python3 \
 && python3 -m pip install --no-cache-dir target/wheels/autumn-*.whl \
 && python3 -m pip install --no-cache-dir -e autumn_kvcache

# Trim the Rust toolchain and the build tree; they are ~1.5 GB and nothing at
# runtime needs them. Keep /src/python/autumn_kvcache (installed with -e).
RUN rm -rf /root/.rustup /root/.cargo/registry /src/target /src/python/target

# `ft serve` defaults to 127.0.0.1:1919 and has NO authentication of any kind —
# no API key, no bearer. Expose it as a ClusterIP only, never a LoadBalancer.
# 1920 is also reserved (distributed init binds server_port + 1).
EXPOSE 1919
# No ENTRYPOINT: the pod supplies the full command, because it has to resolve the
# manager ClusterIP first (autumn's Rust client parses SocketAddr — IP literals
# only, no DNS) and start the FUSE mount before `ft serve` can read the weights.
CMD ["ft", "--help"]
