# vLLM + autumn image — KV-cache connector AND weight loader.
#
# Build context = autumn-rs REPO ROOT (needs the Rust workspace to build the wheel):
#   docker build -f examples/hermes-autumn-memory/vllm-kvcache/Dockerfile \
#     --build-arg RUSTUP_DIST_SERVER=https://rsproxy.cn \
#     --build-arg RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup \
#     --build-arg CARGO_MIRROR=sparse+https://rsproxy.cn/index/ \
#     -t <cr>/vllm-autumn:<tag> .
#
# Pinned to vLLM 0.23.0 — AutumnKVConnector is verified against it (KVConnectorBase_V1
# signatures drift across vLLM releases). Bump VLLM_TAG only after re-validating.
ARG VLLM_TAG=v0.23.0
FROM vllm/vllm-openai:${VLLM_TAG}

# ── Rust mirror knobs (override at build time for China / air-gapped builds) ──
#   RUSTUP_DIST_SERVER / RUSTUP_UPDATE_ROOT : the rustup TOOLCHAIN download mirror
#     (e.g. https://rsproxy.cn + https://rsproxy.cn/rustup). Default = official.
#   CARGO_MIRROR : the crates.io registry mirror for the workspace build
#     (e.g. sparse+https://rsproxy.cn/index/). Empty = crates.io direct.
ARG RUSTUP_DIST_SERVER=https://static.rust-lang.org
ARG RUSTUP_UPDATE_ROOT=https://static.rust-lang.org/rustup
ARG CARGO_MIRROR=
ENV CARGO_HOME=/root/.cargo PATH=/root/.cargo/bin:$PATH \
    RUSTUP_DIST_SERVER=${RUSTUP_DIST_SERVER} \
    RUSTUP_UPDATE_ROOT=${RUSTUP_UPDATE_ROOT}

# Rust + maturin to build the `autumn` PyO3 wheel against vLLM's Python.
# The toolchain download honors RUSTUP_DIST_SERVER / RUSTUP_UPDATE_ROOT above.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl build-essential pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal \
 && pip install --no-cache-dir "maturin>=1,<2"

WORKDIR /src
COPY . .
# Optional crates.io registry mirror for the Rust build.
RUN if [ -n "$CARGO_MIRROR" ]; then mkdir -p "$CARGO_HOME"; \
      printf '[source.crates-io]\nreplace-with="m"\n[source.m]\nregistry="%s"\n' "$CARGO_MIRROR" >> "$CARGO_HOME/config.toml"; fi

# Build + install the native `autumn` PyO3 client, then BOTH pure-python adapters:
#   - autumn_kvcache      : AutumnKVConnector       (KV-cache offload  → --kv-transfer-config)
#   - autumn_vllm_loader  : the autumn weight loader (--load-format autumn)
RUN cd python \
 && maturin build --release --interpreter python3 \
 && pip install --no-cache-dir target/wheels/autumn-*.whl \
 && pip install --no-cache-dir -e autumn_kvcache \
 && pip install --no-cache-dir -e autumn_vllm_loader

# vllm-openai's own entrypoint (`vllm serve`) is kept; the pod supplies the args.
