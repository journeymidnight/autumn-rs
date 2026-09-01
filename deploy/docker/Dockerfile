# autumn-rs all-roles image (manager / extent-node / ps / bootstrap-job / client tooling).
#
# Build from the REPO ROOT (the .dockerignore there keeps target/ out of the context):
#   docker build -f deploy/docker/Dockerfile -t autumn-rs:latest .
#
# One image serves every role; deploy/docker/entrypoint.sh dispatches on its
# first argument (manager | extent-node | ps | bootstrap). See docs/k8s_deploy.md.

# Optional base-image registry prefix. Empty (default) → the official Docker Hub
# images (docker.io/library/...). Set it to build the base layers through a
# mirror when docker.io is slow/blocked, e.g. on a China/Volcengine runner:
#   docker build --build-arg BASE_REGISTRY=docker.m.daocloud.io/ ...   (host only; `library/` is in the FROM)
# Must include a trailing slash. Global ARG (declared before the first FROM) so
# BOTH stages' FROM lines can reference it.
ARG BASE_REGISTRY=

FROM ${BASE_REGISTRY}library/rust:1.93-bookworm AS builder
WORKDIR /build
# Optional crates.io mirror for faster dependency downloads. Empty (default) →
# official crates.io (unchanged for everyone else). Set CARGO_MIRROR to a full
# registry value and it replaces crates.io — e.g. ByteDance's sparse mirror on
# a China/Volcengine runner (use the sparse+ prefix; all deps are crates.io, no
# git deps):
#   --build-arg CARGO_MIRROR=sparse+https://rsproxy.cn/index/
# Written to $CARGO_HOME so the repo's own .cargo/config.toml is untouched, and
# kept before `COPY . .` so it caches independently of source. Re-declared here
# because a global (pre-FROM) ARG isn't visible to a stage's RUN.
ARG CARGO_MIRROR=
RUN if [ -n "$CARGO_MIRROR" ]; then \
      printf '%s\n' \
        '[source.crates-io]' \
        'replace-with = "mirror"' \
        '[source.mirror]' \
        "registry = \"$CARGO_MIRROR\"" \
        >> "$CARGO_HOME/config.toml"; \
    fi
# libfuse3-dev + pkg-config are for `-p autumn-fuse`, which builds the MOUNT
# binary (fuser 0.15 links libfuse3). autumn-server alone would not need them —
# it pulls autumn-fuse with default-features = false (key/schema modules only) —
# but the fuse daemon is now shipped so a consumer pod can mount the `fs/`
# namespace as a POSIX filesystem (docs/ops.md "Fuse daemon runbook").
RUN apt-get update \
    && apt-get install -y --no-install-recommends libfuse3-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY . .
# -p autumn-server builds exactly the server binaries. TCP transport only — the
# ucx feature is deliberately off (RDMA-in-k8s needs hostNetwork + device
# plugins; see docs/k8s_deploy.md).
# autumn-dashboard is the standalone web UI (examples/dashboard); it embeds its
# HTML via include_str!, so the binary alone is self-contained, and it drives the
# cluster only through the autumn-op we already ship.
# autumn-fuse is the FUSE mount daemon (default features = the `fuse` feature).
# memory-mcp is the stdio MCP server over autumn-memory; it ships so an agent
# pod (hermes) can register it as a local stdio MCP server instead of needing a
# long-running HTTP/SSE MCP daemon (docs/autumn_memory_plan.md §12).
# autumn-s3 is the read-only S3 gateway (examples/s3-gateway); it lets engines
# with no loader plugin seam (SGLang, FreeToken) stream weights via their
# built-in runai_streamer. See docs/model_loading.md Recipe D.
RUN cargo build --release \
    -p autumn-server -p autumn-dashboard -p autumn-fuse -p memory-mcp -p autumn-s3

# Runtime: the server binaries link only libc/libgcc; bookworm-slim provides
# bash + getent (libc-bin) which entrypoint.sh needs for DNS→IP resolution. The
# kernel comes from the k8s node and must be >= 5.15 (compio uses io_uring).
# fuse3 is for autumn-fuse: it brings libfuse3 (the binary links it) plus the
# fusermount3 setuid helper, which fuser needs both to mount and to clean up a
# stale mount after a crashed daemon.
FROM ${BASE_REGISTRY}library/debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder \
    /build/target/release/autumn-manager-server \
    /build/target/release/autumn-extent-node \
    /build/target/release/autumn-ps \
    /build/target/release/autumn-client \
    /build/target/release/autumn-op \
    /build/target/release/autumnfs \
    /build/target/release/autumn-dashboard \
    /build/target/release/autumn-fuse \
    /build/target/release/memory-mcp \
    /build/target/release/autumn-s3 \
    /usr/local/bin/
COPY deploy/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
