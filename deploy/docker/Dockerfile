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
#   docker build --build-arg BASE_REGISTRY=docker.m.daocloud.io/library/ ...
# Must include a trailing slash. Global ARG (declared before the first FROM) so
# BOTH stages' FROM lines can reference it.
ARG BASE_REGISTRY=

FROM ${BASE_REGISTRY}rust:1.93-bookworm AS builder
WORKDIR /build
# Optional crates.io mirror for faster dependency downloads. Empty (default) →
# official crates.io (unchanged for everyone else). `--build-arg CARGO_MIRROR=
# rsproxy` switches to ByteDance's rsproxy.cn sparse mirror (fast on a
# China/Volcengine runner; all deps are crates.io, no git deps). Written to
# $CARGO_HOME so the repo's own .cargo/config.toml is untouched. Kept before
# `COPY . .` so it's cached independently of source changes. Re-declared here
# because a global (pre-FROM) ARG isn't visible to a stage's RUN.
ARG CARGO_MIRROR=
RUN if [ "$CARGO_MIRROR" = "rsproxy" ]; then \
      printf '%s\n' \
        '[source.crates-io]' \
        'replace-with = "rsproxy-sparse"' \
        '[source.rsproxy-sparse]' \
        'registry = "sparse+https://rsproxy.cn/index/"' \
        >> "$CARGO_HOME/config.toml"; \
    fi
COPY . .
# -p autumn-server builds exactly the server binaries. It pulls autumn-fuse with
# default-features = false (key/schema modules only), so no libfuse3-dev is
# needed in the builder. TCP transport only — the ucx feature is deliberately
# off (RDMA-in-k8s needs hostNetwork + device plugins; see docs/k8s_deploy.md).
RUN cargo build --release -p autumn-server

# Runtime: binaries link only libc/libgcc; bookworm-slim provides bash + getent
# (libc-bin) which entrypoint.sh needs for DNS→IP resolution. The kernel comes
# from the k8s node and must be >= 5.15 (compio uses io_uring).
FROM ${BASE_REGISTRY}debian:bookworm-slim
COPY --from=builder \
    /build/target/release/autumn-manager-server \
    /build/target/release/autumn-extent-node \
    /build/target/release/autumn-ps \
    /build/target/release/autumn-client \
    /build/target/release/autumn-op \
    /usr/local/bin/
COPY deploy/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
