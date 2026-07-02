# autumn-rs all-roles image (manager / extent-node / ps / bootstrap-job / client tooling).
#
# Build from the REPO ROOT (the .dockerignore there keeps target/ out of the context):
#   docker build -f deploy/docker/Dockerfile -t autumn-rs:latest .
#
# One image serves every role; deploy/docker/entrypoint.sh dispatches on its
# first argument (manager | extent-node | ps | bootstrap). See docs/k8s_deploy.md.

FROM rust:1.93-bookworm AS builder
WORKDIR /build
COPY . .
# -p autumn-server builds exactly the server binaries. It pulls autumn-fuse with
# default-features = false (key/schema modules only), so no libfuse3-dev is
# needed in the builder. TCP transport only — the ucx feature is deliberately
# off (RDMA-in-k8s needs hostNetwork + device plugins; see docs/k8s_deploy.md).
RUN cargo build --release -p autumn-server

# Runtime: binaries link only libc/libgcc; bookworm-slim provides bash + getent
# (libc-bin) which entrypoint.sh needs for DNS→IP resolution. The kernel comes
# from the k8s node and must be >= 5.15 (compio uses io_uring).
FROM debian:bookworm-slim
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
