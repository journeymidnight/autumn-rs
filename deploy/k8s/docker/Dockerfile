# autumn-rs all-roles image (manager / extent-node / ps / bootstrap-job / client tooling).
#
# Build from the REPO ROOT (the .dockerignore there keeps target/ out of the context):
#   docker build -f deploy/k8s/docker/Dockerfile -t autumn-rs:latest .
#
# One image serves every role; deploy/k8s/docker/entrypoint.sh dispatches on its
# first argument (manager | extent-node | ps | bootstrap). See "Deployment" in docs/ops.md.

# Optional base-image registry prefix. Empty (default) → the official Docker Hub
# images (docker.io/library/...). Set it to build the base layers through a
# mirror when docker.io is slow or blocked:
#   docker build --build-arg BASE_REGISTRY=<mirror-host>/ ...   (host only; `library/` is in the FROM)
# Must include a trailing slash. Global ARG (declared before the first FROM) so
# BOTH stages' FROM lines can reference it.
ARG BASE_REGISTRY=

FROM ${BASE_REGISTRY}library/rust:1.95-bookworm AS builder
WORKDIR /build
# Optional crates.io mirror for faster dependency downloads. Empty (default) →
# official crates.io (unchanged for everyone else). Set CARGO_MIRROR to a full
# registry value and it replaces crates.io — e.g. a sparse mirror when
# crates.io is slow (use the sparse+ prefix):
#   --build-arg CARGO_MIRROR=sparse+https://<mirror-host>/index/
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

# One dependency is NOT on crates.io: `[patch.crates-io]` points the compio
# family at a fork (see the workspace Cargo.toml), so cargo fetches it from
# github.com. Optional GitHub mirror for that fetch. Empty (default) → github.com
# directly. If github.com is slow or unreachable:
#   --build-arg GITHUB_MIRROR=https://<mirror-host>/https://github.com/
# The mirror must answer git's fetch (the upload-pack POST) anonymously, not only
# `info/refs`. CARGO_NET_GIT_FETCH_WITH_CLI makes cargo fetch with `git`, so the
# `insteadOf` rewrite applies (its built-in fetcher does not read it).
# The UCX release tarball below comes through the same mirror, so it must also
# serve GitHub release downloads.
ARG GITHUB_MIRROR=
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN if [ -n "$GITHUB_MIRROR" ]; then \
      git config --global url."$GITHUB_MIRROR".insteadOf https://github.com/ \
      && echo "github via $GITHUB_MIRROR"; \
    fi
# libfuse3-dev + pkg-config are for `-p autumn-fuse`, which builds the MOUNT
# binary (fuser 0.15 links libfuse3). autumn-server alone would not need them —
# it depends on autumn-fs, which has no libfuse dependency —
# but the fuse daemon is now shipped so a consumer pod can mount the `fs/`
# namespace as a POSIX filesystem (docs/ops.md "Fuse daemon runbook").
# Optional Debian mirror for the apt steps in this stage. Empty (default) →
# whatever the base image ships (deb.debian.org), unchanged for everyone else.
# If deb.debian.org is slow or unreachable:
#   --build-arg APT_MIRROR=https://<mirror-host>
#
# Pass the HOST, not a full archive path. bookworm's sources are deb822
# (/etc/apt/sources.list.d/debian.sources) and name the archive and the security
# archive as two paths under ONE host:
#   URIs: http://deb.debian.org/debian
#   URIs: http://deb.debian.org/debian-security
# so replacing the host alone redirects both and keeps the suffixes intact. A
# trailing `/debian` or `/` is trimmed first, because that is the form mirror
# pages advertise and pasting it verbatim would otherwise yield `/debian/debian`.
# The old single-line sources.list is handled too, for a base image that has one.
ARG APT_MIRROR=
RUN if [ -n "$APT_MIRROR" ]; then \
      m="${APT_MIRROR%/}"; m="${m%/debian}"; m="${m%/}"; \
      sed -i -E "s#https?://deb\.debian\.org#${m}#g; s#https?://security\.debian\.org#${m}#g" \
        /etc/apt/sources.list /etc/apt/sources.list.d/*.sources /etc/apt/sources.list.d/*.list 2>/dev/null || true; \
      echo "apt via ${m}"; \
    fi
RUN apt-get update \
    && apt-get install -y --no-install-recommends libfuse3-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
# UCX for `--transport ucx` (AUTUMN_TRANSPORT=ucx). The binaries load libucp
# only when UCX is selected, so TCP uses none of this. Debian's UCX is older
# than the 1.17 the binaries require ("UCX library" in docs/ops.md), so it is
# built here and /opt/ucx is copied into the runtime stage. GITHUB_MIRROR
# applies to this download too; the checksum pins it.
ARG UCX_VERSION=1.20.1
ARG UCX_SHA256=545c419a7b5e04643cb8bff5a19b3b5071a8f8f0605f1e8efb36f8f3d7bfb9d3
RUN apt-get update \
    && apt-get install -y --no-install-recommends libibverbs-dev librdmacm-dev \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /tmp/ucx.tar.gz \
      "${GITHUB_MIRROR:-https://github.com/}openucx/ucx/releases/download/v${UCX_VERSION}/ucx-${UCX_VERSION}.tar.gz" \
    && echo "${UCX_SHA256}  /tmp/ucx.tar.gz" | sha256sum -c - \
    && tar xzf /tmp/ucx.tar.gz -C /tmp && cd /tmp/ucx-${UCX_VERSION} \
    && ./contrib/configure-release --prefix=/opt/ucx --disable-static --enable-mt --without-bfd \
      --with-verbs --with-rdmacm --without-cuda --without-rocm --without-java --without-go \
    && make -j"$(nproc)" && make install \
    && rm -rf /tmp/ucx* /opt/ucx/include /opt/ucx/share /opt/ucx/lib/*.la /opt/ucx/lib/ucx/*.la
COPY . .
# -p autumn-server builds exactly the server binaries, with UCX compiled in
# (loaded at run time, see above). Selecting it in k8s still needs hostNetwork,
# an RDMA device plugin and memlock ("Deployment" in docs/ops.md).
# autumn-dashboard is the standalone web UI (crates/server/src/bin/autumn_dashboard); it embeds its
# HTML via include_str!, so the binary alone is self-contained, and it drives the
# cluster only through the autumn-op we already ship.
# autumn-fuse is the FUSE mount daemon.
# autumn-s3 is the S3 gateway (a binary of the autumn-server crate, so it comes
# along with -p autumn-server); it serves model loaders and S3-native tools such
# as LanceDB. See "S3 gateway" in docs/ops.md and examples/lancedb-s3.
RUN cargo build --release \
    -p autumn-server -p autumn-fuse --features autumn-server/ucx,autumn-fuse/ucx

# Ship the SOURCE the Python client is built from, in this image.
#
# A consumer image that embeds the autumn client used to build the wheel from
# its own autumn-rs checkout, so the wheel's wire version came from whatever
# commit that checkout was at while its `autumn-fuse`/`autumnfs` binaries were
# lifted from a SEPARATELY pinned autumn-rs image. Nothing forced the two to
# agree, and a wheel at one wire version with binaries at another is a build
# that looks fine and dies at the handshake — or, if the fingerprint happens to
# match across a layout drift, decodes garbage silently.
#
# Carrying the source here makes that unbuildable: a consumer copies BOTH the
# binaries and the sources from one image tag, so they are the same commit by
# construction, and that tag becomes its only version knob. It also frees the
# consumer's Dockerfile from needing the autumn-rs repo as its build context.
#
# Source rather than a prebuilt wheel on purpose. The wheel is a native
# extension bound to the builder's glibc, and without the `abi3` feature to its
# exact CPython version. A wheel built on this image's Debian base would carry
# this glibc's manylinux tag, and a consumer on an older glibc would refuse it
# or fail at import. Letting the consumer compile against its own runtime
# sidesteps that; it costs 8 MB here and a Rust toolchain in the consumer's
# BUILD stage.

# Runtime: the server binaries link only libc/libgcc (UCX is dlopened from
# /opt/ucx, which needs libibverbs, librdmacm and the mlx5/efa providers); bookworm-slim provides
# bash + getent (libc-bin) which entrypoint.sh needs for DNS→IP resolution. The
# kernel comes from the k8s node and must be >= 5.15 (compio uses io_uring).
# fuse3 is for autumn-fuse: it brings libfuse3 (the binary links it) plus the
# fusermount3 setuid helper, which fuser needs both to mount and to clean up a
# stale mount after a crashed daemon.
FROM ${BASE_REGISTRY}library/debian:bookworm-slim
# Optional Debian mirror for the apt steps in this stage. Empty (default) →
# whatever the base image ships (deb.debian.org), unchanged for everyone else.
# If deb.debian.org is slow or unreachable:
#   --build-arg APT_MIRROR=https://<mirror-host>
#
# Pass the HOST, not a full archive path. bookworm's sources are deb822
# (/etc/apt/sources.list.d/debian.sources) and name the archive and the security
# archive as two paths under ONE host:
#   URIs: http://deb.debian.org/debian
#   URIs: http://deb.debian.org/debian-security
# so replacing the host alone redirects both and keeps the suffixes intact. A
# trailing `/debian` or `/` is trimmed first, because that is the form mirror
# pages advertise and pasting it verbatim would otherwise yield `/debian/debian`.
# The old single-line sources.list is handled too, for a base image that has one.
#
# https is DOWNGRADED to http in this stage, and it has to be. debian-slim
# ships no CA bundle, so apt cannot verify any TLS certificate until
# `ca-certificates` is installed — and that package is one of the two this
# stage installs. An https mirror here fails every index fetch with "No
# system certificates available. Try installing ca-certificates.", which is
# apt asking for the thing it is being run to fetch. Plain http costs nothing
# in trust: apt verifies the archive's GPG signature either way, and the base
# image's own default is http://deb.debian.org. The builder stage above keeps
# whatever scheme it was given — the rust image has certificates.
ARG APT_MIRROR=
RUN if [ -n "$APT_MIRROR" ]; then \
      m="${APT_MIRROR%/}"; m="${m%/debian}"; m="${m%/}"; \
      m="http://${m#*://}"; \
      sed -i -E "s#https?://deb\.debian\.org#${m}#g; s#https?://security\.debian\.org#${m}#g" \
        /etc/apt/sources.list /etc/apt/sources.list.d/*.sources /etc/apt/sources.list.d/*.list 2>/dev/null || true; \
      echo "apt via ${m}"; \
    fi
RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse3 ca-certificates \
      libibverbs1 librdmacm1 ibverbs-providers \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /opt/ucx/ /opt/ucx/
# Fail the build, not the first UCX start, if a library needs something only
# the builder has. /opt/ucx must stay at this path: the modules' RUNPATH names it.
RUN echo /opt/ucx/lib > /etc/ld.so.conf.d/ucx.conf && ldconfig \
    && ! ldd /opt/ucx/lib/*.so.0 /opt/ucx/lib/ucx/*.so.0 | grep 'not found' \
    && /opt/ucx/bin/ucx_info -v
ENV PATH=/opt/ucx/bin:$PATH
# The Rust workspace sources a consumer needs to build the Python client, so it
# can take the client and the binaries from ONE tag and never mismatch the wire
# version. ~8 MB. Nothing in this image uses them; there is no toolchain here.
COPY --from=builder /build/Cargo.toml /build/Cargo.lock /usr/local/share/autumn/src/
COPY --from=builder /build/crates/ /usr/local/share/autumn/src/crates/
COPY --from=builder /build/python/ /usr/local/share/autumn/src/python/
# Build flags (frame pointers; SSE4.2 for crc32c), found from the working dir.
COPY --from=builder /build/.cargo/config.toml /usr/local/share/autumn/src/.cargo/config.toml
COPY --from=builder \
    /build/target/release/autumn-manager-server \
    /build/target/release/autumn-extent-node \
    /build/target/release/autumn-ps \
    /build/target/release/autumn-client \
    /build/target/release/autumn-op \
    /build/target/release/autumnfs \
    /build/target/release/autumn-dashboard \
    /build/target/release/autumn-fuse \
    /build/target/release/autumn-s3 \
    /usr/local/bin/
COPY deploy/k8s/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
