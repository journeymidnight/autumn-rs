# Contributing to autumn-rs

Thanks for taking a look. Bug reports, questions, docs fixes and code are all
welcome. This page says how to build and test the tree, what a change is
expected to carry, and how to send it.

## Before you start

- **Questions and ideas**: open an issue; the feature-request template fits
  most. For anything bigger than a bug fix, open an issue first so the design
  can be agreed before you write the code.
- **Bugs**: use the bug-report template. The version (the image tag or
  the commit), the deployment (bare metal, Kubernetes, compose) and the exact
  error text are what make a report actionable.
- **Security issues**: do not open a public issue. Use GitHub's private
  vulnerability reporting ("Security" tab, "Report a vulnerability").

## Building

Linux ≥ 5.15 (the servers run on io_uring), Rust ≥ 1.95, and `etcd` ≥ 3.5 on
`PATH` for the integration tests. On x86_64 the build enables SSE4.2
(`.cargo/config.toml`).

```bash
cargo build --workspace --bins
```

The FUSE mount needs `libfuse3-dev` and `fuse3`. RDMA (UCX) is opt-in with
`--features ucx` and needs UCX ≥ 1.17. The Python client lives in `python/`
and builds with maturin (see `python/README.md`).

macOS builds the code and runs most unit tests, but the etcd-backed and FUSE
integration tests need Linux.

## Testing

The suite runs under [cargo-nextest](https://nexte.st), one process per test:

```bash
cargo install cargo-nextest --locked
cargo build --workspace --bins           # some tests spawn target/debug/autumn-*
cargo nextest run --workspace
cargo test --workspace --doc
```

CI runs the same with `--features autumn-manager/fuse-tests`, which adds the
FUSE tests (they need `fuse3` and `user_allow_other` in `/etc/fuse.conf`), and
with `toxiproxy-server` installed for the network-fault tests. A few heavy
tests are `#[ignore]` and run by hand; `docs/ops.md` "Test matrix" lists them.

To run one crate or one test file:

```bash
cargo test -p autumn-stream --lib
cargo test -p autumn-manager --test system_namespace_layer_a
```

## What a change carries

- **A test that fails without it.** For a bug fix, the test reproduces the bug.
  Before sending, revert the fix locally and check the test goes red. A test
  that passes either way proves nothing.
- **The root cause, not a guard.** Please do not add retries, timeouts or
  fallbacks around a symptom. Find why it happens and fix that. A log line
  that reports state is always fine.
- **Performance in mind.** This is a storage engine; say in the PR what the
  change costs on the data path, and include numbers if it touches it.
- **The crate's `CLAUDE.md` updated.** Each crate keeps a `CLAUDE.md` that
  records how it works and why it is shaped that way. When you change the
  behaviour it describes, update it in the same change.
- **Docs for anything user-visible.** Operations and manual verification
  steps live in `docs/ops.md`; `README.md` is the user-facing introduction.

Wire and on-disk formats are versioned by hand. If you change an RPC struct,
read "WIRE version discipline" in `crates/manager/CLAUDE.md` first. If you
change a persisted record, read "Persisted records" there too.

## Style

- `cargo fmt` and `cargo clippy` before sending.
- Comments say what the code cannot: the reason, the invariant, the trap.
  Keep them short.
- Commit subjects are a plain sentence saying what changed
  ("Clients resolve host names", not "fix: dns"). The body says why.

## Sending a pull request

1. Fork, branch from `main`, and keep the change to one topic.
2. Make sure `cargo nextest run` passes for the crates you touched.
3. Open the PR against `main`, describing the problem, the cause and how you
   verified the fix. CI must be green before review.

By contributing you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE), the license of this project.
