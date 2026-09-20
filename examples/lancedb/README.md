# LanceDB on Autumn

The native integration uses autumn-object-store 0.14.1 against the LanceDB
FORK (thesues/lancedb, branch autumn-native, from 0.40.0-beta.3) and Lance
13.0.0-beta.6. The standalone workspace and lockfile keep Lance/DataFusion
dependencies out of the Autumn server workspace.

Both demos reach Autumn the same way, through the `autumn://` provider, so the
Rust one covers what Python does without building a wheel.

Use a cluster built from this branch (wire 44). Choose an **empty, dedicated**
scope under an existing namespace. The demo creates and deletes its own table;
its final offline vacuum requires no other users of the scope.

Run the native demo with AUTUMN_MANAGER and AUTUMN_OBJECT_SCOPE set:

    AUTUMN_MANAGER=127.0.0.1:9001 AUTUMN_OBJECT_SCOPE=objects/lance-demo cargo run --locked --manifest-path examples/lancedb/Cargo.toml --bin autumn-lancedb-demo

It creates 100 vectors, appends 100, searches for id 42, deletes 10 rows,
reopens through a separate Autumn client, then appends from two writers and
checks that all 210 rows survive. It verifies that manifests reside in Autumn,
deletes its objects and vacuums unreferenced chunks.

The example just connects: `autumn://<manager>/autumn-demo` with the scope as
a storage option. The fork registers the provider on the sessions it creates,
so there is no registry to assemble — in Rust or in Python. It
names NO commit handler: upstream lance hands an unknown scheme
UnsafeCommitHandler while the fork selects ConditionalPut for autumn://, so
spelling it out would hide the selection the demo exists to check. Its two
writers come from two connections, and each connect defaults its own session,
which means two registries and therefore two independent stores — sharing one
session would have shared a cached store and weakened the race.

The commit handler is selected only on the listing database's create and open
paths. Namespace-backed tables and clone_table still fall to lance's
UnsafeCommitHandler; do not use those against autumn:// for concurrent writes.

The remote feature is enabled solely because this LanceDB revision's job.rs
references Error::Http without a feature guard. No remote service is contacted.

Python, natively (no FUSE):

    cd ../../../lancedb && git checkout autumn-native     # the fork
    cd python && maturin build --release --out <dir>
    pip install <wheel>                                   # into a venv
    python native_py_demo.py --manager 127.0.0.1:9001 --scope objects/lance-demo
    python native_py_demo.py --manager 127.0.0.1:9001 --scope objects/lance-demo --with-session

A prebuilt wheel cannot be handed a custom ObjectStore: Python LanceDB resolves
a store from the URL SCHEME inside its own bundled Rust core, so provider/ has
to be COMPILED IN. That is why this is a fork rather than a patch applied at
build time — the changes are not confined to the Python bindings. Lance also
picks a commit handler from a hard-coded scheme table and gives anything it
does not recognise UnsafeCommitHandler, an unconditional manifest put, so the
fork names ConditionalPutCommitHandler for autumn:// on both the open and the
create path. Without that the adapter's compare-and-swap is bypassed and two
Python writers can claim one version.

The URL carries the manager, `autumn://<host>:<port>/<path>`, which makes a
dataset URI self-contained. The SCOPE is a storage option instead: new_store
receives the TABLE's url, so `autumn://mgr/objects/demo/vectors.lance` offers no
rule for where the scope ends and the object path begins, and a segment-counting
guess would write to the wrong prefix silently when it guessed wrong.

Authenticated clusters: pass `autumn_credential_file` in storage_options (or
--credential-file to the demo). It is read by the client's own
read_credential_file, so the labeled `principal:`/`credential:` pair that
autumn-op principal-create prints is accepted as-is.

Run it BOTH ways. `--with-session` passes an explicit Session; without it,
connect() falls back to a session it builds itself, and that is one of three
entry points a registration change has to cover.

FUSE validation (Python package tested: lancedb 0.39.0) — the fallback for an
UNPATCHED wheel, and the weaker path: it inherits the filesystem's
nontransactional multi-key metadata and per-mount serialization (see
../../docs/lancedb_validation.md). Prefer the native route above.

    python examples/lancedb/fuse_demo.py file:///path/to/test-mount/lancedb

This first tests create-only hard links, then creates a unique table, exercises
CRUD/vector search and a concurrent reader/writer, and drops that table. Use a
dedicated test mount. The local Lance backend publishes manifests with linkat,
which requires the FUSE hard-link support shipped with this adapter.

Matched object workload:

    AUTUMN_MANAGER=127.0.0.1:9001 AUTUMN_OBJECT_SCOPE=objects/bench cargo run --locked --manifest-path examples/lancedb/Cargo.toml --bin object_bench -- autumn
    AWS_ENDPOINT=http://127.0.0.1:9000 AWS_BUCKET=test-bucket AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1 cargo run --locked --manifest-path examples/lancedb/Cargo.toml --bin object_bench -- s3

Each backend runs the same c=8, 64-object reads/writes at 64 KiB, 1 MiB and
4 MiB, then 100 full scans of 1100 fragments. Output is JSON lines with MiB/s,
P50 and P99. Each run uses a unique prefix and deletes only its own objects.
Autumn's retired payloads remain until offline vacuum. For performance work,
build the client and servers in release mode and match replication/durability;
the initial acceptance measurements are explicitly debug-build smoke baselines.

See ../../docs/lancedb_validation.md for measured results and limitations, and
../../docs/ops.md for isolated cluster and upgrade instructions.




