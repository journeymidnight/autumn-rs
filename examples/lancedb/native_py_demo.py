"""Python LanceDB straight onto Autumn, with no FUSE mount anywhere.

Mirrors fuse_demo.py's workload so the two are comparable, minus the hard-link
preamble: that check exists because Lance's LOCAL store publishes manifests
with linkat, and this path never touches a filesystem.

Needs the patched wheel from patches/0001-lancedb-python-autumn-provider.patch;
see README.md. Point it at an empty, dedicated scope: it creates and drops its
own table.
"""
import argparse
import concurrent.futures
import uuid

import lancedb

parser = argparse.ArgumentParser()
parser.add_argument("--manager", required=True, help="host:port of the Autumn manager")
parser.add_argument("--scope", required=True, help="existing namespace/sub-prefix, e.g. objects/lance-demo")
parser.add_argument("--db", default="lancedb", help="path under the scope holding the tables")
parser.add_argument("--credential-file", help="'<principal>\\n<hex>' from autumn-op principal-create")
parser.add_argument(
    "--with-session",
    action="store_true",
    help="pass an explicit lancedb.Session; without it connect() falls back to its own, "
    "which is the path the provider patch is easiest to miss",
)
args = parser.parse_args()

storage_options = {"autumn_scope": args.scope}
if args.credential_file:
    storage_options["autumn_credential_file"] = args.credential_file

uri = f"autumn://{args.manager}/{args.db}"
connect_kwargs = {"storage_options": storage_options}
if args.with_session:
    connect_kwargs["session"] = lancedb.Session()

db = lancedb.connect(uri, **connect_kwargs)
name = "vectors_" + uuid.uuid4().hex
table = db.create_table(name, [{"id": i, "vector": [float(i), 1.0, 2.0, 3.0]} for i in range(100)])
try:
    table.add([{"id": i, "vector": [float(i), 1.0, 2.0, 3.0]} for i in range(100, 200)])
    assert table.count_rows() == 200
    assert table.search([42.0, 1.0, 2.0, 3.0]).limit(1).to_list()[0]["id"] == 42
    table.delete("id < 10")
    assert table.count_rows() == 190

    def reader():
        other = lancedb.connect(uri, **connect_kwargs).open_table(name)
        for _ in range(20):
            assert other.search([42.0, 1.0, 2.0, 3.0]).limit(1).to_list()[0]["id"] == 42

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        reading = pool.submit(reader)
        for i in range(200, 220):
            table.add([{"id": i, "vector": [float(i), 1.0, 2.0, 3.0]}])
        reading.result()
    assert table.count_rows() == 210
    session_note = "explicit session" if args.with_session else "connect()'s own session"
    print(f"native Python LanceDB on {uri} ({session_note}): "
          "create, append, vector search, delete, concurrent reader/writer passed")
finally:
    db.drop_table(name)
