"""Run against a dedicated test FUSE mount; creates a unique table."""
import argparse
import concurrent.futures
import os
import tempfile
import uuid
from urllib.parse import unquote, urlparse

import lancedb

parser = argparse.ArgumentParser()
parser.add_argument("uri", help="file:///absolute/path/inside/test-mount/lancedb")
args = parser.parse_args()
# Exercise the exact publication primitive used by Lance's local store.
parsed = urlparse(args.uri)
if parsed.scheme != "file" or not parsed.path.startswith("/"):
    parser.error("uri must be an absolute file:// URI inside the Autumn FUSE mount")
base = unquote(parsed.path)
os.makedirs(base, exist_ok=True)
with tempfile.TemporaryDirectory(prefix="hardlink-", dir=base) as scratch:
    source, target, loser = [
        os.path.join(scratch, name) for name in ("source", "target", "loser")
    ]
    with open(source, "wb") as out:
        out.write(b"winner")
    with open(loser, "wb") as out:
        out.write(b"loser")
    os.link(source, target)
    try:
        os.link(loser, target)
        raise AssertionError("hard link replaced an existing destination")
    except FileExistsError:
        pass
    os.unlink(source)
    with open(target, "rb") as inp:
        assert inp.read() == b"winner"
db = lancedb.connect(args.uri)
name = "vectors_" + uuid.uuid4().hex
table = db.create_table(
    name,
    [{"id": i, "vector": [float(i), 1.0, 2.0, 3.0]} for i in range(100)],
)
try:
    table.add(
        [
            {"id": i, "vector": [float(i), 1.0, 2.0, 3.0]}
            for i in range(100, 200)
        ]
    )
    assert table.count_rows() == 200
    assert table.search([42.0, 1.0, 2.0, 3.0]).limit(1).to_list()[0]["id"] == 42
    table.delete("id < 10")
    assert table.count_rows() == 190

    def reader():
        other = lancedb.connect(args.uri).open_table(name)
        for _ in range(20):
            nearest = other.search([42.0, 1.0, 2.0, 3.0]).limit(1).to_list()
            assert nearest[0]["id"] == 42

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        reading = pool.submit(reader)
        for i in range(200, 220):
            table.add([{"id": i, "vector": [float(i), 1.0, 2.0, 3.0]}])
        reading.result()
    assert table.count_rows() == 210
    print(
        "FUSE LanceDB: create, append, vector search, delete, "
        "concurrent reader/writer passed"
    )
finally:
    db.drop_table(name)
