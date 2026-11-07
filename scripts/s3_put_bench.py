"""PUT/GET throughput and p95 through the autumn-s3 gateway, stdlib only
(urllib, no signing, no hashing): N threads, COUNT objects of SIZE bytes.
Every GET is compared byte for byte with what was PUT."""
import argparse, os, time, statistics
from urllib.request import Request, urlopen
from concurrent.futures import ThreadPoolExecutor

ap = argparse.ArgumentParser()
ap.add_argument("--endpoint", default="http://127.0.0.1:9100")
ap.add_argument("--bucket", default="bench")
ap.add_argument("--size", type=int, default=16 << 20)
ap.add_argument("--conc", type=int, default=8)
ap.add_argument("--count", type=int, default=64)
ap.add_argument("--prefix", default="run")
ap.add_argument("--no-get", action="store_true")
a = ap.parse_args()
data = os.urandom(a.size)

def put(i):
    t = time.perf_counter()
    r = Request(f"{a.endpoint}/{a.bucket}/{a.prefix}/{i}", data=data, method="PUT")
    with urlopen(r, timeout=120) as resp:
        assert resp.status == 200, resp.status
        resp.read()
    return time.perf_counter() - t

def get(i):
    t = time.perf_counter()
    with urlopen(f"{a.endpoint}/{a.bucket}/{a.prefix}/{i}", timeout=120) as resp:
        b = resp.read()
    assert b == data, f"object {i} mismatch"
    return time.perf_counter() - t

def run(f, name):
    t0 = time.perf_counter()
    with ThreadPoolExecutor(a.conc) as ex:
        lat = sorted(ex.map(f, range(a.count)))
    el = time.perf_counter() - t0
    p95 = lat[int(0.95 * (len(lat) - 1))]
    print(f"{name} conc={a.conc} size={a.size>>20}MiB n={a.count}: "
          f"{a.count*a.size/el/2**20:7.0f} MiB/s  p50 {statistics.median(lat)*1e3:5.0f} ms  p95 {p95*1e3:5.0f} ms")

run(put, "PUT")
if not a.no_get:
    run(get, "GET")
