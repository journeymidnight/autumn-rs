"""Memory-tier eviction check: a hot set read every round must survive writes.

Writes `--pre` cold values, then `--hot` values, then each round reads every
hot value and writes `--cold` new ones (2 MiB each, batches of 256). Prints how
many hot values each round still finds; exits 1 if any round misses one.
Needs `import autumn` and a PS started with the memory tier on `kvc/`.
"""
import argparse, sys
import numpy as np
import autumn

ap = argparse.ArgumentParser()
ap.add_argument("--manager", required=True)
ap.add_argument("--transport", default="tcp")
ap.add_argument("--hot", type=int, default=512)
ap.add_argument("--cold", type=int, default=1024)
ap.add_argument("--pre", type=int, default=0)
ap.add_argument("--rounds", type=int, default=20)
a = ap.parse_args()

if a.transport != "tcp":
    autumn.set_transport(a.transport)
V, BATCH = 2 << 20, 256
bc = autumn.BatchClient(a.manager, scope="kvc")
src = np.random.default_rng(0).integers(0, 255, size=V, dtype=np.uint8)
dst = np.empty((BATCH, V), dtype=np.uint8)

def put(keys):
    for s in range(0, len(keys), BATCH):
        ks = keys[s:s + BATCH]
        assert all(bc.put_from(ks, [src] * len(ks), 0)), "put failed"

def found(keys):
    n = 0
    for s in range(0, len(keys), BATCH):
        ks = keys[s:s + BATCH]
        n += sum(bc.get_into(ks, [dst[i] for i in range(len(ks))]))
    return n

put([f"hotset/pre{i:06d}".encode() for i in range(a.pre)])
hot = [f"hotset/hot{i:06d}".encode() for i in range(a.hot)]
put(hot)
ok = True
for r in range(a.rounds):
    h = found(hot)
    ok &= h == a.hot
    print(f"round {r}: hot found {h}/{a.hot}", flush=True)
    put([f"hotset/c{r:03d}_{i:06d}".encode() for i in range(a.cold)])
sys.exit(0 if ok else 1)
