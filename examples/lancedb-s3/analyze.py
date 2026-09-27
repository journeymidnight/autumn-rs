import json, sys, collections
c = collections.Counter(); ex = {}
for l in open(sys.argv[1]):
    r = json.loads(l)
    p = r["p"]; path, _, q = p.partition("?")
    if path.startswith("/__mark__"): continue
    qk = tuple(sorted(k.split("=")[0] for k in q.split("&") if k)) if q else ()
    depth = "bucket" if path.strip("/").count("/") == 0 else "object"
    hk = tuple(sorted(r["h"].keys() - {"content-length","content-type","x-amz-content-sha256"}))
    key = (r["m"], depth, qk, hk, r["st"])
    c[key] += 1; ex.setdefault(key, r)
for k, n in sorted(c.items(), key=lambda x: -x[1]):
    e = ex[k]
    print(n, k, e["p"][:100], {h:v for h,v in e["h"].items() if h not in ("x-amz-content-sha256","content-type")})
    if "resp" in e: print("    RESP:", e["resp"][:250])
    if "body" in e: print("    BODY:", e["body"][:300])
