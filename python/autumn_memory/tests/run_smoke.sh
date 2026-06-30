#!/usr/bin/env bash
# Self-contained smoke for the `autumn_memory` ergonomic layer (AutumnMemory):
#   build+install the autumn wheel + this package into a throwaway venv, bring
#   up an ISOLATED minimal cluster, drive AutumnMemory (with a FAKE embedder so
#   the vector/hybrid legs run without a real embeddings endpoint), tear down.
#
#   cargo build --workspace
#   bash python/autumn_memory/tests/run_smoke.sh
set -u
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/debug
WORK="${AM_WORK:-/tmp/am-erg}"
VENV="${AM_VENV:-/tmp/am-venv}"
MGR="127.0.0.1:19001"
for b in autumn-manager-server autumn-op autumn-extent-node autumn-ps; do
  [ -x "$BIN/$b" ] || { echo "FAIL: missing $BIN/$b — run: cargo build --workspace"; exit 2; }
done
rm -rf "$WORK"; mkdir -p "$WORK/en0" "$WORK/ps1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
wait_port() { for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[erg] venv + autumn wheel + autumn_memory package"
rm -rf "$VENV"; python3 -m venv "$VENV"
"$VENV/bin/pip" install -q maturin 2>&1 | tail -1
# shellcheck disable=SC1091
source "$VENV/bin/activate"
( cd python && maturin develop 2>&1 | tail -2 ) || { echo FAIL maturin; exit 1; }
pip install -q -e python/autumn_memory 2>&1 | tail -1

echo "[erg] cluster bring-up"
"$BIN/autumn-manager-server" --port 19001 --listen 127.0.0.1 >"$WORK/mgr.log" 2>&1 & PIDS+=($!)
wait_port 19001 20 || { echo FAIL mgr; tail -6 "$WORK/mgr.log"; exit 1; }
"$BIN/autumn-op" --manager "$MGR" format --listen :19101 --advertise 127.0.0.1:19101 "$WORK/en0" >"$WORK/fmt.log" 2>&1 || { echo FAIL fmt; cat "$WORK/fmt.log"; exit 1; }
"$BIN/autumn-extent-node" --data "$WORK/en0" --port 19101 --manager "$MGR" --cpuset 0 --listen 127.0.0.1 >"$WORK/en0.log" 2>&1 & PIDS+=($!)
wait_port 19101 20 || { echo FAIL en; tail -6 "$WORK/en0.log"; exit 1; }
sleep 3
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 >"$WORK/bs.log" 2>&1 || { echo FAIL bootstrap; cat "$WORK/bs.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port 19201 --manager "$MGR" --data "$WORK/ps1" --listen 127.0.0.1 --advertise 127.0.0.1:19201 >"$WORK/ps1.log" 2>&1 & PIDS+=($!)
wait_port 19201 20 || { echo FAIL ps; tail -6 "$WORK/ps1.log"; exit 1; }
sleep 4

echo "[erg] AutumnMemory smoke (fake embedder)"
python - <<'PY'
from autumn_memory import AutumnMemory

# A FAKE deterministic embedder (3-dim, keyed off a couple of marker words) so
# the vector/hybrid legs run end-to-end without a real embeddings endpoint.
def fake_embed(text):
    t = text.lower()
    return [
        1.0 if "cat" in t else 0.0,
        1.0 if "dog" in t else 0.0,
        1.0 if "quantum" in t else 0.0,
    ]

mem = AutumnMemory("127.0.0.1:19001", "__am_erg", "agent-1", embed=fake_embed)

# episodic (dicts in / out, JSON handled by the layer)
mem.append("s1", {"role": "user", "i": 0})
mem.append("s1", {"role": "asst", "i": 1})
r = mem.replay("s1"); assert [e["i"] for e in r] == [0, 1], r
assert mem.recent("s1", 1)[0]["i"] == 1

# facts (objects in / out)
mem.put_fact("p", "profile", {"name": "Alice", "lang": "rust"})
assert mem.get_fact("p", "profile")["name"] == "Alice"
assert mem.get_fact("p", "missing") is None
assert len(mem.list_facts("p")) == 1

# remember + search: remember() indexes BM25 + (because embed is set) the vector leg
mem.remember("d1", "the cat sat on the mat", {"src": "chat"})
mem.remember("d2", "dogs chase cats in the yard")
mem.remember("d3", "quantum error correction codes")

lex = mem.search("cat", 10, mode="lexical")
ids = [h["id"] for h in lex]
assert "d1" in ids and "d2" in ids and "d3" not in ids, ids          # plural fold: cat~cats
assert lex[0]["meta"] is None or isinstance(lex[0]["meta"], dict)

vec = mem.search("a cat please", 2, mode="vector")
assert vec[0]["id"] in ("d1", "d2"), vec                              # cat-direction docs

mem.train(3, 25, 7)
hy = mem.search("dog", 3, mode="hybrid")                              # auto would also be hybrid
assert any(h["id"] == "d2" for h in hy), hy                          # d2 = dogs+cats

for d in ("d1", "d2", "d3"): mem.forget(d)
mem.close()
print("ERG SMOKE OK: AutumnMemory full surface (json + embedder hook)")
PY
RC=$?; echo "===== erg-smoke exit: $RC ====="; exit $RC
