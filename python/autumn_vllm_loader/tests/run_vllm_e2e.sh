#!/usr/bin/env bash
# F-VLLM-LOADER e2e: vLLM loads a real model from autumn via --load-format autumn.
# Local TCP cluster + venv vLLM. Uploads gte-Qwen2-1.5B weights to autumn, then
# vLLM loads them through AutumnModelLoader and runs a forward pass.
set -u
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)" || exit 2
BIN=target/release
WORK="${AFS_WORK:-/tmp/afs-vllm}"
MGR="127.0.0.1:19701"
VENV=/root/vllm-venv/bin
export CUDA_VISIBLE_DEVICES=5
SNAP=$(ls -d ~/.cache/huggingface/hub/models--Alibaba-NLP--gte-Qwen2-1.5B-instruct/snapshots/*/ 2>/dev/null | head -1)
[ -n "$SNAP" ] || { echo "FAIL: gte snapshot not found"; exit 2; }
echo "[t] model snapshot: $SNAP"
rm -rf "$WORK"; mkdir -p "$WORK/ps1"
PIDS=(); cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }; trap cleanup EXIT
wait_port(){ for _ in $(seq 1 "${2:-20}"); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

echo "[t] cluster bring-up (2 EN, TCP)"
"$BIN/autumn-manager-server" --port 19701 --listen 127.0.0.1 >"$WORK/mgr.log" 2>&1 & PIDS+=($!)
wait_port 19701 20 || { echo FAIL mgr; tail "$WORK/mgr.log"; exit 1; }
for i in 0 1; do P=$((19731+i*100)); mkdir -p "$WORK/en$i"
  "$BIN/autumn-op" --manager "$MGR" format "$WORK/en$i" >"$WORK/fmt$i.log" 2>&1 || { echo FAIL fmt$i; cat "$WORK/fmt$i.log"; exit 1; }
  "$BIN/autumn-extent-node" --data "$WORK/en$i" --port "$P" --manager "$MGR" --cpuset "$i" --advertise "127.0.0.1:$P" --listen 127.0.0.1 >"$WORK/en$i.log" 2>&1 & PIDS+=($!)
  wait_port "$P" 20 || { echo FAIL en$i; exit 1; }; done
sleep 3
"$BIN/autumn-op" --manager "$MGR" bootstrap --replication 1+0 >"$WORK/bs.log" 2>&1 || { echo FAIL bootstrap; cat "$WORK/bs.log"; exit 1; }
"$BIN/autumn-ps" --psid 1 --port 19721 --manager "$MGR" --data "$WORK/ps1" --listen 127.0.0.1 --advertise 127.0.0.1:19721 >"$WORK/ps1.log" 2>&1 & PIDS+=($!)
wait_port 19721 20 || { echo FAIL ps; tail "$WORK/ps1.log"; exit 1; }
sleep 4

echo "[t] upload model weights to autumn:models/gte"
AUTUMN_MANAGER="$MGR" SNAP="$SNAP" "$VENV/python" - <<'PY'
import os, glob, autumn
MGR=os.environ["AUTUMN_MANAGER"]; SNAP=os.environ["SNAP"]; ROOT=1
fs=autumn.Fs.connect(MGR)
def ensure_dir(path):
    ino=fs.resolve(path)
    if ino is not None: return ino
    parent,name=path.rsplit("/",1); pino=ensure_dir(parent) if parent else ROOT
    try: fs.mkdir(pino,name)
    except Exception: pass
    return fs.resolve(path)
gte=ensure_dir("/models/gte")
shards=sorted(glob.glob(os.path.join(SNAP,"*.safetensors")))
assert shards, "no local safetensors"
for sp in shards:
    nm=os.path.basename(sp); ino=fs.create(gte,nm); tot=os.path.getsize(sp)
    with open(sp,"rb") as f:
        off=0
        while True:
            b=f.read(64<<20)
            if not b: break
            assert fs.write(ino,off,b)==len(b); off+=len(b)
    fs.flush(ino); assert fs.getattr(ino)["size"]==tot
    print(f"  uploaded {nm} ({tot/1e9:.2f} GB)")
print("upload done:", [n for (n,_,_) in fs.readdir(gte)])
PY
[ $? -eq 0 ] || { echo "FAIL upload"; exit 1; }

echo "[t] vLLM load via --load-format autumn + forward pass"
AUTUMN_MANAGER="$MGR" SNAP="$SNAP" "$VENV/python" - <<'PY'
import os, torch, autumn_vllm_loader
from vllm import LLM
MGR=os.environ["AUTUMN_MANAGER"]; SNAP=os.environ["SNAP"]
llm = LLM(model=SNAP, load_format="autumn", runner="pooling",
          model_loader_extra_config={"manager":MGR,"path":"models/gte","direct_read":True,"n_workers":4},
          enforce_eager=True, gpu_memory_utilization=0.30)
outs = llm.embed(["autumn streams model weights straight into vLLM"])
v = torch.tensor(outs[0].outputs.embedding)
print(f"[t] embedding dim={v.numel()} finite={torch.isfinite(v).all().item()} "
      f"norm={v.float().norm().item():.4f} first5={v[:5].tolist()}")
assert torch.isfinite(v).all() and v.float().norm().item() > 0, "degenerate embedding"
print("VLLM-AUTUMN-LOAD OK — model loaded from autumn via --load-format autumn, forward pass valid")
PY
rc=$?
echo "[t] exit=$rc"
exit $rc
