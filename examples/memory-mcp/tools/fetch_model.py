#!/usr/bin/env python3
"""Download a Model2Vec static embedding model and convert it to the compact
`M2VS` int8 table the memory-mcp `--features static-embed` build loads.

Model2Vec is a distilled token→vector lookup table (no transformer at inference),
so the "model" is one `[vocab, dim]` float matrix + a `tokenizer.json`. We
int8-quantize the matrix (one global scale) and emit:

    [u8;4 "M2VS"][u32 version=1][u32 vocab][u32 dim][f32 scale][i8 vocab*dim]

Keeps the ~30 MB blob OUT of the repo — run once for real semantics; otherwise
the default hash embedder needs no model.

    python3 tools/fetch_model.py --out model.m2vs --tokenizer-out tokenizer.json

Requires: huggingface_hub, numpy, safetensors. potion-base-8M is MIT + 256-dim,
matching the example's EMBED_DIM.
"""
import argparse
import shutil
import struct
import sys


def die(msg):
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--model", default="minishlab/potion-base-8M")
    ap.add_argument("--out", default="model.m2vs")
    ap.add_argument("--tokenizer-out", default="tokenizer.json")
    ap.add_argument("--dim", type=int, default=256)
    args = ap.parse_args()

    try:
        import numpy as np
        from huggingface_hub import hf_hub_download
        from safetensors.numpy import load_file
    except ImportError as e:
        die(f"missing dependency ({e}); pip install huggingface_hub numpy safetensors")

    print(f"downloading {args.model} …", file=sys.stderr)
    st = load_file(hf_hub_download(args.model, "model.safetensors"))
    tok = hf_hub_download(args.model, "tokenizer.json")

    mat = None
    for name, arr in st.items():
        if arr.ndim == 2:
            mat = arr.astype(np.float32)
            print(f"using tensor '{name}' shape={arr.shape}", file=sys.stderr)
            break
    if mat is None:
        die("no 2-D embedding tensor in model.safetensors")

    vocab, dim = mat.shape
    if dim != args.dim:
        die(f"model dim {dim} != --dim {args.dim}; pick a matching model or change EMBED_DIM")

    scale = (float(np.abs(mat).max()) or 1.0) / 127.0
    q = np.clip(np.round(mat / scale), -127, 127).astype(np.int8)

    with open(args.out, "wb") as f:
        f.write(b"M2VS")
        f.write(struct.pack("<III", 1, vocab, dim))
        f.write(struct.pack("<f", scale))
        f.write(q.tobytes())
    shutil.copyfile(tok, args.tokenizer_out)
    print(f"wrote {args.out} ({vocab}x{dim} int8, {(20 + vocab*dim)/1e6:.1f} MB) "
          f"and {args.tokenizer_out}", file=sys.stderr)


if __name__ == "__main__":
    main()
