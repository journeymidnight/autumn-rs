# autumn-vllm-loader

A vLLM model loader (`--load-format autumn`) that streams safetensors weights
straight out of [autumn-rs](../../) into the model over the zero-copy
`autumn.Fs.read_into` seam + batched EN direct-read — the Run:ai-Model-Streamer
pattern on autumn's transport (RDMA/UCX or TCP).

Only the weights come from autumn; `config.json` + tokenizer are read by vLLM
from the `model=` path as usual (the standard "config local, weights from a
streaming backend" split).

## Install

```bash
# needs the `autumn` PyO3 extension (Fs.read_into) + torch + vllm in the env
pip install -e python/autumn_vllm_loader
```

## Use

Upload a model's weights into autumn, then point vLLM at a local dir for
config/tokenizer and at autumn for weights:

```bash
vllm serve /path/to/model_dir --load-format autumn \
    --model-loader-extra-config '{"manager":"mgr:9001","path":"models/llama","transport":"ucx","direct_read":true}'
```

```python
from vllm import LLM
import autumn_vllm_loader  # registers the "autumn" load format
llm = LLM(model="/path/to/model_dir", load_format="autumn",
          model_loader_extra_config={"manager": "mgr:9001", "path": "models/llama"})
```

`model_loader_extra_config` keys: `manager` (required), `path` (required — the
autumn directory holding the `*.safetensors`), `transport` (`tcp`|`ucx`),
`direct_read` (bool, default true), `n_workers` (default 4), `prefetch`
(default 8).
