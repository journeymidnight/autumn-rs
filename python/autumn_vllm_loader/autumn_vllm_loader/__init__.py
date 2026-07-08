"""autumn-vllm-loader: stream model weights from autumn into vLLM.

Importing this package registers the ``autumn`` vLLM load format. Use it with::

    vllm serve <local_dir_with_config_and_tokenizer> --load-format autumn \\
        --model-loader-extra-config '{"manager":"mgr:9001","path":"models/llama"}'

or programmatically::

    from vllm import LLM
    import autumn_vllm_loader  # registers "autumn"
    llm = LLM(model="<local_dir>", load_format="autumn",
              model_loader_extra_config={"manager":"mgr:9001","path":"models/llama"})

config.json + tokenizer are read by vLLM from the ``model=`` path as usual; only
the *.safetensors weights are streamed from autumn (the standard "config local,
weights from a streaming backend" split, like Run:ai Model Streamer / tensorizer).
"""

from __future__ import annotations

from .loader import AutumnModelLoader  # noqa: F401  (import registers "autumn")

__all__ = ["AutumnModelLoader", "register"]


def register() -> None:
    """vLLM `general_plugins` entry point. Importing this module already ran the
    ``@register_model_loader("autumn")`` decorator, so this is a no-op that just
    guarantees the import happened when vLLM loads the plugin."""

