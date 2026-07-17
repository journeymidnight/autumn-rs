"""Offline unit tests for tenant/model-identity isolation (BUG-KVC-TENANT).

No cluster, no vllm, no torch, no `autumn` native module required:

    cd python/autumn_kvcache && uv run --with pytest python -m pytest tests/test_tenant_identity.py -q

Covers the live incident class: Qwen2.5-7B-AWQ and Qwen2.5-32B-AWQ served via
autumn_vllm_loader (both with model path `/model-cfg`, same tokenizer ⇒ same
token ids ⇒ same content hash) MUST land in different tenants; and the
stability requirement: the same deployment MUST fingerprint identically across
processes or the external cache never hits.
"""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

from autumn_kvcache._identity import (
    FINGERPRINT_HEX_LEN,
    fingerprint_from_sources,
    tenant_cfg_from_vllm,
    vllm_identity_sources,
)
from autumn_kvcache._keys import build_tenant_suffix, full_key


# ── fakes mimicking vLLM 0.23 config shapes ─────────────────────────────────

class FakeModelConfig:
    """Duck-typed vLLM 0.23 ModelConfig: accessor methods + dtype/quant attrs."""

    def __init__(
        self,
        model="/model-cfg",
        layers=28,
        hidden=3584,
        kv_heads=4,
        head=128,
        vocab=152064,
        model_type="qwen2",
        dtype="torch.float16",
        quantization="awq",
        revision=None,
        use_mla=False,
    ):
        self.model = model
        self.dtype = dtype
        self.quantization = quantization
        self.revision = revision
        self.use_mla = use_mla
        self.hf_text_config = SimpleNamespace(model_type=model_type)
        self.hf_config = self.hf_text_config
        self._layers, self._hidden = layers, hidden
        self._kv_heads, self._head, self._vocab = kv_heads, head, vocab

    def get_total_num_hidden_layers(self):
        return self._layers

    def get_hidden_size(self):
        return self._hidden

    def get_total_num_kv_heads(self):
        return self._kv_heads

    def get_head_size(self):
        return self._head

    def get_vocab_size(self):
        return self._vocab


def fake_vllm_config(model_config=None, weights_path=None, load_format="autumn",
                     model_id=None, tp_size=1, pp_size=1, rank=0):
    extra = {"endpoint": "mgr:9001"}
    if model_id:
        extra["model_id"] = model_id
    mle = {"manager": "mgr:9001"}
    if weights_path:
        mle["path"] = weights_path
    return SimpleNamespace(
        model_config=model_config or FakeModelConfig(),
        parallel_config=SimpleNamespace(
            tensor_parallel_size=tp_size, pipeline_parallel_size=pp_size, rank=rank
        ),
        load_config=SimpleNamespace(load_format=load_format, model_loader_extra_config=mle),
        kv_transfer_config=SimpleNamespace(kv_connector_extra_config=extra),
    )


# The two live-incident models: SAME served path, same tokenizer, different arch.
QWEN_7B = dict(layers=28, hidden=3584, kv_heads=4, head=128)
QWEN_32B = dict(layers=64, hidden=5120, kv_heads=8, head=128)


def _tenant(vllm_config):
    cfg = tenant_cfg_from_vllm(vllm_config)
    return build_tenant_suffix(cfg, cfg.model_fingerprint)


# ── the incident: different models must get different tenants ───────────────

def test_live_incident_7b_vs_32b_same_path_different_tenant():
    t7 = _tenant(fake_vllm_config(FakeModelConfig(**QWEN_7B), weights_path="models/qwen7b"))
    t32 = _tenant(fake_vllm_config(FakeModelConfig(**QWEN_32B), weights_path="models/qwen32b"))
    assert t7 != t32
    # both still carry the readable model segment
    assert t7.startswith("model-cfg_") and t32.startswith("model-cfg_")


def test_same_arch_different_weights_path_different_tenant():
    """Two finetunes of the same architecture stored at different autumn paths
    (the autumn_vllm_loader case) must NOT share a tenant."""
    a = _tenant(fake_vllm_config(FakeModelConfig(**QWEN_7B), weights_path="models/qwen7b-base"))
    b = _tenant(fake_vllm_config(FakeModelConfig(**QWEN_7B), weights_path="models/qwen7b-sft"))
    assert a != b


def test_each_arch_dimension_splits_tenant():
    base = fake_vllm_config(FakeModelConfig(), weights_path="models/m")
    t0 = _tenant(base)
    for delta in (
        {"layers": 64},
        {"hidden": 5120},
        {"kv_heads": 8},
        {"head": 64},
        {"dtype": "torch.bfloat16"},
        {"quantization": None},
        {"model_type": "llama"},
    ):
        t = _tenant(fake_vllm_config(FakeModelConfig(**delta), weights_path="models/m"))
        assert t != t0, f"tenant did not split on {delta}"


def test_model_id_override_splits_tenant():
    a = _tenant(fake_vllm_config(FakeModelConfig(), weights_path="models/m", model_id="v1"))
    b = _tenant(fake_vllm_config(FakeModelConfig(), weights_path="models/m", model_id="v2"))
    none = _tenant(fake_vllm_config(FakeModelConfig(), weights_path="models/m"))
    assert len({a, b, none}) == 3


# ── stability: same deployment ⇒ same tenant, across processes ───────────────

def test_same_config_stable_tenant():
    mk = lambda: fake_vllm_config(FakeModelConfig(**QWEN_32B), weights_path="models/qwen32b")
    assert _tenant(mk()) == _tenant(mk())


def test_fingerprint_is_pinned_canonical_sha256():
    """Pin the exact canonicalisation (sorted `k=v` NUL-joined, sha256, first
    12 hex). If this test breaks, the tenant of every deployed model changes
    ⇒ full cold-cache rebuild — bump deliberately, never accidentally."""
    sources = {"layers": 28, "dtype": "torch.float16"}
    expect = hashlib.sha256(b"dtype=torch.float16\x00layers=28\x00").hexdigest()[:FINGERPRINT_HEX_LEN]
    assert fingerprint_from_sources(sources) == expect
    # insertion order must not matter
    assert fingerprint_from_sources({"dtype": "torch.float16", "layers": 28}) == expect


def test_fingerprint_empty_and_all_none():
    assert fingerprint_from_sources(None) is None
    assert fingerprint_from_sources({}) is None
    assert fingerprint_from_sources({"a": None, "b": None}) is None


# ── source extraction: vLLM 0.23 accessors + degraded fallbacks ─────────────

def test_identity_sources_prefer_model_config_accessors():
    src = vllm_identity_sources(fake_vllm_config(FakeModelConfig(**QWEN_7B), weights_path="models/q"))
    assert src["layers"] == 28 and src["hidden"] == 3584
    assert src["kv_heads"] == 4 and src["head"] == 128
    assert src["dtype"] == "torch.float16" and src["quant"] == "awq"
    assert src["model_type"] == "qwen2"
    assert src["weights_path"] == "models/q"
    assert src["load_format"] == "autumn"
    assert "mla" not in src


def test_identity_sources_hf_config_fallback():
    """A config exposing only raw HF attributes (no accessor methods) still
    yields the architecture fingerprint."""
    mc = SimpleNamespace(
        model="/model-cfg",
        dtype="torch.bfloat16",
        quantization=None,
        hf_text_config=SimpleNamespace(
            num_hidden_layers=64, hidden_size=5120, num_key_value_heads=8,
            head_dim=128, vocab_size=152064, model_type="qwen2",
        ),
    )
    src = vllm_identity_sources(SimpleNamespace(model_config=mc))
    assert src["layers"] == 64 and src["hidden"] == 5120
    assert src["kv_heads"] == 8 and src["head"] == 128 and src["vocab"] == 152064


def test_identity_sources_load_format_enum_like():
    lc = SimpleNamespace(
        load_format=SimpleNamespace(value="AUTO"), model_loader_extra_config={}
    )
    src = vllm_identity_sources(SimpleNamespace(load_config=lc))
    assert src["load_format"] == "auto"


def test_identity_sources_mla_flag():
    src = vllm_identity_sources(fake_vllm_config(FakeModelConfig(use_mla=True)))
    assert src["mla"] == 1


def test_unfingerprintable_config_degrades_to_none():
    cfg = tenant_cfg_from_vllm(SimpleNamespace())
    assert cfg.model_fingerprint is None
    # tenant falls back to the legacy format (loudly warned at connector init)
    assert build_tenant_suffix(cfg, cfg.model_fingerprint) == "unknown_0_1"


# ── legacy behavior must not regress (sglang default / MLA / tp / pp) ────────

def _sg_cfg(**kw):
    base = dict(model_name="smoke-model", tp_rank=0, tp_size=1, pp_rank=0,
                pp_size=1, is_mla_model=False)
    base.update(kw)
    return SimpleNamespace(**base)


def test_sglang_default_tenant_unchanged():
    # exact strings from before this change — existing sglang caches keep hitting
    assert build_tenant_suffix(_sg_cfg()) == "smoke-model_0_1"
    assert build_tenant_suffix(_sg_cfg(tp_rank=1, tp_size=4)) == "smoke-model_1_4"
    assert build_tenant_suffix(_sg_cfg(is_mla_model=True)) == "smoke-model"
    assert (
        build_tenant_suffix(_sg_cfg(pp_rank=1, pp_size=2)) == "smoke-model_0_1_pp1_2"
    )
    assert build_tenant_suffix(_sg_cfg(model_name="/data/m/x")) == "data_m_x_0_1"
    assert build_tenant_suffix(None) == "default"


def test_fingerprint_slots_between_model_and_parallel_suffix():
    assert build_tenant_suffix(_sg_cfg(), "abc123") == "smoke-model_abc123_0_1"
    assert build_tenant_suffix(_sg_cfg(is_mla_model=True), "abc123") == "smoke-model_abc123"
    assert (
        build_tenant_suffix(_sg_cfg(pp_rank=1, pp_size=2), "abc123")
        == "smoke-model_abc123_0_1_pp1_2"
    )


def test_vllm_tenant_keeps_tp_pp_semantics():
    t = _tenant(fake_vllm_config(FakeModelConfig(**QWEN_7B), weights_path="models/q",
                                 tp_size=4, rank=2))
    assert t.endswith("_2_4")
    # MLA drops the tp suffix (rank-independent KV) but keeps the fingerprint
    tm = _tenant(fake_vllm_config(FakeModelConfig(**QWEN_7B, use_mla=True),
                                  weights_path="models/q", tp_size=4, rank=2))
    assert not tm.endswith("_2_4")
    assert tm != "model-cfg"  # fingerprint present


def test_full_key_layout_with_fingerprint():
    cfg = tenant_cfg_from_vllm(fake_vllm_config(FakeModelConfig(**QWEN_32B),
                                                weights_path="models/qwen32b"))
    tenant = build_tenant_suffix(cfg, cfg.model_fingerprint)
    key = full_key(tenant, "v1/deadbeef/model.layers.0", "vllm")
    assert key.startswith(b"kvc/model-cfg_")
    assert key.endswith(b"/vllm/v1/deadbeef/model.layers.0")


def test_mla_detection_wired_from_use_mla():
    """vLLM-side is_mla_model used to be hardcoded False; it now follows
    ModelConfig.use_mla."""
    assert tenant_cfg_from_vllm(fake_vllm_config(FakeModelConfig(use_mla=True))).is_mla_model
    assert not tenant_cfg_from_vllm(fake_vllm_config(FakeModelConfig())).is_mla_model
