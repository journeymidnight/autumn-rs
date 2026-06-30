# hermes-memory-autumn

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) `MemoryProvider`
backed by **autumn-rs** (the autumn-memory backend). Gives a Hermes agent
durable, cross-session recall on the autumn cluster with **no extra daemon** —
every hook delegates to `autumn_memory.AutumnMemory` (the ergonomic layer over
the Rust core).

It implements the real `agent.memory_provider.MemoryProvider` ABC:

| Hook | Behavior |
|---|---|
| `initialize` | connect `AutumnMemory`; derive the `(tenant, agent)` namespace from the Hermes identity (`user_id` / `agent_identity`); start a background writer |
| `sync_turn` | append the turn to the episodic log + index it for recall (non-blocking — queued) |
| `queue_prefetch` / `prefetch` | recall relevant memory (background) and return the cached block (fast) |
| `get_tool_schemas` / `handle_tool_call` | expose + dispatch `memory_search` / `memory_store` |
| `on_memory_write` | mirror Hermes's built-in MEMORY.md / USER.md writes as facts |
| `shutdown` | drain the queue + close |

Non-primary agent contexts (`cron` / `subagent` / `flush`) are **read-only** —
their writes are skipped so they can't corrupt the user's memory.

## Install (user plugin)

The provider package is the directory `autumn/`. Hermes loads user plugins from
`$HERMES_HOME/plugins/<name>/`, so install it as `autumn`:

```bash
# 1. the autumn Python stack (the autumn PyO3 binding + ergonomic layer)
cd autumn-rs/python && maturin develop          # builds + installs `autumn`
pip install -e autumn-rs/python/autumn_memory    # installs `autumn_memory`

# 2. drop the provider into the Hermes plugins dir
cp -r autumn-rs/python/hermes_memory_autumn/autumn "$HERMES_HOME/plugins/autumn"

# 3. point it at your cluster + activate it
export AUTUMN_MEMORY_MANAGER=127.0.0.1:9001
#   (optional) semantic recall via an OpenAI-compatible /embeddings endpoint:
# export AUTUMN_MEMORY_EMBED_URL=http://127.0.0.1:30000/v1
# export AUTUMN_MEMORY_EMBED_MODEL=BAAI/bge-m3
```

Then set `memory.provider: autumn` in `$HERMES_HOME/config.yaml` (or
`hermes memory setup`).

## Config

| Source | Key | Meaning |
|---|---|---|
| env | `AUTUMN_MEMORY_MANAGER` | cluster manager `host:port` (also read from Hermes config `memory.autumn.manager`) |
| env | `AUTUMN_MEMORY_EMBED_URL` + `_MODEL` | optional embeddings endpoint → enables the vector / hybrid recall leg (else lexical BM25) |

## Verify

```bash
git clone https://github.com/NousResearch/hermes-agent /data/dongmao_dev/hermes-agent
cargo build --workspace
bash python/hermes_memory_autumn/tests/run_hermes_test.sh
#   → "HERMES PROVIDER OK: real MemoryProvider ABC ..." and "===== hermes-test exit: 0 ====="
```

The test drives the provider against the **real** Hermes `MemoryProvider` ABC
(from the checkout at `HERMES_AGENT_PATH`) and an isolated autumn cluster.
