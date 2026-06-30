"""autumn-memory-mcp — a stdio MCP server over `autumn_memory.AutumnMemory`.

A per-session **child process** an MCP host (Claude Desktop / Cursor / Cline /
ChatGPT Developer Mode) spawns over stdio — NOT a long-running daemon (plan
§12a). It is a thin shell around the Rust-backed agent-memory core: every tool
delegates to an `AutumnMemory` handle. Lexical (BM25) search needs no embedder,
so the default server runs with zero external dependencies beyond the cluster.

`build_server(mem)` is the testable factory (drive it in-process with the MCP
in-memory client transport); `main()` reads config (CLI / env, as MCP hosts
launch servers) and runs the stdio loop.
"""

from .server import build_server, main

__all__ = ["build_server", "main"]
