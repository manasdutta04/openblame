# OpenBlame - Agent Guide

> Purpose: concise reference for AI coding agents working in this repo.

---

## What This Project Does

OpenBlame is a local-first CLI investigation agent for OpenMetadata pipelines.  
Given a table FQN, it gathers lineage, quality, schema-change, ownership, and governance metadata, then asks a local Ollama model to produce an incident report.

- No external LLM API required (Ollama only)
- Tool failures are non-fatal and returned as partial dicts with an `error` key
- Surfaces available: CLI (`typer` + `rich`), markdown output, MCP stdio server

---

## Repo Layout

```text
openblame/
|-- openblame/
|   |-- __init__.py
|   |-- agent.py            # OpenBlameAgent + AgentResult
|   |-- cli.py              # Typer entrypoint (openblame)
|   |-- config.py           # Pydantic settings from .env
|   |-- llm.py              # OllamaClient
|   |-- mcp_server.py       # MCP server wrapper
|   |-- reporter.py         # Rich output + markdown save
|   `-- tools/
|       |-- __init__.py
|       |-- lineage.py
|       |-- owners.py
|       |-- quality.py
|       `-- schema_diff.py
|-- tests/
|   |-- conftest.py
|   |-- test_agent.py
|   `-- test_tools.py
|-- .env.example
|-- pyproject.toml
`-- README.md
```

---

## Runtime Flow

```text
CLI (typer)
  -> OpenBlameAgent.investigate(fqn, depth, days)
     -> [parallel] owners.get_owners_and_tags()
     -> [parallel] schema_diff.get_schema_diff()
     -> OllamaClient.plan() -> list[str]
     -> [parallel] lineage.get_lineage()
     -> [parallel] quality.get_quality_tests()
     -> OllamaClient.reason() -> markdown
     -> AgentResult -> reporter / MCP / file output
```

All metadata tools are async `httpx` calls and are gathered with:
`asyncio.gather(..., return_exceptions=True)`.

---

## Key Modules

### `openblame/agent.py`

- `AgentResult`: dataclass carrying investigation outputs
- `OpenBlameAgent`: main orchestrator (`investigate`)
- `_safe_dict`: converts gather exceptions into structured fallback dicts
- `_governance_risks`, `_build_anomalies`, `_build_evidence`: summary builders

### `openblame/llm.py`

- `chat`: wrapper over Ollama chat API
- `plan`: asks model for a JSON list of investigation steps
- `reason`: asks model for markdown incident report with required sections
- `test_connection`: checks local Ollama availability

### `openblame/tools/*`

All tools accept explicit `host` and `token` and return dicts.

- `lineage.get_lineage(...)` -> upstream/downstream graph slice
- `owners.get_owners_and_tags(...)` -> owner/tags/description/tier/domain
- `quality.get_quality_tests(...)` -> pass/fail counts + failure details
- `schema_diff.get_schema_diff(...)` -> current columns + parsed feed changes

### `openblame/reporter.py`

Single presentation layer for Rich tables/panels/trees and report persistence.

### `openblame/config.py`

`Settings` fields:

- `OPENMETADATA_HOST` (default `http://localhost:8585`)
- `OPENMETADATA_JWT_TOKEN` (required)
- `OLLAMA_HOST` (default `http://localhost:11434`)
- `OLLAMA_MODEL` (default `qwen2.5:7b`)

Use `get_config()` (cached), not direct `Settings()` construction in app code.

### `openblame/mcp_server.py`

Exposes MCP tools:

- `investigate_table`
- `get_lineage`
- `get_schema_diff`

Run with `openblame mcp-server`.

---

## CLI Commands

```bash
openblame investigate <table_fqn> --depth 3 --days 7 --output report.md --model qwen2.5:7b
openblame diff <table_fqn> --days 7
openblame lineage <table_fqn> --depth 3 --direction both
openblame mcp-server
```

---

## Development Setup

```bash
pip install -e ".[dev]"
cp .env.example .env
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/ -v -p pytest_asyncio.plugin
```

---

## Invariants You Should Keep

1. Every tool returns a dict and should not raise outward.
2. Keep parallel tool calls in `asyncio.gather(..., return_exceptions=True)`.
3. Route all config via `Settings` / `get_config()`.
4. Keep business logic out of `reporter.py` and printing out of tools.
5. Keep fallback behavior resilient when metadata or LLM calls fail.

---

## Typical Extension Tasks

### Add a New Metadata Tool

1. Add `openblame/tools/my_tool.py` with async function returning dict.
2. Catch exceptions and return fallback dict with `error`.
3. Call it from `OpenBlameAgent.investigate` in parallel with other tools.
4. Add result field to `AgentResult`.
5. Surface results in anomalies/evidence/reporter output.
6. Add tool tests in `tests/test_tools.py`.

### Add a New MCP Tool

1. Register schema in `list_tools()` in `mcp_server.py`.
2. Add branch in `call_tool()`.
3. Return JSON text payload via `types.TextContent`.

### Change Prompting

- Edit plan prompt in `llm.py` (`plan`)
- Edit reasoning system prompt in `llm.py` (`reason`)
- If report section format changes, verify severity and root-cause extraction logic in `agent.py`

---

## What Not To Do

- Do not import agent/LLM objects inside tool modules
- Do not read env vars ad-hoc in tool code
- Do not use `asyncio.run()` inside async functions
- Do not add raw `print()` calls in logic layers

