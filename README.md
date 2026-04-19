# OpenBlame

OpenBlame is a local-first CLI investigation agent for data pipelines running on OpenMetadata. Point it to a table, and it will trace lineage, inspect recent quality failures, parse schema-change events, surface governance gaps, and collect owner metadata to build a concrete incident narrative.

The reasoning layer runs on a local Ollama model, so investigations stay inside your environment. This makes OpenBlame useful for secure internal datasets and fast incident response workflows where you want reproducible, metadata-driven triage without external LLM APIs. The project behaves like "git blame for your data pipeline," but with lineage, observability, and governance context in one investigation loop.

## Why It Stands Out

- Autonomous investigation loop: plan, gather metadata, reason, and draft an incident report
- Native OpenMetadata story: lineage, quality, schema history, owners, tags, domain, and tier
- Governance-aware triage: missing owner, missing tier, missing tags, and missing description are surfaced as explicit operational risks
- Local-first by default: Ollama only, no external LLM API required
- Demo-ready outputs: Rich terminal UX, markdown incident report, and MCP server wrapper

## Architecture

```text
                         +--------------------+
                         |    openblame CLI   |
                         | Typer + Rich UX    |
                         +---------+----------+
                                   |
                                   v
                         +--------------------+
                         |  OpenBlame Agent   |
                         |  ReAct-style loop  |
                         +----+----------+----+
                              |          |
                +-------------+          +------------------+
                v                                         v
   +---------------------------+              +---------------------------+
   | OpenMetadata REST tools   |              | Local Ollama Reasoner     |
   | lineage/quality/diff/owner|              | plan() + reason()         |
   +-------------+-------------+              +-------------+-------------+
                 |                                            |
                 v                                            v
        +---------------------+                     +---------------------+
        | Structured evidence |-------------------->| Markdown report      |
        +---------------------+                     +---------------------+
```

## Prerequisites

- Python 3.11+
- OpenMetadata instance reachable from your machine
- OpenMetadata JWT token with read access
- Ollama installed locally and running (`ollama serve`)
- An installed local model such as `llama3`

## Installation

```bash
pip install openblame
```

For local development:

```bash
pip install -e ".[dev]"
```

Run tests with plugin autoload disabled so unrelated third-party pytest plugins from
`openmetadata-ingestion` do not interfere:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/ -v -p pytest_asyncio.plugin
```

## Quick Start

1. Copy `.env.example` to `.env` and set credentials.
2. Run:

```bash
openblame investigate default.public.orders --depth 3 --days 7
```

Example output:

```text
[CRITICAL] default.public.orders
Root Cause: `order_total` type changed from DECIMAL to STRING without downstream migration.
Impact: 6 downstream tables plus BI dashboard refresh failures.
Owner: Data Platform (data-platform@company.com)
Suggested Fix: Restore compatible type, backfill, rerun failed checks.
```

## Demo Flow

The strongest demo is a single broken metric traced end to end:

1. Pick a table with a recent schema drift or quality failure.
2. Run `openblame investigate <table_fqn>`.
3. Show the agent plan, anomaly panels, governance risk briefing, and final incident report.
4. Highlight the downstream blast radius and owner handoff.
5. End by showing the MCP server or generated GitHub issue payload.

## CLI Commands

### Investigate

```bash
openblame investigate <table_fqn> --depth 3 --days 7 --output report.md --model llama3
```

Runs the full investigation loop, prints a Rich report, optionally writes markdown, and can suggest a GitHub issue payload.

### Schema Diff

```bash
openblame diff default.public.orders --days 7
```

Prints table schema changes over the lookback window.

### Lineage

```bash
openblame lineage default.public.orders --depth 3 --direction both
```

Renders upstream and downstream lineage as a Rich tree.

### MCP Server

```bash
openblame mcp-server
```

Starts OpenBlame as an MCP stdio server exposing investigation tools.

## Configuration

OpenBlame reads `.env` and environment variables:

```env
OPENMETADATA_HOST=http://localhost:8585
OPENMETADATA_JWT_TOKEN=<token>
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=llama3
```

## MCP Server Setup

The server exposes:

- `investigate_table({ table_fqn, depth, days })`
- `get_lineage({ table_fqn, depth, direction })`
- `get_schema_diff({ table_fqn, days })`

Use stdio transport with:

```bash
openblame mcp-server
```

## How It Works

1. Fetch baseline metadata such as owners and schema snapshot.
2. Ask Ollama to produce an investigation plan.
3. Execute OpenMetadata tools in parallel across lineage, quality, schema history, and ownership.
4. Convert raw metadata into evidence, anomalies, governance risks, and downstream blast radius summaries.
5. Send gathered evidence back to Ollama for incident reasoning.
6. Render and optionally persist a markdown report.

Tool failures are non-fatal. OpenBlame continues with partial data whenever possible.

## Publishing

This repo is set up for GitHub Actions CI and can be wired for PyPI Trusted Publishing. Once the PyPI package-name issue is resolved, publishing can be automated from GitHub releases.

## Hackathon Context

Built for the WeMakeDevs x OpenMetadata hackathon as an AI-powered metadata investigator focused on local-first reasoning, blast-radius analysis, and governance-aware incident response workflows.
