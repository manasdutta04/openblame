from __future__ import annotations

import asyncio
import json
from typing import Any

from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from dataghost.agent import DataGhostAgent
from dataghost.config import get_config
from dataghost.llm import OllamaClient
from dataghost.tools import lineage as lineage_tool
from dataghost.tools import schema_diff as diff_tool


server = Server("openblame")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="investigate_table",
            description=(
                "Run a full AI investigation on an OpenMetadata table. Returns an "
                "incident report with root cause, impact, evidence, and suggested fix."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_fqn": {
                        "type": "string",
                        "description": "Fully qualified table name, e.g. default.public.orders",
                    },
                    "depth": {"type": "integer", "default": 3, "description": "Lineage traversal depth"},
                    "days": {
                        "type": "integer",
                        "default": 7,
                        "description": "Lookback window in days for quality tests and schema diffs",
                    },
                },
                "required": ["table_fqn"],
            },
        ),
        types.Tool(
            name="get_lineage",
            description="Get upstream and downstream lineage for a table in OpenMetadata.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_fqn": {"type": "string"},
                    "depth": {"type": "integer", "default": 3},
                    "direction": {
                        "type": "string",
                        "enum": ["upstream", "downstream", "both"],
                        "default": "both",
                    },
                },
                "required": ["table_fqn"],
            },
        ),
        types.Tool(
            name="get_schema_diff",
            description="Get schema changes (column additions, removals, type changes) for a table over the last N days.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_fqn": {"type": "string"},
                    "days": {"type": "integer", "default": 7},
                },
                "required": ["table_fqn"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    config = get_config()
    fqn = str(arguments["table_fqn"])

    if name == "investigate_table":
        llm = OllamaClient(config.ollama_model, config.ollama_host)
        agent = DataGhostAgent(config, llm)
        result = await agent.investigate(
            fqn,
            depth=int(arguments.get("depth", 3)),
            days=int(arguments.get("days", 7)),
        )
        return [
            types.TextContent(
                type="text",
                text=json.dumps(
                    {
                        "report": result.report_markdown,
                        "severity": result.severity,
                        "root_cause": result.root_cause,
                        "affected_entities": result.affected_entities,
                        "fqn": result.fqn,
                    }
                    ,
                    indent=2,
                ),
            )
        ]

    if name == "get_lineage":
        data = await lineage_tool.get_lineage(
            fqn,
            int(arguments.get("depth", 3)),
            str(arguments.get("direction", "both")),
            config.openmetadata_host,
            config.openmetadata_jwt_token,
        )
        return [types.TextContent(type="text", text=json.dumps(data, indent=2))]

    if name == "get_schema_diff":
        data = await diff_tool.get_schema_diff(
            fqn,
            int(arguments.get("days", 7)),
            config.openmetadata_host,
            config.openmetadata_jwt_token,
        )
        return [types.TextContent(type="text", text=json.dumps(data, indent=2))]

    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


async def _serve() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def run() -> None:
    asyncio.run(_serve())
