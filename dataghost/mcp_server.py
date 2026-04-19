from __future__ import annotations

from typing import Any

from dataghost.agent import DataGhostAgent
from dataghost.config import DataGhostConfig
from dataghost.llm import OllamaClient
from dataghost.tools.lineage import get_lineage
from dataghost.tools.schema_diff import get_schema_diff


def run_mcp_server(config: DataGhostConfig) -> None:
    """Expose DataGhost investigation tools through MCP stdio transport."""

    try:
        from mcp.server.fastmcp import FastMCP
    except Exception as error:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "MCP support requires the `mcp` package. Install it and retry."
        ) from error

    llm = OllamaClient(model=config.ollama_model, host=config.ollama_host)
    agent = DataGhostAgent(config=config, llm=llm)
    server = FastMCP("DataGhost")

    @server.tool()
    async def investigate_table(
        table_fqn: str,
        depth: int = 3,
        days: int = 7,
    ) -> dict[str, Any]:
        result = await agent.investigate(fqn=table_fqn, depth=depth, days=days)
        return {
            "report": result.report_markdown,
            "severity": result.severity,
            "affected_entities": result.affected_entities,
        }

    @server.tool()
    async def get_lineage_tool(table_fqn: str, depth: int = 3) -> dict[str, Any]:
        return await get_lineage(table_fqn, depth=depth, direction="both", config=config)

    @server.tool()
    async def get_schema_diff_tool(table_fqn: str, days: int = 7) -> dict[str, Any]:
        return await get_schema_diff(table_fqn, days=days, config=config)

    server.run(transport="stdio")
