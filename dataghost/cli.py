from __future__ import annotations

import asyncio
import json
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
import typer

from dataghost.agent import DataGhostAgent
from dataghost.config import DataGhostConfig, get_config
from dataghost.llm import OllamaClient, OllamaConnectionError
from dataghost.mcp_server import run_mcp_server
from dataghost.reporter import (
    print_investigation_header,
    print_lineage_tree,
    print_planning_step,
    print_reasoning_step,
    print_report,
    print_schema_diff_table,
    save_report,
)
from dataghost.tools.common import (
    OpenMetadataConnectionError,
    TableNotFoundError,
    check_openmetadata_connection,
    resolve_table_entity,
)
from dataghost.tools.lineage import get_lineage
from dataghost.tools.schema_diff import get_schema_diff

app = typer.Typer(help="DataGhost - AI-powered data pipeline investigator")
console = Console()

def _error_panel(title: str, message: str) -> None:
    console.print(Panel(message, title=title, border_style="red"))
async def _ensure_table_exists(config: DataGhostConfig, table_fqn: str) -> None:
    import httpx

    async with httpx.AsyncClient(timeout=config.http_timeout_seconds) as client:
        await resolve_table_entity(table_fqn, config, client)
def _build_issue_curl(result: Any, owner: str, repo: str) -> str:
    payload = {
        "title": f"[DataGhost] Incident on {result.fqn} ({result.severity})",
        "body": result.report_markdown,
    }
    payload_json = json.dumps(payload)
    return (f"curl -X POST https://api.github.com/repos/{owner}/{repo}/issues "
            f"-H \"Accept: application/vnd.github+json\" "
            f"-H \"Authorization: Bearer <GITHUB_TOKEN>\" "
            f"-d '{payload_json}'")
def _with_model(config: DataGhostConfig, model: str | None) -> DataGhostConfig:
    if not model:
        return config
    return config.model_copy(update={"ollama_model": model})
@app.command()
def investigate(
    table_fqn: str = typer.Argument(..., help="Table fully qualified name"),
    depth: int = typer.Option(3, help="Lineage depth"),
    days: int = typer.Option(7, help="Lookback window in days"),
    output: str | None = typer.Option(None, help="Write report markdown to file"),
    model: str | None = typer.Option(None, help="Override Ollama model"),
    github_owner: str | None = typer.Option(None, help="GitHub org/user for issue curl"),
    github_repo: str | None = typer.Option(None, help="GitHub repo for issue curl"),
) -> None:
    config = _with_model(get_config(), model)
    print_investigation_header(table_fqn)
    try:
        asyncio.run(check_openmetadata_connection(config))
        asyncio.run(_ensure_table_exists(config, table_fqn))
    except OpenMetadataConnectionError:
        _error_panel(
            "OpenMetadata Unreachable",
            "Could not connect to OpenMetadata. Ensure OPENMETADATA_HOST is correct and "
            "the service is running.",
        )
        raise typer.Exit(code=1)
    except TableNotFoundError as error:
        suggestions = "\n".join(f"- {item}" for item in error.suggestions) or "- no similar tables found"
        _error_panel(
            "Table Not Found",
            f"{error.fqn}\n\nSimilar table names:\n{suggestions}",
        )
        raise typer.Exit(code=1)
    except RuntimeError as error:
        _error_panel("Configuration Error", str(error))
        raise typer.Exit(code=1)

    llm = OllamaClient(model=config.ollama_model, host=config.ollama_host, console=console)
    phase_text = "Planning"
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task_id = progress.add_task(phase_text, total=None)

        def on_progress(message: str) -> None:
            progress.update(task_id, description=message)
            print_reasoning_step(message)

        agent = DataGhostAgent(config=config, llm=llm, progress_callback=on_progress)
        try:
            result = asyncio.run(agent.investigate(table_fqn, depth=depth, days=days))
            progress.update(task_id, description="Done")
        except OllamaConnectionError:
            _error_panel(
                "Ollama Unreachable",
                "Could not connect to Ollama. Start it with `ollama serve` and ensure "
                "OLLAMA_HOST points to the correct URL.",
            )
            raise typer.Exit(code=1)

    print_planning_step(["Planning complete", "Evidence fetched", "Report generated"])
    print_report(result)
    if output:
        save_report(result, output)
        console.print(f"[green]Saved report:[/green] {output}")

    if Confirm.ask("Open GitHub issue?", default=False):
        if not github_owner or not github_repo:
            _error_panel(
                "Missing GitHub Target",
                "Provide --github-owner and --github-repo to generate the issue curl command.",
            )
            raise typer.Exit(code=1)
        curl_command = _build_issue_curl(result, github_owner, github_repo)
        console.print(Panel(curl_command, title="GitHub Issue curl", border_style="blue"))
@app.command()
def diff(
    table_fqn: str = typer.Argument(..., help="Table fully qualified name"),
    days: int = typer.Option(7, help="Lookback window in days"),
) -> None:
    config = get_config()
    try:
        asyncio.run(check_openmetadata_connection(config))
        asyncio.run(_ensure_table_exists(config, table_fqn))
    except OpenMetadataConnectionError:
        _error_panel(
            "OpenMetadata Unreachable",
            "Could not connect to OpenMetadata. Verify OPENMETADATA_HOST and JWT token.",
        )
        raise typer.Exit(code=1)
    except TableNotFoundError as error:
        _error_panel("Table Not Found", str(error))
        raise typer.Exit(code=1)

    result = asyncio.run(get_schema_diff(table_fqn, days=days, config=config))
    print_schema_diff_table(result)
@app.command()
def lineage(
    table_fqn: str = typer.Argument(..., help="Table fully qualified name"),
    depth: int = typer.Option(3, help="Lineage depth"),
    direction: str = typer.Option("both", help="upstream, downstream, or both"),
) -> None:
    if direction not in {"upstream", "downstream", "both"}:
        _error_panel("Invalid Direction", "direction must be one of: upstream, downstream, both")
        raise typer.Exit(code=1)

    config = get_config()
    try:
        asyncio.run(check_openmetadata_connection(config))
        asyncio.run(_ensure_table_exists(config, table_fqn))
    except OpenMetadataConnectionError:
        _error_panel("OpenMetadata Unreachable", "Cannot reach OpenMetadata service.")
        raise typer.Exit(code=1)
    except TableNotFoundError as error:
        _error_panel("Table Not Found", str(error))
        raise typer.Exit(code=1)

    result = asyncio.run(get_lineage(table_fqn, depth=depth, direction=direction, config=config))
    print_lineage_tree(result)
@app.command("mcp-server")
def mcp_server_command() -> None:
    try:
        config = get_config()
    except RuntimeError as error:
        _error_panel("Configuration Error", str(error))
        raise typer.Exit(code=1)
    run_mcp_server(config)
if __name__ == "__main__":  # pragma: no cover
    app()
