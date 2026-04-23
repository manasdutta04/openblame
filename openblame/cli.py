from __future__ import annotations

import asyncio

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn

from openblame import reporter
from openblame.agent import OpenBlameAgent
from openblame.config import get_config
from openblame.llm import OllamaClient
from openblame.tools import lineage as lineage_tool
from openblame.tools import schema_diff as diff_tool


app = typer.Typer(help="OpenBlame - AI-powered data pipeline investigator")


@app.command()
def investigate(
    table_fqn: str = typer.Argument(..., help="Fully qualified table name"),
    depth: int = typer.Option(3, help="Lineage depth"),
    days: int = typer.Option(7, help="Lookback days"),
    output: str | None = typer.Option(None, help="Save report to this path"),
    model: str | None = typer.Option(None, help="Ollama model override"),
) -> None:
    config = get_config()
    if model:
        config.ollama_model = model

    reporter.print_header(table_fqn)

    llm = OllamaClient(config.ollama_model, config.ollama_host)
    if not llm.test_connection():
        reporter.console.print("[red]Cannot connect to Ollama.[/red] Run: ollama serve")
        raise typer.Exit(1)

    agent = OpenBlameAgent(config, llm)
    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        transient=True,
    ) as progress:
        task_id = progress.add_task("Starting...", total=None)

        def on_status(message: str) -> None:
            progress.update(task_id, description=message)

        result = asyncio.run(
            agent.investigate(
                table_fqn,
                depth=depth,
                days=days,
                on_status=on_status,
            )
        )

    reporter.print_planning(result.plan_steps)
    for anomaly in result.anomalies[:3]:
        reporter.print_anomaly(anomaly)
    reporter.print_briefing(result)
    reporter.print_report(result)

    if output:
        reporter.save_report(result, output)
        reporter.console.print(f"\nReport saved -> [green]{output}[/green]")

    if result.affected_entities:
        answer = typer.confirm("\nOpen GitHub issue?", default=False)
        if answer:
            title = f"[OpenBlame] Incident: {table_fqn} - {result.severity}"
            body = result.report_markdown.replace('"', '\\"').replace("\n", "\\n")
            reporter.console.print(
                "\n[dim]curl -X POST "
                "https://api.github.com/repos/YOUR_ORG/YOUR_REPO/issues \\\n"
                '  -H "Authorization: Bearer $GITHUB_TOKEN" \\\n'
                f'  -d \'{{"title": "{title}", "body": "{body[:300]}..."}}\' [/dim]'
            )


@app.command()
def diff(
    table_fqn: str = typer.Argument(...),
    days: int = typer.Option(7),
) -> None:
    config = get_config()
    reporter.print_header(table_fqn)
    result = asyncio.run(
        diff_tool.get_schema_diff(
            table_fqn,
            days,
            config.openmetadata_host,
            config.openmetadata_jwt_token,
        )
    )
    reporter.print_schema_diff_table(result)


@app.command()
def lineage(
    table_fqn: str = typer.Argument(...),
    depth: int = typer.Option(3),
    direction: str = typer.Option("both"),
) -> None:
    config = get_config()
    reporter.print_header(table_fqn)
    result = asyncio.run(
        lineage_tool.get_lineage(
            table_fqn,
            depth,
            direction,
            config.openmetadata_host,
            config.openmetadata_jwt_token,
        )
    )
    reporter.print_lineage_tree(result)


@app.command(name="mcp-server")
def mcp_server() -> None:
    from openblame.mcp_server import run

    run()


if __name__ == "__main__":
    app()
