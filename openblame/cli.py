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


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    """OpenBlame - AI-powered data pipeline investigator"""
    if ctx.invoked_subcommand is None:
        reporter.print_branding()
        reporter.console.print(ctx.get_help())


@app.command()
def configure() -> None:
    """Interactively configure OpenBlame settings."""
    from rich.prompt import Prompt, Confirm
    from rich.table import Table
    
    reporter.print_branding()
    reporter.console.print("[bold cyan]Setup & Configuration[/bold cyan]\n")
    
    config = get_config()
    
    host = Prompt.ask("OpenMetadata Host", default=config.openmetadata_host)
    token = Prompt.ask("OpenMetadata JWT Token", password=True, default=config.openmetadata_jwt_token)
    ollama_host = Prompt.ask("Ollama Host", default=config.ollama_host)
    
    # Model Selection
    ollama_model = config.ollama_model
    try:
        llm = OllamaClient(config.get_model(), ollama_host)
        models = llm.list_models()
        if models:
            from rich.prompt import Choice
            reporter.console.print("\n[bold]Detected Ollama Models:[/bold]")
            selected = Choice(models)
            ollama_model = Prompt.ask("Select Default Model", choices=models, default=models[0])
        else:
            ollama_model = Prompt.ask("Ollama Model (manual)", default="qwen2.5:7b")
    except Exception:
        ollama_model = Prompt.ask("Ollama Model (manual entry)", default="qwen2.5:7b")

    with open(".env", "w") as f:
        f.write(f"OPENMETADATA_HOST={host}\n")
        f.write(f"OPENMETADATA_JWT_TOKEN={token}\n")
        f.write(f"OLLAMA_HOST={ollama_host}\n")
        f.write(f"OLLAMA_MODEL={ollama_model}\n")

    # Summary Table
    summary = Table(title="\nConfiguration Summary", show_header=False, border_style="green")
    summary.add_row("OpenMetadata Host", host)
    masked_token = (token[:4] + "*" * (len(token) - 4)) if len(token) > 4 else "***"
    summary.add_row("JWT Token", f"[dim]{masked_token}[/dim]")
    summary.add_row("Ollama Host", ollama_host)
    summary.add_row("Active Model", f"[bold green]{ollama_model}[/bold green]")
    
    reporter.console.print(summary)
    reporter.console.print("\n[bold green]✓ Configuration saved to .env[/bold green]")
    reporter.console.print("[dim]You are ready to investigate! Run: [bold]openblame investigate --help[/bold][/dim]")


@app.command()
def investigate(
    table_fqn: str = typer.Argument(..., help="Fully qualified table name"),
    depth: int = typer.Option(3, help="Lineage depth"),
    days: int = typer.Option(7, help="Lookback days"),
    output: str | None = typer.Option(None, help="Save report to this path"),
    model: str | None = typer.Option(None, help="Ollama model override"),
    host: str | None = typer.Option(None, "--host", help="OpenMetadata host override"),
    token: str | None = typer.Option(None, "--token", help="OpenMetadata token override"),
) -> None:
    config = get_config()
    if host: config.openmetadata_host = host
    if token: config.openmetadata_jwt_token = token
    
    if not config.openmetadata_jwt_token:
        reporter.console.print(
            "[yellow]Warning:[/yellow] OPENMETADATA_JWT_TOKEN not set.\n"
            "Run [bold]openblame configure[/bold] or set it in .env"
        )

    reporter.print_header(table_fqn)

    model = model or config.get_model()
    llm = OllamaClient(model, config.ollama_host)
    if not llm.test_connection():
        reporter.console.print(
            f"[red]Cannot connect to Ollama at {config.ollama_host}[/red]\n"
            "Run: [bold]ollama serve[/bold]\n"
            f"Then ensure a model is available: [bold]ollama list[/bold]"
        )
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


@app.command(name="list-models")
def list_models() -> None:
    """Show available Ollama models on this machine."""
    config = get_config()
    llm = OllamaClient(config.get_model(), config.ollama_host)
    
    if not llm.test_connection():
        reporter.console.print("[red]Ollama not running.[/red] Start with: ollama serve")
        raise typer.Exit(1)
    
    models = llm.list_models()
    if not models:
        reporter.console.print("[yellow]No models found.[/yellow] Run: ollama pull qwen2.5:7b")
        return
    
    from rich.table import Table
    table = Table(title="Available Ollama Models", show_header=True, header_style="bold cyan")
    table.add_column("Model", style="green")
    table.add_column("Status")
    
    detected = config.get_model()
    for m in models:
        status = "[bold green]● active[/bold green]" if m == detected else "[dim]○ available[/dim]"
        table.add_row(m, status)
    
    reporter.console.print(table)
    reporter.console.print(f"\n[dim]Run 'openblame configure' to set a default model.[/dim]")


@app.command()
def diff(
    table_fqn: str = typer.Argument(...),
    days: int = typer.Option(7),
    host: str | None = typer.Option(None, "--host"),
    token: str | None = typer.Option(None, "--token"),
) -> None:
    config = get_config()
    host = host or config.openmetadata_host
    token = token or config.openmetadata_jwt_token
    
    reporter.print_header(table_fqn)
    result = asyncio.run(
        diff_tool.get_schema_diff(
            table_fqn,
            days,
            host,
            token,
        )
    )
    reporter.print_schema_diff_table(result)


@app.command()
def lineage(
    table_fqn: str = typer.Argument(...),
    depth: int = typer.Option(3),
    direction: str = typer.Option("both"),
    host: str | None = typer.Option(None, "--host"),
    token: str | None = typer.Option(None, "--token"),
) -> None:
    config = get_config()
    host = host or config.openmetadata_host
    token = token or config.openmetadata_jwt_token

    reporter.print_header(table_fqn)
    result = asyncio.run(
        lineage_tool.get_lineage(
            table_fqn,
            depth,
            direction,
            host,
            token,
        )
    )
    reporter.print_lineage_tree(result)


@app.command(name="mcp-server")
def mcp_server() -> None:
    from openblame.mcp_server import run

    run()


if __name__ == "__main__":
    app()
