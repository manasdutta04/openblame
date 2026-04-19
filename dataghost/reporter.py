from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

if TYPE_CHECKING:
    from dataghost.agent import AgentResult


console = Console()

SEVERITY_COLORS: dict[str, str] = {
    "CRITICAL": "bold red",
    "HIGH": "dark_orange",
    "MEDIUM": "yellow",
    "LOW": "green",
    "UNKNOWN": "dim",
}

LOGO = """\
[bold cyan]
  ██████╗  █████╗ ████████╗ █████╗  ██████╗ ██╗  ██╗ ██████╗ ███████╗████████╗
  ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔════╝ ██║  ██║██╔═══██╗██╔════╝╚══██╔══╝
  ██║  ██║███████║   ██║   ███████║██║  ███╗███████║██║   ██║███████╗   ██║
  ██║  ██║██╔══██║   ██║   ██╔══██║██║   ██║██╔══██║██║   ██║╚════██║   ██║
  ██████╔╝██║  ██║   ██║   ██║  ██║╚██████╔╝██║  ██║╚██████╔╝███████║   ██║
  ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚══════╝   ╚═╝
[/bold cyan][dim]                   AI-powered data pipeline investigator[/dim]"""


def print_header(fqn: str) -> None:
    console.print(LOGO)
    console.print(
        Panel(
            f"[bold]Investigating:[/bold] [cyan]{fqn}[/cyan]",
            border_style="cyan",
            padding=(0, 2),
        )
    )


def print_planning(steps: list[str]) -> None:
    body = "\n".join(f"  [dim]->[/dim] {step}" for step in steps)
    console.print(Panel(body, title="[blue]Investigation Plan[/blue]", border_style="blue"))


def print_anomaly(message: str) -> None:
    console.print(Panel(message, title="[red]Anomaly Detected[/red]", border_style="red"))


def print_status(message: str) -> None:
    console.print(f"[dim]{message}[/dim]")


def print_report(result: AgentResult) -> None:
    sev_color = SEVERITY_COLORS.get(result.severity, "dim")
    sev_text = Text(f"  {result.severity}  ", style=f"bold {sev_color} on default")

    console.print()
    console.print(Panel(sev_text, title="Severity", border_style=sev_color, expand=False))

    if result.root_cause:
        console.print(
            Panel(
                result.root_cause,
                title="[bold]Root Cause[/bold]",
                border_style="yellow",
            )
        )

    if result.affected_entities:
        table = Table(title="Affected Entities", show_header=True, header_style="bold magenta")
        table.add_column("Table / Entity", style="cyan")
        for entity in result.affected_entities:
            table.add_row(entity)
        console.print(table)

    if result.report_markdown:
        console.print(
            Panel(
                Markdown(result.report_markdown),
                title="[bold]Full Report[/bold]",
                border_style="dim",
            )
        )


def print_lineage_tree(lineage: dict[str, Any]) -> None:
    root_label = f"[bold cyan]{lineage.get('entity', 'unknown')}[/bold cyan]"
    tree = Tree(root_label)

    upstream = lineage.get("upstream") or []
    if upstream:
        up_branch = tree.add("[dim]↑ upstream[/dim]")
        for node in upstream:
            label = node.get("fqn") or node.get("display_name") or str(node)
            owner = node.get("owner")
            suffix = f" [dim]({owner})[/dim]" if owner else ""
            up_branch.add(f"[green]{label}[/green]{suffix}")

    downstream = lineage.get("downstream") or []
    if downstream:
        down_branch = tree.add("[dim]↓ downstream[/dim]")
        for node in downstream:
            label = node.get("fqn") or node.get("display_name") or str(node)
            owner = node.get("owner")
            suffix = f" [dim]({owner})[/dim]" if owner else ""
            down_branch.add(f"[yellow]{label}[/yellow]{suffix}")

    if not upstream and not downstream:
        tree.add("[dim]No lineage found[/dim]")

    console.print(tree)

    if lineage.get("error"):
        console.print(f"[dim red]Warning: {lineage['error']}[/dim red]")


def print_schema_diff_table(diff: dict[str, Any]) -> None:
    changes = diff.get("changes") or []

    if not changes:
        console.print(
            Panel(
                "[green]No schema changes found in the lookback window.[/green]",
                border_style="green",
            )
        )
        return

    table = Table(title="Schema Changes", show_header=True, header_style="bold magenta")
    table.add_column("Column", style="cyan")
    table.add_column("Change")
    table.add_column("Old Value", style="red")
    table.add_column("New Value", style="green")
    table.add_column("When", style="dim")
    table.add_column("By", style="dim")

    for change in changes:
        change_type = change.get("change_type", "")
        style = ""
        if change_type == "removed":
            style = "red"
        elif change_type == "added":
            style = "green"
        elif change_type == "type_changed":
            style = "yellow"

        table.add_row(
            change.get("column", ""),
            Text(change_type.replace("_", " "), style=style),
            change.get("old_value") or "-",
            change.get("new_value") or "-",
            change.get("changed_at", "")[:19],
            change.get("changed_by", ""),
        )

    console.print(table)
    if diff.get("error"):
        console.print(f"[dim red]Warning: {diff['error']}[/dim red]")


def save_report(result: AgentResult, path: str) -> None:
    content = f"# DataGhost Incident Report\n\n**Table:** `{result.fqn}`\n**Severity:** {result.severity}\n\n"
    content += result.report_markdown
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
