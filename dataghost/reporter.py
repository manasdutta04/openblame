from __future__ import annotations

from pathlib import Path
from typing import Any

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.tree import Tree


console = Console()

SEVERITY_COLORS = {
    "CRITICAL": "red",
    "HIGH": "dark_orange",
    "MEDIUM": "yellow",
    "LOW": "green",
    "UNKNOWN": "dim",
}

LOGO = """[bold cyan]
  ██████╗  █████╗ ████████╗ █████╗  ██████╗ ██╗  ██╗ ██████╗ ███████╗████████╗
  ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔════╝ ██║  ██║██╔═══██╗██╔════╝╚══██╔══╝
  ██║  ██║███████║   ██║   ███████║██║  ███╗███████║██║   ██║███████╗   ██║
  ██║  ██║██╔══██║   ██║   ██╔══██║██║   ██║██╔══██║██║   ██║╚════██║   ██║
  ██████╔╝██║  ██║   ██║   ██║  ██║╚██████╔╝██║  ██║╚██████╔╝███████║   ██║
  ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚══════╝   ╚═╝
[/bold cyan][dim]              AI-powered data pipeline investigator[/dim]"""


def print_header(fqn: str) -> None:
    body = Text.from_markup(LOGO)
    body.append(f"\n\nInvestigating: {fqn}\n", style="bold white")
    console.print(Panel(body, border_style="blue", title="DataGhost"))


def print_planning(steps: list[str]) -> None:
    lines = "\n".join(f"- {step}" for step in steps) if steps else "- No plan generated"
    console.print(Panel(lines, title="Plan", border_style="blue"))


def print_anomaly(message: str) -> None:
    console.print(Panel(message, title="Anomaly", border_style="red"))


def print_status(message: str) -> None:
    console.print(f"[dim]{message}[/dim]")


def print_report(result: Any) -> None:
    severity = str(getattr(result, "severity", "UNKNOWN")).upper()
    color = SEVERITY_COLORS.get(severity, "dim")
    badge = Text(f" {severity} ", style=f"bold white on {color}")
    console.print(Panel(badge, title="Severity", border_style=color))
    console.print(Markdown(getattr(result, "report_markdown", "") or "_No report generated_"))


def print_lineage_tree(lineage: dict[str, Any]) -> None:
    entity = lineage.get("entity", "unknown")
    if isinstance(entity, dict):
        root_label = str(entity.get("fqn") or entity.get("fullyQualifiedName") or "unknown")
    else:
        root_label = str(entity)

    tree = Tree(root_label)
    upstream_branch = tree.add("upstream")
    for node in lineage.get("upstream", []):
        upstream_branch.add(str(node.get("fqn") or node.get("display_name") or "unknown"))

    downstream_branch = tree.add("downstream")
    for node in lineage.get("downstream", []):
        downstream_branch.add(str(node.get("fqn") or node.get("display_name") or "unknown"))
    console.print(tree)


def print_schema_diff_table(diff: dict[str, Any]) -> None:
    table = Table(title="Schema Changes")
    table.add_column("column")
    table.add_column("change")
    table.add_column("old")
    table.add_column("new")
    table.add_column("when")
    table.add_column("who")
    for change in diff.get("changes", []):
        table.add_row(
            str(change.get("column") or ""),
            str(change.get("change_type") or ""),
            str(change.get("old_value") or ""),
            str(change.get("new_value") or ""),
            str(change.get("changed_at") or ""),
            str(change.get("changed_by") or ""),
        )
    if not diff.get("changes"):
        table.add_row("-", "No schema changes detected", "-", "-", "-", "-")
    console.print(table)


def save_report(result: Any, path: str) -> None:
    Path(path).write_text(str(getattr(result, "report_markdown", "")), encoding="utf-8")
