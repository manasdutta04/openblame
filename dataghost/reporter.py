from __future__ import annotations

from pathlib import Path
from typing import Any

from rich import box
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.tree import Tree


console = Console()

ASCII_ART = r"""
  ██████╗  █████╗ ████████╗ █████╗  ██████╗ ██╗  ██╗ ██████╗ ███████╗████████╗
  ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔════╝ ██║  ██║██╔═══██╗██╔════╝╚══██╔══╝
  ██║  ██║███████║   ██║   ███████║██║  ███╗███████║██║   ██║███████╗   ██║
  ██║  ██║██╔══██║   ██║   ██╔══██║██║   ██║██╔══██║██║   ██║╚════██║   ██║
  ██████╔╝██║  ██║   ██║   ██║  ██║╚██████╔╝██║  ██║╚██████╔╝███████║   ██║
  ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚══════╝   ╚═╝
                     AI-powered data pipeline investigator
"""


def print_investigation_header(fqn: str) -> None:
    body = Text(ASCII_ART.strip("\n"), style="cyan")
    body.append(f"\n\nInvestigating: {fqn}", style="bold white")
    console.print(Panel(body, border_style="blue", title="DataGhost"))


def print_planning_step(steps: list[str]) -> None:
    text = "\n".join(f"- {step}" for step in steps) if steps else "- No plan generated"
    console.print(Panel(text, title="Plan", border_style="blue"))


def print_anomaly(anomaly: dict[str, Any]) -> None:
    message = "\n".join(f"{key}: {value}" for key, value in anomaly.items())
    console.print(Panel(message, title="Anomaly", border_style="red"))


def print_reasoning_step(message: str) -> None:
    console.print(f"[dim]{message}[/dim]")


def print_report(result: Any) -> None:
    severity_colors = {
        "LOW": "green",
        "MEDIUM": "yellow",
        "HIGH": "dark_orange",
        "CRITICAL": "red",
    }
    severity = str(getattr(result, "severity", "UNKNOWN")).upper()
    badge = Text(f" {severity} ", style=f"bold white on {severity_colors.get(severity, 'grey50')}")
    console.print(Panel(badge, title="Severity", border_style=severity_colors.get(severity, "white")))

    root_cause = str(getattr(result, "root_cause", "Not identified."))
    console.print(Panel(root_cause, title="Root Cause", border_style="yellow"))

    impacted = getattr(result, "affected_entities", []) or []
    impact_table = Table(title="Impact", box=box.SIMPLE_HEAVY)
    impact_table.add_column("Affected Entity")
    if impacted:
        for item in impacted:
            impact_table.add_row(str(item))
    else:
        impact_table.add_row("No downstream impact identified")
    console.print(impact_table)

    quality = getattr(result, "quality", {}) or {}
    schema = getattr(result, "schema_diff", {}) or {}
    evidence = [
        f"Quality failures: {quality.get('failed', 0)}",
        f"Schema changes: {len(schema.get('changes', []))}",
        f"Upstream entities: {len((getattr(result, 'lineage', {}) or {}).get('upstream', []))}",
        f"Downstream entities: {len((getattr(result, 'lineage', {}) or {}).get('downstream', []))}",
    ]
    console.print(Panel("\n".join(f"- {line}" for line in evidence), title="Evidence", border_style="blue"))

    owners = (getattr(result, "owners", {}) or {}).get("owners", [])
    owner_line = ", ".join(
        f"{item.get('name')} <{item.get('email')}>".strip() for item in owners
    ) or "Unknown owner"
    console.print(Panel(owner_line, title="Owner", border_style="magenta"))

    markdown = str(getattr(result, "report_markdown", "")).strip()
    if "```" in markdown:
        console.print(Panel(Syntax(markdown, "markdown"), title="Suggested Fix / Report"))
    else:
        console.print(Panel(Markdown(markdown or "_No report generated_"), title="Report"))


def save_report(result: Any, path: str) -> None:
    Path(path).write_text(str(getattr(result, "report_markdown", "")), encoding="utf-8")


def print_lineage_tree(lineage: dict[str, Any]) -> None:
    entity = lineage.get("entity", {})
    root_label = (
        f"{entity.get('fqn', 'unknown')} "
        f"(owner={entity.get('owner', 'unknown')}, quality={entity.get('quality_status', 'unknown')})"
    )
    root = Tree(root_label)

    upstream_branch = root.add("upstream")
    for node in lineage.get("upstream", []):
        upstream_branch.add(
            f"{node.get('fqn')} (owner={node.get('owner')}, q={node.get('quality_status', 'unknown')})"
        )

    downstream_branch = root.add("downstream")
    for node in lineage.get("downstream", []):
        downstream_branch.add(
            f"{node.get('fqn')} (owner={node.get('owner')}, q={node.get('quality_status', 'unknown')})"
        )
    console.print(root)


def print_schema_diff_table(diff: dict[str, Any]) -> None:
    table = Table(title="Schema Changes", box=box.SIMPLE_HEAVY)
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
        table.add_row("-", "no changes detected", "-", "-", "-", "-")
    console.print(table)
