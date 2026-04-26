from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

if TYPE_CHECKING:
    from openblame.agent import AgentResult


console = Console()

SEVERITY_COLORS: dict[str, str] = {
    "CRITICAL": "bold red",
    "HIGH": "dark_orange",
    "MEDIUM": "yellow",
    "LOW": "green",
    "UNKNOWN": "dim",
}

LOGO = r"""[bold cyan]
                         _     _                      
                        | |   | |                     
   ___  _ __   ___ _ __ | |__ | | __ _ _ __ ___   ___ 
  / _ \| '_ \ / _ \ '_ \| '_ \| |/ _` | '_ ` _ \ / _ \
 | (_) | |_) |  __/ | | | |_) | | (_| | | | | | |  __/
  \___/| .__/ \___|_| |_|_.__/|_|\__,_|_| |_| |_|\___|
       | |                                            
       |_|                                            
[/bold cyan]"""


def print_branding() -> None:
    console.print(LOGO)
    console.print("[dim italic]AI-powered metadata incident investigator[/dim italic]\n")


def print_header(fqn: str) -> None:
    console.print(LOGO)
    console.print(
        Panel(
            f"[bold]Investigating[/bold] [cyan]{fqn}[/cyan]",
            subtitle="[dim]OpenMetadata lineage + quality + governance + Ollama reasoning[/dim]",
            border_style="cyan",
            padding=(0, 2),
        )
    )


def print_planning(steps: list[str]) -> None:
    if not steps:
        return
    body = "\n".join(f"  [bold cyan]{index}.[/bold cyan] {step}" for index, step in enumerate(steps, start=1))
    console.print(Panel(body, title="[blue]Agent Plan[/blue]", border_style="blue"))


def print_anomaly(message: str) -> None:
    console.print(Panel(message, title="[red]Anomaly Detected[/red]", border_style="red"))


def print_status(message: str) -> None:
    console.print(f"[dim]{message}[/dim]")


def print_briefing(result: AgentResult) -> None:
    table = Table(title="Investigation Briefing", show_header=True, header_style="bold magenta")
    table.add_column("Signal", style="cyan", no_wrap=True)
    table.add_column("Value", overflow="fold")

    owners = result.owners.get("owners") or []
    owner_text = ", ".join(_owner_label(owner) for owner in owners) if owners else "No owner assigned"
    table.add_row("Owner", owner_text)
    table.add_row("Tier", str(result.owners.get("tier") or "Not tagged"))
    table.add_row("Domain", str(result.owners.get("domain") or "Not assigned"))
    table.add_row("Tags", ", ".join(result.owners.get("tags") or []) or "No tags")
    table.add_row("Quality", _quality_summary(result))
    table.add_row("Blast Radius", _impact_summary(result))
    table.add_row("Schema Activity", _schema_summary(result))

    console.print(table)

    if result.governance_risks:
        body = "\n".join(f"  - {risk}" for risk in result.governance_risks)
        console.print(Panel(body, title="[yellow]Governance Risks[/yellow]", border_style="yellow"))

    if result.evidence:
        body = "\n".join(f"  - {item}" for item in result.evidence)
        console.print(Panel(body, title="[green]Evidence Snapshot[/green]", border_style="green"))


def print_report(result: AgentResult) -> None:
    sev_color = SEVERITY_COLORS.get(result.severity, "dim")
    sev_text = Text(f"  {result.severity}  ", style=f"bold {sev_color} on default")

    console.print()
    console.print(Panel(sev_text, title="Severity", border_style=sev_color, expand=False))

    if result.root_cause:
        console.print(
            Panel(
                result.root_cause,
                title="[bold]Most Likely Root Cause[/bold]",
                border_style="yellow",
            )
        )

    if result.affected_entities:
        impact = Table(title="Affected Entities", show_header=True, header_style="bold magenta")
        impact.add_column("Table / Entity", style="cyan")
        for entity in result.affected_entities:
            impact.add_row(entity)
        console.print(impact)

    if result.report_markdown:
        console.print(
            Panel(
                Markdown(result.report_markdown),
                title="[bold]Incident Report[/bold]",
                border_style="dim",
            )
        )


def print_lineage_tree(lineage: dict[str, Any]) -> None:
    root_label = f"[bold cyan]{lineage.get('entity', 'unknown')}[/bold cyan]"
    tree = Tree(root_label)

    upstream = lineage.get("upstream") or []
    if upstream:
        up_branch = tree.add("[dim]^ upstream[/dim]")
        for node in upstream:
            label = node.get("fqn") or node.get("display_name") or str(node)
            owner = node.get("owner")
            suffix = f" [dim]({owner})[/dim]" if owner else ""
            up_branch.add(f"[green]{label}[/green]{suffix}")

    downstream = lineage.get("downstream") or []
    if downstream:
        down_branch = tree.add("[dim]v downstream[/dim]")
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
        style = "white"
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
    content = f"# OpenBlame Incident Report\n\n**Table:** `{result.fqn}`\n**Severity:** {result.severity}\n\n"
    content += result.report_markdown
    with open(path, "w", encoding="utf-8") as file_handle:
        file_handle.write(content)


def _owner_label(owner: dict[str, Any]) -> str:
    name = str(owner.get("name") or "unknown")
    email = str(owner.get("email") or "").strip()
    return f"{name} <{email}>" if email else name


def _quality_summary(result: AgentResult) -> str:
    total = int(result.quality.get("total_tests", 0) or 0)
    failed = int(result.quality.get("failed", 0) or 0)
    passed = int(result.quality.get("passed", 0) or 0)
    if total == 0:
        return "No recent quality tests found"
    return f"{failed} failed / {passed} passed in lookback window"


def _impact_summary(result: AgentResult) -> str:
    affected = result.affected_entities
    if not affected:
        return "No downstream entities detected"
    preview = ", ".join(affected[:3])
    suffix = " ..." if len(affected) > 3 else ""
    return f"{len(affected)} downstream entities ({preview}{suffix})"


def _schema_summary(result: AgentResult) -> str:
    changes = result.schema_diff.get("changes") or []
    if not changes:
        return "No recent schema changes"
    return f"{len(changes)} schema change events detected"
