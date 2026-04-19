from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable

from dataghost.config import Settings
from dataghost.llm import OllamaClient
from dataghost.tools import lineage, owners, quality, schema_diff


@dataclass
class AgentResult:
    fqn: str
    lineage: dict[str, Any] = field(default_factory=dict)
    quality: dict[str, Any] = field(default_factory=dict)
    schema_diff: dict[str, Any] = field(default_factory=dict)
    owners: dict[str, Any] = field(default_factory=dict)
    report_markdown: str = ""
    severity: str = "UNKNOWN"
    root_cause: str = ""
    affected_entities: list[str] = field(default_factory=list)


class DataGhostAgent:
    def __init__(self, config: Settings, llm: OllamaClient):
        self.config = config
        self.llm = llm

    async def investigate(
        self,
        fqn: str,
        depth: int = 3,
        days: int = 7,
        on_status: Callable[[str], None] | None = None,
    ) -> AgentResult:
        result = AgentResult(fqn=fqn)

        def emit(message: str) -> None:
            if on_status:
                on_status(message)

        emit("Fetching baseline metadata...")
        owner_data, diff_data = await asyncio.gather(
            owners.get_owners_and_tags(
                fqn,
                self.config.openmetadata_host,
                self.config.openmetadata_jwt_token,
            ),
            schema_diff.get_schema_diff(
                fqn,
                days,
                self.config.openmetadata_host,
                self.config.openmetadata_jwt_token,
            ),
            return_exceptions=True,
        )
        result.owners = _safe_dict(owner_data, "owners fetch failed")
        result.schema_diff = _safe_dict(diff_data, "schema diff fetch failed")

        emit("Planning investigation...")
        context = (
            f"Table: {fqn}. Owners: {result.owners.get('owners')}. "
            f"Schema changes: {len(result.schema_diff.get('changes', []))}."
        )
        try:
            steps = self.llm.plan(fqn, context)
        except Exception:
            steps = ["Investigate metadata anomalies", "Check lineage", "Review quality tests"]
        emit(f"Plan: {len(steps)} steps")

        emit("Fetching lineage and quality data...")
        lineage_data, quality_data = await asyncio.gather(
            lineage.get_lineage(
                fqn,
                depth,
                "both",
                self.config.openmetadata_host,
                self.config.openmetadata_jwt_token,
            ),
            quality.get_quality_tests(
                fqn,
                days,
                self.config.openmetadata_host,
                self.config.openmetadata_jwt_token,
            ),
            return_exceptions=True,
        )
        result.lineage = _safe_dict(lineage_data, "lineage fetch failed", {"entity": fqn})
        result.quality = _safe_dict(
            quality_data,
            "quality fetch failed",
            {"total_tests": 0, "passed": 0, "failed": 0, "failures": []},
        )

        emit("Reasoning with Ollama...")
        gathered = {
            "table_fqn": fqn,
            "owners": result.owners,
            "lineage": result.lineage,
            "quality": result.quality,
            "schema_diff": result.schema_diff,
            "plan_steps": steps,
        }
        try:
            report = self.llm.reason(gathered)
        except Exception as error:
            report = f"## Severity: UNKNOWN\n\n**Root Cause**\nLLM reasoning failed: {error}"
        result.report_markdown = report

        for line in report.splitlines():
            upper_line = line.upper()
            for severity in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
                if severity in upper_line:
                    result.severity = severity
                    break
            if result.severity != "UNKNOWN":
                break

        downstream = result.lineage.get("downstream", [])
        if isinstance(downstream, list):
            result.affected_entities = [str(node.get("fqn", "")) for node in downstream if node.get("fqn")]

        lines = report.splitlines()
        for index, line in enumerate(lines):
            if "root cause" in line.lower():
                for candidate in lines[index + 1 :]:
                    cleaned = candidate.strip().lstrip("- ").lstrip("* ")
                    if cleaned:
                        result.root_cause = cleaned
                        break
                if result.root_cause:
                    break

        return result


def _safe_dict(
    value: dict[str, Any] | Exception,
    fallback_error: str,
    template: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    base = template.copy() if template else {}
    if isinstance(value, Exception):
        base["error"] = str(value)
    else:
        base["error"] = fallback_error
    return base
