from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable

from openblame.config import Settings
from openblame.llm import OllamaClient
from openblame.tools import lineage, owners, quality, schema_diff


@dataclass
class AgentResult:
    fqn: str
    lineage: dict[str, Any] = field(default_factory=dict)
    quality: dict[str, Any] = field(default_factory=dict)
    schema_diff: dict[str, Any] = field(default_factory=dict)
    owners: dict[str, Any] = field(default_factory=dict)
    plan_steps: list[str] = field(default_factory=list)
    anomalies: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    governance_risks: list[str] = field(default_factory=list)
    report_markdown: str = ""
    severity: str = "UNKNOWN"
    root_cause: str = ""
    affected_entities: list[str] = field(default_factory=list)


class OpenBlameAgent:
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
        result.plan_steps = steps
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

        result.governance_risks = _governance_risks(result.owners)
        result.anomalies = _build_anomalies(result)
        result.evidence = _build_evidence(result)

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


# Backward compatibility aliases
DataGhostResult = AgentResult
DataGhostAgent = OpenBlameAgent


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


def _governance_risks(owner_data: dict[str, Any]) -> list[str]:
    risks: list[str] = []
    if not owner_data.get("owners"):
        risks.append("No owner is assigned, so incident routing will depend on tribal knowledge.")
    if not owner_data.get("description"):
        risks.append("The table description is missing, which makes impact analysis and handoffs slower.")
    if not owner_data.get("tier"):
        risks.append("No Tier tag is set, so business criticality is not explicitly encoded.")
    if not owner_data.get("domain"):
        risks.append("No domain is assigned, so governance boundaries are harder to enforce.")
    if not owner_data.get("tags"):
        risks.append("No metadata tags are present, which weakens classification and policy automation.")
    return risks


def _build_anomalies(result: AgentResult) -> list[str]:
    anomalies: list[str] = []
    for change in (result.schema_diff.get("changes") or [])[:3]:
        column = change.get("column") or "unknown column"
        when = change.get("changed_at") or "recently"
        old_value = change.get("old_value") or "unknown"
        new_value = change.get("new_value") or "unknown"
        change_type = change.get("change_type") or "changed"
        if change_type == "type_changed":
            anomalies.append(f"Schema drift: `{column}` changed from `{old_value}` to `{new_value}` at {when}.")
        elif change_type == "added":
            anomalies.append(f"Schema drift: `{column}` was added at {when}.")
        elif change_type == "removed":
            anomalies.append(f"Schema drift: `{column}` was removed at {when}.")

    for failure in (result.quality.get("failures") or [])[:3]:
        test_name = failure.get("test_name") or "unnamed test"
        column = failure.get("column") or "table-level"
        actual = failure.get("actual") or "no result message"
        failed_at = failure.get("failed_at") or "recently"
        anomalies.append(
            f"Quality regression: `{test_name}` failed on `{column}` at {failed_at} with result: {actual}."
        )

    if result.governance_risks:
        anomalies.append(f"Governance gap: {result.governance_risks[0]}")

    return anomalies


def _build_evidence(result: AgentResult) -> list[str]:
    evidence: list[str] = []

    owners = result.owners.get("owners") or []
    if owners:
        lead = owners[0]
        owner_name = lead.get("name") or "unassigned"
        owner_email = lead.get("email") or "no email"
        evidence.append(f"Primary owner: {owner_name} ({owner_email}).")

    tags = result.owners.get("tags") or []
    if tags:
        evidence.append(f"Tags on asset: {', '.join(tags[:4])}.")

    domain = result.owners.get("domain")
    if domain:
        evidence.append(f"Domain: {domain}.")

    downstream = result.affected_entities
    if downstream:
        preview = ", ".join(downstream[:3])
        suffix = " ..." if len(downstream) > 3 else ""
        evidence.append(
            f"Downstream blast radius: {len(downstream)} entities depend on this table ({preview}{suffix})."
        )

    failures = result.quality.get("failed", 0)
    total_tests = result.quality.get("total_tests", 0)
    if total_tests:
        evidence.append(f"Quality posture: {failures} of {total_tests} recent tests are failing.")

    if result.schema_diff.get("changes"):
        evidence.append(
            f"Recent schema volatility: {len(result.schema_diff.get('changes', []))} column change events in lookback window."
        )

    evidence.extend(result.governance_risks[:2])
    return evidence



