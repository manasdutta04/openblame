from __future__ import annotations

import asyncio
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Callable

from dataghost.config import DataGhostConfig
from dataghost.llm import OllamaClient
from dataghost.tools.lineage import get_lineage
from dataghost.tools.owners import get_owners_and_tags
from dataghost.tools.quality import get_quality_tests
from dataghost.tools.schema_diff import get_schema_diff


ProgressCallback = Callable[[str], None]


@dataclass(slots=True)
class AgentResult:
    fqn: str
    lineage: dict[str, Any]
    quality: dict[str, Any]
    schema_diff: dict[str, Any]
    owners: dict[str, Any]
    report_markdown: str
    severity: str
    root_cause: str
    affected_entities: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class DataGhostAgent:
    def __init__(
        self,
        config: DataGhostConfig,
        llm: OllamaClient,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        self.config = config
        self.llm = llm
        self.progress_callback = progress_callback

    def _emit(self, message: str) -> None:
        if self.progress_callback:
            self.progress_callback(message)

    async def investigate(self, fqn: str, depth: int, days: int) -> AgentResult:
        self._emit("Planning: collecting baseline metadata")
        owners_task = asyncio.create_task(get_owners_and_tags(fqn, self.config))
        baseline_schema_task = asyncio.create_task(get_schema_diff(fqn, days, self.config))
        owners, baseline_schema = await asyncio.gather(
            owners_task,
            baseline_schema_task,
            return_exceptions=True,
        )

        owners_data = owners if isinstance(owners, dict) else _owners_default(str(owners))
        baseline_data = (
            baseline_schema
            if isinstance(baseline_schema, dict)
            else _schema_default(str(baseline_schema))
        )

        planning_context = json.dumps(
            {
                "fqn": fqn,
                "owners": owners_data,
                "current_columns": baseline_data.get("current_columns", []),
            },
            default=str,
        )
        self._emit("Planning: asking LLM for tool steps")
        try:
            steps = self.llm.plan(planning_context)
        except Exception as error:
            steps = [f"LLM planning unavailable: {error}"]

        self._emit("Fetching: running lineage, quality, and schema tools")
        lineage_task = asyncio.create_task(get_lineage(fqn, depth, "both", self.config))
        quality_task = asyncio.create_task(get_quality_tests(fqn, days, self.config))
        schema_task = asyncio.create_task(get_schema_diff(fqn, days, self.config))
        lineage, quality, schema_diff = await asyncio.gather(
            lineage_task,
            quality_task,
            schema_task,
            return_exceptions=True,
        )

        lineage_data = lineage if isinstance(lineage, dict) else _lineage_default(fqn, str(lineage))
        quality_data = quality if isinstance(quality, dict) else _quality_default(str(quality))
        schema_data = schema_diff if isinstance(schema_diff, dict) else _schema_default(str(schema_diff))
        if not schema_data.get("current_columns"):
            schema_data["current_columns"] = baseline_data.get("current_columns", [])

        gathered_data = {
            "fqn": fqn,
            "steps": steps,
            "lineage": lineage_data,
            "quality": quality_data,
            "schema_diff": schema_data,
            "owners": owners_data,
        }

        self._emit("Reasoning: generating incident report")
        try:
            report_markdown = self.llm.reason(gathered_data)
        except Exception as error:
            report_markdown = (
                "## Root Cause\n"
                "Reasoning model unavailable.\n\n"
                f"## Evidence\n- LLM error: {error}\n"
            )

        severity = _extract_field(report_markdown, "Severity") or "UNKNOWN"
        root_cause = _extract_field(report_markdown, "Root Cause") or "Not identified."
        affected_entities = _affected_entities(lineage_data)

        self._emit("Done: investigation complete")
        return AgentResult(
            fqn=fqn,
            lineage=lineage_data,
            quality=quality_data,
            schema_diff=schema_data,
            owners=owners_data,
            report_markdown=report_markdown,
            severity=severity.upper(),
            root_cause=root_cause,
            affected_entities=affected_entities,
        )


def _extract_field(markdown: str, field: str) -> str | None:
    pattern = rf"(?im)^\s*\d+\.\s*\*?\*?{re.escape(field)}\*?\*?\s*[—:-]\s*(.+)$"
    match = re.search(pattern, markdown)
    if match:
        return match.group(1).strip().strip("*")
    header_pattern = rf"(?ims)^#+\s*{re.escape(field)}\s*\n(.+?)(?:\n#+\s|\Z)"
    header = re.search(header_pattern, markdown)
    if header:
        return header.group(1).strip().splitlines()[0]
    return None


def _affected_entities(lineage: dict[str, Any]) -> list[str]:
    values = [item.get("fqn") for item in lineage.get("upstream", []) + lineage.get("downstream", [])]
    unique = sorted({str(item) for item in values if item})
    return unique


def _lineage_default(fqn: str, error: str) -> dict[str, Any]:
    return {"entity": {"fqn": fqn}, "upstream": [], "downstream": [], "_error": error}


def _quality_default(error: str) -> dict[str, Any]:
    return {"total_tests": 0, "passed": 0, "failed": 0, "failures": [], "_error": error}


def _schema_default(error: str) -> dict[str, Any]:
    return {"current_columns": [], "changes": [], "_error": error}


def _owners_default(error: str) -> dict[str, Any]:
    return {
        "owners": [],
        "tags": [],
        "description": "",
        "tier": None,
        "domain": None,
        "last_updated_by": "",
        "last_updated_at": "",
        "_error": error,
    }
