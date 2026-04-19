from __future__ import annotations

import json
from typing import Any

import ollama
from rich.live import Live
from rich.text import Text


class OllamaClient:
    def __init__(self, model: str, host: str):
        self.model = model
        self.client = ollama.Client(host=host)

    def chat(
        self,
        messages: list[dict[str, Any]],
        system: str = "",
        stream: bool = False,
    ) -> str:
        full_messages: list[dict[str, Any]] = []
        if system:
            full_messages.append({"role": "system", "content": system})
        full_messages.extend(messages)

        if stream:
            collected = ""
            response = self.client.chat(
                model=self.model,
                messages=full_messages,
                stream=True,
            )
            with Live(Text(""), refresh_per_second=20) as live:
                for chunk in response:
                    token = str((chunk.get("message") or {}).get("content") or "")
                    if token:
                        collected += token
                        live.update(Text(collected))
            return collected

        response = self.client.chat(model=self.model, messages=full_messages)
        return str((response.get("message") or {}).get("content") or "")

    def plan(self, table_fqn: str, context_summary: str) -> list[str]:
        prompt = f"""You are a data reliability engineer investigating a potential incident.
Table: {table_fqn}
Context: {context_summary}

List the investigation steps needed. Respond ONLY with a valid JSON array of short strings.
Example: ["Check upstream lineage for schema changes", "Review quality test failures in last 7 days"]
No explanation, no markdown, just the JSON array."""

        result = self.chat([{"role": "user", "content": prompt}])
        cleaned = result.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`")
            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:].strip()
        try:
            steps = json.loads(cleaned)
            if isinstance(steps, list):
                return [str(step) for step in steps]
        except json.JSONDecodeError:
            return [
                "Investigate metadata anomalies",
                "Check lineage",
                "Review quality tests",
            ]
        return ["Investigate metadata anomalies"]

    def reason(self, gathered_data: dict[str, Any]) -> str:
        system = """You are DataGhost, a data reliability expert. Analyze the provided metadata and produce a concise incident report with these sections:

**Root Cause** - Most likely cause of data issues (be specific, use actual names)
**Impact** - Which tables, dashboards, or consumers are affected and how many
**Evidence** - Specific facts: column names, type changes, test failure counts, dates
**Owner** - Who owns the affected asset (name and email if available)
**Suggested Fix** - Concrete numbered steps to resolve
**Severity** - ONE of: LOW / MEDIUM / HIGH / CRITICAL

Rules:
- Use actual values from the data, never say "N/A" or "unknown" if data is present
- If no issues found, say clearly: "No anomalies detected."
- Write in plain English
- Format as markdown
- Start with the severity line: `## Severity: HIGH`"""

        content = (
            "Investigate this table and its metadata:\n\n```json\n"
            f"{json.dumps(gathered_data, indent=2, default=str)}\n```"
        )
        return self.chat([{"role": "user", "content": content}], system=system)

    def test_connection(self) -> bool:
        try:
            self.client.list()
            return True
        except Exception:
            return False
