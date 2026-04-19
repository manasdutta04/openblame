from __future__ import annotations

import json
import re
from typing import Any

from ollama import Client
from rich.console import Console
from rich.live import Live
from rich.text import Text


SYSTEM_PROMPT = """You are DataGhost, a data reliability expert. You have been given full metadata
about a data pipeline entity. Analyze it and produce a concise incident report with:

1. **Root Cause** — What is the most likely cause of data issues?
2. **Impact** — Which tables, dashboards, or consumers are affected?
3. **Evidence** — Specific facts from the metadata (schema changes, failed tests, lineage)
4. **Owner** — Who owns the affected asset?
5. **Suggested Fix** — Concrete steps to resolve the issue.
6. **Severity** — LOW / MEDIUM / HIGH / CRITICAL based on downstream impact count.

Be specific. Use the actual column names, table names, dates, and owners from the data.
If no issues are found, say so clearly. Write in plain English, no jargon.
Format as markdown."""


class OllamaConnectionError(RuntimeError):
    """Raised when Ollama is unavailable."""


class OllamaClient:
    def __init__(self, model: str, host: str, console: Console | None = None) -> None:
        self.model = model
        self.host = host.rstrip("/")
        self.client = Client(host=self.host)
        self.console = console or Console()

    def chat(
        self,
        messages: list[dict[str, str]],
        system: str = "",
        *,
        stream: bool = False,
    ) -> str:
        payload = list(messages)
        if system:
            payload = [{"role": "system", "content": system}, *payload]
        try:
            if not stream:
                response = self.client.chat(
                    model=self.model,
                    messages=payload,
                    stream=False,
                )
                return str((response.get("message") or {}).get("content") or "").strip()

            buffer = ""
            stream_response = self.client.chat(
                model=self.model,
                messages=payload,
                stream=True,
            )
            with Live(Text(""), console=self.console, refresh_per_second=20) as live:
                for chunk in stream_response:
                    token = str((chunk.get("message") or {}).get("content") or "")
                    if not token:
                        continue
                    buffer += token
                    live.update(Text(buffer))
            return buffer.strip()
        except Exception as error:  # pragma: no cover - depends on local ollama runtime
            raise OllamaConnectionError(
                f"Could not reach Ollama at {self.host}. Is `ollama serve` running?"
            ) from error

    def plan(self, context: str) -> list[str]:
        prompt = (
            "You are a data reliability engineer. Given this metadata context, "
            "list the exact tool calls needed to investigate a potential incident. "
            "Respond ONLY with a JSON array of step strings.\n\n"
            f"Context:\n{context}"
        )
        raw = self.chat([{"role": "user", "content": prompt}], stream=False)
        return _parse_step_array(raw)

    def reason(self, gathered_data: dict[str, Any]) -> str:
        payload = json.dumps(gathered_data, indent=2, default=str)
        return self.chat(
            [{"role": "user", "content": payload}],
            system=SYSTEM_PROMPT,
            stream=True,
        )


def _parse_step_array(raw: str) -> list[str]:
    try:
        loaded = json.loads(raw)
        if isinstance(loaded, list):
            return [str(item) for item in loaded]
    except json.JSONDecodeError:
        pass

    match = re.search(r"\[[\s\S]*\]", raw)
    if match:
        try:
            loaded = json.loads(match.group(0))
            if isinstance(loaded, list):
                return [str(item) for item in loaded]
        except json.JSONDecodeError:
            pass

    fallback = [line.strip("-* ").strip() for line in raw.splitlines() if line.strip()]
    return fallback[:5] if fallback else ["Fetch lineage", "Fetch quality", "Fetch schema diff"]
