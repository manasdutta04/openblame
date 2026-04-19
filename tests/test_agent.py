from __future__ import annotations

import pytest

from dataghost.agent import AgentResult, DataGhostAgent


class FakeLLM:
    def __init__(self, plan_output: list[str], reason_output: str) -> None:
        self._plan_output = plan_output
        self._reason_output = reason_output

    def plan(self, context: str) -> list[str]:
        assert "fqn" in context
        return self._plan_output

    def reason(self, gathered_data: dict[str, object]) -> str:
        assert "lineage" in gathered_data
        return self._reason_output


@pytest.mark.asyncio()
async def test_agent_full_loop_returns_required_fields(
    config,
    mock_om_routes,
    ollama_plan_response,
    ollama_reason_response,
    table_fqn,
) -> None:
    llm = FakeLLM(
        plan_output=["Fetch lineage", "Fetch quality", "Fetch schema diff"],
        reason_output=ollama_reason_response,
    )
    agent = DataGhostAgent(config=config, llm=llm)
    result = await agent.investigate(table_fqn, depth=3, days=7)

    assert isinstance(result, AgentResult)
    assert result.fqn == table_fqn
    assert isinstance(result.lineage, dict)
    assert isinstance(result.quality, dict)
    assert isinstance(result.schema_diff, dict)
    assert isinstance(result.owners, dict)
    assert isinstance(result.report_markdown, str)
    assert isinstance(result.affected_entities, list)
    assert result.severity == "HIGH"


@pytest.mark.asyncio()
async def test_agent_continues_on_partial_tool_failure(
    config,
    mock_om_routes,
    table_fqn,
    monkeypatch,
) -> None:
    async def broken_quality(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("quality endpoint timed out")

    monkeypatch.setattr("dataghost.agent.get_quality_tests", broken_quality)
    llm = FakeLLM(
        plan_output=["Fetch lineage", "Fetch quality", "Fetch schema diff"],
        reason_output="## Root Cause\nUnknown\n\n## Severity\nMEDIUM\n",
    )
    agent = DataGhostAgent(config=config, llm=llm)
    result = await agent.investigate(table_fqn, depth=2, days=3)
    assert result.report_markdown
    assert result.quality.get("_error")
    assert result.severity == "MEDIUM"


@pytest.mark.asyncio()
async def test_agent_severity_fallback_unknown(
    config,
    mock_om_routes,
    table_fqn,
) -> None:
    llm = FakeLLM(
        plan_output=["Fetch lineage"],
        reason_output="## Root Cause\nNo issue detected.\n",
    )
    agent = DataGhostAgent(config=config, llm=llm)
    result = await agent.investigate(table_fqn, depth=1, days=1)
    assert result.severity == "UNKNOWN"
