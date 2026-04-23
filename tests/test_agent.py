from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from openblame.agent import AgentResult, OpenBlameAgent


@pytest.fixture
def mock_llm() -> MagicMock:
    llm = MagicMock()
    llm.plan.return_value = ["Check lineage", "Review quality tests"]
    llm.reason.return_value = (
        "## Severity: HIGH\n\n"
        "**Root Cause**\n"
        "Schema change in orders.discount."
    )
    return llm


@pytest.fixture
def mock_config() -> MagicMock:
    config = MagicMock()
    config.openmetadata_host = "http://localhost:8585"
    config.openmetadata_jwt_token = "test"
    return config


@pytest.mark.asyncio
async def test_investigate_returns_result(mock_config: MagicMock, mock_llm: MagicMock) -> None:
    with (
        patch("openblame.agent.owners.get_owners_and_tags", new_callable=AsyncMock) as mock_owners,
        patch("openblame.agent.schema_diff.get_schema_diff", new_callable=AsyncMock) as mock_diff,
        patch("openblame.agent.lineage.get_lineage", new_callable=AsyncMock) as mock_lineage,
        patch("openblame.agent.quality.get_quality_tests", new_callable=AsyncMock) as mock_quality,
    ):
        mock_owners.return_value = {
            "owners": [{"name": "alice"}],
            "tags": [],
            "description": "",
            "tier": None,
            "domain": None,
            "last_updated_by": "alice",
            "last_updated_at": "",
        }
        mock_diff.return_value = {"current_columns": [], "changes": []}
        mock_lineage.return_value = {
            "entity": "test.table",
            "upstream": [],
            "downstream": [{"fqn": "test.downstream"}],
        }
        mock_quality.return_value = {
            "total_tests": 5,
            "passed": 3,
            "failed": 2,
            "failures": [],
        }

        agent = OpenBlameAgent(mock_config, mock_llm)
        result = await agent.investigate("test.table", depth=2, days=7)

        assert isinstance(result, AgentResult)
        assert result.fqn == "test.table"
        assert result.severity == "HIGH"
        assert "downstream" in result.affected_entities[0]
        assert result.report_markdown != ""


@pytest.mark.asyncio
async def test_investigate_handles_tool_failures(mock_config: MagicMock, mock_llm: MagicMock) -> None:
    with (
        patch("openblame.agent.owners.get_owners_and_tags", new_callable=AsyncMock) as mock_owners,
        patch("openblame.agent.schema_diff.get_schema_diff", new_callable=AsyncMock) as mock_diff,
        patch("openblame.agent.lineage.get_lineage", new_callable=AsyncMock) as mock_lineage,
        patch("openblame.agent.quality.get_quality_tests", new_callable=AsyncMock) as mock_quality,
    ):
        mock_owners.return_value = {
            "owners": [],
            "tags": [],
            "description": "",
            "tier": None,
            "domain": None,
            "last_updated_by": "",
            "last_updated_at": "",
            "error": "timeout",
        }
        mock_diff.return_value = {"current_columns": [], "changes": [], "error": "not found"}
        mock_lineage.return_value = {
            "entity": "test.table",
            "upstream": [],
            "downstream": [],
            "error": "timeout",
        }
        mock_quality.return_value = {
            "total_tests": 0,
            "passed": 0,
            "failed": 0,
            "failures": [],
        }

        agent = OpenBlameAgent(mock_config, mock_llm)
        result = await agent.investigate("test.table")
        assert isinstance(result, AgentResult)
