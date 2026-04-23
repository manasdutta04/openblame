from __future__ import annotations

import httpx
import pytest
import respx

from tests.conftest import (
    MOCK_FEED_RESPONSE,
    MOCK_HOST,
    MOCK_LINEAGE_RESPONSE,
    MOCK_QUALITY_RESPONSE,
    MOCK_TABLE_RESPONSE,
    MOCK_TOKEN,
)
from openblame.tools import lineage, owners, quality, schema_diff


@pytest.mark.asyncio
async def test_get_lineage_success(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/lineage/table/name/default.public.orders").mock(
            return_value=httpx.Response(200, json=MOCK_LINEAGE_RESPONSE)
        )
        result = await lineage.get_lineage("default.public.orders", 3, "both", MOCK_HOST, MOCK_TOKEN)
        assert result["entity"] == "default.public.orders"
        assert len(result["upstream"]) == 1
        assert len(result["downstream"]) == 1
        assert result["upstream"][0]["fqn"] == "default.public.raw_orders"
        assert result["downstream"][0]["fqn"] == "default.public.revenue_summary"


@pytest.mark.asyncio
async def test_get_lineage_upstream_only(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/lineage/table/name/default.public.orders").mock(
            return_value=httpx.Response(200, json=MOCK_LINEAGE_RESPONSE)
        )
        result = await lineage.get_lineage("default.public.orders", 3, "upstream", MOCK_HOST, MOCK_TOKEN)
        assert len(result["upstream"]) == 1
        assert result["downstream"] == []


@pytest.mark.asyncio
async def test_get_lineage_404_graceful(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/lineage/table/name/default.public.orders").mock(
            return_value=httpx.Response(404)
        )
        result = await lineage.get_lineage("default.public.orders", 3, "both", MOCK_HOST, MOCK_TOKEN)
        assert result["upstream"] == []
        assert result["downstream"] == []
        assert "error" in result


@pytest.mark.asyncio
async def test_get_lineage_network_error_graceful(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/lineage/table/name/default.public.orders").mock(
            side_effect=httpx.ConnectError("refused")
        )
        result = await lineage.get_lineage("default.public.orders", 3, "both", MOCK_HOST, MOCK_TOKEN)
        assert "error" in result


@pytest.mark.asyncio
async def test_get_quality_tests_success(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/dataQuality/testCases").mock(
            return_value=httpx.Response(200, json=MOCK_QUALITY_RESPONSE)
        )
        result = await quality.get_quality_tests("default.public.orders", 7, MOCK_HOST, MOCK_TOKEN)
        assert result["total_tests"] == 1
        assert result["failed"] == 1
        assert result["passed"] == 0
        assert result["failures"][0]["column"] == "order_total"
        assert result["failures"][0]["test_name"] == "orders.order_total.not_null"


@pytest.mark.asyncio
async def test_get_quality_tests_empty(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/dataQuality/testCases").mock(
            return_value=httpx.Response(200, json={"data": [], "paging": {"total": 0}})
        )
        result = await quality.get_quality_tests("default.public.orders", 7, MOCK_HOST, MOCK_TOKEN)
        assert result["total_tests"] == 0
        assert result["failures"] == []


@pytest.mark.asyncio
async def test_get_quality_tests_error_graceful(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/dataQuality/testCases").mock(
            side_effect=httpx.ConnectError("refused")
        )
        result = await quality.get_quality_tests("default.public.orders", 7, MOCK_HOST, MOCK_TOKEN)
        assert result["total_tests"] == 0
        assert "error" in result


@pytest.mark.asyncio
async def test_get_owners_success(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/tables/name/default.public.orders").mock(
            return_value=httpx.Response(200, json=MOCK_TABLE_RESPONSE)
        )
        result = await owners.get_owners_and_tags("default.public.orders", MOCK_HOST, MOCK_TOKEN)
        assert result["owners"][0]["name"] == "alice"
        assert result["owners"][0]["email"] == "alice@company.com"
        assert "Tier.Tier1" in result["tags"]
        assert result["tier"] == "Tier.Tier1"
        assert result["description"] == "Main orders table"


@pytest.mark.asyncio
async def test_get_owners_error_graceful(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/tables/name/default.public.orders").mock(
            return_value=httpx.Response(403)
        )
        result = await owners.get_owners_and_tags("default.public.orders", MOCK_HOST, MOCK_TOKEN)
        assert result["owners"] == []
        assert "error" in result


@pytest.mark.asyncio
async def test_get_schema_diff_success(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/tables/name/default.public.orders").mock(
            return_value=httpx.Response(200, json=MOCK_TABLE_RESPONSE)
        )
        respx.get(f"{MOCK_HOST}/api/v1/feed").mock(
            return_value=httpx.Response(200, json=MOCK_FEED_RESPONSE)
        )
        result = await schema_diff.get_schema_diff("default.public.orders", 7, MOCK_HOST, MOCK_TOKEN)
        assert len(result["current_columns"]) == 2
        assert len(result["changes"]) == 1
        change = result["changes"][0]
        assert change["column"] == "order_total"
        assert change["change_type"] == "type_changed"
        assert change["old_value"] == "DECIMAL"
        assert change["new_value"] == "DOUBLE"


@pytest.mark.asyncio
async def test_get_schema_diff_no_changes(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/tables/name/default.public.orders").mock(
            return_value=httpx.Response(200, json=MOCK_TABLE_RESPONSE)
        )
        respx.get(f"{MOCK_HOST}/api/v1/feed").mock(
            return_value=httpx.Response(200, json={"data": [], "paging": {"total": 0}})
        )
        result = await schema_diff.get_schema_diff("default.public.orders", 7, MOCK_HOST, MOCK_TOKEN)
        assert result["changes"] == []
        assert len(result["current_columns"]) == 2
