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
from dataghost.tools import lineage, owners, quality, schema_diff


@pytest.mark.asyncio
async def test_get_lineage_success(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/lineage/table/name/default.public.orders").mock(
            return_value=httpx.Response(200, json=MOCK_LINEAGE_RESPONSE)
        )
        result = await lineage.get_lineage(
            "default.public.orders",
            3,
            "both",
            MOCK_HOST,
            MOCK_TOKEN,
        )
        assert result["entity"] == "default.public.orders"
        assert len(result["upstream"]) == 1
        assert len(result["downstream"]) == 1
        assert result["upstream"][0]["fqn"] == "default.public.raw_orders"


@pytest.mark.asyncio
async def test_get_lineage_graceful_on_error(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/lineage/table/name/default.public.orders").mock(
            return_value=httpx.Response(404)
        )
        result = await lineage.get_lineage(
            "default.public.orders",
            3,
            "both",
            MOCK_HOST,
            MOCK_TOKEN,
        )
        assert result["upstream"] == []
        assert result["downstream"] == []
        assert "error" in result


@pytest.mark.asyncio
async def test_get_quality_success(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/dataQuality/testCases").mock(
            return_value=httpx.Response(200, json=MOCK_QUALITY_RESPONSE)
        )
        result = await quality.get_quality_tests(
            "default.public.orders",
            7,
            MOCK_HOST,
            MOCK_TOKEN,
        )
        assert result["total_tests"] == 1
        assert result["failed"] == 1
        assert result["failures"][0]["column"] == "order_total"


@pytest.mark.asyncio
async def test_get_owners_success(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/tables/name/default.public.orders").mock(
            return_value=httpx.Response(200, json=MOCK_TABLE_RESPONSE)
        )
        result = await owners.get_owners_and_tags(
            "default.public.orders",
            MOCK_HOST,
            MOCK_TOKEN,
        )
        assert result["owners"][0]["name"] == "alice"
        assert "Tier.Tier1" in result["tags"]
        assert result["tier"] == "Tier.Tier1"


@pytest.mark.asyncio
async def test_get_schema_diff_success(mock_settings: None) -> None:
    with respx.mock:
        respx.get(f"{MOCK_HOST}/api/v1/tables/name/default.public.orders").mock(
            return_value=httpx.Response(200, json=MOCK_TABLE_RESPONSE)
        )
        respx.get(f"{MOCK_HOST}/api/v1/feed").mock(
            return_value=httpx.Response(200, json=MOCK_FEED_RESPONSE)
        )
        result = await schema_diff.get_schema_diff(
            "default.public.orders",
            7,
            MOCK_HOST,
            MOCK_TOKEN,
        )
        assert len(result["current_columns"]) == 2
        assert len(result["changes"]) == 1
        assert result["changes"][0]["change_type"] == "type_changed"
