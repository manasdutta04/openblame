from __future__ import annotations

from urllib.parse import quote

import pytest
import respx
from httpx import Response

from dataghost.tools.common import OpenMetadataConnectionError
from dataghost.tools.lineage import get_lineage
from dataghost.tools.owners import get_owners_and_tags
from dataghost.tools.quality import get_quality_tests
from dataghost.tools.schema_diff import get_schema_diff


@pytest.mark.asyncio()
async def test_get_lineage_happy_path(config, table_fqn, lineage_payload) -> None:
    encoded = quote(table_fqn, safe="")
    with respx.mock(assert_all_called=True) as mock:
        mock.get(f"{config.openmetadata_host}/api/v1/lineage/table/{encoded}").mock(
            return_value=Response(200, json=lineage_payload)
        )
        result = await get_lineage(table_fqn, depth=3, direction="both", config=config)
    assert result["entity"]["fqn"] == table_fqn
    assert len(result["upstream"]) == 1
    assert len(result["downstream"]) == 1


@pytest.mark.asyncio()
async def test_get_lineage_empty_response(config, table_fqn) -> None:
    encoded = quote(table_fqn, safe="")
    with respx.mock(assert_all_called=True) as mock:
        mock.get(f"{config.openmetadata_host}/api/v1/lineage/table/{encoded}").mock(
            return_value=Response(404, json={"message": "not found"})
        )
        result = await get_lineage(table_fqn, depth=3, direction="both", config=config)
    assert result["upstream"] == []
    assert result["downstream"] == []


@pytest.mark.asyncio()
async def test_get_quality_tests_happy_path(config, table_fqn, quality_payload) -> None:
    with respx.mock(assert_all_called=True) as mock:
        mock.get(f"{config.openmetadata_host}/api/v1/dataQuality/testSuites").mock(
            return_value=Response(200, json={"data": []})
        )
        mock.get(f"{config.openmetadata_host}/api/v1/dataQuality/testCases").mock(
            return_value=Response(200, json=quality_payload)
        )
        result = await get_quality_tests(table_fqn, days=7, config=config)
    assert result["total_tests"] == 2
    assert result["failed"] == 1
    assert result["passed"] == 1
    assert result["failures"][0]["test_name"] == "order_total_not_null"


@pytest.mark.asyncio()
async def test_get_schema_diff_happy_path(config, table_fqn, table_payload, feed_payload) -> None:
    encoded = quote(table_fqn, safe="")
    with respx.mock(assert_all_called=True) as mock:
        mock.get(f"{config.openmetadata_host}/api/v1/tables/name/{encoded}").mock(
            return_value=Response(200, json=table_payload)
        )
        mock.get(f"{config.openmetadata_host}/api/v1/feed").mock(
            return_value=Response(200, json=feed_payload)
        )
        result = await get_schema_diff(table_fqn, days=7, config=config)
    assert len(result["current_columns"]) == 2
    assert result["changes"][0]["change_type"] == "type_changed"
    assert result["changes"][0]["column"] == "order_total"


@pytest.mark.asyncio()
async def test_get_owners_and_tags_happy_path(config, table_fqn, table_payload) -> None:
    encoded = quote(table_fqn, safe="")
    with respx.mock(assert_all_called=True) as mock:
        mock.get(f"{config.openmetadata_host}/api/v1/tables/name/{encoded}").mock(
            return_value=Response(200, json=table_payload)
        )
        result = await get_owners_and_tags(table_fqn, config)
    assert result["owners"][0]["name"] == "data-platform"
    assert "PII.None" in result["tags"]


@pytest.mark.asyncio()
async def test_tools_handle_unreachable_api(config, table_fqn, monkeypatch) -> None:
    async def raise_connect(*args, **kwargs):  # noqa: ANN002, ANN003
        raise OpenMetadataConnectionError("boom")

    monkeypatch.setattr("dataghost.tools.lineage.om_get", raise_connect)
    monkeypatch.setattr("dataghost.tools.quality.om_get", raise_connect)
    monkeypatch.setattr("dataghost.tools.schema_diff.resolve_table_entity", raise_connect)
    monkeypatch.setattr("dataghost.tools.owners.resolve_table_entity", raise_connect)
    lineage = await get_lineage(table_fqn, depth=3, direction="both", config=config)
    quality = await get_quality_tests(table_fqn, days=7, config=config)
    schema = await get_schema_diff(table_fqn, days=7, config=config)
    owners = await get_owners_and_tags(table_fqn, config=config)
    assert "_error" in lineage
    assert "_error" in quality
    assert "_error" in schema
    assert "_error" in owners
