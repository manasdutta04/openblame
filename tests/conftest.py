from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import pytest
import respx
from httpx import Response

from dataghost.config import DataGhostConfig


@pytest.fixture()
def config() -> DataGhostConfig:
    return DataGhostConfig(
        openmetadata_host="http://om.local:8585",
        openmetadata_jwt_token="test-token",
        ollama_host="http://ollama.local:11434",
        ollama_model="llama3",
    )


@pytest.fixture()
def table_fqn() -> str:
    return "default.public.orders"


@pytest.fixture()
def now_millis() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)


@pytest.fixture()
def table_payload(table_fqn: str, now_millis: int) -> dict[str, Any]:
    return {
        "id": "table-1",
        "name": "orders",
        "fullyQualifiedName": table_fqn,
        "updatedBy": "alice",
        "updatedAt": now_millis,
        "owners": [{"name": "data-platform", "email": "data@example.com"}],
        "tags": [{"tagFQN": "PII.None"}],
        "description": "Orders fact table",
        "tier": {"tagFQN": "Tier.Tier2"},
        "domain": {"fullyQualifiedName": "sales"},
        "columns": [
            {"name": "order_id", "dataType": "BIGINT", "constraint": "PRIMARY_KEY"},
            {"name": "order_total", "dataType": "DECIMAL", "constraint": "NULL"},
        ],
    }


@pytest.fixture()
def lineage_payload(table_fqn: str) -> dict[str, Any]:
    return {
        "entity": {
            "id": "root",
            "fullyQualifiedName": table_fqn,
            "owners": [{"name": "data-platform"}],
            "updatedAt": 1715000000000,
        },
        "nodes": [
            {"id": "up-1", "fullyQualifiedName": "default.public.raw_orders", "owners": []},
            {"id": "down-1", "fullyQualifiedName": "default.analytics.orders_daily", "owners": []},
        ],
        "upstreamEdges": [{"fromEntity": {"id": "up-1"}, "toEntity": {"id": "root"}}],
        "downstreamEdges": [{"fromEntity": {"id": "root"}, "toEntity": {"id": "down-1"}}],
    }


@pytest.fixture()
def quality_payload(table_fqn: str, now_millis: int) -> dict[str, Any]:
    return {
        "data": [
            {
                "name": "order_total_not_null",
                "entityLink": f"<#E::table::{table_fqn}::columns::order_total>",
                "parameterValues": [{"name": "threshold", "value": "0"}],
                "testCaseResult": {
                    "timestamp": now_millis,
                    "testResultValue": "Failed",
                    "result": "3 null values",
                },
            },
            {
                "name": "order_id_unique",
                "entityLink": f"<#E::table::{table_fqn}::columns::order_id>",
                "testCaseResult": {
                    "timestamp": now_millis,
                    "testResultValue": "Success",
                },
            },
        ]
    }


@pytest.fixture()
def feed_payload(now_millis: int) -> dict[str, Any]:
    return {
        "data": [
            {
                "updatedAt": now_millis,
                "updatedBy": "bob",
                "changeDescription": {
                    "fieldsUpdated": [
                        {
                            "name": "columns",
                            "oldValue": {"name": "order_total", "dataType": "DECIMAL"},
                            "newValue": {"name": "order_total", "dataType": "STRING"},
                        }
                    ],
                    "fieldsAdded": [],
                    "fieldsDeleted": [],
                },
            }
        ]
    }


@pytest.fixture()
def mock_om_routes(
    config: DataGhostConfig,
    table_fqn: str,
    table_payload: dict[str, Any],
    lineage_payload: dict[str, Any],
    quality_payload: dict[str, Any],
    feed_payload: dict[str, Any],
) -> Any:
    encoded = quote(table_fqn, safe="")
    with respx.mock(assert_all_called=False) as mock:
        base = config.openmetadata_host
        mock.get(f"{base}/api/v1/system/version").mock(return_value=Response(200, json={"version": "1.0"}))
        mock.get(f"{base}/api/v1/tables/name/{encoded}").mock(return_value=Response(200, json=table_payload))
        mock.get(f"{base}/api/v1/lineage/table/{encoded}").mock(return_value=Response(200, json=lineage_payload))
        mock.get(f"{base}/api/v1/dataQuality/testSuites").mock(return_value=Response(200, json={"data": []}))
        mock.get(f"{base}/api/v1/dataQuality/testCases").mock(return_value=Response(200, json=quality_payload))
        mock.get(f"{base}/api/v1/feed").mock(return_value=Response(200, json=feed_payload))
        yield mock


@pytest.fixture()
def ollama_plan_response() -> str:
    return '["Fetch lineage", "Fetch quality", "Fetch schema diff"]'


@pytest.fixture()
def ollama_reason_response() -> str:
    return (
        "## Root Cause\n"
        "Type change on `order_total` from DECIMAL to STRING.\n\n"
        "## Impact\n"
        "- default.analytics.orders_daily\n\n"
        "## Evidence\n"
        "- Failed not-null check on order_total\n\n"
        "## Owner\n"
        "data-platform\n\n"
        "## Suggested Fix\n"
        "Revert schema and backfill.\n\n"
        "## Severity\n"
        "HIGH\n"
    )
