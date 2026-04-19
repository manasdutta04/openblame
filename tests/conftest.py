from __future__ import annotations

from datetime import UTC, datetime

import pytest


MOCK_HOST = "http://localhost:8585"
MOCK_TOKEN = "test-token"


@pytest.fixture
def mock_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENMETADATA_HOST", MOCK_HOST)
    monkeypatch.setenv("OPENMETADATA_JWT_TOKEN", MOCK_TOKEN)
    monkeypatch.setenv("OLLAMA_HOST", "http://localhost:11434")
    monkeypatch.setenv("OLLAMA_MODEL", "llama3")


def _now_millis() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)


MOCK_LINEAGE_RESPONSE = {
    "entity": {
        "id": "abc-123",
        "name": "orders",
        "fullyQualifiedName": "default.public.orders",
    },
    "nodes": [
        {
            "id": "def-456",
            "fullyQualifiedName": "default.public.raw_orders",
            "name": "raw_orders",
        },
        {
            "id": "ghi-789",
            "fullyQualifiedName": "default.public.revenue_summary",
            "name": "revenue_summary",
        },
    ],
    "upstreamEdges": [{"fromEntity": "def-456", "toEntity": "abc-123"}],
    "downstreamEdges": [{"fromEntity": "abc-123", "toEntity": "ghi-789"}],
}

MOCK_QUALITY_RESPONSE = {
    "data": [
        {
            "name": "orders.order_total.not_null",
            "entityLink": "<#E::table::default.public.orders::columns::order_total>",
            "testCaseResult": {
                "testCaseStatus": "Failed",
                "timestamp": _now_millis(),
                "result": "Found 42 null values",
                "testResultValue": [{"name": "nullCount", "value": "42"}],
            },
        }
    ],
    "paging": {"total": 1},
}

MOCK_TABLE_RESPONSE = {
    "id": "abc-123",
    "name": "orders",
    "fullyQualifiedName": "default.public.orders",
    "columns": [
        {"name": "order_id", "dataType": "BIGINT", "nullable": False},
        {"name": "order_total", "dataType": "DECIMAL", "nullable": True},
    ],
    "owners": [{"name": "alice", "email": "alice@company.com"}],
    "tags": [{"tagFQN": "Tier.Tier1"}, {"tagFQN": "PII.Sensitive"}],
    "description": "Main orders table",
    "updatedBy": "alice",
    "updatedAt": _now_millis(),
}

MOCK_FEED_RESPONSE = {
    "data": [
        {
            "updatedAt": _now_millis(),
            "updatedBy": "alice",
            "fieldChanges": [
                {
                    "name": "columns/order_total/dataType",
                    "oldValue": "DECIMAL",
                    "newValue": "DOUBLE",
                }
            ],
        }
    ]
}
