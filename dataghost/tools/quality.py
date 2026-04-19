from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx


def _iso_from_millis(value: int | None) -> str:
    if not value:
        return ""
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()


def _column_from_entity_link(entity_link: str) -> str:
    marker = "::columns::"
    if marker not in entity_link:
        return "table-level"
    return entity_link.split(marker, 1)[-1].rstrip(">")


def _expected_from_result(test_result: dict[str, Any]) -> str:
    values = test_result.get("testResultValue")
    if isinstance(values, list) and values:
        first = values[0]
        if isinstance(first, dict):
            return str(first.get("value") or "")
        return str(first)
    return ""


async def get_quality_tests(
    fqn: str,
    days: int,
    host: str,
    token: str,
) -> dict[str, Any]:
    default = {"total_tests": 0, "passed": 0, "failed": 0, "failures": []}
    entity_link = f"<#E::table::{fqn}>"
    url = f"{host.rstrip('/')}/api/v1/dataQuality/testCases"
    params = {"entityLink": entity_link, "limit": 50}
    headers = {"Authorization": f"Bearer {token}"}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            payload = response.json()

        cutoff = datetime.now(tz=UTC) - timedelta(days=days)
        relevant_cases: list[dict[str, Any]] = []
        for item in payload.get("data") or []:
            result = item.get("testCaseResult") or {}
            timestamp = result.get("timestamp")
            if not timestamp:
                continue
            test_time = datetime.fromtimestamp(int(timestamp) / 1000, tz=UTC)
            if test_time >= cutoff:
                relevant_cases.append(item)

        passed = 0
        failed = 0
        failures: list[dict[str, str]] = []
        for case in relevant_cases:
            result = case.get("testCaseResult") or {}
            status = str(result.get("testCaseStatus") or "").upper()
            if status in {"SUCCESS", "PASSED", "PASS"}:
                passed += 1
            elif status in {"FAILED", "FAIL"}:
                failed += 1
                failures.append(
                    {
                        "test_name": str(case.get("name") or ""),
                        "column": _column_from_entity_link(str(case.get("entityLink") or "")),
                        "failed_at": _iso_from_millis(int(result.get("timestamp") or 0)),
                        "expected": _expected_from_result(result),
                        "actual": str(result.get("result") or ""),
                    }
                )

        return {
            "total_tests": len(relevant_cases),
            "passed": passed,
            "failed": failed,
            "failures": failures,
        }
    except Exception as error:
        return {**default, "error": str(error)}
