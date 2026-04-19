from __future__ import annotations

from datetime import timedelta
from typing import Any

import httpx

from dataghost.config import DataGhostConfig
from dataghost.tools.common import OpenMetadataError, now_utc, om_get, to_iso


def _is_within_days(ts_millis: int | None, days: int) -> bool:
    if not ts_millis:
        return False
    cutoff = now_utc() - timedelta(days=days)
    return ts_millis >= int(cutoff.timestamp() * 1000)


def _extract_status(result: dict[str, Any]) -> str:
    raw = (
        result.get("testResultValue")
        or result.get("testCaseStatus")
        or result.get("status")
        or "unknown"
    )
    return str(raw).lower()


def _extract_expected(case: dict[str, Any]) -> str:
    parameter_values = case.get("parameterValues") or []
    if parameter_values and isinstance(parameter_values, list):
        return ", ".join(str(item.get("value")) for item in parameter_values if item)
    return ""


async def get_quality_tests(
    fqn: str,
    days: int,
    config: DataGhostConfig,
) -> dict[str, Any]:
    """
    Return quality pass/fail summary and failed checks for a table within N days.
    """

    default = {"total_tests": 0, "passed": 0, "failed": 0, "failures": []}
    try:
        async with httpx.AsyncClient(timeout=config.http_timeout_seconds) as client:
            await om_get(
                client,
                config,
                "/api/v1/dataQuality/testSuites",
                params={"limit": 500},
                allow_404=True,
            )
            cases_payload = await om_get(
                client,
                config,
                "/api/v1/dataQuality/testCases",
                params={"limit": 1000},
                allow_404=True,
            )
        test_cases = (cases_payload or {}).get("data") or []
        relevant = []
        for case in test_cases:
            entity_link = str(case.get("entityLink") or "")
            if fqn not in entity_link:
                continue
            result = case.get("testCaseResult") or {}
            timestamp = result.get("timestamp") or case.get("updatedAt")
            if timestamp and not _is_within_days(int(timestamp), days):
                continue
            relevant.append(case)

        passed = 0
        failed = 0
        failures: list[dict[str, str]] = []
        for case in relevant:
            result = case.get("testCaseResult") or {}
            status = _extract_status(result)
            if status in {"success", "passed", "pass"}:
                passed += 1
            elif status in {"failed", "fail"}:
                failed += 1
                failures.append(
                    {
                        "test_name": str(case.get("name") or ""),
                        "column": str(case.get("entityLink") or "").split("::")[-1],
                        "failed_at": to_iso(int(result.get("timestamp") or 0)),
                        "expected": _extract_expected(case),
                        "actual": str(
                            result.get("result")
                            or result.get("observedValue")
                            or "unknown"
                        ),
                    }
                )
        return {
            "total_tests": len(relevant),
            "passed": passed,
            "failed": failed,
            "failures": failures,
        }
    except OpenMetadataError as error:
        return {**default, "_error": str(error)}
