from __future__ import annotations

from datetime import timedelta
from typing import Any

import httpx

from dataghost.config import DataGhostConfig
from dataghost.tools.common import OpenMetadataError, now_utc, om_get, resolve_table_entity, to_iso


def _current_columns(table: dict[str, Any]) -> list[dict[str, Any]]:
    columns = table.get("columns") or []
    normalized: list[dict[str, Any]] = []
    for col in columns:
        constraints = str(col.get("constraint") or "").upper()
        normalized.append(
            {
                "name": str(col.get("name") or ""),
                "type": str(col.get("dataType") or ""),
                "nullable": constraints not in {"NOT_NULL", "PRIMARY_KEY"},
            }
        )
    return normalized


def _within_days(ts_millis: int | None, days: int) -> bool:
    if not ts_millis:
        return False
    cutoff = now_utc() - timedelta(days=days)
    return ts_millis >= int(cutoff.timestamp() * 1000)


def _parse_openmetadata_change(
    event: dict[str, Any], changed_at: str, changed_by: str
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    desc = event.get("changeDescription") or {}
    for item in desc.get("fieldsAdded") or []:
        if str(item.get("name")) != "columns":
            continue
        col = (item.get("newValue") or {}).get("name") or "unknown"
        new_type = (item.get("newValue") or {}).get("dataType")
        changes.append(
            {
                "column": str(col),
                "change_type": "added",
                "old_value": None,
                "new_value": str(new_type) if new_type else None,
                "changed_at": changed_at,
                "changed_by": changed_by,
            }
        )
    for item in desc.get("fieldsDeleted") or []:
        if str(item.get("name")) != "columns":
            continue
        col = (item.get("oldValue") or {}).get("name") or "unknown"
        old_type = (item.get("oldValue") or {}).get("dataType")
        changes.append(
            {
                "column": str(col),
                "change_type": "removed",
                "old_value": str(old_type) if old_type else None,
                "new_value": None,
                "changed_at": changed_at,
                "changed_by": changed_by,
            }
        )
    for item in desc.get("fieldsUpdated") or []:
        if str(item.get("name")) != "columns":
            continue
        old_value = item.get("oldValue") or {}
        new_value = item.get("newValue") or {}
        if old_value.get("dataType") == new_value.get("dataType"):
            continue
        changes.append(
            {
                "column": str(new_value.get("name") or old_value.get("name") or "unknown"),
                "change_type": "type_changed",
                "old_value": str(old_value.get("dataType") or ""),
                "new_value": str(new_value.get("dataType") or ""),
                "changed_at": changed_at,
                "changed_by": changed_by,
            }
        )
    return changes


async def get_schema_diff(
    fqn: str,
    days: int,
    config: DataGhostConfig,
) -> dict[str, Any]:
    """
    Fetch current table schema and parse feed events for schema changes.
    """

    default = {"current_columns": [], "changes": []}
    try:
        async with httpx.AsyncClient(timeout=config.http_timeout_seconds) as client:
            table = await resolve_table_entity(fqn, config, client)
            feed_payload = await om_get(
                client,
                config,
                "/api/v1/feed",
                params={"entityLink": fqn, "type": "EntityUpdated", "limit": 200},
                allow_404=True,
            )

        changes: list[dict[str, Any]] = []
        for thread in (feed_payload or {}).get("data") or []:
            updated_at = int(thread.get("updatedAt") or 0)
            if updated_at and not _within_days(updated_at, days):
                continue
            changes.extend(
                _parse_openmetadata_change(
                    thread,
                    changed_at=to_iso(updated_at),
                    changed_by=str(thread.get("updatedBy") or "unknown"),
                )
            )
        return {"current_columns": _current_columns(table), "changes": changes}
    except OpenMetadataError as error:
        return {**default, "_error": str(error)}
