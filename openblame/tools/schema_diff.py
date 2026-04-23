from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import httpx


def _iso_from_millis(value: int | None) -> str:
    if not value:
        return ""
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()


def _current_columns(table: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for column in table.get("columns") or []:
        nullable = bool(column.get("nullable", True))
        if "constraint" in column:
            constraint = str(column.get("constraint") or "").upper()
            if constraint in {"NOT_NULL", "PRIMARY_KEY"}:
                nullable = False
        rows.append(
            {
                "name": str(column.get("name") or ""),
                "dataType": str(column.get("dataType") or ""),
                "nullable": nullable,
            }
        )
    return rows


def _type_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        if value.get("dataType") is not None:
            return str(value.get("dataType"))
        if value.get("type") is not None:
            return str(value.get("type"))
        return str(value)
    return str(value)


def _column_from_name(field_name: str) -> str:
    if field_name.startswith("columns/"):
        parts = field_name.split("/")
        if len(parts) >= 2 and parts[1]:
            return parts[1]
    return "unknown"


def _parse_field_changes(
    event: dict[str, Any],
    changed_at: str,
    changed_by: str,
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []

    for field_change in event.get("fieldChanges") or []:
        name = str(field_change.get("name") or "")
        if not name.startswith("columns/"):
            continue

        old_value = _type_value(field_change.get("oldValue"))
        new_value = _type_value(field_change.get("newValue"))
        if old_value is None and new_value is not None:
            change_type = "added"
        elif old_value is not None and new_value is None:
            change_type = "removed"
        else:
            change_type = "type_changed"

        changes.append(
            {
                "column": _column_from_name(name),
                "change_type": change_type,
                "old_value": old_value,
                "new_value": new_value,
                "changed_at": changed_at,
                "changed_by": changed_by,
            }
        )

    change_description = event.get("changeDescription") or {}
    for item in change_description.get("fieldsAdded") or []:
        raw_name = str(item.get("name") or "")
        if raw_name not in {"columns"} and not raw_name.startswith("columns/"):
            continue
        new_value = item.get("newValue") or {}
        changes.append(
            {
                "column": str(new_value.get("name") or _column_from_name(raw_name)),
                "change_type": "added",
                "old_value": None,
                "new_value": _type_value(new_value),
                "changed_at": changed_at,
                "changed_by": changed_by,
            }
        )
    for item in change_description.get("fieldsDeleted") or []:
        raw_name = str(item.get("name") or "")
        if raw_name not in {"columns"} and not raw_name.startswith("columns/"):
            continue
        old_value = item.get("oldValue") or {}
        changes.append(
            {
                "column": str(old_value.get("name") or _column_from_name(raw_name)),
                "change_type": "removed",
                "old_value": _type_value(old_value),
                "new_value": None,
                "changed_at": changed_at,
                "changed_by": changed_by,
            }
        )
    for item in change_description.get("fieldsUpdated") or []:
        raw_name = str(item.get("name") or "")
        if raw_name not in {"columns"} and not raw_name.startswith("columns/"):
            continue
        old_value_obj = item.get("oldValue") or {}
        new_value_obj = item.get("newValue") or {}
        old_value = _type_value(old_value_obj)
        new_value = _type_value(new_value_obj)
        if old_value == new_value:
            continue
        changes.append(
            {
                "column": str(
                    new_value_obj.get("name")
                    or old_value_obj.get("name")
                    or _column_from_name(raw_name)
                ),
                "change_type": "type_changed",
                "old_value": old_value,
                "new_value": new_value,
                "changed_at": changed_at,
                "changed_by": changed_by,
            }
        )

    return changes


async def get_schema_diff(
    fqn: str,
    days: int,
    host: str,
    token: str,
) -> dict[str, Any]:
    default = {"current_columns": [], "changes": []}
    encoded_fqn = quote(fqn, safe="")
    table_url = f"{host.rstrip('/')}/api/v1/tables/name/{encoded_fqn}"
    feed_url = f"{host.rstrip('/')}/api/v1/feed"
    params = {
        "entityLink": f"<#E::table::{fqn}>",
        "type": "EntityUpdated",
        "limit": 25,
    }
    headers = {"Authorization": f"Bearer {token}"}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            table_response = await client.get(table_url, headers=headers)
            table_response.raise_for_status()
            table_data = table_response.json()

            feed_response = await client.get(feed_url, params=params, headers=headers)
            feed_response.raise_for_status()
            feed_data = feed_response.json()

        cutoff = datetime.now(tz=UTC) - timedelta(days=days)
        changes: list[dict[str, Any]] = []
        for event in feed_data.get("data") or []:
            timestamp = int(event.get("updatedAt") or 0)
            if timestamp:
                updated_dt = datetime.fromtimestamp(timestamp / 1000, tz=UTC)
                if updated_dt < cutoff:
                    continue
            changed_at = _iso_from_millis(timestamp)
            changed_by = str(event.get("updatedBy") or "unknown")
            changes.extend(_parse_field_changes(event, changed_at, changed_by))

        return {"current_columns": _current_columns(table_data), "changes": changes}
    except Exception as error:
        return {**default, "error": str(error)}
