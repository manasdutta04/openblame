from __future__ import annotations

from typing import Any

import httpx

from dataghost.config import DataGhostConfig
from dataghost.tools.common import OpenMetadataError, resolve_table_entity, to_iso


def _owners(table: dict[str, Any]) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    owners = table.get("owners") or []
    for item in owners:
        results.append(
            {
                "name": str(item.get("name") or item.get("displayName") or "unknown"),
                "email": str(item.get("email") or ""),
            }
        )
    if not results and table.get("owner"):
        owner = table.get("owner") or {}
        results.append(
            {
                "name": str(owner.get("name") or owner.get("displayName") or "unknown"),
                "email": str(owner.get("email") or ""),
            }
        )
    return results


def _tags(table: dict[str, Any]) -> list[str]:
    tags = table.get("tags") or []
    values = [str(item.get("tagFQN") or item.get("labelType") or "") for item in tags]
    return [item for item in values if item]


async def get_owners_and_tags(fqn: str, config: DataGhostConfig) -> dict[str, Any]:
    """
    Return normalized ownership, tags, and metadata for a table.
    """

    default = {
        "owners": [],
        "tags": [],
        "description": "",
        "tier": None,
        "domain": None,
        "last_updated_by": "",
        "last_updated_at": "",
    }
    try:
        async with httpx.AsyncClient(timeout=config.http_timeout_seconds) as client:
            table = await resolve_table_entity(fqn, config, client)

        return {
            "owners": _owners(table),
            "tags": _tags(table),
            "description": str(table.get("description") or ""),
            "tier": table.get("tier", {}).get("tagFQN") if table.get("tier") else None,
            "domain": table.get("domain", {}).get("fullyQualifiedName")
            if table.get("domain")
            else None,
            "last_updated_by": str(table.get("updatedBy") or ""),
            "last_updated_at": to_iso(int(table.get("updatedAt") or 0)),
        }
    except OpenMetadataError as error:
        return {**default, "_error": str(error)}
