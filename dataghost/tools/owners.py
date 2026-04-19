from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import httpx


def _iso_from_millis(value: int | None) -> str:
    if not value:
        return ""
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()


def _extract_owners(table: dict[str, Any]) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    owners = table.get("owners") or []
    if isinstance(owners, list):
        for owner in owners:
            rows.append(
                {
                    "name": str(owner.get("name") or owner.get("displayName") or ""),
                    "email": owner.get("email"),
                }
            )
    if not rows and isinstance(table.get("owner"), dict):
        owner = table.get("owner") or {}
        rows.append(
            {
                "name": str(owner.get("name") or owner.get("displayName") or ""),
                "email": owner.get("email"),
            }
        )
    return rows


async def get_owners_and_tags(
    fqn: str,
    host: str,
    token: str,
) -> dict[str, Any]:
    default = {
        "owners": [],
        "tags": [],
        "description": "",
        "tier": None,
        "domain": None,
        "last_updated_by": "",
        "last_updated_at": "",
    }

    encoded_fqn = quote(fqn, safe="")
    url = f"{host.rstrip('/')}/api/v1/tables/name/{encoded_fqn}"
    params = {"fields": "owners,tags,domain,extension"}
    headers = {"Authorization": f"Bearer {token}"}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            table = response.json()

        tags = [str(tag.get("tagFQN") or "") for tag in table.get("tags") or []]
        tags = [tag for tag in tags if tag]
        tier = next((tag for tag in tags if tag.startswith("Tier")), None)
        domain = None
        if isinstance(table.get("domain"), dict):
            domain = str(
                table["domain"].get("fullyQualifiedName")
                or table["domain"].get("name")
                or ""
            ) or None

        return {
            "owners": _extract_owners(table),
            "tags": tags,
            "description": str(table.get("description") or ""),
            "tier": tier,
            "domain": domain,
            "last_updated_by": str(table.get("updatedBy") or ""),
            "last_updated_at": _iso_from_millis(int(table.get("updatedAt") or 0)),
        }
    except Exception as error:
        return {**default, "error": str(error)}
