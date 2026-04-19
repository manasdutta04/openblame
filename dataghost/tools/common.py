from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import httpx

from dataghost.config import DataGhostConfig


class OpenMetadataError(RuntimeError):
    """Base error for OpenMetadata operations."""


class OpenMetadataConnectionError(OpenMetadataError):
    """Raised when OpenMetadata cannot be reached."""


@dataclass(slots=True)
class TableNotFoundError(OpenMetadataError):
    fqn: str
    suggestions: list[str]

    def __str__(self) -> str:  # pragma: no cover - trivial
        message = f"Table not found: {self.fqn}"
        if self.suggestions:
            message += f" (did you mean: {', '.join(self.suggestions[:5])})"
        return message


def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def to_iso(ts_millis: int | None) -> str:
    if not ts_millis:
        return ""
    return datetime.fromtimestamp(ts_millis / 1000, tz=UTC).isoformat()


def _base_url(config: DataGhostConfig) -> str:
    return config.openmetadata_host.rstrip("/")


def _headers(config: DataGhostConfig) -> dict[str, str]:
    return {
        **config.om_auth_header,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


async def om_get(
    client: httpx.AsyncClient,
    config: DataGhostConfig,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    allow_404: bool = False,
) -> Any:
    url = f"{_base_url(config)}{path}"
    try:
        response = await client.get(url, headers=_headers(config), params=params)
    except httpx.HTTPError as error:
        raise OpenMetadataConnectionError(
            f"Could not reach OpenMetadata at {config.openmetadata_host}"
        ) from error

    if response.status_code == 404 and allow_404:
        return None
    if response.status_code >= 400:
        raise OpenMetadataError(
            f"OpenMetadata request failed: {response.status_code} {response.text}"
        )
    if not response.text:
        return {}
    return response.json()


async def check_openmetadata_connection(config: DataGhostConfig) -> None:
    async with httpx.AsyncClient(timeout=config.http_timeout_seconds) as client:
        await om_get(client, config, "/api/v1/system/version")


def _table_to_fqn(table: dict[str, Any]) -> str:
    return str(table.get("fullyQualifiedName") or table.get("name") or "")


async def _search_table_suggestions(
    client: httpx.AsyncClient, config: DataGhostConfig, query: str
) -> list[str]:
    payload = await om_get(
        client,
        config,
        "/api/v1/tables",
        params={"limit": 50, "fields": "owners,tags"},
        allow_404=True,
    )
    items = (payload or {}).get("data", [])
    query_lower = query.lower()
    matches = [
        _table_to_fqn(item)
        for item in items
        if query_lower in _table_to_fqn(item).lower()
    ]
    return [item for item in matches if item][:5]


async def resolve_table_entity(
    fqn: str,
    config: DataGhostConfig,
    client: httpx.AsyncClient,
) -> dict[str, Any]:
    encoded = quote(fqn, safe="")
    candidates = [
        f"/api/v1/tables/name/{encoded}",
        f"/api/v1/tables/{encoded}",
    ]
    for path in candidates:
        payload = await om_get(client, config, path, allow_404=True)
        if payload:
            return payload

    simple_name = fqn.split(".")[-1] if "." in fqn else fqn
    suggestions = await _search_table_suggestions(client, config, simple_name)
    raise TableNotFoundError(fqn=fqn, suggestions=suggestions)
