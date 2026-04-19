from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx

from dataghost.config import DataGhostConfig
from dataghost.tools.common import OpenMetadataError, om_get


def _owner_name(entity: dict[str, Any]) -> str:
    owners = entity.get("owners") or []
    if owners and isinstance(owners, list):
        owner = owners[0]
        return str(owner.get("name") or owner.get("displayName") or "unknown")
    return "unknown"


def _node_entry(node: dict[str, Any]) -> dict[str, str]:
    return {
        "fqn": str(node.get("fullyQualifiedName") or node.get("name") or ""),
        "owner": _owner_name(node),
        "last_modified": str(node.get("updatedAt") or ""),
        "quality_status": str(node.get("qualityStatus") or "unknown"),
    }


def _parse_lineage_payload(payload: dict[str, Any]) -> dict[str, Any]:
    entity = payload.get("entity") or {}
    nodes = payload.get("nodes") or []
    id_map = {str(node.get("id")): node for node in nodes if node.get("id")}
    root_id = str(entity.get("id") or "")

    upstream: list[dict[str, str]] = []
    downstream: list[dict[str, str]] = []

    for edge in payload.get("upstreamEdges") or []:
        from_entity = edge.get("fromEntity") or {}
        to_entity = edge.get("toEntity") or {}
        if str(to_entity.get("id")) != root_id:
            continue
        node = id_map.get(str(from_entity.get("id"))) or from_entity
        upstream.append(_node_entry(node))

    for edge in payload.get("downstreamEdges") or []:
        from_entity = edge.get("fromEntity") or {}
        to_entity = edge.get("toEntity") or {}
        if str(from_entity.get("id")) != root_id:
            continue
        node = id_map.get(str(to_entity.get("id"))) or to_entity
        downstream.append(_node_entry(node))

    entity_item = {
        "fqn": str(entity.get("fullyQualifiedName") or entity.get("name") or ""),
        "owner": _owner_name(entity),
        "last_modified": str(entity.get("updatedAt") or ""),
        "quality_status": str(entity.get("qualityStatus") or "unknown"),
    }
    return {"entity": entity_item, "upstream": upstream, "downstream": downstream}


async def get_lineage(
    fqn: str,
    depth: int,
    direction: str,
    config: DataGhostConfig,
) -> dict[str, Any]:
    """
    Call OpenMetadata lineage API and return normalized upstream/downstream structure.
    """

    default = {"entity": {"fqn": fqn}, "upstream": [], "downstream": []}
    try:
        encoded = quote(fqn, safe="")
        upstream_depth = depth if direction in {"upstream", "both"} else 0
        downstream_depth = depth if direction in {"downstream", "both"} else 0
        params = {"upstreamDepth": upstream_depth, "downstreamDepth": downstream_depth}
        async with httpx.AsyncClient(timeout=config.http_timeout_seconds) as client:
            payload = await om_get(
                client,
                config,
                f"/api/v1/lineage/table/{encoded}",
                params=params,
                allow_404=True,
            )
        if not payload:
            return default
        return _parse_lineage_payload(payload)
    except OpenMetadataError as error:
        return {**default, "_error": str(error)}
