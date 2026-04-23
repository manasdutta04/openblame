from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx


def _edge_entity_id(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("id") or "")
    if isinstance(value, str):
        return value
    return str(value or "")


def _owner_name(node: dict[str, Any]) -> str | None:
    owner = node.get("owner")
    if isinstance(owner, dict):
        return str(owner.get("name") or owner.get("displayName") or "") or None
    owners = node.get("owners") or []
    if isinstance(owners, list) and owners:
        first = owners[0]
        if isinstance(first, dict):
            return str(first.get("name") or first.get("displayName") or "") or None
    return None


def _node_payload(node: dict[str, Any]) -> dict[str, Any]:
    return {
        "fqn": str(node.get("fullyQualifiedName") or node.get("name") or ""),
        "display_name": str(node.get("displayName") or node.get("name") or ""),
        "type": str(node.get("type") or "table"),
        "owner": _owner_name(node),
    }


async def get_lineage(
    fqn: str,
    depth: int,
    direction: str,
    host: str,
    token: str,
) -> dict[str, Any]:
    encoded = quote(fqn, safe="")
    url = f"{host.rstrip('/')}/api/v1/lineage/table/name/{encoded}"
    params = {
        "upstreamDepth": depth if direction in {"upstream", "both"} else 0,
        "downstreamDepth": depth if direction in {"downstream", "both"} else 0,
    }
    headers = {"Authorization": f"Bearer {token}"}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

        entity = data.get("entity") or {}
        entity_id = str(entity.get("id") or "")
        nodes = data.get("nodes") or []
        node_by_id = {str(node.get("id")): node for node in nodes if node.get("id")}

        upstream_ids: list[str] = []
        for edge in data.get("upstreamEdges") or []:
            if _edge_entity_id(edge.get("toEntity")) == entity_id:
                from_id = _edge_entity_id(edge.get("fromEntity"))
                if from_id:
                    upstream_ids.append(from_id)

        downstream_ids: list[str] = []
        for edge in data.get("downstreamEdges") or []:
            if _edge_entity_id(edge.get("fromEntity")) == entity_id:
                to_id = _edge_entity_id(edge.get("toEntity"))
                if to_id:
                    downstream_ids.append(to_id)

        upstream = [
            _node_payload(node_by_id[node_id])
            for node_id in upstream_ids
            if node_id in node_by_id
        ]
        downstream = [
            _node_payload(node_by_id[node_id])
            for node_id in downstream_ids
            if node_id in node_by_id
        ]

        if direction == "upstream":
            downstream = []
        elif direction == "downstream":
            upstream = []

        entity_fqn = str(entity.get("fullyQualifiedName") or entity.get("name") or fqn)
        return {
            "entity": entity_fqn,
            "upstream": upstream,
            "downstream": downstream,
        }
    except Exception as error:
        return {"entity": fqn, "upstream": [], "downstream": [], "error": str(error)}
