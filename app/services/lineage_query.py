"""图谱血缘查询：按 field/table 顶点返回邻域内的顶点与边。"""
import logging

import anyio
from fastapi import HTTPException

from app.config import get_settings
from app.schemas import (
    LineageDirection,
    LineageEdge,
    LineageRequest,
    LineageResponse,
    LineageVertex,
)
from app.services.arango_aql import execute_aql
from app.services.lineage_aql import explore_lineage_aql

logger = logging.getLogger("metadata_pipeline.lineage_query")

_RELATION_LABELS = {
    "table_contain_field": "表包含字段",
    "table_build_table": "表构建表",
    "field_build_field": "字段构建字段",
}

_TRAVERSAL = {
    LineageDirection.BOTH: "ANY",
    LineageDirection.UPSTREAM: "INBOUND",
    LineageDirection.DOWNSTREAM: "OUTBOUND",
}


def _vertex_kind(vertex_id: str) -> str:
    return vertex_id.split("/", 1)[0] if "/" in vertex_id else "unknown"


def _relation_label(edge_collection: str) -> str:
    return _RELATION_LABELS.get(edge_collection, edge_collection)


def _fetch_lineage_rows(vertex_id: str, max_depth: int, direction: LineageDirection) -> list[dict]:
    settings = get_settings()
    traversal = _TRAVERSAL[direction]
    rows = execute_aql(
        explore_lineage_aql(traversal),
        bind_vars={
            "start_vertex": vertex_id,
            "max_depth": max_depth,
            "@table_contain_field": settings.arango_table_contain_field,
            "@table_build_table": settings.arango_table_build_table,
            "@field_build_field": settings.arango_field_build_field,
            "name_attr": settings.entity_name_attr,
            "name_cn_attr": settings.entity_name_cn_attr,
        },
        ttl=120,
    )
    return rows


def _build_response(
    vertex_id: str,
    max_depth: int,
    direction: LineageDirection,
    rows: list[dict],
) -> LineageResponse:
    if not rows:
        raise HTTPException(status_code=404, detail=f"顶点不存在: {vertex_id}")

    vertices_map: dict[str, LineageVertex] = {}
    edges_map: dict[tuple[str, str, str], LineageEdge] = {}

    for row in rows:
        vid = row["vertex_id"]
        hops = int(row.get("hops") or 0)
        existing = vertices_map.get(vid)
        if existing is None or hops < existing.hops:
            vertices_map[vid] = LineageVertex(
                id=vid,
                kind=_vertex_kind(vid),
                name_en=row.get("name_en"),
                name_cn=row.get("name_cn"),
                hops=hops,
            )

        edge_from = row.get("edge_from")
        edge_to = row.get("edge_to")
        edge_collection = row.get("edge_collection")
        if edge_from and edge_to and edge_collection:
            key = (edge_from, edge_to, edge_collection)
            if key not in edges_map:
                edges_map[key] = LineageEdge(
                    from_id=edge_from,
                    to_id=edge_to,
                    edge_collection=edge_collection,
                    relation=_relation_label(edge_collection),
                )

    vertices = sorted(vertices_map.values(), key=lambda v: (v.hops, v.id))
    edges = sorted(edges_map.values(), key=lambda e: (e.edge_collection, e.from_id, e.to_id))

    return LineageResponse(
        start_vertex=vertex_id,
        vertex_kind=_vertex_kind(vertex_id),
        direction=direction,
        max_depth=max_depth,
        vertices=vertices,
        edges=edges,
    )


async def query_lineage(request: LineageRequest) -> LineageResponse:
    settings = get_settings()
    max_depth = request.max_depth or settings.max_trace_depth
    vertex_id = request.vertex_id.strip()

    if not vertex_id or "/" not in vertex_id:
        raise HTTPException(
            status_code=400,
            detail="vertex_id 格式应为 field/xxx 或 table/xxx",
        )

    kind = _vertex_kind(vertex_id)
    if kind not in (settings.arango_field_collection, settings.arango_table_collection):
        raise HTTPException(
            status_code=400,
            detail=f"vertex_id 前缀须为 {settings.arango_field_collection}/ 或 {settings.arango_table_collection}/",
        )

    logger.info(
        "查询血缘: vertex_id=%s, direction=%s, max_depth=%s",
        vertex_id,
        request.direction.value,
        max_depth,
    )
    rows = await anyio.to_thread.run_sync(
        _fetch_lineage_rows,
        vertex_id,
        max_depth,
        request.direction,
    )
    response = _build_response(vertex_id, max_depth, request.direction, rows)
    logger.info(
        "血缘查询完成: vertex_id=%s, vertices=%s, edges=%s",
        vertex_id,
        len(response.vertices),
        len(response.edges),
    )
    return response
