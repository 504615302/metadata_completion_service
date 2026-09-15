"""
从 SQL 抽取字段级关系数组：fdd（血缘）/ join（JOIN ON）/ fdr（WHERE）。
仅返回结果，不落库。
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import HTTPException

from app.clients.llm import extract_sql_field_relations_with_llm
from app.schemas import SqlFieldRelationItem, SqlFieldRelationRequest, SqlFieldRelType

logger = logging.getLogger(__name__)

_REL_TYPE_MAP = {
    "fdd": SqlFieldRelType.FDD,
    "join": SqlFieldRelType.JOIN,
    "fdr": SqlFieldRelType.FDR,
}


def _nz(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _split_schema_table(
    schema_name: Any,
    table_name: Any,
) -> tuple[Optional[str], Optional[str]]:
    """拆出 schema / table；若 table 写成 schema.table 则再拆一次。"""
    schema = _nz(schema_name)
    table = _nz(table_name)
    if not table:
        return schema, None
    if "." in table and schema is None:
        left, right = table.rsplit(".", 1)
        left, right = left.strip(), right.strip()
        if left and right:
            return left, right
    if schema and table.lower().startswith(schema.lower() + "."):
        table = table[len(schema) + 1 :].strip() or table
    return schema, table


def _normalize_relations(data: dict) -> list[SqlFieldRelationItem]:
    raw_list = data.get("relations") or data.get("items") or []
    if not isinstance(raw_list, list):
        return []

    out: list[SqlFieldRelationItem] = []
    seen: set[tuple] = set()

    for raw in raw_list:
        if not isinstance(raw, dict):
            continue
        rel_raw = str(raw.get("rel_type") or raw.get("relation") or "").strip().lower()
        rel_type = _REL_TYPE_MAP.get(rel_raw)
        if rel_type is None:
            continue

        src_schema, src_table = _split_schema_table(
            raw.get("source_schema_name") or raw.get("source_schema"),
            raw.get("source_table_name") or raw.get("source_table"),
        )
        tgt_schema, tgt_table = _split_schema_table(
            raw.get("target_schema_name") or raw.get("target_schema"),
            raw.get("target_table_name") or raw.get("target_table"),
        )
        src_field = _nz(raw.get("source_field_name") or raw.get("source_field"))
        tgt_field = _nz(raw.get("target_field_name") or raw.get("target_field"))
        exp_fragment = _nz(
            raw.get("exp_fragment")
            or raw.get("sql_fragment")
            or raw.get("expression")
            or raw.get("expr")
        )

        if not src_table or not tgt_table or not src_field or not tgt_field:
            continue
        if src_field in ("*", ".*") or tgt_field in ("*", ".*"):
            continue

        key = (
            (src_schema or "").lower(),
            src_table.lower(),
            src_field.lower(),
            (tgt_schema or "").lower(),
            tgt_table.lower(),
            tgt_field.lower(),
            rel_type.value,
            (exp_fragment or "").lower(),
        )
        if key in seen:
            continue
        seen.add(key)

        out.append(
            SqlFieldRelationItem(
                source_schema_name=src_schema,
                source_table_name=src_table,
                source_field_name=src_field,
                target_schema_name=tgt_schema,
                target_table_name=tgt_table,
                target_field_name=tgt_field,
                rel_type=rel_type,
                exp_fragment=exp_fragment,
            )
        )

    return out


async def extract_sql_field_relations(
    request: SqlFieldRelationRequest,
) -> list[SqlFieldRelationItem]:
    sql = (request.sql or "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="sql 不能为空")

    try:
        data = await extract_sql_field_relations_with_llm(
            sql,
            app_name=request.workspace_name,
        )
    except Exception as exc:
        logger.exception("SQL 字段关系抽取失败")
        raise HTTPException(status_code=500, detail=f"字段关系抽取失败: {exc}") from exc

    if data.get("parse_ok") is False:
        err = _nz(data.get("parse_error")) or "SQL 无法解析"
        raise HTTPException(status_code=422, detail=err)

    relations = _normalize_relations(data)
    logger.info(
        "SQL 字段关系抽取完成: count=%s (fdd=%s join=%s fdr=%s)",
        len(relations),
        sum(1 for r in relations if r.rel_type == SqlFieldRelType.FDD),
        sum(1 for r in relations if r.rel_type == SqlFieldRelType.JOIN),
        sum(1 for r in relations if r.rel_type == SqlFieldRelType.FDR),
    )
    return relations
