"""
未限定列（如 SELECT name）在多表 JOIN 时，用 ArangoDB 字段归属消歧 source_table。
"""
from __future__ import annotations

import logging
from typing import Optional

import anyio

from app.config import get_settings
from app.schemas import InsertFieldLineage, InsertFieldRelation, InsertSqlParseItem
from app.services.arango_aql import execute_aql
from app.services.lineage_aql import tables_owning_field_aql

logger = logging.getLogger(__name__)


def _owners_sync(field_name: str, candidates: list[str]) -> list[dict]:
    if not field_name or not candidates:
        return []
    settings = get_settings()
    return execute_aql(
        tables_owning_field_aql(),
        bind_vars={
            "@table_collection": settings.arango_table_collection,
            "@table_contain_field": settings.arango_table_contain_field,
            "candidates": candidates,
            "field_name": field_name,
            "name_attr": settings.entity_name_attr,
            "name_cn_attr": settings.entity_name_cn_attr,
            "schema_attr": settings.table_schema_attr,
        },
        ttl=60,
    )


def _field_basename(col_sql: str) -> str:
    parts = [p for p in (col_sql or "").split(".") if p]
    return parts[-1] if parts else (col_sql or "")


def _needs_disambiguate(field: InsertFieldRelation, candidate_tables: list[str]) -> bool:
    if len(candidate_tables) <= 1:
        return False
    # 已唯一确定
    if len(field.source_tables) == 1:
        return False
    # 来源列都已带表前缀且解析唯一时，source_tables 应已是精确值；多表或空则需消歧
    return len(field.source_tables) != 1


async def resolve_source_tables_from_graph(
    item: InsertSqlParseItem,
    *,
    candidate_tables: Optional[list[str]] = None,
) -> InsertSqlParseItem:
    """
    对 source_table 不唯一的字段，用图谱「表包含字段」精确归属。
    仅当某字段名只属于候选表中的一张表时写入；多表同名则保持不猜测。
    """
    if not item.parse_ok:
        return item

    # 候选：优先用表级血缘中的源表；否则用字段上挂过的表
    tables = list(candidate_tables or [])
    if not tables:
        seen: set[str] = set()
        for rel in item.table_relations:
            if rel.source_table and rel.source_table not in seen:
                seen.add(rel.source_table)
                tables.append(rel.source_table)
    if len(tables) <= 1:
        return item

    cache: dict[str, list[dict]] = {}

    async def owners(field_name: str) -> list[dict]:
        key = field_name.lower()
        if key not in cache:
            cache[key] = await anyio.to_thread.run_sync(_owners_sync, field_name, tables)
        return cache[key]

    new_fields: list[InsertFieldRelation] = []
    for field in item.fields:
        if not _needs_disambiguate(field, tables):
            new_fields.append(field)
            continue

        # 从 source_columns 取未限定的字段名；若为空用 field_name
        names: list[str] = []
        for sc in field.source_columns or []:
            # 已带表前缀的不需要消歧整字段，但可能是表达式多列
            if "." in sc:
                continue
            names.append(_field_basename(sc))
        if not names:
            # 无明确源列时，用目标字段名在候选表中查找（常见 SELECT name 映射）
            names = [field.field_name]

        resolved: list[str] = []
        name_cn = field.name_cn
        for n in names:
            rows = await owners(n)
            refs = [r["table_ref"] for r in rows if r.get("table_ref")]
            uniq = list(dict.fromkeys(refs))
            if len(uniq) == 1:
                if uniq[0] not in resolved:
                    resolved.append(uniq[0])
                if not name_cn:
                    for r in rows:
                        if r.get("field_name_cn"):
                            name_cn = r["field_name_cn"]
                            break
            elif len(uniq) > 1:
                logger.info(
                    "字段归属不唯一，不猜测: field=%s candidates=%s",
                    n,
                    uniq,
                )

        if len(resolved) == 1:
            src = resolved[0]
            # 补全 source_columns 前缀
            new_cols: list[str] = []
            for sc in field.source_columns or [field.field_name]:
                if "." in sc:
                    new_cols.append(sc)
                else:
                    new_cols.append(f"{src}.{_field_basename(sc)}")
            if not new_cols:
                new_cols = [f"{src}.{field.field_name}"]
            field = field.model_copy(
                update={
                    "source_tables": [src],
                    "source_columns": new_cols,
                    "source_expr": field.source_expr or new_cols[0],
                    "name_cn": name_cn or field.name_cn,
                }
            )
            logger.info(
                "图谱消歧 source_table: field=%s -> %s",
                field.field_name,
                src,
            )
        elif not field.source_tables and resolved:
            # 多个源列分别落到不同表：保留多个（精确列表，不是全 JOIN 表）
            field = field.model_copy(update={"source_tables": resolved, "name_cn": name_cn or field.name_cn})

        new_fields.append(field)

    # 同步修正 field_relations 中 source_table 为空的边
    new_lineage: list[InsertFieldLineage] = []
    field_src_map = {f.field_name: f.source_tables for f in new_fields}
    for rel in item.field_relations:
        if rel.source_table:
            new_lineage.append(rel)
            continue
        tables_for = field_src_map.get(rel.target_field) or []
        if len(tables_for) == 1:
            new_lineage.append(
                rel.model_copy(update={"source_table": tables_for[0]})
            )
        else:
            # 按 source_field 再查一次
            rows = await owners(rel.source_field)
            refs = list(dict.fromkeys(r["table_ref"] for r in rows if r.get("table_ref")))
            if len(refs) == 1:
                new_lineage.append(rel.model_copy(update={"source_table": refs[0]}))
            else:
                new_lineage.append(rel)

    # 若 lineage 缺失但 fields 已消歧，补边
    lineage_keys = {
        (r.source_table, r.source_field, r.target_table, r.target_field)
        for r in new_lineage
    }
    for f in new_fields:
        if len(f.source_tables) != 1:
            continue
        src = f.source_tables[0]
        for sc in f.source_columns or []:
            src_field = _field_basename(sc)
            key = (src, src_field, f.target_table, f.field_name)
            if key in lineage_keys:
                continue
            new_lineage.append(
                InsertFieldLineage(
                    source_table=src,
                    source_field=src_field,
                    target_table=f.target_table,
                    target_field=f.field_name,
                    source_expr=f.source_expr,
                    relation="insert_select",
                )
            )
            lineage_keys.add(key)

    item.fields = new_fields
    item.field_relations = new_lineage
    return item
