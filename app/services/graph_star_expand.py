"""
SQL 中 SELECT * / t.* 时，从 ArangoDB 拉取对应表字段并展开字段级血缘。
"""
from __future__ import annotations

import logging
from typing import Optional

import anyio

from app.config import get_settings
from app.schemas import InsertFieldLineage, InsertFieldRelation, InsertSqlParseItem
from app.services.arango_aql import execute_aql
from app.services.lineage_aql import fields_of_table_aql

logger = logging.getLogger(__name__)


def _split_table_ref(table_ref: str) -> tuple[Optional[str], str]:
    """'ods.user' -> ('ods', 'user')；'user' -> (None, 'user')。"""
    parts = [p for p in (table_ref or "").split(".") if p]
    if not parts:
        return None, ""
    if len(parts) == 1:
        return None, parts[0]
    return parts[-2], parts[-1]


def _fetch_fields_sync(table_ref: str, prefer_schema: Optional[str] = None) -> list[dict]:
    settings = get_settings()
    schema_from_ref, table_name = _split_table_ref(table_ref)
    if not table_name:
        return []

    prefer = prefer_schema if not schema_from_ref else schema_from_ref
    return execute_aql(
        fields_of_table_aql(),
        bind_vars={
            "@table_collection": settings.arango_table_collection,
            "@table_contain_field": settings.arango_table_contain_field,
            "table_name": table_name,
            "schema_name": schema_from_ref or "",
            "prefer_schema": prefer or "",
            "name_attr": settings.entity_name_attr,
            "name_cn_attr": settings.entity_name_cn_attr,
            "schema_attr": settings.table_schema_attr,
        },
        ttl=120,
    )


def _merge_star_fields(
    item: InsertSqlParseItem,
    source_table: str,
    graph_fields: list[dict],
) -> InsertSqlParseItem:
    if not graph_fields:
        return item

    existing_names = {f.field_name for f in item.fields}
    pending = list(item.pending_target_columns)
    target_table = item.target_table

    src_fields: list[tuple[str, Optional[str]]] = []
    for row in graph_fields:
        name = row.get("field_name")
        if name:
            src_fields.append((str(name), row.get("field_name_cn")))

    if not item.table_name_cn:
        for row in graph_fields:
            if row.get("table_name_cn"):
                item.table_name_cn = row["table_name_cn"]
                break

    pairs: list[tuple[str, str, Optional[str]]] = []
    if pending:
        src_by_name = {n.lower(): (n, cn) for n, cn in src_fields}
        used_src: set[str] = set()
        unmatched: list[str] = []
        for tgt_col in pending:
            hit = src_by_name.get(tgt_col.lower())
            if hit and hit[0] not in used_src:
                pairs.append((tgt_col, hit[0], hit[1]))
                used_src.add(hit[0])
            else:
                unmatched.append(tgt_col)
        unused = [(n, cn) for n, cn in src_fields if n not in used_src]
        for i, tgt_col in enumerate(unmatched):
            if i < len(unused):
                n, cn = unused[i]
                pairs.append((tgt_col, n, cn))
    else:
        for n, cn in src_fields:
            pairs.append((n, n, cn))

    new_fields = list(item.fields)
    new_lineage = list(item.field_relations)
    lineage_keys = {
        (r.source_table, r.source_field, r.target_table, r.target_field, r.relation)
        for r in new_lineage
    }

    for tgt_field, src_field, name_cn in pairs:
        if tgt_field not in existing_names:
            new_fields.append(
                InsertFieldRelation(
                    target_table=target_table,
                    field_name=tgt_field,
                    name_cn=name_cn,
                    source_expr=f"{source_table}.{src_field}",
                    source_columns=[f"{source_table}.{src_field}"],
                    source_tables=[source_table],
                )
            )
            existing_names.add(tgt_field)
        else:
            for f in new_fields:
                if f.field_name != tgt_field:
                    continue
                if not f.name_cn and name_cn:
                    f.name_cn = name_cn
                if not f.source_columns:
                    f.source_columns = [f"{source_table}.{src_field}"]
                if source_table not in f.source_tables:
                    f.source_tables.append(source_table)
                if not f.source_expr:
                    f.source_expr = f"{source_table}.{src_field}"
                break

        key = (source_table, src_field, target_table, tgt_field, "insert_select")
        if key not in lineage_keys:
            new_lineage.append(
                InsertFieldLineage(
                    source_table=source_table,
                    source_field=src_field,
                    target_table=target_table,
                    target_field=tgt_field,
                    source_expr=f"{source_table}.{src_field}",
                    relation="insert_select",
                )
            )
            lineage_keys.add(key)

    item.fields = new_fields
    item.field_relations = new_lineage
    item.pending_target_columns = []
    return item


async def expand_star_fields_from_graph(
    item: InsertSqlParseItem,
    *,
    schema_name: Optional[str] = None,
) -> InsertSqlParseItem:
    """含 star_source_tables 时查 ArangoDB 展开字段；未命中不失败。"""
    if not item.parse_ok or not item.star_source_tables or item.star_expanded:
        return item

    prefer_schema = schema_name
    if not prefer_schema and item.target_table:
        prefer_schema, _ = _split_table_ref(item.target_table)

    expanded_any = False
    for src in item.star_source_tables:
        rows = await anyio.to_thread.run_sync(_fetch_fields_sync, src, prefer_schema)
        if not rows:
            logger.warning("ArangoDB 未找到表字段，无法展开 *: table=%s", src)
            continue
        logger.info("展开 SELECT *: source_table=%s, fields=%s", src, len(rows))
        item = _merge_star_fields(item, src, rows)
        expanded_any = True

    item.star_expanded = expanded_any
    return item
