"""
用大模型解析 INSERT SQL：目标表/字段、源表、字段级血缘，以及注释/别名中的中文名。
若含 SELECT * / t.* 且大模型返回了 star_source_tables，可再从 ArangoDB 展开字段。
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from app.clients.llm import parse_insert_sql_with_llm
from app.schemas import (
    InsertFieldLineage,
    InsertFieldRelation,
    InsertParseSource,
    InsertSqlParseItem,
    InsertTableRelation,
)

logger = logging.getLogger(__name__)


def _item_from_llm_dict(data: dict, sql: str) -> InsertSqlParseItem:
    """把 LLM JSON 规范成 InsertSqlParseItem。"""
    fields_raw = data.get("fields") or []
    relations_raw = data.get("table_relations") or []
    field_lineage_raw = data.get("field_relations") or []
    fields: list[InsertFieldRelation] = []
    for f in fields_raw:
        if not isinstance(f, dict) or not f.get("field_name"):
            continue
        fname = str(f["field_name"]).strip()
        if fname in ("*", ".*") or fname.endswith(".*"):
            continue
        fields.append(
            InsertFieldRelation(
                target_table=f.get("target_table") or data.get("target_table"),
                field_name=fname,
                name_cn=f.get("name_cn"),
                source_expr=f.get("source_expr"),
                source_columns=[
                    str(x) for x in (f.get("source_columns") or []) if str(x) not in ("*", ".*")
                ],
                source_tables=[str(x) for x in (f.get("source_tables") or [])],
            )
        )
    relations: list[InsertTableRelation] = []
    for r in relations_raw:
        if not isinstance(r, dict) or not r.get("source_table") or not r.get("target_table"):
            continue
        relations.append(
            InsertTableRelation(
                source_table=str(r["source_table"]),
                target_table=str(r["target_table"]),
                relation=str(r.get("relation") or "insert_select"),
            )
        )

    field_relations: list[InsertFieldLineage] = []
    for r in field_lineage_raw:
        if not isinstance(r, dict) or not r.get("source_field") or not r.get("target_field"):
            continue
        sf = str(r["source_field"]).strip()
        tf = str(r["target_field"]).strip()
        if sf in ("*", ".*") or tf in ("*", ".*"):
            continue
        field_relations.append(
            InsertFieldLineage(
                source_table=r.get("source_table"),
                source_field=sf,
                target_table=r.get("target_table") or data.get("target_table"),
                target_field=tf,
                source_expr=r.get("source_expr"),
                relation=str(r.get("relation") or "insert_select"),
            )
        )

    if not field_relations:
        for f in fields:
            for sc in f.source_columns:
                parts = sc.split(".")
                if len(parts) >= 2:
                    src_field = parts[-1]
                    src_table = ".".join(parts[:-1])
                else:
                    src_field = sc
                    src_table = f.source_tables[0] if len(f.source_tables) == 1 else None
                if src_field in ("*", ".*"):
                    continue
                field_relations.append(
                    InsertFieldLineage(
                        source_table=src_table,
                        source_field=src_field,
                        target_table=f.target_table,
                        target_field=f.field_name,
                        source_expr=f.source_expr,
                        relation="insert_select",
                    )
                )

    # star_source_tables / pending_target_columns：只保留大模型返回值
    star_source_tables = [str(x).strip() for x in (data.get("star_source_tables") or []) if str(x).strip()]
    pending_target_columns = [
        str(x).strip() for x in (data.get("pending_target_columns") or []) if str(x).strip()
    ]

    parse_ok = bool(data.get("parse_ok", True)) and bool(
        data.get("target_table") or fields or star_source_tables
    )
    llm_err = data.get("parse_error")
    if not parse_ok and not llm_err:
        llm_err = "大模型未能解析出有效的表/字段信息"

    return InsertSqlParseItem(
        parse_ok=parse_ok,
        parse_error=f"LLM: {llm_err}" if llm_err and not parse_ok else None,
        parse_source=InsertParseSource.LLM if parse_ok else None,
        target_table=data.get("target_table"),
        table_name_cn=data.get("table_name_cn"),
        fields=fields,
        table_relations=relations,
        field_relations=field_relations,
        star_source_tables=star_source_tables,
        pending_target_columns=pending_target_columns,
        raw_sql=sql,
    )


async def parse_insert_sql(
    sql: str,
    *,
    schema_name: Optional[str] = None,
    app_name: Optional[str] = None,
    expand_star_from_graph: bool = True,
) -> InsertSqlParseItem:
    """
    用大模型解析 INSERT SQL 血缘。
    若大模型返回 star_source_tables，可再从 ArangoDB 展开对应表字段。
    """
    from app.services.graph_star_expand import expand_star_fields_from_graph

    text = (sql or "").strip()
    if not text:
        return InsertSqlParseItem(parse_ok=False, parse_error="SQL 为空", raw_sql=sql or "")

    logger.info("使用大模型解析 INSERT SQL, sql_length=%s", len(text))
    try:
        data = await parse_insert_sql_with_llm(
            text,
            schema_name=schema_name,
            app_name=app_name,
        )
        result = _item_from_llm_dict(data, text)
        if not result.parse_ok:
            return InsertSqlParseItem(
                parse_ok=False,
                parse_error=result.parse_error or "大模型解析失败",
                raw_sql=text,
            )
        result.parse_error = None
    except httpx.HTTPError as e:
        logger.exception("大模型解析 INSERT SQL 调用失败")
        return InsertSqlParseItem(
            parse_ok=False,
            parse_error=f"LLM 调用失败: {e}",
            raw_sql=text,
        )
    except Exception as e:  # noqa: BLE001
        logger.exception("大模型解析 INSERT SQL 结果处理失败")
        return InsertSqlParseItem(
            parse_ok=False,
            parse_error=f"LLM 解析失败: {e}",
            raw_sql=text,
        )

    # 仅当大模型返回了 star_source_tables 时，才从图库展开字段
    if expand_star_from_graph and result.parse_ok and result.star_source_tables:
        result = await expand_star_fields_from_graph(result, schema_name=schema_name)
        if result.star_expanded:
            logger.info(
                "SELECT * 已从图库展开字段: tables=%s, fields=%s, field_relations=%s",
                result.star_source_tables,
                len(result.fields),
                len(result.field_relations),
            )
        else:
            logger.warning(
                "SELECT * 图库展开未命中字段: tables=%s",
                result.star_source_tables,
            )

    if result.parse_ok:
        from app.services.graph_source_resolve import resolve_source_tables_from_graph

        result = await resolve_source_tables_from_graph(result)
    return result
