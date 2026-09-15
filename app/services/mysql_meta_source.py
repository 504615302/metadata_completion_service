"""
从 MySQL 日表扫描待补全字段/表（不再用 ArangoDB 作为待办清单来源）。

待办条件：
  completion_type = '0'   -- 待办初始，处理后回填 1/2/50/51/52

completion_type 含义：
  0 = 无需补全（待办初始）
  1 = 溯源补全
  2 = 人工智能补全
  3 = （预留）
  50 = 无血缘关系，人工待补全（manual_pending）
  51 = 有关系但无可用中文（relation_pending）
  52 = 溯源中文名与日表 en_filed_name 一致（cn_eq_en）

图谱溯源仍通过 graph_id 访问 ArangoDB。
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from app.config import get_settings
from app.db import get_mysql_conn
from app.schemas import CompletionTable, MissingField, MissingTable
from app.services.mysql_schema import quote_ident

logger = logging.getLogger(__name__)

# 待办：尚未写入补全结果类型
_PENDING_WHERE = "completion_type = '0'"


def _task_table_exists_sql(outer_alias: str = "daily") -> str:
    """日报 schema_name + table_name 必须同时存在于任务表的同一行。"""
    settings = get_settings()
    task_table = quote_ident(settings.mysql_task_tables_table)
    return f"""
        EXISTS (
            SELECT 1
            FROM {task_table} AS task_table
            WHERE task_table.schema_name = {outer_alias}.schema_name
              AND task_table.table_name = {outer_alias}.table_name
        )
    """


def _nz(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_arango_id(raw: Any, *, collection: str) -> str:
    """日表 graph_id 只有 _key（如 p123.s212...），拼成 Arango _id：collection/key。

    例：p123.s212.xxx → field/p123.s212.xxx 或 table/p123.s212.xxx
    若已带 field/...、table/...，统一成配置集合名。
    """
    value = _nz(raw)
    if not value:
        return ""
    if "/" in value:
        _, key = value.split("/", 1)
        key = key.strip()
        return f"{collection}/{key}" if key else ""
    return f"{collection}/{value}"


def _vertex_id(*, graph_id: Any, collection: str, mysql_id: Any) -> str:
    arango_id = _to_arango_id(graph_id, collection=collection)
    if arango_id:
        raw_s = _nz(graph_id)
        if raw_s != arango_id:
            logger.info("日表 graph_id=%s → Arango _id=%s", raw_s, arango_id)
        return arango_id
    fallback = f"{collection}/mysql-{mysql_id}"
    logger.warning("日表无 graph_id，使用占位 _id=%s", fallback)
    return fallback


def _to_missing_field(row: dict) -> MissingField | None:
    mysql_id = row.get("id")
    name_en = _nz(row.get("en_filed_name"))
    if not name_en:
        logger.warning("跳过无英文字段名的日表行: id=%s, row_keys=%s", mysql_id, list(row.keys()))
        return None
    settings = get_settings()
    raw_graph_id = row.get("graph_id")
    vertex_id = _vertex_id(
        graph_id=raw_graph_id,
        collection=settings.arango_field_collection,
        mysql_id=mysql_id,
    )
    key = vertex_id.split("/", 1)[-1]
    schema_name = _nz(row.get("schema_name")) or None
    table_name = _nz(row.get("table_name")) or None
    logger.info(
        "[字段日表映射] mysql_id=%s, graph_id=%r → arango_id=%s, "
        "name_en=%s, schema=%s, table=%s, completion_type=%s",
        mysql_id,
        raw_graph_id,
        vertex_id,
        name_en,
        schema_name,
        table_name,
        row.get("completion_type"),
    )
    return MissingField(
        key=key,
        id=vertex_id,
        name_en=name_en,
        name_cn=None,
        table_id=None,
        table_vertex_id=None,
        table_name_en=table_name,
        table_name_cn=None,
        mysql_id=int(mysql_id) if mysql_id is not None else None,
        schema_name=schema_name,
    )


def _to_missing_table(row: dict) -> MissingTable | None:
    mysql_id = row.get("id")
    name_en = _nz(row.get("table_name"))
    if not name_en:
        logger.warning("跳过无表名的日表行: id=%s", mysql_id)
        return None
    settings = get_settings()
    raw_graph_id = row.get("graph_id")
    vertex_id = _vertex_id(
        graph_id=raw_graph_id,
        collection=settings.arango_table_collection,
        mysql_id=mysql_id,
    )
    key = vertex_id.split("/", 1)[-1]
    logger.info(
        "[表日表映射] mysql_id=%s, graph_id=%r → arango_id=%s, "
        "name_en=%s, schema=%s, completion_type=%s",
        mysql_id,
        raw_graph_id,
        vertex_id,
        name_en,
        _nz(row.get("schema_name")) or None,
        row.get("completion_type"),
    )
    return MissingTable(
        key=key,
        id=vertex_id,
        object_key=None,
        name_en=name_en,
        name_cn=None,
        mysql_id=int(mysql_id) if mysql_id is not None else None,
        schema_name=_nz(row.get("schema_name")) or None,
    )


def _to_completion_table(row: dict) -> CompletionTable | None:
    missing = _to_missing_table(row)
    if missing is None:
        return None
    return CompletionTable(
        key=missing.key,
        id=missing.id,
        object_key=missing.object_key,
        name_en=missing.name_en,
        name_cn=missing.name_cn,
        mysql_id=missing.mysql_id,
        schema_name=missing.schema_name,
    )


def fetch_missing_fields(limit: int = 500, offset: int = 0, *, after_id: int = 0) -> list[MissingField]:
    settings = get_settings()
    table = quote_ident(settings.mysql_fields_daily_table)
    task_table_exists = _task_table_exists_sql()
    if after_id <= 0 and offset > 0:
        logger.warning("fetch_missing_fields 已改用 after_id 游标，忽略 offset=%s", offset)
    logger.info(
        "从 MySQL 拉取待补全字段: table=%s, after_id=%s, limit=%s",
        settings.mysql_fields_daily_table,
        after_id,
        limit,
    )
    sql = f"""
        SELECT
            id, schema_name, table_name, en_filed_name,
            graph_id, completion_type
        FROM {table} AS daily
        WHERE {_PENDING_WHERE}
          AND {task_table_exists}
          AND id > %s
          AND en_filed_name IS NOT NULL
          AND TRIM(en_filed_name) <> ''
        ORDER BY id ASC
        LIMIT %s
    """
    with get_mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (max(0, after_id), limit))
            rows = list(cur.fetchall())

    fields: list[MissingField] = []
    for row in rows:
        item = _to_missing_field(row)
        if item is not None:
            fields.append(item)
    logger.info("待补全字段拉取完成: mysql_rows=%s, count=%s", len(rows), len(fields))
    return fields


def fetch_missing_tables(limit: int = 500, offset: int = 0, *, after_id: int = 0) -> list[MissingTable]:
    settings = get_settings()
    table = quote_ident(settings.mysql_tables_daily_table)
    task_table_exists = _task_table_exists_sql()
    if after_id <= 0 and offset > 0:
        logger.warning("fetch_missing_tables 已改用 after_id 游标，忽略 offset=%s", offset)
    logger.info(
        "从 MySQL 拉取待补全表: table=%s, after_id=%s, limit=%s",
        settings.mysql_tables_daily_table,
        after_id,
        limit,
    )
    sql = f"""
        SELECT
            id, schema_name, table_name, graph_id, completion_type
        FROM {table} AS daily
        WHERE {_PENDING_WHERE}
          AND {task_table_exists}
          AND id > %s
          AND table_name IS NOT NULL
          AND TRIM(table_name) <> ''
        ORDER BY id ASC
        LIMIT %s
    """
    with get_mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (max(0, after_id), limit))
            rows = list(cur.fetchall())

    tables: list[MissingTable] = []
    for row in rows:
        item = _to_missing_table(row)
        if item is not None:
            tables.append(item)
    logger.info("待补全表拉取完成: mysql_rows=%s, count=%s", len(rows), len(tables))
    return tables


def fetch_all_tables(limit: int = 500, offset: int = 0, *, after_id: int = 0) -> list[CompletionTable]:
    """分页拉取得补全表（与 fetch_missing_tables 同过滤条件）。"""
    settings = get_settings()
    table = quote_ident(settings.mysql_tables_daily_table)
    task_table_exists = _task_table_exists_sql()
    if after_id <= 0 and offset > 0:
        logger.warning("fetch_all_tables 已改用 after_id 游标，忽略 offset=%s", offset)
    sql = f"""
        SELECT
            id, schema_name, table_name, graph_id, completion_type
        FROM {table} AS daily
        WHERE {_PENDING_WHERE}
          AND {task_table_exists}
          AND id > %s
          AND table_name IS NOT NULL
          AND TRIM(table_name) <> ''
        ORDER BY id ASC
        LIMIT %s
    """
    with get_mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (max(0, after_id), limit))
            rows = list(cur.fetchall())

    out: list[CompletionTable] = []
    for row in rows:
        item = _to_completion_table(row)
        if item is not None:
            out.append(item)
    logger.info("合并流水线待补全表: count=%s", len(out))
    return out


def fetch_missing_fields_for_table(
    table_vertex_id: str,
    *,
    table_object_key: Optional[str] = None,
    table_name_en: Optional[str] = None,
    table_name_cn: Optional[str] = None,
    schema_name: Optional[str] = None,
    limit: int = 5000,
) -> list[MissingField]:
    """按 schema + 表英文名，从字段日表拉取该表下待补全字段。"""
    del table_vertex_id, table_object_key
    settings = get_settings()
    table = quote_ident(settings.mysql_fields_daily_table)
    task_table_exists = _task_table_exists_sql()
    name_en = _nz(table_name_en)
    if not name_en:
        return []

    where = [
        _PENDING_WHERE,
        task_table_exists,
        "en_filed_name IS NOT NULL",
        "TRIM(en_filed_name) <> ''",
        "TRIM(table_name) = %s",
    ]
    params: list[Any] = [name_en]
    schema = _nz(schema_name)
    if schema:
        where.append("TRIM(schema_name) = %s")
        params.append(schema)

    sql = f"""
        SELECT
            id, schema_name, table_name, en_filed_name,
            graph_id, completion_type
        FROM {table} AS daily
        WHERE {' AND '.join(where)}
        ORDER BY id ASC
        LIMIT %s
    """
    params.append(limit)

    with get_mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall())

    fields: list[MissingField] = []
    for row in rows:
        item = _to_missing_field(row)
        if item is None:
            continue
        item.table_name_en = name_en
        item.table_name_cn = table_name_cn
        fields.append(item)
    logger.info(
        "表下待补全字段: schema=%s, table=%s, count=%s",
        schema or "",
        name_en,
        len(fields),
    )
    return fields
