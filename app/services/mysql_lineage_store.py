"""
将 INSERT SQL 解析出的血缘写入 MySQL。

raw_sql 列保留但不写入；SQL 溯源只写 raw_sql_id（源表行 id）。
job_id / task_id 自源表带入并写入结果表。
"""
from __future__ import annotations

import json
import logging
from typing import Optional

import anyio

from app.config import get_settings
from app.db import get_mysql_conn
from app.schemas import InsertSqlParseItem, InsertSqlParseRow
from app.services.mysql_schema import quote_ident

logger = logging.getLogger(__name__)


def _nz(value: Optional[str]) -> str:
    """NULL 统一成空串，便于 UNIQUE / UPSERT。"""
    return (value or "").strip()


def _json_list(values: Optional[list]) -> str:
    return json.dumps(values or [], ensure_ascii=False)


def _save_parse_rows_sync(rows: list[InsertSqlParseRow]) -> dict[str, int]:
    """同步写入血缘；供 anyio.to_thread 调用。"""
    settings = get_settings()
    tbl = quote_ident(settings.mysql_table_lineage_table)
    fld = quote_ident(settings.mysql_field_lineage_table)
    meta = quote_ident(settings.mysql_field_meta_table)

    table_n = 0
    field_n = 0
    meta_n = 0

    with get_mysql_conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                parse = row.parse
                if not parse.parse_ok:
                    continue
                # 源表已改为 workspace_name；结果表 app_name 列仍复用该值
                app = _nz(row.workspace_name)
                schema = ""
                parse_source = parse.parse_source.value if parse.parse_source else None
                table_cn = parse.table_name_cn
                raw_sql_id = row.raw_sql_id
                job_id = row.job_id
                task_id = row.task_id

                for rel in parse.table_relations:
                    cur.execute(
                        f"""
                        INSERT INTO {tbl} (
                            app_name, schema_name, source_table, target_table,
                            relation, parse_source, target_table_cn, raw_sql_id,
                            job_id, task_id, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(6))
                        ON DUPLICATE KEY UPDATE
                            parse_source = VALUES(parse_source),
                            target_table_cn = VALUES(target_table_cn),
                            raw_sql_id = COALESCE(NULLIF(VALUES(raw_sql_id), ''), raw_sql_id),
                            job_id = COALESCE(VALUES(job_id), job_id),
                            task_id = COALESCE(VALUES(task_id), task_id),
                            updated_at = NOW(6)
                        """,
                        (
                            app,
                            schema,
                            rel.source_table,
                            rel.target_table,
                            rel.relation,
                            parse_source,
                            table_cn,
                            raw_sql_id,
                            job_id,
                            task_id,
                        ),
                    )
                    table_n += 1

                for rel in parse.field_relations:
                    cur.execute(
                        f"""
                        INSERT INTO {fld} (
                            app_name, schema_name,
                            source_table, source_field,
                            target_table, target_field,
                            source_expr, relation, parse_source, raw_sql_id,
                            job_id, task_id, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(6))
                        ON DUPLICATE KEY UPDATE
                            source_expr = VALUES(source_expr),
                            parse_source = VALUES(parse_source),
                            raw_sql_id = COALESCE(NULLIF(VALUES(raw_sql_id), ''), raw_sql_id),
                            job_id = COALESCE(VALUES(job_id), job_id),
                            task_id = COALESCE(VALUES(task_id), task_id),
                            updated_at = NOW(6)
                        """,
                        (
                            app,
                            schema,
                            _nz(rel.source_table),
                            rel.source_field,
                            _nz(rel.target_table),
                            rel.target_field,
                            rel.source_expr,
                            rel.relation,
                            parse_source,
                            raw_sql_id,
                            job_id,
                            task_id,
                        ),
                    )
                    field_n += 1

                for f in parse.fields:
                    cur.execute(
                        f"""
                        INSERT INTO {meta} (
                            app_name, schema_name, target_table, field_name,
                            name_cn, source_expr, source_columns, source_tables,
                            parse_source, table_name_cn, raw_sql_id,
                            job_id, task_id, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(6))
                        ON DUPLICATE KEY UPDATE
                            name_cn = VALUES(name_cn),
                            source_expr = VALUES(source_expr),
                            source_columns = VALUES(source_columns),
                            source_tables = VALUES(source_tables),
                            parse_source = VALUES(parse_source),
                            table_name_cn = VALUES(table_name_cn),
                            raw_sql_id = COALESCE(NULLIF(VALUES(raw_sql_id), ''), raw_sql_id),
                            job_id = COALESCE(VALUES(job_id), job_id),
                            task_id = COALESCE(VALUES(task_id), task_id),
                            updated_at = NOW(6)
                        """,
                        (
                            app,
                            schema,
                            _nz(f.target_table),
                            f.field_name,
                            f.name_cn,
                            f.source_expr,
                            _json_list(f.source_columns),
                            _json_list(f.source_tables),
                            parse_source,
                            table_cn,
                            raw_sql_id,
                            job_id,
                            task_id,
                        ),
                    )
                    meta_n += 1

        conn.commit()

    logger.info(
        "血缘已写入 MySQL: table_lineage=%s, field_lineage=%s, field_meta=%s",
        table_n,
        field_n,
        meta_n,
    )
    return {
        "table_lineage": table_n,
        "field_lineage": field_n,
        "field_meta": meta_n,
    }


async def save_parse_rows(rows: list[InsertSqlParseRow]) -> dict[str, int]:
    """
    持久化解析结果到表/字段血缘表与字段元数据表。
    仅写入 parse_ok=True 的行；同键冲突则更新。
    """
    return await anyio.to_thread.run_sync(_save_parse_rows_sync, rows)


async def save_parse_item(
    parse: InsertSqlParseItem,
    *,
    app_name: Optional[str] = None,
    schema_name: Optional[str] = None,  # 保留签名兼容；源表已无 schema，落库写空串
    raw_sql_id: Optional[str] = None,
    job_id: Optional[int] = None,
    task_id: Optional[int] = None,
) -> dict[str, int]:
    _ = schema_name
    return await save_parse_rows(
        [
            InsertSqlParseRow(
                raw_sql_id=raw_sql_id,
                job_id=job_id,
                task_id=task_id,
                workspace_name=app_name,
                parse=parse,
            )
        ]
    )
