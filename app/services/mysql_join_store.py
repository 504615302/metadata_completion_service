"""
将 SQL 表关联提取结果写入 MySQL insert_table_join。

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
from app.schemas import SqlJoinExtractItem, SqlJoinExtractRow
from app.services.mysql_schema import join_unique_hash, quote_ident

logger = logging.getLogger(__name__)


def _nz(value: Optional[str]) -> str:
    return (value or "").strip()


def _json_list(values: Optional[list]) -> str:
    return json.dumps(values or [], ensure_ascii=False)


def _save_join_rows_sync(rows: list[SqlJoinExtractRow]) -> dict[str, int]:
    settings = get_settings()
    tbl = quote_ident(settings.mysql_table_join_table)
    join_n = 0

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
                raw_sql_id = row.raw_sql_id
                job_id = row.job_id
                task_id = row.task_id

                for rel in parse.joins:
                    primary_schema = _nz(rel.primary.schema_name)
                    primary_table = rel.primary.table_name
                    secondary_schema = _nz(rel.secondary.schema_name)
                    secondary_table = rel.secondary.table_name
                    join_expression = rel.join_expression
                    key_hash = join_unique_hash(
                        app_name=app,
                        schema_name=schema,
                        primary_schema_name=primary_schema,
                        primary_table_name=primary_table,
                        secondary_schema_name=secondary_schema,
                        secondary_table_name=secondary_table,
                        join_expression=join_expression,
                    )
                    cur.execute(
                        f"""
                        INSERT INTO {tbl} (
                            app_name, schema_name,
                            primary_role, primary_schema_name, primary_table_name, primary_join_fields,
                            secondary_role, secondary_schema_name, secondary_table_name, secondary_join_fields,
                            join_type, join_expression, join_key_hash, parse_source, raw_sql_id,
                            job_id, task_id, updated_at
                        ) VALUES (
                            %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, NOW(6)
                        )
                        ON DUPLICATE KEY UPDATE
                            primary_role = VALUES(primary_role),
                            primary_join_fields = VALUES(primary_join_fields),
                            secondary_role = VALUES(secondary_role),
                            secondary_join_fields = VALUES(secondary_join_fields),
                            join_type = VALUES(join_type),
                            join_expression = VALUES(join_expression),
                            parse_source = VALUES(parse_source),
                            raw_sql_id = COALESCE(NULLIF(VALUES(raw_sql_id), ''), raw_sql_id),
                            job_id = COALESCE(VALUES(job_id), job_id),
                            task_id = COALESCE(VALUES(task_id), task_id),
                            updated_at = NOW(6)
                        """,
                        (
                            app,
                            schema,
                            rel.primary.role.value,
                            primary_schema,
                            primary_table,
                            _json_list(rel.primary.join_fields),
                            rel.secondary.role.value,
                            secondary_schema,
                            secondary_table,
                            _json_list(rel.secondary.join_fields),
                            _nz(rel.join_type),
                            join_expression,
                            key_hash,
                            parse_source,
                            raw_sql_id,
                            job_id,
                            task_id,
                        ),
                    )
                    join_n += 1

        conn.commit()

    logger.info("表关联已写入 MySQL: table_join=%s", join_n)
    return {"table_join": join_n}


async def save_join_rows(rows: list[SqlJoinExtractRow]) -> dict[str, int]:
    return await anyio.to_thread.run_sync(_save_join_rows_sync, rows)


async def save_join_item(
    parse: SqlJoinExtractItem,
    *,
    app_name: Optional[str] = None,
    schema_name: Optional[str] = None,  # 保留签名兼容；源表已无 schema，落库写空串
    raw_sql_id: Optional[str] = None,
    job_id: Optional[int] = None,
    task_id: Optional[int] = None,
) -> dict[str, int]:
    _ = schema_name
    return await save_join_rows(
        [
            SqlJoinExtractRow(
                raw_sql_id=raw_sql_id,
                job_id=job_id,
                task_id=task_id,
                workspace_name=app_name,
                parse=parse,
            )
        ]
    )
