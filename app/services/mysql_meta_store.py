"""
将字段/表补全结果回写到 MySQL 日表。

completion_type：
  0 = 无需补全 / 待办初始
  1 = 溯源补全（graph）
  2 = 人工智能补全（llm）
  50 = 无血缘关系，人工待补全（manual_pending）
  51 = 有关系但无可用中文（relation_pending）
  52 = 溯源中文名与日表 en_filed_name/table_name 一致（cn_eq_en）

补全结束后更新 completion_type 及结果字段。
"""
from __future__ import annotations

import logging
from typing import Optional

import anyio

from app.config import get_settings
from app.db import get_mysql_conn
from app.schemas import FieldMetadataResult, ResultSource, TableMetadataResult
from app.services.mysql_schema import quote_ident

logger = logging.getLogger(__name__)

# ResultSource -> 日表 completion_type
_COMPLETION_TYPE_BY_SOURCE: dict[ResultSource, str] = {
    ResultSource.GRAPH: "1",
    ResultSource.LLM: "2",
    ResultSource.MANUAL_PENDING: "50",
    ResultSource.RELATION_PENDING: "51",
    ResultSource.CN_EQ_EN: "52",
    ResultSource.FIELD_INFER: "2",
}


def completion_type_of(source: ResultSource) -> str:
    return _COMPLETION_TYPE_BY_SOURCE.get(source, "50")


def _update_field_row_sync(mysql_id: int, result: FieldMetadataResult) -> None:
    settings = get_settings()
    table = quote_ident(settings.mysql_fields_daily_table)
    ctype = completion_type_of(result.source)
    sql = f"""
        UPDATE {table}
        SET
            smart_complete_name = %s,
            reason = %s,
            via_vertex = %s,
            hops = %s,
            similarity = %s,
            saved_time = NOW(6),
            completion_type = %s
        WHERE id = %s
    """
    params = (
        result.name_cn,
        result.reason.value if result.reason else None,
        result.via_vertex,
        result.hops,
        result.similarity,
        ctype,
        mysql_id,
    )
    logger.info(
        "[字段日表UPDATE] 准备 mysql_id=%s, table=%s, params=%s",
        mysql_id,
        settings.mysql_fields_daily_table,
        {
            "smart_complete_name": result.name_cn,
            "reason": result.reason.value if result.reason else None,
            "via_vertex": result.via_vertex,
            "hops": result.hops,
            "similarity": result.similarity,
            "completion_type": ctype,
            "source": result.source.value,
        },
    )
    with get_mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            affected = cur.rowcount
        conn.commit()
    logger.info(
        "[字段日表UPDATE] 完成 mysql_id=%s, affected_rows=%s, source=%s, completion_type=%s, name_cn=%r",
        mysql_id,
        affected,
        result.source.value,
        ctype,
        result.name_cn,
    )
    if affected == 0:
        logger.warning("[字段日表UPDATE] 未更新到任何行 mysql_id=%s", mysql_id)


def _update_table_row_sync(mysql_id: int, result: TableMetadataResult) -> None:
    settings = get_settings()
    table = quote_ident(settings.mysql_tables_daily_table)
    ctype = completion_type_of(result.source)
    sql = f"""
        UPDATE {table}
        SET
            smart_complete_name = %s,
            reason = %s,
            via_vertex = %s,
            hops = %s,
            similarity = %s,
            saved_time = NOW(6),
            completion_type = %s
        WHERE id = %s
    """
    params = (
        result.name_cn,
        result.reason.value if result.reason else None,
        result.via_vertex,
        result.hops,
        result.similarity,
        ctype,
        mysql_id,
    )
    logger.info(
        "[表日表UPDATE] 准备 mysql_id=%s, table=%s, params=%s",
        mysql_id,
        settings.mysql_tables_daily_table,
        {
            "smart_complete_name": result.name_cn,
            "reason": result.reason.value if result.reason else None,
            "via_vertex": result.via_vertex,
            "hops": result.hops,
            "similarity": result.similarity,
            "completion_type": ctype,
            "source": result.source.value,
        },
    )
    with get_mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            affected = cur.rowcount
        conn.commit()
    logger.info(
        "[表日表UPDATE] 完成 mysql_id=%s, affected_rows=%s, source=%s, completion_type=%s, name_cn=%r",
        mysql_id,
        affected,
        result.source.value,
        ctype,
        result.name_cn,
    )
    if affected == 0:
        logger.warning("[表日表UPDATE] 未更新到任何行 mysql_id=%s", mysql_id)


async def persist_field_completion(
    mysql_id: Optional[int],
    result: FieldMetadataResult,
) -> None:
    if mysql_id is None:
        return
    await anyio.to_thread.run_sync(_update_field_row_sync, mysql_id, result)


async def persist_table_completion(
    mysql_id: Optional[int],
    result: TableMetadataResult,
) -> None:
    if mysql_id is None:
        return
    await anyio.to_thread.run_sync(_update_table_row_sync, mysql_id, result)
