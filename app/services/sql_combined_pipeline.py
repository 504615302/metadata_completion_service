"""
单条 SQL 合并溯源：并行执行 INSERT 血缘解析 + 表关联提取，并写入对应结果表。

- 血缘 → insert_table_lineage / insert_field_lineage / insert_field_meta
- 关联 → insert_table_join
结果表只写 raw_sql_id，不写 raw_sql 原文。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from app.schemas import (
    InsertSqlParseItem,
    InsertSqlParsePersistStats,
    InsertSqlParseRow,
    SqlCombinedTraceRequest,
    SqlCombinedTraceResponse,
    SqlJoinExtractItem,
    SqlJoinExtractPersistStats,
    SqlJoinExtractRow,
)
from app.services.insert_sql_parse import parse_insert_sql
from app.services.insert_sql_progress import mark_insert_sql_processed
from app.services.mysql_join_store import save_join_item
from app.services.mysql_lineage_store import save_parse_item
from app.services.sql_join_extract import extract_sql_joins
from app.services.sql_join_progress import mark_sql_join_processed

logger = logging.getLogger(__name__)


def _normalize_raw_sql_id(value: str, *, max_len: int = 64) -> str:
    text = (value or "").strip()
    if not text:
        raise ValueError("raw_sql_id 不能为空")
    if len(text) > max_len:
        logger.warning("raw_sql_id 超长，截断到 %s: id=%r", max_len, text)
        return text[:max_len]
    return text


async def _run_lineage(
    sql: str,
    *,
    raw_sql_id: str,
    workspace_name: Optional[str],
    job_id: Optional[int],
    task_id: Optional[int],
    persist: bool,
    expand_star_from_graph: bool,
    mark_processed: bool,
) -> InsertSqlParseRow:
    try:
        parsed = await parse_insert_sql(
            sql,
            app_name=workspace_name,
            schema_name=None,
            expand_star_from_graph=expand_star_from_graph,
        )
    except Exception:
        logger.exception("合并溯源-血缘解析异常 raw_sql_id=%s", raw_sql_id)
        parsed = InsertSqlParseItem(
            parse_ok=False,
            parse_error="解析过程异常",
            raw_sql=sql,
        )

    persisted: Optional[InsertSqlParsePersistStats] = None
    if persist and parsed.parse_ok:
        try:
            stats = await save_parse_item(
                parsed,
                app_name=workspace_name,
                schema_name=None,
                raw_sql_id=raw_sql_id,
                job_id=job_id,
                task_id=task_id,
            )
            persisted = InsertSqlParsePersistStats(**stats)
        except Exception:
            logger.exception("合并溯源-血缘落库失败 raw_sql_id=%s", raw_sql_id)

    if mark_processed:
        try:
            await mark_insert_sql_processed(
                raw_sql_id,
                parse_ok=parsed.parse_ok,
                app_name=workspace_name,
                schema_name=None,
            )
        except Exception:
            logger.exception("合并溯源-血缘 Redis 标记失败 raw_sql_id=%s", raw_sql_id)

    return InsertSqlParseRow(
        raw_sql_id=raw_sql_id,
        job_id=job_id,
        task_id=task_id,
        workspace_name=workspace_name,
        parse=parsed,
        persisted=persisted,
    )


async def _run_join(
    sql: str,
    *,
    raw_sql_id: str,
    workspace_name: Optional[str],
    job_id: Optional[int],
    task_id: Optional[int],
    persist: bool,
    mark_processed: bool,
) -> SqlJoinExtractRow:
    try:
        parsed = await extract_sql_joins(
            sql,
            app_name=workspace_name,
            schema_name=None,
        )
    except Exception:
        logger.exception("合并溯源-表关联提取异常 raw_sql_id=%s", raw_sql_id)
        parsed = SqlJoinExtractItem(
            parse_ok=False,
            parse_error="提取过程异常",
            raw_sql=sql,
        )

    persisted: Optional[SqlJoinExtractPersistStats] = None
    if persist and parsed.parse_ok:
        try:
            stats = await save_join_item(
                parsed,
                app_name=workspace_name,
                schema_name=None,
                raw_sql_id=raw_sql_id,
                job_id=job_id,
                task_id=task_id,
            )
            persisted = SqlJoinExtractPersistStats(**stats)
        except Exception:
            logger.exception("合并溯源-表关联落库失败 raw_sql_id=%s", raw_sql_id)

    if mark_processed:
        try:
            await mark_sql_join_processed(
                raw_sql_id,
                parse_ok=parsed.parse_ok,
                app_name=workspace_name,
                schema_name=None,
                join_count=len(parsed.joins),
            )
        except Exception:
            logger.exception("合并溯源-表关联 Redis 标记失败 raw_sql_id=%s", raw_sql_id)

    return SqlJoinExtractRow(
        raw_sql_id=raw_sql_id,
        job_id=job_id,
        task_id=task_id,
        workspace_name=workspace_name,
        parse=parsed,
        persisted=persisted,
    )


async def run_sql_combined_trace(request: SqlCombinedTraceRequest) -> SqlCombinedTraceResponse:
    """并行跑血缘 + 表关联，写结果表与 Redis 进度。"""
    raw_sql_id = _normalize_raw_sql_id(request.raw_sql_id)
    sql = request.sql or ""
    workspace_name = request.workspace_name

    logger.info(
        "合并溯源启动: raw_sql_id=%s, persist=%s, mark_processed=%s",
        raw_sql_id,
        request.persist,
        request.mark_processed,
    )

    lineage_row, join_row = await asyncio.gather(
        _run_lineage(
            sql,
            raw_sql_id=raw_sql_id,
            workspace_name=workspace_name,
            job_id=request.job_id,
            task_id=request.task_id,
            persist=request.persist,
            expand_star_from_graph=request.expand_star_from_graph,
            mark_processed=request.mark_processed,
        ),
        _run_join(
            sql,
            raw_sql_id=raw_sql_id,
            workspace_name=workspace_name,
            job_id=request.job_id,
            task_id=request.task_id,
            persist=request.persist,
            mark_processed=request.mark_processed,
        ),
    )

    logger.info(
        "合并溯源完成: raw_sql_id=%s, lineage_ok=%s, join_ok=%s, "
        "lineage_persisted=%s, join_persisted=%s",
        raw_sql_id,
        lineage_row.parse.parse_ok,
        join_row.parse.parse_ok,
        lineage_row.persisted.model_dump() if lineage_row.persisted else None,
        join_row.persisted.model_dump() if join_row.persisted else None,
    )

    return SqlCombinedTraceResponse(
        raw_sql_id=raw_sql_id,
        workspace_name=workspace_name,
        job_id=request.job_id,
        task_id=request.task_id,
        lineage=lineage_row,
        join=join_row,
    )
