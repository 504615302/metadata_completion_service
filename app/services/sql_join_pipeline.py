"""
从 MySQL 源表批量读取 SQL，提取表关联关系。
支持全量翻页、Redis 跳过已处理、页内并发调用 LLM。
默认仅处理 workspace_name = MYSQL_WORKSPACE_FILTER（面向基层数据服务）。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import anyio

from app.config import get_settings
from app.db import get_mysql_conn
from app.schemas import (
    InsertParseSource,
    SqlJoinExtractItem,
    SqlJoinExtractPersistStats,
    SqlJoinExtractRequest,
    SqlJoinExtractResponse,
    SqlJoinExtractRow,
    SqlJoinTextRequest,
)
from app.services.concurrent_runner import resolve_concurrency, run_bounded
from app.services.mysql_join_store import save_join_item
from app.services.mysql_schema import quote_ident
from app.services.sql_join_extract import extract_sql_joins
from app.services.sql_join_progress import mark_sql_join_processed, should_skip_sql_join

logger = logging.getLogger(__name__)


def _resolve_workspace_filter(override: Optional[str]) -> Optional[str]:
    settings = get_settings()
    raw = override if override is not None else settings.mysql_workspace_filter
    value = (raw or "").strip()
    return value or None


def _fetch_page(
    *,
    table: str,
    col_sql: str,
    col_workspace: str,
    where_sql: str,
    params: list[Any],
) -> list[dict]:
    query = f"""
        SELECT
            id,
            {col_sql} AS sql_text,
            {col_workspace} AS workspace_name,
            job_id,
            task_id
        FROM {table}
        WHERE {where_sql}
        ORDER BY id ASC
        LIMIT %s OFFSET %s
    """
    with get_mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return list(cur.fetchall())


def _normalize_sql(sql_text: Any) -> str:
    if hasattr(sql_text, "decode"):
        sql_text = sql_text.decode("utf-8", errors="replace")
    return str(sql_text or "")


def _normalize_raw_sql_id(value: Any, *, max_len: int = 64) -> str | None:
    """源表 id 统一为 VARCHAR(64) 字符串；空值返回 None。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    text = str(value).strip()
    if not text:
        return None
    if len(text) > max_len:
        logger.warning("源表 id 超长，截断到 %s: id=%r", max_len, text)
        return text[:max_len]
    return text


def _normalize_int_id(value: Any) -> int | None:
    """源表 job_id / task_id 转为 int；无法解析则 None。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != value:  # NaN
            return None
        return int(value)
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        logger.warning("无法解析为 int: %r", value)
        return None


async def fetch_and_extract_sql_joins(request: SqlJoinExtractRequest) -> SqlJoinExtractResponse:
    settings = get_settings()
    concurrency = resolve_concurrency(request.concurrency, settings.pipeline_concurrency)

    table = quote_ident(settings.mysql_sql_source_table)
    col_sql = quote_ident(settings.mysql_col_sql)
    col_workspace = quote_ident(settings.mysql_col_workspace_name)

    where_parts: list[str] = [f"{col_sql} IS NOT NULL", f"TRIM({col_sql}) <> ''"]
    base_params: list[Any] = []

    workspace_filter = _resolve_workspace_filter(request.workspace_name)
    if workspace_filter:
        base_params.append(workspace_filter)
        where_parts.append(f"{col_workspace} = %s")

    where_sql = " AND ".join(where_parts)
    page_size = request.limit
    offset = request.offset

    results: list[SqlJoinExtractRow] = []
    scanned = 0
    skipped = 0
    processed = 0
    ok = 0
    failed = 0
    llm_ok = 0
    pages = 0
    join_count = 0
    persist_stats = {"table_join": 0}
    stats_lock = asyncio.Lock()

    logger.info(
        "SQL 表关联批量提取启动: workspace_filter=%s, process_all=%s, skip_processed=%s, "
        "retry_failed=%s, page_size=%s, offset=%s, concurrency=%s",
        workspace_filter,
        request.process_all,
        request.skip_processed,
        request.retry_failed,
        page_size,
        offset,
        concurrency,
    )

    while True:
        pages += 1
        page_params = list(base_params) + [page_size, offset]

        def _do_fetch(p: list[Any] = page_params) -> list[dict]:
            return _fetch_page(
                table=table,
                col_sql=col_sql,
                col_workspace=col_workspace,
                where_sql=where_sql,
                params=p,
            )

        rows_raw = await anyio.to_thread.run_sync(_do_fetch)
        if not rows_raw:
            logger.info("SQL 表关联分页结束: page=%s, offset=%s", pages, offset)
            break

        scanned += len(rows_raw)
        logger.info(
            "SQL 表关联第 %s 页: offset=%s, rows=%s, concurrency=%s",
            pages,
            offset,
            len(rows_raw),
            concurrency,
        )

        work_rows: list[dict] = []
        for row in rows_raw:
            row_id = row.get("id")
            if row_id is not None and await should_skip_sql_join(
                row_id,
                skip_processed=request.skip_processed,
                retry_failed=request.retry_failed,
            ):
                skipped += 1
                continue
            work_rows.append(row)

        async def _handle(row: dict) -> None:
            nonlocal processed, ok, failed, llm_ok, join_count
            row_id = row.get("id")
            sql_text = _normalize_sql(row.get("sql_text"))
            workspace_name = row.get("workspace_name")
            # 源表 id 可能为字符串；统一规范为 VARCHAR(64)
            raw_sql_id = _normalize_raw_sql_id(row_id)
            job_id = _normalize_int_id(row.get("job_id"))
            task_id = _normalize_int_id(row.get("task_id"))

            try:
                parsed = await extract_sql_joins(
                    sql_text,
                    app_name=workspace_name,
                    schema_name=None,
                )
            except Exception as exc:
                logger.exception(
                    "SQL 表关联提取异常 id=%s workspace=%s",
                    row_id,
                    workspace_name,
                )
                parsed = SqlJoinExtractItem(
                    parse_ok=False,
                    parse_error=f"提取过程异常: {exc}",
                    raw_sql=sql_text,
                )

            item = SqlJoinExtractRow(
                raw_sql_id=raw_sql_id,
                job_id=job_id,
                task_id=task_id,
                workspace_name=workspace_name,
                parse=parsed,
            )

            async with stats_lock:
                processed += 1
                if parsed.parse_ok:
                    ok += 1
                    join_count += len(parsed.joins)
                    if parsed.parse_source == InsertParseSource.LLM:
                        llm_ok += 1
                else:
                    failed += 1
                if request.include_results:
                    results.append(item)

            if row_id is not None and (parsed.parse_ok or request.mark_failed_as_processed):
                await mark_sql_join_processed(
                    row_id,
                    parse_ok=parsed.parse_ok,
                    app_name=workspace_name,
                    schema_name=None,
                    join_count=len(parsed.joins),
                )

            if request.persist and parsed.parse_ok:
                try:
                    stats = await save_join_item(
                        parsed,
                        app_name=workspace_name,
                        schema_name=None,
                        raw_sql_id=raw_sql_id,
                        job_id=job_id,
                        task_id=task_id,
                    )
                    async with stats_lock:
                        persist_stats["table_join"] += int(stats.get("table_join", 0))
                except Exception:
                    logger.exception(
                        "SQL 表关联落库失败 id=%s workspace=%s",
                        row_id,
                        workspace_name,
                    )

        await run_bounded(work_rows, concurrency, _handle)

        if not request.process_all or len(rows_raw) < page_size:
            break
        offset += len(rows_raw)

    persisted: Optional[SqlJoinExtractPersistStats] = None
    if request.persist and persist_stats["table_join"]:
        persisted = SqlJoinExtractPersistStats(**persist_stats)

    logger.info(
        "SQL 表关联批量提取完成: scanned=%s, skipped=%s, processed=%s, ok=%s, "
        "failed=%s, join_count=%s, pages=%s",
        scanned,
        skipped,
        processed,
        ok,
        failed,
        join_count,
        pages,
    )
    return SqlJoinExtractResponse(
        total=scanned,
        scanned=scanned,
        skipped=skipped,
        processed=processed,
        parse_ok=ok,
        parse_failed=failed,
        llm_ok=llm_ok,
        pages=pages,
        concurrency=concurrency,
        join_count=join_count,
        persisted=persisted,
        results=results,
    )


async def extract_sql_joins_text(request: SqlJoinTextRequest) -> SqlJoinExtractRow:
    """不读库，直接提取传入 SQL 的表关联。"""
    parsed = await extract_sql_joins(
        request.sql,
        app_name=request.workspace_name,
        schema_name=None,
    )
    persisted: Optional[SqlJoinExtractPersistStats] = None
    if request.persist and parsed.parse_ok:
        stats = await save_join_item(
            parsed,
            app_name=request.workspace_name,
            schema_name=None,
        )
        persisted = SqlJoinExtractPersistStats(**stats)
    return SqlJoinExtractRow(
        workspace_name=request.workspace_name,
        parse=parsed,
        persisted=persisted,
    )
