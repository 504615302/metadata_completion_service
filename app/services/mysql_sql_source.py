"""
从 MySQL 读取待解析 INSERT SQL，并用大模型解析表/字段关系与中文名。
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
    InsertSqlParsePersistStats,
    InsertSqlParseRequest,
    InsertSqlParseResponse,
    InsertSqlParseRow,
)
from app.services.concurrent_runner import resolve_concurrency, run_bounded
from app.services.insert_sql_parse import parse_insert_sql
from app.services.insert_sql_progress import mark_insert_sql_processed, should_skip_insert_sql
from app.services.mysql_lineage_store import save_parse_item
from app.services.mysql_schema import quote_ident

logger = logging.getLogger(__name__)


def _resolve_workspace_filter(override: Optional[str]) -> Optional[str]:
    """请求覆盖优先，否则用配置；空串表示不过滤。"""
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


async def fetch_and_parse_insert_sql(request: InsertSqlParseRequest) -> InsertSqlParseResponse:
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

    results: list[InsertSqlParseRow] = []
    scanned = 0
    skipped = 0
    processed = 0
    ok = 0
    failed = 0
    llm_ok = 0
    pages = 0
    persist_stats = {"table_lineage": 0, "field_lineage": 0, "field_meta": 0}
    stats_lock = asyncio.Lock()

    logger.info(
        "INSERT SQL 批量解析启动: workspace_filter=%s, process_all=%s, skip_processed=%s, "
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
            logger.info("INSERT SQL 分页结束: page=%s, offset=%s", pages, offset)
            break

        scanned += len(rows_raw)
        logger.info(
            "INSERT SQL 第 %s 页: offset=%s, rows=%s, concurrency=%s",
            pages,
            offset,
            len(rows_raw),
            concurrency,
        )

        work_rows: list[dict] = []
        for row in rows_raw:
            row_id = row.get("id")
            if row_id is not None and await should_skip_insert_sql(
                row_id,
                skip_processed=request.skip_processed,
                retry_failed=request.retry_failed,
            ):
                skipped += 1
                logger.info(
                    "跳过已处理 INSERT SQL: id=%s (retry_failed=%s)",
                    row_id,
                    request.retry_failed,
                )
                continue
            work_rows.append(row)

        async def _handle(row: dict) -> None:
            nonlocal processed, ok, failed, llm_ok
            row_id = row.get("id")
            sql_text = _normalize_sql(row.get("sql_text"))
            workspace_name = row.get("workspace_name")
            raw_sql_id = _normalize_raw_sql_id(row_id)
            job_id = _normalize_int_id(row.get("job_id"))
            task_id = _normalize_int_id(row.get("task_id"))

            try:
                parsed = await parse_insert_sql(
                    sql_text,
                    app_name=workspace_name,
                    schema_name=None,
                    expand_star_from_graph=request.expand_star_from_graph,
                )
            except Exception:
                logger.exception(
                    "INSERT SQL 解析异常 id=%s workspace=%s",
                    row_id,
                    workspace_name,
                )
                from app.schemas import InsertSqlParseItem

                parsed = InsertSqlParseItem(
                    parse_ok=False,
                    parse_error="解析过程异常",
                    raw_sql=sql_text,
                )

            item = InsertSqlParseRow(
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
                    if parsed.parse_source == InsertParseSource.LLM:
                        llm_ok += 1
                else:
                    failed += 1
                    logger.warning(
                        "INSERT SQL 解析失败 id=%s workspace=%s err=%s",
                        row_id,
                        workspace_name,
                        parsed.parse_error,
                    )
                if request.include_results:
                    results.append(item)

            if row_id is not None and (parsed.parse_ok or request.mark_failed_as_processed):
                await mark_insert_sql_processed(
                    row_id,
                    parse_ok=parsed.parse_ok,
                    app_name=workspace_name,
                    schema_name=None,
                )

            if request.persist and parsed.parse_ok:
                try:
                    stats = await save_parse_item(
                        parsed,
                        app_name=workspace_name,
                        schema_name=None,
                        raw_sql_id=raw_sql_id,
                        job_id=job_id,
                        task_id=task_id,
                    )
                    async with stats_lock:
                        for k in persist_stats:
                            persist_stats[k] += int(stats.get(k, 0))
                except Exception:
                    logger.exception(
                        "INSERT SQL 落库失败 id=%s workspace=%s",
                        row_id,
                        workspace_name,
                    )

        await run_bounded(work_rows, concurrency, _handle)

        if not request.process_all or len(rows_raw) < page_size:
            break
        offset += len(rows_raw)

    persisted: Optional[InsertSqlParsePersistStats] = None
    if request.persist and any(persist_stats.values()):
        persisted = InsertSqlParsePersistStats(**persist_stats)

    logger.info(
        "INSERT SQL 批量解析完成: scanned=%s, skipped=%s, processed=%s, ok=%s, "
        "failed=%s, pages=%s, concurrency=%s",
        scanned,
        skipped,
        processed,
        ok,
        failed,
        pages,
        concurrency,
    )
    return InsertSqlParseResponse(
        total=scanned,
        scanned=scanned,
        skipped=skipped,
        processed=processed,
        parse_ok=ok,
        parse_failed=failed,
        llm_ok=llm_ok,
        pages=pages,
        concurrency=concurrency,
        persisted=persisted,
        results=results,
    )


async def parse_insert_sql_text(
    sql: str,
    *,
    workspace_name: Optional[str] = None,
    persist: bool = True,
    expand_star_from_graph: bool = True,
) -> InsertSqlParseRow:
    """不读库，直接解析传入的 SQL（便于联调）。"""
    parsed = await parse_insert_sql(
        sql,
        app_name=workspace_name,
        schema_name=None,
        expand_star_from_graph=expand_star_from_graph,
    )
    persisted: Optional[InsertSqlParsePersistStats] = None
    if persist and parsed.parse_ok:
        stats = await save_parse_item(
            parsed,
            app_name=workspace_name,
            schema_name=None,
        )
        persisted = InsertSqlParsePersistStats(**stats)
    return InsertSqlParseRow(
        workspace_name=workspace_name,
        parse=parsed,
        persisted=persisted,
    )
