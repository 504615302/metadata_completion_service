"""定时检测 insert_sql_source 中的新 SQL，执行血缘解析与表关联提取。

依赖既有 Redis 进度（skip_processed）：
- 血缘：metadata-completion:insert-sql:{id}
- 关联：metadata-completion:sql-join:{id}
未打标的行视为新数据；本轮均跳过则只记日志、不额外调用 LLM。
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.config import get_settings
from app.db import get_redis
from app.schemas import InsertSqlParseRequest, SqlJoinExtractRequest
from app.services.mysql_sql_source import fetch_and_parse_insert_sql
from app.services.sql_join_pipeline import fetch_and_extract_sql_joins

logger = logging.getLogger("metadata_pipeline.sql_scheduler")


def _next_interval_run(now: datetime, interval_hours: int) -> datetime:
    """按整点对齐到 interval_hours 边界（如 3h -> 0/3/6/...）。"""
    if interval_hours < 1:
        raise ValueError("sql_scheduler_interval_hours 必须 >= 1")
    block = (now.hour // interval_hours) * interval_hours
    target = now.replace(hour=block, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(hours=interval_hours)
    return target


def _slot_key(scheduled_at: datetime, interval_hours: int) -> str:
    """同一时间窗口内多副本只跑一次。"""
    return scheduled_at.strftime(f"%Y-%m-%d-%H-{interval_hours}h")


async def _run_once(scheduled_at: datetime) -> None:
    settings = get_settings()
    redis = get_redis()
    slot = _slot_key(scheduled_at, settings.sql_scheduler_interval_hours)
    lock_key = f"{settings.sql_scheduler_lock_key_prefix}{slot}"

    acquired = await redis.set(
        lock_key,
        f"running:{datetime.now(scheduled_at.tzinfo).isoformat()}",
        nx=True,
        ex=settings.sql_scheduler_lock_ttl_seconds,
    )
    if not acquired:
        logger.info("SQL 定时任务已被其他进程执行，跳过: slot=%s", slot)
        return

    logger.info("SQL 定时任务开始: scheduled_at=%s, slot=%s", scheduled_at.isoformat(), slot)
    try:
        parse_req = InsertSqlParseRequest(
            limit=settings.sql_scheduler_page_size,
            process_all=True,
            skip_processed=True,
            retry_failed=False,
            persist=True,
            include_results=False,
        )
        parse_result = await fetch_and_parse_insert_sql(parse_req)

        join_req = SqlJoinExtractRequest(
            limit=settings.sql_scheduler_page_size,
            process_all=True,
            skip_processed=True,
            retry_failed=False,
            persist=True,
            include_results=False,
        )
        join_result = await fetch_and_extract_sql_joins(join_req)

        if parse_result.processed == 0 and join_result.processed == 0:
            logger.info(
                "SQL 定时任务：无新数据（均已处理或为空） scanned_lineage=%s scanned_join=%s",
                parse_result.scanned,
                join_result.scanned,
            )
        else:
            logger.info(
                "SQL 定时任务完成: lineage processed=%s ok=%s failed=%s; "
                "join processed=%s ok=%s failed=%s",
                parse_result.processed,
                parse_result.parse_ok,
                parse_result.parse_failed,
                join_result.processed,
                join_result.parse_ok,
                join_result.parse_failed,
            )

        await redis.set(
            lock_key,
            f"completed:{datetime.now(scheduled_at.tzinfo).isoformat()}",
            xx=True,
            ex=settings.sql_scheduler_lock_ttl_seconds,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("SQL 定时任务失败: scheduled_at=%s", scheduled_at.isoformat())
        await redis.set(
            lock_key,
            f"failed:{datetime.now(scheduled_at.tzinfo).isoformat()}",
            xx=True,
            ex=settings.sql_scheduler_lock_ttl_seconds,
        )


async def run_sql_scheduler() -> None:
    """常驻调度循环；FastAPI 关闭时由 lifespan 取消。"""
    settings = get_settings()
    timezone = ZoneInfo(settings.scheduler_timezone)
    interval = settings.sql_scheduler_interval_hours

    logger.info(
        "SQL 定时调度器已启动: timezone=%s, interval_hours=%s",
        settings.scheduler_timezone,
        interval,
    )
    while True:
        now = datetime.now(timezone)
        scheduled_at = _next_interval_run(now, interval)
        wait_seconds = max((scheduled_at - now).total_seconds(), 0)
        logger.info(
            "下一次 SQL 检测时间: scheduled_at=%s, wait_seconds=%.0f",
            scheduled_at.isoformat(),
            wait_seconds,
        )
        await asyncio.sleep(wait_seconds)
        await _run_once(scheduled_at)


def start_sql_scheduler() -> asyncio.Task[None] | None:
    settings = get_settings()
    if not settings.sql_scheduler_enabled:
        logger.info("SQL 定时调度器未启用")
        return None
    return asyncio.create_task(
        run_sql_scheduler(),
        name="metadata-sql-interval-scheduler",
    )


async def stop_sql_scheduler(task: asyncio.Task[None] | None) -> None:
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("SQL 定时调度器已停止")
