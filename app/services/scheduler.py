"""每天定时执行表优先元数据补全。"""
import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.config import get_settings
from app.db import get_redis
from app.schemas import CompletionRequest
from app.services.combined_pipeline import run_combined_pipeline

logger = logging.getLogger("metadata_pipeline.scheduler")


def _next_run_at(now: datetime, hour: int, minute: int) -> datetime:
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError("定时任务的 hour 必须为 0..23，minute 必须为 0..59")
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


async def _run_once(scheduled_at: datetime) -> None:
    """同一自然日只允许一个进程/副本执行任务。"""
    settings = get_settings()
    redis = get_redis()
    date_key = scheduled_at.strftime("%Y-%m-%d")
    lock_key = f"{settings.scheduler_lock_key_prefix}{date_key}"

    acquired = await redis.set(
        lock_key,
        f"running:{datetime.now(scheduled_at.tzinfo).isoformat()}",
        nx=True,
        ex=settings.scheduler_lock_ttl_seconds,
    )
    if not acquired:
        logger.info("定时补全已被其他进程执行，跳过: date=%s", date_key)
        return

    logger.info("每日定时补全开始: scheduled_at=%s", scheduled_at.isoformat())
    try:
        result = await run_combined_pipeline(
            CompletionRequest(
                limit=settings.scheduler_page_size,
                process_all=True,
            )
        )
        await redis.set(
            lock_key,
            f"completed:{datetime.now(scheduled_at.tzinfo).isoformat()}",
            xx=True,
            ex=settings.scheduler_lock_ttl_seconds,
        )
        logger.info("每日定时补全完成: %s", result.model_dump())
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("每日定时补全失败: scheduled_at=%s", scheduled_at.isoformat())
        await redis.set(
            lock_key,
            f"failed:{datetime.now(scheduled_at.tzinfo).isoformat()}",
            xx=True,
            ex=settings.scheduler_lock_ttl_seconds,
        )


async def run_daily_scheduler() -> None:
    """常驻调度循环；FastAPI 关闭时由 lifespan 取消。"""
    settings = get_settings()
    timezone = ZoneInfo(settings.scheduler_timezone)

    logger.info(
        "每日补全调度器已启动: timezone=%s, time=%02d:%02d",
        settings.scheduler_timezone,
        settings.scheduler_hour,
        settings.scheduler_minute,
    )
    while True:
        now = datetime.now(timezone)
        scheduled_at = _next_run_at(
            now,
            settings.scheduler_hour,
            settings.scheduler_minute,
        )
        wait_seconds = max((scheduled_at - now).total_seconds(), 0)
        logger.info(
            "下一次每日补全时间: scheduled_at=%s, wait_seconds=%.0f",
            scheduled_at.isoformat(),
            wait_seconds,
        )
        await asyncio.sleep(wait_seconds)
        await _run_once(scheduled_at)


def start_daily_scheduler() -> asyncio.Task[None] | None:
    settings = get_settings()
    if not settings.scheduler_enabled:
        logger.info("每日补全调度器未启用")
        return None
    return asyncio.create_task(
        run_daily_scheduler(),
        name="metadata-completion-daily-scheduler",
    )


async def stop_daily_scheduler(task: asyncio.Task[None] | None) -> None:
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("每日补全调度器已停止")
