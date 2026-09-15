"""
SQL 表关联提取进度：Redis 标记已处理的 insert_sql_source 行。

Key：{redis_join_key_prefix}:{row_id}
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from app.config import get_settings
from app.db import get_redis
from app.schemas import SqlJoinExtractStatsResponse

logger = logging.getLogger(__name__)


def _prefix() -> str:
    settings = get_settings()
    return (settings.redis_join_key_prefix or "").rstrip(":")


def _key(row_id: int | str) -> str:
    return f"{_prefix()}:{row_id}"


async def get_sql_join_progress_status(row_id: int | str) -> str | None:
    redis = get_redis()
    raw = await redis.get(_key(row_id))
    if raw is None:
        return None
    try:
        payload = json.loads(raw)
        status = str(payload.get("status") or "").lower()
        if status in {"ok", "failed"}:
            return status
        return "unknown"
    except (json.JSONDecodeError, TypeError):
        return "unknown"


async def should_skip_sql_join(
    row_id: int | str,
    *,
    skip_processed: bool,
    retry_failed: bool = False,
) -> bool:
    if not skip_processed:
        return False
    status = await get_sql_join_progress_status(row_id)
    if status is None:
        return False
    if retry_failed:
        return status == "ok"
    return True


async def mark_sql_join_processed(
    row_id: int | str,
    *,
    parse_ok: bool,
    app_name: str | None = None,
    schema_name: str | None = None,
    join_count: int = 0,
) -> None:
    redis = get_redis()
    payload = {
        "status": "ok" if parse_ok else "failed",
        "app_name": app_name,
        "schema_name": schema_name,
        "join_count": join_count,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    await redis.set(_key(row_id), json.dumps(payload, ensure_ascii=False))
    logger.info(
        "SQL 表关联已标记处理: row_id=%s, parse_ok=%s, join_count=%s",
        row_id,
        parse_ok,
        join_count,
    )


async def count_sql_join_extract_stats(*, batch_size: int = 500) -> SqlJoinExtractStatsResponse:
    redis = get_redis()
    prefix = _prefix()
    pattern = f"{prefix}:*"

    total = 0
    ok = 0
    failed = 0
    unknown = 0
    pending_keys: list[str] = []

    async def _consume(keys: list[str]) -> None:
        nonlocal total, ok, failed, unknown
        if not keys:
            return
        values = await redis.mget(keys)
        for raw in values:
            total += 1
            if not raw:
                unknown += 1
                continue
            try:
                payload = json.loads(raw)
                status = str(payload.get("status") or "").lower()
                if status == "ok":
                    ok += 1
                elif status == "failed":
                    failed += 1
                else:
                    unknown += 1
            except (json.JSONDecodeError, TypeError):
                unknown += 1

    logger.info("开始统计 SQL 表关联提取进度, pattern=%s", pattern)
    async for key in redis.scan_iter(match=pattern, count=batch_size):
        pending_keys.append(key)
        if len(pending_keys) >= batch_size:
            await _consume(pending_keys)
            pending_keys.clear()

    if pending_keys:
        await _consume(pending_keys)

    return SqlJoinExtractStatsResponse(
        prefix=prefix,
        total=total,
        parse_ok=ok,
        parse_failed=failed,
        unknown=unknown,
    )
