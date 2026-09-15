"""
INSERT SQL 解析进度：用 Redis 记录已处理的源表行，便于全量跑数时跳过。

Key：{redis_record_key_prefix}:{row_id}
例如：metadata-completion:insert-sql:123
Value：简短 JSON（status / 时间等）
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from app.config import get_settings
from app.db import get_redis
from app.schemas import InsertSqlParseStatsResponse

logger = logging.getLogger(__name__)


def _prefix() -> str:
    settings = get_settings()
    return (settings.redis_record_key_prefix or "").rstrip(":")


def _key(row_id: int | str) -> str:
    return f"{_prefix()}:{row_id}"


async def is_insert_sql_processed(row_id: int | str) -> bool:
    redis = get_redis()
    return bool(await redis.exists(_key(row_id)))


async def get_insert_sql_progress_status(row_id: int | str) -> str | None:
    """
    读取 Redis 进度 status。
    返回：'ok' | 'failed' | 'unknown' | None（key 不存在）
    """
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


async def should_skip_insert_sql(
    row_id: int | str,
    *,
    skip_processed: bool,
    retry_failed: bool = False,
) -> bool:
    """
    是否跳过该行：
    - skip_processed=false：不跳过
    - skip_processed=true, retry_failed=false：Redis 有 key 就跳过
    - skip_processed=true, retry_failed=true：仅 status=ok 跳过；failed/unknown/无 key 重跑
    """
    if not skip_processed:
        return False
    status = await get_insert_sql_progress_status(row_id)
    if status is None:
        return False
    if retry_failed:
        return status == "ok"
    return True


async def mark_insert_sql_processed(
    row_id: int | str,
    *,
    parse_ok: bool,
    app_name: str | None = None,
    schema_name: str | None = None,
) -> None:
    redis = get_redis()
    payload = {
        "status": "ok" if parse_ok else "failed",
        "app_name": app_name,
        "schema_name": schema_name,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    # 成功/失败都记为已处理，避免失败行反复打满 LLM；需重跑可清 Redis 对应 key
    await redis.set(_key(row_id), json.dumps(payload, ensure_ascii=False))
    logger.info("INSERT SQL 已标记处理: row_id=%s, parse_ok=%s", row_id, parse_ok)


async def count_insert_sql_parse_stats(*, batch_size: int = 500) -> InsertSqlParseStatsResponse:
    """
    扫描 Redis 解析进度，统计总数 / 成功 / 失败。
    total = ok + failed + unknown（凡写入过进度的 key）
    """
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

    logger.info("开始统计 INSERT SQL 解析进度, pattern=%s", pattern)
    async for key in redis.scan_iter(match=pattern, count=batch_size):
        pending_keys.append(key)
        if len(pending_keys) >= batch_size:
            await _consume(pending_keys)
            pending_keys.clear()

    if pending_keys:
        await _consume(pending_keys)

    logger.info(
        "INSERT SQL 解析进度统计完成: total=%s, ok=%s, failed=%s, unknown=%s",
        total,
        ok,
        failed,
        unknown,
    )
    return InsertSqlParseStatsResponse(
        prefix=prefix,
        total=total,
        parse_ok=ok,
        parse_failed=failed,
        unknown=unknown,
    )
