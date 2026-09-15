"""
第四步："将溯源到的中文名，大模型溯源到的中文名，以及未能匹配到需要人工溯源的保存至REDIS中"。

字段与表两种补全结果统一写入同一 Redis 前缀目录（默认 meta-completion:），
均使用 ArangoDB 顶点 _id 作为 key 后缀（field/xxx、table/xxx）；MANUAL_PENDING 时额外带上 reason。

Key 格式：{prefix}{entity_id}，例如 meta-completion:field/123 或 meta-completion:table/456。
entity_id 已存在则跳过，不覆盖已有结果。
"""
import logging

from app.config import get_settings
from app.db import get_redis
from app.schemas import FieldMetadataResult, RedisResultQueryResponse, TableMetadataResult

logger = logging.getLogger("metadata_pipeline.result_store")


def _key(entity_id: str) -> str:
    settings = get_settings()
    return f"{settings.redis_result_key_prefix}{entity_id}"


async def save_result(result: FieldMetadataResult, *, overwrite: bool = False) -> bool:
    """写入字段补全结果到 Redis。overwrite=False 时 key 已存在则跳过；返回 True 表示本次已写入。"""
    return await _save_payload(result.field_id, result.model_dump_json(), overwrite=overwrite)


async def save_table_result(result: TableMetadataResult, *, overwrite: bool = False) -> bool:
    """写入表补全结果到 Redis。overwrite=False 时 key 已存在则跳过；返回 True 表示本次已写入。"""
    return await _save_payload(result.table_id, result.model_dump_json(), overwrite=overwrite)


async def _save_payload(entity_id: str, payload: str, *, overwrite: bool = False) -> bool:
    redis = get_redis()
    key = _key(entity_id)
    if overwrite:
        await redis.set(key, payload)
        logger.info("Redis 写入成功(覆盖): entity_id=%s", entity_id)
        return True
    saved = await redis.set(key, payload, nx=True)
    if saved:
        logger.info("Redis 写入成功: entity_id=%s", entity_id)
    else:
        logger.info("Redis key 已存在，跳过写入: entity_id=%s", entity_id)
    return bool(saved)


async def get_table_result(table_id: str) -> TableMetadataResult | None:
    import json

    redis = get_redis()
    raw = await redis.get(_key(table_id))
    return TableMetadataResult(**json.loads(raw)) if raw else None


async def exists_result(entity_id: str) -> bool:
    """该实体是否已在 Redis 中存在处理结果（即已经处理过）。"""
    redis = get_redis()
    return bool(await redis.exists(_key(entity_id)))


async def clear_results_by_prefix(prefix: str, *, batch_size: int = 500) -> tuple[str, int]:
    """
    删除指定前缀下的全部 Redis key，返回 (规范化后的 prefix, deleted_count)。
    禁止空前缀或通配过宽（如仅 *），避免误删整库。
    """
    normalized = (prefix or "").strip()
    if not normalized:
        raise ValueError("prefix 不能为空")
    if normalized in {"*", "?"} or set(normalized) <= {"*", "?"}:
        raise ValueError("prefix 不能仅为通配符，请传入具体前缀")

    redis = get_redis()
    pattern = f"{normalized}*" if not normalized.endswith("*") else normalized
    # 用于返回展示的前缀（去掉末尾扫描用的 *）
    display_prefix = normalized.rstrip("*")
    pending: list[str] = []
    deleted = 0

    logger.info("开始按前缀删除 Redis key, prefix=%s, pattern=%s", display_prefix, pattern)
    async for key in redis.scan_iter(match=pattern, count=batch_size):
        pending.append(key)
        if len(pending) >= batch_size:
            deleted += int(await redis.delete(*pending))
            pending.clear()

    if pending:
        deleted += int(await redis.delete(*pending))

    logger.info("按前缀删除完成, prefix=%s, deleted=%s", display_prefix, deleted)
    return display_prefix, deleted


async def clear_all_results(*, batch_size: int = 500) -> tuple[str, int]:
    """删除配置项 redis_result_key_prefix 前缀下的全部 key。"""
    settings = get_settings()
    return await clear_results_by_prefix(settings.redis_result_key_prefix, batch_size=batch_size)


def _normalize_record(payload: dict) -> dict | None:
    entity_id = payload.get("field_id") or payload.get("table_id")
    if not entity_id:
        return None
    entity_kind = entity_id.split("/", 1)[0] if "/" in entity_id else "unknown"
    return {
        "entity_id": entity_id,
        "entity_kind": entity_kind,
        **payload,
    }


def _has_name_cn(payload: dict) -> bool:
    name_cn = payload.get("name_cn")
    return name_cn is not None and str(name_cn).strip() != ""


async def list_results_by_source(
    source: str,
    limit: int = 100,
    *,
    name_cn_not_empty: bool = False,
    batch_size: int = 500,
) -> RedisResultQueryResponse:
    """扫描 Redis，返回指定 source 的前 limit 条补全结果。"""
    import json

    settings = get_settings()
    prefix = settings.redis_result_key_prefix
    redis = get_redis()
    pattern = f"{prefix}*"
    target_source = source.strip().lower()
    results: list[dict] = []
    pending_keys: list[str] = []

    async def _consume_batch(keys: list[str]) -> bool:
        if not keys or len(results) >= limit:
            return len(results) >= limit
        values = await redis.mget(keys)
        for raw in values:
            if len(results) >= limit:
                return True
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if str(payload.get("source", "")).lower() != target_source:
                continue
            if name_cn_not_empty and not _has_name_cn(payload):
                continue
            record = _normalize_record(payload)
            if record is not None:
                results.append(record)
        return len(results) >= limit

    logger.info(
        "按 source 查询 Redis 补全结果: source=%s, limit=%s, name_cn_not_empty=%s, prefix=%s",
        target_source,
        limit,
        name_cn_not_empty,
        prefix,
    )
    async for key in redis.scan_iter(match=pattern, count=batch_size):
        pending_keys.append(key)
        if len(pending_keys) >= batch_size:
            if await _consume_batch(pending_keys):
                break
            pending_keys.clear()
    if len(results) < limit and pending_keys:
        await _consume_batch(pending_keys)

    logger.info("按 source 查询完成: source=%s, returned=%s", target_source, len(results))
    return RedisResultQueryResponse(
        prefix=prefix,
        source=target_source,
        limit=limit,
        name_cn_not_empty=name_cn_not_empty,
        returned=len(results),
        results=results,
    )
