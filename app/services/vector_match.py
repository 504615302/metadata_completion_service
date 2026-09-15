"""
第三步：向量匹配候选池仅来自上游表（table_build_table INBOUND 血缘方向）。
上游表/其下属字段向量化后，与缺失实体做余弦相似度；LLM 评分基于该候选池。
"""
import logging
import math
import re

import anyio

from app.clients.embeddings import embed_batch
from app.services.arango_aql import execute_aql
from app.config import get_settings
from app.services.completion_log import candidates_preview
from app.services.lineage_aql import related_upstream_fields_aql, related_upstream_tables_aql
from app.schemas import CandidateField, CandidateTable, MissingField, MissingTable

logger = logging.getLogger("metadata_pipeline.vector_match")


def _normalize_identifier(value: str | None) -> str:
    """把 snake/camel/kebab 标识转换成更适合语义向量模型的文本。"""
    if not value:
        return "unknown"
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    value = re.sub(r"[_\-.\/]+", " ", value)
    return " ".join(value.lower().split())


def _field_embedding_text(name_en: str, table_name_en: str | None) -> str:
    return (
        f"field name: {_normalize_identifier(name_en)}; "
        f"table name: {_normalize_identifier(table_name_en)}"
    )


def _table_embedding_text(name_en: str) -> str:
    return f"table name: {_normalize_identifier(name_en)}"


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _fetch_related_fields(field: MissingField, max_depth: int) -> list[dict]:
    settings = get_settings()
    start = (field.id or "").strip()
    if start and "/" not in start:
        start = f"{settings.arango_field_collection}/{start}"
    logger.info(
        "[字段向量] 拉上游字段 start_vertex=%s, max_depth=%s, limit=%s, "
        "edges=table_contain_field+%s",
        start,
        max_depth,
        settings.max_vector_candidates,
        settings.arango_table_build_table,
    )
    rows = execute_aql(
        related_upstream_fields_aql(),
        bind_vars={
            "start_vertex": start,
            "max_depth": max_depth,
            "@table_contain_field": settings.arango_table_contain_field,
            "@table_build_table": settings.arango_table_build_table,
            "table_collection": settings.arango_table_collection,
            "name_cn_attr": settings.entity_name_cn_attr,
            "name_attr": settings.entity_name_attr,
            "object_key_attr": settings.table_object_key_attr,
            "limit": settings.max_vector_candidates,
        },
    )
    logger.info("[字段向量] 上游字段原始行数=%s, start_vertex=%s", len(rows), start)
    if rows:
        sample = rows[:3]
        logger.info(
            "[字段向量] 上游样例=%s",
            [
                {
                    "id": r.get("id"),
                    "name_en": r.get("name_en"),
                    "name_cn": r.get("name_cn"),
                    "table": r.get("table_name_en"),
                }
                for r in sample
            ],
        )
    return rows


def _fetch_related_tables(table: MissingTable, max_depth: int) -> list[dict]:
    settings = get_settings()
    start = (table.id or "").strip()
    if start and "/" not in start:
        start = f"{settings.arango_table_collection}/{start}"
    logger.info(
        "[表向量] 拉上游表 start_vertex=%s, max_depth=%s, limit=%s, edge=%s",
        start,
        max_depth,
        settings.max_vector_candidates,
        settings.arango_table_build_table,
    )
    rows = execute_aql(
        related_upstream_tables_aql(),
        bind_vars={
            "start_vertex": start,
            "max_depth": max_depth,
            "@table_build_table": settings.arango_table_build_table,
            "table_collection": settings.arango_table_collection,
            "name_cn_attr": settings.entity_name_cn_attr,
            "name_attr": settings.entity_name_attr,
            "limit": settings.max_vector_candidates,
        },
    )
    logger.info("[表向量] 上游表原始行数=%s, start_vertex=%s", len(rows), start)
    if rows:
        logger.info(
            "[表向量] 上游样例=%s",
            [
                {"id": r.get("id"), "name_en": r.get("name_en"), "name_cn": r.get("name_cn")}
                for r in rows[:3]
            ],
        )
    return rows


async def find_candidates(field: MissingField, max_depth: int | None = None) -> list[CandidateField]:
    settings = get_settings()
    depth = max_depth or settings.max_trace_depth
    query_text = _field_embedding_text(field.name_en, field.table_name_en)
    logger.info(
        "[字段向量] 开始 field_id=%s, name_en=%s, table_en=%s, embed_text=%r, max_depth=%s",
        field.id,
        field.name_en,
        field.table_name_en,
        query_text,
        depth,
    )
    related = await anyio.to_thread.run_sync(_fetch_related_fields, field, depth)

    if not related:
        logger.info("[字段向量] 候选为空 field_id=%s", field.id)
        return []

    texts = [query_text] + [
        _field_embedding_text(r["name_en"], r.get("table_name_en")) for r in related
    ]
    logger.info("[字段向量] 开始 embedding batch_size=%s", len(texts))
    vectors = await embed_batch(texts)
    target_vec, candidate_vecs = vectors[0], vectors[1:]
    candidates = [
        CandidateField(
            id=r["id"],
            name_en=r["name_en"],
            name_cn=r["name_cn"],
            similarity=_cosine_similarity(target_vec, vec),
            table_id=r.get("table_id"),
            table_name_en=r.get("table_name_en"),
            table_name_cn=r.get("table_name_cn"),
        )
        for r, vec in zip(related, candidate_vecs)
    ]
    candidates.sort(key=lambda c: c.similarity, reverse=True)
    logger.info(
        "[字段向量] 完成 field_id=%s, count=%s, top=%s",
        field.id,
        len(candidates),
        candidates_preview(candidates),
    )
    return candidates


async def find_table_candidates(table: MissingTable, max_depth: int | None = None) -> list[CandidateTable]:
    settings = get_settings()
    depth = max_depth or settings.max_trace_depth
    query_text = _table_embedding_text(table.name_en)
    logger.info(
        "[表向量] 开始 table_id=%s, name_en=%s, embed_text=%r, max_depth=%s",
        table.id,
        table.name_en,
        query_text,
        depth,
    )
    related = await anyio.to_thread.run_sync(_fetch_related_tables, table, depth)

    if not related:
        logger.info("[表向量] 候选为空 table_id=%s", table.id)
        return []

    texts = [query_text] + [_table_embedding_text(r["name_en"]) for r in related]
    logger.info("[表向量] 开始 embedding batch_size=%s", len(texts))
    vectors = await embed_batch(texts)
    target_vec, candidate_vecs = vectors[0], vectors[1:]
    candidates = [
        CandidateTable(
            id=r["id"],
            name_en=r["name_en"],
            name_cn=r["name_cn"],
            similarity=_cosine_similarity(target_vec, vec),
        )
        for r, vec in zip(related, candidate_vecs)
    ]
    candidates.sort(key=lambda c: c.similarity, reverse=True)
    logger.info(
        "[表向量] 完成 table_id=%s, count=%s, top=%s",
        table.id,
        len(candidates),
        candidates_preview(candidates),
    )
    return candidates
