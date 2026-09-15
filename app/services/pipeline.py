"""
字段中文名补全流水线：

第一步 扫描 MySQL dim_prj_meta2_fields_daily
       （completion_type='0'）
第二步 用 graph_id 做图谱溯源（ArangoDB）
第三步 未命中 -> 向量候选 + LLM
第四步 回写日表（回填 completion_type）
       graph→1 / llm→2
       无关系→50 / 有关系无中文→51 / 溯源中文名=日表en_filed_name→52
       不写 Redis
"""
import logging
import time
from typing import Optional

import anyio

from app.config import get_settings
from app.schemas import (
    CompletionRequest,
    FieldMetadataResult,
    MissingField,
    ResultSource,
    ReviewReason,
    RunStats,
)
from app.services.completion_log import candidates_preview, field_ctx, result_summary
from app.services.concurrent_runner import LockedStats, resolve_concurrency, run_bounded
from app.services.graph_source import trace_via_graph
from app.services.llm_score import score_candidates_and_pick_best
from app.services.mysql_meta_source import fetch_missing_fields
from app.services.mysql_meta_store import persist_field_completion
from app.services.vector_match import find_candidates

logger = logging.getLogger("metadata_pipeline")


def _cn_eq_en(name_cn: Optional[str], name_en: Optional[str]) -> bool:
    """溯源命中中文名 vs 日表 en_filed_name（忽略大小写与首尾空白）。"""
    a = (name_cn or "").strip().lower()
    b = (name_en or "").strip().lower()
    return bool(a) and a == b


def _pending_source(has_relation: bool) -> ResultSource:
    """无关系→50；有关系但无可用中文→51。"""
    return ResultSource.RELATION_PENDING if has_relation else ResultSource.MANUAL_PENDING


def _pending_reason(has_relation: bool, fallback: ReviewReason) -> ReviewReason:
    if not has_relation:
        return ReviewReason.NO_RELATION
    if fallback == ReviewReason.NO_CANDIDATE:
        return ReviewReason.RELATION_NO_CN
    return fallback


async def _save_field(field: MissingField, result: FieldMetadataResult) -> None:
    try:
        logger.info(
            "[字段回写] 开始 %s | %s",
            field_ctx(field),
            result_summary(
                source=result.source,
                name_cn=result.name_cn,
                via_vertex=result.via_vertex,
                hops=result.hops,
                similarity=result.similarity,
                reason=result.reason,
                reasoning=result.reasoning,
            ),
        )
        await persist_field_completion(field.mysql_id, result)
    except Exception:
        logger.exception(
            "[字段回写] 失败 %s",
            field_ctx(field),
        )


async def _process_one_field(
    field: MissingField,
    max_depth: int,
    similarity_threshold: float,
    stats: LockedStats,
) -> None:
    settings = get_settings()
    t0 = time.perf_counter()
    logger.info(
        "[字段补全] 开始处理 %s | max_depth=%s, similarity_threshold=%s",
        field_ctx(field),
        max_depth,
        similarity_threshold,
    )

    # ---- 图谱溯源 ----
    logger.info("[字段补全] 步骤1/图谱溯源 起点 arango_id=%s", field.id)
    trace = await anyio.to_thread.run_sync(trace_via_graph, field.id, max_depth)
    logger.info(
        "[字段补全] 步骤1/图谱结果 %s | has_relation=%s, found=%s, "
        "trace_name_en=%r, daily_en=%r, name_cn=%r, via_vertex=%s, hops=%s",
        field_ctx(field),
        trace.has_relation,
        trace.found,
        trace.name_en,
        field.name_en,
        trace.name_cn,
        trace.via_vertex,
        trace.hops,
    )
    if trace.found:
        if _cn_eq_en(trace.name_cn, field.name_en):
            await _save_field(
                field,
                FieldMetadataResult(
                    field_id=field.id,
                    name_en=field.name_en,
                    name_cn=trace.name_cn,
                    source=ResultSource.CN_EQ_EN,
                    reason=ReviewReason.CN_EQ_EN,
                    via_vertex=trace.via_vertex,
                    hops=trace.hops,
                ),
            )
            await stats.add(manual_pending=1)
            logger.info(
                "[字段补全] 结束(溯源中文名=日表en_filed_name→52) %s | "
                "name_cn=%r, en_filed_name=%r | elapsed=%.3fs",
                field_ctx(field),
                trace.name_cn,
                field.name_en,
                time.perf_counter() - t0,
            )
            return
        await _save_field(
            field,
            FieldMetadataResult(
                field_id=field.id,
                name_en=field.name_en,
                name_cn=trace.name_cn,
                source=ResultSource.GRAPH,
                via_vertex=trace.via_vertex,
                hops=trace.hops,
            ),
        )
        await stats.add(graph_hit=1)
        logger.info(
            "[字段补全] 结束(图谱命中) %s | elapsed=%.3fs",
            field_ctx(field),
            time.perf_counter() - t0,
        )
        return

    # ---- 向量 + LLM ----
    logger.info(
        "[字段补全] 步骤2/向量候选 图谱未命中，拉取上游表字段 %s",
        field_ctx(field),
    )
    candidates = await find_candidates(field, max_depth)
    best_by_similarity = candidates[0] if candidates else None
    logger.info(
        "[字段补全] 步骤2/向量结果 %s | total=%s, top=%s",
        field_ctx(field),
        len(candidates),
        candidates_preview(candidates),
    )
    if best_by_similarity is None:
        await _save_field(
            field,
            FieldMetadataResult(
                field_id=field.id,
                name_en=field.name_en,
                source=_pending_source(trace.has_relation),
                reason=_pending_reason(trace.has_relation, ReviewReason.NO_CANDIDATE),
            ),
        )
        await stats.add(manual_pending=1, manual_pending_no_candidate=1)
        logger.info(
            "[字段补全] 结束(无候选→%s) %s | elapsed=%.3fs",
            "51" if trace.has_relation else "50",
            field_ctx(field),
            time.perf_counter() - t0,
        )
        return

    qualified = [
        c for c in candidates if c.similarity >= similarity_threshold
    ][: settings.llm_max_candidates]
    logger.info(
        "[字段补全] 步骤2/相似度筛选 %s | threshold=%s, qualified=%s/%s, qualified_top=%s",
        field_ctx(field),
        similarity_threshold,
        len(qualified),
        len(candidates),
        candidates_preview(qualified),
    )
    if not qualified:
        await _save_field(
            field,
            FieldMetadataResult(
                field_id=field.id,
                name_en=field.name_en,
                name_cn="",
                source=_pending_source(trace.has_relation),
                reason=_pending_reason(trace.has_relation, ReviewReason.SIMILARITY_LOW),
                via_vertex=best_by_similarity.id,
                similarity=best_by_similarity.similarity,
            ),
        )
        await stats.add(manual_pending=1, manual_pending_similarity_low=1)
        logger.info(
            "[字段补全] 结束(相似度不足→%s) %s | best_sim=%s, via=%s | elapsed=%.3fs",
            "51" if trace.has_relation else "50",
            field_ctx(field),
            best_by_similarity.similarity,
            best_by_similarity.id,
            time.perf_counter() - t0,
        )
        return

    logger.info(
        "[字段补全] 步骤3/LLM挑选 %s | candidate_count=%s",
        field_ctx(field),
        len(qualified),
    )
    best, pick = await score_candidates_and_pick_best(field, qualified)
    logger.info(
        "[字段补全] 步骤3/LLM结果 %s | choice=%s, reasoning=%r, best_id=%s, best_cn=%r",
        field_ctx(field),
        pick.choice_index,
        pick.reasoning,
        getattr(best, "id", None),
        getattr(best, "name_cn", None),
    )
    if best is None:
        await _save_field(
            field,
            FieldMetadataResult(
                field_id=field.id,
                name_en=field.name_en,
                name_cn="",
                source=_pending_source(trace.has_relation),
                reason=_pending_reason(trace.has_relation, ReviewReason.LLM_REJECTED),
                reasoning=pick.reasoning or None,
            ),
        )
        await stats.add(manual_pending=1, manual_pending_llm_rejected=1)
        logger.info(
            "[字段补全] 结束(LLM未选中→%s) %s | elapsed=%.3fs",
            "51" if trace.has_relation else "50",
            field_ctx(field),
            time.perf_counter() - t0,
        )
        return

    await _save_field(
        field,
        FieldMetadataResult(
            field_id=field.id,
            name_en=field.name_en,
            name_cn=best.name_cn,
            source=ResultSource.LLM,
            reasoning=pick.reasoning or None,
            via_vertex=best.id,
            similarity=best.similarity,
        ),
    )
    await stats.add(llm_matched=1)
    logger.info(
        "[字段补全] 结束(LLM命中) %s | name_cn=%r, via=%s, sim=%s | elapsed=%.3fs",
        field_ctx(field),
        best.name_cn,
        best.id,
        best.similarity,
        time.perf_counter() - t0,
    )


async def _handle_fields_page(
    fields: list[MissingField],
    max_depth: int,
    similarity_threshold: float,
    stats: LockedStats,
    concurrency: int = 1,
) -> None:
    async def _one(field: MissingField) -> None:
        try:
            await _process_one_field(
                field,
                max_depth,
                similarity_threshold,
                stats,
            )
        except Exception:
            logger.exception("[字段补全] 处理异常 %s", field_ctx(field))
            await stats.add(errors=1)

    await run_bounded(fields, concurrency, _one)


async def run_pipeline(request: CompletionRequest) -> RunStats:
    settings = get_settings()
    max_depth = request.max_trace_depth or settings.max_trace_depth
    similarity_threshold = request.similarity_threshold or settings.similarity_threshold
    concurrency = resolve_concurrency(request.concurrency, settings.pipeline_concurrency)
    page_size = request.limit

    logger.info(
        "[字段流水线] 启动 page_size=%s, process_all=%s, max_depth=%s, "
        "similarity_threshold=%s, concurrency=%s, field_collection=%s",
        page_size,
        request.process_all,
        max_depth,
        similarity_threshold,
        concurrency,
        settings.arango_field_collection,
    )

    stats = RunStats()
    locked = LockedStats(stats)
    after_id = 0
    page = 0
    t_all = time.perf_counter()

    while True:
        page += 1
        t_page = time.perf_counter()
        fields = await anyio.to_thread.run_sync(
            lambda aid=after_id: fetch_missing_fields(page_size, after_id=aid)
        )
        if not fields:
            logger.info(
                "[字段流水线] 分页结束 page=%s, after_id=%s（本页无待办）",
                page,
                after_id,
            )
            break

        preview = "; ".join(
            f"id={f.mysql_id}/{f.id}/{f.name_en}" for f in fields[:5]
        )
        logger.info(
            "[字段流水线] 第%s页 after_id=%s, count=%s, 样例=%s%s",
            page,
            after_id,
            len(fields),
            preview,
            " ..." if len(fields) > 5 else "",
        )
        stats.pages = page
        stats.total_fields += len(fields)
        await _handle_fields_page(
            fields,
            max_depth,
            similarity_threshold,
            locked,
            concurrency=concurrency,
        )
        logger.info(
            "[字段流水线] 第%s页完成 count=%s, elapsed=%.3fs, 累计stats=%s",
            page,
            len(fields),
            time.perf_counter() - t_page,
            stats.model_dump(),
        )

        page_ids = [f.mysql_id for f in fields if f.mysql_id is not None]
        if page_ids:
            after_id = max(page_ids)
        if not request.process_all or len(fields) < page_size:
            break

    logger.info(
        "[字段流水线] 全部完成 elapsed=%.3fs, stats=%s",
        time.perf_counter() - t_all,
        stats.model_dump(),
    )
    return stats
