"""
表级元数据补全流水线：

第一步 扫描 MySQL dim_prj_meta2_tables_daily
       （completion_type='0'）
第二步 用 graph_id 做图谱溯源（ArangoDB）
第三步 未命中 -> 向量候选 + LLM
第四步 回写日表（回填 completion_type）
       graph→1 / llm→2
       无关系→50 / 有关系无中文→51 / 溯源中文名=日表table_name→52
       不写 Redis
"""
import logging
import time
from typing import Optional

import anyio

from app.config import get_settings
from app.schemas import (
    CompletionRequest,
    MissingTable,
    ResultSource,
    ReviewReason,
    TableMetadataResult,
    TableRunStats,
)
from app.services.completion_log import candidates_preview, result_summary, table_ctx
from app.services.concurrent_runner import LockedStats, resolve_concurrency, run_bounded
from app.services.graph_source import trace_via_graph_for_table
from app.services.llm_score import score_table_candidates_and_pick_best
from app.services.mysql_meta_source import fetch_missing_tables
from app.services.mysql_meta_store import persist_table_completion
from app.services.vector_match import find_table_candidates

logger = logging.getLogger("metadata_pipeline.table")


def _cn_eq_en(name_cn: Optional[str], name_en: Optional[str]) -> bool:
    """溯源命中中文名 vs 日表 table_name（忽略大小写与首尾空白）。"""
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


async def _save_table(table: MissingTable, result: TableMetadataResult) -> None:
    try:
        logger.info(
            "[表回写] 开始 %s | %s",
            table_ctx(table),
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
        await persist_table_completion(table.mysql_id, result)
    except Exception:
        logger.exception("[表回写] 失败 %s", table_ctx(table))


async def _process_one_table(
    table: MissingTable,
    max_depth: int,
    similarity_threshold: float,
    stats: LockedStats,
) -> None:
    settings = get_settings()
    t0 = time.perf_counter()
    logger.info(
        "[表补全] 开始处理 %s | max_depth=%s, similarity_threshold=%s",
        table_ctx(table),
        max_depth,
        similarity_threshold,
    )

    logger.info("[表补全] 步骤1/图谱溯源 起点 arango_id=%s", table.id)
    trace = await anyio.to_thread.run_sync(trace_via_graph_for_table, table.id, max_depth)
    logger.info(
        "[表补全] 步骤1/图谱结果 %s | has_relation=%s, found=%s, "
        "trace_name_en=%r, daily_en=%r, name_cn=%r, via_vertex=%s, hops=%s",
        table_ctx(table),
        trace.has_relation,
        trace.found,
        trace.name_en,
        table.name_en,
        trace.name_cn,
        trace.via_vertex,
        trace.hops,
    )
    if trace.found:
        if _cn_eq_en(trace.name_cn, table.name_en):
            await _save_table(
                table,
                TableMetadataResult(
                    table_id=table.id,
                    name_en=table.name_en,
                    name_cn=trace.name_cn,
                    source=ResultSource.CN_EQ_EN,
                    reason=ReviewReason.CN_EQ_EN,
                    via_vertex=trace.via_vertex,
                    hops=trace.hops,
                ),
            )
            await stats.add(manual_pending=1)
            logger.info(
                "[表补全] 结束(溯源中文名=日表table_name→52) %s | "
                "name_cn=%r, table_name=%r | elapsed=%.3fs",
                table_ctx(table),
                trace.name_cn,
                table.name_en,
                time.perf_counter() - t0,
            )
            return
        await _save_table(
            table,
            TableMetadataResult(
                table_id=table.id,
                name_en=table.name_en,
                name_cn=trace.name_cn,
                source=ResultSource.GRAPH,
                via_vertex=trace.via_vertex,
                hops=trace.hops,
            ),
        )
        await stats.add(graph_hit=1)
        logger.info(
            "[表补全] 结束(图谱命中) %s | elapsed=%.3fs",
            table_ctx(table),
            time.perf_counter() - t0,
        )
        return

    logger.info("[表补全] 步骤2/向量候选 图谱未命中 %s", table_ctx(table))
    candidates = await find_table_candidates(table, max_depth)
    best_by_similarity = candidates[0] if candidates else None
    logger.info(
        "[表补全] 步骤2/向量结果 %s | total=%s, top=%s",
        table_ctx(table),
        len(candidates),
        candidates_preview(candidates),
    )

    if best_by_similarity is None:
        await _save_table(
            table,
            TableMetadataResult(
                table_id=table.id,
                name_en=table.name_en,
                source=_pending_source(trace.has_relation),
                reason=_pending_reason(trace.has_relation, ReviewReason.NO_CANDIDATE),
            ),
        )
        await stats.add(manual_pending=1, manual_pending_no_candidate=1)
        logger.info(
            "[表补全] 结束(无候选→%s) %s | elapsed=%.3fs",
            "51" if trace.has_relation else "50",
            table_ctx(table),
            time.perf_counter() - t0,
        )
        return

    qualified = [
        c for c in candidates if c.similarity >= similarity_threshold
    ][: settings.llm_max_candidates]
    logger.info(
        "[表补全] 步骤2/相似度筛选 %s | threshold=%s, qualified=%s/%s, qualified_top=%s",
        table_ctx(table),
        similarity_threshold,
        len(qualified),
        len(candidates),
        candidates_preview(qualified),
    )
    if not qualified:
        await _save_table(
            table,
            TableMetadataResult(
                table_id=table.id,
                name_en=table.name_en,
                name_cn="",
                source=_pending_source(trace.has_relation),
                reason=_pending_reason(trace.has_relation, ReviewReason.SIMILARITY_LOW),
                via_vertex=best_by_similarity.id,
                similarity=best_by_similarity.similarity,
            ),
        )
        await stats.add(manual_pending=1, manual_pending_similarity_low=1)
        logger.info(
            "[表补全] 结束(相似度不足→%s) %s | best_sim=%s | elapsed=%.3fs",
            "51" if trace.has_relation else "50",
            table_ctx(table),
            best_by_similarity.similarity,
            time.perf_counter() - t0,
        )
        return

    logger.info(
        "[表补全] 步骤3/LLM挑选 %s | candidate_count=%s",
        table_ctx(table),
        len(qualified),
    )
    best, pick = await score_table_candidates_and_pick_best(table, qualified)
    logger.info(
        "[表补全] 步骤3/LLM结果 %s | choice=%s, reasoning=%r, best_id=%s, best_cn=%r",
        table_ctx(table),
        pick.choice_index,
        pick.reasoning,
        getattr(best, "id", None),
        getattr(best, "name_cn", None),
    )
    if best is None:
        await _save_table(
            table,
            TableMetadataResult(
                table_id=table.id,
                name_en=table.name_en,
                name_cn="",
                source=_pending_source(trace.has_relation),
                reason=_pending_reason(trace.has_relation, ReviewReason.LLM_REJECTED),
                reasoning=pick.reasoning or None,
            ),
        )
        await stats.add(manual_pending=1, manual_pending_llm_rejected=1)
        logger.info(
            "[表补全] 结束(LLM未选中→%s) %s | elapsed=%.3fs",
            "51" if trace.has_relation else "50",
            table_ctx(table),
            time.perf_counter() - t0,
        )
        return

    await _save_table(
        table,
        TableMetadataResult(
            table_id=table.id,
            name_en=table.name_en,
            name_cn=best.name_cn,
            source=ResultSource.LLM,
            reasoning=pick.reasoning or None,
            via_vertex=best.id,
            similarity=best.similarity,
        ),
    )
    await stats.add(llm_matched=1)
    logger.info(
        "[表补全] 结束(LLM命中) %s | name_cn=%r, via=%s, sim=%s | elapsed=%.3fs",
        table_ctx(table),
        best.name_cn,
        best.id,
        best.similarity,
        time.perf_counter() - t0,
    )


async def _handle_tables_page(
    tables: list[MissingTable],
    max_depth: int,
    similarity_threshold: float,
    stats: LockedStats,
    concurrency: int = 1,
) -> None:
    async def _one(table: MissingTable) -> None:
        try:
            await _process_one_table(
                table,
                max_depth,
                similarity_threshold,
                stats,
            )
        except Exception:
            logger.exception("[表补全] 处理异常 %s", table_ctx(table))
            await stats.add(errors=1)

    await run_bounded(tables, concurrency, _one)


async def run_table_pipeline(request: CompletionRequest) -> TableRunStats:
    settings = get_settings()
    max_depth = request.max_trace_depth or settings.max_trace_depth
    similarity_threshold = request.similarity_threshold or settings.similarity_threshold
    concurrency = resolve_concurrency(request.concurrency, settings.pipeline_concurrency)
    page_size = request.limit

    logger.info(
        "[表流水线] 启动 page_size=%s, process_all=%s, max_depth=%s, "
        "similarity_threshold=%s, concurrency=%s",
        page_size,
        request.process_all,
        max_depth,
        similarity_threshold,
        concurrency,
    )

    stats = TableRunStats()
    locked = LockedStats(stats)
    after_id = 0
    page = 0
    t_all = time.perf_counter()

    while True:
        page += 1
        t_page = time.perf_counter()
        tables = await anyio.to_thread.run_sync(
            lambda aid=after_id: fetch_missing_tables(page_size, after_id=aid)
        )
        if not tables:
            logger.info(
                "[表流水线] 分页结束 page=%s, after_id=%s（本页无待办）",
                page,
                after_id,
            )
            break

        preview = "; ".join(
            f"id={t.mysql_id}/{t.id}/{t.name_en}" for t in tables[:5]
        )
        logger.info(
            "[表流水线] 第%s页 after_id=%s, count=%s, 样例=%s%s",
            page,
            after_id,
            len(tables),
            preview,
            " ..." if len(tables) > 5 else "",
        )
        stats.pages = page
        stats.total_tables += len(tables)
        await _handle_tables_page(
            tables,
            max_depth,
            similarity_threshold,
            locked,
            concurrency=concurrency,
        )
        logger.info(
            "[表流水线] 第%s页完成 count=%s, elapsed=%.3fs, 累计stats=%s",
            page,
            len(tables),
            time.perf_counter() - t_page,
            stats.model_dump(),
        )

        page_ids = [t.mysql_id for t in tables if t.mysql_id is not None]
        if page_ids:
            after_id = max(page_ids)
        if not request.process_all or len(tables) < page_size:
            break

    logger.info(
        "[表流水线] 全部完成 elapsed=%.3fs, stats=%s",
        time.perf_counter() - t_all,
        stats.model_dump(),
    )
    return stats
