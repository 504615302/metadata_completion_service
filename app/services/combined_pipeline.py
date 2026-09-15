"""
合并补全：先补全全部待办表，再补全全部待办字段。

1. 跑表补全流水线（completion_type=0）
2. 再跑字段补全流水线（completion_type=0）
"""
import logging

from app.config import get_settings
from app.schemas import CombinedRunStats, CompletionRequest
from app.services.concurrent_runner import resolve_concurrency
from app.services.pipeline import run_pipeline
from app.services.table_pipeline import run_table_pipeline

logger = logging.getLogger("metadata_pipeline.combined")


async def run_combined_pipeline(request: CompletionRequest) -> CombinedRunStats:
    settings = get_settings()
    concurrency = resolve_concurrency(request.concurrency, settings.pipeline_concurrency)
    page_size = request.limit

    # 合并接口始终全量翻页
    req = request.model_copy(update={"process_all": True})

    logger.info(
        "合并补全启动: 先表后字段, page_size=%s, max_depth=%s, concurrency=%s",
        page_size,
        req.max_trace_depth or settings.max_trace_depth,
        concurrency,
    )

    logger.info("合并补全阶段1/2：表中文名补全")
    table_stats = await run_table_pipeline(req)

    logger.info("合并补全阶段2/2：字段中文名补全")
    field_stats = await run_pipeline(req)

    result = CombinedRunStats(
        pages=max(table_stats.pages, field_stats.pages),
        page_size=page_size,
        concurrency=concurrency,
        fields=field_stats,
        tables=table_stats,
    )
    logger.info("合并补全完成: %s", result.model_dump())
    return result
