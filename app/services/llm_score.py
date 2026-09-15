"""
LLM 多候选挑选服务层：一次传入候选池，由模型选出最合适的一条。
候选池为空或模型未选中时返回 (None, pick_result)。
"""
import logging

import httpx

from app.clients.llm import pick_best_field_candidate, pick_best_table_candidate
from app.schemas import CandidateField, CandidateTable, LLMPickResult, MissingField, MissingTable

logger = logging.getLogger("metadata_pipeline.llm_score")


async def score_candidates_and_pick_best(
    field: MissingField,
    candidates: list[CandidateField],
) -> tuple[CandidateField | None, LLMPickResult]:
    if not candidates:
        logger.info("字段候选池为空，跳过 LLM: field_id=%s", field.id)
        return None, LLMPickResult(choice_index=None, reasoning="候选池为空")

    logger.info(
        "字段 LLM 多候选挑选开始: field_id=%s, candidate_count=%s",
        field.id,
        len(candidates),
    )
    try:
        pick = await pick_best_field_candidate(
            name_en=field.name_en,
            table_name_en=field.table_name_en,
            table_name_cn=field.table_name_cn,
            candidates=candidates,
        )
    except httpx.HTTPError:
        logger.exception(
            "字段 LLM 多候选挑选调用失败: field_id=%s，转人工溯源",
            field.id,
        )
        return None, LLMPickResult(
            choice_index=None,
            reasoning="LLM服务调用失败，已转人工溯源",
        )

    if pick.choice_index is None:
        logger.info(
            "字段 LLM 未选中候选: field_id=%s, reasoning=%s",
            field.id,
            pick.reasoning,
        )
        return None, pick

    best = candidates[pick.choice_index]
    logger.info(
        "字段 LLM 多候选挑选完成: field_id=%s, choice=%s, via=%s",
        field.id,
        pick.choice_index,
        best.id,
    )
    return best, pick


async def score_table_candidates_and_pick_best(
    table: MissingTable,
    candidates: list[CandidateTable],
) -> tuple[CandidateTable | None, LLMPickResult]:
    if not candidates:
        logger.info("表候选池为空，跳过 LLM: table_id=%s", table.id)
        return None, LLMPickResult(choice_index=None, reasoning="候选池为空")

    logger.info(
        "表 LLM 多候选挑选开始: table_id=%s, candidate_count=%s",
        table.id,
        len(candidates),
    )
    try:
        pick = await pick_best_table_candidate(
            name_en=table.name_en,
            candidates=candidates,
        )
    except httpx.HTTPError:
        logger.exception(
            "表 LLM 多候选挑选调用失败: table_id=%s，转人工溯源",
            table.id,
        )
        return None, LLMPickResult(
            choice_index=None,
            reasoning="LLM服务调用失败，已转人工溯源",
        )

    if pick.choice_index is None:
        logger.info(
            "表 LLM 未选中候选: table_id=%s, reasoning=%s",
            table.id,
            pick.reasoning,
        )
        return None, pick

    best = candidates[pick.choice_index]
    logger.info(
        "表 LLM 多候选挑选完成: table_id=%s, choice=%s, via=%s",
        table.id,
        pick.choice_index,
        best.id,
    )
    return best, pick
