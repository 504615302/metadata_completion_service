"""
从 SQL 文本提取表关联关系（主表 / 从表 / 关联表达式）。
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from app.clients.llm import extract_sql_joins_with_llm
from app.schemas import (
    InsertParseSource,
    SqlJoinExtractItem,
    SqlJoinRelation,
    SqlJoinTableSide,
    TableJoinRole,
)

logger = logging.getLogger(__name__)

# 中间层/短别名启发式：用于告警；若 LLM 给了 alias_map 则优先用映射展开
_LIKELY_TEMP_NAME = re.compile(
    r"^(tmp|temp|cte|t|sub|sq|deriv|mid|stage)\d*$",
    re.IGNORECASE,
)
_LIKELY_SHORT_ALIAS = re.compile(r"^[a-z][a-z0-9_]{0,3}$", re.IGNORECASE)


def _as_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple)):
        out: list[str] = []
        for x in value:
            s = str(x).strip()
            if s:
                out.append(s)
        return out
    return []


def _normalize_alias_map(raw: Any) -> dict[str, str]:
    """LLM alias_map → {alias_lower: physical_table_ref}。"""
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        alias = str(k or "").strip()
        phys = str(v or "").strip()
        if not alias or not phys:
            continue
        out[alias.lower()] = phys
    return out


def _resolve_side_via_alias_map(
    side: SqlJoinTableSide,
    alias_map: dict[str, str],
    *,
    fallback_schema: Optional[str] = None,
) -> SqlJoinTableSide:
    """若 table_name 仍是别名，用 alias_map 展开为物理表。"""
    if not alias_map:
        return side
    key = side.table_name.strip().lower()
    # 已是 schema.table 时一般不再是别名
    if side.schema_name:
        qual = f"{side.schema_name}.{side.table_name}".lower()
        if qual in alias_map:
            key = qual
        elif key not in alias_map:
            return side
    elif key not in alias_map:
        return side

    phys = alias_map.get(key) or alias_map.get(side.table_name.strip().lower())
    if not phys:
        return side
    schema, table = _split_schema_table(
        schema_name=None,
        table_name=phys,
        fallback_schema=fallback_schema or side.schema_name,
    )
    if not table or table.lower() == side.table_name.lower():
        return side
    logger.info(
        "JOIN 侧别名已展开: %s → %s",
        _qualified_name(side),
        f"{schema + '.' if schema else ''}{table}",
    )
    return SqlJoinTableSide(
        role=side.role,
        schema_name=schema,
        table_name=table,
        join_fields=list(side.join_fields),
    )


def _rewrite_join_expression(
    expr: str,
    *,
    old_primary: SqlJoinTableSide,
    new_primary: SqlJoinTableSide,
    old_secondary: SqlJoinTableSide,
    new_secondary: SqlJoinTableSide,
) -> str:
    text = expr or ""
    replacements = [
        (_qualified_name(old_primary), _qualified_name(new_primary)),
        (_qualified_name(old_secondary), _qualified_name(new_secondary)),
        (old_primary.table_name, _qualified_name(new_primary)),
        (old_secondary.table_name, _qualified_name(new_secondary)),
    ]
    # 长串优先，避免短别名误伤
    replacements.sort(key=lambda x: len(x[0]), reverse=True)
    for old, new in replacements:
        if not old or old == new:
            continue
        text = re.sub(rf"(?i)\b{re.escape(old)}\b", new, text)
    return text


def _split_schema_table(
    *,
    schema_name: Optional[str],
    table_name: Optional[str],
    fallback_schema: Optional[str] = None,
) -> tuple[Optional[str], str]:
    """规范化 schema / table；若 table 写成 schema.table 则拆开。"""
    schema = (schema_name or "").strip() or None
    table = (table_name or "").strip()
    if not table:
        return schema or fallback_schema, ""

    if "." in table and not schema:
        parts = table.split(".", 1)
        if len(parts) == 2 and parts[0].strip() and parts[1].strip():
            return parts[0].strip(), parts[1].strip()

    if not schema and fallback_schema:
        schema = fallback_schema.strip() or None
    return schema, table


def _side_from_dict(
    data: Any,
    *,
    expected_role: TableJoinRole,
    fallback_schema: Optional[str] = None,
) -> Optional[SqlJoinTableSide]:
    if not isinstance(data, dict):
        return None
    schema, table = _split_schema_table(
        schema_name=data.get("schema_name"),
        table_name=data.get("table_name") or data.get("table"),
        fallback_schema=fallback_schema,
    )
    if not table:
        return None
    return SqlJoinTableSide(
        role=expected_role,
        schema_name=schema,
        table_name=table,
        join_fields=_as_str_list(data.get("join_fields") or data.get("fields")),
    )


def _qualified_name(side: SqlJoinTableSide) -> str:
    if side.schema_name:
        return f"{side.schema_name}.{side.table_name}"
    return side.table_name


def _join_dedupe_key(rel: SqlJoinRelation) -> tuple:
    """稳定去重键：表对无向 + 字段集合 + join_type。"""
    left = _qualified_name(rel.primary).lower()
    right = _qualified_name(rel.secondary).lower()
    pair = tuple(sorted((left, right)))
    field_pairs: list[str] = []
    for i, pf in enumerate(rel.primary.join_fields):
        sf = (
            rel.secondary.join_fields[i]
            if i < len(rel.secondary.join_fields)
            else (rel.secondary.join_fields[0] if rel.secondary.join_fields else pf)
        )
        a, b = sorted((pf.strip().lower(), sf.strip().lower()))
        field_pairs.append(f"{a}={b}")
    fields = tuple(sorted(set(field_pairs)))
    jtype = (rel.join_type or "").strip().lower()
    return pair + (fields, jtype)


def _sort_key(rel: SqlJoinRelation) -> tuple:
    return (
        _qualified_name(rel.primary).lower(),
        _qualified_name(rel.secondary).lower(),
        (rel.join_type or "").lower(),
        ",".join(rel.primary.join_fields).lower(),
        ",".join(rel.secondary.join_fields).lower(),
        (rel.join_expression or "").lower(),
    )


def _stabilize_joins(joins: list[SqlJoinRelation]) -> list[SqlJoinRelation]:
    """去重 + 稳定排序，保证同输入落库/回显顺序一致。"""
    best: dict[tuple, SqlJoinRelation] = {}
    for rel in joins:
        key = _join_dedupe_key(rel)
        prev = best.get(key)
        if prev is None or len(rel.join_expression or "") > len(prev.join_expression or ""):
            best[key] = rel
    return sorted(best.values(), key=_sort_key)


def _warn_likely_temp_sides(joins: list[SqlJoinRelation]) -> None:
    suspects: list[str] = []
    for rel in joins:
        for side in (rel.primary, rel.secondary):
            name = side.table_name
            if (
                _LIKELY_TEMP_NAME.match(name)
                or name.lower().startswith(("tmp_", "temp_", "cte_"))
                or (_LIKELY_SHORT_ALIAS.match(name) and not side.schema_name)
            ):
                suspects.append(_qualified_name(side))
    if suspects:
        uniq = sorted(set(suspects))
        logger.warning(
            "JOIN 提取结果疑似仍含别名/中间层表名（未完全展开到物理表）: %s",
            uniq[:20],
        )


def _item_from_llm_dict(data: dict, sql: str, *, fallback_schema: Optional[str] = None) -> SqlJoinExtractItem:
    alias_map = _normalize_alias_map(data.get("alias_map"))
    joins_raw = data.get("joins") or []
    joins: list[SqlJoinRelation] = []
    for raw in joins_raw:
        if not isinstance(raw, dict):
            continue
        primary = _side_from_dict(
            raw.get("primary") or raw.get("main_table") or raw.get("主表"),
            expected_role=TableJoinRole.PRIMARY,
            fallback_schema=fallback_schema,
        )
        secondary = _side_from_dict(
            raw.get("secondary") or raw.get("join_table") or raw.get("从表"),
            expected_role=TableJoinRole.SECONDARY,
            fallback_schema=fallback_schema,
        )
        if primary is None or secondary is None:
            continue

        old_primary, old_secondary = primary, secondary
        primary = _resolve_side_via_alias_map(
            primary, alias_map, fallback_schema=fallback_schema
        )
        secondary = _resolve_side_via_alias_map(
            secondary, alias_map, fallback_schema=fallback_schema
        )

        # 同表自关联跳过（展开错误或无效边）
        if _qualified_name(primary).lower() == _qualified_name(secondary).lower():
            continue
        expr = str(raw.get("join_expression") or raw.get("expression") or "").strip()
        join_type = (
            str(raw.get("join_type")).strip().lower() if raw.get("join_type") else None
        )
        if expr and (
            old_primary.table_name != primary.table_name
            or old_secondary.table_name != secondary.table_name
            or old_primary.schema_name != primary.schema_name
            or old_secondary.schema_name != secondary.schema_name
        ):
            expr = _rewrite_join_expression(
                expr,
                old_primary=old_primary,
                new_primary=primary,
                old_secondary=old_secondary,
                new_secondary=secondary,
            )
        if not expr:
            jt = join_type or "join"
            left = _qualified_name(primary)
            right = _qualified_name(secondary)
            on_parts: list[str] = []
            for i, pf in enumerate(primary.join_fields):
                sf = secondary.join_fields[i] if i < len(secondary.join_fields) else (
                    secondary.join_fields[0] if secondary.join_fields else pf
                )
                on_parts.append(f"{left}.{pf}={right}.{sf}")
            on_clause = " and ".join(on_parts) if on_parts else ""
            expr = f"{left} {jt} join {right}" + (f" on {on_clause}" if on_clause else "")

        joins.append(
            SqlJoinRelation(
                primary=primary,
                secondary=secondary,
                join_type=join_type,
                join_expression=expr,
            )
        )

    joins = _stabilize_joins(joins)
    _warn_likely_temp_sides(joins)

    llm_ok_flag = data.get("parse_ok")
    if llm_ok_flag is None:
        parse_ok = True
    else:
        parse_ok = bool(llm_ok_flag)

    llm_err = data.get("parse_error")
    # 无 JOIN 也算成功；仅当模型明确失败或关键字段非法时失败
    if not parse_ok and not llm_err:
        llm_err = "大模型未能解析出有效的表关联信息"

    return SqlJoinExtractItem(
        parse_ok=parse_ok,
        parse_error=f"LLM: {llm_err}" if llm_err and not parse_ok else None,
        parse_source=InsertParseSource.LLM if parse_ok else None,
        joins=joins if parse_ok else [],
        raw_sql=sql,
    )


async def extract_sql_joins(
    sql: str,
    *,
    schema_name: Optional[str] = None,
    app_name: Optional[str] = None,
) -> SqlJoinExtractItem:
    text = (sql or "").strip()
    if not text:
        return SqlJoinExtractItem(parse_ok=False, parse_error="SQL 为空", raw_sql=sql or "")

    logger.info("使用大模型提取 SQL 表关联, sql_length=%s", len(text))
    try:
        data = await extract_sql_joins_with_llm(
            text,
            schema_name=schema_name,
            app_name=app_name,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("SQL 表关联提取失败")
        return SqlJoinExtractItem(
            parse_ok=False,
            parse_error=f"LLM 调用失败: {exc}",
            raw_sql=text,
        )

    return _item_from_llm_dict(data, text, fallback_schema=schema_name)
