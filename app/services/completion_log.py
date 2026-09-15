"""补全排查用日志格式化。"""
from __future__ import annotations

from typing import Any, Optional, Sequence


def field_ctx(field: Any) -> str:
    return (
        f"mysql_id={getattr(field, 'mysql_id', None)}, "
        f"arango_id={getattr(field, 'id', None)}, "
        f"key={getattr(field, 'key', None)}, "
        f"name_en={getattr(field, 'name_en', None)}, "
        f"schema={getattr(field, 'schema_name', None)}, "
        f"table_en={getattr(field, 'table_name_en', None)}"
    )


def table_ctx(table: Any) -> str:
    return (
        f"mysql_id={getattr(table, 'mysql_id', None)}, "
        f"arango_id={getattr(table, 'id', None)}, "
        f"key={getattr(table, 'key', None)}, "
        f"name_en={getattr(table, 'name_en', None)}, "
        f"schema={getattr(table, 'schema_name', None)}, "
        f"object_key={getattr(table, 'object_key', None)}"
    )


def candidates_preview(candidates: Sequence[Any], *, limit: int = 5) -> str:
    if not candidates:
        return "[]"
    parts: list[str] = []
    for i, c in enumerate(candidates[:limit]):
        parts.append(
            f"[{i}] id={getattr(c, 'id', None)}, "
            f"en={getattr(c, 'name_en', None)}, "
            f"cn={getattr(c, 'name_cn', None)}, "
            f"sim={getattr(c, 'similarity', None)}, "
            f"table={getattr(c, 'table_name_en', None)}"
        )
    more = len(candidates) - limit
    if more > 0:
        parts.append(f"...(+{more})")
    return " | ".join(parts)


def result_summary(
    *,
    source: Any,
    name_cn: Optional[str] = None,
    via_vertex: Optional[str] = None,
    hops: Optional[int] = None,
    similarity: Optional[float] = None,
    reason: Any = None,
    reasoning: Optional[str] = None,
) -> str:
    src = getattr(source, "value", source)
    reason_v = getattr(reason, "value", reason) if reason is not None else None
    return (
        f"source={src}, name_cn={name_cn!r}, via_vertex={via_vertex}, "
        f"hops={hops}, similarity={similarity}, reason={reason_v}, "
        f"reasoning={reasoning!r}"
    )
