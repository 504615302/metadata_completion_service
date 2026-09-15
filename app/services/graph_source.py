"""
图谱数据访问：缺失实体拉取、字段/表溯源、表下字段查询。
字段溯源：仅沿 field_build_field 查找已有 name_cn 的关联字段。
表溯源：沿 table_build_table 找最近已有 name_cn 的上游表。
起点须为完整 Arango _id；若传入仅 _key（p123.s212...）会自动补 field/ 或 table/。
"""
from app.services.arango_aql import execute_aql
from app.config import get_settings
from app.services.analysis_layer import analysis_layer_bind_vars, analysis_layer_filter
from app.services.lineage_aql import trace_field_via_graph_aql, trace_table_upstream_aql
from app.schemas import CompletionTable, GraphTraceResult, MissingField, MissingTable
import logging
from pydantic import ValidationError
logger = logging.getLogger("metadata_pipeline.graph_source")


def _ensure_arango_id(vertex_id: str, *, collection: str) -> str:
    """p123.s212... → field/p123.s212...（或 table/...）。"""
    vid = (vertex_id or "").strip()
    if not vid:
        return vid
    if "/" in vid:
        _, key = vid.split("/", 1)
        return f"{collection}/{key.strip()}" if key.strip() else vid
    return f"{collection}/{vid}"


def _document_exists(vertex_id: str) -> bool:
    rows = execute_aql(
        "RETURN DOCUMENT(@id) != null",
        bind_vars={"id": vertex_id},
    )
    return bool(rows and rows[0] is True)


def _peek_start_doc(vertex_id: str) -> dict | None:
    """读取起点文档关键属性，便于排查 graph_id / name / name_cn。"""
    settings = get_settings()
    rows = execute_aql(
        """
        LET d = DOCUMENT(@id)
        RETURN d == null ? null : {
            _id: d._id,
            _key: d._key,
            name: d[@name_attr],
            name_cn: d[@name_cn_attr]
        }
        """,
        bind_vars={
            "id": vertex_id,
            "name_attr": settings.entity_name_attr,
            "name_cn_attr": settings.entity_name_cn_attr,
        },
    )
    if not rows:
        return None
    return rows[0]


def _table_analysis_filter(var: str = "t") -> str:
    settings = get_settings()
    if not settings.analysis_layer_filter_enabled:
        return ""
    return analysis_layer_filter(var)


def _missing_fields_aql() -> str:
    settings = get_settings()
    table_filter = _table_analysis_filter("t")

    # 分析层开启时：从 table 出发 OUTBOUND 找 field，避免先 LIMIT 再过滤导致查不到
    if settings.analysis_layer_filter_enabled and table_filter:
        return f"""
FOR pair IN (
    FOR t IN @@table_collection
        FILTER t[@name_attr] != null AND t[@name_attr] != ""
        {table_filter}
        FOR f IN 1..1 ANY t._id @@table_contain_field
            FILTER f[@name_cn_attr] == null OR f[@name_cn_attr] == ""
            FILTER f[@name_attr] != null AND f[@name_attr] != ""
            RETURN {{ field: f, table: t }}
)
SORT pair.field._key ASC
LIMIT @offset, @limit
RETURN {{
    key: pair.field._key,
    id: pair.field._id,
    name_en: pair.field[@name_attr],
    table_id: pair.table[@object_key_attr],
    table_vertex_id: pair.table._id,
    table_name_en: pair.table[@name_attr],
    table_name_cn: pair.table[@name_cn_attr]
}}
"""

    table_filter_optional = _table_analysis_filter("table")
    return f"""
FOR f IN @@field_collection
    FILTER f[@name_cn_attr] == null OR f[@name_cn_attr] == ""
    FILTER f[@name_attr] != null AND f[@name_attr] != ""
    LET table_in = FIRST(
        FOR v IN 1..1 INBOUND f._id @@table_contain_field
            RETURN v
    )
    LET table_out = FIRST(
        FOR v IN 1..1 OUTBOUND f._id @@table_contain_field
            RETURN v
    )
    LET table = table_in != null ? table_in : table_out
    FILTER table != null
    {table_filter_optional}
    SORT f._key ASC
    LIMIT @offset, @limit
    RETURN {{
        key: f._key,
        id: f._id,
        name_en: f[@name_attr],
        table_id: table[@object_key_attr],
        table_vertex_id: table._id,
        table_name_en: table[@name_attr],
        table_name_cn: table[@name_cn_attr]
    }}
"""


def _missing_tables_aql() -> str:
    return f"""
FOR t IN @@table_collection
    FILTER t[@name_cn_attr] == null OR t[@name_cn_attr] == ""
    FILTER t[@name_attr] != null AND t[@name_attr] != ""
    {_table_analysis_filter("t")}
    SORT t._key ASC
    LIMIT @offset, @limit
    RETURN {{
        key: t._key,
        id: t._id,
        object_key: t[@object_key_attr],
        name_en: t[@name_attr]
    }}
"""


def _all_tables_aql() -> str:
    """分页拉取全部有效表（含已有中文名的表）。"""
    return f"""
FOR t IN @@table_collection
    FILTER t[@name_attr] != null AND t[@name_attr] != ""
    {_table_analysis_filter("t")}
    SORT t._key ASC
    LIMIT @offset, @limit
    RETURN {{
        key: t._key,
        id: t._id,
        object_key: t[@object_key_attr],
        name_en: t[@name_attr],
        name_cn: t[@name_cn_attr]
    }}
"""


_MISSING_FIELDS_FOR_TABLE_AQL = """
FOR f IN OUTBOUND @table_id @@table_contain_field
    FILTER f[@name_cn_attr] == null OR f[@name_cn_attr] == ""
    FILTER f[@name_attr] != null AND f[@name_attr] != ""
    SORT f._key ASC
    LIMIT @limit
    RETURN {
        key: f._key,
        id: f._id,
        name_en: f[@name_attr],
        table_id: @table_object_key,
        table_vertex_id: @table_id,
        table_name_en: @table_name_en,
        table_name_cn: @table_name_cn
    }
"""


def _trace_table_aql() -> str:
    return trace_table_upstream_aql()


_TRACE_AQL = trace_field_via_graph_aql()


def _to_missing_field(row: dict) -> MissingField | None:
    try:
        return MissingField(**row)
    except ValidationError:
        logger.warning("跳过无效字段记录: row=%s", row)
        return None


def _to_missing_table(row: dict) -> MissingTable | None:
    try:
        return MissingTable(**row)
    except ValidationError:
        logger.warning("跳过无效表记录: row=%s", row)
        return None


def _to_completion_table(row: dict) -> CompletionTable | None:
    try:
        return CompletionTable(**row)
    except ValidationError:
        logger.warning("跳过无效待补全表记录: row=%s", row)
        return None

def _missing_fields_bind_vars(settings, offset: int, limit: int) -> dict:
    """按实际 AQL 分支只注入查询里声明过的 bind 参数，避免 ERR 1552。"""
    table_filter = _table_analysis_filter("t")
    base = {
        "@table_contain_field": settings.arango_table_contain_field,
        "name_cn_attr": settings.entity_name_cn_attr,
        "name_attr": settings.entity_name_attr,
        "object_key_attr": settings.table_object_key_attr,
        "offset": offset,
        "limit": limit,
    }
    if settings.analysis_layer_filter_enabled and table_filter:
        base["@table_collection"] = settings.arango_table_collection
        base.update(analysis_layer_bind_vars())
    else:
        base["@field_collection"] = settings.arango_field_collection
        if table_filter:
            base.update(analysis_layer_bind_vars())
    return base


def fetch_missing_fields(limit: int = 500, offset: int = 0) -> list[MissingField]:
    settings = get_settings()
    logger.info(
        "开始拉取缺失中文名的字段(分析层), offset=%s, limit=%s, prefixes=%s",
        offset,
        limit,
        settings.analysis_layer_prefixes_list() if settings.analysis_layer_filter_enabled else "disabled",
    )
    rows = execute_aql(
        _missing_fields_aql(),
        bind_vars=_missing_fields_bind_vars(settings, offset, limit),
        batch_size=max(limit, 1),
        ttl=600,
    )
    fields: list[MissingField] = []
    skipped = 0
    for row in rows:
        field = _to_missing_field(row)
        if field is not None:
            fields.append(field)
        else:
            skipped += 1
    if skipped:
        logger.warning("缺失中文名字段校验跳过 %s 条", skipped)
    logger.info("缺失中文名字段拉取完成, aql_rows=%s, count=%s", len(rows), len(fields))
    return fields


def fetch_missing_tables(limit: int = 500, offset: int = 0) -> list[MissingTable]:
    settings = get_settings()
    logger.info(
        "开始拉取缺失中文名的表(分析层), offset=%s, limit=%s, prefixes=%s",
        offset,
        limit,
        settings.analysis_layer_prefixes_list() if settings.analysis_layer_filter_enabled else "disabled",
    )
    rows = execute_aql(
        _missing_tables_aql(),
        bind_vars={
            "@table_collection": settings.arango_table_collection,
            "name_cn_attr": settings.entity_name_cn_attr,
            "name_attr": settings.entity_name_attr,
            "object_key_attr": settings.table_object_key_attr,
            "offset": offset,
            "limit": limit,
            **analysis_layer_bind_vars(),
        },
        batch_size=max(limit, 1),
        ttl=600,
    )
    tables: list[MissingTable] = []
    for row in rows:
        table = _to_missing_table(row)
        if table is not None:
            tables.append(table)
    logger.info("缺失中文名表拉取完成, count=%s", len(tables))
    return tables


def fetch_all_tables(limit: int = 500, offset: int = 0) -> list[CompletionTable]:
    """分页拉取全部有效表。"""
    settings = get_settings()
    logger.info(
        "开始拉取全部表(分析层), offset=%s, limit=%s, prefixes=%s",
        offset,
        limit,
        settings.analysis_layer_prefixes_list() if settings.analysis_layer_filter_enabled else "disabled",
    )
    rows = execute_aql(
        _all_tables_aql(),
        bind_vars={
            "@table_collection": settings.arango_table_collection,
            "name_cn_attr": settings.entity_name_cn_attr,
            "name_attr": settings.entity_name_attr,
            "object_key_attr": settings.table_object_key_attr,
            "offset": offset,
            "limit": limit,
            **analysis_layer_bind_vars(),
        },
        batch_size=max(limit, 1),
        ttl=600,
    )
    tables: list[CompletionTable] = []
    for row in rows:
        table = _to_completion_table(row)
        if table is not None:
            tables.append(table)
    logger.info("全部表拉取完成, count=%s", len(tables))
    return tables


def fetch_missing_fields_for_table(
    table_vertex_id: str,
    *,
    table_object_key: str | None = None,
    table_name_en: str | None = None,
    table_name_cn: str | None = None,
    limit: int = 5000,
) -> list[MissingField]:
    """拉取指定表下 name_cn 为空的字段。"""
    settings = get_settings()
    logger.info(
        "开始拉取表下缺失中文名字段: table_id=%s, limit=%s",
        table_vertex_id,
        limit,
    )
    rows = execute_aql(
        _MISSING_FIELDS_FOR_TABLE_AQL,
        bind_vars={
            "table_id": table_vertex_id,
            "@table_contain_field": settings.arango_table_contain_field,
            "name_cn_attr": settings.entity_name_cn_attr,
            "name_attr": settings.entity_name_attr,
            "table_object_key": table_object_key,
            "table_name_en": table_name_en,
            "table_name_cn": table_name_cn,
            "limit": limit,
        },
        batch_size=max(limit, 1),
        ttl=600,
    )
    fields: list[MissingField] = []
    skipped = 0
    for row in rows:
        field = _to_missing_field(row)
        if field is not None:
            fields.append(field)
        else:
            skipped += 1
    if skipped:
        logger.warning(
            "表下缺失中文名字段校验跳过 %s 条: table_id=%s",
            skipped,
            table_vertex_id,
        )
    logger.info(
        "表下缺失中文名字段拉取完成: table_id=%s, count=%s",
        table_vertex_id,
        len(fields),
    )
    return fields


def trace_via_graph(vertex_id: str, max_depth: int | None = None) -> GraphTraceResult:
    settings = get_settings()
    depth = max_depth or settings.max_trace_depth
    raw = (vertex_id or "").strip()
    start = _ensure_arango_id(raw, collection=settings.arango_field_collection)
    logger.info(
        "[字段图谱] 入参 raw=%r → start_vertex=%r, max_depth=%s, edge=%s, name_cn_attr=%s",
        raw,
        start,
        depth,
        settings.arango_field_build_field,
        settings.entity_name_cn_attr,
    )
    if not start:
        logger.warning("[字段图谱] 空起点，跳过")
        return GraphTraceResult(found=False, has_relation=False)

    doc = _peek_start_doc(start)
    if doc is None:
        logger.warning(
            "[字段图谱] DOCUMENT 不存在: start_vertex=%s "
            "（请核对日表 graph_id 是否等于 Arango field._key）",
            start,
        )
        return GraphTraceResult(found=False, has_relation=False)
    logger.info(
        "[字段图谱] 起点文档存在: _id=%s, _key=%s, name=%r, name_cn=%r",
        doc.get("_id"),
        doc.get("_key"),
        doc.get("name"),
        doc.get("name_cn"),
    )

    results = execute_aql(
        _TRACE_AQL,
        bind_vars={
            "start_vertex": start,
            "max_depth": depth,
            "@field_build_field": settings.arango_field_build_field,
            "field_collection": settings.arango_field_collection,
            "name_attr": settings.entity_name_attr,
            "name_cn_attr": settings.entity_name_cn_attr,
        },
    )
    logger.info("[字段图谱] AQL 返回行数=%s, start_vertex=%s", len(results), start)
    if not results:
        logger.info("[字段图谱] 未命中: start_vertex=%s", start)
        return GraphTraceResult(found=False, has_relation=False)
    hit = results[0] or {}
    has_relation = bool(hit.get("has_relation"))
    found = bool(hit.get("found"))
    logger.info(
        "[字段图谱] 结果: start=%s → has_relation=%s, found=%s, name_en=%r, via=%s, hops=%s, name_cn=%r",
        start,
        has_relation,
        found,
        hit.get("name_en"),
        hit.get("via_vertex"),
        hit.get("hops"),
        hit.get("name_cn"),
    )
    return GraphTraceResult(
        found=found,
        has_relation=has_relation,
        name_en=hit.get("name_en") if found else None,
        name_cn=hit.get("name_cn") if found else None,
        via_vertex=hit.get("via_vertex") if found else None,
        hops=hit.get("hops") if found else None,
    )


def trace_via_graph_for_table(table_id: str, max_depth: int | None = None) -> GraphTraceResult:
    """表级溯源：仅在 table 顶点中查找已有 name_cn 的最近邻。"""
    settings = get_settings()
    depth = max_depth or settings.max_trace_depth
    raw = (table_id or "").strip()
    start = _ensure_arango_id(raw, collection=settings.arango_table_collection)
    logger.info(
        "[表图谱] 入参 raw=%r → start_vertex=%r, max_depth=%s, edge=%s, name_cn_attr=%s",
        raw,
        start,
        depth,
        settings.arango_table_build_table,
        settings.entity_name_cn_attr,
    )
    if not start:
        logger.warning("[表图谱] 空起点，跳过")
        return GraphTraceResult(found=False, has_relation=False)

    doc = _peek_start_doc(start)
    if doc is None:
        logger.warning(
            "[表图谱] DOCUMENT 不存在: start_vertex=%s "
            "（请核对日表 graph_id 是否等于 Arango table._key）",
            start,
        )
        return GraphTraceResult(found=False, has_relation=False)
    logger.info(
        "[表图谱] 起点文档存在: _id=%s, _key=%s, name=%r, name_cn=%r",
        doc.get("_id"),
        doc.get("_key"),
        doc.get("name"),
        doc.get("name_cn"),
    )

    results = execute_aql(
        _trace_table_aql(),
        bind_vars={
            "start_vertex": start,
            "max_depth": depth,
            "@table_build_table": settings.arango_table_build_table,
            "table_collection": settings.arango_table_collection,
            "name_attr": settings.entity_name_attr,
            "name_cn_attr": settings.entity_name_cn_attr,
        },
    )
    logger.info("[表图谱] AQL 返回行数=%s, start_vertex=%s", len(results), start)
    if not results:
        logger.info("[表图谱] 未命中: start_vertex=%s", start)
        return GraphTraceResult(found=False, has_relation=False)
    hit = results[0] or {}
    has_relation = bool(hit.get("has_relation"))
    found = bool(hit.get("found"))
    logger.info(
        "[表图谱] 结果: start=%s → has_relation=%s, found=%s, name_en=%r, via=%s, hops=%s, name_cn=%r",
        start,
        has_relation,
        found,
        hit.get("name_en"),
        hit.get("via_vertex"),
        hit.get("hops"),
        hit.get("name_cn"),
    )
    return GraphTraceResult(
        found=found,
        has_relation=has_relation,
        name_en=hit.get("name_en") if found else None,
        name_cn=hit.get("name_cn") if found else None,
        via_vertex=hit.get("via_vertex") if found else None,
        hops=hit.get("hops") if found else None,
    )
