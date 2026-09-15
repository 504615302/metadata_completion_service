"""字段/表血缘查询 AQL。"""


def trace_table_upstream_aql() -> str:
    """表级图谱溯源：探测是否有关系，并找最近已有 name_cn 的表。

    返回：
      has_relation — max_depth 内是否存在关联表
      found / name_cn / via_vertex / hops — 是否命中带中文的关联表
    """
    return """
LET has_relation = LENGTH(
    FOR v IN 1..@max_depth ANY @start_vertex @@table_build_table
        FILTER IS_SAME_COLLECTION(@table_collection, v)
        FILTER v._id != @start_vertex
        LIMIT 1
        RETURN 1
) > 0
LET hit = FIRST(
    FOR v, e, p IN 1..@max_depth ANY @start_vertex @@table_build_table
        FILTER IS_SAME_COLLECTION(@table_collection, v)
        FILTER v[@name_cn_attr] != null AND v[@name_cn_attr] != ""
        FILTER v._id != @start_vertex
        SORT LENGTH(p.edges) ASC
        LIMIT 1
        RETURN {
            name_en: v[@name_attr],
            name_cn: v[@name_cn_attr],
            via_vertex: v._id,
            hops: LENGTH(p.edges)
        }
)
RETURN {
    has_relation: has_relation,
    found: hit != null,
    name_en: hit == null ? null : hit.name_en,
    name_cn: hit == null ? null : hit.name_cn,
    via_vertex: hit == null ? null : hit.via_vertex,
    hops: hit == null ? null : hit.hops
}
"""


def trace_field_via_graph_aql() -> str:
    """字段级图谱溯源：
    仅通过 field_build_field 查找关联字段；不要求英文名一致。
    INBOUND 上游方向；同时探测是否有关系、是否命中带中文的关联字段。
    """
    return """
LET has_relation = LENGTH(
    FOR f IN 1..@max_depth INBOUND @start_vertex @@field_build_field
        FILTER IS_SAME_COLLECTION(@field_collection, f)
        FILTER f._id != @start_vertex
        LIMIT 1
        RETURN 1
) > 0
LET hit = FIRST(
    FOR f, e, p IN 1..@max_depth INBOUND @start_vertex @@field_build_field
        FILTER IS_SAME_COLLECTION(@field_collection, f)
        FILTER f[@name_cn_attr] != null AND f[@name_cn_attr] != ""
        FILTER f._id != @start_vertex
        SORT LENGTH(p.edges) ASC, f._id ASC
        LIMIT 1
        RETURN {
            name_en: f[@name_attr],
            name_cn: f[@name_cn_attr],
            via_vertex: f._id,
            hops: LENGTH(p.edges)
        }
)
RETURN {
    has_relation: has_relation,
    found: hit != null,
    name_en: hit == null ? null : hit.name_en,
    name_cn: hit == null ? null : hit.name_cn,
    via_vertex: hit == null ? null : hit.via_vertex,
    hops: hit == null ? null : hit.hops
}
"""


def related_upstream_tables_aql() -> str:
    """向量候选：上游表中已有 name_cn 的表。"""
    return """
FOR v, e, p IN 1..@max_depth ANY @start_vertex
    @@table_build_table
    FILTER IS_SAME_COLLECTION(@table_collection, v)
    FILTER v[@name_cn_attr] != null AND v[@name_cn_attr] != ""
    FILTER v._id != @start_vertex
    LIMIT @limit
    RETURN DISTINCT {
        id: v._id,
        name_en: v[@name_attr],
        name_cn: v[@name_cn_attr]
    }
"""


def related_upstream_fields_aql() -> str:
    """向量候选：上游表下属已有 name_cn 的字段。"""
    return """
LET parent_table = FIRST(
    FOR t IN 1..1 ANY @start_vertex @@table_contain_field
        RETURN t
)
FILTER parent_table != null
FOR row IN (
    FOR upstream IN 1..@max_depth INBOUND parent_table._id @@table_build_table
        FILTER IS_SAME_COLLECTION(@table_collection, upstream)
        FOR f IN 1..1 OUTBOUND upstream._id @@table_contain_field
            FILTER f[@name_cn_attr] != null AND f[@name_cn_attr] != ""
            FILTER f[@name_attr] != null AND f[@name_attr] != ""
            RETURN {
                id: f._id,
                name_en: f[@name_attr],
                name_cn: f[@name_cn_attr],
                table_id: upstream[@object_key_attr],
                table_name_en: upstream[@name_attr],
                table_name_cn: upstream[@name_cn_attr]
            }
)
LIMIT @limit
RETURN DISTINCT row
"""


def tables_owning_field_aql() -> str:
    """在候选表列表中，查出哪些表拥有指定字段名（用于消歧未限定列）。"""
    return """
FOR ref IN @candidates
    LET parts = SPLIT(ref, ".")
    LET tname = parts[LENGTH(parts) - 1]
    LET sname = LENGTH(parts) >= 2 ? parts[LENGTH(parts) - 2] : ""
    FOR t IN @@table_collection
        FILTER t[@name_attr] != null AND t[@name_attr] != ""
        FILTER LOWER(t[@name_attr]) == LOWER(tname)
        FILTER sname == "" OR LOWER(t[@schema_attr]) == LOWER(sname)
        FOR f IN 1..1 ANY t._id @@table_contain_field
            FILTER f[@name_attr] != null AND f[@name_attr] != ""
            FILTER LOWER(f[@name_attr]) == LOWER(@field_name)
            RETURN DISTINCT {
                table_ref: ref,
                field_name: f[@name_attr],
                field_name_cn: f[@name_cn_attr],
                table_name_cn: t[@name_cn_attr]
            }
"""


def fields_of_table_aql() -> str:
    """按表名（及可选 schema）定位 table，再取 table_contain_field 下全部字段。"""
    return """
LET tables = (
    FOR t IN @@table_collection
        FILTER t[@name_attr] != null AND t[@name_attr] != ""
        FILTER LOWER(t[@name_attr]) == LOWER(@table_name)
        FILTER @schema_name == null OR @schema_name == ""
            OR LOWER(t[@schema_attr]) == LOWER(@schema_name)
        LIMIT 5
        RETURN t
)
LET table_doc = LENGTH(tables) == 1 ? tables[0] : (
    @prefer_schema != null AND @prefer_schema != ""
        ? FIRST(
            FOR t IN tables
                FILTER LOWER(t[@schema_attr]) == LOWER(@prefer_schema)
                RETURN t
          )
        : tables[0]
)
FILTER table_doc != null
FOR f IN 1..1 ANY table_doc._id @@table_contain_field
    FILTER f[@name_attr] != null AND f[@name_attr] != ""
    SORT f[@name_attr] ASC
    RETURN {
        table_id: table_doc._id,
        table_name: table_doc[@name_attr],
        table_schema: table_doc[@schema_attr],
        table_name_cn: table_doc[@name_cn_attr],
        field_id: f._id,
        field_name: f[@name_attr],
        field_name_cn: f[@name_cn_attr]
    }
"""


def explore_lineage_aql(traversal: str) -> str:
    """遍历血缘邻域，返回起点及路径上的顶点与边。traversal: ANY / INBOUND / OUTBOUND。"""
    return f"""
LET start_doc = DOCUMENT(@start_vertex)
FILTER start_doc != null

LET start_row = {{
    hops: 0,
    vertex_id: start_doc._id,
    name_en: start_doc[@name_attr],
    name_cn: start_doc[@name_cn_attr],
    edge_from: null,
    edge_to: null,
    edge_collection: null
}}

FOR row IN UNION(
    [start_row],
    (
        FOR v, e, p IN 1..@max_depth {traversal} @start_vertex
            @@table_contain_field, @@table_build_table, @@field_build_field
            FOR edge IN p.edges
                RETURN {{
                    hops: LENGTH(p.edges),
                    vertex_id: v._id,
                    name_en: v[@name_attr],
                    name_cn: v[@name_cn_attr],
                    edge_from: edge._from,
                    edge_to: edge._to,
                    edge_collection: SPLIT(edge._id, "/")[0]
                }}
    )
)
RETURN row
"""
