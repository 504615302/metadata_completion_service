"""
流水线中流转的数据结构，命名对应你描述的四个步骤。
"""
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class MissingField(BaseModel):
    """待补全字段（来源：MySQL dim_prj_meta2_fields_daily）"""
    key: str            # field 顶点的 _key
    id: str             # field 顶点的 _id（field/xxx），图谱操作用；优先取 graph_id
    name_en: str         # 字段英文名（en_filed_name）
    name_cn: Optional[str] = None  # 日表原始中文（ch_field_name），用于与溯源结果比对
    table_id: Optional[str] = None  # 所属 table 的 object_key（业务标识）
    table_vertex_id: Optional[str] = None  # 所属 table 的 _id，图谱操作用
    table_name_en: Optional[str] = None
    table_name_cn: Optional[str] = None
    mysql_id: Optional[int] = None  # 日表主键 id，用于回写
    schema_name: Optional[str] = None


class MissingTable(BaseModel):
    """待补全表（来源：MySQL dim_prj_meta2_tables_daily）"""
    key: str
    id: str             # table 顶点的 _id（table/xxx），图谱操作用；优先取 graph_id
    object_key: Optional[str] = None  # 业务标识 object_key，与 field.table_id 对应
    name_en: str
    name_cn: Optional[str] = None  # 日表原始中文（ch_table_name），用于与溯源结果比对
    mysql_id: Optional[int] = None
    schema_name: Optional[str] = None

    def store_id(self) -> str:
        """Redis key 与结果 JSON 中的 table_id，与 field 一致使用顶点 _id（table/xxx）。"""
        return self.id


class CompletionTable(BaseModel):
    """表遍历编排：逐表判断是补表中文还是补下属字段中文"""
    key: str
    id: str
    object_key: Optional[str] = None
    name_en: str
    name_cn: Optional[str] = None
    mysql_id: Optional[int] = None
    schema_name: Optional[str] = None

    def table_cn_missing(self) -> bool:
        return self.name_cn is None or self.name_cn == ""

    def to_missing_table(self) -> MissingTable:
        return MissingTable(
            key=self.key,
            id=self.id,
            object_key=self.object_key,
            name_en=self.name_en,
            name_cn=self.name_cn,
            mysql_id=self.mysql_id,
            schema_name=self.schema_name,
        )


class GraphTraceResult(BaseModel):
    """第二步：图谱溯源结果"""
    found: bool
    has_relation: bool = False         # max_depth 内是否存在血缘关系边
    name_en: Optional[str] = None      # 命中顶点英文名，与日表 en_filed_name / table_name 比对
    name_cn: Optional[str] = None
    via_vertex: Optional[str] = None   # 命中的顶点 _id（可能是 table 也可能是 field）
    hops: Optional[int] = None


class CandidateField(BaseModel):
    """第三步：向量化后的候选字段"""
    id: str
    name_en: str
    name_cn: str
    similarity: float   # 余弦相似度 0~1
    table_id: Optional[str] = None
    table_name_en: Optional[str] = None
    table_name_cn: Optional[str] = None


class CandidateTable(BaseModel):
    """第三步：向量化后的候选表"""
    id: str
    name_en: str
    name_cn: str
    similarity: float


class LLMPickResult(BaseModel):
    """多候选一次挑选：choice_index 为候选列表下标；None 表示没有合适候选或候选池为空。"""
    choice_index: Optional[int] = None
    reasoning: str = ""


class ResultSource(str, Enum):
    GRAPH = "graph"                  # 图谱溯源命中
    FIELD_INFER = "field_infer"      # 历史结果兼容（已不再产生）
    LLM = "llm"                      # 向量检索 + LLM 评分通过，自动补全
    MANUAL_PENDING = "manual_pending"  # 无血缘关系，需要人工溯源 → completion_type=50
    RELATION_PENDING = "relation_pending"  # 有关系但无可用中文 → 51
    CN_EQ_EN = "cn_eq_en"            # 溯源中文名与日表 en_filed_name 一致 → 52


class ReviewReason(str, Enum):
    NO_CANDIDATE = "no_candidate"           # 图谱邻域内没有任何可比较的候选字段
    SIMILARITY_LOW = "similarity_low"       # 最高相似度 <= 阈值
    LLM_REJECTED = "llm_rejected"           # LLM 明确判断候选语义不适用
    NO_RELATION = "no_relation"             # 补全过程中没有任何血缘关系
    RELATION_NO_CN = "relation_no_cn"       # 有关系，但关系中没有中文
    CN_EQ_EN = "cn_eq_en"                   # 溯源命中中文名与日表 en_filed_name 一致


class FieldMetadataResult(BaseModel):
    """第四步：最终写入 Redis 的结果，三类来源统一走这一个结构"""
    field_id: str
    name_en: str
    name_cn: Optional[str] = None      # MANUAL_PENDING 时可能为空，或者是低置信度的参考值
    source: ResultSource
    reason: Optional[ReviewReason] = None    # 仅 MANUAL_PENDING 时有值（业务原因枚举）
    reasoning: Optional[str] = None          # LLM 给出的挑选/拒绝理由
    via_vertex: Optional[str] = None          # GRAPH 来源：命中的顶点；LLM 来源：候选字段 id
    hops: Optional[int] = None
    similarity: Optional[float] = None
    confidence: Optional[float] = None
    saved_at: datetime = datetime.utcnow()


class TableMetadataResult(BaseModel):
    """表级补全结果，与字段补全写入同一 Redis 前缀（key 为 table/xxx，与 field/xxx 格式一致）"""
    table_id: str
    name_en: str
    name_cn: Optional[str] = None
    source: ResultSource
    reason: Optional[ReviewReason] = None
    reasoning: Optional[str] = None          # LLM 给出的挑选/拒绝理由
    via_vertex: Optional[str] = None
    hops: Optional[int] = None
    similarity: Optional[float] = None
    confidence: Optional[float] = None
    saved_at: datetime = datetime.utcnow()


class CompletionRequest(BaseModel):
    limit: int = 5000                          # 每页条数（单独接口默认 5000；合并接口默认 50）
    process_all: bool = False                    # True 时分页直到 field/table 全部遍历完
    max_trace_depth: Optional[int] = None      # 覆盖 .env 里的默认跳数
    similarity_threshold: Optional[float] = None
    # 并发度：同时处理的表/字段数；不传则用 .env 的 PIPELINE_CONCURRENCY
    concurrency: Optional[int] = Field(default=None, ge=1, le=64)


class RunStats(BaseModel):
    total_fields: int = 0
    pages: int = 0
    skipped: int = 0        # 已处理过、直接从 Redis 跳过，未重新处理的字段数
    graph_hit: int = 0
    llm_matched: int = 0
    manual_pending: int = 0
    manual_pending_no_candidate: int = 0
    manual_pending_similarity_low: int = 0
    manual_pending_confidence_low: int = 0
    manual_pending_llm_rejected: int = 0
    errors: int = 0


class TableRunStats(BaseModel):
    total_tables: int = 0
    pages: int = 0
    skipped: int = 0
    graph_hit: int = 0
    llm_matched: int = 0
    manual_pending: int = 0
    manual_pending_no_candidate: int = 0
    manual_pending_similarity_low: int = 0
    manual_pending_confidence_low: int = 0
    manual_pending_llm_rejected: int = 0
    errors: int = 0


class ClearRedisResponse(BaseModel):
    prefix: str
    deleted: int


class RedisResultQueryResponse(BaseModel):
    """按 source 查询 Redis 补全结果"""
    prefix: str
    source: str
    limit: int
    name_cn_not_empty: bool = False
    returned: int
    results: list[dict]


class CombinedRunStats(BaseModel):
    """字段 + 表合并补全统计"""
    pages: int = 0
    page_size: int = 50
    concurrency: int = 1
    fields: RunStats
    tables: TableRunStats


class LineageDirection(str, Enum):
    BOTH = "both"
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"


class LineageRequest(BaseModel):
    """查询图谱血缘：传入 field/xxx 或 table/xxx。"""
    vertex_id: str
    max_depth: Optional[int] = None
    direction: LineageDirection = LineageDirection.BOTH


class LineageVertex(BaseModel):
    id: str
    kind: str
    name_en: Optional[str] = None
    name_cn: Optional[str] = None
    hops: int = 0


class LineageEdge(BaseModel):
    from_id: str
    to_id: str
    edge_collection: str
    relation: str


class LineageResponse(BaseModel):
    start_vertex: str
    vertex_kind: str
    direction: LineageDirection
    max_depth: int
    vertices: list[LineageVertex]
    edges: list[LineageEdge]


# ---------- INSERT SQL 解析（MySQL 源 + 大模型） ----------


class InsertFieldRelation(BaseModel):
    """目标字段及其来源、中文名（表-字段关系）"""
    target_table: Optional[str] = None
    field_name: str
    name_cn: Optional[str] = None
    source_expr: Optional[str] = None
    source_columns: list[str] = Field(default_factory=list)
    source_tables: list[str] = Field(default_factory=list)


class InsertTableRelation(BaseModel):
    """INSERT...SELECT 产生的表级血缘"""
    source_table: str
    target_table: str
    relation: str = "insert_select"


class InsertFieldLineage(BaseModel):
    """字段到字段的血缘（源字段 -> 目标字段）"""
    source_table: Optional[str] = None
    source_field: str
    target_table: Optional[str] = None
    target_field: str
    source_expr: Optional[str] = None
    relation: str = "insert_select"


class InsertParseSource(str, Enum):
    LLM = "llm"


class InsertSqlParseItem(BaseModel):
    """单条 SQL 的解析结果"""
    parse_ok: bool = True
    parse_error: Optional[str] = None
    parse_source: Optional[InsertParseSource] = None  # llm
    target_table: Optional[str] = None
    table_name_cn: Optional[str] = None
    fields: list[InsertFieldRelation] = Field(default_factory=list)
    table_relations: list[InsertTableRelation] = Field(default_factory=list)
    field_relations: list[InsertFieldLineage] = Field(default_factory=list)
    # SELECT * / t.* 对应源表，后续从 ArangoDB 展开字段
    star_source_tables: list[str] = Field(default_factory=list)
    pending_target_columns: list[str] = Field(default_factory=list)
    star_expanded: bool = False
    # 仅内存/接口回显；结果表不写 raw_sql，改存 raw_sql_id
    raw_sql: str = ""


class InsertSqlParsePersistStats(BaseModel):
    table_lineage: int = 0
    field_lineage: int = 0
    field_meta: int = 0


class InsertSqlParseRow(BaseModel):
    """MySQL 一行源数据 + 解析结果"""
    raw_sql_id: Optional[str] = None  # 源表 SQL 行 id（VARCHAR(64)）；结果表写 raw_sql_id，不写 raw_sql
    job_id: Optional[int] = None  # 源表 job_id，写入四张结果表
    task_id: Optional[int] = None  # 源表 task_id，写入四张结果表
    workspace_name: Optional[str] = None
    parse: InsertSqlParseItem
    persisted: Optional[InsertSqlParsePersistStats] = None


class InsertSqlParseRequest(BaseModel):
    """从 MySQL 批量读取并解析 INSERT SQL"""
    limit: int = Field(default=100, ge=1, le=5000, description="每页条数；process_all=true 时作为翻页大小")
    offset: int = Field(default=0, ge=0, description="起始偏移；process_all=true 时从该 offset 一直翻到末尾")
    workspace_name: Optional[str] = Field(
        default=None,
        description="覆盖 MYSQL_WORKSPACE_FILTER；支持英文逗号分隔多个工作空间",
    )
    persist: bool = True  # 解析成功后写入血缘结果表
    expand_star_from_graph: bool = True  # SELECT * / t.* 时从 ArangoDB 展开字段
    process_all: bool = True  # True：翻页处理库中全部（或过滤后）数据
    skip_processed: bool = True  # True：按 Redis 进度决定是否跳过
    retry_failed: bool = Field(
        default=False,
        description=(
            "仅当 skip_processed=true 时生效。"
            "false：Redis 有记录（成功或失败）一律跳过；"
            "true：仅跳过 status=ok，status=failed 的会重新解析"
        ),
    )
    mark_failed_as_processed: bool = True  # 解析失败是否也记入 Redis（避免反复失败刷 LLM）
    include_results: bool = False  # True：响应中带回本轮新解析的明细（全量时可能很大）
    concurrency: Optional[int] = Field(
        default=None,
        ge=1,
        le=64,
        description="同时调用 LLM 解析的条数；不传则用 PIPELINE_CONCURRENCY",
    )


class InsertSqlTextRequest(BaseModel):
    """直接传 SQL 文本解析（不读库，便于联调）"""
    sql: str
    workspace_name: Optional[str] = None
    persist: bool = True
    expand_star_from_graph: bool = True


class InsertSqlParseResponse(BaseModel):
    total: int = 0  # 本轮扫描到的源表行数（含跳过）
    scanned: int = 0  # 同 total，兼容语义
    skipped: int = 0  # Redis 已处理而跳过
    processed: int = 0  # 本轮实际解析条数
    parse_ok: int = 0
    parse_failed: int = 0
    llm_ok: int = 0
    pages: int = 0
    concurrency: int = 1
    persisted: Optional[InsertSqlParsePersistStats] = None
    results: list[InsertSqlParseRow] = Field(default_factory=list)


class InsertSqlParseStatsResponse(BaseModel):
    """INSERT SQL 血缘解析进度统计（基于 Redis 标记）"""
    prefix: str
    total: int = 0  # 已解析总条数（成功+失败+未知）
    parse_ok: int = 0  # 解析成功
    parse_failed: int = 0  # 解析失败
    unknown: int = 0  # status 异常或无法解析的标记


# ---------- SQL 表关联（JOIN）提取 ----------


class TableJoinRole(str, Enum):
    """主表 / 从表标识"""
    PRIMARY = "primary"      # 主表
    SECONDARY = "secondary"  # 从表


class SqlJoinTableSide(BaseModel):
    """关联一侧：主表或从表"""
    role: TableJoinRole
    schema_name: Optional[str] = None
    table_name: str
    join_fields: list[str] = Field(default_factory=list, description="该侧参与关联的字段")


class SqlJoinRelation(BaseModel):
    """一条表关联关系：主表 + 从表 + 关联表达式"""
    primary: SqlJoinTableSide
    secondary: SqlJoinTableSide
    join_type: Optional[str] = None  # left / right / inner / full / cross / ...
    join_expression: str  # 如 A left join B on A.id=B.id


class SqlJoinExtractItem(BaseModel):
    """单条 SQL 的关联提取结果"""
    parse_ok: bool = True
    parse_error: Optional[str] = None
    parse_source: Optional[InsertParseSource] = None
    joins: list[SqlJoinRelation] = Field(default_factory=list)
    # 仅内存/接口回显；结果表不写 raw_sql，改存 raw_sql_id
    raw_sql: str = ""


class SqlJoinExtractPersistStats(BaseModel):
    table_join: int = 0


class SqlJoinExtractRow(BaseModel):
    """源表一行 + 关联提取结果"""
    raw_sql_id: Optional[str] = None  # 源表 SQL 行 id（VARCHAR(64)）；结果表写 raw_sql_id，不写 raw_sql
    job_id: Optional[int] = None  # 源表 job_id，写入结果表
    task_id: Optional[int] = None  # 源表 task_id，写入结果表
    workspace_name: Optional[str] = None
    parse: SqlJoinExtractItem
    persisted: Optional[SqlJoinExtractPersistStats] = None


class SqlJoinExtractRequest(BaseModel):
    """从 MySQL insert_sql_source 批量提取表关联"""
    limit: int = Field(default=100, ge=1, le=5000, description="每页条数；process_all=true 时作为翻页大小")
    offset: int = Field(default=0, ge=0, description="起始偏移")
    workspace_name: Optional[str] = Field(
        default=None,
        description="覆盖 MYSQL_WORKSPACE_FILTER；支持英文逗号分隔多个工作空间",
    )
    persist: bool = True
    process_all: bool = True
    skip_processed: bool = True
    retry_failed: bool = Field(
        default=False,
        description=(
            "仅当 skip_processed=true 时生效。"
            "false：Redis 有记录一律跳过；"
            "true：仅跳过 status=ok，失败的会重新提取"
        ),
    )
    mark_failed_as_processed: bool = True
    include_results: bool = False
    concurrency: Optional[int] = Field(
        default=None,
        ge=1,
        le=64,
        description="同时调用 LLM 的条数；不传则用 PIPELINE_CONCURRENCY",
    )


class SqlJoinTextRequest(BaseModel):
    """直接传 SQL 文本提取关联（不读库）"""
    sql: str
    workspace_name: Optional[str] = None
    persist: bool = True


class SqlJoinTestRequest(BaseModel):
    """单条 SQL 联调：默认不落库，只看解析效果"""
    sql: str = Field(..., description="待解析的 SQL 文本")
    workspace_name: Optional[str] = None
    persist: bool = Field(default=False, description="是否写入 insert_table_join；联调建议 false")


class SqlJoinExtractResponse(BaseModel):
    total: int = 0
    scanned: int = 0
    skipped: int = 0
    processed: int = 0
    parse_ok: int = 0
    parse_failed: int = 0
    llm_ok: int = 0
    pages: int = 0
    concurrency: int = 1
    join_count: int = 0  # 本轮新提取到的关联条数合计
    persisted: Optional[SqlJoinExtractPersistStats] = None
    results: list[SqlJoinExtractRow] = Field(default_factory=list)


class SqlJoinExtractStatsResponse(BaseModel):
    """表关联提取进度统计（基于 Redis）"""
    prefix: str
    total: int = 0
    parse_ok: int = 0
    parse_failed: int = 0
    unknown: int = 0


# ---------- SQL 血缘 + 表关联 合并溯源（单条） ----------


class SqlCombinedTraceRequest(BaseModel):
    """传入单条 SQL 与 raw_sql_id，并行做血缘解析与表关联提取并落库。"""
    sql: str = Field(..., description="待解析的 SQL 文本")
    raw_sql_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="源表 SQL 行 id，写入结果表 raw_sql_id，并用于 Redis 进度标记",
    )
    job_id: int = Field(..., description="任务 job_id，写入结果表")
    task_id: int = Field(..., description="任务 task_id，写入结果表")
    workspace_name: Optional[str] = None
    persist: bool = Field(default=True, description="解析成功后写入血缘/关联结果表")
    expand_star_from_graph: bool = Field(
        default=True,
        description="SELECT * / t.* 时从 ArangoDB 展开字段（仅血缘）",
    )
    mark_processed: bool = Field(
        default=True,
        description="是否写入 Redis 进度（血缘/关联各自前缀），便于批量任务跳过",
    )


class SqlCombinedTraceResponse(BaseModel):
    """单条 SQL 合并溯源结果"""
    raw_sql_id: str
    workspace_name: Optional[str] = None
    job_id: int
    task_id: int
    lineage: InsertSqlParseRow
    join: SqlJoinExtractRow


# ---------- SQL 字段级关系抽取（fdd / join / fdr） ----------


class SqlFieldRelType(str, Enum):
    """字段关系类型"""
    FDD = "fdd"    # 血缘：INSERT 目标字段 ← SELECT 源字段
    JOIN = "join"  # JOIN/ON 中的等值关联
    FDR = "fdr"    # WHERE 中的跨表等值关联


class SqlFieldRelationItem(BaseModel):
    """一条字段级关系"""
    source_schema_name: Optional[str] = None
    source_table_name: str
    source_field_name: str
    target_schema_name: Optional[str] = None
    target_table_name: str
    target_field_name: str
    rel_type: SqlFieldRelType
    exp_fragment: Optional[str] = Field(
        default=None,
        description="对应的 SQL 片段，如投影表达式 / ON 条件 / WHERE 等值条件",
    )


class SqlFieldRelationRequest(BaseModel):
    """传入 SQL，抽取 fdd/join/fdr 字段关系（不落库）"""
    sql: str = Field(..., description="待解析的 SQL 文本")
    workspace_name: Optional[str] = None


class MysqlTableCheckItem(BaseModel):
    """单张表检查结果"""
    name: str
    role: str  # source_readonly / result
    exists: bool
    row_count: Optional[int] = None
    error: Optional[str] = None


class MysqlHealthResponse(BaseModel):
    """MySQL 连接与表存在性检查"""
    ok: bool
    connected: bool
    host: str
    port: int
    database: str
    server_version: Optional[str] = None
    error: Optional[str] = None
    tables: list[MysqlTableCheckItem] = Field(default_factory=list)
    missing_tables: list[str] = Field(default_factory=list)
