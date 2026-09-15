"""
FastAPI 入口。
"""
from contextlib import asynccontextmanager
import logging
import os

from fastapi import FastAPI, HTTPException, Query

from app.schemas import (
    ClearRedisResponse,
    CombinedRunStats,
    CompletionRequest,
    InsertSqlParseRequest,
    InsertSqlParseResponse,
    InsertSqlParseRow,
    InsertSqlParseStatsResponse,
    InsertSqlTextRequest,
    LineageRequest,
    LineageResponse,
    MysqlHealthResponse,
    RedisResultQueryResponse,
    ResultSource,
    RunStats,
    SqlCombinedTraceRequest,
    SqlCombinedTraceResponse,
    SqlFieldRelationItem,
    SqlFieldRelationRequest,
    SqlJoinExtractRequest,
    SqlJoinExtractResponse,
    SqlJoinExtractRow,
    SqlJoinExtractStatsResponse,
    SqlJoinTestRequest,
    SqlJoinTextRequest,
    TableRunStats,
)
from app.services.pipeline import run_pipeline
from app.services.table_pipeline import run_table_pipeline
from app.services.combined_pipeline import run_combined_pipeline
from app.services.lineage_query import query_lineage
from app.services.mysql_sql_source import fetch_and_parse_insert_sql, parse_insert_sql_text
from app.services.insert_sql_progress import count_insert_sql_parse_stats
from app.services.sql_combined_pipeline import run_sql_combined_trace
from app.services.sql_field_relation import extract_sql_field_relations
from app.services.sql_join_pipeline import extract_sql_joins_text, fetch_and_extract_sql_joins
from app.services.sql_join_progress import count_sql_join_extract_stats
from app.services.result_store import clear_results_by_prefix, list_results_by_source
from app.services.scheduler import start_daily_scheduler, stop_daily_scheduler
from app.services.sql_scheduler import start_sql_scheduler, stop_sql_scheduler
from app.services.mysql_health import check_mysql_health
import argparse
import uvicorn
logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(_: FastAPI):
    from app.services.mysql_schema import ensure_insert_sql_tables

    try:
        await ensure_insert_sql_tables()
    except Exception:
        logging.getLogger(__name__).exception(
            "MySQL 自动建表失败（可检查 MYSQL 连接或设 MYSQL_AUTO_CREATE_TABLES=false）"
        )

    scheduler_task = start_daily_scheduler()
    sql_scheduler_task = start_sql_scheduler()
    try:
        yield
    finally:
        await stop_sql_scheduler(sql_scheduler_task)
        await stop_daily_scheduler(scheduler_task)


app = FastAPI(
    title="元数据补全服务",
    description=(
        "MySQL 日表扫描待补全 field/table -> 图谱溯源(ArangoDB) -> "
        "向量检索+LLM评分 -> 回写日表 completion_type"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.post(
    "/metadata/completion/field",
    response_model=RunStats,
    summary="字段补全：扫描日表 completion_type=0",
)
async def metadata_field_completion(request: CompletionRequest = CompletionRequest()) -> RunStats:
    return await run_pipeline(request)


@app.post(
    "/metadata/completion/table",
    response_model=TableRunStats,
    summary="表补全：扫描日表 completion_type=0",
)
async def metadata_table_completion(request: CompletionRequest = CompletionRequest()) -> TableRunStats:
    return await run_table_pipeline(request)


@app.post(
    "/metadata/completion/",
    response_model=CombinedRunStats,
    summary="合并补全：先全量补表，再全量补字段（待办来自 MySQL 日表）",
)
async def metadata_combined_completion(
    request: CompletionRequest = CompletionRequest(limit=50, process_all=True),
) -> CombinedRunStats:
    """
    两阶段补全（强制 process_all=true）：
    1. 扫描 tables_daily（completion_type=0），补完全部待办表
    2. 再扫描 fields_daily（completion_type=0），补完全部待办字段

    并发度由请求体 concurrency 控制（不传则用 PIPELINE_CONCURRENCY）。
    """
    return await run_combined_pipeline(request)


@app.post(
    "/metadata/lineage",
    response_model=LineageResponse,
    summary="查询字段或表在图谱中的血缘关系",
)
async def metadata_lineage(request: LineageRequest) -> LineageResponse:
    """
    传入 field/xxx 或 table/xxx，返回图谱邻域内的顶点与边。

    - direction=both：三边任意方向遍历（默认）
    - direction=upstream：仅 INBOUND（上游表 / 上游字段 / 所属表）
    - direction=downstream：仅 OUTBOUND（下游表 / 下游字段 / 下属字段）
    """
    return await query_lineage(request)


@app.delete(
    "/metadata/completion/redis",
    response_model=ClearRedisResponse,
    summary="按指定前缀删除 Redis key",
)
async def clear_completion_redis(
    prefix: str = Query(
        ...,
        description=(
            "要删除的 key 前缀，如 meta-completion: 或 metadata-completion:sql-join:；"
            "将删除该前缀下全部匹配 key"
        ),
        min_length=1,
    ),
) -> ClearRedisResponse:
    """
    传入前缀后删除 `prefix*` 匹配的全部 key。

    示例：
    - `prefix=meta-completion:` → 删补全结果
    - `prefix=metadata-completion:sql-join:` → 删表关联进度
    - `prefix=metadata-completion:insert-sql:` → 删血缘解析进度
    """
    try:
        used_prefix, deleted = await clear_results_by_prefix(prefix)
        return ClearRedisResponse(prefix=used_prefix, deleted=deleted)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.get(
    "/metadata/completion/redis/results",
    response_model=RedisResultQueryResponse,
    summary="按 source 查询 Redis 补全结果（数量可配）",
)
async def redis_results_by_source(
    source: str = Query(..., description="来源类型，如 llm、manual_pending、graph"),
    limit: int = Query(100, ge=1, le=1000, description="返回条数上限"),
    name_cn_not_empty: bool = Query(False, description="为 true 时仅返回 name_cn 不为空的记录"),
) -> RedisResultQueryResponse:
    allowed = {s.value for s in ResultSource}
    normalized = source.strip().lower()
    if normalized not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"source 须为以下之一: {', '.join(sorted(allowed))}",
        )
    return await list_results_by_source(normalized, limit, name_cn_not_empty=name_cn_not_empty)


@app.post(
    "/metadata/sql/insert/ensure-tables",
    summary="手动触发：自动创建血缘/关联结果表（不改原生 SQL 源表）",
)
async def ensure_insert_sql_tables_api() -> dict:
    from app.services.mysql_schema import ensure_insert_sql_tables

    try:
        tables = await ensure_insert_sql_tables(force=True)
        return {"ok": True, "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"建表失败: {e}") from e


@app.post(
    "/metadata/sql/insert/parse",
    response_model=InsertSqlParseResponse,
    summary="从 MySQL 全量/分页解析 INSERT SQL（Redis 跳过已处理）",
)
async def parse_insert_sql_from_pg(
    request: InsertSqlParseRequest = InsertSqlParseRequest(),
) -> InsertSqlParseResponse:
    """
    从 MySQL 读取源表（默认列：id / SQL / workspace_name），
    由大模型解析 INSERT 血缘。

    - 默认仅处理 workspace_name = MYSQL_WORKSPACE_FILTER（面向基层数据服务）
    - process_all=true（默认）：按 limit 翻页直到扫完
    - skip_processed=true（默认）：按 Redis `{redis_record_key_prefix}:{id}` 决定跳过
      - retry_failed=false（默认）：有记录（成功/失败）一律跳过
      - retry_failed=true：仅跳过 status=ok；失败的会重新解析
    - 每条解析结束后先写 Redis 进度，再立即落库（persist=true 且 parse_ok）
    - concurrency：页内并发调用 LLM 的条数（默认用 PIPELINE_CONCURRENCY）
    - include_results=true 时才在响应中带回本轮新解析明细
    """
    try:
        return await fetch_and_parse_insert_sql(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取或解析失败: {e}") from e


@app.get(
    "/metadata/sql/insert/parse/stats",
    response_model=InsertSqlParseStatsResponse,
    summary="统计 INSERT SQL 血缘解析总数与失败数（基于 Redis 进度）",
)
async def insert_sql_parse_stats() -> InsertSqlParseStatsResponse:
    """
    扫描 Redis 前缀 `{redis_record_key_prefix}:*`，按 status 汇总：
    - total：已解析总条数
    - parse_ok：解析成功
    - parse_failed：解析失败
    """
    try:
        return await count_insert_sql_parse_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"统计失败: {e}") from e


@app.post(
    "/metadata/sql/insert/parse-text",
    response_model=InsertSqlParseRow,
    summary="直接解析 INSERT SQL 文本（大模型）",
)
async def parse_insert_sql_direct(request: InsertSqlTextRequest) -> InsertSqlParseRow:
    return await parse_insert_sql_text(
        request.sql,
        workspace_name=request.workspace_name,
        persist=request.persist,
        expand_star_from_graph=request.expand_star_from_graph,
    )


@app.post(
    "/metadata/sql/join/extract",
    response_model=SqlJoinExtractResponse,
    summary="从 MySQL insert_sql_source 提取表关联（主表/从表/关联表达式）",
)
async def extract_sql_joins_from_pg(
    request: SqlJoinExtractRequest = SqlJoinExtractRequest(),
) -> SqlJoinExtractResponse:
    """
    读取源表中的 SQL，用大模型提取 JOIN 关联：

    - 默认仅处理 workspace_name = MYSQL_WORKSPACE_FILTER（面向基层数据服务）
    - 主表 primary：schema_name、table_name、join_fields
    - 从表 secondary：schema_name、table_name、join_fields
    - join_expression：如 `A left join B on A.id=B.id`

    默认写入 MySQL `insert_table_join`，并用独立 Redis 前缀跳过已处理行。
    """
    try:
        return await fetch_and_extract_sql_joins(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"表关联提取失败: {e}") from e


@app.get(
    "/metadata/sql/join/extract/stats",
    response_model=SqlJoinExtractStatsResponse,
    summary="统计表关联提取进度（基于 Redis）",
)
async def sql_join_extract_stats() -> SqlJoinExtractStatsResponse:
    try:
        return await count_sql_join_extract_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"统计失败: {e}") from e


@app.post(
    "/metadata/sql/join/extract-text",
    response_model=SqlJoinExtractRow,
    summary="直接从 SQL 文本提取表关联（大模型）",
)
async def extract_sql_joins_direct(request: SqlJoinTextRequest) -> SqlJoinExtractRow:
    return await extract_sql_joins_text(request)


@app.post(
    "/metadata/sql/join/extract-test",
    response_model=SqlJoinExtractRow,
    summary="【联调】输入单条 SQL，查看表关联解析效果（默认不落库）",
)
async def extract_sql_joins_test(request: SqlJoinTestRequest) -> SqlJoinExtractRow:
    """
    专门用于看解析效果：传入一条 SQL，返回主表/从表/关联表达式。
    默认 persist=false，不会写入 insert_table_join。
    """
    return await extract_sql_joins_text(
        SqlJoinTextRequest(
            sql=request.sql,
            workspace_name=request.workspace_name,
            persist=request.persist,
        )
    )


@app.post(
    "/metadata/sql/trace",
    response_model=SqlCombinedTraceResponse,
    summary="单条 SQL 合并溯源：并行血缘解析 + 表关联提取并落库",
)
async def sql_combined_trace(request: SqlCombinedTraceRequest) -> SqlCombinedTraceResponse:
    """
    传入一条 SQL、raw_sql_id、job_id、task_id，同时启动：

    1. INSERT 血缘解析 → `insert_table_lineage` / `insert_field_lineage` / `insert_field_meta`
    2. 表关联提取 → `insert_table_join`

    两者并行执行；结果表写 `raw_sql_id` / `job_id` / `task_id`（不写 SQL 原文）。
    默认 `persist=true`、`mark_processed=true`（写入 Redis，便于批量任务跳过）。
    """
    try:
        return await run_sql_combined_trace(request)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"合并溯源失败: {e}") from e


@app.post(
    "/metadata/sql/field-relations",
    response_model=list[SqlFieldRelationItem],
    summary="传入 SQL，返回字段级关系数组（fdd/join/fdr）",
)
async def sql_field_relations(
    request: SqlFieldRelationRequest,
) -> list[SqlFieldRelationItem]:
    """
    解析 SQL，返回数组，每项形如：

    ```json
    {
      "source_schema_name": "ods",
      "source_table_name": "user",
      "source_field_name": "id",
      "target_schema_name": "dws",
      "target_table_name": "fact",
      "target_field_name": "user_id",
      "rel_type": "fdd",
      "exp_fragment": "u.id AS user_id"
    }
    ```

    - `fdd`：血缘（INSERT 目标字段 ← SELECT 源字段）
    - `join`：JOIN ON / USING 中的跨表等值
    - `fdr`：WHERE 中的跨表等值关联
    - `exp_fragment`：该关系对应的 SQL 片段

    仅返回结果，不落库。
    """
    return await extract_sql_field_relations(request)


@app.get("/healthz", summary="健康检查")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get(
    "/metadata/mysql/health",
    response_model=MysqlHealthResponse,
    summary="检查 MySQL 连接与相关表是否存在",
)
async def mysql_health() -> MysqlHealthResponse:
    """
    验证：
    1. MySQL 能否连通（当前配置的 host/database）
    2. 源表与结果表是否已创建

    - source_readonly：原生 SQL 源表（只读，服务不建）
    - result：血缘/关联结果表（可自动创建）

    `ok=true` 表示连接成功且所列表面都存在。
    """
    return await check_mysql_health()



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', type=bool, default=False)

    args = parser.parse_args()
    port = int(os.getenv("PORT", "18084"))
    if args.debug:
        uvicorn.run('app.main:app', host="0.0.0.0", port=port, reload=args.debug)
    else:
        uvicorn.run('app.main:app', host="0.0.0.0", port=port, reload=args.debug)


