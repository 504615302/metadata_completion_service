"""
OpenAI 兼容的 Chat Completions 客户端：一次传入多个候选，由模型挑选最合适的一条。
"""
import asyncio
import json
import logging
import re
from typing import Any, Awaitable, Callable, Optional, TypeVar

import httpx

from app.config import get_settings
from app.schemas import CandidateField, CandidateTable, LLMPickResult

logger = logging.getLogger("metadata_pipeline.llm")

T = TypeVar("T")

# ```json ... ``` 或 '''json ... ''' / """json ... """
_FENCE_RE = re.compile(
    r"(?s)(?:```|'''|\"\"\")(?:\s*json)?\s*(.*?)\s*(?:```|'''|\"\"\")",
    re.IGNORECASE,
)


async def _with_llm_retry(label: str, fn: Callable[[], Awaitable[T]]) -> T:
    """
    大模型调用失败重试：最多 llm_max_retries 次（含首次），指数退避。
    可重试：网络/超时/HTTP 错误、响应 JSON 解析失败等。
    """
    settings = get_settings()
    max_attempts = max(1, settings.llm_max_retries)
    base_delay = max(0.0, settings.llm_retry_base_delay)
    last_exc: BaseException | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await fn()
        except (
            httpx.HTTPError,
            httpx.TimeoutException,
            httpx.TransportError,
            json.JSONDecodeError,
            ValueError,
            KeyError,
            IndexError,
            TypeError,
        ) as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "%s 失败，将重试 (%s/%s): %s; sleep=%.1fs",
                label,
                attempt,
                max_attempts,
                exc,
                delay,
            )
            if delay > 0:
                await asyncio.sleep(delay)

    assert last_exc is not None
    logger.error("%s 重试 %s 次后仍失败: %s", label, max_attempts, last_exc)
    raise last_exc


def _loads_llm_json(content: str) -> Any:
    """
    解析 LLM 输出：兼容纯 JSON，以及被以下形式包裹的内容：
    - ```json ... ```
    - '''json ... '''
    - \"\"\"json ... \"\"\"
    """
    text = (content or "").strip()
    if not text:
        raise ValueError("LLM 返回内容为空")

    # 统一弯引号，避免模型输出中文引号导致失败
    text = (
        text.replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )

    candidates: list[str] = [text]
    for m in _FENCE_RE.finditer(text):
        inner = (m.group(1) or "").strip()
        if inner:
            # 去掉内层可能残留的 json 标签行
            if inner.lower().startswith("json"):
                inner2 = inner[4:].lstrip(" \t\r\n:")
                if inner2:
                    candidates.insert(0, inner2)
            candidates.insert(0, inner)

    for opener, closer in (("{", "}"), ("[", "]")):
        start = text.find(opener)
        end = text.rfind(closer)
        if start >= 0 and end > start:
            candidates.append(text[start : end + 1])

    last_err: Exception | None = None
    seen: set[str] = set()
    for cand in candidates:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        try:
            return json.loads(cand)
        except json.JSONDecodeError as e:
            last_err = e

    preview = text[:240].replace("\n", "\\n")
    raise ValueError(f"无法从 LLM 输出解析 JSON: {last_err}; preview={preview!r}")


_FIELD_PICK_SYSTEM_PROMPT = """
你是企业元数据治理专家，负责从多个候选字段中，为目标字段挑选最合适的中文名。

## 任务
给定一个缺失中文名的目标字段（含所属表），以及若干候选字段（各有英文名、中文名、所属表、向量相似度），
从候选列表中选出一个最合适的，将其中文名作为目标字段中文名；若都不合适，则不选。

## 审核要点
1. 向量相似度仅供参考，英文字面相近不代表中文名可用
2. 重点判断目标字段与候选字段英文名语义是否一致
3. 结合表级上下文与字段级语义，避免业务域/粒度冲突
4. 缩写不明确、无法确定时，必须不选（choice 为 null）

## 输出要求
只输出 json，不要输出其他内容，不要使用 markdown/三引号代码块
{"choice": null或从0开始的候选序号整数, "reasoning": "100字以内的选择理由"}
"""

_TABLE_PICK_SYSTEM_PROMPT = """
你是企业元数据治理专家，负责从多个候选表中，为目标表挑选最合适的中文名。

## 任务
给定一个缺失中文名的目标表，以及若干候选表（各有英文名、中文名、向量相似度），
从候选列表中选出一个最合适的，将其中文名作为目标表中文名；若都不合适，则不选。

## 审核要点
1. 向量相似度仅供参考，英文字面相近不代表中文名可用
2. 重点判断目标表与候选表英文名业务语义是否一致
3. 结合数据仓库/业务域命名习惯；主题、分层、粒度不一致时不选

## 输出要求
只输出 json，不要输出其他内容，不要使用 markdown/三引号代码块
{"choice": null或从0开始的候选序号整数, "reasoning": "100字以内的选择理由"}
"""


def _format_table(name_en: Optional[str], name_cn: Optional[str]) -> str:
    if not name_en and not name_cn:
        return "(未知)"
    parts: list[str] = []
    if name_en:
        parts.append(f"英文名={name_en}")
    if name_cn:
        parts.append(f"中文名={name_cn}")
    return "，".join(parts)


def _parse_choice(value: object, candidate_count: int) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"", "null", "none", "nil"}:
            return None
        value = int(text)
    if isinstance(value, bool):
        raise ValueError("choice 不能是布尔值")
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    if not isinstance(value, int):
        raise ValueError("choice 必须是 null 或整数序号")
    if value < 0 or value >= candidate_count:
        raise ValueError(f"choice 越界: {value}, candidate_count={candidate_count}")
    return value


async def _call_llm_pick(system_prompt: str, user_prompt: str, candidate_count: int) -> LLMPickResult:
    settings = get_settings()
    logger.info("调用 LLM 多候选挑选, candidates=%s, prompt_length=%s", candidate_count, len(user_prompt))

    async def _once() -> LLMPickResult:
        async with httpx.AsyncClient(base_url=settings.llm_base_url, timeout=settings.llm_timeout_seconds) as client:
            resp = await client.post(
                "/chat/completions",
                headers={"Authorization": f"{settings.llm_api_key}"},
                json={
                    "model": settings.llm_model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0,
                    "response_format": {"type": "json_object"},
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
        parsed = _loads_llm_json(content)
        choice = _parse_choice(parsed.get("choice"), candidate_count)
        return LLMPickResult(
            choice_index=choice,
            reasoning=str(parsed.get("reasoning", "")),
        )

    try:
        result = await _with_llm_retry("LLM 多候选挑选", _once)
        logger.info("LLM 多候选挑选完成: choice=%s", result.choice_index)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.warning("LLM 多候选挑选最终失败: %s", exc)
        return LLMPickResult(
            choice_index=None,
            reasoning=f"LLM调用失败，未选定候选: {exc}",
        )


async def pick_best_field_candidate(
    *,
    name_en: str,
    table_name_en: Optional[str],
    table_name_cn: Optional[str],
    candidates: list[CandidateField],
) -> LLMPickResult:
    """多候选一次挑选；候选为空时不调用模型，直接返回空选。"""
    if not candidates:
        return LLMPickResult(choice_index=None, reasoning="候选池为空")

    lines: list[str] = []
    for idx, c in enumerate(candidates):
        lines.append(
            f"[{idx}] 所属表：{_format_table(c.table_name_en, c.table_name_cn)}；"
            f"字段英文名：{c.name_en}；字段中文名：{c.name_cn}；"
            f"向量相似度：{c.similarity:.4f}"
        )
    user_prompt = (
        f"【目标字段】\n"
        f"所属表：{_format_table(table_name_en, table_name_cn)}\n"
        f"字段英文名：{name_en}\n"
        f"字段中文名：（缺失，待补全）\n\n"
        f"【候选字段列表】（共 {len(candidates)} 个）\n"
        + "\n".join(lines)
        + "\n\n请从上述候选中选出最合适的一个序号；若都不合适，choice 返回 null，并说明理由。"
    )
    return await _call_llm_pick(_FIELD_PICK_SYSTEM_PROMPT, user_prompt, len(candidates))


async def pick_best_table_candidate(
    *,
    name_en: str,
    candidates: list[CandidateTable],
) -> LLMPickResult:
    """多候选一次挑选；候选为空时不调用模型，直接返回空选。"""
    if not candidates:
        return LLMPickResult(choice_index=None, reasoning="候选池为空")

    lines: list[str] = []
    for idx, c in enumerate(candidates):
        lines.append(
            f"[{idx}] 表英文名：{c.name_en}；表中文名：{c.name_cn}；"
            f"向量相似度：{c.similarity:.4f}"
        )
    user_prompt = (
        f"【目标表】\n"
        f"表英文名：{name_en}\n"
        f"表中文名：（缺失，待补全）\n\n"
        f"【候选表列表】（共 {len(candidates)} 个）\n"
        + "\n".join(lines)
        + "\n\n请从上述候选中选出最合适的一个序号；若都不合适，choice 返回 null，并说明理由。"
    )
    return await _call_llm_pick(_TABLE_PICK_SYSTEM_PROMPT, user_prompt, len(candidates))


_INSERT_SQL_PARSE_SYSTEM_PROMPT = """
你是元数据治理专家，擅长从 INSERT SQL 中抽取表字段血缘与中文名。

## 支持的 SQL 方言
本提示词支持多种数据库方言，请按相应语法解析：
- **标准 SQL**：INSERT INTO ... VALUES/SELECT
- **PostgreSQL 特有语法**：
  - ON CONFLICT (column) DO UPDATE/SET/NOTHING（解析时忽略 DO UPDATE 部分，保留源字段血缘）
  - INSERT ... RETURNING（RETURNING 子句不影响血缘解析）
  - INSERT ... ON CONFLICT ... DO NOTHING（忽略 ON CONFLICT 部分，解析 INTO 后的表和 SELECT 部分）
  - WITH 子句（CTE）：解析 CTE 名称映射，但血缘追溯到 CTE 定义的源表

## 任务
给定一条（可能语法不规范、方言混杂）的 INSERT SQL，提取：
1. 目标表（含 schema/db 前缀，如 dws.user_info）
2. 目标表中文名（若注释或别名中有）
3. 目标字段列表：字段英文名、中文名（来自注释/中文别名）、来源表达式、来源列、来源表
4. 表级血缘：INSERT...SELECT 时每个源表到目标表的关系
5. 字段级血缘：每个源字段到目标字段的映射（field_relations）
6. 若出现 SELECT * 或 alias.*：填写 star_source_tables（真实表名，不要只写别名）

## 规则
1. 只根据 SQL 文本推断，不要编造不存在的表/字段
2. 中文名优先取 /* ... */、-- 注释，或含中文的 AS 别名；没有则 name_cn 为 null
3. source_expr 写完整投影表达式（可保留别名/函数包装）
4. source_columns / source_field 必须输出最底层物理列（见下方专节）
5. field_relations 中 source_table、fields 中 source_tables 尽量解析为真实表名（不要只写别名/CTE/子查询别名）
6. 遇到 * / t.* 时：
   - 禁止臆造具体字段列表填进 fields（字段由系统从图数据库自动展开）
   - 不要输出 field_name="*" 的条目
   - 把对应真实源表写入 star_source_tables（必须是 schema.table 或 SQL 中的真实表名，禁止只写别名）
     例：u.* 且 FROM ods.user u → ["ods.user"]；裸 * → FROM/JOIN 中全部真实源表
   - 若 INSERT 写了显式列清单而 SELECT 是 *，把目标列名写入 pending_target_columns
   - 非 * 的普通投影仍正常写入 fields / field_relations
7. 遇到复杂 SQL 结构时：
   - **UNION/UNION ALL**：分别解析每个 SELECT 部分，合并血缘
   - **子查询**：追溯到子查询引用的真实源表与最底层列
   - **CTE (WITH 子句)**：将 CTE 名称作为中间层，血缘与 source_columns 都追溯到 CTE 定义内的物理表/列
   - **多表 JOIN**：正确识别 FROM/JOIN 中的源表；JOIN ON 仅用于理解关联关系，ON 中的列不写入字段血缘
8. **WHERE 条件不作为溯源依据（强制）**
   - SQL 溯源只看 INSERT 目标列与 SELECT/VALUES 投影（含 CTE、子查询内的投影），**不要**根据 WHERE 子句建立表级或字段级血缘
   - WHERE（含 HAVING，以及子查询/CTE 内部的 WHERE）里出现的表、列、表达式：一律忽略，禁止写入 fields、field_relations、table_relations、source_columns、star_source_tables
   - 某列若**仅**出现在 WHERE、未出现在 SELECT 投影中 → 不输出该列的任何血缘
   - 某表若**仅**因 WHERE 条件被引用、未出现在 FROM/JOIN/投影路径中 → 不把该表当作源表
   - 例：`INSERT INTO dws.t (a) SELECT u.id FROM ods.user u WHERE u.status = 1 AND EXISTS (SELECT 1 FROM ods.dim d WHERE d.id = u.dim_id)`
     → 只溯源 `ods.user.id → a`；不要为 `u.status`、`ods.dim` / `d.id` 建血缘
9. 无法确定的字段可省略；完全无法解析时返回 parse_ok=false
10. **同来源、不同目标字段名：全部保留（强制）**
   - 若多个目标字段来自同一最底层源列/同一表达式，但目标字段名不同，必须各自输出一条 fields 与对应 field_relations，禁止合并、去重或只留一条
   - 例：`INSERT INTO dws.t (user_id, uid) SELECT a.id, a.id FROM ods.user a`
     → 两条 fields：`user_id` 与 `uid`，source_columns 都可为 `["ods.user.id"]`
     → 两条 field_relations：`id → user_id`、`id → uid`
   - 例：`SELECT a.name AS name_cn, a.name AS name_en FROM ods.user a`
     → `name_cn`、`name_en` 两条都保留，不要因为源列相同而删掉其中一条
   - 判断键是目标字段名（target_field / field_name）是否不同，不是 source 是否相同

## source_columns / source_field：必须最底层（强制）
`source_columns`（fields 内）与 `source_field`（field_relations 内）一律输出穿透后的最底层物理列，禁止停在中间层。

要求：
1. 穿透 CTE、子查询别名、视图式中间结果，直到真实物理表上的列
2. 推荐格式：`"schema.table.column"` 或至少 `"table.column"`；不要只写别名列如 `"u.id"` / `"cte.col"` / `"sub.calc_id"`（除非无法再往下追溯）
3. 表达式含多个底层列时，`source_columns` 列出全部最底层列；`field_relations` 可为每个底层列各出一条
4. `source_expr` 仍保留原始投影表达式；不要把函数名、常量当作 source_columns
5. 与 source_tables / source_table 一致：表名也必须是对应的最底层物理表
6. 查询语句中where或WHERE条件后的字段禁止作为数据表、字段的source_table / source_columns溯源属性，不得作为 source_table / source_columns 输出。
7. 子查询或关联查询语句中当SELECT查询的字段不使用{数据表别名.字段}，即FROM后来源表的别名+字段查询时，默认取FROM主表作为source_table / source_columns 输出。

正确示例：
WITH s AS (SELECT a.id AS aid FROM ods.user a)
INSERT INTO dws.t (user_id) SELECT aid FROM s
→ source_columns: ["ods.user.id"]
→ source_field: "id"，source_table: "ods.user"
→ 错误：source_columns: ["s.aid"] 或 ["aid"]

SELECT sub.calc_id FROM (SELECT c.calc_id FROM un_cms20.chart c) sub
→ source_columns: ["un_cms20.chart.calc_id"]
→ 错误：source_columns: ["sub.calc_id"]


例如：
INSERT INTO buf_pls_loss_sub_admin.ads_grid_tqxs_loss_arch_equip_switch_df(VOLT_LEVEL)
SELECT
t.VOLT_LEVEL
FROM (
  SELECT
  (
    select
    lo.loss_code
    from buf_pls_loss_sub_admin.ads_grid_tqxs_loss_pms_pub_code_df lo
    where
    lo.type_code = 'VOLT_LEVEL'
    and lo.pms_code = tb.voltage_level
    and lo.org_id = 'F017B5E52AD95DA8E043621DE60A04B4'
  ) VOLT_LEVEL
  FROM buf_psrmg_powerresourcedb.t_ast_ds_breaker at,buf_psrmg_powerresourcedb.t_psr_ds_p_breaker tb
) t
正确：
source_table = "buf_pls_loss_sub_admin.ads_grid_tqxs_loss_pms_pub_code_df"
source_columns = ["buf_pls_loss_sub_admin.ads_grid_tqxs_loss_pms_pub_code_df.loss_code"]
source_field = "loss_code"
错误：
"source_columns": [
                "buf_pls_loss_sub_admin.ads_grid_tqxs_loss_pms_pub_code_df.loss_code",
                "buf_psrmg_powerresourcedb.t_psr_ds_p_breaker.voltage_level"
            ]
 "source_tables": [
                "buf_pls_loss_sub_admin.ads_grid_tqxs_loss_pms_pub_code_df",
                "buf_psrmg_powerresourcedb.t_psr_ds_p_breaker"
            ]

例如：
insert into ads_cst_zhga_query_cons_base_company_info
select
distinct cert_type as cert_type_code,
t6.cust_no AS cust_no
from buf_cms20_hayxhx.elec_cons_cust t1
left join  buf_cms20_hayxhx.CERT_SET t6 
on t1.cust_no = t6.cons_no

正确：

source_table = "buf_cms20_hayxhx.elec_cons_cust"
source_columns = ["buf_cms20_hayxhx.elec_cons_cust.cert_type"]
source_field = "cert_type"

错误：
source_columns: ["buf_cms20_hayxhx.CERT_SET.cert_type"]
source_tables: ["buf_cms20_hayxhx.CERT_SET"]

## 子查询（Derived Table）规则（强制）
若字段来自子查询（FROM (...) alias）：
1. alias 仅作为中间层，不得作为 source_table / source_columns 输出；
2. 必须继续追溯到子查询内部真实来源表与最底层列；
3. field_relations、fields、table_relations 中的 source_table 必须是真实物理表；
4. source_columns 必须是最底层物理列（如 schema.table.column），禁止输出 CTE/子查询别名列；
5. 禁止输出 CTE 名称、子查询别名、临时表别名作为 source_table。
例如：

SELECT sub.calc_id
FROM (
    SELECT c.calc_id
    FROM un_cms20.un94_02_cms20_inst_bilg_card b
    LEFT JOIN  un_cms20.chart c
) sub

正确：
source_table = "un_cms20.chart"
source_columns = ["un_cms20.chart.calc_id"]
source_field = "calc_id"

错误：
source_table = "sub"
source_columns = ["sub.calc_id"]

## 血缘 relation 字段取值约定
| relation 值 | 含义 |
|-------------|------|
| insert_select | INSERT ... SELECT 标准形式 |
| insert_values | INSERT ... VALUES 直接值形式 |
| insert_upsert | ON CONFLICT DO UPDATE 形式 |

## 时间/日期函数：一律过滤（强制）
凡投影/赋值的来源本质是时间、日期、时间戳类函数时，直接忽略，不要写入 fields、field_relations，也不要把函数名当成 source_field。

判定与处理：
1. 整式仅为时间/日期函数（无真实表字段）→ 该目标列完全跳过，不输出
   例：current_timestamp()、now()、sysdate、getdate()、CURRENT_DATE、CURRENT_TIMESTAMP、
       localtimestamp、systimestamp、unix_timestamp()、from_unixtime(unix_timestamp())、
       date_format(current_timestamp(),'yyyyMMdd')、to_char(sysdate,'yyyymmdd')、
       trunc(sysdate)、cast(current_date as string) 等
2. 字面量日期/时间（如 '2024-01-01'、DATE '2024-01-01'、TIMESTAMP'...'）同理跳过
3. 表达式里嵌套了时间函数但仍引用真实列 → 只保留真实列血缘，忽略时间函数本身
   例：date_add(a.dt, 1) / date_format(u.create_time,'yyyy-MM-dd')
       → source_field 取 a.dt / u.create_time，source_table 取对应真实表；不要输出 current_* / date_add 等函数名作为来源
4. 下列名称（大小写不敏感，含常见方言）均视为时间函数，出现为唯一来源时过滤：
   current_date, current_timestamp, current_time, localtime, localtimestamp,
   now, sysdate, systimestamp, getdate, getutcdate, curdate, curtime,
   unix_timestamp, from_unixtime, to_unix_timestamp, from_utc_timestamp, to_utc_timestamp,
   date_format, to_date, to_timestamp, to_char(仅日期时间场景), trunc(日期), date_trunc,
   date_add, date_sub, datediff, months_between, add_months, last_day, next_day,
   extract, date_part, year, month, day, hour, minute, second（仅对常数/时间函数取值时）,
   timestamp, date（构造函数且参数非表字段时）
5. 禁止为时间函数编造 source_table / source_field；禁止把函数结果当成字段血缘

## 输出要求
只输出 json，不要输出其他内容，不要使用 markdown/三引号代码块（禁止 ```json 与 '''json），结构如下：
{
  "parse_ok": true,
  "parse_error": null,
  "target_table": "schema.table",
  "table_name_cn": null或字符串,
  "star_source_tables": ["ods.src"],
  "pending_target_columns": [],
  "fields": [
    {
      "target_table": "schema.table",
      "field_name": "col",
      "name_cn": null或字符串,
      "source_expr": "u.col AS col",
      "source_columns": ["ods.src.col"],
      "source_tables": ["ods.src"]
    }
  ],
  "table_relations": [
    {"source_table": "ods.src", "target_table": "schema.table", "relation": "insert_select"}
  ],
  "field_relations": [
    {
      "source_table": "ods.src",
      "source_field": "col",
      "target_table": "schema.table",
      "target_field": "col",
      "source_expr": "u.col",
      "relation": "insert_select"
    }
  ]
}
"""


async def parse_insert_sql_with_llm(
    sql: str,
    *,
    schema_name: Optional[str] = None,
    app_name: Optional[str] = None,
) -> dict:
    """
    用大模型解析 INSERT SQL，返回与 InsertSqlParseItem 对齐的 dict（不含 parse_source/raw_sql）。
    调用失败或 JSON 非法时最多重试 llm_max_retries 次，仍失败则抛出异常，由上层捕获。
    """
    settings = get_settings()
    context_bits: list[str] = []
    if app_name:
        context_bits.append(f"工作空间：{app_name}")
    if schema_name:
        context_bits.append(f"schema_name：{schema_name}")
    context = ("\n".join(context_bits) + "\n\n") if context_bits else ""

    user_prompt = (
        f"{context}"
        f"请解析以下 INSERT SQL：\n```sql\n{sql}\n```"
    )
    logger.info("调用 LLM 解析 INSERT SQL, sql_length=%s", len(sql))

    async def _once() -> dict:
        async with httpx.AsyncClient(base_url=settings.llm_base_url, timeout=settings.llm_timeout_seconds) as client:
            resp = await client.post(
                "/chat/completions",
                headers={"Authorization": f"{settings.llm_api_key}"},
                json={
                    "model": settings.llm_model,
                    "messages": [
                        {"role": "system", "content": _INSERT_SQL_PARSE_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0,
                    "response_format": {"type": "json_object"},
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]

        parsed = _loads_llm_json(content)
        if not isinstance(parsed, dict):
            raise ValueError("LLM 返回的不是 JSON 对象")
        return parsed

    parsed = await _with_llm_retry("LLM INSERT SQL 解析", _once)
    logger.info(
        "LLM INSERT SQL 解析完成: parse_ok=%s, fields=%s",
        parsed.get("parse_ok"),
        len(parsed.get("fields") or []),
    )
    return parsed


_SQL_JOIN_EXTRACT_SYSTEM_PROMPT = """
你是 SQL 元数据血缘解析器。你的唯一任务是穷举 SQL 中所有“不同物理表字段之间的等值关联谓词”，并逐谓词输出表关联。准确性和完整性优先，禁止猜测。

## 任务
给定一条 SQL（常见为 INSERT ... SELECT，也可能是纯 SELECT / 含 WITH CTE / 含临时表 / 逗号多表），
提取其中**全部**物理表等值关联边。

## 【最高优先级】唯一输出粒度
1. 一个有效等值谓词对应 `joins` 数组中的一条记录，严格一对一。
2. 即使多个谓词连接相同的两张表，也必须分别输出，禁止合并字段或表达式。
3. 一个谓词只能输出一次，禁止重复。
4. `a.id=b.a_id AND a.org_id=b.org_id` 必须输出 2 条，不能输出 1 条。

每条关联区分：
- **primary**：等值谓词左侧的物理表
- **secondary**：等值谓词右侧的物理表

核心目标：输出 SQL 里体现的**每一对最底层物理表之间的等值关联边**。
不要只保留 JOIN 关键字那一条边；不要停在别名 / CTE / 临时表 / 子查询名。

## 【强制】四步工作流（必须按顺序在内心完成，再输出 json）
1. **建别名映射 alias_map**
   - 扫描整条 SQL（含 WITH / 子查询 / 临时表定义）中所有：
     `schema.table alias`、`table alias`、`table AS alias`、`FROM (SELECT ...) alias`
   - 写成 alias → 最底层物理表（`schema.table` 或 `table`）
   - 短别名（a/b/c/t/mp/cust 等）必须映射到真实表，绝不能把别名当表名输出
2. **先枚举全部等值谓词，禁止边找边输出**
   - 每个 JOIN / LEFT JOIN / RIGHT JOIN / INNER JOIN / FULL JOIN / CROSS JOIN
   - USING(...)
   - 逗号多表 / 旧式多表：`FROM a, b WHERE a.x=b.y`
   - ON / WHERE 中所有 `表或别名.字段 = 表或别名.字段`（两侧指向不同物理表）
   - CTE / 临时表 / 子查询**内部**的关联也要枚举
   - 按 SQL 从内到外、从左到右，把候选谓词编号为 P1、P2、P3……
   - 每个 `X.col = Y.col` 单独编号；同一个 ON 中由 AND 连接的多个等值条件不能合并
3. **逐谓词展开到物理表并生成结果**
   - 用 alias_map 把边上的别名、CTE、临时表、子查询别名全部替换为物理表
   - 字段若来自 CTE 透传列，追溯到源头物理表字段后再输出
   - 对每个有效 Pi 恰好生成一条记录；无法唯一追溯到物理字段时跳过，禁止猜测
   - primary 固定为谓词左侧，secondary 固定为谓词右侧
   - 两侧 join_fields 都只能包含当前谓词的一个字段
4. **自检（不通过就修正后再输出）**
   - joins 里任何 table_name **不得**等于 SQL 中的别名、CTE 名、临时表名、子查询别名
   - join_expression 里的表名必须与 primary/secondary 的物理表一致（禁止写别名）
   - 令 N=可展开到不同物理表的有效等值谓词数，必须满足 `len(joins) = N`
   - 按 P1...PN 逐项核销，每个有效谓词恰好出现一次，不能遗漏或重复
   - 同一 JOIN 的 ON 含多组跨表等值时，每组必须是独立记录
   - 每条 join_expression 只能包含当前一个谓词，禁止用 AND 合并其他谓词

## 每条关联需输出
1. primary / secondary：schema_name、table_name、join_fields
2. join_type：left / right / inner / full / cross / comma，小写
3. join_expression：只描述当前一个等值谓词，如 `schema.t1.id = schema.t2.fid`
   - 只用物理表名；一条 joins 只描述一个谓词和一对物理表；禁止三表或多个谓词塞进同一条

## 规则
1. 只根据 SQL 推断，不要编造不存在的表/字段
2. schema_name：SQL 写了 schema.table 则拆出 schema；未写则为 null。table_name **不含** schema，且必须是物理表名
3. join_fields 只列当前谓词两侧的一个字段名（不含表前缀），每侧数组长度必须为 1；USING(col) 则两侧都为该列
4. 多表连环 JOIN：每个等值谓词单独一条；primary=谓词左侧物理表，secondary=谓词右侧物理表
5. **ON / USING 多层拆边（强制）**：
   - 把 ON 拆成若干 `X.col = Y.col`；凡不同物理表对 (X,Y) 各输出一条
   - 例：`join buf_cms20_hayxhx.inst_elec_cons mp on c.srv_loc_elec_id=mp.srv_loc_id and mp.cust_id=cust.cust_id`
     若 c→phys_c、cust→phys_cust、mp→inst_elec_cons，则必须输出 2 条：
     a) phys_c ↔ inst_elec_cons
     b) inst_elec_cons ↔ phys_cust
     禁止输出 c/mp/cust 这种别名；禁止只输出其中一条
6. INSERT 目标表不是 JOIN 对象（除非它也出现在 FROM/JOIN 中）
7. **WHERE / ON 中的跨表等值都要提取**：
   - 提取范围：FROM/JOIN/ON/USING，以及 CTE/临时表/子查询内部同类结构
   - 另：多表 FROM（含逗号连接）时，WHERE 里两侧指向不同物理表的等值也要提取，join_type=comma
   - 不要提取：与常量比较、单表谓词、无关函数条件
8. 同一对物理表的多组等值字段严禁合并；每个谓词各输出一条，同时不要输出完全重复的谓词
9. 无任何多表关联：parse_ok=true，joins=[]，alias_map 仍尽量给出
10. 完全无法解析：parse_ok=false，填写 parse_error

## 【强制】禁止输出的“假表名”
下列一律不能出现在 primary.table_name / secondary.table_name / join_expression 中：
- 表别名：a、b、c、t、t1、mp、cust、u、o 等
- CTE 名、WITH 子句名
- 临时表名：tmp_* / temp_* / 会话临时表
- 子查询别名：FROM (SELECT ...) x 中的 x
- 仅作容器的中间结果名

必须递归展开到 SQL 中可识别的最底层物理基表（优先保留 schema.table）。

### 别名反例（禁止）
错误：
```json
{"primary":{"table_name":"c"}, "secondary":{"table_name":"mp"}}
```
正确（先映射 c→真实表、mp→真实表后再输出）：
```json
{"primary":{"schema_name":"buf_xxx","table_name":"elec_cons"}, "secondary":{"schema_name":"buf_cms20_hayxhx","table_name":"inst_elec_cons"}}
```

### 嵌套 CTE 示例（必须遵循）
输入：
```sql
WITH t1 AS (
  SELECT a.id AS aid, a.name
  FROM ods.user a
  LEFT JOIN ods.profile p ON a.id = p.user_id
),
t2 AS (
  SELECT t1.aid, o.amt
  FROM t1
  INNER JOIN ods.order o ON t1.aid = o.user_id
)
SELECT t2.aid, py.pay_id
FROM t2
LEFT JOIN ods.pay py ON t2.aid = py.user_id
```
正确 joins（只保留物理表，且必须 3 条）：
1) ods.user ↔ ods.profile
2) ods.user ↔ ods.order
3) ods.user ↔ ods.pay
错误：出现 t1/t2/a/p/o/py；或只输出外层一条边。

### 多 JOIN + 复合 ON 示例（数量必须精确）
输入：
```sql
SELECT *
FROM ods.customer c
LEFT JOIN ods.orders o
  ON c.id = o.customer_id
 AND c.tenant_id = o.tenant_id
INNER JOIN ods.payment p
  ON o.id = p.order_id
WHERE c.region_id = p.region_id
  AND c.status = 'VALID'
```
有效跨表等值谓词 N=4，因此 joins 必须恰好返回 4 条：
1) ods.customer.id = ods.orders.customer_id，join_type=left
2) ods.customer.tenant_id = ods.orders.tenant_id，join_type=left
3) ods.orders.id = ods.payment.order_id，join_type=inner
4) ods.customer.region_id = ods.payment.region_id，join_type=comma
`c.status = 'VALID'` 是字段与常量比较，不输出。

## 输出稳定性
1. 同样 SQL → 同样 joins 集合（左右表、单个字段对、join_type）
2. 不确定的中间层一律继续向下展开，不要省略
3. table_name / join_fields 保持 SQL 原始大小写；join_type 小写
4. 只输出 json，不要 markdown / 解释文字

## 输出 json 结构
{
  "parse_ok": true,
  "parse_error": null,
  "alias_map": {
    "a": "ods.user",
    "p": "ods.profile",
    "mp": "buf_cms20_hayxhx.inst_elec_cons"
  },
  "joins": [
    {
      "primary": {
        "role": "primary",
        "schema_name": "ods",
        "table_name": "user",
        "join_fields": ["id"]
      },
      "secondary": {
        "role": "secondary",
        "schema_name": "ods",
        "table_name": "order",
        "join_fields": ["user_id"]
      },
      "join_type": "left",
      "join_expression": "ods.user left join ods.order on ods.user.id=ods.order.user_id"
    }
  ]
}
说明：alias_map 必填（可空对象），用于自检；joins 中表名必须已是物理表。
"""


async def extract_sql_joins_with_llm(
    sql: str,
    *,
    schema_name: Optional[str] = None,
    app_name: Optional[str] = None,
) -> dict:
    """
    用大模型从 SQL 提取表关联（主表/从表/关联表达式），返回 dict。
    """
    settings = get_settings()
    context_bits: list[str] = []
    if app_name:
        context_bits.append(f"工作空间：{app_name}")
    if schema_name:
        context_bits.append(f"默认 schema_name（仅当 SQL 未写 schema 时可参考）：{schema_name}")
    context = ("\n".join(context_bits) + "\n\n") if context_bits else ""

    user_prompt = (
        f"{context}"
        "请提取以下 SQL 中的全部表关联关系。\n"
        "必须先构建 alias_map（别名/CTE/临时表 → 物理表），再输出 joins。\n"
        "硬性要求：\n"
        "1) joins 的 table_name / join_expression 禁止出现别名，只能是物理表名；\n"
        "2) 先逐个编号全部跨表等值谓词，再生成结果；一个谓词必须恰好对应一条 joins 记录；\n"
        "3) CTE/临时表/子查询内部关联也要提取并展开；\n"
        "4) 同一表对的多个等值谓词严禁合并，每侧 join_fields 必须恰好一个字段；\n"
        "5) primary=谓词左侧，secondary=谓词右侧；\n"
        "6) 输出前核对：有效谓词数 N 必须等于 len(joins)，并用 alias_map 清除全部别名。\n"
        f"```sql\n{sql}\n```"
    )
    logger.info("调用 LLM 提取 SQL 表关联, sql_length=%s", len(sql))

    async def _once() -> dict:
        async with httpx.AsyncClient(base_url=settings.llm_base_url, timeout=settings.llm_timeout_seconds) as client:
            resp = await client.post(
                "/chat/completions",
                headers={"Authorization": f"{settings.llm_api_key}"},
                json={
                    "model": settings.llm_model,
                    "messages": [
                        {"role": "system", "content": _SQL_JOIN_EXTRACT_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0,
                    "top_p": 1,
                    "seed": 42,
                    "response_format": {"type": "json_object"},
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]

        parsed = _loads_llm_json(content)
        if not isinstance(parsed, dict):
            raise ValueError("LLM 返回的不是 JSON 对象")
        return parsed

    parsed = await _with_llm_retry("LLM SQL 表关联提取", _once)
    logger.info(
        "LLM SQL 表关联提取完成: parse_ok=%s, joins=%s",
        parsed.get("parse_ok"),
        len(parsed.get("joins") or []),
    )
    return parsed


_SQL_FIELD_RELATION_SYSTEM_PROMPT = """
你是元数据治理专家，擅长从 SQL 中抽取**字段级**关系边。

## 任务
给定一条 SQL（常见 INSERT ... SELECT，也可是纯 SELECT / WITH CTE），输出全部字段关系，每条含：
- source_schema_name / source_table_name / source_field_name
- target_schema_name / target_table_name / target_field_name
- rel_type：只能是 fdd | join | fdr
- exp_fragment：该关系对应的 SQL 片段原文（尽量贴近原 SQL）

## rel_type 定义（强制）
1. **fdd**（血缘关系）
   - INSERT 目标列 ← SELECT/VALUES 投影中的最底层物理源列
   - source = 源表字段，target = INSERT 目标表字段
   - 不含 JOIN ON、不含 WHERE 条件列
   - exp_fragment：投影/赋值片段，如 `u.id AS user_id` 或 `u.id`
2. **join**（JOIN 中的关系）
   - 来自 JOIN / LEFT|RIGHT|INNER|FULL JOIN 的 **ON / USING** 中的跨表等值：`A.col = B.col`
   - source / target 为等值两侧物理表字段（按 SQL 书写左右顺序：左侧=source，右侧=target）
   - 不含 WHERE 谓词
   - exp_fragment：ON/USING 中的等值片段，如 `u.id = o.user_id` 或 `USING (id)`
3. **fdr**（WHERE 中的关联关系）
   - 来自 **WHERE**（含 HAVING、以及子查询/CTE 内部 WHERE）中两侧指向**不同物理表**的等值：`A.col = B.col`
   - 含旧式多表 `FROM a, b WHERE a.x = b.y` 的等值
   - 不含 JOIN ON；不含与常量比较、单表谓词
   - exp_fragment：WHERE 中的等值片段，如 `u.dept_id = d.id`

## 工作流
1. 建立 alias_map：别名 / CTE / 临时表 / 子查询别名 → 最底层物理表（schema.table 或 table）
2. 分别收集三类边，全部展开到物理表与物理列
3. schema：SQL 写了 schema.table 则拆出 schema_name，table_name 不含 schema；未写则 schema_name 为 null
4. 自检：任何 table_name 不得是别名/CTE/临时表名；rel_type 只能是 fdd/join/fdr

## 规则
1. 只根据 SQL 推断，禁止编造
2. 字段名不含表前缀
3. SELECT * / t.*：若无法展开具体列，可跳过对应 fdd；能推断的仍输出
4. 表达式含多列时，fdd 可为每个底层源列各出一条到同一目标列
5. 时间/日期函数结果不当作源字段；常量不当作字段边
6. 同一边不要重复输出；join 与 fdr 不要混标
7. 无任何关系：parse_ok=true，relations=[]
8. 完全无法解析：parse_ok=false，填写 parse_error，relations=[]

## 输出 JSON（仅此结构）
{
  "parse_ok": true,
  "parse_error": null,
  "alias_map": {"u": "ods.user"},
  "relations": [
    {
      "source_schema_name": "ods",
      "source_table_name": "user",
      "source_field_name": "id",
      "target_schema_name": "dws",
      "target_table_name": "fact",
      "target_field_name": "user_id",
      "rel_type": "fdd",
      "exp_fragment": "u.id AS user_id"
    },
    {
      "source_schema_name": "ods",
      "source_table_name": "user",
      "source_field_name": "id",
      "target_schema_name": "ods",
      "target_table_name": "order",
      "target_field_name": "user_id",
      "rel_type": "join",
      "exp_fragment": "u.id = o.user_id"
    },
    {
      "source_schema_name": "ods",
      "source_table_name": "user",
      "source_field_name": "dept_id",
      "target_schema_name": "ods",
      "target_table_name": "dept",
      "target_field_name": "id",
      "rel_type": "fdr",
      "exp_fragment": "u.dept_id = d.id"
    }
  ]
}
"""


async def extract_sql_field_relations_with_llm(
    sql: str,
    *,
    app_name: Optional[str] = None,
) -> dict:
    """用大模型抽取 SQL 字段级关系（fdd/join/fdr），返回 dict。"""
    settings = get_settings()
    context_bits: list[str] = []
    if app_name:
        context_bits.append(f"工作空间：{app_name}")
    context = ("\n".join(context_bits) + "\n\n") if context_bits else ""

    user_prompt = (
        f"{context}"
        "请抽取以下 SQL 的字段级关系（rel_type=fdd/join/fdr）。\n"
        "硬性要求：\n"
        "1) fdd=血缘投影；join=JOIN ON/USING；fdr=WHERE 跨表等值；三类不得混标；\n"
        "2) 表名必须是物理表，禁止别名；\n"
        "3) schema 与 table 拆开输出；\n"
        "4) 每条必须带 exp_fragment（对应 SQL 片段原文）。\n"
        f"```sql\n{sql}\n```"
    )
    logger.info("调用 LLM 抽取 SQL 字段关系, sql_length=%s", len(sql))

    async def _once() -> dict:
        async with httpx.AsyncClient(
            base_url=settings.llm_base_url, timeout=settings.llm_timeout_seconds
        ) as client:
            resp = await client.post(
                "/chat/completions",
                headers={"Authorization": f"{settings.llm_api_key}"},
                json={
                    "model": settings.llm_model,
                    "messages": [
                        {"role": "system", "content": _SQL_FIELD_RELATION_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0,
                    "top_p": 1,
                    "seed": 42,
                    "response_format": {"type": "json_object"},
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]

        parsed = _loads_llm_json(content)
        if not isinstance(parsed, dict):
            raise ValueError("LLM 返回的不是 JSON 对象")
        return parsed

    parsed = await _with_llm_retry("LLM SQL 字段关系抽取", _once)
    logger.info(
        "LLM SQL 字段关系抽取完成: parse_ok=%s, relations=%s",
        parsed.get("parse_ok"),
        len(parsed.get("relations") or []),
    )
    return parsed
