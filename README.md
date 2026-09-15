# 元数据补全服务

单接口 `POST /metadata/completion`，严格按你描述的四步实现：

```
第一步  遍历 field 表，取出 name_cn 为空的字段
第二步  ArangoDB 图谱溯源
        跨 table_contain_field / table_build_table / field_build_field 三张关系表，
        ANY 方向混合遍历，找离缺失字段最近的、name_cn 已有值的顶点（field 或 table 都算）
          命中 -> 结果 source=graph
第三步  未命中 -> 在图谱邻域内找已有 name_cn 的候选 field，向量化后与缺失字段做余弦相似度
          没有候选 / 最高相似度 <= 阈值 -> 结果 source=manual_pending
          相似度达标 -> 取最相似候选，英文名+候选中文名交给 LLM 评分
              confidence <= 阈值 -> 结果 source=manual_pending（附带候选值供参考）
              confidence >  阈值 -> 结果 source=llm
第四步  以上三种结果统一写入 Redis
```

## 代码结构

| 步骤 | 代码位置 |
|---|---|
| 第一步 遍历缺失字段 | `app/services/graph_source.py::fetch_missing_fields` |
| 第二步 图谱溯源 | `app/services/graph_source.py::trace_via_graph` |
| 第三步 候选向量化 + 余弦相似度 | `app/services/vector_match.py` |
| 第三步 LLM 评分 | `app/clients/llm.py` + `app/services/llm_score.py` |
| 第四步 保存 Redis | `app/services/result_store.py` |
| 整体编排 | `app/services/pipeline.py` |
| FastAPI 入口 | `app/main.py` |

## ⚠️ 需要你确认的假设

你的需求里只提到 `table` / `field` 两个实体表都有 `name_cn` 属性，没提英文名/标识字段叫什么，
我假设它叫 **`name`**（`.env` 里 `ENTITY_NAME_ATTR` 可改）。如果你实际的属性名不是 `name`
（比如 `field_name` / `en_name` 之类），改一下 `.env` 就行，不用碰代码。

另外几个我做了默认选择、但你可能想调整的地方：

1. **图谱溯源的"命中"范围**：我让 `table`、`field` 两种顶点只要有 `name_cn` 都算命中
   （比如字段挂在某张表下，表本身有中文名，也可以作为溯源来源）。如果你只想认"字段"
   互相溯源、不想让表的中文名参与，告诉我，我改成只筛 `field` 顶点。
2. **溯源跳数**：`MAX_TRACE_DEPTH=3`，三张关系表混着走，跳数越大越可能溯源到语义上不相关
   的字段。第三步向量匹配的候选范围也复用同一个跳数（可以在请求体里分开传 `max_trace_depth`
   覆盖默认值）。
3. **第三步候选池大小**：`MAX_VECTOR_CANDIDATES=20`，避免某个字段在图谱里关联字段特别多时
   一次性对着几十上百个字段算 embedding、拖慢单次请求。

## 快速开始

```bash
pip install -r requirements.txt
cp .env.example .env   # 按实际环境改配置，尤其是 ENTITY_NAME_ATTR
uvicorn app.main:app --reload
```

打开 `http://127.0.0.1:8000/docs` 调试接口：

```bash
curl -X POST http://127.0.0.1:8000/metadata/completion \
  -H "Content-Type: application/json" \
  -d '{"limit": 200}'
```

返回：

```json
{
  "total_fields": 80,
  "graph_hit": 30,
  "llm_matched": 28,
  "manual_pending": 22,
  "manual_pending_no_candidate": 5,
  "manual_pending_similarity_low": 10,
  "manual_pending_confidence_low": 7,
  "errors": 0
}
```

请求体所有字段都可选：

```json
{
  "limit": 500,
  "max_trace_depth": 3,
  "similarity_threshold": 0.8,
  "confidence_threshold": 0.9
}
```

## Redis 里的结果长什么样

key 是 `meta-completion:{field_id}`（前缀可在 `.env` 改），value 是 JSON。同一 `field_id` 已存在时会跳过，不重复写入：

```json
{
  "field_id": "field/123",
  "name_en": "usr_addr",
  "name_cn": "用户地址",
  "source": "llm",
  "reason": null,
  "via_vertex": "field/456",
  "similarity": 0.87,
  "confidence": 0.93,
  "saved_at": "2026-07-09T10:00:00"
}
```

`source=manual_pending` 时 `reason` 会是 `no_candidate` / `similarity_low` / `confidence_low`
三者之一，`name_cn` 可能为空（没有任何候选时），也可能是相似度/置信度不够但仍供人工参考的候选值。

## 需要你准备的外部资源

- **ArangoDB**：`table`、`field` 两个 vertex collection，`table_contain_field` /
  `table_build_table` / `field_build_field` 三个 edge collection，按你现有图谱结构即可，
  本服务不建库建表。
- **Redis**：任意实例，只用 `SET`/`GET`。
- **Embedding / LLM**：都走 OpenAI 兼容协议，`.env` 里各自配置 `base_url` / `api_key` / `model`，
  可以接官方 API，也可以接自建兼容网关。
