"""
OpenAI 兼容的 Embeddings 客户端，用于第三步"将关联的字段进行向量化"。
"""
import httpx

from app.config import get_settings


async def embed_batch(texts: list[str]) -> list[list[float]]:
    """批量算向量，减少候选字段较多时的请求次数。"""
    if not texts:
        return []
    settings = get_settings()
    async with httpx.AsyncClient(base_url=settings.embedding_base_url, timeout=60.0) as client:
        resp = await client.post(
            "/embeddings",
            headers={"Authorization": f"{settings.embedding_api_key}"},
            json={"model": settings.embedding_model, "input": texts},
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        data.sort(key=lambda d: d.get("index", 0))
        return [d["embedding"] for d in data]
