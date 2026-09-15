"""
流水线并发控制：用 Semaphore 限制同时处理的记录数，stats 用锁保证并发安全。
"""
import asyncio
from typing import Awaitable, Callable, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class LockedStats:
    """并发场景下安全地累加 RunStats / TableRunStats 计数。"""

    def __init__(self, stats: BaseModel) -> None:
        self.stats = stats
        self._lock = asyncio.Lock()

    async def add(self, **increments: int) -> None:
        async with self._lock:
            for key, delta in increments.items():
                setattr(self.stats, key, getattr(self.stats, key) + delta)


def resolve_concurrency(request_value: int | None, default: int) -> int:
    """请求体优先，否则用配置默认值；至少为 1。"""
    if request_value is not None:
        return max(1, request_value)
    return max(1, default)


async def run_bounded(
    items: list[T],
    concurrency: int,
    handler: Callable[[T], Awaitable[None]],
) -> None:
    """最多 concurrency 条记录同时执行 handler。"""
    if not items:
        return
    sem = asyncio.Semaphore(max(1, concurrency))

    async def _run(item: T) -> None:
        async with sem:
            await handler(item)

    await asyncio.gather(*(_run(item) for item in items))
