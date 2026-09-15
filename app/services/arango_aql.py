"""
ArangoDB AQL 查询封装：超时自动重试，重试过程不打 error/exception 日志。
"""
import logging
import time
from typing import Any

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

from app.config import get_settings
from app.db import get_arango_db

logger = logging.getLogger("metadata_pipeline.arango")

_RETRYABLE_EXCEPTIONS = (
    TimeoutError,
    RequestsTimeout,
    RequestsConnectionError,
    ConnectionError,
    OSError,
)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, _RETRYABLE_EXCEPTIONS):
        return True
    msg = str(exc).lower()
    return "timeout" in msg or "timed out" in msg


def execute_aql(query: str, bind_vars: dict | None = None, **execute_kwargs: Any) -> list[dict]:
    """
    执行 AQL 并返回全部行。超时等可重试错误最多重试 arango_max_retries 次；
    重试期间仅 debug 日志，全部失败后 warning 一次并返回空列表（不抛异常）。
    """
    settings = get_settings()
    max_retries = settings.arango_max_retries
    base_delay = settings.arango_retry_base_delay
    db = get_arango_db()
    bind_vars = bind_vars or {}

    for attempt in range(max_retries + 1):
        try:
            cursor = db.aql.execute(query, bind_vars=bind_vars, **execute_kwargs)
            return list(cursor)
        except Exception as exc:
            if not _is_retryable(exc) or attempt >= max_retries:
                if _is_retryable(exc):
                    logger.warning(
                        "ArangoDB 查询超时，已重试 %s 次仍失败，返回空结果",
                        max_retries,
                    )
                else:
                    logger.warning("ArangoDB 查询失败: %s", exc)
                    err = str(exc).lower()
                    if "501" in err or "syntax" in err or "parse" in err:
                        logger.warning("AQL 语法错误，查询片段: %s", query[:800])
                        logger.warning("bind_vars keys: %s", sorted(bind_vars.keys()))
                return []

            delay = base_delay * (2**attempt)
            logger.debug(
                "ArangoDB 查询超时，第 %s/%s 次重试，%.1fs 后重试",
                attempt + 1,
                max_retries,
                delay,
            )
            time.sleep(delay)

    return []
