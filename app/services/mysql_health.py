"""MySQL 连接与表存在性检查。"""
from __future__ import annotations

import logging
from typing import Any, Optional

import anyio

from app.config import get_settings
from app.db import get_mysql_conn
from app.schemas import MysqlHealthResponse, MysqlTableCheckItem
from app.services.mysql_schema import quote_ident

logger = logging.getLogger(__name__)


def _table_exists(cur: Any, database: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (database, table),
    )
    return cur.fetchone() is not None


def _safe_count(cur: Any, table: str) -> Optional[int]:
    try:
        cur.execute(f"SELECT COUNT(*) AS cnt FROM {quote_ident(table)}")
        row = cur.fetchone() or {}
        return int(row.get("cnt") or 0)
    except Exception as e:
        logger.warning("统计表行数失败: table=%s err=%s", table, e)
        return None


def _check_mysql_sync() -> MysqlHealthResponse:
    settings = get_settings()
    host = settings.mysql_host
    port = settings.mysql_port
    database = settings.mysql_database

    expected: list[tuple[str, str]] = [
        (settings.mysql_sql_source_table, "source_readonly"),
        (settings.mysql_table_lineage_table, "result"),
        (settings.mysql_field_lineage_table, "result"),
        (settings.mysql_field_meta_table, "result"),
        (settings.mysql_table_join_table, "result"),
    ]

    try:
        with get_mysql_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION() AS version")
                ver_row = cur.fetchone() or {}
                version = str(ver_row.get("version") or "") or None

                tables: list[MysqlTableCheckItem] = []
                missing: list[str] = []
                for name, role in expected:
                    try:
                        exists = _table_exists(cur, database, name)
                        row_count = _safe_count(cur, name) if exists else None
                        tables.append(
                            MysqlTableCheckItem(
                                name=name,
                                role=role,
                                exists=exists,
                                row_count=row_count,
                            )
                        )
                        if not exists:
                            missing.append(name)
                    except Exception as e:
                        tables.append(
                            MysqlTableCheckItem(
                                name=name,
                                role=role,
                                exists=False,
                                error=str(e),
                            )
                        )
                        missing.append(name)

                return MysqlHealthResponse(
                    ok=len(missing) == 0,
                    connected=True,
                    host=host,
                    port=port,
                    database=database,
                    server_version=version,
                    tables=tables,
                    missing_tables=missing,
                )
    except Exception as e:
        logger.exception("MySQL 健康检查失败")
        return MysqlHealthResponse(
            ok=False,
            connected=False,
            host=host,
            port=port,
            database=database,
            error=str(e),
            tables=[
                MysqlTableCheckItem(name=name, role=role, exists=False)
                for name, role in expected
            ],
            missing_tables=[name for name, _ in expected],
        )


async def check_mysql_health() -> MysqlHealthResponse:
    return await anyio.to_thread.run_sync(_check_mysql_sync)
