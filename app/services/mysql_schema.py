"""
MySQL 表结构：启动时仅自动创建「结果表」（若不存在）。

源表（如 workspace_task_sql_record_temp）是库内原生表：只读，不创建、不改结构。
结果表：表级血缘 / 字段级血缘 / 字段中文名 / 表关联。

raw_sql 列保留不删除，业务写入恒为 NULL；
新增 raw_sql_id VARCHAR(64) 存源表 SQL 行 id；
新增 job_id / task_id INT，自源表 workspace_task_sql_record_temp 带入。

不使用 CREATE TABLE/INDEX IF NOT EXISTS（部分代理/兼容层会报 1064），
改为 information_schema 探测后再 CREATE TABLE（索引写在建表语句内）。

唯一索引按 utf8mb4（每字符最多 4 字节）控制在 InnoDB 3072 字节以内。
"""
from __future__ import annotations

import hashlib
import logging

import anyio
from pymysql.connections import Connection

from app.config import get_settings
from app.db import get_mysql_conn

logger = logging.getLogger(__name__)


def quote_ident(name: str) -> str:
    """反引号包裹标识符，支持中文/空格列名。"""
    if not name or not name.strip():
        raise ValueError("标识符不能为空")
    if any(c in name for c in ("`", ";", "--", "/*", "*/", "\x00")):
        raise ValueError(f"非法标识符: {name!r}")
    if "." in name and not name.startswith("."):
        parts = name.split(".")
        if all(p.strip() for p in parts):
            return ".".join(f"`{p}`" for p in parts)
    return f"`{name}`"


def join_unique_hash(
    *,
    app_name: str,
    schema_name: str,
    primary_schema_name: str,
    primary_table_name: str,
    secondary_schema_name: str,
    secondary_table_name: str,
    join_expression: str,
) -> str:
    """表关联去重键：对业务唯一字段做 SHA256，避免超长 UNIQUE 索引。"""
    raw = "\0".join(
        [
            app_name or "",
            schema_name or "",
            primary_schema_name or "",
            primary_table_name or "",
            secondary_schema_name or "",
            secondary_table_name or "",
            join_expression or "",
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _table_exists(conn: Connection, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 AS ok
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def _column_exists(conn: Connection, schema: str, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 AS ok
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (schema, table, column),
        )
        return cur.fetchone() is not None


def _column_data_type(conn: Connection, schema: str, table: str, column: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DATA_TYPE AS data_type, CHARACTER_MAXIMUM_LENGTH AS char_len
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (schema, table, column),
        )
        row = cur.fetchone()
        if not row:
            return None
        data_type = str(row.get("data_type") or "").lower()
        char_len = row.get("char_len")
        if data_type == "varchar" and char_len is not None:
            return f"varchar({int(char_len)})"
        return data_type or None


def _ensure_raw_sql_id_column(conn: Connection, schema: str, table: str) -> None:
    """已有结果表补齐/校正 raw_sql_id 为 VARCHAR(64)，不删除 raw_sql。"""
    if not _table_exists(conn, schema, table):
        return
    table_ref = f"{quote_ident(schema)}.{quote_ident(table)}"
    if not _column_exists(conn, schema, table, "raw_sql_id"):
        after = "raw_sql" if _column_exists(conn, schema, table, "raw_sql") else "id"
        ddl = f"ALTER TABLE {table_ref} ADD COLUMN `raw_sql_id` VARCHAR(64) NULL AFTER `{after}`"
        with conn.cursor() as cur:
            cur.execute(ddl)
        logger.info("已为结果表补充 raw_sql_id VARCHAR(64): %s.%s", schema, table)
        return

    current = _column_data_type(conn, schema, table, "raw_sql_id")
    if current == "varchar(64)":
        return
    ddl = f"ALTER TABLE {table_ref} MODIFY COLUMN `raw_sql_id` VARCHAR(64) NULL"
    with conn.cursor() as cur:
        cur.execute(ddl)
    logger.info(
        "已将 raw_sql_id 调整为 VARCHAR(64): %s.%s (was %s)",
        schema,
        table,
        current,
    )


def _ensure_int_column(
    conn: Connection,
    schema: str,
    table: str,
    column: str,
    *,
    after: str,
) -> None:
    """已有结果表补齐 INT NULL 列（如 job_id / task_id）。"""
    if not _table_exists(conn, schema, table):
        return
    if _column_exists(conn, schema, table, column):
        return
    table_ref = f"{quote_ident(schema)}.{quote_ident(table)}"
    after_col = after if _column_exists(conn, schema, table, after) else "id"
    ddl = (
        f"ALTER TABLE {table_ref} ADD COLUMN `{column}` INT NULL "
        f"AFTER `{after_col}`"
    )
    with conn.cursor() as cur:
        cur.execute(ddl)
    logger.info("已为结果表补充 %s INT: %s.%s", column, schema, table)


def _ensure_job_task_id_columns(conn: Connection, schema: str, table: str) -> None:
    """已有结果表补齐 job_id / task_id（源表同名字段）。"""
    _ensure_int_column(conn, schema, table, "job_id", after="raw_sql_id")
    _ensure_int_column(conn, schema, table, "task_id", after="job_id")


async def ensure_insert_sql_tables(*, force: bool = False) -> dict[str, str]:
    """
    自动创建血缘/关联等结果表（若不存在）。
    不创建、不修改 SQL 源表（MYSQL_SQL_SOURCE_TABLE，库内原生表，仅读取）。
    force=True 时忽略 mysql_auto_create_tables 开关（供手动接口使用）。
    """
    settings = get_settings()
    if not force and not settings.mysql_auto_create_tables:
        logger.info("mysql_auto_create_tables=false，跳过自动建表")
        return {}

    schema = settings.mysql_database
    tbl_lineage_name = settings.mysql_table_lineage_table
    fld_lineage_name = settings.mysql_field_lineage_table
    fld_meta_name = settings.mysql_field_meta_table
    tbl_join_name = settings.mysql_table_join_table

    tbl_lineage = quote_ident(tbl_lineage_name)
    fld_lineage = quote_ident(fld_lineage_name)
    fld_meta = quote_ident(fld_meta_name)
    tbl_join = quote_ident(tbl_join_name)

    # VARCHAR 参与 UNIQUE 时：utf8mb4 下总长度须 < 768 字符（3072/4）
    # raw_sql 保留列、业务不写；溯源用 raw_sql_id
    create_sql_by_table: dict[str, str] = {
        tbl_lineage_name: f"""
        CREATE TABLE {tbl_lineage} (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            app_name VARCHAR(64) NOT NULL DEFAULT '',
            schema_name VARCHAR(64) NOT NULL DEFAULT '',
            source_table VARCHAR(128) NOT NULL,
            target_table VARCHAR(128) NOT NULL,
            relation VARCHAR(32) NOT NULL DEFAULT 'insert_select',
            parse_source VARCHAR(64) NULL,
            target_table_cn VARCHAR(512) NULL,
            raw_sql LONGTEXT NULL,
            raw_sql_id VARCHAR(64) NULL,
            job_id INT NULL,
            task_id INT NULL,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
            UNIQUE KEY uk_table_lineage (
                app_name, schema_name, source_table, target_table, relation
            ),
            KEY idx_table_lineage_target (target_table),
            KEY idx_table_lineage_raw_sql_id (raw_sql_id),
            KEY idx_table_lineage_job_task (job_id, task_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        fld_lineage_name: f"""
        CREATE TABLE {fld_lineage} (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            app_name VARCHAR(64) NOT NULL DEFAULT '',
            schema_name VARCHAR(64) NOT NULL DEFAULT '',
            source_table VARCHAR(128) NOT NULL DEFAULT '',
            source_field VARCHAR(128) NOT NULL,
            target_table VARCHAR(128) NOT NULL DEFAULT '',
            target_field VARCHAR(128) NOT NULL,
            source_expr TEXT NULL,
            relation VARCHAR(32) NOT NULL DEFAULT 'insert_select',
            parse_source VARCHAR(64) NULL,
            raw_sql LONGTEXT NULL,
            raw_sql_id VARCHAR(64) NULL,
            job_id INT NULL,
            task_id INT NULL,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
            UNIQUE KEY uk_field_lineage (
                app_name, schema_name,
                source_table, source_field,
                target_table, target_field, relation
            ),
            KEY idx_field_lineage_target (target_table, target_field),
            KEY idx_field_lineage_raw_sql_id (raw_sql_id),
            KEY idx_field_lineage_job_task (job_id, task_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        fld_meta_name: f"""
        CREATE TABLE {fld_meta} (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            app_name VARCHAR(64) NOT NULL DEFAULT '',
            schema_name VARCHAR(64) NOT NULL DEFAULT '',
            target_table VARCHAR(128) NOT NULL DEFAULT '',
            field_name VARCHAR(128) NOT NULL,
            name_cn VARCHAR(512) NULL,
            source_expr TEXT NULL,
            source_columns JSON NULL,
            source_tables JSON NULL,
            parse_source VARCHAR(64) NULL,
            table_name_cn VARCHAR(512) NULL,
            raw_sql LONGTEXT NULL,
            raw_sql_id VARCHAR(64) NULL,
            job_id INT NULL,
            task_id INT NULL,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
            UNIQUE KEY uk_field_meta (app_name, schema_name, target_table, field_name),
            KEY idx_field_meta_target (target_table, field_name),
            KEY idx_field_meta_raw_sql_id (raw_sql_id),
            KEY idx_field_meta_job_task (job_id, task_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        tbl_join_name: f"""
        CREATE TABLE {tbl_join} (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            source_row_id BIGINT NULL,
            app_name VARCHAR(64) NOT NULL DEFAULT '',
            schema_name VARCHAR(64) NOT NULL DEFAULT '',
            primary_role VARCHAR(32) NOT NULL DEFAULT 'primary',
            primary_schema_name VARCHAR(64) NOT NULL DEFAULT '',
            primary_table_name VARCHAR(128) NOT NULL,
            primary_join_fields JSON NOT NULL,
            secondary_role VARCHAR(32) NOT NULL DEFAULT 'secondary',
            secondary_schema_name VARCHAR(64) NOT NULL DEFAULT '',
            secondary_table_name VARCHAR(128) NOT NULL,
            secondary_join_fields JSON NOT NULL,
            join_type VARCHAR(64) NOT NULL DEFAULT '',
            join_expression TEXT NOT NULL,
            join_key_hash CHAR(64) NOT NULL,
            parse_source VARCHAR(64) NULL,
            raw_sql LONGTEXT NULL,
            raw_sql_id VARCHAR(64) NULL,
            job_id INT NULL,
            task_id INT NULL,
            created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
            UNIQUE KEY uk_table_join (join_key_hash),
            KEY idx_table_join_primary (primary_table_name, secondary_table_name),
            KEY idx_table_join_raw_sql_id (raw_sql_id),
            KEY idx_table_join_job_task (job_id, task_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    }

    result_tables = (
        tbl_lineage_name,
        fld_lineage_name,
        fld_meta_name,
        tbl_join_name,
    )

    def _run() -> None:
        with get_mysql_conn() as conn:
            with conn.cursor() as cur:
                for table_name, ddl in create_sql_by_table.items():
                    if _table_exists(conn, schema, table_name):
                        logger.info("结果表已存在，跳过创建: %s.%s", schema, table_name)
                        continue
                    cur.execute(ddl)
                    logger.info("已创建结果表: %s.%s", schema, table_name)
            for table_name in result_tables:
                _ensure_raw_sql_id_column(conn, schema, table_name)
                _ensure_job_task_id_columns(conn, schema, table_name)
            conn.commit()

    await anyio.to_thread.run_sync(_run)

    logger.info(
        "MySQL 结果表已就绪（源表 %s 为原生表，不创建/不改）: "
        "table_lineage=%s, field_lineage=%s, field_meta=%s, table_join=%s",
        settings.mysql_sql_source_table,
        tbl_lineage_name,
        fld_lineage_name,
        fld_meta_name,
        tbl_join_name,
    )
    return {
        "source_readonly": settings.mysql_sql_source_table,
        "table_lineage": tbl_lineage_name,
        "field_lineage": fld_lineage_name,
        "field_meta": fld_meta_name,
        "table_join": tbl_join_name,
    }
