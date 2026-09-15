"""
连接管理，进程内单例。
ArangoDB / MySQL 用同步客户端（服务层里用 anyio.to_thread 调用），
Redis 用异步客户端。
"""
from functools import lru_cache
from contextlib import contextmanager
from typing import Iterator

import redis.asyncio as aioredis
from arango import ArangoClient
from arango.database import StandardDatabase
import pymysql
from pymysql.connections import Connection
from pymysql.cursors import DictCursor

from app.config import get_settings


@lru_cache
def get_arango_db() -> StandardDatabase:
    settings = get_settings()
    client = ArangoClient(hosts=settings.arango_hosts.rstrip("/"))
    return client.db(settings.arango_db, username=settings.arango_user, password=settings.arango_password)


@lru_cache
def get_redis():
    settings = get_settings()
    # protocol=2 强制使用 RESP2 协议握手，避免 Redis 4.x（不支持 HELLO 命令）报
    # "unknow command 'HELLO'" 错误
    return aioredis.from_url(settings.redis_url, decode_responses=True, protocol=2)


@contextmanager
def get_mysql_conn() -> Iterator[Connection]:
    """
    同步 MySQL 连接（DictCursor）。请在 anyio.to_thread.run_sync 中调用，避免阻塞事件循环。
    Windows 测试机与 Linux 正式机共用同一套代码，仅通过 .env 区分 host/port/账号。
    """
    s = get_settings()
    conn = pymysql.connect(
        host=s.mysql_host,
        port=s.mysql_port,
        user=s.mysql_user,
        password=s.mysql_password,
        database=s.mysql_database,
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=False,
    )
    try:
        yield conn
    finally:
        conn.close()
