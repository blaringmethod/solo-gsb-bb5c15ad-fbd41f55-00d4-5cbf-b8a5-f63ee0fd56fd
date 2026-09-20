"""SQLite 访问层。

每个工作线程持有自己的连接（thread-local），WAL 模式下读写互不阻塞；
所有写操作统一通过 :func:`immediate` 以 BEGIN IMMEDIATE 开启事务，
把冲突检查与插入/更新放进同一个串行化事务，杜绝并发分片导致的重叠区间。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from . import settings

_local = threading.local()


def _db_path() -> Path:
    url = settings.database_url
    if url.startswith("sqlite:///"):
        return Path(url[len("sqlite:///"):])
    # 内存库等其他形式：交给 sqlite3 直接处理。
    return Path(url)


def get_conn() -> sqlite3.Connection:
    """返回当前线程的连接（惰性创建）。"""
    conn: sqlite3.Connection | None = getattr(_local, "conn", None)
    if conn is None:
        path = _db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(path),
            timeout=30.0,
            isolation_level=None,  # 手动事务
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        _local.conn = conn
    return conn


@contextmanager
def immediate() -> Iterator[sqlite3.Connection]:
    """开启 BEGIN IMMEDIATE 事务，提交/回滚由上下文自动处理。"""
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()


SCHEMA = """
CREATE TABLE IF NOT EXISTS upload_sessions (
    id             TEXT PRIMARY KEY,
    filename       TEXT NOT NULL,
    total_size     INTEGER NOT NULL CHECK (total_size >= 0),
    sha256_expected TEXT NOT NULL,
    chunk_size     INTEGER NOT NULL CHECK (chunk_size >= 0),
    status         TEXT NOT NULL CHECK (status IN ('uploading','completed','aborted','expired')),
    bytes_received INTEGER NOT NULL DEFAULT 0 CHECK (bytes_received >= 0),
    created_at     TEXT NOT NULL,
    expires_at     TEXT NOT NULL,
    last_access_at TEXT NOT NULL,
    completed_at   TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id    TEXT NOT NULL REFERENCES upload_sessions(id) ON DELETE CASCADE,
    start_offset  INTEGER NOT NULL CHECK (start_offset >= 0),
    end_offset    INTEGER NOT NULL CHECK (end_offset > start_offset),
    size          INTEGER NOT NULL CHECK (size > 0),
    received_at   TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_chunks_session_offsets
    ON chunks(session_id, start_offset);
CREATE INDEX IF NOT EXISTS idx_chunks_session_interval
    ON chunks(session_id, start_offset, end_offset);
CREATE INDEX IF NOT EXISTS idx_sessions_expires
    ON upload_sessions(status, expires_at);
"""


def init_db() -> None:
    conn = get_conn()
    conn.executescript(SCHEMA)
