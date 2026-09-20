"""FastAPI 入口：弱网客户端的断点续传文件 API。

接口概览（前缀 /api/v1）：
  POST   /sessions                      创建上传会话
  GET    /sessions/{id}                 查询状态与缺失区间
  PUT    /sessions/{id}/chunks          提交二进制分片（?start=&end=，半开区间）
  POST   /sessions/{id}/complete        原子完成（区间全覆盖 + SHA-256 校验）
  DELETE /sessions/{id}                 放弃会话
  GET    /sessions/{id}/download        下载成品（仅 completed 可用）
  POST   /sessions/{id}/tickets         签发受控下载票据（有效期 1～1440 分钟、1～100 次）
  GET    /tickets/{token}/download      凭票下载（Range 也计一次，剩余次数原子扣减）
  DELETE /tickets/{token}               撤销票据（幂等）
"""
from __future__ import annotations

import hashlib
import re
import secrets
import shutil
import sqlite3
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from anyio.to_thread import run_sync
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, RedirectResponse

from . import db, schemas, settings, storage
from .cleanup import start_janitor
from .locks import session_locks

# secrets.token_urlsafe(32) 的实际输出：43 个 URL-safe Base64 字符（无填充）。
_TICKET_RE = re.compile(r"^[A-Za-z0-9_-]{20,128}$")


# ---------------------------------------------------------------------------
# 响应装配
# ---------------------------------------------------------------------------

def _session_payload(row: sqlite3.Row) -> dict:
    conn = db.get_conn()
    chunk_rows = conn.execute(
        "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
        (row["id"],),
    ).fetchall()
    received = [(r["start_offset"], r["end_offset"]) for r in chunk_rows]
    missing = storage.missing_intervals(received, row["total_size"])
    complete = row["status"] == "completed"
    return {
        "session_id": row["id"],
        "filename": row["filename"],
        "total_size": row["total_size"],
        "sha256_expected": row["sha256_expected"],
        "chunk_size": row["chunk_size"],
        "status": row["status"],
        "bytes_received": row["bytes_received"],
        "complete": complete,
        "fully_received": not missing,
        "missing_ranges": [{"start": s, "end": e} for s, e in missing],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "last_access_at": row["last_access_at"],
        "completed_at": row["completed_at"],
        "download_url": f"/api/v1/sessions/{row['id']}/download" if complete else None,
    }


def _http(status: int, error: str, **extra: object) -> HTTPException:
    detail: dict[str, object] = {"error": error}
    detail.update(extra)
    return HTTPException(status_code=status, detail=detail)


def _load_session(session_id: str) -> sqlite3.Row:
    """读取会话；调用方必须持有该会话的锁。

    顺带做懒过期：uploading 且已过 expires_at 时立即标记 expired 并删除分片。
    completed 正常返回；aborted/expired 返回 410；不存在返回 404。
    """
    conn = db.get_conn()
    row = conn.execute(
        "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
    ).fetchone()
    if row is None:
        raise _http(404, "session not found")
    if row["status"] in ("aborted", "expired"):
        raise _http(410, f"session {row['status']}")
    if row["status"] == "completed":
        return row

    if storage.parse_iso(row["expires_at"]) <= storage.utcnow():
        with db.immediate() as tx:
            tx.execute(
                "UPDATE upload_sessions SET status = 'expired' "
                "WHERE id = ? AND status = 'uploading'",
                (session_id,),
            )
        shutil.rmtree(storage.parts_dir(session_id), ignore_errors=True)
        raise _http(410, "session expired")
    return row


def _existing_intervals(tx: sqlite3.Connection, session_id: str) -> list[tuple[int, int]]:
    return [
        (r["start_offset"], r["end_offset"])
        for r in tx.execute(
            "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
            (session_id,),
        ).fetchall()
    ]


# ---------------------------------------------------------------------------
# 生命周期：初始化存储/数据库，启动后台清理
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    storage.ensure_dirs()
    db.init_db()
    _, stop_event = start_janitor()
    try:
        yield
    finally:
        stop_event.set()


app = FastAPI(
    title="Resumable Upload API",
    version="1.0.0",
    description="弱网客户端断点续传文件 API：分片乱序提交、区间校验、原子完成。",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# 会话管理
# ---------------------------------------------------------------------------

@app.post(
    "/api/v1/sessions",
    status_code=201,
    response_model=schemas.SessionResponse,
    tags=["sessions"],
)
def create_session(body: schemas.CreateSessionRequest) -> dict:
    import uuid

    session_id = uuid.uuid4().hex
    now_iso, expires_iso = storage.future_expiry()
    storage.ensure_dirs()
    storage.parts_dir(session_id).mkdir(parents=True, exist_ok=True)
    with db.immediate() as conn:
        conn.execute(
            "INSERT INTO upload_sessions ("
            "id, filename, total_size, sha256_expected, chunk_size, "
            "status, bytes_received, created_at, expires_at, last_access_at"
            ") VALUES (?, ?, ?, ?, ?, 'uploading', 0, ?, ?, ?)",
            (
                session_id,
                body.filename,
                body.total_size,
                body.sha256,
                body.chunk_size,
                now_iso,
                expires_iso,
                now_iso,
            ),
        )
        row = conn.execute(
            "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
        ).fetchone()
    return _session_payload(row)


@app.get(
    "/api/v1/sessions/{session_id}",
    response_model=schemas.SessionResponse,
    tags=["sessions"],
)
def get_session(session_id: str) -> dict:
    with session_locks.get(session_id):
        row = _load_session(session_id)
        return _session_payload(row)


@app.delete("/api/v1/sessions/{session_id}", status_code=204, tags=["sessions"])
def abort_session(session_id: str) -> Response:
    with session_locks.get(session_id):
        with db.immediate() as tx:
            tx.execute(
                "DELETE FROM upload_sessions WHERE id = ?", (session_id,)
            )  # chunks 由 ON DELETE CASCADE 删除
        storage.remove_session_tree(session_id)
    session_locks.discard(session_id)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# 分片提交
# ---------------------------------------------------------------------------

def _prepare_chunk(session_id: str, start: int, end: int, declared_length: int) -> None:
    """在线程池中执行：会话状态、越界、重叠与大小的首次权威校验，并滑动 TTL。"""
    with session_locks.get(session_id):
        row = _load_session(session_id)
        if row["status"] == "completed":
            raise _http(409, "session already completed; no more chunks accepted")
        total = row["total_size"]
        if end > total:
            raise _http(
                400,
                "chunk range exceeds total_size",
                start=start,
                end=end,
                total_size=total,
            )
        if settings.max_chunk_bytes and end - start > settings.max_chunk_bytes:
            raise _http(
                413,
                "chunk larger than MAX_CHUNK_BYTES",
                max_chunk_bytes=settings.max_chunk_bytes,
            )
        if declared_length != end - start:
            raise _http(
                400,
                "content-length does not match end-start",
                content_length=declared_length,
                expected=end - start,
            )
        conn = db.get_conn()
        existing = [
            (r["start_offset"], r["end_offset"])
            for r in conn.execute(
                "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
                (session_id,),
            ).fetchall()
        ]
        if storage.overlaps_any(start, end, existing):
            raise _http(
                409,
                "chunk overlaps an already received range",
                start=start,
                end=end,
            )
        now = storage.utcnow()
        with db.immediate() as tx:
            tx.execute(
                "UPDATE upload_sessions SET last_access_at = ?, expires_at = ? "
                "WHERE id = ? AND status = 'uploading'",
                (
                    storage.iso(now),
                    storage.iso(now + timedelta(seconds=settings.ttl_seconds)),
                    session_id,
                ),
            )


def _commit_chunk(session_id: str, start: int, end: int, tmp_file: str) -> dict:
    """落盘临时分片并在单个 IMMEDIATE 事务中二次校验 + 登记。"""
    tmp_path = Path(tmp_file)
    with session_locks.get(session_id):
        try:
            with db.immediate() as tx:
                row = tx.execute(
                    "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
                ).fetchone()
                if row is None:
                    raise _http(404, "session not found")
                if row["status"] in ("aborted", "expired") or (
                    row["status"] == "uploading"
                    and storage.parse_iso(row["expires_at"]) <= storage.utcnow()
                ):
                    raise _http(410, "session no longer active")
                if row["status"] == "completed":
                    raise _http(409, "session already completed")
                if end > row["total_size"]:
                    raise _http(400, "chunk range exceeds total_size")

                existing = _existing_intervals(tx, session_id)
                if storage.overlaps_any(start, end, existing):
                    raise _http(
                        409,
                        "chunk overlaps an already received range",
                        start=start,
                        end=end,
                    )

                # 事务内移动文件：若后续 SQL 失败，事务回滚且下面的 except 会删除文件。
                final = storage.part_path(session_id, start, end)
                final.parent.mkdir(parents=True, exist_ok=True)
                tmp_path.replace(final)

                now = storage.utcnow()
                now_iso = storage.iso(now)
                expires_iso = storage.iso(
                    now + timedelta(seconds=settings.ttl_seconds)
                )
                tx.execute(
                    "INSERT INTO chunks "
                    "(session_id, start_offset, end_offset, size, received_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (session_id, start, end, end - start, now_iso),
                )
                tx.execute(
                    "UPDATE upload_sessions "
                    "SET bytes_received = bytes_received + ?, "
                    "last_access_at = ?, expires_at = ? WHERE id = ?",
                    (end - start, now_iso, expires_iso, session_id),
                )
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

        fresh = db.get_conn().execute(
            "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
        ).fetchone()
        payload = _session_payload(fresh)
        return {
            "session_id": session_id,
            "received": {"start": start, "end": end},
            "bytes_received": payload["bytes_received"],
            "missing_ranges": payload["missing_ranges"],
            "fully_received": payload["fully_received"],
            "expires_at": payload["expires_at"],
        }


@app.put(
    "/api/v1/sessions/{session_id}/chunks",
    response_model=schemas.ChunkResponse,
    tags=["chunks"],
)
async def put_chunk(
    session_id: str,
    request: Request,
    start: int = Query(..., ge=0, description="起始偏移（含）"),
    end: int = Query(..., ge=0, description="结束偏移（不含），半开区间 [start, end)"),
) -> dict:
    if end <= start:
        raise _http(
            400, "invalid range: require 0 <= start < end", start=start, end=end
        )
    declared_raw = request.headers.get("content-length")
    if declared_raw is None:
        raise _http(411, "content-length header is required")
    try:
        declared_length = int(declared_raw)
    except ValueError:
        raise _http(400, "invalid content-length header")
    if declared_length != end - start:
        raise _http(
            400,
            "content-length does not match end-start",
            content_length=declared_length,
            expected=end - start,
        )
    if settings.max_chunk_bytes and declared_length > settings.max_chunk_bytes:
        raise _http(
            413, "chunk larger than MAX_CHUNK_BYTES",
            max_chunk_bytes=settings.max_chunk_bytes,
        )

    # 首次校验 + TTL 滑动（阻塞 DB 操作丢到线程池，避免卡住事件循环）。
    await run_sync(_prepare_chunk, session_id, start, end, declared_length)

    # 流式落临时文件，弱网下也不会把整分片读进内存。
    tmp = storage.new_temp_file()
    received = 0
    try:
        with tmp.open("wb") as out:
            async for block in request.stream():
                received += len(block)
                if received > declared_length:
                    raise _http(
                        413,
                        "request body longer than declared range",
                        declared=declared_length,
                    )
                out.write(block)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    if received != end - start:
        tmp.unlink(missing_ok=True)
        raise _http(
            400,
            "body length does not match declared range",
            bytes_received=received,
            expected=end - start,
        )

    return await run_sync(_commit_chunk, session_id, start, end, str(tmp))


# ---------------------------------------------------------------------------
# 完成与下载
# ---------------------------------------------------------------------------

@app.post(
    "/api/v1/sessions/{session_id}/complete",
    response_model=schemas.SessionResponse,
    tags=["sessions"],
)
def complete_session(session_id: str) -> dict:
    with session_locks.get(session_id):
        row = _load_session(session_id)
        if row["status"] == "completed":
            return _session_payload(row)

        conn = db.get_conn()
        intervals = sorted(
            (r["start_offset"], r["end_offset"])
            for r in conn.execute(
                "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
                (session_id,),
            ).fetchall()
        )
        missing = storage.missing_intervals(intervals, row["total_size"])
        if missing:
            raise _http(
                409,
                "range incomplete; missing ranges remain",
                missing_ranges=[{"start": s, "end": e} for s, e in missing],
            )

        # 拼装 + 整文件 SHA-256。成品先落在 tmp，绝不提前暴露。
        try:
            tmp_file, digest = storage.assemble_artifact(
                session_id, intervals, row["total_size"]
            )
        except (ValueError, FileNotFoundError, OSError) as exc:
            raise _http(409, f"cannot assemble artifact: {exc}") from exc

        if digest != row["sha256_expected"]:
            Path(tmp_file).unlink(missing_ok=True)
            raise _http(
                422,
                "sha256 mismatch",
                expected=row["sha256_expected"],
                actual=digest,
            )

        storage.files_dir().mkdir(parents=True, exist_ok=True)
        # 与状态翻转放进同一个事务窗口：提交成功前成品不可见；
        # 任一步失败都回滚并删除刚落地的文件，保证“失败没有可下载成品”。
        try:
            with db.immediate() as tx:
                current = tx.execute(
                    "SELECT status FROM upload_sessions WHERE id = ?", (session_id,)
                ).fetchone()
                if current is None or current["status"] != "uploading":
                    raise _http(409, "session is not uploadable")
                storage.atomic_publish(tmp_file, session_id)
                now_iso = storage.iso(storage.utcnow())
                tx.execute(
                    "UPDATE upload_sessions SET "
                    "status = 'completed', bytes_received = ?, "
                    "completed_at = ?, last_access_at = ? WHERE id = ?",
                    (row["total_size"], now_iso, now_iso, session_id),
                )
        except BaseException:
            storage.artifact_path(session_id).unlink(missing_ok=True)
            Path(tmp_file).unlink(missing_ok=True)
            raise

        # 提交成功后才删除分片，节省磁盘。
        shutil.rmtree(storage.parts_dir(session_id), ignore_errors=True)
        fresh = conn.execute(
            "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
        ).fetchone()
        return _session_payload(fresh)


@app.get("/api/v1/sessions/{session_id}/download", tags=["sessions"])
def download_session(session_id: str) -> FileResponse:
    conn = db.get_conn()
    row = conn.execute(
        "SELECT filename FROM upload_sessions WHERE id = ? AND status = 'completed'",
        (session_id,),
    ).fetchone()
    path = storage.artifact_path(session_id)
    if row is None or not path.is_file():
        raise _http(404, "no completed artifact for this session")
    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=storage.safe_filename(row["filename"]),
    )


# ---------------------------------------------------------------------------
# 受控下载票据
# ---------------------------------------------------------------------------

def _hash_token(token: str) -> str:
    """票据只以 SHA-256 形式入库，明文不落盘。"""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _ticket_payload(row: sqlite3.Row, ttl_minutes: int, token: str) -> dict:
    return {
        "token": token,
        "download_url": f"/api/v1/tickets/{token}/download",
        "session_id": row["session_id"],
        "ttl_minutes": ttl_minutes,
        "max_downloads": row["max_downloads"],
        "download_count": row["download_count"],
        "remaining_downloads": row["max_downloads"] - row["download_count"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "revoked": bool(row["revoked"]),
    }


@app.post(
    "/api/v1/sessions/{session_id}/tickets",
    status_code=201,
    response_model=schemas.TicketResponse,
    tags=["tickets"],
)
def issue_ticket(session_id: str, body: schemas.IssueTicketRequest) -> dict:
    conn = db.get_conn()
    row = conn.execute(
        "SELECT id FROM upload_sessions WHERE id = ? AND status = 'completed'",
        (session_id,),
    ).fetchone()
    path = storage.artifact_path(session_id)
    if row is None or not path.is_file():
        raise _http(404, "no completed artifact for this session")

    token = secrets.token_urlsafe(32)
    now = storage.utcnow()
    now_iso = storage.iso(now)
    expires_iso = storage.iso(now + timedelta(minutes=body.ttl_minutes))
    with db.immediate() as tx:
        tx.execute(
            "INSERT INTO download_tickets ("
            "token_hash, session_id, max_downloads, download_count, "
            "created_at, expires_at, revoked"
            ") VALUES (?, ?, ?, 0, ?, ?, 0)",
            (
                _hash_token(token),
                session_id,
                body.max_downloads,
                now_iso,
                expires_iso,
            ),
        )
        stored = tx.execute(
            "SELECT * FROM download_tickets WHERE token_hash = ?",
            (_hash_token(token),),
        ).fetchone()
    payload = _ticket_payload(stored, body.ttl_minutes, token)
    return payload


def _consume_ticket(token: str) -> tuple[sqlite3.Row, Path, str]:
    """在单个 BEGIN IMMEDIATE 事务中完成全部校验并原子扣减一次。

    返回 (票据行, 成品路径, 文件名)。校验失败一律抛 HTTPException，
    且绝不更新 download_count。

    扣减使用带 ``download_count < max_downloads`` 条件的 UPDATE，
    配合表上的 CHECK 约束，并发请求在 SQLite 写事务层面被串行化，
    不可能把下载次数推过上限。成品存在性也在事务内确认，缺文件不计数。
    """
    if not _TICKET_RE.match(token):
        raise _http(404, "ticket not found")
    token_hash = _hash_token(token)
    now_iso = storage.iso(storage.utcnow())
    with db.immediate() as tx:
        row = tx.execute(
            "SELECT * FROM download_tickets WHERE token_hash = ?", (token_hash,)
        ).fetchone()
        if row is None:
            raise _http(404, "ticket not found")
        if row["revoked"]:
            raise _http(403, "ticket revoked")
        if row["expires_at"] <= now_iso:
            raise _http(410, "ticket expired")
        if row["download_count"] >= row["max_downloads"]:
            raise _http(403, "ticket download limit reached")
        session = tx.execute(
            "SELECT status, filename FROM upload_sessions WHERE id = ?",
            (row["session_id"],),
        ).fetchone()
        if session is None:
            raise _http(404, "ticket not found")
        if session["status"] != "completed":
            raise _http(410, "artifact is no longer available")
        path = storage.artifact_path(row["session_id"])
        if not path.is_file():
            raise _http(404, "artifact is no longer available")
        cur = tx.execute(
            "UPDATE download_tickets SET download_count = download_count + 1 "
            "WHERE token_hash = ? AND revoked = 0 AND expires_at > ? "
            "AND download_count < max_downloads",
            (token_hash, now_iso),
        )
        if cur.rowcount != 1:
            # 与并发请求竞争失败（理论上被上面的检查覆盖，防御性兜底）：
            # 本次不计数，交给客户端重试时收到明确的 4xx。
            raise _http(403, "ticket download limit reached")
        ticket = tx.execute(
            "SELECT * FROM download_tickets WHERE token_hash = ?", (token_hash,)
        ).fetchone()
        return ticket, path, session["filename"]


@app.get("/api/v1/tickets/{token}/download", tags=["tickets"])
def download_with_ticket(token: str) -> FileResponse:
    # 校验与扣减在同一个 IMMEDIATE 事务内完成：
    # 未过期 / 未撤销 / 剩余次数充足才扣一次；任何失败都不扣减。
    ticket, path, filename = _consume_ticket(token)
    remaining = ticket["max_downloads"] - ticket["download_count"]
    response = FileResponse(
        path,
        media_type="application/octet-stream",
        filename=storage.safe_filename(filename),
    )
    # 每次 HTTP 请求（含 Range 续传，以及 416 不满足区间）都按一次计数。
    response.headers["X-Ticket-Remaining-Downloads"] = str(remaining)
    return response


@app.delete("/api/v1/tickets/{token}", status_code=204, tags=["tickets"])
def revoke_ticket(token: str) -> Response:
    """撤销票据：之后凭票下载一律 403。

    重复撤销与撤销不存在/格式非法的票据都返回 204，
    使该接口保持幂等，调用方无需区分撤销前状态。
    """
    if not _TICKET_RE.match(token):
        return Response(status_code=204)
    token_hash = _hash_token(token)
    now_iso = storage.iso(storage.utcnow())
    with db.immediate() as tx:
        tx.execute(
            "UPDATE download_tickets SET revoked = 1, revoked_at = ? "
            "WHERE token_hash = ? AND revoked = 0",
            (now_iso, token_hash),
        )
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# 健康检查与文档
# ---------------------------------------------------------------------------

@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")
