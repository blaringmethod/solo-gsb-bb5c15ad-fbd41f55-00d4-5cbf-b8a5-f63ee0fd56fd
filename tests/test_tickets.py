"""受控下载票据的端到端测试。

覆盖：
- 签发参数与响应约束、明文仅返回一次、库内只存哈希
- 凭票下载、Range 也按一次计数、剩余次数响应头
- 上限耗尽、撤销、过期、票据不存在、会话删除等失败路径均不扣减
- 并发请求在 IMMEDIATE 事务中串行化，成功数严格等于上限
- 原有会话下载接口行为不变
"""
from __future__ import annotations

import hashlib
import random
import threading

from fastapi.testclient import TestClient

from app import db, storage


def _complete_session(client: TestClient, data: bytes, name: str = "data.bin") -> str:
    resp = client.post(
        "/api/v1/sessions",
        json={
            "filename": name,
            "total_size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        },
    )
    assert resp.status_code == 201, resp.text
    sid = resp.json()["session_id"]
    if data:
        resp = client.put(
            f"/api/v1/sessions/{sid}/chunks?start=0&end={len(data)}",
            content=data,
        )
        assert resp.status_code == 200, resp.text
    resp = client.post(f"/api/v1/sessions/{sid}/complete")
    assert resp.status_code == 200, resp.text
    return sid


def _issue(
    client: TestClient,
    sid: str,
    ttl_minutes: int = 60,
    max_downloads: int = 5,
) -> dict:
    resp = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"ttl_minutes": ttl_minutes, "max_downloads": max_downloads},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def _ticket_row(token: str):
    return db.get_conn().execute(
        "SELECT * FROM download_tickets WHERE token_hash = ?",
        (hashlib.sha256(token.encode()).hexdigest(),),
    ).fetchone()


# ---------------------------------------------------------------------------
# 签发
# ---------------------------------------------------------------------------

def test_issue_ticket_for_completed_session(client: TestClient) -> None:
    data = b"ticketed payload " * 10
    sid = _complete_session(client, data)
    body = _issue(client, sid, ttl_minutes=30, max_downloads=3)

    assert set(body) == {
        "token",
        "download_url",
        "session_id",
        "ttl_minutes",
        "max_downloads",
        "download_count",
        "remaining_downloads",
        "created_at",
        "expires_at",
        "revoked",
    }
    assert body["session_id"] == sid
    assert body["ttl_minutes"] == 30
    assert body["max_downloads"] == 3
    assert body["download_count"] == 0
    assert body["remaining_downloads"] == 3
    assert body["revoked"] is False
    assert body["download_url"] == f"/api/v1/tickets/{body['token']}/download"
    assert len(body["token"]) >= 40  # secrets.token_urlsafe(32) -> 43 字符


def test_issue_validates_ttl_and_limit_bounds(client: TestClient) -> None:
    sid = _complete_session(client, b"abc")

    for bad_ttl in (0, -1, 1441):
        r = client.post(
            f"/api/v1/sessions/{sid}/tickets",
            json={"ttl_minutes": bad_ttl, "max_downloads": 1},
        )
        assert r.status_code == 422, bad_ttl

    for bad_limit in (0, -5, 101):
        r = client.post(
            f"/api/v1/sessions/{sid}/tickets",
            json={"ttl_minutes": 60, "max_downloads": bad_limit},
        )
        assert r.status_code == 422, bad_limit

    # 边界值合法
    assert _issue(client, sid, ttl_minutes=1, max_downloads=1)["token"]
    assert _issue(client, sid, ttl_minutes=1440, max_downloads=100)["token"]


def test_ticket_only_for_completed_session(client: TestClient) -> None:
    resp = client.post(
        "/api/v1/sessions",
        json={"filename": "x.bin", "total_size": 1, "sha256": "a" * 64},
    )
    sid = resp.json()["session_id"]
    r = client.post(
        f"/api/v1/sessions/{sid}/tickets",
        json={"ttl_minutes": 60, "max_downloads": 1},
    )
    assert r.status_code == 404

    assert (
        client.post(
            "/api/v1/sessions/nope/tickets",
            json={"ttl_minutes": 60, "max_downloads": 1},
        ).status_code
        == 404
    )


def test_server_stores_only_token_hash(client: TestClient) -> None:
    sid = _complete_session(client, b"secret-bytes")
    body = _issue(client, sid)
    token = body["token"]

    conn = db.get_conn()
    row = conn.execute(
        "SELECT token_hash FROM download_tickets WHERE session_id = ?", (sid,)
    ).fetchone()
    assert row is not None
    assert row["token_hash"] == hashlib.sha256(token.encode()).hexdigest()
    # 明文及其前缀不得以任何形式落库
    for column in ("token_hash", "session_id", "created_at", "expires_at"):
        hit = conn.execute(
            f"SELECT 1 FROM download_tickets WHERE {column} LIKE ?",
            (f"%{token}%",),
        ).fetchone()
        assert hit is None


# ---------------------------------------------------------------------------
# 凭票下载
# ---------------------------------------------------------------------------

def test_download_with_ticket_full_and_range_counts_once(client: TestClient) -> None:
    data = bytes(range(256)) * 8  # 2048 字节
    sid = _complete_session(client, data, name="range.bin")
    token = _issue(client, sid, max_downloads=5)["token"]
    url = f"/api/v1/tickets/{token}/download"

    # 完整下载
    r = client.get(url)
    assert r.status_code == 200
    assert r.content == data
    assert r.headers["x-ticket-remaining-downloads"] == "4"
    assert _ticket_row(token)["download_count"] == 1

    # Range 请求：返回对应区间，也按一次计数
    r = client.get(url, headers={"Range": "bytes=100-199"})
    assert r.status_code == 206
    assert r.content == data[100:200]
    assert r.headers["content-range"].startswith("bytes 100-199/2048")
    assert r.headers["x-ticket-remaining-downloads"] == "3"
    assert _ticket_row(token)["download_count"] == 2

    # 开区间 Range（bytes=1900-）同样计数
    r = client.get(url, headers={"Range": "bytes=1900-"})
    assert r.status_code == 206
    assert r.content == data[1900:]
    assert _ticket_row(token)["download_count"] == 3

    # 无法满足的 Range（416）请求仍到达端点，同样按一次计数
    r = client.get(url, headers={"Range": "bytes=9999-"})
    assert r.status_code == 416
    assert _ticket_row(token)["download_count"] == 4

    # 畸形 Range（400）同样按一次计数
    r = client.get(url, headers={"Range": "wat"})
    assert r.status_code == 400
    assert _ticket_row(token)["download_count"] == 5

    # 次数已耗尽：之后任何请求都被拒绝
    r = client.get(url)
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "ticket download limit reached"
    assert r.headers.get("x-ticket-remaining-downloads") is None
    # 拒绝不得扣减
    assert _ticket_row(token)["download_count"] == 5


def test_limit_one_allows_exactly_one_download(client: TestClient) -> None:
    sid = _complete_session(client, b"once")
    token = _issue(client, sid, max_downloads=1)["token"]
    url = f"/api/v1/tickets/{token}/download"

    r = client.get(url)
    assert r.status_code == 200
    assert r.content == b"once"
    assert r.headers["x-ticket-remaining-downloads"] == "0"

    r = client.get(url)
    assert r.status_code == 403
    assert _ticket_row(token)["download_count"] == 1


def test_unknown_and_malformed_tokens_are_404(client: TestClient) -> None:
    import secrets

    # 随机但格式合法、从未签发的票据
    r = client.get(f"/api/v1/tickets/{secrets.token_urlsafe(32)}/download")
    assert r.status_code == 404
    # 长度不足 / 含非法字符（URL 编码后传入），不匹配合法票据格式
    for bad in ("short", "B" * 43 + "%21%21", "x" * 20 + "%20" + "y" * 10):
        r = client.get(f"/api/v1/tickets/{bad}/download")
        assert r.status_code == 404, bad


def test_revoked_ticket_rejected_without_decrement(client: TestClient) -> None:
    sid = _complete_session(client, b"revoke me")
    token = _issue(client, sid, max_downloads=3)["token"]
    url = f"/api/v1/tickets/{token}/download"

    assert client.get(url).status_code == 200
    assert _ticket_row(token)["download_count"] == 1

    r = client.delete(f"/api/v1/tickets/{token}")
    assert r.status_code == 204
    row = _ticket_row(token)
    assert row["revoked"] == 1
    assert row["revoked_at"] is not None

    r = client.get(url)
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "ticket revoked"
    # 撤销后的失败请求不扣减
    assert _ticket_row(token)["download_count"] == 1

    # 撤销幂等：重复撤销仍 204，次数不变
    assert client.delete(f"/api/v1/tickets/{token}").status_code == 204
    assert _ticket_row(token)["download_count"] == 1

    # 撤销不存在/畸形的票据也返回 204
    assert client.delete(f"/api/v1/tickets/{'B' * 43}").status_code == 204
    assert client.delete("/api/v1/tickets/not-a-token!!").status_code == 204


def test_expired_ticket_rejected_without_decrement(client: TestClient) -> None:
    sid = _complete_session(client, b"ticking")
    token = _issue(client, sid, ttl_minutes=1, max_downloads=2)["token"]
    url = f"/api/v1/tickets/{token}/download"

    # 直接把 expires_at 改到过去，避免测试真的等待
    db.get_conn().execute(
        "UPDATE download_tickets SET expires_at = ? WHERE token_hash = ?",
        ("2000-01-01T00:00:00Z", hashlib.sha256(token.encode()).hexdigest()),
    )

    r = client.get(url)
    assert r.status_code == 410
    assert r.json()["detail"]["error"] == "ticket expired"
    assert _ticket_row(token)["download_count"] == 0


def test_ticket_cascade_deletes_with_session(client: TestClient) -> None:
    sid = _complete_session(client, b"doomed")
    token = _issue(client, sid)["token"]
    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    assert _ticket_row(token) is None
    r = client.get(f"/api/v1/tickets/{token}/download")
    assert r.status_code == 404
    # 原会话下载接口同样失效
    assert client.get(f"/api/v1/sessions/{sid}/download").status_code == 404


def test_concurrent_downloads_never_exceed_limit(client: TestClient) -> None:
    rng = random.Random(7)
    data = bytes(rng.randrange(256) for _ in range(64 * 1024))
    sid = _complete_session(client, data)
    limit = 5
    token = _issue(client, sid, max_downloads=limit)["token"]
    url = f"/api/v1/tickets/{token}/download"

    results: list[int] = []
    remaining_headers: list[str | None] = []
    barrier = threading.Barrier(20)
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        # 混合完整下载与 Range 请求；无论哪种都只占一次额度
        headers = (
            {"Range": f"bytes={rng.randrange(0, 32768)}-"}
            if rng.random() < 0.5
            else {}
        )
        r = client.get(url, headers=headers)
        with lock:
            results.append(r.status_code)
            remaining_headers.append(r.headers.get("x-ticket-remaining-downloads"))

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(200) + results.count(206) == limit
    assert all(code == 403 for code in results if code not in (200, 206))
    row = _ticket_row(token)
    assert row["download_count"] == limit
    # 成功响应上的剩余额度恰好覆盖 limit-1 ... 0，且不会出现负数
    seen = sorted(int(h) for h in remaining_headers if h is not None)
    assert seen == list(range(limit))


def test_legacy_session_download_unchanged(client: TestClient) -> None:
    data = b"legacy " * 20
    sid = _complete_session(client, data, name="legacy.bin")

    # 原有接口无需票据，且不与票据计数互相影响
    r = client.get(f"/api/v1/sessions/{sid}/download")
    assert r.status_code == 200
    assert r.content == data
    assert "attachment" in r.headers["content-disposition"]

    token = _issue(client, sid, max_downloads=1)["token"]
    r = client.get(f"/api/v1/tickets/{token}/download")
    assert r.status_code == 200
    # 会话下载接口仍然可用，票据耗尽也不影响它
    assert client.get(f"/api/v1/sessions/{sid}/download").status_code == 200
    assert client.get(f"/api/v1/tickets/{token}/download").status_code == 403
