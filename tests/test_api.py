"""端到端行为测试。"""
from __future__ import annotations

import hashlib
import random
import time

from fastapi.testclient import TestClient

from app import settings
from app.cleanup import run_cleanup
from app import storage


def _create(client: TestClient, data: bytes, name: str = "data.bin", chunk_size: int = 0):
    resp = client.post(
        "/api/v1/sessions",
        json={
            "filename": name,
            "total_size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "chunk_size": chunk_size,
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["session_id"]


def test_out_of_order_upload_and_download(client: TestClient) -> None:
    rng = random.Random(42)
    data = bytes(rng.randrange(256) for _ in range(1024 * 1024))  # 1 MiB
    sid = _create(client, data, chunk_size=256 * 1024)

    # 完成前下载必须 404——不得存在可下载成品。
    assert client.get(f"/api/v1/sessions/{sid}/download").status_code == 404

    # 切成大小不一的 9 片，打乱顺序提交。
    cuts = [0, 37, 120_000, 120_001, 260_000, 500_000, 700_000, 999_999, 1_000_000]
    ranges = list(zip(cuts, cuts[1:] + [len(data)]))
    rng.shuffle(ranges)
    for start, end in ranges:
        resp = client.put(
            f"/api/v1/sessions/{sid}/chunks?start={start}&end={end}",
            content=data[start:end],
        )
        assert resp.status_code == 200, (start, end, resp.text)
        assert resp.json()["received"] == {"start": start, "end": end}

    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["fully_received"] is True
    assert status["missing_ranges"] == []
    assert status["status"] == "uploading"
    assert status["bytes_received"] == len(data)

    done = client.post(f"/api/v1/sessions/{sid}/complete")
    assert done.status_code == 200, done.text
    body = done.json()
    assert body["status"] == "completed"
    assert body["complete"] is True

    dl = client.get(f"/api/v1/sessions/{sid}/download")
    assert dl.status_code == 200
    assert dl.content == data
    assert "attachment" in dl.headers["content-disposition"]

    # 已完成的会话不再接受分片；complete 幂等。
    dup = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=1", content=data[:1]
    )
    assert dup.status_code == 409
    again = client.post(f"/api/v1/sessions/{sid}/complete")
    assert again.status_code == 200
    assert again.json()["status"] == "completed"


def test_reject_out_of_bounds_overlap_and_length(client: TestClient) -> None:
    data = b"abcdefghij" * 100
    sid = _create(client, data)
    total = len(data)

    # 越界（end 超过 total_size）
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=990&end=1001",
        content=data[990:1000] + b"x",
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "chunk range exceeds total_size"

    # 非法区间 start >= end
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=10&end=10", content=b""
    )
    assert r.status_code == 400

    # content-length 与区间不一致
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=10", content=b"short"
    )
    assert r.status_code == 400
    assert "content-length" in r.json()["detail"]["error"]

    # 正常收一片
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=100", content=data[:100]
    )
    assert r.status_code == 200, r.text

    # 完全重叠
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=50&end=150",
        content=data[50:150],
    )
    assert r.status_code == 409
    assert "overlaps" in r.json()["detail"]["error"]

    # 边界相邻不算重叠：[100,200) 合法
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=100&end=200",
        content=data[100:200],
    )
    assert r.status_code == 200, r.text

    # 状态接口返回的缺失区间必须准确
    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["missing_ranges"] == [{"start": 200, "end": total}]


def test_sha256_mismatch_keeps_session_unfinished(client: TestClient) -> None:
    data = bytes(range(256)) * 4
    # 用错误 sha 创建会话并提交正确字节
    bad = client.post(
        "/api/v1/sessions",
        json={
            "filename": "bad.bin",
            "total_size": len(data),
            "sha256": "0" * 64,
        },
    ).json()["session_id"]
    client.put(
        f"/api/v1/sessions/{bad}/chunks?start=0&end={len(data)}", content=data
    )
    r = client.post(f"/api/v1/sessions/{bad}/complete")
    assert r.status_code == 422
    assert r.json()["detail"]["error"] == "sha256 mismatch"

    # 失败后仍不可下载，会话仍是 uploading
    assert client.get(f"/api/v1/sessions/{bad}/download").status_code == 404
    status = client.get(f"/api/v1/sessions/{bad}").json()
    assert status["status"] == "uploading"
    assert status["fully_received"] is True


def test_complete_with_gaps_reports_missing(client: TestClient) -> None:
    data = bytes(r % 251 for r in range(5000))
    sid = _create(client, data)
    client.put(
        f"/api/v1/sessions/{sid}/chunks?start=100&end=1000",
        content=data[100:1000],
    )
    r = client.post(f"/api/v1/sessions/{sid}/complete")
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["missing_ranges"] == [
        {"start": 0, "end": 100},
        {"start": 1000, "end": 5000},
    ]
    assert client.get(f"/api/v1/sessions/{sid}/download").status_code == 404


def test_abort_session(client: TestClient) -> None:
    data = b"hello"
    sid = _create(client, data)
    client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=5", content=data
    )
    r = client.delete(f"/api/v1/sessions/{sid}")
    assert r.status_code == 204
    # 删除幂等
    assert client.delete(f"/api/v1/sessions/{sid}").status_code == 204
    assert client.get(f"/api/v1/sessions/{sid}").status_code == 404
    assert not storage.parts_dir(sid).exists()


def test_empty_file_roundtrip(client: TestClient) -> None:
    sid = _create(client, b"", name="empty.txt")
    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["fully_received"] is True
    r = client.post(f"/api/v1/sessions/{sid}/complete")
    assert r.status_code == 200, r.text
    dl = client.get(f"/api/v1/sessions/{sid}/download")
    assert dl.status_code == 200
    assert dl.content == b""


def test_expired_session_is_rejected_and_cleaned(client: TestClient, monkeypatch) -> None:
    data = b"x" * 1000
    # 创建前就把 TTL 调小：expires_at 在创建/分片提交时按当前 TTL 计算。
    monkeypatch.setattr(settings, "ttl_seconds", 1)
    sid = _create(client, data)
    client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=100", content=data[:100]
    )

    time.sleep(1.1)
    # 后台轮询间隔很长，直接驱动一轮清理（生产中由 janitor 周期调用）。
    expired = run_cleanup()
    assert sid in expired

    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=100&end=200",
        content=data[100:200],
    )
    assert r.status_code == 410
    status = client.get(f"/api/v1/sessions/{sid}")
    assert status.status_code == 410
    assert client.get(f"/api/v1/sessions/{sid}/download").status_code == 404
    assert not storage.parts_dir(sid).exists()


def test_unknown_session_errors(client: TestClient) -> None:
    sid = "deadbeef" * 4
    assert client.get(f"/api/v1/sessions/{sid}").status_code == 404
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=1", content=b"x"
    )
    assert r.status_code == 404
    assert client.post(f"/api/v1/sessions/{sid}/complete").status_code == 404


def test_invalid_create_requests(client: TestClient) -> None:
    r = client.post(
        "/api/v1/sessions",
        json={"filename": "x.bin", "total_size": -1, "sha256": "a" * 64},
    )
    assert r.status_code == 422

    r = client.post(
        "/api/v1/sessions",
        json={"filename": "x.bin", "total_size": 1, "sha256": "xyz"},
    )
    assert r.status_code == 422

    r = client.post(
        "/api/v1/sessions",
        json={"filename": "   ", "total_size": 1, "sha256": "a" * 64},
    )
    assert r.status_code == 422


def test_concurrent_overlapping_chunks_only_one_wins(client: TestClient) -> None:
    import threading

    data = b"".join(bytes([i % 251]) for i in range(100))
    sid = _create(client, data)
    results: list[int] = []
    barrier = threading.Barrier(5)

    def worker() -> None:
        barrier.wait()  # 尽量放大同时提交同一区间的竞态窗口
        r = client.put(
            f"/api/v1/sessions/{sid}/chunks?start=0&end=100", content=data
        )
        results.append(r.status_code)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results).count(200) == 1
    assert all(code == 409 for code in results if code != 200)
    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["bytes_received"] == 100
    assert status["fully_received"] is True
