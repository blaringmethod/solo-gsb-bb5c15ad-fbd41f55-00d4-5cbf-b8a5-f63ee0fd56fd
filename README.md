# 断点续传文件 API（Resumable Upload API）

面向弱网客户端的分片上传/断点续传服务：客户端先创建上传会话，再按**任意顺序**提交带起止偏移的二进制分片；服务端拒绝越界或与已收区间重叠的分片，并通过状态接口返回缺失区间。仅当分片**完整覆盖整个文件**且整文件 **SHA-256 校验通过**时才原子完成并开放下载——任何失败都不会产生可下载成品。

技术栈：Python 3.12 · FastAPI · SQLite（WAL 模式） · Uvicorn，容器化一键启动。

---

## 一键启动

需要 Docker 与 Docker Compose v2（`docker compose`）：

```bash
docker compose up --build -d
```

服务监听宿主机 **8000** 端口：

- 健康检查：<http://localhost:8000/health>
- 交互式文档（Swagger UI）：<http://localhost:8000/docs>
- 停止并清理：`docker compose down`（数据保留在命名卷 `resumable-data` 中；`-v` 连数据一起删除）

> 单容器单 uvicorn worker 部署。会话级互斥锁是进程内对象，多副本水平扩容需要额外的共享锁/存储，本版本不涉及。

---

## 配置项

全部配置通过环境变量覆盖（见 `docker-compose.yml` 的 `environment` 段）：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/data` | 数据根目录（SQLite 库、分片、成品、临时文件） |
| `DATABASE_URL` | `sqlite:///$DATA_DIR/resumable.db` | SQLite 文件路径（形如 `sqlite:////path/to.db`） |
| `SESSION_TTL_SECONDS` | `86400`（1 天） | 会话存活时长；每次分片活动向后滑动（滑动过期） |
| `CLEANUP_INTERVAL_SECONDS` | `300`（5 分钟） | 后台清理线程的运行间隔 |
| `MAX_CHUNK_BYTES` | `67108864`（64 MiB） | 单个分片最大字节数，`0` 表示不限制 |
| `MAX_UPLOAD_BYTES` | `107374182400`（100 GiB） | 整文件最大字节数，`0` 表示不限制 |
| `MAX_FILENAME_LENGTH` | `255` | 文件名最大长度 |

数据目录布局：

```
$DATA_DIR/
├── resumable.db (+ -wal / -shm)     SQLite 数据库
├── files/<session_id>               完成后的成品（原子 rename 落地）
├── parts/<session_id>/<start>-<end> 已接收分片
└── tmp/                             正在接收/拼装的临时文件（启动周期清理）
```

---

## API 约定

- Base URL：`/api/v1`
- 偏移量为字节偏移，采用**半开区间 `[start, end)`**，即 `end - start = 分片字节数`
- 分片可以按**任意顺序**提交，允许重连后只补传缺失部分
- 错误响应体：`{"detail": {"error": "<错误码描述>", ...附加上下文}}`

### 1. 创建上传会话

```http
POST /api/v1/sessions
Content-Type: application/json

{
  "filename": "movie.mp4",
  "total_size": 104857600,
  "sha256": "9f1b...（64 位小写十六进制，允许 sha256: 前缀）",
  "chunk_size": 4194304
}
```

`chunk_size` 仅为客户端建议值，可为 0。返回 `201` 与完整状态（含 `session_id`、`expires_at`、初始 `missing_ranges`）。

### 2. 提交二进制分片

```http
PUT /api/v1/sessions/{session_id}/chunks?start=0&end=4194304
Content-Type: application/octet-stream
Content-Length: 4194304

<原始二进制分片>
```

服务端校验（任一不过即拒绝，不写入任何分片数据）：

- 会话存在且处于 `uploading`（未过期/未放弃/未完成）
- `0 <= start < end <= total_size` —— 越界返回 `400`
- `Content-Length == end - start`，且实际请求体长度一致 —— 不一致返回 `400/413`
- 分片不超过 `MAX_CHUNK_BYTES` —— 超限返回 `413`
- `[start, end)` 与任何已收区间**不得重叠**（相邻允许，如已有 `[0,100)` 再传 `[100,200)`）—— 重叠返回 `409`

返回当前累计字节数与剩余 `missing_ranges`。并发提交同一区间时只有一个成功，其余得到 `409`。

### 3. 查询状态 / 缺失区间

```http
GET /api/v1/sessions/{session_id}
```

```json
{
  "session_id": "...",
  "filename": "movie.mp4",
  "total_size": 104857600,
  "status": "uploading",
  "bytes_received": 8388608,
  "fully_received": false,
  "complete": false,
  "missing_ranges": [{"start": 4194304, "end": 8388608}, {"start": 12582912, "end": 104857600}],
  "expires_at": "2026-09-21T12:00:00Z",
  "last_access_at": "2026-09-20T12:00:00Z",
  "completed_at": null,
  "download_url": null
}
```

客户端断连恢复时：先查状态，按 `missing_ranges` 补传即可。

### 4. 原子完成

```http
POST /api/v1/sessions/{session_id}/complete
```

- 区间未全覆盖 → `409`，响应里带当前 `missing_ranges`，不产生成品
- 已覆盖但 SHA-256 不匹配 → `422`（返回 `expected`/`actual`），会话保持 `uploading`，客户端可修正后重试或放弃；不产生成品
- 全部通过 → 拼装临时文件 → 校验 → 与状态翻转在同一事务窗口内**原子发布**，状态变为 `completed`，开放下载。完成接口幂等。

### 5. 下载成品

```http
GET /api/v1/sessions/{session_id}/download
```

仅 `completed` 会话返回 `200`（支持 `Range` 断点下载续传）；其他状态一律 `404`，失败流程中不存在任何可下载产物。

### 6. 放弃会话

```http
DELETE /api/v1/sessions/{session_id}
```

幂等，返回 `204`：删除会话记录、已收分片与成品。

### 状态码与生命周期

| 状态 | 含义 |
| --- | --- |
| `uploading` | 收片中；超过 `expires_at` 未活动则过期 |
| `completed` | 校验通过、成品可下载 |
| `expired` | 已被懒过期或后台清理标记，再提交分片得到 `410`，分片已清除 |
| `aborted` | 已放弃（删除接口直接硬删除，正常不可见） |

---

## 过期与清理

- **滑动过期**：创建会话按 `SESSION_TTL_SECONDS` 生成 `expires_at`；每次成功的分片活动自动顺延。
- **后台清理线程**：每 `CLEANUP_INTERVAL_SECONDS` 执行一轮，把过期会话标记为 `expired`、删除其分片目录，并清理已完成会话的分片、孤儿目录和崩溃残留的临时文件。
- **访问时懒过期**：即使后台线程尚未轮到，对已过期会话的任何操作都会立即得到 `410` 并触发清理。
- 已完成的成品**不**随会话过期删除。

## 弱网客户端建议

- 分片大小取 256 KiB～4 MiB；上传失败时按指数退避重试同一分片（重复提交重叠分片会得到 `409`，可直接忽略并查状态）。
- 恢复后先调 `GET /sessions/{id}`，只补 `missing_ranges`。
- 全部补齐后调 `complete`；`422 sha256 mismatch` 说明有分片内容损坏，应放弃会话后重新创建上传。

---

## 本地开发与测试

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt pytest httpx
DATA_DIR=./data uvicorn app.main:app --reload --port 8000

# 测试（自动使用临时数据目录）
python3 -m pytest -q
```

测试覆盖：乱序完整上传与下载、越界/重叠/长度不符拒绝、缺失区间计算、并发重叠竞态（仅一个成功）、SHA-256 不匹配、空文件、放弃、过期清理与未知会话。
