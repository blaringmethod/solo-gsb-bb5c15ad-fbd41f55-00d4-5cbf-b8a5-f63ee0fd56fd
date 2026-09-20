"""请求/响应模型。"""
from __future__ import annotations

import re

from pydantic import BaseModel, Field, field_validator

from . import settings

_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


class CreateSessionRequest(BaseModel):
    filename: str = Field(..., description="原始文件名")
    total_size: int = Field(..., ge=0, description="文件总字节数")
    sha256: str = Field(..., description="整文件 SHA-256，64 位小写十六进制（可带 sha256: 前缀）")
    chunk_size: int = Field(
        default=0, ge=0, description="建议分片大小（字节），仅作提示；0 表示无建议"
    )

    @field_validator("filename")
    @classmethod
    def _validate_filename(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("filename must not be empty")
        if len(value) > settings.max_filename_length:
            raise ValueError(f"filename too long (max {settings.max_filename_length})")
        return value

    @field_validator("total_size")
    @classmethod
    def _validate_size(cls, value: int) -> int:
        if settings.max_upload_bytes and value > settings.max_upload_bytes:
            raise ValueError(
                f"total_size exceeds server limit {settings.max_upload_bytes} bytes"
            )
        return value

    @field_validator("sha256")
    @classmethod
    def _validate_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if value.startswith("sha256:"):
            value = value[len("sha256:"):]
        if not _SHA256_HEX.match(value):
            raise ValueError("sha256 must be 64 lowercase hex characters")
        return value


class MissingRange(BaseModel):
    start: int
    end: int


class SessionResponse(BaseModel):
    session_id: str
    filename: str
    total_size: int
    sha256_expected: str
    chunk_size: int
    status: str
    bytes_received: int
    complete: bool
    fully_received: bool
    missing_ranges: list[MissingRange]
    created_at: str
    expires_at: str
    last_access_at: str
    completed_at: str | None = None
    download_url: str | None = None


class ChunkResponse(BaseModel):
    session_id: str
    received: MissingRange
    bytes_received: int
    missing_ranges: list[MissingRange]
    fully_received: bool
    expires_at: str
