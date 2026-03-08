from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: Literal["system", "user", "assistant", "tool"]
    content: Any


class EnqueueChatInput(BaseModel):
    model: str = Field(min_length=1)
    messages: list[ChatMessage] = Field(min_length=1)
    options: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    client_id: str | None = Field(default=None, min_length=1)
    max_attempts: int | None = Field(default=None, ge=1, le=20)


class HasuraActionRequest(BaseModel):
    action: dict[str, Any]
    input: EnqueueChatInput
    session_variables: dict[str, str] = Field(default_factory=dict)
    request_query: str | None = None


class EnqueueChatResponse(BaseModel):
    request_id: str
    status: str


class ChatRequestStatus(BaseModel):
    request_id: str
    client_id: str
    status: str
    request_payload: dict[str, Any] | None = None
    response_payload: dict[str, Any] | None = None
    error_payload: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    attempt_count: int = 0
    max_attempts: int = 0
    kafka_topic: str
    kafka_partition: int | None = None
    kafka_offset: int | None = None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None
    expires_at: datetime | None = None
