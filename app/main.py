from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import FastAPI, Header, HTTPException

from app.config import Settings, load_settings
from app.db import ChatDatabase
from app.kafka_consumer import KafkaRequestConsumer
from app.kafka_publisher import KafkaResponsePublisher
from app.llm import LLMClient
from app.models import (
    ChatRequestStatus,
    EnqueueChatInput,
    EnqueueChatResponse,
    HasuraActionRequest,
)
from app.worker import ChatWorker


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

_LOG = logging.getLogger("ollama_async_subscription_service.main")


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    db = ChatDatabase(settings.database_url, settings.async_messages_table)
    kafka_consumer = KafkaRequestConsumer(settings)
    kafka_publisher = KafkaResponsePublisher(settings)
    llm_client = LLMClient(
        base_url=settings.llm_api_base_url,
        api_key=settings.llm_api_key,
        timeout_seconds=settings.llm_timeout_seconds,
    )
    worker = ChatWorker(
        kafka_consumer=kafka_consumer,
        llm_client=llm_client,
        kafka_publisher=kafka_publisher,
        worker_id=settings.worker_id,
        default_model=settings.llm_default_model,
        handler_names=settings.request_handler_names,
        poll_seconds=settings.worker_poll_seconds,
        response_expires_seconds=settings.response_expires_seconds,
    )

    app.state.settings = settings
    app.state.db = db
    app.state.worker = worker
    app.state.kafka_consumer = kafka_consumer
    app.state.kafka_publisher = kafka_publisher

    worker.start()
    yield
    worker.stop()
    kafka_consumer.close()
    kafka_publisher.close()


app = FastAPI(
    title="Ollama Async Subscription Service",
    version="0.2.0",
    lifespan=lifespan,
)


def _resolve_client_id(input_payload: EnqueueChatInput, session_variables: dict[str, str]) -> str:
    return (
        session_variables.get("x-hasura-user-id")
        or input_payload.client_id
        or session_variables.get("x-hasura-role")
        or "anonymous"
    )


def _build_request_message(
    *,
    settings: Settings,
    request_id: str,
    client_id: str,
    input_payload: EnqueueChatInput,
    session_variables: dict[str, str],
) -> dict[str, Any]:
    now = datetime.now(UTC)
    expires_at = now + timedelta(seconds=settings.response_expires_seconds)
    metadata = dict(input_payload.metadata or {})
    metadata["submitted_via"] = "enqueue_api"
    return {
        "spec_version": "async.request.v1",
        "request_id": request_id,
        "client_id": client_id,
        "route": {
            "handler": settings.request_handler_name,
            "operation": "chat.completion",
        },
        "payload": {
            "model": input_payload.model,
            "messages": [msg.model_dump() for msg in input_payload.messages],
            "options": input_payload.options or {},
        },
        "options": {
            "priority": "normal",
            "reply_topic": settings.async_response_topic,
            "expires_at": _iso_z(expires_at),
        },
        "metadata": metadata,
        "session_variables": session_variables or {},
        "submitted_at": _iso_z(now),
    }


def _publish_request_to_kafka(request_id: str, message: dict[str, Any]) -> None:
    settings: Settings = app.state.settings
    kafka_publisher: KafkaResponsePublisher = app.state.kafka_publisher
    kafka_publisher.publish(request_id, message, topic=settings.async_request_topic)


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    db: ChatDatabase = app.state.db
    worker: ChatWorker = app.state.worker
    db_ok = db.ping()
    payload = {
        "ok": db_ok and worker.is_alive(),
        "db_ok": db_ok,
        "worker_alive": worker.is_alive(),
    }
    if not payload["ok"]:
        raise HTTPException(status_code=503, detail=payload)
    return payload


@app.post("/enqueue", response_model=EnqueueChatResponse)
def enqueue_chat(input_payload: EnqueueChatInput) -> EnqueueChatResponse:
    db: ChatDatabase = app.state.db
    settings: Settings = app.state.settings
    session_variables: dict[str, str] = {}
    client_id = _resolve_client_id(input_payload, session_variables)

    row = db.insert_chat_request(
        client_id=client_id,
        model=input_payload.model,
        messages=[msg.model_dump() for msg in input_payload.messages],
        options=input_payload.options,
        metadata=input_payload.metadata,
        max_attempts=input_payload.max_attempts or settings.default_max_attempts,
        session_variables=session_variables,
    )
    request_message = _build_request_message(
        settings=settings,
        request_id=row["request_id"],
        client_id=client_id,
        input_payload=input_payload,
        session_variables=session_variables,
    )
    try:
        _publish_request_to_kafka(row["request_id"], request_message)
    except Exception as exc:
        db.requeue_request(row["request_id"], f"kafka publish failed: {type(exc).__name__}: {exc}")
        _LOG.exception("failed to publish request to Kafka id=%s", row["request_id"])
        raise HTTPException(status_code=502, detail="failed to publish request to Kafka")

    return EnqueueChatResponse(request_id=row["request_id"], status=row["status"])


@app.post("/hasura/actions/enqueue_chat", response_model=EnqueueChatResponse)
def hasura_enqueue_chat(
    body: HasuraActionRequest,
    x_hasura_action_secret: str | None = Header(default=None, alias="X-Hasura-Action-Secret"),
) -> EnqueueChatResponse:
    settings: Settings = app.state.settings
    db: ChatDatabase = app.state.db

    if settings.hasura_action_secret and x_hasura_action_secret != settings.hasura_action_secret:
        raise HTTPException(status_code=401, detail="Invalid action secret")

    session_variables = body.session_variables or {}
    client_id = _resolve_client_id(body.input, session_variables)

    row = db.insert_chat_request(
        client_id=client_id,
        model=body.input.model,
        messages=[msg.model_dump() for msg in body.input.messages],
        options=body.input.options,
        metadata=body.input.metadata,
        max_attempts=body.input.max_attempts or settings.default_max_attempts,
        session_variables=session_variables,
    )
    request_message = _build_request_message(
        settings=settings,
        request_id=row["request_id"],
        client_id=client_id,
        input_payload=body.input,
        session_variables=session_variables,
    )
    try:
        _publish_request_to_kafka(row["request_id"], request_message)
    except Exception as exc:
        db.requeue_request(row["request_id"], f"kafka publish failed: {type(exc).__name__}: {exc}")
        _LOG.exception("failed to publish request to Kafka id=%s", row["request_id"])
        raise HTTPException(status_code=502, detail="failed to publish request to Kafka")

    return EnqueueChatResponse(request_id=row["request_id"], status=row["status"])


@app.get("/jobs/{request_id}", response_model=ChatRequestStatus)
def get_chat_job(request_id: str) -> ChatRequestStatus:
    db: ChatDatabase = app.state.db

    row = db.get_chat_request(request_id)
    if row is None:
        raise HTTPException(status_code=404, detail="chat request not found")
    return ChatRequestStatus(**row)
