from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

from app.config import load_settings
from app.kafka_consumer import KafkaRequestConsumer
from app.kafka_publisher import KafkaResponsePublisher
from app.llm import LLMClient
from app.worker import ChatWorker


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
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
    version="0.3.0",
    lifespan=lifespan,
)


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    worker: ChatWorker = app.state.worker
    payload = {
        "ok": worker.is_alive(),
        "worker_alive": worker.is_alive(),
    }
    if not payload["ok"]:
        raise HTTPException(status_code=503, detail=payload)
    return payload
