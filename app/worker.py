from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

from app.kafka_consumer import KafkaRequestConsumer
from app.kafka_publisher import KafkaResponsePublisher
from app.llm import LLMClient


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class _IgnoreMessage(Exception):
    pass


class ChatWorker:
    def __init__(
        self,
        *,
        kafka_consumer: KafkaRequestConsumer,
        llm_client: LLMClient,
        kafka_publisher: KafkaResponsePublisher,
        worker_id: str,
        default_model: str,
        handler_names: tuple[str, ...],
        poll_seconds: float,
        response_expires_seconds: int,
    ) -> None:
        self._kafka_consumer = kafka_consumer
        self._llm_client = llm_client
        self._kafka_publisher = kafka_publisher
        self._worker_id = worker_id
        self._default_model = default_model
        self._handler_names = {name.strip().lower() for name in handler_names if name.strip()}
        self._poll_seconds = poll_seconds
        self._response_expires_seconds = response_expires_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._log = logging.getLogger("ollama_async_subscription_service.worker")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="chat-worker", daemon=True)
        self._thread.start()

    def stop(self, timeout_seconds: float = 10.0) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout_seconds)

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def _build_response_message(
        self,
        *,
        request_id: str,
        client_id: str | None,
        status: str,
        metadata: dict,
        expires_at: datetime | None,
        response_payload: dict | None = None,
        error_payload: dict | None = None,
    ) -> dict:
        completed_at = datetime.now(UTC)
        effective_expires = expires_at or (completed_at + timedelta(seconds=self._response_expires_seconds))

        out_metadata = {k: v for k, v in dict(metadata or {}).items() if k != "_async"}
        out_metadata["worker"] = self._worker_id

        message = {
            "request_id": request_id,
            "status": status,
            "metadata": out_metadata,
            "completed_at": _iso_z(completed_at),
            "expires_at": _iso_z(effective_expires),
        }
        if client_id:
            message["client_id"] = client_id
        if response_payload is not None:
            message["response_payload"] = response_payload
        if error_payload is not None:
            message["error_payload"] = error_payload
        return message

    def _extract_request(self, record: Any) -> dict[str, Any]:
        payload = record.value
        if not isinstance(payload, dict):
            raise ValueError("Kafka message value must be a JSON object")

        spec_version = str(payload.get("spec_version") or "").strip()
        if spec_version and spec_version != "async.request.v1":
            raise _IgnoreMessage(f"unsupported spec_version '{spec_version}'")

        action = str(payload.get("action") or "").strip()
        if action and action != "publish_async_request":
            raise _IgnoreMessage(f"unsupported action '{action}'")

        request_id = str(payload.get("request_id") or record.key or "").strip()
        if not request_id:
            raise ValueError("Kafka request is missing request_id")

        route = payload.get("route")
        route = route if isinstance(route, dict) else {}
        handler = str(route.get("handler") or "").strip().lower()
        operation = str(route.get("operation") or "").strip().lower()

        if self._handler_names and handler and handler not in self._handler_names:
            raise _IgnoreMessage(f"handler '{handler}' is not assigned to this worker")
        if operation and operation != "chat.completion":
            raise _IgnoreMessage(f"operation '{operation}' is not supported")

        request_payload_raw = payload.get("payload")
        request_payload = request_payload_raw if isinstance(request_payload_raw, dict) else {}

        model = str(request_payload.get("model") or self._default_model).strip()
        if not model:
            model = self._default_model

        messages = request_payload.get("messages")
        prompt = request_payload.get("prompt")
        if not isinstance(prompt, str) and isinstance(request_payload_raw, str):
            prompt = request_payload_raw
        if not isinstance(messages, list) or not messages:
            if isinstance(prompt, str) and prompt.strip():
                messages = [{"role": "user", "content": prompt.strip()}]
            else:
                raise ValueError("Kafka request payload must include 'messages' or non-empty 'prompt'")

        options = request_payload.get("options")
        options = options if isinstance(options, dict) else {}

        metadata = payload.get("metadata")
        metadata = dict(metadata) if isinstance(metadata, dict) else {}
        if handler:
            metadata["handler"] = handler
        if operation:
            metadata["operation"] = operation
        conversation_id = request_payload.get("conversationId") or request_payload.get("conversation_id")
        if isinstance(conversation_id, str) and conversation_id.strip():
            metadata["conversation_id"] = conversation_id.strip()

        message_options = payload.get("options")
        message_options = message_options if isinstance(message_options, dict) else {}
        reply_topic = str(message_options.get("reply_topic") or "").strip() or None
        expires_at = _parse_dt(message_options.get("expires_at"))

        client_id = payload.get("client_id")
        if client_id is not None:
            client_id = str(client_id).strip() or None

        return {
            "request_id": request_id,
            "client_id": client_id,
            "model": model,
            "messages": messages,
            "options": options,
            "metadata": metadata,
            "expires_at": expires_at,
            "reply_topic": reply_topic,
        }

    def _publish_error(
        self,
        *,
        request_id: str,
        client_id: str | None,
        metadata: dict[str, Any],
        expires_at: datetime | None,
        reply_topic: str | None,
        error: Exception,
    ) -> None:
        error_payload = {"error": f"{type(error).__name__}: {error}", "retryable": False}
        message = self._build_response_message(
            request_id=request_id,
            client_id=client_id,
            status="error",
            metadata=metadata,
            expires_at=expires_at,
            error_payload=error_payload,
        )
        self._kafka_publisher.publish(request_id, message, topic=reply_topic)

    def _handle_record(self, record: Any) -> bool:
        payload = record.value if isinstance(record.value, dict) else {}
        request_id_fallback = str(payload.get("request_id") or record.key or "").strip()
        reply_topic_fallback = None
        if isinstance(payload.get("options"), dict):
            reply_topic_fallback = str(payload["options"].get("reply_topic") or "").strip() or None

        try:
            request = self._extract_request(record)
        except _IgnoreMessage as exc:
            self._log.debug("request skipped topic=%s partition=%s offset=%s reason=%s", record.topic, record.partition, record.offset, exc)
            return True
        except Exception as exc:
            if request_id_fallback:
                try:
                    self._publish_error(
                        request_id=request_id_fallback,
                        client_id=str(payload.get("client_id") or "").strip() or None,
                        metadata=dict(payload["metadata"]) if isinstance(payload.get("metadata"), dict) else {},
                        expires_at=None,
                        reply_topic=reply_topic_fallback,
                        error=exc,
                    )
                except Exception:
                    self._log.exception("failed to publish parse error response id=%s", request_id_fallback)
                    return False
            self._log.warning(
                "invalid request message topic=%s partition=%s offset=%s error=%s",
                record.topic,
                record.partition,
                record.offset,
                exc,
            )
            return True

        request_id = request["request_id"]
        client_id = request["client_id"]
        expires_at = request["expires_at"]
        reply_topic = request["reply_topic"]
        metadata = dict(request["metadata"])

        try:
            result = self._llm_client.chat_completion(
                model=request["model"],
                messages=request["messages"],
                options=request["options"],
            )
            message = self._build_response_message(
                request_id=request_id,
                client_id=client_id,
                status="completed",
                metadata=metadata,
                expires_at=expires_at,
                response_payload=result,
            )
            self._kafka_publisher.publish(request_id, message, topic=reply_topic)
            self._log.info(
                "response published id=%s topic=%s partition=%s offset=%s",
                request_id,
                record.topic,
                record.partition,
                record.offset,
            )
            return True
        except Exception as exc:
            self._log.exception("request processing failed id=%s", request_id)
            try:
                self._publish_error(
                    request_id=request_id,
                    client_id=client_id,
                    metadata=metadata,
                    expires_at=expires_at,
                    reply_topic=reply_topic,
                    error=exc,
                )
                return True
            except Exception:
                self._log.exception("failed to publish error response id=%s", request_id)
                return False

    def _run(self) -> None:
        self._log.info("chat worker started (worker_id=%s)", self._worker_id)
        while not self._stop_event.is_set():
            try:
                timeout_ms = max(int(self._poll_seconds * 1000), 100)
                records = self._kafka_consumer.poll(timeout_ms=timeout_ms, max_records=1)
            except Exception:
                self._log.exception("failed to poll request topic")
                self._stop_event.wait(self._poll_seconds)
                continue

            if not records:
                continue

            for record in records:
                should_commit = False
                try:
                    should_commit = self._handle_record(record)
                except Exception:
                    self._log.exception(
                        "unexpected worker failure topic=%s partition=%s offset=%s",
                        record.topic,
                        record.partition,
                        record.offset,
                    )

                if should_commit:
                    try:
                        self._kafka_consumer.commit_message(record)
                    except Exception:
                        self._log.exception(
                            "failed to commit offset topic=%s partition=%s offset=%s",
                            record.topic,
                            record.partition,
                            record.offset,
                        )
                        self._stop_event.wait(self._poll_seconds)

        self._log.info("chat worker stopped")
