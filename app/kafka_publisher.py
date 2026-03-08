from __future__ import annotations

import json
from typing import Any

from kafka import KafkaProducer

from app.config import Settings


class KafkaResponsePublisher:
    def __init__(self, settings: Settings) -> None:
        producer_config: dict[str, Any] = {
            "bootstrap_servers": settings.kafka_bootstrap_servers,
            "client_id": settings.worker_id,
            "acks": "all",
            "retries": 5,
            "value_serializer": lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
            "key_serializer": lambda value: value.encode("utf-8"),
            "security_protocol": settings.kafka_security_protocol,
        }

        if settings.kafka_security_protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
            if not settings.kafka_sasl_mechanism:
                raise RuntimeError("KAFKA_SASL_MECHANISM is required for SASL security protocols")
            if not settings.kafka_sasl_username or not settings.kafka_sasl_password:
                raise RuntimeError("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for SASL")
            producer_config["sasl_mechanism"] = settings.kafka_sasl_mechanism
            producer_config["sasl_plain_username"] = settings.kafka_sasl_username
            producer_config["sasl_plain_password"] = settings.kafka_sasl_password

        if settings.kafka_security_protocol in {"SSL", "SASL_SSL"} and settings.kafka_ssl_cafile:
            producer_config["ssl_cafile"] = settings.kafka_ssl_cafile

        self._topic = settings.async_response_topic
        self._producer = KafkaProducer(**producer_config)

    def publish(self, request_id: str, message: dict[str, Any], topic: str | None = None) -> None:
        target_topic = topic or self._topic
        future = self._producer.send(target_topic, key=request_id, value=message)
        future.get(timeout=10)

    def close(self) -> None:
        self._producer.flush(timeout=10)
        self._producer.close(timeout=10)
