from __future__ import annotations

import json
from typing import Any

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

from app.config import Settings


class KafkaRequestConsumer:
    def __init__(self, settings: Settings) -> None:
        consumer_config: dict[str, Any] = {
            "bootstrap_servers": settings.kafka_bootstrap_servers,
            "client_id": f"{settings.worker_id}-consumer",
            "group_id": settings.kafka_request_consumer_group,
            "enable_auto_commit": False,
            "auto_offset_reset": settings.kafka_auto_offset_reset,
            "value_deserializer": lambda value: json.loads(value.decode("utf-8"))
            if value is not None
            else None,
            "key_deserializer": lambda value: value.decode("utf-8")
            if value is not None
            else None,
            "security_protocol": settings.kafka_security_protocol,
            "request_timeout_ms": 30000,
            "session_timeout_ms": 10000,
            "heartbeat_interval_ms": 3000,
        }

        if settings.kafka_security_protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
            if not settings.kafka_sasl_mechanism:
                raise RuntimeError("KAFKA_SASL_MECHANISM is required for SASL security protocols")
            if not settings.kafka_sasl_username or not settings.kafka_sasl_password:
                raise RuntimeError("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for SASL")
            consumer_config["sasl_mechanism"] = settings.kafka_sasl_mechanism
            consumer_config["sasl_plain_username"] = settings.kafka_sasl_username
            consumer_config["sasl_plain_password"] = settings.kafka_sasl_password

        if settings.kafka_security_protocol in {"SSL", "SASL_SSL"} and settings.kafka_ssl_cafile:
            consumer_config["ssl_cafile"] = settings.kafka_ssl_cafile

        self._consumer = KafkaConsumer(settings.async_request_topic, **consumer_config)

    def poll(self, timeout_ms: int = 1000, max_records: int = 1) -> list[Any]:
        records: list[Any] = []
        batches = self._consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        for batch in batches.values():
            records.extend(batch)
        return records

    def commit_message(self, message: Any) -> None:
        topic_partition = TopicPartition(message.topic, message.partition)
        try:
            offset_meta = OffsetAndMetadata(message.offset + 1, "", -1)
        except TypeError:
            offset_meta = OffsetAndMetadata(message.offset + 1, "")
        self._consumer.commit({topic_partition: offset_meta})

    def close(self) -> None:
        self._consumer.close(timeout=10)
