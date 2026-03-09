from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(value, minimum)


@dataclass(frozen=True)
class Settings:
    async_request_topic: str
    async_response_topic: str
    response_expires_seconds: int
    llm_api_base_url: str
    llm_api_key: str | None
    llm_default_model: str
    llm_timeout_seconds: float
    worker_poll_seconds: float
    worker_id: str
    request_handler_names: tuple[str, ...]
    kafka_bootstrap_servers: list[str]
    kafka_security_protocol: str
    kafka_sasl_mechanism: str | None
    kafka_sasl_username: str | None
    kafka_sasl_password: str | None
    kafka_ssl_cafile: str | None
    kafka_request_consumer_group: str
    kafka_auto_offset_reset: str


def load_settings() -> Settings:
    async_request_topic = os.getenv(
        "ASYNC_REQUEST_TOPIC", "graphql.async.requests.v1"
    ).strip()
    if not async_request_topic:
        raise RuntimeError("ASYNC_REQUEST_TOPIC must not be empty")

    async_response_topic = os.getenv(
        "ASYNC_RESPONSE_TOPIC", "graphql.async.responses.v1"
    ).strip()
    if not async_response_topic:
        raise RuntimeError("ASYNC_RESPONSE_TOPIC must not be empty")

    kafka_bootstrap_servers_raw = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not kafka_bootstrap_servers_raw:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is required")
    kafka_bootstrap_servers = [
        server.strip()
        for server in kafka_bootstrap_servers_raw.split(",")
        if server.strip()
    ]
    if not kafka_bootstrap_servers:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS must contain at least one server")

    kafka_security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip().upper()
    if kafka_security_protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
        raise RuntimeError(
            "KAFKA_SECURITY_PROTOCOL must be one of PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL"
        )

    kafka_sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    if kafka_sasl_mechanism is not None:
        kafka_sasl_mechanism = kafka_sasl_mechanism.strip() or None

    kafka_sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    if kafka_sasl_username is not None:
        kafka_sasl_username = kafka_sasl_username.strip() or None

    kafka_sasl_password = os.getenv("KAFKA_SASL_PASSWORD")
    if kafka_sasl_password is not None:
        kafka_sasl_password = kafka_sasl_password.strip() or None

    kafka_ssl_cafile = os.getenv("KAFKA_SSL_CAFILE")
    if kafka_ssl_cafile is not None:
        kafka_ssl_cafile = kafka_ssl_cafile.strip() or None

    kafka_request_consumer_group = os.getenv(
        "KAFKA_REQUEST_CONSUMER_GROUP", "ollama-async-subscription-service"
    ).strip()
    if not kafka_request_consumer_group:
        raise RuntimeError("KAFKA_REQUEST_CONSUMER_GROUP must not be empty")

    kafka_auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest").strip().lower()
    if kafka_auto_offset_reset not in {"earliest", "latest"}:
        raise RuntimeError("KAFKA_AUTO_OFFSET_RESET must be 'earliest' or 'latest'")

    llm_api_base_url = os.getenv(
        "LLM_API_BASE_URL", "http://open-webui.ollama.svc.cluster.local/v1"
    ).strip()

    llm_api_key = os.getenv("LLM_API_KEY")
    if llm_api_key is not None:
        llm_api_key = llm_api_key.strip() or None

    llm_default_model = os.getenv("LLM_DEFAULT_MODEL", "deepseek-r1:14b").strip()
    if not llm_default_model:
        raise RuntimeError("LLM_DEFAULT_MODEL must not be empty")

    request_handler_names_raw = os.getenv(
        "REQUEST_HANDLER_NAMES", "ai-service,ollama,ollama-async-subscription-service"
    ).strip()
    request_handler_names: list[str] = []
    if request_handler_names_raw:
        for entry in request_handler_names_raw.split(","):
            normalized = entry.strip().lower()
            if normalized and normalized not in request_handler_names:
                request_handler_names.append(normalized)
    if not request_handler_names:
        request_handler_names = ["ai-service"]

    return Settings(
        async_request_topic=async_request_topic,
        async_response_topic=async_response_topic,
        response_expires_seconds=_env_int("ASYNC_RESPONSE_EXPIRES_SECONDS", 86400),
        llm_api_base_url=llm_api_base_url,
        llm_api_key=llm_api_key,
        llm_default_model=llm_default_model,
        llm_timeout_seconds=_env_float("LLM_TIMEOUT_SECONDS", 120.0),
        worker_poll_seconds=_env_float("WORKER_POLL_SECONDS", 1.0),
        worker_id=os.getenv("WORKER_ID", f"{socket.gethostname()}-worker").strip(),
        request_handler_names=tuple(request_handler_names),
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_security_protocol=kafka_security_protocol,
        kafka_sasl_mechanism=kafka_sasl_mechanism,
        kafka_sasl_username=kafka_sasl_username,
        kafka_sasl_password=kafka_sasl_password,
        kafka_ssl_cafile=kafka_ssl_cafile,
        kafka_request_consumer_group=kafka_request_consumer_group,
        kafka_auto_offset_reset=kafka_auto_offset_reset,
    )
