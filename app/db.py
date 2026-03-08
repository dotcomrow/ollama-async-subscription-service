from __future__ import annotations

import re
import uuid
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

_TABLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$")


class ChatDatabase:
    def __init__(self, database_url: str, async_messages_table: str) -> None:
        self._database_url = database_url
        self._table = self._validate_table_ident(async_messages_table)

    @staticmethod
    def _validate_table_ident(value: str) -> str:
        if not _TABLE_IDENT_RE.match(value):
            raise ValueError(
                "ASYNC_MESSAGES_TABLE must be <table> or <schema.table> and contain only letters, numbers, and underscores"
            )
        return value

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._database_url, row_factory=dict_row)

    def ping(self) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM {self._table} LIMIT 1")
                cur.fetchone()
            return True
        except Exception:
            return False

    def insert_chat_request(
        self,
        *,
        client_id: str,
        model: str,
        messages: list[dict[str, Any]],
        options: dict[str, Any],
        metadata: dict[str, Any],
        max_attempts: int,
        session_variables: dict[str, str],
    ) -> dict[str, Any]:
        request_id = str(uuid.uuid4())

        merged_metadata = dict(metadata or {})
        async_meta = merged_metadata.get("_async")
        if not isinstance(async_meta, dict):
            async_meta = {}
        async_meta["attempt_count"] = 0
        async_meta["max_attempts"] = max_attempts
        async_meta["worker_id"] = None
        merged_metadata["_async"] = async_meta

        request_payload = {
            "model": model,
            "messages": messages,
            "options": options or {},
            "metadata": metadata or {},
            "session_variables": session_variables or {},
        }

        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._table} (
                    request_id,
                    client_id,
                    status,
                    request_payload,
                    metadata
                ) VALUES (%s, %s, 'pending', %s, %s)
                RETURNING
                    request_id,
                    status,
                    client_id,
                    request_payload,
                    response_payload,
                    error_payload,
                    metadata,
                    COALESCE((metadata #>> '{{_async,attempt_count}}')::integer, 0) AS attempt_count,
                    COALESCE((metadata #>> '{{_async,max_attempts}}')::integer, 0) AS max_attempts,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    created_at,
                    updated_at,
                    completed_at,
                    expires_at
                """,
                (
                    request_id,
                    client_id,
                    Json(request_payload),
                    Json(merged_metadata),
                ),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("Failed to insert chat request")
            return row

    def get_chat_request(self, request_id: str) -> dict[str, Any] | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    request_id,
                    status,
                    client_id,
                    request_payload,
                    response_payload,
                    error_payload,
                    metadata,
                    COALESCE((metadata #>> '{{_async,attempt_count}}')::integer, 0) AS attempt_count,
                    COALESCE((metadata #>> '{{_async,max_attempts}}')::integer, 0) AS max_attempts,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    created_at,
                    updated_at,
                    completed_at,
                    expires_at
                FROM {self._table}
                WHERE request_id = %s
                """,
                (request_id,),
            )
            return cur.fetchone()

    def claim_next_pending_request(self, worker_id: str) -> dict[str, Any] | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                WITH next_job AS (
                    SELECT id
                    FROM {self._table}
                    WHERE status = 'pending'
                      AND (expires_at IS NULL OR expires_at > now())
                    ORDER BY created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE {self._table} AS req
                SET
                    status = 'processing',
                    updated_at = now(),
                    metadata = jsonb_set(
                        jsonb_set(
                            COALESCE(req.metadata, '{{}}'::jsonb),
                            '{{_async,attempt_count}}',
                            to_jsonb(COALESCE((req.metadata #>> '{{_async,attempt_count}}')::integer, 0) + 1),
                            true
                        ),
                        '{{_async,worker_id}}',
                        to_jsonb(%s::text),
                        true
                    )
                FROM next_job
                WHERE req.id = next_job.id
                RETURNING
                    req.request_id,
                    req.client_id,
                    req.request_payload,
                    req.metadata,
                    req.expires_at,
                    COALESCE((req.metadata #>> '{{_async,attempt_count}}')::integer, 0) AS attempt_count,
                    COALESCE((req.metadata #>> '{{_async,max_attempts}}')::integer, 1) AS max_attempts
                """,
                (worker_id,),
            )
            return cur.fetchone()

    def requeue_request(self, request_id: str, error: str) -> None:
        error_payload = {"error": error[:4000], "retryable": True}
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self._table}
                SET
                    status = 'pending',
                    error_payload = %s,
                    updated_at = now()
                WHERE request_id = %s
                """,
                (Json(error_payload), request_id),
            )
