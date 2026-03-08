# ollama-async-subscription-service

Async chat queue service for Hasura + Open WebUI/Ollama.

## What it does

- Consumes async request messages from Kafka topic `graphql.async.requests.v1`.
- Filters messages by handler name and processes matching requests with an OpenAI-compatible endpoint (Open WebUI/Ollama).
- Publishes completion/failure responses to Kafka topic `graphql.async.responses.v1` (or request-level `options.reply_topic`).
- GraphQL service consumes Kafka responses and updates async message state.
- Optional API enqueue endpoints are still available and now publish request envelopes to the request topic.

## API

- `GET /healthz`
- `POST /enqueue`
- `POST /hasura/actions/enqueue_chat`
- `GET /jobs/{request_id}`

## Environment variables

- `DATABASE_URL` (required): Postgres connection string.
- `ASYNC_MESSAGES_TABLE` (default: `graphql.client_async_messages`)
- `ASYNC_REQUEST_TOPIC` (default: `graphql.async.requests.v1`)
- `ASYNC_RESPONSE_TOPIC` (default: `graphql.async.responses.v1`)
- `ASYNC_RESPONSE_EXPIRES_SECONDS` (default: `86400`)
- `LLM_API_BASE_URL` (default: `http://open-webui.ollama.svc.cluster.local/v1`)
- `LLM_API_KEY` (optional): Bearer token for the LLM API.
- `LLM_DEFAULT_MODEL` (default: `deepseek-r1:14b`)
- `LLM_TIMEOUT_SECONDS` (default: `120`)
- `WORKER_POLL_SECONDS` (default: `1`)
- `WORKER_ID` (default: `<hostname>-worker`)
- `DEFAULT_MAX_ATTEMPTS` (default: `3`)
- `REQUEST_HANDLER_NAME` (default: `ai-service`): handler name used when this service enqueues API-originated requests.
- `REQUEST_HANDLER_NAMES` (default: `ai-service,ollama,ollama-async-subscription-service`): consumer accepts only these route handlers.
- `HASURA_ACTION_SECRET` (optional but recommended)
- `KAFKA_BOOTSTRAP_SERVERS` (required): comma-separated brokers (for example `kafka-1:9092,kafka-2:9092`)
- `KAFKA_REQUEST_CONSUMER_GROUP` (default: `ollama-async-subscription-service`)
- `KAFKA_AUTO_OFFSET_RESET` (default: `earliest`): `earliest` or `latest`
- `KAFKA_SECURITY_PROTOCOL` (default: `PLAINTEXT`): `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`
- `KAFKA_SASL_MECHANISM` (required for SASL): for example `SCRAM-SHA-512` or `PLAIN`
- `KAFKA_SASL_USERNAME` (required for SASL)
- `KAFKA_SASL_PASSWORD` (required for SASL)
- `KAFKA_SSL_CAFILE` (optional): CA bundle path when using SSL/SASL_SSL

## Database setup

`graphql.client_async_messages` must already exist (managed by GraphQL service).
Optional helper script (indexes/trigger only):

```sql
\i sql/001_init.sql
```

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL='postgresql://user:pass@host:5432/dbname'
export ASYNC_MESSAGES_TABLE='graphql.client_async_messages'
export KAFKA_BOOTSTRAP_SERVERS='kafka-1:9092,kafka-2:9092'
export ASYNC_REQUEST_TOPIC='graphql.async.requests.v1'
export ASYNC_RESPONSE_TOPIC='graphql.async.responses.v1'
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## Docker build/run

```bash
docker build -t ghcr.io/<your-org>/ollama-async-subscription-service:latest .

docker run --rm -p 8080:8080 \
  -e DATABASE_URL='postgresql://user:pass@host:5432/dbname' \
  -e ASYNC_MESSAGES_TABLE='graphql.client_async_messages' \
  -e KAFKA_BOOTSTRAP_SERVERS='kafka-1:9092,kafka-2:9092' \
  -e ASYNC_REQUEST_TOPIC='graphql.async.requests.v1' \
  -e ASYNC_RESPONSE_TOPIC='graphql.async.responses.v1' \
  -e LLM_API_BASE_URL='http://open-webui.ollama.svc.cluster.local/v1' \
  ghcr.io/<your-org>/ollama-async-subscription-service:latest
```

## Hasura integration pattern

1. Track `graphql.client_async_messages` in Hasura.
2. Publish request envelopes to `graphql.async.requests.v1` (for example via `publish_async_request` action).
3. Optional: use this service's `enqueue_chat` API if you also want direct HTTP enqueue support.
4. Client flow:
   - Call request submit mutation and get `request_id`.
   - Start GraphQL subscription on `client_async_messages(where: {request_id: {_eq: $request_id}})`.
   - Render updates until `status` is `completed` or `error`.

Example subscription:

```graphql
subscription AsyncChat($requestId: String!) {
  client_async_messages(where: {request_id: {_eq: $requestId}}) {
    request_id
    client_id
    status
    response_payload
    error_payload
    metadata
    created_at
    updated_at
    completed_at
    expires_at
  }
}
```

## Request envelope consumed from Kafka (`ASYNC_REQUEST_TOPIC`)

```json
{
  "spec_version": "async.request.v1",
  "request_id": "4f929e0b-1052-4f63-b081-5388ae073715",
  "client_id": "8502ffdd-2b71-4229-b7f5-e722659c16dc",
  "route": {
    "handler": "ai-service",
    "operation": "chat.completion"
  },
  "payload": {
    "conversationId": "",
    "prompt": "test"
  },
  "options": {
    "priority": "normal",
    "reply_topic": "graphql.async.responses.v1",
    "expires_at": "2026-03-09T20:49:11.038758Z"
  },
  "metadata": {
    "instanceId": "mfe-example-chat:1773002938269",
    "moduleKey": "mfe-example-chat",
    "source": "local-preview"
  },
  "submitted_at": "2026-03-08T20:49:11.038758Z",
  "trace": {
    "request_id": "4f929e0b-1052-4f63-b081-5388ae073715"
  },
  "action": "publish_async_request"
}
```

If `payload.messages` is provided, it is used directly. If not provided, the worker accepts `payload.prompt` and builds a single user message automatically.

## Request body for `/hasura/actions/enqueue_chat`

Hasura action body is still supported and transformed into the request envelope above:

```json
{
  "action": {"name": "enqueue_chat"},
  "input": {
    "model": "deepseek-r1:14b",
    "messages": [{"role": "user", "content": "Hello"}],
    "options": {"temperature": 0.2},
    "metadata": {"conversation_id": "abc123"},
    "client_id": "user-123",
    "max_attempts": 3
  },
  "session_variables": {
    "x-hasura-user-id": "user-123",
    "x-hasura-role": "user"
  }
}
```

Action response shape:

```json
{
  "request_id": "req-9f7d1e8a",
  "status": "pending"
}
```

## Kafka response message

Published to topic `graphql.async.responses.v1` (configurable via `ASYNC_RESPONSE_TOPIC`):

```json
{
  "request_id": "req-9f7d1e8a",
  "client_id": "user-123",
  "status": "completed",
  "response_payload": { "result": { "ok": true } },
  "metadata": { "worker": "billing-service" },
  "completed_at": "2026-03-07T22:14:00Z",
  "expires_at": "2026-03-08T22:14:00Z"
}
```
