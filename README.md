# ollama-async-subscription-service

Stateless async worker for Hasura + Kafka + Open WebUI/Ollama.

## What it does

- Consumes request envelopes from Kafka topic `graphql.async.requests.v1`.
- Filters by handler name.
- Calls an OpenAI-compatible chat completion endpoint.
- Publishes final responses to Kafka topic `graphql.async.responses.v1` (or per-message `options.reply_topic`).

This service does not write to, read from, or require a database.

## API

- `GET /healthz`

## Environment variables

- `ASYNC_REQUEST_TOPIC` (default: `graphql.async.requests.v1`)
- `ASYNC_RESPONSE_TOPIC` (default: `graphql.async.responses.v1`)
- `ASYNC_RESPONSE_EXPIRES_SECONDS` (default: `86400`)
- `LLM_API_BASE_URL` (default: `http://open-webui.ollama.svc.cluster.local/v1`)
- `LLM_API_KEY` (optional): Bearer token for the LLM API.
- `LLM_DEFAULT_MODEL` (default: `deepseek-r1:14b`)
- `LLM_TIMEOUT_SECONDS` (default: `120`)
- `WORKER_POLL_SECONDS` (default: `1`)
- `WORKER_ID` (default: `<hostname>-worker`)
- `REQUEST_HANDLER_NAMES` (default: `ai-service,ollama,ollama-async-subscription-service`)
- `KAFKA_BOOTSTRAP_SERVERS` (required): comma-separated brokers (for example `kafka-1:9092,kafka-2:9092`)
- `KAFKA_REQUEST_CONSUMER_GROUP` (default: `ollama-async-subscription-service`)
- `KAFKA_AUTO_OFFSET_RESET` (default: `earliest`): `earliest` or `latest`
- `KAFKA_SECURITY_PROTOCOL` (default: `PLAINTEXT`): `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`
- `KAFKA_SASL_MECHANISM` (required for SASL): for example `SCRAM-SHA-256`
- `KAFKA_SASL_USERNAME` (required for SASL)
- `KAFKA_SASL_PASSWORD` (required for SASL)
- `KAFKA_SSL_CAFILE` (optional): CA bundle path when using SSL/SASL_SSL

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export KAFKA_BOOTSTRAP_SERVERS='kafka-1:9092,kafka-2:9092'
export ASYNC_REQUEST_TOPIC='graphql.async.requests.v1'
export ASYNC_RESPONSE_TOPIC='graphql.async.responses.v1'
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## Docker build/run

```bash
docker build -t ghcr.io/<your-org>/ollama-async-subscription-service:latest .

docker run --rm -p 8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS='kafka-1:9092,kafka-2:9092' \
  -e ASYNC_REQUEST_TOPIC='graphql.async.requests.v1' \
  -e ASYNC_RESPONSE_TOPIC='graphql.async.responses.v1' \
  -e LLM_API_BASE_URL='http://open-webui.ollama.svc.cluster.local/v1' \
  ghcr.io/<your-org>/ollama-async-subscription-service:latest
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
  "submitted_at": "2026-03-08T20:49:11.038758Z"
}
```

If `payload.messages` is provided, it is used directly. If not provided, `payload.prompt` is converted into a single user message.

## Kafka response message

Published to topic `graphql.async.responses.v1` (configurable via `ASYNC_RESPONSE_TOPIC`):

```json
{
  "request_id": "req-9f7d1e8a",
  "client_id": "user-123",
  "status": "completed",
  "response_payload": {
    "answer_text": "Hello from Ollama",
    "provider": "openai-compatible",
    "raw": {
      "id": "chatcmpl-123",
      "choices": [
        {
          "index": 0,
          "message": {
            "role": "assistant",
            "content": "Hello from Ollama"
          }
        }
      ]
    }
  },
  "metadata": { "worker": "ollama-async-subscription-service" },
  "completed_at": "2026-03-07T22:14:00Z",
  "expires_at": "2026-03-08T22:14:00Z"
}
```
