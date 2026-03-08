from __future__ import annotations

from typing import Any

import httpx


class LLMClient:
    def __init__(self, base_url: str, api_key: str | None, timeout_seconds: float) -> None:
        base = base_url.rstrip("/")
        if base.endswith("/chat/completions"):
            self._chat_url = base
        else:
            self._chat_url = f"{base}/chat/completions"
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds

    def chat_completion(
        self,
        model: str,
        messages: list[dict[str, Any]],
        options: dict[str, Any],
    ) -> dict[str, Any]:
        # Force non-streaming completion calls and drop stream-only options.
        payload_options = dict(options or {})
        payload_options.pop("stream", None)
        payload_options.pop("stream_options", None)

        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "stream": False,
        }
        payload.update(payload_options)
        payload["stream"] = False

        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"

        with httpx.Client(timeout=self._timeout_seconds) as client:
            response = client.post(self._chat_url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        answer_text = ""
        choices = data.get("choices") if isinstance(data, dict) else None
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                message = first.get("message")
                if isinstance(message, dict):
                    content = message.get("content")
                    if isinstance(content, str):
                        answer_text = content
                    elif isinstance(content, list):
                        parts: list[str] = []
                        for item in content:
                            if isinstance(item, dict):
                                text = item.get("text")
                                if isinstance(text, str):
                                    parts.append(text)
                        answer_text = "".join(parts)

        return {
            "answer_text": answer_text,
            "provider": "openai-compatible",
            "raw": data,
        }
