from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx


@dataclass(frozen=True)
class GroqClient:
    """
    Calls Groq via OpenAI-compatible API.

    Endpoint: POST {base_url}/chat/completions
    Docs typically mirror OpenAI's schema for chat.completions.
    """

    api_key: str
    base_url: str = "https://api.groq.com/openai/v1"
    timeout_s: float = 60.0

    def chat(self, *, model: str, messages: List[Dict[str, str]], max_tokens: int = 2048) -> str:
        url = f"{self.base_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "max_tokens": int(max(1, min(int(max_tokens), 8192))),
        }
        with httpx.Client(timeout=self.timeout_s) as client:
            r = client.post(url, headers=headers, json=payload)
            if r.status_code != 200:
                raise RuntimeError(f"groq_http_{r.status_code}: {r.text[:1500]}")
            data = r.json()

        # OpenAI-compatible response
        try:
            return data["choices"][0]["message"]["content"]
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"groq_response_parse_error: {data}") from e


