from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx
import time


@dataclass(frozen=True)
class OpenRouterClient:
    """
    Calls OpenRouter via OpenAI-compatible API.

    Endpoint: POST {base_url}/chat/completions
    Docs: https://openrouter.ai/docs
    """

    api_key: str
    base_url: str = "https://openrouter.ai/api/v1"
    timeout_s: float = 60.0
    site_url: str | None = None
    site_name: str | None = None
    max_retries: int = 3
    retry_backoff_s: float = 0.8

    def chat(self, *, model: str, messages: List[Dict[str, str]], max_tokens: int = 2048) -> str:
        url = f"{self.base_url.rstrip('/')}/chat/completions"
        headers: Dict[str, str] = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        # Optional ranking headers
        if self.site_url:
            headers["HTTP-Referer"] = self.site_url
        if self.site_name:
            headers["X-Title"] = self.site_name

        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            # IMPORTANT: OpenRouter may otherwise assume an extremely high max_tokens (e.g. 65536),
            # which can fail with 402 "more credits" even for small prompts.
            "max_tokens": int(max(1, min(int(max_tokens), 8192))),
        }

        last_err: Exception | None = None
        for attempt in range(1, max(1, int(self.max_retries)) + 1):
            try:
                with httpx.Client(timeout=self.timeout_s) as client:
                    r = client.post(url, headers=headers, json=payload)
                    if r.status_code != 200:
                        raise RuntimeError(f"openrouter_http_{r.status_code}: {r.text[:1500]}")
                    data = r.json()
                try:
                    return data["choices"][0]["message"]["content"]
                except Exception as e:  # noqa: BLE001
                    raise RuntimeError(f"openrouter_response_parse_error: {data}") from e
            except (
                httpx.ReadError,
                httpx.RemoteProtocolError,
                httpx.ProtocolError,
                httpx.ConnectError,
                httpx.TimeoutException,
            ) as e:
                last_err = e
                # Transient network/proxy issues are common (ex: "incomplete chunked read").
                if attempt < int(self.max_retries):
                    time.sleep(self.retry_backoff_s * (2 ** (attempt - 1)))
                    continue
                raise RuntimeError(f"openrouter_transient_error after {attempt} attempts: {e}") from e
            except Exception as e:  # noqa: BLE001
                # Non-network errors should not be retried (ex: bad request, parse error).
                last_err = e
                raise

        # Should be unreachable, but keep a defensive error.
        raise RuntimeError(f"openrouter_failed: {last_err}")


