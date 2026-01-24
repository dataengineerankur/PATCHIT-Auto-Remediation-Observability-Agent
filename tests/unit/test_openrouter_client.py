from __future__ import annotations

import types

import httpx

from patchit.llm.openrouter_client import OpenRouterClient


class _FakeResp:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = "x"

    def json(self):
        return self._payload


class _FakeClient:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url, headers=None, json=None):
        assert url.endswith("/chat/completions")
        assert "Authorization" in (headers or {})
        assert json and "model" in json and "messages" in json
        return _FakeResp(200, {"choices": [{"message": {"content": "ok"}}]})


def test_openrouter_client_parses_chat_response(monkeypatch):
    monkeypatch.setattr(httpx, "Client", _FakeClient)
    c = OpenRouterClient(api_key="test")
    out = c.chat(model="openai/gpt-5.2", messages=[{"role": "user", "content": "hi"}])
    assert out == "ok"






