from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx


@dataclass(frozen=True)
class CursorAgentTarget:
    branch_name: str
    url: str | None = None
    pr_url: str | None = None
    auto_create_pr: bool | None = None
    open_as_cursor_github_app: bool | None = None


@dataclass(frozen=True)
class CursorAgent:
    id: str
    name: str | None
    status: str
    source_repository: str | None = None
    source_ref: str | None = None
    target: CursorAgentTarget | None = None
    summary: str | None = None


class CursorCloudAgentsClient:
    """
    Minimal client for Cursor Cloud Agents API.

    Docs:
    - https://cursor.com/docs/cloud-agent/api/endpoints
    """

    def __init__(self, *, api_key: str, base_url: str = "https://api.cursor.com", timeout_s: float = 60.0) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout_s = float(timeout_s)

    def _sleep_for_rate_limit(self, *, response: httpx.Response, attempt: int) -> float:
        """
        Cursor rate limits the API (commonly 60 req/min). Respect Retry-After when present.
        """
        ra = response.headers.get("retry-after")
        try:
            if ra is not None:
                v = float(ra)
                if v > 0:
                    return min(90.0, v)
        except Exception:
            pass
        # Exponential backoff with jitter-less cap
        return min(90.0, 2.0 * (2**max(0, attempt - 1)))

    def _request_with_retries(self, method: str, path: str, *, json_body: Dict[str, Any] | None = None) -> httpx.Response:
        with httpx.Client(base_url=self._base_url, timeout=self._timeout_s, auth=(self._api_key, "")) as client:
            last: httpx.Response | None = None
            for attempt in range(1, 6):
                r = client.request(method, path, json=json_body)
                last = r
                if r.status_code == 429:
                    time.sleep(self._sleep_for_rate_limit(response=r, attempt=attempt))
                    continue
                # Retry transient errors
                if r.status_code in (408, 409, 425, 500, 502, 503, 504):
                    time.sleep(min(10.0, 0.6 * attempt))
                    continue
                return r
            assert last is not None
            return last

    def launch_agent(
        self,
        *,
        repository: str,
        ref: str,
        branch_name: str,
        prompt_text: str,
        auto_create_pr: bool = False,
        open_as_cursor_github_app: bool = True,
    ) -> CursorAgent:
        # NOTE: Cursor's API is strict about request keys. In practice, some deployments reject top-level
        # "name" (and other optional fields) with `unrecognized_keys`. Keep the payload minimal and spec-like.
        payload: Dict[str, Any] = {
            "source": {"repository": repository, "ref": ref},
            "target": {
                "branchName": branch_name,
                "autoCreatePr": bool(auto_create_pr),
                "openAsCursorGithubApp": bool(open_as_cursor_github_app),
            },
            "prompt": {"text": prompt_text},
        }
        r = self._request_with_retries("POST", "/v0/agents", json_body=payload)
        if r.status_code >= 400:
            raise RuntimeError(f"cursor_http_{r.status_code}: {r.text[:1200]}")
        data = r.json() or {}
        return _parse_agent(data)

    def get_agent(self, *, agent_id: str) -> CursorAgent:
        r = self._request_with_retries("GET", f"/v0/agents/{agent_id}")
        if r.status_code >= 400:
            raise RuntimeError(f"cursor_http_{r.status_code}: {r.text[:1200]}")
        data = r.json() or {}
        return _parse_agent(data)

    def wait_for_terminal(
        self,
        *,
        agent_id: str,
        poll_interval_s: float = 3.0,
        max_wait_s: float = 600.0,
    ) -> CursorAgent:
        deadline = time.monotonic() + float(max_wait_s)
        while True:
            a = self.get_agent(agent_id=agent_id)
            st = (a.status or "").upper()
            if st in ("FINISHED", "FAILED", "STOPPED", "CANCELLED"):
                return a
            if time.monotonic() >= deadline:
                raise RuntimeError(f"cursor_agent_timeout_{max_wait_s}s")
            time.sleep(max(0.5, float(poll_interval_s)))


def _parse_agent(data: Dict[str, Any]) -> CursorAgent:
    aid = str(data.get("id") or "")
    if not aid:
        raise RuntimeError("cursor_agent_missing_id")
    source = data.get("source") or {}
    target = data.get("target") or {}
    tgt: Optional[CursorAgentTarget] = None
    bn = target.get("branchName") or target.get("branch_name")
    if isinstance(bn, str) and bn:
        tgt = CursorAgentTarget(
            branch_name=bn,
            url=(target.get("url") if isinstance(target.get("url"), str) else None),
            pr_url=(target.get("prUrl") if isinstance(target.get("prUrl"), str) else None),
            auto_create_pr=(target.get("autoCreatePr") if isinstance(target.get("autoCreatePr"), bool) else None),
            open_as_cursor_github_app=(
                target.get("openAsCursorGithubApp") if isinstance(target.get("openAsCursorGithubApp"), bool) else None
            ),
        )
    return CursorAgent(
        id=aid,
        name=(data.get("name") if isinstance(data.get("name"), str) else None),
        status=str(data.get("status") or ""),
        source_repository=(source.get("repository") if isinstance(source.get("repository"), str) else None),
        source_ref=(source.get("ref") if isinstance(source.get("ref"), str) else None),
        target=tgt,
        summary=(data.get("summary") if isinstance(data.get("summary"), str) else None),
    )



