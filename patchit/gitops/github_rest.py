from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from patchit.models import PullRequestResult


@dataclass(frozen=True)
class GitHubRestClient:
    """
    Minimal GitHub REST wrapper (Phase-1+).

    Supports:
    - create branch from base
    - upsert files via the Contents API (creates commits server-side)
    - create PR

    Notes:
    - We intentionally avoid git pushes; this works with only HTTPS + token.
    - This client is designed to be mockable in tests (httpx transport override).
    """

    token: str
    repo: str  # owner/name
    api_base: str = "https://api.github.com"
    timeout_s: float = 15.0
    transport: httpx.BaseTransport | None = None

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
        }

    def _client(self) -> httpx.Client:
        return httpx.Client(timeout=self.timeout_s, transport=self.transport)

    def get_repo_default_branch(self) -> str:
        url = f"{self.api_base}/repos/{self.repo}"
        with self._client() as c:
            r = c.get(url, headers=self._headers())
            r.raise_for_status()
            data = r.json()
        return str(data.get("default_branch") or "main")

    def get_branch_head_sha(self, *, branch: str) -> str:
        # GET /repos/{owner}/{repo}/git/ref/heads/{branch}
        url = f"{self.api_base}/repos/{self.repo}/git/ref/heads/{branch}"
        with self._client() as c:
            r = c.get(url, headers=self._headers())
            r.raise_for_status()
            data = r.json()
        return str((data.get("object") or {}).get("sha"))

    def create_branch(self, *, new_branch: str, from_sha: str) -> None:
        url = f"{self.api_base}/repos/{self.repo}/git/refs"
        payload = {"ref": f"refs/heads/{new_branch}", "sha": from_sha}
        with self._client() as c:
            r = c.post(url, headers=self._headers(), json=payload)
            # 422 if branch exists; treat as idempotent.
            if r.status_code == 422:
                return
            r.raise_for_status()

    def get_file_sha(self, *, path: str, ref: str) -> Optional[str]:
        url = f"{self.api_base}/repos/{self.repo}/contents/{path.lstrip('/')}"
        with self._client() as c:
            r = c.get(url, headers=self._headers(), params={"ref": ref})
            if r.status_code == 404:
                return None
            r.raise_for_status()
            data = r.json()
        return str(data.get("sha")) if isinstance(data, dict) and data.get("sha") else None

    def upsert_file(
        self,
        *,
        path: str,
        content_text: str,
        branch: str,
        message: str,
        known_sha: Optional[str] = None,
    ) -> None:
        url = f"{self.api_base}/repos/{self.repo}/contents/{path.lstrip('/')}"
        b64 = base64.b64encode(content_text.encode("utf-8")).decode("ascii")
        payload: Dict[str, Any] = {"message": message, "content": b64, "branch": branch}
        if known_sha:
            payload["sha"] = known_sha
        with self._client() as c:
            r = c.put(url, headers=self._headers(), json=payload)
            r.raise_for_status()

    def create_pull_request(self, *, title: str, body: str, head: str, base: str) -> PullRequestResult:
        url = f"{self.api_base}/repos/{self.repo}/pulls"
        payload = {"title": title, "body": body, "head": head, "base": base}
        with self._client() as c:
            r = c.post(url, headers=self._headers(), json=payload)
            r.raise_for_status()
            data = r.json()
        return PullRequestResult(
            mode="real",
            pr_number=int(data["number"]),
            pr_title=str(data["title"]),
            pr_url=str(data["html_url"]),
            branch_name=head,
        )



