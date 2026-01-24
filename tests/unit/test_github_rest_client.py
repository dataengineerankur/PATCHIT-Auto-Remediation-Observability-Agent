from __future__ import annotations

import json
from typing import Any, Dict, Optional

import httpx

from patchit.gitops.github_rest import GitHubRestClient


def _make_transport() -> httpx.MockTransport:
    # In-memory “server state”
    state: Dict[str, Any] = {
        "default_branch": "main",
        "refs": {"heads/main": {"sha": "BASESHA"}},
        "files": {"airflow/dags/example.py": {"sha": "FILESHA", "content": "print('hi')\n"}},
        "prs": [],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        method = request.method.upper()

        if method == "GET" and path.endswith("/repos/owner/repo"):
            return httpx.Response(200, json={"default_branch": state["default_branch"]})

        if method == "GET" and path.endswith("/repos/owner/repo/git/ref/heads/main"):
            return httpx.Response(200, json={"object": {"sha": state["refs"]["heads/main"]["sha"]}})

        if method == "POST" and path.endswith("/repos/owner/repo/git/refs"):
            body = json.loads(request.content.decode("utf-8"))
            ref = body["ref"].replace("refs/", "")
            sha = body["sha"]
            # Simulate “already exists” => 422
            if ref in state["refs"]:
                return httpx.Response(422, json={"message": "Reference already exists"})
            state["refs"][ref] = {"sha": sha}
            return httpx.Response(201, json={"ref": body["ref"], "object": {"sha": sha}})

        if method == "GET" and path.endswith("/repos/owner/repo/contents/airflow/dags/example.py"):
            ref = request.url.params.get("ref", "main")
            _ = ref
            return httpx.Response(200, json={"sha": state["files"]["airflow/dags/example.py"]["sha"]})

        if method == "PUT" and path.endswith("/repos/owner/repo/contents/airflow/dags/example.py"):
            body = json.loads(request.content.decode("utf-8"))
            assert body["branch"].startswith("patchit/")
            assert "content" in body
            # Update sha to prove the call happened
            state["files"]["airflow/dags/example.py"]["sha"] = "NEWFILESHA"
            return httpx.Response(200, json={"content": {"sha": "NEWFILESHA"}})

        if method == "POST" and path.endswith("/repos/owner/repo/pulls"):
            body = json.loads(request.content.decode("utf-8"))
            pr_number = 123
            state["prs"].append(body)
            return httpx.Response(
                201,
                json={"number": pr_number, "title": body["title"], "html_url": f"https://github.com/owner/repo/pull/{pr_number}"},
            )

        return httpx.Response(404, json={"message": f"unhandled {method} {path}"})

    return httpx.MockTransport(handler)


def test_github_rest_client_branch_file_pr_flow() -> None:
    transport = _make_transport()
    c = GitHubRestClient(token="t", repo="owner/repo", transport=transport)

    base = c.get_repo_default_branch()
    assert base == "main"

    sha = c.get_branch_head_sha(branch=base)
    assert sha == "BASESHA"

    c.create_branch(new_branch="patchit/abcd1234", from_sha=sha)

    file_sha: Optional[str] = c.get_file_sha(path="airflow/dags/example.py", ref=base)
    assert file_sha == "FILESHA"

    c.upsert_file(
        path="airflow/dags/example.py",
        content_text="print('patched')\n",
        branch="patchit/abcd1234",
        message="msg",
        known_sha=file_sha,
    )

    pr = c.create_pull_request(title="t", body="b", head="patchit/abcd1234", base=base)
    assert pr.mode == "real"
    assert pr.pr_number == 123


