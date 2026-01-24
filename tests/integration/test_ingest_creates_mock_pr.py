from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from patchit.models import UniversalFailureEvent
from patchit.service.app import create_app
from patchit.settings import Settings


def test_ingest_creates_mock_pr_and_audit_log(tmp_path) -> None:
    # Arrange a fake Airflow log file containing a traceback pointing at airflow task code
    logs_dir = tmp_path / "repo" / "airflow" / "logs"
    logs_dir.mkdir(parents=True)
    log_path = logs_dir / "task.log"
    log_path.write_text(
        "\n".join(
            [
                "INFO - starting",
                "Traceback (most recent call last):",
                "  File \"/opt/airflow/dags/task_lib/fetch.py\", line 27, in fetch_payload",
                "    raise UpstreamApiError(f\"status_code={r.status_code} error payload: {r.text}\")",
                "task_lib.errors.UpstreamApiError: status_code=500 error payload: {\"detail\": \"upstream_unavailable\"}",
            ]
        ),
        encoding="utf-8",
    )

    mock_pr_dir = tmp_path / "mock_github"
    audit_path = tmp_path / "audit.jsonl"

    s = Settings(
        github_mode="mock",
        github_repo="local/mock",
        mock_github_dir=str(mock_pr_dir),
        audit_log_path=str(audit_path),
        patch_store_dir=str(tmp_path / "patches"),
        repo_root=".",
        # For this test, force deterministic fallback + verify-before-PR (no external LLM needed).
        agent_mode="off",
        allow_deterministic_fallback=True,
        code_translate_from="/opt/airflow/dags",
        code_translate_to=str(tmp_path / "repo" / "airflow" / "dags"),
    )
    app = create_app(s)
    client = TestClient(app)

    ev = UniversalFailureEvent(
        event_id="e1",
        platform="airflow",
        pipeline_id="patchit_complex_test_dag",
        run_id="manual__1",
        task_id="fetch_payload",
        try_number=1,
        status="failed",
        log_uri=f"file://{log_path}",
        artifact_uris=[],
        metadata={},
    )

    # Act
    r = client.post("/events/ingest", json=ev.model_dump(mode="json"))
    assert r.status_code == 200, r.text
    body = r.json()

    # Assert mock PR exists
    assert body["pr"] is not None
    prs_dir = mock_pr_dir / "prs"
    assert prs_dir.exists()
    pr_files = list(prs_dir.glob("*.json"))
    assert pr_files, "expected mock PR metadata file"

    # Assert audit log exists and contains received event
    assert audit_path.exists()
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    assert any(json.loads(ln)["event_type"] == "event.received" for ln in lines)



