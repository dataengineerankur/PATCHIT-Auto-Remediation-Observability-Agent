from __future__ import annotations

import os
from pathlib import Path

from patchit.parsers.log_fetcher import LogFetcher


def test_log_fetcher_attempt_fallback_reads_existing_attempt(tmp_path: Path) -> None:
    logs = tmp_path / "logs" / "dag_id=x" / "run_id=r" / "task_id=t"
    logs.mkdir(parents=True)
    (logs / "attempt=1.log").write_text("one\n", encoding="utf-8")
    (logs / "attempt=2.log").write_text("two\n", encoding="utf-8")

    uri = f"file://{logs}/attempt=3.log"
    out = LogFetcher().fetch(uri)
    assert out.strip() == "two"


def test_log_fetcher_translate_from_to_for_file_uri(tmp_path: Path) -> None:
    # Given a URI path that doesn't exist (original), but does exist after translation.
    original_root = "/opt/airflow/logs"
    translated_root = str(tmp_path / "mounted_logs")

    real_path = Path(translated_root) / "dag_id=a" / "run_id=b" / "task_id=c"
    real_path.mkdir(parents=True)
    (real_path / "attempt=1.log").write_text("ok\n", encoding="utf-8")

    uri = f"file://{original_root}/dag_id=a/run_id=b/task_id=c/attempt=1.log"
    out = LogFetcher(translate_from=original_root, translate_to=translated_root).fetch(uri)
    assert out.strip() == "ok"






