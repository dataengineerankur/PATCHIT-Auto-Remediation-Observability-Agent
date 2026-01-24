from __future__ import annotations

from patchit.context.collector import CodeContextCollector
from patchit.models import StackFrame


def test_context_collector_translates_paths(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    real = repo / "airflow" / "dags" / "task_lib"
    real.mkdir(parents=True)
    f = real / "x.py"
    f.write_text("a=1\nb=2\nc=3\n", encoding="utf-8")

    collector = CodeContextCollector(
        project_root=str(repo),
        translate_from="/opt/airflow/dags",
        translate_to=str(repo / "airflow" / "dags"),
    )
    frames = [StackFrame(file_path="/opt/airflow/dags/task_lib/x.py", line_number=2)]
    ctx = collector.collect(frames)
    assert "airflow/dags/task_lib/x.py" in ctx
    assert "2:" in str(ctx["airflow/dags/task_lib/x.py"]["snippet"])



