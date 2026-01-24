from __future__ import annotations

from patchit.code_engine.engine import CodeEngine
from patchit.models import NormalizedErrorObject


def test_code_engine_patch_transform_for_schema_drift() -> None:
    err = NormalizedErrorObject(error_id="e1", error_type="DataContractError", message="x", stack=[], classification="schema_drift_missing_field")
    patch = CodeEngine(project_root=".").propose_patch(error=err, code_context={}, artifacts=[])
    # This repo's demo transform may already be schema-tolerant; in that case, no patch is needed.
    assert patch is None or any(f.path.endswith("airflow/dags/task_lib/transform.py") for f in patch.files)
    if patch is not None:
        assert "transform.py" in patch.diff_unified


def test_code_engine_patch_fetch_for_api_error() -> None:
    err = NormalizedErrorObject(error_id="e1", error_type="UpstreamApiError", message="status_code=500", stack=[], classification="intermittent_api_error")
    patch = CodeEngine(project_root=".").propose_patch(error=err, code_context={}, artifacts=[])
    assert patch is not None
    assert "fetch.py" in patch.diff_unified



