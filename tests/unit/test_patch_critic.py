from __future__ import annotations

from patchit.critic.patch_critic import should_reject_patch
from patchit.models import NormalizedErrorObject, PatchFileChange, PatchOperation, PatchProposal


def test_patch_critic_rejects_logging_only_for_missing_artifact():
    err = NormalizedErrorObject(
        error_id="e",
        error_type="FileNotFoundError",
        message="No such file or directory: enriched.json",
        classification="python_file_not_found",
    )
    patch = PatchProposal(
        patch_id="p",
        title="test",
        rationale="x",
        diff_unified="\n".join(
            [
                "--- a/repo/airflow/dags/grocery_load_dag.py",
                "+++ b/repo/airflow/dags/grocery_load_dag.py",
                "@@ -1,3 +1,7 @@",
                "+import logging",
                "+logger = logging.getLogger(__name__)",
                "+# debug",
                "+logger.info('missing enriched')",
            ]
        )
        + "\n",
        files=[PatchFileChange(path="airflow/dags/grocery_load_dag.py", operation=PatchOperation.update)],
        confidence=0.5,
        requires_human_approval=True,
    )
    res = should_reject_patch(error=err, patch=patch)
    assert res.ok is False
    assert res.patch_class == "logging_only"


def test_patch_critic_allows_behavior_change_for_missing_artifact():
    err = NormalizedErrorObject(
        error_id="e",
        error_type="FileNotFoundError",
        message="No such file or directory: enriched.json",
        classification="python_file_not_found",
    )
    patch = PatchProposal(
        patch_id="p",
        title="test",
        rationale="x",
        diff_unified="\n".join(
            [
                "--- a/repo/x.py",
                "+++ b/repo/x.py",
                "@@ -1,2 +1,2 @@",
                "-raise FileNotFoundError('x')",
                "+return None",
            ]
        )
        + "\n",
        files=[PatchFileChange(path="x.py", operation=PatchOperation.update)],
        confidence=0.5,
        requires_human_approval=True,
    )
    res = should_reject_patch(error=err, patch=patch)
    assert res.ok is True






