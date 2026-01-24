from __future__ import annotations

import os
import subprocess
from types import SimpleNamespace

import pytest

from patchit.local_sync.worktree import apply_patch_to_worktree


def _cp(rc: int, out: str = "", err: str = ""):
    return SimpleNamespace(returncode=rc, stdout=out, stderr=err)


def test_worktree_falls_back_to_patch_when_git_apply_fails(monkeypatch, tmp_path):
    """
    Regression test for Cursor-style diffs where `git apply` returns "corrupt patch"
    but `patch -p1` can still apply the unified diff.
    """
    repo = tmp_path / "repo"
    wt_root = tmp_path / "wts"
    repo.mkdir()
    wt_root.mkdir()

    # minimal "is a git repo" check
    (repo / ".git").mkdir()

    calls = []

    def fake_run(cmd, cwd=None, check=False, capture_output=False, text=False, timeout=None):
        calls.append({"cmd": cmd, "cwd": cwd})
        # branch delete + worktree add should succeed
        if cmd[:2] == ["git", "branch"]:
            return _cp(0)
        if cmd[:3] == ["git", "worktree", "add"]:
            # create worktree path as if git created it
            os.makedirs(cmd[4], exist_ok=True)  # wt_path
            return _cp(0, out="worktree added")
        if cmd[:2] == ["git", "apply"]:
            return _cp(128, err="error: corrupt patch at line 31\n")
        if cmd[:1] == ["patch"]:
            return _cp(0, out="patching file airflow/dags/x.py\n")
        return _cp(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    res = apply_patch_to_worktree(
        repo_path=str(repo),
        worktree_root=str(wt_root),
        patch_id="565e78c108954e7eacf74f8c67d6e996",
        diff_text="```diff\ndiff --git a/repo/airflow/dags/x.py b/repo/airflow/dags/x.py\n--- a/repo/airflow/dags/x.py\n+++ b/repo/airflow/dags/x.py\n@@ -1,1 +1,1 @@\n-print('a')\n+print('b')\n```",
        base_ref="HEAD",
    )

    assert res.ok is True
    assert res.stage in ("ok_patch_fallback", "ok")
    # ensure both commands were attempted in order: git apply -> patch fallback
    cmd_strs = [" ".join(c["cmd"]) for c in calls]
    assert any(s.startswith("git apply ") for s in cmd_strs)
    assert any(s.startswith("patch -p1 ") for s in cmd_strs)


