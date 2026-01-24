from __future__ import annotations

import json

from patchit.repos.registry import select_repo_context
from patchit.settings import Settings


def test_repo_registry_selects_by_pipeline_id_regex() -> None:
    s = Settings()
    s.repo_registry_json = json.dumps(
        [
            {"key": "repoA", "pipeline_id_regex": "^alpha_", "repo_root": "repos/a", "github_repo": "org/a"},
            {"key": "repoB", "pipeline_id_regex": "^beta_", "repo_root": "repos/b", "github_repo": "org/b"},
        ]
    )

    a = select_repo_context(settings=s, pipeline_id="alpha_payments")
    assert a.key == "repoA"
    assert a.github_repo == "org/a"
    assert a.repo_root_dir.endswith("repos/a")

    b = select_repo_context(settings=s, pipeline_id="beta_risk")
    assert b.key == "repoB"
    assert b.github_repo == "org/b"
    assert b.repo_root_dir.endswith("repos/b")


def test_repo_registry_falls_back_to_default() -> None:
    s = Settings()
    s.repo_root = "repo"
    s.github_repo = "owner/default"
    s.github_base_branch = "main"

    ctx = select_repo_context(settings=s, pipeline_id="no_match")
    assert ctx.key == "default"
    assert ctx.github_repo == "owner/default"


