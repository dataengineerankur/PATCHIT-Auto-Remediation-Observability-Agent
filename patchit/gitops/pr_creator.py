from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Callable
from typing import Dict

from patchit.gitops.github_rest import GitHubRestClient
from patchit.gitops.mock_github import MockGitHub
from patchit.models import PatchProposal, PullRequestResult
from patchit.settings import Settings


@dataclass(frozen=True)
class PullRequestCreator:
    settings: Settings

    def create(self, *, title: str, body: str, patch: PatchProposal) -> PullRequestResult:
        if self.settings.github_mode == "mock":
            return MockGitHub(self.settings.mock_github_dir).create_pr(
                repo=self.settings.github_repo or "local/mock",
                title=title,
                body=body,
                patch=patch,
                public_base_url=self.settings.public_base_url,
            )

        if not self.settings.github_token or not self.settings.github_repo:
            raise ValueError("PATCHIT_GITHUB_TOKEN and PATCHIT_GITHUB_REPO are required for github_mode=real")

        head_branch = f"patchit/{patch.patch_id[:8]}"
        client = GitHubRestClient(token=self.settings.github_token, repo=self.settings.github_repo)

        base_branch = self.settings.github_base_branch or client.get_repo_default_branch()
        base_sha = client.get_branch_head_sha(branch=base_branch)
        client.create_branch(new_branch=head_branch, from_sha=base_sha)

        # Materialize patch to file contents in a local sandbox so we can upload exact changes.
        repo_path = os.path.join(os.getcwd(), self.settings.repo_root)
        file_contents = _materialize_patch(repo_path=repo_path, diff_text=patch.diff_unified, files=[f.path for f in patch.files])

        commit_msg = f"PATCHIT: {patch.title} ({patch.patch_id[:8]})"
        for path, content in file_contents.items():
            # Determine if file exists on base; if yes include sha for update.
            sha = client.get_file_sha(path=path, ref=base_branch)
            client.upsert_file(path=path, content_text=content, branch=head_branch, message=commit_msg, known_sha=sha)

        return client.create_pull_request(title=title, body=body, head=head_branch, base=base_branch)


def _normalize_repo_path(p: str) -> str:
    p = (p or "").lstrip("/")
    for prefix in ("repo/", "./repo/"):
        if p.startswith(prefix):
            p = p[len(prefix) :]
    return p


def _materialize_patch(*, repo_path: str, diff_text: str, files: list[str]) -> Dict[str, str]:
    """
    Apply a unified diff to a temporary copy of the repo and return final content for touched files.
    This is used for `github_mode=real` so we can upload file contents via the GitHub Contents API.
    """
    if not diff_text.strip():
        raise ValueError("empty patch diff")

    norm_files = [_normalize_repo_path(f) for f in files]
    if not norm_files:
        # We still can apply diff, but we won't know what to upload; fail fast.
        raise ValueError("patch has no file list; cannot create real PR safely")

    with tempfile.TemporaryDirectory(prefix="patchit_pr_") as td:
        # IMPORTANT:
        # We apply diffs from the *workspace root* (td), with the repository located at td/repo.
        # This matches our verifier and supports diffs that reference paths like:
        #   - repo/airflow/...
        #   - ./repo/airflow/...
        # And (via symlinks below) also supports diffs that reference:
        #   - airflow/...
        #   - dbt/...
        dst_repo = os.path.join(td, "repo")

        def _ignore_runtime_artifacts(dirpath: str, names: list[str]) -> set[str]:
            """
            IMPORTANT:
            Our PATCHIT demo stacks mount runtime data inside the repo tree (e.g. `airflow/patchit_var`),
            and those runtime dirs can contain nested git worktrees that themselves contain `repo/...`,
            causing pathological `repo/repo/repo/...` recursion and ENOSPC during PR creation.

            PR creation only needs source code, so we aggressively ignore common runtime artifact dirs.
            """
            ignore: set[str] = set()
            for n in names:
                # Generic heavy / non-source directories
                if n in {
                    ".git",
                    ".venv",
                    "venv",
                    "__pycache__",
                    ".pytest_cache",
                    ".mypy_cache",
                    "node_modules",
                    ".ruff_cache",
                    ".cache",
                }:
                    ignore.add(n)
                    continue

                # Runtime directories commonly mounted in our stacks
                # - airflow/logs: can be huge
                # - airflow/pgdata: can be huge
                # - airflow/patchit_var: contains audit/reports/patches/worktrees (worktrees can include nested repos)
                if n in {"logs", "pgdata", "patchit_var", "worktrees"}:
                    ignore.add(n)
                    continue

            return ignore

        shutil.copytree(
            repo_path,
            dst_repo,
            dirs_exist_ok=True,
            ignore=_ignore_runtime_artifacts,
        )

        # Create top-level symlinks (td/<dir> -> td/repo/<dir>) so diffs that reference
        # non-prefixed repo paths (e.g., airflow/..., dbt/...) still apply cleanly.
        try:
            for name in os.listdir(dst_repo):
                if name.startswith("."):
                    continue
                src = os.path.join(dst_repo, name)
                link = os.path.join(td, name)
                if os.path.exists(link):
                    continue
                if os.path.isdir(src):
                    os.symlink(src, link)
        except Exception:
            # Best-effort; patch may still apply via repo/ prefixed paths.
            pass

        patch_path = os.path.join(td, "patch.diff")
        with open(patch_path, "w", encoding="utf-8") as f:
            f.write(diff_text)
            if not diff_text.endswith("\n"):
                f.write("\n")

        # Prefer git-apply.
        try:
            subprocess.run(["git", "init"], cwd=td, check=True, capture_output=True, text=True)
            subprocess.run(
                ["git", "apply", "--unsafe-paths", "--whitespace=nowarn", patch_path],
                cwd=td,
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception:
            # Fallback to patch
            subprocess.run(["patch", "-p1", "--batch", "--forward", "-i", patch_path], cwd=td, check=True, capture_output=True, text=True)

        out: Dict[str, str] = {}
        for rp in norm_files:
            abs_path = os.path.join(dst_repo, rp)
            if not os.path.exists(abs_path):
                # Deletions aren't supported in Phase-1 real mode.
                raise ValueError(f"real PR mode does not support deleting files (missing after patch): {rp}")
            with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
                out[rp] = f.read()
        return out



