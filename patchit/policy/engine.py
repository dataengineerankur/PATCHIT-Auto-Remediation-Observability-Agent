from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass

from patchit.models import PatchProposal, PolicyDecision, RemediationPolicy


@dataclass(frozen=True)
class PolicyEngine:
    policy: RemediationPolicy
    repo_root: str | None = None
    require_tracked_files: bool = False

    def _normalize_path(self, p: str) -> str:
        # Agents sometimes emit diffs rooted at "repo/..." (our container mount).
        # Normalize to repo-relative paths so allowlists work predictably.
        if p.startswith("repo/"):
            return p[len("repo/") :]
        if p.startswith("./repo/"):
            return p[len("./repo/") :]
        p = p.lstrip("./")
        # Canonicalize docker-mounted Airflow extra_dags paths to the real repo path.
        # In docker-compose we mount `airflow_extra_dags/` into Airflow at `airflow/dags/extra_dags/`.
        if p.startswith("airflow/dags/extra_dags/"):
            p = "airflow_extra_dags/" + p[len("airflow/dags/extra_dags/") :]
        return p

    def evaluate_patch(self, patch: PatchProposal) -> PolicyDecision:
        reasons: list[str] = []

        if len(patch.files) > self.policy.max_files_touched:
            reasons.append(f"touches too many files: {len(patch.files)} > {self.policy.max_files_touched}")

        if len(patch.diff_unified.encode("utf-8")) > self.policy.max_diff_bytes:
            reasons.append("diff too large")

        for fc in patch.files:
            p = self._normalize_path(fc.path)
            if any(p.startswith(prefix) for prefix in self.policy.deny_write_paths):
                reasons.append(f"denied path: {p}")
                continue
            if not any(p.startswith(prefix) for prefix in self.policy.allow_write_paths):
                reasons.append(f"not allowlisted: {p}")
                continue

            # If we intend to apply patches via git (worktree / PR), only allow tracked files.
            # Untracked files won't exist in worktrees and will cause `git apply` to fail with "No such file or directory".
            if self.require_tracked_files and self.repo_root:
                try:
                    repo = os.path.abspath(self.repo_root)
                    r = subprocess.run(
                        ["git", "-C", repo, "ls-files", "--error-unmatch", p],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    if r.returncode != 0:
                        reasons.append(f"not tracked in git (worktree cannot apply patch): {p}")
                except Exception:
                    # If git isn't available / repo_root isn't a git repo, don't hard-fail policy.
                    pass

        allowed = len(reasons) == 0
        return PolicyDecision(allowed=allowed, reasons=reasons)



