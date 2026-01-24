from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass

from patchit.models import PatchProposal, PullRequestResult


@dataclass(frozen=True)
class MockGitHub:
    """
    Phase-1 MVP PR creation in mock-mode (no network, no real git required).

    It writes:
      - PR metadata: .mock_github/prs/<n>.json
      - patch:       .mock_github/prs/<n>.diff
    """

    root_dir: str

    def create_pr(
        self,
        *,
        repo: str,
        title: str,
        body: str,
        patch: PatchProposal,
        public_base_url: str = "http://localhost:8088",
    ) -> PullRequestResult:
        os.makedirs(os.path.join(self.root_dir, "prs"), exist_ok=True)
        pr_number = int(time.time())
        branch = f"patchit/{patch.patch_id[:8]}"

        pr_dir = os.path.join(self.root_dir, "prs")
        meta_path = os.path.join(pr_dir, f"{pr_number}.json")
        diff_path = os.path.join(pr_dir, f"{pr_number}.diff")

        with open(diff_path, "w", encoding="utf-8") as f:
            f.write(patch.diff_unified)

        meta = {
            "pr_number": pr_number,
            "repo": repo,
            "title": title,
            "body": body,
            "branch": branch,
            "patch_id": patch.patch_id,
            "files": [fc.model_dump() for fc in patch.files],
            "diff_path": diff_path,
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

        pr_url = f"{public_base_url.rstrip('/')}/mock/pr/{pr_number}"
        return PullRequestResult(
            mode="mock",
            pr_number=pr_number,
            pr_title=title,
            pr_url=pr_url,
            branch_name=branch,
        )



