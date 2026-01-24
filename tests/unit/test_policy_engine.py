from __future__ import annotations

from patchit.models import PatchFileChange, PatchOperation, PatchProposal, RemediationPolicy
from patchit.policy.engine import PolicyEngine


def test_policy_denies_non_allowlisted_paths() -> None:
    policy = RemediationPolicy(allow_write_paths=["patchit/"], deny_write_paths=[".git/"])
    engine = PolicyEngine(policy)
    patch = PatchProposal(
        patch_id="p1",
        title="x",
        rationale="x",
        diff_unified="diff --git a/x b/x",
        files=[PatchFileChange(path="secrets/token.txt", operation=PatchOperation.update)],
        confidence=0.9,
        requires_human_approval=True,
    )
    d = engine.evaluate_patch(patch)
    assert d.allowed is False
    assert any("not allowlisted" in r or "denied" in r for r in d.reasons)



