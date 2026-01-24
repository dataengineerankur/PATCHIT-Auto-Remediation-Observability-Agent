from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from patchit.models import NormalizedErrorObject, PatchProposal, PolicyDecision, PullRequestResult, RootCauseReport, UniversalFailureEvent


@dataclass(frozen=True)
class EvidencePack:
    correlation_id: str
    payload: Dict[str, Any]
    path: str


def _safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def build_evidence_payload(
    *,
    correlation_id: str,
    event: UniversalFailureEvent,
    error: NormalizedErrorObject,
    policy: PolicyDecision,
    patch: PatchProposal | None,
    pr: PullRequestResult | None,
    report: RootCauseReport | None,
    agent_mode: str | None,
    agent_model: str | None,
    artifact_summaries: Any = None,
    code_context: Any = None,
    agent_pipeline: Any = None,
) -> Dict[str, Any]:
    rollback = None
    if pr and getattr(pr, "pr_url", None):
        rollback = {"type": "revert_pr", "instructions": f"Revert the PR: {pr.pr_url}"}
    elif patch and getattr(patch, "patch_id", None):
        rollback = {"type": "revert_patch", "instructions": f"Revert the patch by reverting commit(s) from patch_id={patch.patch_id}."}

    return {
        "schema": "patchit.evidence_pack.v1",
        "generated_at_unix": time.time(),
        "correlation_id": correlation_id,
        "event": event.model_dump(mode="json"),
        "error": error.model_dump(mode="json"),
        "agent": {"mode": agent_mode, "model": agent_model},
        "agent_pipeline": agent_pipeline,
        "policy": policy.model_dump(mode="json"),
        "patch": patch.model_dump(mode="json") if patch else None,
        "pr": pr.model_dump(mode="json") if pr else None,
        "report": report.model_dump(mode="json") if report else None,
        "artifacts": [a.model_dump(mode="json") for a in (artifact_summaries or [])] if artifact_summaries else [],
        "code_context": code_context or {},
        "rollback_plan": rollback,
    }


def persist_evidence_pack(
    *,
    store_dir: str,
    correlation_id: str,
    payload: Dict[str, Any],
) -> EvidencePack:
    _safe_mkdir(store_dir)
    path = os.path.join(store_dir, f"{correlation_id}.evidence.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=False)
    return EvidencePack(correlation_id=correlation_id, payload=payload, path=path)



