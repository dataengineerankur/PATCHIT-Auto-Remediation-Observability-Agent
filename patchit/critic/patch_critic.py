from __future__ import annotations

import re
from dataclasses import dataclass

from patchit.models import NormalizedErrorObject, PatchProposal


@dataclass(frozen=True)
class PatchCriticResult:
    ok: bool
    patch_class: str
    reason: str | None = None
    feedback_for_agent: str | None = None


_DIFF_META_PREFIXES = (
    "diff --git ",
    "index ",
    "--- a/",
    "+++ b/",
    "@@ ",
    "\\ No newline at end of file",
)


def _iter_changed_lines(diff: str) -> tuple[list[str], list[str]]:
    added: list[str] = []
    removed: list[str] = []
    for ln in (diff or "").splitlines():
        if not ln:
            continue
        if ln.startswith(_DIFF_META_PREFIXES):
            continue
        if ln.startswith("+") and not ln.startswith("+++ "):
            added.append(ln[1:].rstrip())
        elif ln.startswith("-") and not ln.startswith("--- "):
            removed.append(ln[1:].rstrip())
    return (added, removed)


_LOG_ONLY_ALLOW = (
    re.compile(r"^\s*$"),
    re.compile(r"^\s*#"),
    re.compile(r'^\s*"""'),  # docstrings
    re.compile(r"^\s*import\s+logging\b"),
    re.compile(r"^\s*from\s+logging\s+import\b"),
    re.compile(r"^\s*logger\s*=\s*logging\.getLogger\("),
    re.compile(r"^\s*logg(er|ing)\."),
    re.compile(r"^\s*print\("),  # still "diagnostic-only"
)


def classify_patch(*, patch: PatchProposal) -> str:
    added, removed = _iter_changed_lines(patch.diff_unified or "")
    if not added and not removed:
        return "empty"
    # If no removals and all additions are logging/comments/docs, treat as log-only.
    if not removed:
        non_diag = []
        for ln in added:
            if any(rx.search(ln) for rx in _LOG_ONLY_ALLOW):
                continue
            non_diag.append(ln)
        if not non_diag:
            return "logging_only"
    return "behavior_change"


def should_reject_patch(*, error: NormalizedErrorObject, patch: PatchProposal) -> PatchCriticResult:
    """
    Hard guardrails to prevent "looks plausible but doesn't fix anything" PRs.
    For now, focus on the most damaging class: missing-artifact failures where
    the agent proposes only more logging at the consumer.
    """
    pclass = classify_patch(patch=patch)
    cls = (error.classification or "").lower()
    et = (error.error_type or "").lower()
    msg = (error.message or "").lower()
    touched_paths = [f.path for f in (patch.files or [])]

    def _is_dbt_like_path(p: str) -> bool:
        # Repo-agnostic heuristic: dbt models are usually under a `models/` subtree in a dbt project.
        # We avoid relying on project folder names like `dbt_retail/`.
        if not p:
            return False
        if p.endswith("dbt_project.yml"):
            return True
        if p.endswith(".sql") and ("/models/" in p or p.startswith("models/")):
            return True
        if p.endswith((".yml", ".yaml")) and ("/models/" in p or p.startswith("models/")):
            return True
        return False

    # DBT guardrail (repo-agnostic): avoid "fixing" missing-column failures by deleting scenario harness
    # or rewriting the model wholesale. We want targeted, evidence-based changes while preserving harness.
    if any(_is_dbt_like_path(p) for p in touched_paths):
        added, removed = _iter_changed_lines(patch.diff_unified or "")

        removed_join = "\n".join(removed).lower()
        added_join = "\n".join(added).lower()

        removed_scenario_harness = any(
            key in removed_join
            for key in (
                "scenario-driven model",
                "var('scenario'",
                "set sc = var(",
                "sc ==",
                "{% if sc",
                "{%- if sc",
            )
        )
        preserved_scenario_harness = any(
            key in added_join
            for key in (
                "scenario-driven model",
                "var('scenario'",
                "set sc = var(",
                "sc ==",
                "{% if sc",
                "{%- if sc",
            )
        )

        # If we removed the harness but didn't preserve it, reject.
        if removed_scenario_harness and not preserved_scenario_harness:
            return PatchCriticResult(
                ok=False,
                patch_class="dbt_harness_deleted",
                reason="dbt_scenario_harness_deleted",
                feedback_for_agent=(
                    "REJECTED: You removed a scenario-driven DBT test harness (scenario vars / conditional branches).\n"
                    "This repo uses deterministic scenarios to test PATCHIT. You MUST preserve the harness and apply the smallest targeted fix.\n"
                    "For missing-column errors, change only the referenced column (guided by the DB error + hint) and keep the scenario structure intact."
                ),
            )

        # If error class is dbt_missing_column, reject patches that change dbt `source()` targets (often unrelated and risky)
        # unless the error explicitly indicates a missing source/table.
        if "dbt_missing_column" in cls and "source" not in msg and "relation" not in msg:
            # crude but effective: detect a change to lines containing `source(`.
            removed_sources = {ln.strip() for ln in removed if "source(" in ln}
            added_sources = {ln.strip() for ln in added if "source(" in ln}
            if removed_sources and added_sources and removed_sources != added_sources:
                return PatchCriticResult(
                    ok=False,
                    patch_class="dbt_source_changed_for_missing_column",
                    reason="dbt_source_changed_for_missing_column",
                    feedback_for_agent=(
                        "REJECTED: The failure is a missing COLUMN, but you changed dbt `source()` targets.\n"
                        "Do not change sources/tables for missing-column errors. Fix the column reference instead (use the DB hint).\n"
                        "Keep changes minimal and localized."
                    ),
                )

    is_missing_artifact = (
        "file_not_found" in cls
        or "filenotfounderror" in et
        or "no such file or directory" in msg
        or "enriched.json" in msg
    )
    if is_missing_artifact and pclass == "logging_only":
        return PatchCriticResult(
            ok=False,
            patch_class=pclass,
            reason="logging_only_for_missing_artifact",
            feedback_for_agent=(
                "REJECTED: This patch only adds logging/comments and does not resolve the missing artifact.\n"
                "For missing-artifact failures, you MUST fix the producer/upstream task that should create the artifact, "
                "or implement an explicit branching/skip/quarantine path (do not silently change semantics unless converge_to_green).\n"
                "Use the DAG dependency graph + code search to identify the producer and make the artifact write atomic."
            ),
        )
    return PatchCriticResult(ok=True, patch_class=pclass, reason=None, feedback_for_agent=None)




