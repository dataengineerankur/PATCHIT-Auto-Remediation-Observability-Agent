from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional


@dataclass(frozen=True)
class RepoSpec:
    key: str
    pipeline_id_regex: str | None = None
    # Optional: match repos by stack trace / file paths (useful for blast-radius/shared-lib incidents
    # where pipeline_id alone is ambiguous).
    stack_path_regexes: list[str] | None = None
    repo_root: str = "repo"
    github_repo: str | None = None
    github_base_branch: str | None = None
    cursor_repository: str | None = None
    cursor_ref: str | None = None
    local_sync_repo_path: str | None = None
    code_translate_from: str | None = None
    code_translate_to: str | None = None
    file_translate_from: str | None = None
    file_translate_to: str | None = None

    def _matches_pipeline(self, *, pipeline_id: str | None) -> bool:
        if not pipeline_id or not self.pipeline_id_regex:
            return False
        try:
            return re.search(self.pipeline_id_regex, pipeline_id) is not None
        except re.error:
            return False

    def _matches_stack_paths(self, *, stack_paths: Iterable[str] | None) -> bool:
        pats = self.stack_path_regexes or []
        if not pats:
            return False
        if not stack_paths:
            return False
        for p in stack_paths:
            if not p:
                continue
            for pat in pats:
                try:
                    if re.search(pat, p) is not None:
                        return True
                except re.error:
                    continue
        return False

    def match_score(self, *, pipeline_id: str | None, stack_paths: Iterable[str] | None) -> int:
        """
        Higher is better. Prefer stack-path matches over pipeline_id matches because they
        point to the *actual code location* that failed.
        """
        score = 0
        if self._matches_pipeline(pipeline_id=pipeline_id):
            score = max(score, 10)
        if self._matches_stack_paths(stack_paths=stack_paths):
            score = max(score, 50)
        return score


@dataclass(frozen=True)
class RepoContext:
    key: str
    repo_root_dir: str
    github_repo: str | None
    github_base_branch: str | None
    cursor_repository: str | None
    cursor_ref: str | None
    local_sync_repo_path: str | None
    code_translate_from: str | None
    code_translate_to: str | None
    file_translate_from: str | None
    file_translate_to: str | None


def _abs_repo_root(repo_root: str) -> str:
    rr = repo_root or "repo"
    return rr if os.path.isabs(rr) else os.path.join(os.getcwd(), rr)


def parse_repo_registry(repo_registry_json: str | None) -> List[RepoSpec]:
    """
    Parse PATCHIT_REPO_REGISTRY_JSON (list of dicts). Invalid entries are ignored.
    """
    if not repo_registry_json:
        return []
    try:
        raw = json.loads(repo_registry_json)
    except Exception:
        return []
    if not isinstance(raw, list):
        return []

    out: List[RepoSpec] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        key = row.get("key")
        if not isinstance(key, str) or not key.strip():
            continue
        spr: list[str] | None = None
        if isinstance(row.get("stack_path_regexes"), str):
            spr = [row.get("stack_path_regexes")]  # type: ignore[list-item]
        elif isinstance(row.get("stack_path_regexes"), list):
            items: list[str] = []
            for it in row.get("stack_path_regexes") or []:
                if isinstance(it, str) and it.strip():
                    items.append(it.strip())
            spr = items or None
        out.append(
            RepoSpec(
                key=key.strip(),
                pipeline_id_regex=row.get("pipeline_id_regex") if isinstance(row.get("pipeline_id_regex"), str) else None,
                stack_path_regexes=spr,
                repo_root=row.get("repo_root") if isinstance(row.get("repo_root"), str) else "repo",
                github_repo=row.get("github_repo") if isinstance(row.get("github_repo"), str) else None,
                github_base_branch=row.get("github_base_branch") if isinstance(row.get("github_base_branch"), str) else None,
                cursor_repository=row.get("cursor_repository") if isinstance(row.get("cursor_repository"), str) else None,
                cursor_ref=row.get("cursor_ref") if isinstance(row.get("cursor_ref"), str) else None,
                local_sync_repo_path=row.get("local_sync_repo_path") if isinstance(row.get("local_sync_repo_path"), str) else None,
                code_translate_from=row.get("code_translate_from") if isinstance(row.get("code_translate_from"), str) else None,
                code_translate_to=row.get("code_translate_to") if isinstance(row.get("code_translate_to"), str) else None,
                file_translate_from=row.get("file_translate_from") if isinstance(row.get("file_translate_from"), str) else None,
                file_translate_to=row.get("file_translate_to") if isinstance(row.get("file_translate_to"), str) else None,
            )
        )
    return out


def select_repo_context(*, settings: Any, pipeline_id: str | None, error_stack_paths: Iterable[str] | None = None) -> RepoContext:
    """
    Select the best matching repo for a pipeline_id, else fall back to the default single-repo settings.
    """
    specs = parse_repo_registry(getattr(settings, "repo_registry_json", None))
    chosen: Optional[RepoSpec] = None
    best = -1
    for s in specs:
        score = s.match_score(pipeline_id=pipeline_id, stack_paths=error_stack_paths)
        if score > best:
            best = score
            chosen = s

    if chosen is None:
        repo_root = getattr(settings, "repo_root", "repo")
        return RepoContext(
            key="default",
            repo_root_dir=_abs_repo_root(repo_root),
            github_repo=getattr(settings, "github_repo", None),
            github_base_branch=getattr(settings, "github_base_branch", None),
            cursor_repository=getattr(settings, "cursor_repository", None),
            cursor_ref=getattr(settings, "cursor_ref", None),
            local_sync_repo_path=getattr(settings, "local_sync_repo_path", None),
            code_translate_from=getattr(settings, "code_translate_from", None),
            code_translate_to=getattr(settings, "code_translate_to", None),
            file_translate_from=getattr(settings, "file_translate_from", None),
            file_translate_to=getattr(settings, "file_translate_to", None),
        )

    return RepoContext(
        key=chosen.key,
        repo_root_dir=_abs_repo_root(chosen.repo_root),
        github_repo=chosen.github_repo or getattr(settings, "github_repo", None),
        github_base_branch=chosen.github_base_branch or getattr(settings, "github_base_branch", None),
        cursor_repository=chosen.cursor_repository or getattr(settings, "cursor_repository", None),
        cursor_ref=chosen.cursor_ref or getattr(settings, "cursor_ref", None),
        local_sync_repo_path=chosen.local_sync_repo_path or getattr(settings, "local_sync_repo_path", None),
        code_translate_from=chosen.code_translate_from or getattr(settings, "code_translate_from", None),
        code_translate_to=chosen.code_translate_to or getattr(settings, "code_translate_to", None),
        file_translate_from=chosen.file_translate_from or getattr(settings, "file_translate_from", None),
        file_translate_to=chosen.file_translate_to or getattr(settings, "file_translate_to", None),
    )


