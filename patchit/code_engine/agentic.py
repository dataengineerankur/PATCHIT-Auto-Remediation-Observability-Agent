from __future__ import annotations

import difflib
import json
import os
import re
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from patchit.llm.cursor_cloud_agents_client import CursorCloudAgentsClient
from patchit.llm.groq_client import GroqClient
from patchit.llm.openrouter_client import OpenRouterClient
from patchit.models import NormalizedErrorObject, PatchFileChange, PatchOperation, PatchProposal, UniversalFailureEvent


_CURSOR_AGENT_LOCK = threading.Lock()


@dataclass(frozen=True)
class AgenticCodeEngine:
    """
    Always-on agentic fixer:
      - Calls external LLM agent service (Bytez) to propose a unified diff.
      - Converts to PatchProposal with minimal metadata.
    """

    mode: str  # openrouter|groq|cursor|bytez|inhouse
    agent_url: str | None = None  # bytez
    inhouse_agent_url: str | None = None
    inhouse_api_key: str | None = None
    inhouse_headers_json: str | None = None
    inhouse_timeout_s: float = 60.0
    groq_api_key: str | None = None  # groq
    groq_base_url: str = "https://api.groq.com/openai/v1"
    groq_model: str = "openai/gpt-oss-120b"
    openrouter_api_key: str | None = None  # openrouter
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    openrouter_model: str = "openai/gpt-5.2"
    openrouter_site_url: str | None = None
    openrouter_site_name: str | None = None
    repo_root: str = "repo"
    timeout_s: float = 60.0
    # Prefer JSON structured patches (PATCHIT synthesizes unified diff). Reduces malformed diff failures.
    patch_format: str = "structured"  # structured|diff
    # Two-pass is higher quality but doubles LLM calls; structured mode can be single-pass.
    use_two_pass: bool = False

    # Cursor Cloud Agents (optional)
    cursor_api_key: str | None = None
    cursor_base_url: str = "https://api.cursor.com"
    cursor_repository: str | None = None  # e.g., https://github.com/org/repo
    cursor_ref: str = "main"
    cursor_branch_prefix: str = "patchit/cursor"
    cursor_poll_interval_s: float = 3.0
    cursor_max_wait_s: float = 600.0
    cursor_auto_create_pr: bool = False
    # Used to fetch a full diff of the Cursor-created branch.
    github_token: str | None = None
    github_repo: str | None = None  # owner/name
    github_base_branch: str = "main"

    def propose_patch(
        self,
        *,
        event: UniversalFailureEvent,
        error: NormalizedErrorObject,
        code_context: Dict[str, Dict[str, object]],
        artifact_summaries: List[Dict[str, Any]],
        codebase_index: Dict[str, Any],
        verifier_feedback: str | None = None,
        repair_goal: str | None = None,
    ) -> PatchProposal | None:
        cursor_meta: Dict[str, Any] = {}
        payload = {
            "event": event.model_dump(mode="json"),
            "normalized_error": error.model_dump(mode="json"),
            "code_context": code_context,
            "artifact_summaries": artifact_summaries,
            "codebase_index": codebase_index,
            "verifier_feedback": verifier_feedback,
            "repair_goal": repair_goal or "remediate_failure",
        }

        if self.mode in ("groq", "openrouter"):
            if (self.patch_format or "structured") == "structured":
                # Single-call structured patch: avoid diff formatting failures and cut LLM calls in half.
                synthesized = self._synthesize_diff_from_structured_patch_primary(
                    event=event,
                    error=error,
                    code_context=code_context,
                    artifact_summaries=artifact_summaries,
                    codebase_index=codebase_index,
                    verifier_feedback=verifier_feedback,
                )
                if not synthesized:
                    return None
                diff = synthesized
            elif not self.use_two_pass:
                # Single-call diff prompt (faster, fewer calls)
                diff_prompt = _build_diff_prompt(payload, analysis_summary="(single-pass: skip analysis)")
                diff = self._chat(messages=[{"role": "user", "content": diff_prompt}])
            else:
            # Pass 1: ask for analysis + plan (forces codebase understanding before diff)
                analysis_prompt = _build_analysis_prompt(payload)
                analysis = self._chat(messages=[{"role": "user", "content": analysis_prompt}])

                # Pass 2: generate diff, with the analysis as additional grounding
                diff_prompt = _build_diff_prompt(payload, analysis_summary=analysis)
                diff = self._chat(messages=[{"role": "user", "content": diff_prompt}])
        elif self.mode == "bytez":
            if not self.agent_url:
                raise RuntimeError("bytez_agent_url_missing")
            with httpx.Client(timeout=self.timeout_s) as client:
                r = client.post(self.agent_url, json=payload)
                if r.status_code != 200:
                    raise RuntimeError(f"agent_http_{r.status_code}: {r.text[:1200]}")
                diff = (r.json() or {}).get("diff", "") or ""
        elif self.mode == "cursor":
            diff, cursor_meta = self._cursor_propose_diff(payload=payload)
        elif self.mode == "inhouse":
            diff = self._call_inhouse_agent(payload=payload)
        else:
            raise RuntimeError(f"unsupported_agent_mode: {self.mode}")

        diff = _sanitize_unified_diff(diff)
        diff = _canonicalize_diff_paths(diff)
        diff = _repair_new_file_diff_missing_hunks(diff)
        if not diff or "diff --git" not in diff and "--- a/" not in diff:
            return None

        # If verifier says the patch is corrupt / malformed, force structured patch synthesis
        # even if the diff *looks* plausible (git/patch are stricter than heuristics).
        force_structured = False
        if verifier_feedback:
            fb = verifier_feedback.lower()
            if "corrupt patch" in fb or "malformed patch" in fb or "patch fragment without header" in fb or "misordered hunks" in fb:
                force_structured = True

        if not force_structured and _has_basic_hunks(diff):
            # Accept basic hunks even if stricter heuristics fail.
            pass
        elif force_structured or (not _looks_like_real_unified_diff(diff)):
            # Fallback (task-agnostic): ask for a structured patch and synthesize a real unified diff ourselves.
            synthesized = self._synthesize_diff_from_structured_patch(
                event=event,
                error=error,
                code_context=code_context,
                artifact_summaries=artifact_summaries,
                codebase_index=codebase_index,
                verifier_feedback=verifier_feedback,
                invalid_diff=diff,
            )
            if synthesized:
                diff = synthesized
            else:
                # If we still have explicit hunks and file headers, accept the raw diff.
                if _has_basic_hunks(diff):
                    pass
                else:
                    raise RuntimeError(f"invalid_diff_format: missing_hunks\nhead={diff[:400]!r}\ntail={diff[-400:]!r}")

        files = _extract_files_from_diff(diff)
        if not files:
            # Safety: refuse to create PRs if we can't infer touched files.
            return None

        rationale = f"Generated by agent provider={self.mode} model={self._model_label()} using codebase index + stack trace + artifacts."
        if self.mode == "cursor":
            # Attach Cursor metadata so the service can optionally adopt Cursor-created PRs (when enabled).
            try:
                rationale += "\nCURSOR_META:" + json.dumps(cursor_meta, separators=(",", ":"))
            except Exception:
                pass

        return PatchProposal(
            patch_id=uuid.uuid4().hex,
            title=f"PATCHIT agentic fix: {error.error_type}",
            rationale=rationale,
            diff_unified=diff,
            files=files,
            tests_to_run=[],
            confidence=0.6,
            requires_human_approval=True,
        )

    def _model_label(self) -> str:
        if self.mode == "groq":
            return self.groq_model
        if self.mode == "openrouter":
            return self.openrouter_model
        if self.mode == "cursor":
            return "cursor_cloud_agent"
        if self.mode == "inhouse":
            return "inhouse_agent"
        return "bytez"

    def _chat(self, *, messages: List[Dict[str, str]], max_tokens: int = 2048) -> str:
        if self.mode == "groq":
            if not self.groq_api_key:
                raise RuntimeError("groq_api_key_missing")
            client = GroqClient(api_key=self.groq_api_key, base_url=self.groq_base_url, timeout_s=self.timeout_s)
            return client.chat(model=self.groq_model, messages=messages, max_tokens=max_tokens)
        if self.mode == "openrouter":
            if not self.openrouter_api_key:
                raise RuntimeError("openrouter_api_key_missing")
            client = OpenRouterClient(
                api_key=self.openrouter_api_key,
                base_url=self.openrouter_base_url,
                timeout_s=self.timeout_s,
                site_url=self.openrouter_site_url,
                site_name=self.openrouter_site_name,
            )
            try:
                return client.chat(model=self.openrouter_model, messages=messages, max_tokens=max_tokens)
            except Exception as e:  # noqa: BLE001
                # Agentic fallback (still LLM): OpenRouter -> Groq when OpenRouter fails (credits/quota).
                msg = str(e)
                if self.groq_api_key and ("openrouter_http_402" in msg or "more credits" in msg.lower()):
                    groq = GroqClient(api_key=self.groq_api_key, base_url=self.groq_base_url, timeout_s=self.timeout_s)
                    return groq.chat(model=self.groq_model, messages=messages, max_tokens=max_tokens)
                raise
        raise RuntimeError(f"unsupported_agent_mode_for_chat: {self.mode}")

    def _call_inhouse_agent(self, *, payload: Dict[str, Any]) -> str:
        if not self.inhouse_agent_url:
            raise RuntimeError("inhouse_agent_url_missing")
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.inhouse_headers_json:
            try:
                extra = json.loads(self.inhouse_headers_json) or {}
                if isinstance(extra, dict):
                    headers.update({str(k): str(v) for k, v in extra.items()})
            except json.JSONDecodeError:
                pass
        if self.inhouse_api_key and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {self.inhouse_api_key}"
        timeout_s = self.inhouse_timeout_s or self.timeout_s
        with httpx.Client(timeout=timeout_s) as client:
            r = client.post(self.inhouse_agent_url, json=payload, headers=headers)
            if r.status_code != 200:
                raise RuntimeError(f"inhouse_http_{r.status_code}: {r.text[:1200]}")
            body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        if not isinstance(body, dict):
            return ""
        diff_text = body.get("diff") or body.get("patch")
        if isinstance(diff_text, str) and diff_text.strip():
            return diff_text
        # Fallback for OpenAI-style completions/chat responses.
        choices = body.get("choices")
        if isinstance(choices, list):
            texts: list[str] = []
            for choice in choices:
                if not isinstance(choice, dict):
                    continue
                if isinstance(choice.get("text"), str):
                    texts.append(choice["text"])
                    continue
                message = choice.get("message")
                if isinstance(message, dict) and isinstance(message.get("content"), str):
                    texts.append(message["content"])
            merged = "\n".join(t.strip() for t in texts if isinstance(t, str) and t.strip())
            return merged
        return ""

    def _cursor_propose_diff(self, *, payload: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        """
        Launch a Cursor Cloud Agent that edits the repo on a branch, then fetch the resulting diff via GitHub compare.
        We still run PATCHIT's local sandbox verification on this diff before creating any PR.
        """
        if not self.cursor_api_key:
            raise RuntimeError("cursor_api_key_missing")
        if not self.github_token:
            raise RuntimeError("cursor_requires_github_token")
        if not self.github_repo:
            raise RuntimeError("cursor_requires_github_repo")

        repo_url = self.cursor_repository
        if not repo_url:
            # Derive from github_repo
            repo_url = f"https://github.com/{self.github_repo}"

        # Compose a deterministic branch name to avoid collisions.
        branch = f"{self.cursor_branch_prefix}/{uuid.uuid4().hex[:10]}"

        # Keep the Cursor prompt short but high-signal; Cursor will read the repo itself.
        evt = payload.get("event") or {}
        err = payload.get("normalized_error") or {}
        cc = payload.get("code_context") or {}
        goal = payload.get("repair_goal") or "remediate_failure"
        scenario = None
        try:
            scenario = ((evt.get("metadata") or {}).get("scenario")) if isinstance(evt, dict) else None
        except Exception:
            scenario = None

        prompt_text = "\n".join(
            [
                "You are PATCHIT Cursor Agent running inside Cursor Cloud Agents.",
                f"REPAIR GOAL: {goal}",
                "",
                "Task:",
                "- Fix the failure described below with minimal, safe changes.",
                "- Do NOT add any PATCHIT-specific code, hooks, or comments to DAGs/models.",
                "- Do NOT commit generated artifacts/caches (e.g. __pycache__/ , *.pyc, logs/, target/, pgdata/, .env).",
                "- Preserve dbt scenario/test harness logic (e.g., var('scenario') branches).",
                "- If this is a dbt missing-column error, correct the column reference (do not change the source() unless clearly required).",
                "- Run the smallest relevant checks/tests and keep the change minimal.",
                "",
                "CRITICAL CONTROL FLOW RULES (MUST FOLLOW):",
                "- If the failing code has `raise Exception(...)` for a scenario, you MUST REMOVE or REPLACE that raise statement.",
                "- Do NOT add a second `if` block for the same condition below the original - MODIFY the original block instead.",
                "- Code after `raise` or `return` is UNREACHABLE. Never add 'fix' code after these statements.",
                "- The fix must make the scenario PASS, not just add new code that can't execute.",
                "- If you see `if scenario == 'X': raise RuntimeError(...)`, REPLACE the raise with the actual fix logic.",
                "- WRONG: Adding a new `if scenario == 'X':` block after the existing one that raises.",
                "- CORRECT: Modifying the existing `if scenario == 'X':` block to remove the raise and add the fix.",
                "",
                f"Scenario (if provided): {scenario!r}",
                "",
                "Failure Event (JSON):",
                json.dumps(evt, indent=2)[:8000],
                "",
                "Normalized Error (JSON):",
                json.dumps(err, indent=2)[:10000],
                "",
                "Code Context excerpts (JSON):",
                json.dumps(cc, indent=2)[:10000],
            ]
        )

        # Cursor Cloud Agents APIs are rate limited; PATCHIT may process multiple failures concurrently (poller + users).
        # Serialize Cursor API usage to avoid 429 storms.
        with _CURSOR_AGENT_LOCK:
            client = CursorCloudAgentsClient(
                api_key=self.cursor_api_key, base_url=self.cursor_base_url, timeout_s=self.timeout_s
            )
            agent = client.launch_agent(
                repository=repo_url,
                ref=self.cursor_ref or self.github_base_branch,
                branch_name=branch,
                prompt_text=prompt_text,
                auto_create_pr=bool(self.cursor_auto_create_pr),
                open_as_cursor_github_app=True,
            )

            agent = client.wait_for_terminal(
                agent_id=agent.id,
                poll_interval_s=self.cursor_poll_interval_s,
                max_wait_s=self.cursor_max_wait_s,
            )
        st = (agent.status or "").upper()
        if st != "FINISHED":
            raise RuntimeError(f"cursor_agent_failed status={agent.status!r}")
        if not agent.target or not agent.target.branch_name:
            raise RuntimeError("cursor_agent_missing_branch")

        cursor_meta: Dict[str, Any] = {
            "agent_id": agent.id,
            "status": agent.status,
            "branch": (agent.target.branch_name if agent.target else None),
            "agent_url": (agent.target.url if agent.target else None),
            "pr_url": (agent.target.pr_url if agent.target else None),
            "auto_create_pr": bool(self.cursor_auto_create_pr),
        }

        # Fetch a full unified diff using GitHub compare (Accept: diff).
        base = self.github_base_branch or "main"
        head = agent.target.branch_name
        url = f"https://api.github.com/repos/{self.github_repo}/compare/{base}...{head}"
        headers = {
            "Authorization": f"token {self.github_token}",
            "Accept": "application/vnd.github.v3.diff",
            "User-Agent": "patchit",
        }
        with httpx.Client(timeout=self.timeout_s) as gh:
            r = gh.get(url, headers=headers)
            if r.status_code >= 400:
                raise RuntimeError(f"github_compare_http_{r.status_code}: {r.text[:1200]}")
            diff = r.text or ""
        return diff, cursor_meta

    def _synthesize_diff_from_structured_patch(
        self,
        *,
        event: UniversalFailureEvent,
        error: NormalizedErrorObject,
        code_context: Dict[str, Dict[str, object]],
        artifact_summaries: List[Dict[str, Any]],
        codebase_index: Dict[str, Any],
        verifier_feedback: str | None,
        invalid_diff: str,
    ) -> str | None:
        """
        If the model can't reliably format unified diff hunks, request a JSON patch:
          { "files": [ { "path": "...", "operation": "update", "new_content": "..." } ] }

        Then compute a correct unified diff via difflib so sandbox apply is stable.
        """
        if self.mode not in ("groq", "openrouter"):
            return None
        # Ensure configured for the chosen provider
        if self.mode == "groq" and not self.groq_api_key:
            return None
        if self.mode == "openrouter" and not self.openrouter_api_key:
            return None

        candidate_paths = _extract_paths_from_any_diff(invalid_diff)
        if not candidate_paths:
            # Fall back to stack/context paths if diff is too malformed to extract.
            candidate_paths = _infer_candidate_paths_from_context(code_context)

        files_payload: List[Dict[str, Any]] = []
        for p in candidate_paths[:4]:
            old = _read_repo_file(repo_root=self.repo_root, rel_path=p, max_bytes=80_000)
            if old is None:
                continue
            files_payload.append({"path": p, "old_content": old})

        prompt = _build_structured_patch_prompt(
            event=event,
            error=error,
            code_context=code_context,
            artifact_summaries=artifact_summaries,
            codebase_index=codebase_index,
            verifier_feedback=verifier_feedback,
            invalid_diff=invalid_diff,
            files=files_payload,
        )
        raw = self._chat(messages=[{"role": "user", "content": prompt}])
        raw = (raw or "").strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        try:
            data = json.loads(raw)
        except Exception:  # noqa: BLE001
            return None

        file_edits = data.get("files") if isinstance(data, dict) else None
        if not isinstance(file_edits, list):
            return None

        diffs: List[str] = []
        for fe in file_edits[:5]:
            if not isinstance(fe, dict):
                continue
            path = fe.get("path")
            op = fe.get("operation", "update")
            new_content = fe.get("new_content")
            if not isinstance(path, str) or not isinstance(new_content, str):
                continue
            if path.startswith("repo/"):
                path = path[len("repo/") :]
            path = _canonicalize_repo_path(path)
            old_content = _read_repo_file(repo_root=self.repo_root, rel_path=path, max_bytes=200_000)
            if old_content is None:
                # For MVP, skip non-existent files rather than guessing adds.
                continue
            if op != "update":
                continue
            diffs.append(_unified_diff_for_update(path=path, old=old_content, new=new_content))

        out = "\n".join([d for d in diffs if d.strip()]).strip()
        if out and not out.endswith("\n"):
            out += "\n"
        # Validate synthesized diff
        if out and _looks_like_real_unified_diff(out):
            return out
        return None

    def _synthesize_diff_from_structured_patch_primary(
        self,
        *,
        event: UniversalFailureEvent,
        error: NormalizedErrorObject,
        code_context: Dict[str, Dict[str, object]],
        artifact_summaries: List[Dict[str, Any]],
        codebase_index: Dict[str, Any],
        verifier_feedback: str | None,
    ) -> str | None:
        """
        Primary (fast) structured patch mode:
          - Ask for JSON file updates (full new_content)
          - Synthesize a valid unified diff via difflib
        """
        if self.mode not in ("groq", "openrouter"):
            return None
        if self.mode == "groq" and not self.groq_api_key:
            return None
        if self.mode == "openrouter" and not self.openrouter_api_key:
            return None

        candidate_paths = _infer_candidate_paths_from_context(code_context)
        # Keep the prompt small enough for providers with strict context windows (Groq).
        max_files = 2 if self.mode == "groq" else 4
        max_bytes = 60_000 if self.mode == "groq" else 120_000
        files_payload: List[Dict[str, Any]] = []
        for p in candidate_paths[:max_files]:
            old = _read_repo_file(repo_root=self.repo_root, rel_path=p, max_bytes=max_bytes)
            if old is None:
                continue
            files_payload.append({"path": p, "old_content": old})
        if not files_payload:
            return None

        prompt = _build_structured_patch_primary_prompt(
            event=event,
            error=error,
            code_context=code_context,
            artifact_summaries=artifact_summaries,
            codebase_index=codebase_index,
            verifier_feedback=verifier_feedback,
            files=files_payload,
        )
        raw = self._chat(messages=[{"role": "user", "content": prompt}])
        raw = (raw or "").strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        try:
            data = json.loads(raw)
        except Exception:  # noqa: BLE001
            return None

        file_edits = data.get("files") if isinstance(data, dict) else None
        if not isinstance(file_edits, list):
            return None

        diffs: List[str] = []
        for fe in file_edits[:5]:
            if not isinstance(fe, dict):
                continue
            path = fe.get("path")
            op = fe.get("operation", "update")
            new_content = fe.get("new_content")
            if not isinstance(path, str) or not isinstance(new_content, str):
                continue
            if path.startswith("repo/"):
                path = path[len("repo/") :]
            path = _canonicalize_repo_path(path)
            old_content = _read_repo_file(repo_root=self.repo_root, rel_path=path, max_bytes=250_000)
            if old_content is None or op != "update":
                continue
            diffs.append(_unified_diff_for_update(path=path, old=old_content, new=new_content))

        out = "\n".join([d for d in diffs if d.strip()]).strip()
        if out and not out.endswith("\n"):
            out += "\n"
        if out and _looks_like_real_unified_diff(out):
            return out
        return None


def _sanitize_unified_diff(raw: str) -> str:
    """
    LLMs sometimes wrap diffs in markdown fences or omit a trailing newline.
    Both can break patch application. Normalize here.
    """
    if raw is None:
        return ""
    s = raw.strip()
    if not s:
        return ""

    # Remove markdown fences (and other non-diff sentinel lines) if present.
    lines = []
    for ln in s.splitlines():
        if ln.strip().startswith("```"):
            continue
        if ln.startswith("*** End"):
            continue
        lines.append(ln.rstrip("\r"))
    s = "\n".join(lines).strip()

    # Ensure the diff starts at the first recognizable diff header.
    for marker in ("diff --git ", "--- a/"):
        idx = s.find(marker)
        if idx != -1:
            s = s[idx:].lstrip()
            break

    # Drop diff blocks that are almost always noise / non-reproducible (generated artifacts, binaries, caches).
    s = _filter_unified_diff_file_blocks(s)

    # Ensure final newline so patch/git apply don't complain.
    if s and not s.endswith("\n"):
        s += "\n"
    return s


_UNWANTED_DIFF_PATH_RE = re.compile(
    r"(^|/)(__pycache__|\.pytest_cache|\.mypy_cache|\.ruff_cache)(/|$)|"
    r"\.(pyc|pyo|pyd)$|"
    r"(^|/)([^/]+\.(egg-info|dist-info))(/|$)|"
    r"(^|/)(logs|pgdata|var|target)(/|$)|"
    r"(^|/)\.env(\.|$)|"
    r"(^|/)\.gitignore$",
    flags=re.IGNORECASE,
)


def _filter_unified_diff_file_blocks(diff: str) -> str:
    """
    Remove whole-file diff blocks for:
    - binaries (GitHub compare can include "Binary files ... differ")
    - generated caches/artifacts (e.g. __pycache__/ *.pyc)

    Cursor Cloud Agents can accidentally include these in commits; keeping them causes invalid diff errors
    and/or unreviewable PR noise.
    """
    if not diff or "diff --git " not in diff:
        return diff

    out_blocks: List[str] = []
    cur: List[str] = []

    def _flush(block: List[str]) -> None:
        if not block:
            return
        # Identify file path from "diff --git a/... b/..."
        header = block[0] if block else ""
        m = re.match(r"diff --git a/(.+?) b/(.+)$", header.strip())
        b_path = m.group(2) if m else ""
        # Skip binary patches and unwanted/generated files
        if any("GIT binary patch" in ln for ln in block) or any(ln.startswith("Binary files ") for ln in block):
            return
        if b_path and _UNWANTED_DIFF_PATH_RE.search(b_path):
            return
        out_blocks.append("\n".join(block))

    for ln in diff.splitlines():
        if ln.startswith("diff --git "):
            _flush(cur)
            cur = [ln]
        else:
            cur.append(ln)
    _flush(cur)

    return ("\n".join([b for b in out_blocks if b.strip()]).strip() + "\n") if out_blocks else ""


def _canonicalize_diff_paths(diff: str) -> str:
    """
    Canonicalize known path aliases inside the diff text itself.
    This prevents sandbox `git apply` failures when a diff references container-mounted paths.
    """
    if not diff:
        return diff
    # Airflow mounts repo/airflow_extra_dags at repo/airflow/dags/extra_dags
    repls = [
        ("a/repo/airflow/dags/extra_dags/", "a/repo/airflow_extra_dags/"),
        ("b/repo/airflow/dags/extra_dags/", "b/repo/airflow_extra_dags/"),
        ("--- a/repo/airflow/dags/extra_dags/", "--- a/repo/airflow_extra_dags/"),
        ("+++ b/repo/airflow/dags/extra_dags/", "+++ b/repo/airflow_extra_dags/"),
        ("diff --git a/repo/airflow/dags/extra_dags/", "diff --git a/repo/airflow_extra_dags/"),
        (" b/repo/airflow/dags/extra_dags/", " b/repo/airflow_extra_dags/"),
    ]
    out = diff
    for a, b in repls:
        out = out.replace(a, b)
    return out


def _has_basic_hunks(diff: str) -> bool:
    if not diff:
        return False
    has_hunk = "@@ " in diff
    has_new_file_header = ("new file mode" in diff) and ("--- /dev/null" in diff) and ("+++ b/" in diff)
    has_regular_header = ("--- a/" in diff) and ("+++ b/" in diff)
    return has_hunk and (has_regular_header or has_new_file_header)


def _repair_new_file_diff_missing_hunks(diff: str) -> str:
    """
    Some providers return "new file mode" blocks without @@ hunks.
    Convert those into valid unified diff hunks so git/patch can apply them.
    """
    if not diff or "diff --git " not in diff:
        return diff

    blocks: List[str] = []
    cur: List[str] = []

    def _flush(block: List[str]) -> None:
        if not block:
            return
        has_new_file = any("new file mode" in ln for ln in block)
        has_dev_null = any(ln.startswith("--- /dev/null") for ln in block)
        has_b_header = any(ln.startswith("+++ b/") for ln in block)
        has_hunk = any(_is_valid_unified_hunk_header(ln) for ln in block)
        if has_new_file and has_dev_null and has_b_header and not has_hunk:
            header: List[str] = []
            content: List[str] = []
            seen_b = False
            for ln in block:
                header.append(ln)
                if ln.startswith("+++ b/"):
                    seen_b = True
                    continue
                if seen_b:
                    # Remaining lines are file content; normalize to added lines.
                    if ln.startswith("+"):
                        content.append(ln)
                    else:
                        content.append("+" + ln)
            if content:
                hunk = [f"@@ -0,0 +1,{len(content)} @@"]
                block = header + hunk + content
        blocks.append("\n".join(block))

    for ln in diff.splitlines():
        if ln.startswith("diff --git "):
            _flush(cur)
            cur = [ln]
        else:
            cur.append(ln)
    _flush(cur)

    out = "\n".join([b for b in blocks if b.strip()]).strip()
    if out and not out.endswith("\n"):
        out += "\n"
    return out


def _looks_like_real_unified_diff(diff: str) -> bool:
    """
    Require actual file headers and at least one hunk; otherwise tools like `git apply` will reject it.
    """
    lines = diff.splitlines()
    has_regular_headers = any(l.startswith("--- a/") for l in lines) and any(l.startswith("+++ b/") for l in lines)
    has_new_file_headers = ("new file mode" in diff) and any(l.startswith("--- /dev/null") for l in lines) and any(
        l.startswith("+++ b/") for l in lines
    )
    has_file_headers = has_regular_headers or has_new_file_headers

    # Unified diff hunks must look like: @@ -l,s +l,s @@
    has_valid_hunk = any(_is_valid_unified_hunk_header(l) for l in lines)

    # A minimal diff must include at least one real added/removed line (excluding headers).
    has_real_change = any(
        (l.startswith("+") and not l.startswith("+++ "))
        or (l.startswith("-") and not l.startswith("--- "))
        for l in lines
    )

    # Reject diffs that contain multiple file header blocks for the same file; these often
    # confuse `patch` and are a common LLM failure mode.
    header_paths: Dict[str, int] = {}
    for l in lines:
        if l.startswith("--- a/"):
            p = l[len("--- a/") :].strip()
            header_paths[p] = header_paths.get(p, 0) + 1
    has_duplicate_file_blocks = any(v > 1 for v in header_paths.values())

    return has_file_headers and has_valid_hunk and has_real_change and (not has_duplicate_file_blocks)


def _is_valid_unified_hunk_header(line: str) -> bool:
    # Example: @@ -12,7 +12,9 @@ optional context
    if not line.startswith("@@ "):
        return False
    if " @@" not in line:
        return False
    try:
        header = line.split(" @@")[0]  # "@@ -12,7 +12,9"
        parts = header.split()
        if len(parts) < 3:
            return False
        old_part = parts[1]  # "-12,7"
        new_part = parts[2]  # "+12,9"
        if not old_part.startswith("-") or not new_part.startswith("+"):
            return False
        # Validate digits and optional ",digits"
        for p in (old_part[1:], new_part[1:]):
            if "," in p:
                a, b = p.split(",", 1)
                if not a.isdigit() or not b.isdigit():
                    return False
            else:
                if not p.isdigit():
                    return False
        return True
    except Exception:  # noqa: BLE001
        return False


def _extract_files_from_diff(diff: str) -> List[PatchFileChange]:
    files: List[PatchFileChange] = []
    for ln in diff.splitlines():
        if ln.startswith("diff --git "):
            parts = ln.split()
            if len(parts) >= 4:
                b = parts[3]
                if b.startswith("b/"):
                    path = b[2:]
                    if path.startswith("repo/"):
                        path = path[len("repo/") :]
                    path = _canonicalize_repo_path(path)
                    files.append(PatchFileChange(path=path, operation=PatchOperation.update))
        if ln.startswith("+++ b/"):
            path = ln[len("+++ b/") :].strip()
            if path and path != "/dev/null":
                if path.startswith("repo/"):
                    path = path[len("repo/") :]
                path = _canonicalize_repo_path(path)
                files.append(PatchFileChange(path=path, operation=PatchOperation.update))
    # De-dupe
    seen = set()
    out: List[PatchFileChange] = []
    for f in files:
        if f.path not in seen:
            seen.add(f.path)
            out.append(f)
    return out


def _unified_diff_for_update(*, path: str, old: str, new: str) -> str:
    old_lines = old.splitlines(True)
    new_lines = new.splitlines(True)
    return "".join(
        difflib.unified_diff(
            old_lines,
            new_lines,
            fromfile=f"a/repo/{path}",
            tofile=f"b/repo/{path}",
            n=3,
        )
    )


def _read_repo_file(*, repo_root: str, rel_path: str, max_bytes: int) -> str | None:
    if os.path.isabs(rel_path):
        return None
    p = rel_path
    if p.startswith("repo/"):
        p = p[len("repo/") :]
    p = _canonicalize_repo_path(p)
    repo_abs = repo_root if os.path.isabs(repo_root) else os.path.join(os.getcwd(), repo_root)
    abs_path = os.path.join(repo_abs, p)
    if not os.path.exists(abs_path) or not os.path.isfile(abs_path):
        return None
    try:
        if os.path.getsize(abs_path) > max_bytes:
            return None
        with open(abs_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:  # noqa: BLE001
        return None


def _extract_paths_from_any_diff(text: str) -> List[str]:
    paths: List[str] = []
    for ln in (text or "").splitlines():
        if ln.startswith("+++ b/"):
            p = ln[len("+++ b/") :].strip()
            if p and p != "/dev/null":
                if p.startswith("repo/"):
                    p = p[len("repo/") :]
                paths.append(_canonicalize_repo_path(p))
        if ln.startswith("--- a/"):
            p = ln[len("--- a/") :].strip()
            if p and p != "/dev/null":
                if p.startswith("repo/"):
                    p = p[len("repo/") :]
                paths.append(_canonicalize_repo_path(p))
    # De-dupe keep order
    seen = set()
    out: List[str] = []
    for p in paths:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _infer_candidate_paths_from_context(code_context: Dict[str, Dict[str, object]]) -> List[str]:
    # code_context keys are like "repo/airflow/..." already
    out: List[str] = []
    for k in code_context.keys():
        p = k
        if p.startswith("repo/"):
            p = p[len("repo/") :]
        out.append(_canonicalize_repo_path(p))
    return out


def _canonicalize_repo_path(p: str) -> str:
    """
    Canonicalize docker-mounted aliases to the real repo layout.
    In docker-compose we mount `airflow_extra_dags/` into Airflow at `airflow/dags/extra_dags/`.
    """
    if p.startswith("airflow/dags/extra_dags/"):
        return "airflow_extra_dags/" + p[len("airflow/dags/extra_dags/") :]
    return p


def _build_structured_patch_prompt(
    *,
    event: UniversalFailureEvent,
    error: NormalizedErrorObject,
    code_context: Dict[str, Dict[str, object]],
    artifact_summaries: List[Dict[str, Any]],
    codebase_index: Dict[str, Any],
    verifier_feedback: str | None,
    invalid_diff: str,
    files: List[Dict[str, Any]],
) -> str:
    goal = (codebase_index or {}).get("repair_goal") or "remediate_failure"
    goal_line = (
        "REPAIR GOAL: converge_to_green — make the pipeline/job succeed; it is OK to change failure semantics to avoid exceptions, but keep changes minimal and safe."
        if goal == "converge_to_green"
        else "REPAIR GOAL: remediate_failure — fix root cause while preserving failure semantics (do not silently convert failures into success)."
    )
    return "\n".join(
        [
            "You are PATCHIT Fix Agent.",
            goal_line,
            "The previous attempt produced an INVALID unified diff (bad @@ hunks).",
            "Instead, output ONLY valid JSON (no markdown) in this schema:",
            '{ "files": [ { "path": "relative/path.ext", "operation": "update", "new_content": "FULL NEW FILE CONTENT" } ] }',
            "",
            "HARD RULES:",
            "- Output ONLY JSON. No prose. No code fences.",
            "- Keep changes minimal and safe.",
            "- If REPAIR GOAL is remediate_failure: Do NOT change failure semantics (do not turn failures into success silently).",
            "- If REPAIR GOAL is converge_to_green: Prefer making the failing code path succeed with safe fallbacks; avoid broad refactors.",
            "- Only include files that actually need modification.",
            "",
            "CRITICAL CONTROL FLOW RULES:",
            "- If the failing code has `raise Exception(...)` for a scenario, REMOVE or REPLACE that raise statement.",
            "- Do NOT add a second `if` block for the same condition - MODIFY the original block.",
            "- Code after `raise` or `return` is UNREACHABLE. Never add fix code after these statements.",
            "- WRONG: Adding `if scenario == 'X': fix_code` after `if scenario == 'X': raise ...`",
            "- CORRECT: Replace the raise with the fix inside the SAME block.",
            "",
            "FAILURE EVENT (JSON):",
            _json_for_prompt(event.model_dump(mode="json"), max_chars=8_000),
            "",
            "NORMALIZED ERROR (JSON):",
            _json_for_prompt(error.model_dump(mode="json"), max_chars=10_000),
            "",
            "ARTIFACT SUMMARIES (JSON):",
            _json_for_prompt(artifact_summaries, max_chars=6_000),
            "",
            "CODE CONTEXT (JSON):",
            _json_for_prompt(code_context, max_chars=10_000),
            "",
            "CODEBASE INDEX (JSON):",
            _compact_codebase_index_for_prompt(codebase_index, max_chars=8_000),
            "",
            "VERIFIER FEEDBACK (if any):",
            verifier_feedback or "null",
            "",
            "INVALID DIFF (for reference; do NOT repeat it):",
            invalid_diff[:2000],
            "",
            "CURRENT FILE CONTENTS (authoritative):",
            json.dumps(files, indent=2),
        ]
    )


def _build_structured_patch_primary_prompt(
    *,
    event: UniversalFailureEvent,
    error: NormalizedErrorObject,
    code_context: Dict[str, Dict[str, object]],
    artifact_summaries: List[Dict[str, Any]],
    codebase_index: Dict[str, Any],
    verifier_feedback: str | None,
    files: List[Dict[str, Any]],
) -> str:
    goal = (codebase_index or {}).get("repair_goal") or "remediate_failure"
    goal_line = (
        "REPAIR GOAL: converge_to_green — make the pipeline/job succeed; it is OK to change failure semantics to avoid exceptions, but keep changes minimal and safe."
        if goal == "converge_to_green"
        else "REPAIR GOAL: remediate_failure — fix root cause while preserving failure semantics (do not silently convert failures into success)."
    )
    return "\n".join(
        [
            "You are PATCHIT Fix Agent.",
            goal_line,
            "Output ONLY valid JSON (no markdown) in this schema:",
            '{ "files": [ { "path": "relative/path.ext", "operation": "update", "new_content": "FULL NEW FILE CONTENT" } ] }',
            "",
            "HARD RULES:",
            "- Output ONLY JSON. No prose. No code fences.",
            "- Keep changes minimal and safe.",
            "- Only include files that actually need modification.",
            "- Use CURRENT FILE CONTENTS as authoritative source of truth (do not hallucinate).",
            "- Ensure the updated file content is complete and syntactically valid.",
            "- If editing DBT models: preserve any scenario/test harness logic (e.g., var('scenario') branches).",
            "- For dbt_missing_column errors: do NOT rewrite the whole model and do NOT change `source()` targets unless the error explicitly indicates the source/table is wrong. Prefer the smallest fix guided by the DB error hint.",
            "",
            "CRITICAL CONTROL FLOW RULES:",
            "- If the failing code has `raise Exception(...)` for a scenario, REMOVE or REPLACE that raise statement.",
            "- Do NOT add a second `if` block for the same condition - MODIFY the original block.",
            "- Code after `raise` or `return` is UNREACHABLE. Never add fix code after these statements.",
            "- WRONG: Adding `if scenario == 'X': fix_code` after `if scenario == 'X': raise ...`",
            "- CORRECT: Replace the raise with the fix inside the SAME block.",
            "",
            "FAILURE EVENT (JSON):",
            _json_for_prompt(event.model_dump(mode="json"), max_chars=8_000),
            "",
            "NORMALIZED ERROR (JSON):",
            _json_for_prompt(error.model_dump(mode="json"), max_chars=10_000),
            "",
            "ARTIFACT SUMMARIES (JSON):",
            _json_for_prompt(artifact_summaries, max_chars=6_000),
            "",
            "CODE CONTEXT (JSON):",
            _json_for_prompt(code_context, max_chars=10_000),
            "",
            "CODEBASE INDEX (JSON):",
            _compact_codebase_index_for_prompt(codebase_index, max_chars=8_000),
            "",
            "VERIFIER FEEDBACK (if any):",
            verifier_feedback or "null",
            "",
            "CURRENT FILE CONTENTS (authoritative):",
            _json_for_prompt(files, max_chars=18_000),
        ]
    )


def _json_for_prompt(obj: Any, *, max_chars: int) -> str:
    """
    Render JSON for prompts with a hard size cap. If the JSON is too large, include a
    best-effort summary so providers with small context windows won't reject the request.
    """
    try:
        s = json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:  # noqa: BLE001
        s = repr(obj)
    if len(s) <= max_chars:
        return s
    # Keep head+tail to preserve key context while capping size.
    keep = max(2000, max_chars // 2)
    return s[:keep] + "\n...<TRUNCATED_FOR_CONTEXT_WINDOW>...\n" + s[-keep:]


def _compact_codebase_index_for_prompt(codebase_index: Dict[str, Any], *, max_chars: int) -> str:
    """
    CODEBASE INDEX can be enormous (hundreds/thousands of files). Emit a compact subset:
      - file count
      - DAG ids + task ids (trimmed)
      - triggers (trimmed)
      - python file list (trimmed)
    """
    if not isinstance(codebase_index, dict):
        return _json_for_prompt(codebase_index, max_chars=max_chars)

    dags = codebase_index.get("airflow_dags") or []
    triggers = codebase_index.get("airflow_dag_triggers") or []
    py = codebase_index.get("python_files") or []

    compact: Dict[str, Any] = {
        "file_count_indexed": codebase_index.get("file_count_indexed"),
        "airflow_dags": dags[:50],
        "airflow_dag_triggers": triggers[:50],
        "python_files": [],
    }
    # Keep only lightweight python file metadata to avoid exploding the prompt.
    for row in py[:120]:
        if isinstance(row, dict):
            compact["python_files"].append({"path": row.get("path"), "imports": (row.get("imports") or [])[:20]})
        elif isinstance(row, str):
            compact["python_files"].append(row)

    rendered = _json_for_prompt(compact, max_chars=max_chars)
    if len(rendered) <= max_chars:
        return rendered

    # Final fallback: show only keys + a small file-path sample.
    paths: List[str] = []
    for row in py[:200]:
        if isinstance(row, dict) and isinstance(row.get("path"), str):
            paths.append(row["path"])
        elif isinstance(row, str):
            paths.append(row)
        if len(paths) >= 80:
            break
    minimal = {
        "file_count_indexed": codebase_index.get("file_count_indexed"),
        "keys": sorted(list(codebase_index.keys()))[:50],
        "python_file_paths_sample": paths,
    }
    return _json_for_prompt(minimal, max_chars=max_chars)


def _build_prompt(payload: Dict[str, Any]) -> str:
    """
    Shared prompt for Groq/OpenAI-compatible and Bytez paths.
    """
    event = payload.get("event", {})
    normalized_error = payload.get("normalized_error", {})
    code_context = payload.get("code_context", {})
    artifact_summaries = payload.get("artifact_summaries", [])
    codebase_index = payload.get("codebase_index", {})

    return "\n".join(
        [
            "You are PATCHIT Fix Agent. Generate a minimal SAFE patch as a unified diff.",
            "",
            "HARD RULES:",
            "- Output ONLY a unified diff (git apply compatible). No prose outside the diff.",
            "- Touch the fewest files possible. Prefer targeted guardrails (validation, try/except, retries).",
            "- Do NOT modify: .git/, venv/, .venv/, terraform/, secrets/, config.env",
            "- Assume human review; do not introduce risky broad refactors.",
            "",
            "CRITICAL CONTROL FLOW RULES:",
            "- If the failing code has `raise Exception(...)` for a scenario, REMOVE or REPLACE that raise.",
            "- Do NOT add a second `if` block for the same condition - MODIFY the original block.",
            "- Code after `raise` or `return` is UNREACHABLE. Never add fix code after these.",
            "",
            "FAILURE EVENT (JSON):",
            __import__("json").dumps(event, indent=2),
            "",
            "NORMALIZED ERROR (JSON):",
            __import__("json").dumps(normalized_error, indent=2),
            "",
            "ARTIFACT SUMMARIES (JSON):",
            __import__("json").dumps(artifact_summaries, indent=2),
            "",
            "CODE CONTEXT SNIPPETS (JSON):",
            __import__("json").dumps(code_context, indent=2),
            "",
            "CODEBASE INDEX (JSON):",
            __import__("json").dumps(codebase_index, indent=2),
            "",
            "TASK:",
            "- Propose a patch to fix the likely root cause.",
            "- If the failure is in a DAG/task code file, patch that file. If it is in a shared lib, patch shared lib.",
            "- Ensure the patch compiles and won’t break other scenarios.",
            "",
            "OUTPUT:",
            "- Unified diff only.",
        ]
    )


def _build_analysis_prompt(payload: Dict[str, Any]) -> str:
    """
    Pass 1: build a real understanding + pinpoint the correct fix location and why.
    """
    goal = payload.get("repair_goal") or "remediate_failure"
    goal_lines = (
        [
            "REPAIR GOAL: converge_to_green",
            "- Make the pipeline/job succeed end-to-end (green).",
            "- It is OK to change failure semantics (avoid raising) if that is the safest minimal path.",
            "- If pipeline is patchit_chaos_dag: treat chaos helpers as REQUIRED to succeed; modify chaos step helpers (especially read_json) to return safe defaults instead of throwing.",
        ]
        if goal == "converge_to_green"
        else [
            "REPAIR GOAL: remediate_failure",
            "- Fix root cause while preserving failure semantics (do not silently convert failures into success).",
        ]
    )

    event = payload.get("event", {})
    normalized_error = payload.get("normalized_error", {})
    code_context = payload.get("code_context", {})
    artifact_summaries = payload.get("artifact_summaries", [])
    codebase_index = payload.get("codebase_index", {})
    verifier_feedback = payload.get("verifier_feedback")

    return "\n".join(
        [
            "You are PATCHIT Fix Agent. First, deeply understand the codebase and failure chain.",
            "",
            *goal_lines,
            "",
            "UPSTREAM AWARENESS REQUIREMENT (do this explicitly):",
            "- Use codebase_index.airflow_dags (task edges) AND codebase_index.airflow_dag_triggers (TriggerDagRunOperator) to trace upstream sources.",
            "- If the failure is a missing artifact/FileNotFoundError, identify:",
            "  (a) the expected producer task,",
            "  (b) the upstream DAG that should have produced it (if cross-DAG), and",
            "  (c) whether run_id/trigger conf propagation is likely broken (look at run_id_value_expr).",
            "",
            "IMPORTANT SPECIAL CASES:",
            "- Missing artifact / FileNotFoundError: do NOT propose 'logging-only' patches. Those are diagnostics, not remediation.",
            "  Instead, fix the upstream producer that should write the artifact (and make writes atomic), or implement explicit branching/skip/quarantine logic.",
            "",
            "Return a concise report with these sections:",
            "1) Root cause (what exactly failed and why)",
            "2) Upstream vs downstream (which component should be fixed and why)",
            "3) Exact fix location (file + function + line region)",
            "4) Minimal safe change plan (bullets)",
            "5) Safety checks (what to avoid changing)",
            "",
            "CRITICAL RULE:",
            "- If REPAIR GOAL is remediate_failure: Do NOT change failure semantics (do not turn failures into success silently).",
            "- If REPAIR GOAL is converge_to_green: prioritize making the failing code path succeed with safe fallbacks.",
            "- For data contract failures, prefer fixing upstream writer/atomicity or explicit quarantine branching.",
            "",
            "FAILURE EVENT (JSON):",
            __import__("json").dumps(event, indent=2),
            "",
            "NORMALIZED ERROR (JSON):",
            __import__("json").dumps(normalized_error, indent=2),
            "",
            "ARTIFACT SUMMARIES (JSON):",
            __import__("json").dumps(artifact_summaries, indent=2),
            "",
            "CODE CONTEXT SNIPPETS (JSON):",
            __import__("json").dumps(code_context, indent=2),
            "",
            "CODEBASE INDEX (JSON):",
            __import__("json").dumps(codebase_index, indent=2),
            "",
            "SANDBOX VERIFIER FEEDBACK (if present; use this to iterate):",
            (verifier_feedback or "null"),
        ]
    )


def _build_diff_prompt(payload: Dict[str, Any], *, analysis_summary: str) -> str:
    """
    Pass 2: generate diff, grounded by the pass-1 analysis.
    """
    goal = payload.get("repair_goal") or "remediate_failure"
    goal_rules = (
        [
            "REPAIR GOAL: converge_to_green — make the pipeline/job succeed (green).",
            "- It is OK to change failure semantics for this pipeline to avoid exceptions if that is the safest minimal change.",
            "- Prefer safe fallbacks over broad refactors.",
            "- For patchit_chaos_dag: make chaos_lib helpers non-throwing; in particular, read_json must handle malformed JSON and return a safe default.",
        ]
        if goal == "converge_to_green"
        else [
            "REPAIR GOAL: remediate_failure — fix root cause while preserving failure semantics.",
            "- Do NOT silently convert failures into success.",
        ]
    )

    event = payload.get("event", {})
    normalized_error = payload.get("normalized_error", {})
    code_context = payload.get("code_context", {})
    artifact_summaries = payload.get("artifact_summaries", [])
    codebase_index = payload.get("codebase_index", {})
    verifier_feedback = payload.get("verifier_feedback")

    return "\n".join(
        [
            "You are PATCHIT Fix Agent. Generate a minimal SAFE patch as a unified diff.",
            "",
            "HARD RULES:",
            "- Output ONLY a unified diff (git apply compatible). No prose outside the diff.",
            "- Touch the fewest files possible.",
            "- If REPAIR GOAL is remediate_failure: do NOT change failure semantics silently.",
            "- Prefer fixing upstream causes (atomic writes/temp rename, retry/backoff for race) rather than suppressing validation.",
            "- Do NOT modify: .git/, venv/, .venv/, terraform/, secrets/, config.env",
            "- The diff MUST be COMPLETE and VALID. It MUST include:",
            "  - file headers: '--- a/…' and '+++ b/…'",
            "  - at least one hunk header in EXACT unified format: '@@ -<oldLine>,<oldCount> +<newLine>,<newCount> @@'",
            "  - at least one actual change line starting with '+' or '-'",
            "- Do NOT output placeholders like '...' or 'more code'.",
            "- End the diff with a final newline.",
            "",
            "CRITICAL CONTROL FLOW RULES:",
            "- If the failing code has `raise Exception(...)` for a scenario, REMOVE or REPLACE that raise in your diff.",
            "- Do NOT add a second `if` block for the same condition - MODIFY the original block.",
            "- Code after `raise` or `return` is UNREACHABLE. Never add fix code after these.",
            "- Your diff must REMOVE or MODIFY the original raise line, not add code after it.",
            "",
            *goal_rules,
            "",
            "PASS-1 ANALYSIS (authoritative intent):",
            analysis_summary,
            "",
            "FAILURE EVENT (JSON):",
            __import__("json").dumps(event, indent=2),
            "",
            "NORMALIZED ERROR (JSON):",
            __import__("json").dumps(normalized_error, indent=2),
            "",
            "ARTIFACT SUMMARIES (JSON):",
            __import__("json").dumps(artifact_summaries, indent=2),
            "",
            "CODE CONTEXT SNIPPETS (JSON):",
            __import__("json").dumps(code_context, indent=2),
            "",
            "CODEBASE INDEX (JSON):",
            __import__("json").dumps(codebase_index, indent=2),
            "",
            "SANDBOX VERIFIER FEEDBACK (if present; you MUST fix this and iterate):",
            (verifier_feedback or "null"),
        ]
    )


