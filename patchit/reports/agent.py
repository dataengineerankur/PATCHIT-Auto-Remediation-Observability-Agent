from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from patchit.llm.groq_client import GroqClient
from patchit.llm.openrouter_client import OpenRouterClient
from patchit.models import NormalizedErrorObject, RootCauseCategory, RootCauseReport, UniversalFailureEvent


def _to_jsonable(obj: Any, *, max_depth: int = 6) -> Any:
    """
    Convert arbitrary objects (dataclasses, Pydantic models, sets, enums) to JSON-safe primitives.
    """
    if max_depth <= 0:
        return str(obj)
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(x, max_depth=max_depth - 1) for x in obj]
    if isinstance(obj, set):
        return [_to_jsonable(x, max_depth=max_depth - 1) for x in sorted(list(obj), key=lambda v: str(v))][:200]
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            out[str(k)] = _to_jsonable(v, max_depth=max_depth - 1)
        return out

    # Pydantic v2
    md = getattr(obj, "model_dump", None)
    if callable(md):
        try:
            return _to_jsonable(md(mode="json"), max_depth=max_depth - 1)
        except Exception:
            try:
                return _to_jsonable(md(), max_depth=max_depth - 1)
            except Exception:
                pass

    # dataclasses / plain objects
    d = getattr(obj, "__dict__", None)
    if isinstance(d, dict):
        return _to_jsonable(d, max_depth=max_depth - 1)

    # Enum-like
    v = getattr(obj, "value", None)
    if isinstance(v, (str, int, float, bool)):
        return v

    return str(obj)


def _extract_runtime_failure_snippet(text: str, *, max_chars: int = 9000) -> str:
    """
    Local validation output tends to end with docker-compose teardown, so the last N
    chars are often unhelpful. This function tries to extract the *signal* region.

    Heuristics:
      - include any lines around "Traceback", "ERROR", "FAILED", "state=failed"
      - include the first + last ~200 lines as context
    """
    if not text:
        return ""

    lines = text.splitlines()
    if not lines:
        return ""

    head = lines[:200]
    tail = lines[-260:]

    patterns = [
        r"\bTraceback \(most recent call last\):",
        r"\bERROR\b",
        r"\bException\b",
        r"\bFAILED\b",
        r"\bstate=failed\b",
        r"\bTask exited with return code\b",
        r"airflow dags list-runs",
        r"airflow tasks test",
        r"docker(\-compose)? .* (up|exec|logs)",
        r"poll\[\d+\]",
    ]
    rx = re.compile("|".join(patterns), re.IGNORECASE)

    hit_idxs: list[int] = []
    for i, ln in enumerate(lines):
        if rx.search(ln):
            hit_idxs.append(i)

    windows: list[str] = []
    # Capture small windows around up to first 12 hits.
    for i in hit_idxs[:12]:
        lo = max(0, i - 12)
        hi = min(len(lines), i + 45)
        windows.extend(lines[lo:hi])

    # De-dupe while preserving order
    seen: set[str] = set()
    uniq: list[str] = []
    for ln in (head + ["--- snip ---"] + windows + ["--- tail ---"] + tail):
        if ln in seen:
            continue
        seen.add(ln)
        uniq.append(ln)

    out = "\n".join(uniq).strip()
    if len(out) > max_chars:
        out = out[-max_chars:]
    return out


def _safe_json_extract(text: str) -> Dict[str, Any]:
    """
    Models sometimes wrap JSON in fences or extra prose; try hard to extract a JSON object.
    """
    t = (text or "").strip()
    t = t.replace("```json", "").replace("```", "").strip()

    # If the model returned extra text, try to find the first {...} block.
    if not t.startswith("{"):
        m = re.search(r"\{[\s\S]*\}", t)
        if m:
            t = m.group(0)
    data = json.loads(t)
    if not isinstance(data, dict):
        raise ValueError("report_agent_non_object_json")
    return data


@dataclass(frozen=True)
class ReportAgent:
    """
    LLM-backed report generator. Intended to be pipeline-agnostic: it reasons from
    the UniversalFailureEvent + NormalizedErrorObject + codebase index + runtime validation output.
    """

    mode: str  # openrouter|groq
    groq_api_key: str | None = None
    groq_base_url: str = "https://api.groq.com/openai/v1"
    groq_model: str = "openai/gpt-oss-120b"
    openrouter_api_key: str | None = None
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    openrouter_model: str = "openai/gpt-5.2"
    openrouter_site_url: str | None = None
    openrouter_site_name: str | None = None
    timeout_s: float = 60.0

    def _chat(self, *, messages: List[Dict[str, str]]) -> str:
        if self.mode == "groq":
            if not self.groq_api_key:
                raise RuntimeError("groq_api_key_missing")
            client = GroqClient(api_key=self.groq_api_key, base_url=self.groq_base_url, timeout_s=self.timeout_s)
            return client.chat(model=self.groq_model, messages=messages, max_tokens=2048)
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
                return client.chat(model=self.openrouter_model, messages=messages, max_tokens=2048)
            except Exception as e:  # noqa: BLE001
                msg = str(e)
                if self.groq_api_key and ("openrouter_http_402" in msg or "more credits" in msg.lower()):
                    groq = GroqClient(api_key=self.groq_api_key, base_url=self.groq_base_url, timeout_s=self.timeout_s)
                    return groq.chat(model=self.groq_model, messages=messages, max_tokens=2048)
                raise
        raise RuntimeError(f"unsupported_report_agent_mode: {self.mode}")

    def generate(
        self,
        *,
        event: UniversalFailureEvent,
        error_fingerprint: str,
        error: NormalizedErrorObject,
        artifact_summaries: List[Any],
        codebase_index: Dict[str, Any],
        local_validation_output: str | None,
        local_validation_output_path: str | None,
        patch_id: str | None,
        patch_title: str | None,
        patch_files: List[str],
        attempt: int,
        prior_attempts: List[Dict[str, Any]] | None = None,
    ) -> RootCauseReport:
        snippet = _extract_runtime_failure_snippet(local_validation_output or "")
        artifacts_json = _to_jsonable(artifact_summaries)
        codebase_json = _to_jsonable(
            {
                k: codebase_index.get(k)
                for k in ["airflow_dags", "airflow_dag_triggers", "artifact_contracts"]
                if isinstance(codebase_index, dict)
            }
        )
        prior_json = _to_jsonable(prior_attempts or [])
        schema = {
            "type": "object",
            "required": ["category", "failure_summary", "recommended_next_steps", "notes"],
            "properties": {
                "category": {"type": "string", "enum": [c.value for c in RootCauseCategory]},
                "failure_summary": {"type": "string"},
                "recommended_next_steps": {"type": "array", "items": {"type": "string"}},
                "notes": {"type": "string"},
            },
        }

        prompt = (
            "You are PATCHIT's ReportAgent. Your job: produce an audit-ready root-cause analysis report for a failed "
            "pipeline task. You must reason about upstream DAGs/tasks and artifacts from the provided context.\n\n"
            "CRITICAL: Output MUST be a single JSON object and MUST conform to this JSON Schema:\n"
            f"{json.dumps(schema)}\n\n"
            "Guidance:\n"
            "- Be specific: name the DAG(s), task(s), and artifact(s) involved.\n"
            "- If you infer an upstream producer, say WHY (trigger graph, artifact contract, error text).\n"
            "- If the failure is an env/infra issue (ports, docker, DB), say so and give concrete ops steps.\n"
            "- Do NOT propose patches in this report. Only diagnosis + next steps.\n\n"
            "Context:\n"
            f"- attempt: {attempt}\n"
            f"- patch_id: {patch_id}\n"
            f"- patch_title: {patch_title}\n"
            f"- patch_files: {patch_files}\n"
            f"- local_validation_output_path: {local_validation_output_path}\n\n"
            "UniversalFailureEvent:\n"
            f"{json.dumps(event.model_dump(mode='json'))}\n\n"
            "NormalizedErrorObject:\n"
            f"{json.dumps(error.model_dump(mode='json'))}\n\n"
            "artifact_summaries:\n"
            f"{json.dumps(artifacts_json)}\n\n"
            "codebase_index (high level; includes DAG triggers/graph):\n"
            f"{json.dumps(codebase_json)}\n\n"
            "local_validation_output_snippet (signal extracted):\n"
            f"{snippet}\n\n"
            "prior_attempts (most recent first):\n"
            f"{json.dumps(prior_json)}\n"
        )

        raw = self._chat(messages=[{"role": "user", "content": prompt}])
        data = _safe_json_extract(raw)

        cat = data.get("category", RootCauseCategory.unknown.value)
        try:
            category = RootCauseCategory(cat)
        except Exception:
            category = RootCauseCategory.unknown

        return RootCauseReport(
            event_id=event.event_id,
            pipeline_id=event.pipeline_id,
            run_id=event.run_id,
            task_id=event.task_id,
            error_fingerprint=error_fingerprint,
            failure_summary=str(data.get("failure_summary") or "").strip() or "Root cause analysis unavailable.",
            category=category,
            recommended_next_steps=[str(s).strip() for s in (data.get("recommended_next_steps") or []) if str(s).strip()],
            timeline=[],
            notes=str(data.get("notes") or "").strip()
            + (
                "\n\n== Attachments ==\n"
                + (f"- local_validation_output_path: {local_validation_output_path}\n" if local_validation_output_path else "")
            ),
        )


