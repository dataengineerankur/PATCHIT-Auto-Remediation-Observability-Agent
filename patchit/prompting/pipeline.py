from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Tuple

from patchit.llm.groq_client import GroqClient
from patchit.llm.openrouter_client import OpenRouterClient
from patchit.prompting.contracts import DecideStageOutput, FixStageOutput, PromptPipelineTrace, RCAStageOutput, StageResult, ValidateStageOutput
from patchit.prompting.meta import META_PROMPT_V1, NEGATIVE_RULES_V1
from patchit.prompting.scoring import extract_json_block, score_json_stage


Provider = Literal["groq", "openrouter"]


@dataclass(frozen=True)
class PromptPipelineConfig:
    provider: Provider
    model: str
    timeout_s: float
    groq_api_key: str | None = None
    groq_base_url: str = "https://api.groq.com/openai/v1"
    openrouter_api_key: str | None = None
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    openrouter_site_url: str | None = None
    openrouter_site_name: str | None = None


def _chat(cfg: PromptPipelineConfig, *, system: str, user: str, max_tokens: int = 2048) -> str:
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]
    if cfg.provider == "groq":
        if not cfg.groq_api_key:
            raise RuntimeError("groq_api_key_missing")
        client = GroqClient(api_key=cfg.groq_api_key, base_url=cfg.groq_base_url, timeout_s=cfg.timeout_s)
        return client.chat(model=cfg.model, messages=messages, max_tokens=max_tokens)
    if cfg.provider == "openrouter":
        if not cfg.openrouter_api_key:
            raise RuntimeError("openrouter_api_key_missing")
        client = OpenRouterClient(
            api_key=cfg.openrouter_api_key,
            base_url=cfg.openrouter_base_url,
            timeout_s=cfg.timeout_s,
            site_url=cfg.openrouter_site_url,
            site_name=cfg.openrouter_site_name,
        )
        return client.chat(model=cfg.model, messages=messages, max_tokens=max_tokens)
    raise RuntimeError(f"unsupported_provider: {cfg.provider}")


def _context_pack(*, payload: Dict[str, Any], max_chars: int = 14000) -> str:
    """
    Strict context pack: relevant-only, forbid external assumptions.
    """
    # Keep this deterministic and compact.
    evt = payload.get("event") or {}
    err = payload.get("normalized_error") or {}
    cc = payload.get("code_context") or {}
    arts = payload.get("artifact_summaries") or []
    vf = payload.get("verifier_feedback")
    prior = (payload.get("codebase_index") or {}).get("prior_attempts") or []

    blob = {
        "event": evt,
        "normalized_error": err,
        "artifact_summaries": arts[:10],
        "code_context_files": list(cc.keys())[:30],
        "verifier_feedback": vf,
        "prior_attempts": prior[:8],
    }
    s = json.dumps(blob, indent=2)[:max_chars]
    return s


def _engineered_instructions(*, stage: str, playbook: Dict[str, Any] | None = None) -> str:
    extra_neg = []
    try:
        extra_neg = list((playbook or {}).get("extra_negative_rules") or [])
    except Exception:
        extra_neg = []
    neg_rules = list(NEGATIVE_RULES_V1) + [str(x) for x in extra_neg if str(x).strip()]
    neg = "\n".join([f"- {r}" for r in neg_rules])
    prefer_action = str((playbook or {}).get("prefer_action") or "").strip()
    pb_note = ""
    if prefer_action:
        pb_note = f"\nPLAYBOOK PREFERENCE: prefer_action={prefer_action} (only if consistent with evidence/safety)\n"
    if stage == "rca":
        return (
            "ENGINEERED SEQUENCE (RCA stage):\n"
            "1) Analyze inputs (logs/context/artifacts)\n"
            "2) Identify risks and unsafe moves\n"
            "3) Trace root cause (producer vs consumer vs infra)\n"
            "4) Evaluate safety and confidence\n"
            "5) Output RCA JSON only\n\n"
            f"NEGATIVE RULES:\n{neg}\n"
            + pb_note
        )
    if stage == "fix":
        return (
            "ENGINEERED SEQUENCE (Fix stage):\n"
            "1) Use RCA + evidence to decide propose_patch vs refuse\n"
            "2) If propose_patch: produce STRUCTURED edits for minimal safe change\n"
            "3) Preserve semantics; do not silence failures\n"
            "4) List smallest relevant tests/commands to validate\n"
            "5) Output Fix JSON only\n\n"
            "CRITICAL CONTROL FLOW RULES:\n"
            "- If the failing code has `raise Exception(...)` for a scenario, you MUST REMOVE or REPLACE that raise.\n"
            "- Do NOT add a second if-block for the same condition below the original - MODIFY the original block.\n"
            "- Code after `raise` or `return` is UNREACHABLE. Never add 'fix' code after these statements.\n"
            "- The fix must make the scenario PASS, not just add new code that can't execute.\n\n"
            f"NEGATIVE RULES:\n{neg}\n"
            + pb_note
        )
    if stage == "validate":
        return (
            "ENGINEERED SEQUENCE (Validation stage):\n"
            "1) Propose a minimal validation plan that proves the fix or proves refusal\n"
            "2) List expected success signals\n"
            "3) Include rollback plan\n"
            "4) Output Validate JSON only\n\n"
            f"NEGATIVE RULES:\n{neg}\n"
            + pb_note
        )
    if stage == "decide":
        return (
            "ENGINEERED SEQUENCE (Decision stage):\n"
            "1) Decide create_pr vs refuse vs diagnose_only\n"
            "2) Explain reasoning and confidence\n"
            "3) If PR: provide Markdown body\n"
            "4) Output Decide JSON only\n\n"
            f"NEGATIVE RULES:\n{neg}\n"
            + pb_note
        )
    return f"stage={stage}"


def _variant_prompts(*, stage: str, playbook: Dict[str, Any] | None = None) -> List[Tuple[str, str]]:
    """
    Returns (variant_id, modality) pairs. Modality drives instruction style.
    """
    prefer = str((playbook or {}).get("prefer_modality") or "").strip()
    if stage in ("rca", "validate", "decide"):
        base = [("A_concise", "step_by_step"), ("B_risk_first", "bullets")]
        if prefer and prefer in ("bullets", "step_by_step", "diff_reasoning"):
            # Make preferred modality the first candidate
            base = [(f"P_prefer_{prefer}", prefer)] + [b for b in base if b[1] != prefer]
        return base
    # Fix stage gets an extra variant to push diff-based reasoning.
    base = [("A_concise", "step_by_step"), ("B_risk_first", "bullets"), ("C_diff_reasoning", "diff_reasoning")]
    if prefer and prefer in ("bullets", "step_by_step", "diff_reasoning"):
        base = [(f"P_prefer_{prefer}", prefer)] + [b for b in base if b[1] != prefer]
    return base


def _stage_user_prompt(*, stage: str, modality: str, payload: Dict[str, Any], prior_stage_json: Dict[str, Any] | None) -> str:
    ctx = _context_pack(payload=payload)
    prior = json.dumps(prior_stage_json, indent=2) if prior_stage_json else ""
    if modality == "bullets":
        fmt = "Use short bullets in fields like evidence/risks/next_checks; still output JSON only."
    elif modality == "diff_reasoning":
        fmt = "Think in terms of minimal diffs and file edits; still output JSON only."
    else:
        fmt = "Be step-by-step internally, but output JSON only."

    if stage == "rca":
        return (
            f"{fmt}\n\n"
            "Return JSON matching schema `patchit.prompt.rca.v1`.\n"
            "Include evidence strings that quote file paths, task ids, and exact error messages.\n\n"
            f"CONTEXT_PACK_JSON:\n{ctx}\n"
        )
    if stage == "fix":
        return (
            f"{fmt}\n\n"
            "Return JSON matching schema `patchit.prompt.fix.v1`.\n"
            "- If refusing, set decision=refuse and include refusal_reason.\n"
            "- If proposing, set decision=propose_patch and include edits[].path + edits[].new_content.\n"
            "- Only operation=update is supported.\n\n"
            f"RCA_JSON:\n{prior}\n\n"
            f"CONTEXT_PACK_JSON:\n{ctx}\n"
        )
    if stage == "validate":
        return (
            f"{fmt}\n\n"
            "Return JSON matching schema `patchit.prompt.validate.v1`.\n"
            f"FIX_JSON:\n{prior}\n\n"
            f"CONTEXT_PACK_JSON:\n{ctx}\n"
        )
    if stage == "decide":
        return (
            f"{fmt}\n\n"
            "Return JSON matching schema `patchit.prompt.decide.v1`.\n"
            f"FIX_JSON:\n{prior}\n\n"
            f"CONTEXT_PACK_JSON:\n{ctx}\n"
        )
    raise RuntimeError(f"unsupported_stage: {stage}")


def run_prompt_pipeline(
    cfg: PromptPipelineConfig,
    *,
    payload: Dict[str, Any],
    playbook: Dict[str, Any] | None = None,
) -> PromptPipelineTrace:
    """
    Full methodology pipeline:
    - RCA -> Fix -> Validate -> Decide
    - For each stage: run prompt variants, parse JSON, score, pick best
    """
    trace = PromptPipelineTrace()
    if playbook:
        trace.playbook_applied = dict(playbook)

    system = META_PROMPT_V1
    selected_json: Dict[str, Any] | None = None

    for stage in ("rca", "fix", "validate", "decide"):
        best: StageResult | None = None
        for variant_id, modality in _variant_prompts(stage=stage, playbook=playbook):
            user = _engineered_instructions(stage=stage, playbook=playbook) + "\n" + _stage_user_prompt(
                stage=stage,
                modality=modality,
                payload=payload,
                prior_stage_json=selected_json,
            )
            t0 = time.time()
            raw = _chat(cfg, system=system, user=user, max_tokens=2400 if stage == "fix" else 1800)
            parsed, perr = extract_json_block(raw)
            sr = score_json_stage(stage=stage, parsed=parsed, raw=raw, parse_error=perr)
            res = StageResult(
                stage=stage, variant_id=variant_id, modality=modality, score=sr.score, raw_text=raw, parsed=parsed, parse_error=perr
            )
            trace.all_results.append(res)
            if (best is None) or (res.score > best.score):
                best = res
        if not best:
            trace.notes.append(f"{stage}: no result")
            break
        trace.selected[stage] = best
        # Advance chaining JSON
        selected_json = best.parsed or None

    return trace


def parse_typed_outputs(trace: PromptPipelineTrace) -> tuple[RCAStageOutput | None, FixStageOutput | None, ValidateStageOutput | None, DecideStageOutput | None]:
    rca = None
    fix = None
    val = None
    dec = None
    if (r := trace.selected.get("rca")) and r.parsed:
        try:
            rca = RCAStageOutput.model_validate(r.parsed)
        except Exception:
            rca = None
    if (f := trace.selected.get("fix")) and f.parsed:
        try:
            fix = FixStageOutput.model_validate(f.parsed)
        except Exception:
            fix = None
    if (v := trace.selected.get("validate")) and v.parsed:
        try:
            val = ValidateStageOutput.model_validate(v.parsed)
        except Exception:
            val = None
    if (d := trace.selected.get("decide")) and d.parsed:
        try:
            dec = DecideStageOutput.model_validate(d.parsed)
        except Exception:
            dec = None
    return rca, fix, val, dec


