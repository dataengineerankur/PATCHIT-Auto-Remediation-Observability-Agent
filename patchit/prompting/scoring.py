from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Tuple


@dataclass(frozen=True)
class ScoreResult:
    score: float
    reasons: list[str]


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def score_json_stage(*, stage: str, parsed: Dict[str, Any] | None, raw: str, parse_error: str | None) -> ScoreResult:
    """
    Lightweight heuristic scoring:
    - Valid JSON with expected schema and required fields scores higher.
    - Missing confidence/evidence is penalized.
    - Refusal without a reason is penalized.
    """
    reasons: list[str] = []
    if parse_error or not parsed:
        return ScoreResult(score=0.0, reasons=[f"parse_error={parse_error or 'unknown'}"])

    s = 0.4
    schema = str(parsed.get("schema") or "")
    if schema.startswith("patchit.prompt.") and schema.endswith(".v1"):
        s += 0.2
    else:
        reasons.append("missing_or_wrong_schema")

    conf = parsed.get("confidence")
    if isinstance(conf, (int, float)):
        s += 0.15
        if conf < 0.35:
            reasons.append("low_confidence")
    else:
        reasons.append("missing_confidence")

    # Stage-specific checks
    if stage == "rca":
        if isinstance(parsed.get("root_cause_hypothesis"), str) and parsed.get("root_cause_hypothesis"):
            s += 0.15
        else:
            reasons.append("missing_root_cause_hypothesis")
        ev = parsed.get("evidence")
        if isinstance(ev, list) and len(ev) >= 2:
            s += 0.1
        else:
            reasons.append("thin_evidence")
    elif stage == "fix":
        decision = parsed.get("decision")
        if decision in ("propose_patch", "refuse"):
            s += 0.1
        else:
            reasons.append("missing_decision")
        if decision == "refuse":
            rr = parsed.get("refusal_reason")
            if isinstance(rr, str) and rr.strip():
                s += 0.1
            else:
                reasons.append("refusal_missing_reason")
        else:
            edits = parsed.get("edits")
            if isinstance(edits, list) and len(edits) >= 1:
                s += 0.2
            else:
                reasons.append("no_edits")
    elif stage == "validate":
        plan = parsed.get("validation_plan")
        if isinstance(plan, list) and len(plan) >= 1:
            s += 0.2
        else:
            reasons.append("missing_validation_plan")
    elif stage == "decide":
        act = parsed.get("final_action")
        if act in ("create_pr", "adopt_cursor_pr", "refuse", "diagnose_only"):
            s += 0.15
        else:
            reasons.append("missing_final_action")
        rs = parsed.get("reasons")
        if isinstance(rs, list) and len(rs) >= 1:
            s += 0.1
        else:
            reasons.append("missing_reasons")

    # Penalize extremely long raw (often indicates rambling / not strictly JSON)
    if len(raw) > 12000:
        reasons.append("very_long_output")
        s -= 0.1

    return ScoreResult(score=_clamp01(s), reasons=reasons)


def extract_json_block(text: str) -> Tuple[Dict[str, Any] | None, str | None]:
    """
    Parse JSON from raw output. Prefer strict JSON-only, but tolerate accidental leading/trailing whitespace.
    """
    t = (text or "").strip()
    if not t:
        return None, "empty"
    try:
        return json.loads(t), None
    except Exception as e1:  # noqa: BLE001
        # Heuristic: find first { and last } and retry
        i = t.find("{")
        j = t.rfind("}")
        if i != -1 and j != -1 and j > i:
            try:
                return json.loads(t[i : j + 1]), None
            except Exception as e2:  # noqa: BLE001
                return None, f"json_parse_failed: {type(e2).__name__}: {e2}"
        return None, f"json_parse_failed: {type(e1).__name__}: {e1}"


