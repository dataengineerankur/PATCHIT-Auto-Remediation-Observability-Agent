from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class Risk(BaseModel):
    id: str
    severity: Literal["low", "medium", "high", "critical"]
    description: str
    mitigation: str | None = None


class RCAStageOutput(BaseModel):
    schema: Literal["patchit.prompt.rca.v1"] = "patchit.prompt.rca.v1"
    summary: str
    root_cause_hypothesis: str
    producer_or_consumer: Literal["producer", "consumer", "infra", "unknown"] = "unknown"
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: List[str] = Field(default_factory=list, description="Short bullets quoting log lines / file paths / artifacts.")
    risks: List[Risk] = Field(default_factory=list)
    next_checks: List[str] = Field(default_factory=list)


class PatchEdit(BaseModel):
    path: str
    operation: Literal["update"] = "update"
    new_content: str


class FixStageOutput(BaseModel):
    schema: Literal["patchit.prompt.fix.v1"] = "patchit.prompt.fix.v1"
    decision: Literal["propose_patch", "refuse"] = "propose_patch"
    refusal_reason: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str
    edits: List[PatchEdit] = Field(default_factory=list)
    tests_to_run: List[str] = Field(default_factory=list)
    safety_notes: List[str] = Field(default_factory=list)


class ValidateStageOutput(BaseModel):
    schema: Literal["patchit.prompt.validate.v1"] = "patchit.prompt.validate.v1"
    validation_plan: List[str] = Field(default_factory=list)
    expected_signal: List[str] = Field(default_factory=list)
    rollback_plan: List[str] = Field(default_factory=list)


class DecideStageOutput(BaseModel):
    schema: Literal["patchit.prompt.decide.v1"] = "patchit.prompt.decide.v1"
    final_action: Literal["create_pr", "adopt_cursor_pr", "refuse", "diagnose_only"] = "diagnose_only"
    reasons: List[str] = Field(default_factory=list)
    pr_body_markdown: str = ""
    confidence: float = Field(ge=0.0, le=1.0)


class StageResult(BaseModel):
    stage: Literal["rca", "fix", "validate", "decide"]
    variant_id: str
    modality: str
    score: float
    raw_text: str
    parsed: Dict[str, Any] | None = None
    parse_error: str | None = None


class PromptPipelineTrace(BaseModel):
    schema: Literal["patchit.prompt.pipeline_trace.v1"] = "patchit.prompt.pipeline_trace.v1"
    selected: Dict[str, StageResult] = Field(default_factory=dict)
    all_results: List[StageResult] = Field(default_factory=list)
    playbook_applied: Dict[str, Any] = Field(default_factory=dict)
    notes: List[str] = Field(default_factory=list)


