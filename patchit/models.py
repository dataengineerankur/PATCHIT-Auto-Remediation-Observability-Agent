from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class Platform(str, Enum):
    airflow = "airflow"
    dbt = "dbt"


class StackFrame(BaseModel):
    file_path: str
    line_number: int
    function: Optional[str] = None
    code_line: Optional[str] = None


class UniversalFailureEvent(BaseModel):
    """
    Platform-agnostic failure event emitted by adapters (Airflow, dbt, etc).
    This is the single contract ingested by the PATCHIT service.
    """

    event_id: str = Field(..., description="Stable id for de-dupe/correlation.")
    platform: Platform
    pipeline_id: str
    run_id: str
    task_id: Optional[str] = None
    try_number: Optional[int] = None
    detected_at: datetime = Field(default_factory=lambda: datetime.utcnow())

    status: Literal["failed", "upstream_failed"] = "failed"

    # Where to fetch logs + artifacts
    log_uri: Optional[str] = Field(
        default=None,
        description="file://... or http(s)://... depending on adapter",
    )
    artifact_uris: List[str] = Field(default_factory=list)

    # Optional platform-specific details (kept, but never required)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class NormalizedErrorObject(BaseModel):
    """
    Normalized error record derived from logs/stack traces across platforms.
    """

    error_id: str
    error_type: str
    message: str
    stack: List[StackFrame] = Field(default_factory=list)
    raw_excerpt: Optional[str] = None

    # Root cause reasoning
    classification: Optional[str] = None
    likely_root_cause: Optional[str] = None
    likely_upstream_task_id: Optional[str] = None
    confidence: float = 0.5


class PatchOperation(str, Enum):
    update = "update"
    add = "add"


class PatchFileChange(BaseModel):
    path: str
    operation: PatchOperation


class PatchProposal(BaseModel):
    patch_id: str
    title: str
    rationale: str
    diff_unified: str
    files: List[PatchFileChange]
    tests_to_run: List[str] = Field(default_factory=list)
    confidence: float = 0.5
    requires_human_approval: bool = True


class RemediationPolicy(BaseModel):
    name: str = "default"
    require_human_approval: bool = True
    allow_write_paths: List[str] = Field(
        default_factory=lambda: [
            "airflow/dags/",
            "airflow_extra_dags/",
            # Common repo roots for standalone Airflow DAG repos (e.g., 2-10-example-dags)
            "dags/",
            "dbt/",
            # Many repos keep dbt projects in folders like dbt_retail/, dbt_finance/, etc.
            # Policy prefixes are simple startswith checks, so `dbt_` covers that common convention.
            "dbt_",
            # Common dbt project layout at repo root
            "models/",
            "tests/",
            # Airflow include/ helpers (shared libs)
            "include/",
            "patchit/",
            "mock_api/",
            # Common Python repo layout (enterprise multi-repo support):
            # Spark/shared repos often keep all code under src/
            "src/",
            # Common Spark repo layout (e.g., pyspark-example-project)
            "jobs/",
            "pyproject.toml",
            "README.md",
            # SWE-bench repo root
            "swebench/",
        ]
    )
    deny_write_paths: List[str] = Field(
        default_factory=lambda: [
            ".git/",
            "venv/",
            ".venv/",
            "secrets/",
            "terraform/",
            "config.env",
        ]
    )
    max_files_touched: int = 5
    max_diff_bytes: int = 60_000


class PolicyDecision(BaseModel):
    allowed: bool
    reasons: List[str] = Field(default_factory=list)


class PullRequestResult(BaseModel):
    mode: Literal["mock", "real"]
    pr_number: int
    pr_title: str
    pr_url: str
    branch_name: str


class RemediationResult(BaseModel):
    event: UniversalFailureEvent
    error: NormalizedErrorObject
    patch: Optional[PatchProposal] = None
    policy: PolicyDecision
    pr: Optional[PullRequestResult] = None
    report: Optional["RootCauseReport"] = None
    audit_correlation_id: str


class RootCauseCategory(str, Enum):
    code_bug = "code_bug"
    env_mismatch = "env_mismatch"
    dependency_issue = "dependency_issue"
    infra_issue = "infra_issue"
    data_issue = "data_issue"
    unknown = "unknown"


class AttemptSummary(BaseModel):
    attempt_id: str
    patch_id: Optional[str] = None
    patch_title: Optional[str] = None
    verify_ok: Optional[bool] = None
    verify_summary: Optional[str] = None
    pr_url: Optional[str] = None
    notes: Optional[str] = None


class RootCauseReport(BaseModel):
    """
    Human-readable, audit-ready report emitted when PATCHIT decides to stop auto-fixing.
    """

    event_id: str
    pipeline_id: str
    run_id: str
    task_id: Optional[str] = None
    error_fingerprint: str

    failure_summary: str
    category: RootCauseCategory = RootCauseCategory.unknown
    recommended_next_steps: List[str] = Field(default_factory=list)

    timeline: List[AttemptSummary] = Field(default_factory=list)
    notes: Optional[str] = None




