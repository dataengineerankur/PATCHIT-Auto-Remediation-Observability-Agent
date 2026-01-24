from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from patchit.models import RootCauseReport


def render_root_cause_report_md(*, report: RootCauseReport, title: Optional[str] = None) -> str:
    """
    Render a human-readable markdown document suitable for download + auditing.
    """
    now = datetime.now(timezone.utc).isoformat()
    lines: list[str] = []
    lines.append(f"# {title or 'PATCHIT Root Cause Report'}")
    lines.append("")
    lines.append(f"- generated_at: `{now}`")
    lines.append(f"- event_id: `{report.event_id}`")
    lines.append(f"- pipeline_id: `{report.pipeline_id}`")
    lines.append(f"- run_id: `{report.run_id}`")
    if report.task_id:
        lines.append(f"- task_id: `{report.task_id}`")
    lines.append(f"- error_fingerprint: `{report.error_fingerprint}`")
    # Prefer enum value for readability
    try:
        cat = report.category.value  # type: ignore[attr-defined]
    except Exception:
        cat = str(report.category)
    lines.append(f"- category: `{cat}`")
    lines.append("")

    lines.append("## Failure summary")
    lines.append("")
    lines.append(report.failure_summary.strip() if report.failure_summary else "(none)")
    lines.append("")

    if report.timeline:
        lines.append("## Attempt timeline")
        lines.append("")
        for a in report.timeline:
            lines.append(f"- attempt_id: `{a.attempt_id}`")
            if a.patch_id:
                lines.append(f"  - patch_id: `{a.patch_id}`")
            if a.patch_title:
                lines.append(f"  - patch_title: `{a.patch_title}`")
            if a.verify_ok is not None:
                lines.append(f"  - verify_ok: `{a.verify_ok}`")
            if a.verify_summary:
                lines.append(f"  - verify_summary: `{a.verify_summary}`")
            if a.pr_url:
                lines.append(f"  - pr_url: `{a.pr_url}`")
            if a.notes:
                lines.append(f"  - notes: {a.notes}")
        lines.append("")

    if report.recommended_next_steps:
        lines.append("## Recommended next steps")
        lines.append("")
        for s in report.recommended_next_steps:
            lines.append(f"- {s}")
        lines.append("")

    if report.notes:
        lines.append("## Notes")
        lines.append("")
        lines.append(report.notes.strip())
        lines.append("")

    # Ensure trailing newline for nicer downloads/diffs
    return "\n".join(lines).rstrip() + "\n"


