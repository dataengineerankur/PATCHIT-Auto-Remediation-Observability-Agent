import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import altair as alt
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
AUDIT_PATH = ROOT / "var" / "audit" / "patchit_audit.jsonl"
EVIDENCE_DIR = ROOT / "var" / "evidence"
REPORTS_DIR = ROOT / "var" / "reports"
SWE_BENCH_ROOT = ROOT.parents[0] / "SWE-bench"
SWE_BENCH_LOGS = SWE_BENCH_ROOT / "logs" / "run_evaluation"

st.set_page_config(page_title="PATCHIT Observability", layout="wide")
st.markdown(
    "<h1 style='margin-bottom:0'>PATCHIT Observability Dashboard</h1>",
    unsafe_allow_html=True,
)
st.markdown(
    "<div class='section-note' style='margin-bottom:16px'>"
    "Live operational insights for remediation health, quality, and velocity."
    "</div>",
    unsafe_allow_html=True,
)

st.markdown(
    """
    <style>
    .stApp {
        background: radial-gradient(1200px 800px at 10% 0%, #0b1220 0%, #0b1120 40%, #0f172a 100%);
        color: #e2e8f0;
    }
    header[data-testid="stHeader"] {
        display: none;
    }
    div[data-testid="stDecoration"] {
        display: none;
    }
    #MainMenu, footer {
        visibility: hidden;
    }
    @keyframes glowPulse {
        0% { box-shadow: 0 0 0 rgba(96, 165, 250, 0.0); }
        50% { box-shadow: 0 0 16px rgba(99, 102, 241, 0.25); }
        100% { box-shadow: 0 0 0 rgba(96, 165, 250, 0.0); }
    }
    .stSidebar {
        background: #0b1220;
    }
    .block-container {
        padding-top: 1.6rem;
        padding-bottom: 2rem;
    }
    .kpi-card {
        background: linear-gradient(135deg, #0f172a 0%, #111827 100%);
        border: 1px solid #1f2937;
        border-radius: 14px;
        padding: 16px 18px;
        box-shadow: 0 4px 12px rgba(15, 23, 42, 0.2);
    }
    .kpi-card:hover {
        border-color: #334155;
        animation: glowPulse 1.6s ease-in-out infinite;
        transform: translateY(-2px);
        transition: transform 0.2s ease;
    }
    .kpi-label {
        font-size: 12px;
        color: #93c5fd;
        letter-spacing: 0.4px;
        text-transform: uppercase;
        margin-bottom: 6px;
    }
    .kpi-value {
        font-size: 26px;
        font-weight: 700;
        color: #f8fafc;
        margin-bottom: 2px;
    }
    .kpi-sub {
        font-size: 12px;
        color: #cbd5f5;
    }
    .section-note {
        color: #94a3b8;
        font-size: 12px;
    }
    .chart-card {
        background: #0b1220;
        border: 1px solid #1f2937;
        border-radius: 16px;
        padding: 10px 16px 4px 16px;
        box-shadow: 0 6px 16px rgba(15, 23, 42, 0.24);
    }
    .chart-card:hover {
        border-color: #334155;
        box-shadow: 0 8px 22px rgba(15, 23, 42, 0.35);
        transition: all 0.2s ease;
    }
    .chart-title {
        color: #cbd5f5;
        font-size: 14px;
        letter-spacing: 0.3px;
        text-transform: uppercase;
        margin: 8px 0 4px 0;
    }
    section[data-testid="stSidebar"] label {
        color: #cbd5f5 !important;
    }
    section[data-testid="stSidebar"] span {
        color: #e2e8f0 !important;
    }
    section[data-testid="stSidebar"] div[data-baseweb="tag"] {
        background: #1e293b !important;
        color: #e2e8f0 !important;
        border: 1px solid #334155 !important;
    }
    section[data-testid="stSidebar"] input {
        color: #e2e8f0 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def patchit_theme():
    return {
        "config": {
            "background": "#0b1220",
            "view": {"stroke": "#0b1220"},
            "axis": {
                "labelColor": "#cbd5f5",
                "titleColor": "#cbd5f5",
                "gridColor": "#1f2937",
                "domainColor": "#1f2937",
            },
            "legend": {
                "labelColor": "#cbd5f5",
                "titleColor": "#94a3b8",
            },
        }
    }


alt.themes.register("patchit", patchit_theme)
alt.themes.enable("patchit")


def load_audit() -> pd.DataFrame:
    if not AUDIT_PATH.exists():
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    with AUDIT_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    df = pd.json_normalize(rows)
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    return df


def load_evidence() -> pd.DataFrame:
    if not EVIDENCE_DIR.exists():
        return pd.DataFrame()
    rows = []
    for path in EVIDENCE_DIR.glob("*.json"):
        try:
            rows.append(json.loads(path.read_text()))
        except Exception:
            continue
    return pd.json_normalize(rows)


def load_swebench_reports() -> pd.DataFrame:
    if not SWE_BENCH_LOGS.exists():
        return pd.DataFrame()
    rows = []
    for report in SWE_BENCH_LOGS.glob("**/report.json"):
        try:
            payload = json.loads(report.read_text())
        except Exception:
            continue
        for instance_id, details in payload.items():
            rows.append(
                {
                    "instance_id": instance_id,
                    "resolved": details.get("resolved"),
                    "patch_applied": details.get("patch_successfully_applied"),
                    "patch_exists": details.get("patch_exists"),
                    "run_id": report.parents[2].name,
                }
            )
    return pd.DataFrame(rows)


def load_swebench_summaries() -> pd.DataFrame:
    if not SWE_BENCH_ROOT.exists():
        return pd.DataFrame()
    rows = []
    for path in SWE_BENCH_ROOT.glob("patchit__*.json"):
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        stem = path.name.replace(".json", "")
        if "." in stem:
            model, run_id = stem.split(".", 1)
        else:
            model, run_id = stem, "unknown"
        rows.append(
            {
                "run_id": run_id,
                "model": model,
                "total": payload.get("total_instances", 0),
                "submitted": payload.get("submitted_instances", 0),
                "resolved": payload.get("resolved_instances", 0),
                "errors": payload.get("error_instances", 0),
                "empty": payload.get("empty_patch_instances", 0),
                "path": str(path),
                "ts": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc),
            }
        )
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def load_data():
    return load_audit(), load_evidence(), load_swebench_reports(), load_swebench_summaries()


audit_df, evidence_df, swebench_df, swebench_summary_df = load_data()

if audit_df.empty:
    st.warning("No audit data found yet.")
    st.stop()

if "payload.repo_key" in audit_df.columns:
    audit_df["repo_key"] = audit_df["payload.repo_key"].fillna("unknown")
else:
    audit_df["repo_key"] = "unknown"

if "ts" in audit_df.columns and audit_df["ts"].notna().any():
    min_ts = audit_df["ts"].min().to_pydatetime()
    max_ts = audit_df["ts"].max().to_pydatetime()
else:
    min_ts = datetime.now(timezone.utc)
    max_ts = datetime.now(timezone.utc)

st.sidebar.header("Filters")
time_window = st.sidebar.selectbox(
    "Time window",
    ["Last 24h", "Last 7d", "Last 30d", "All data", "Custom"],
    index=2,
)
if time_window == "Custom":
    date_range = st.sidebar.date_input(
        "Date range (UTC)",
        (min_ts.date(), max_ts.date()),
    )
    start_dt = datetime.combine(date_range[0], datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(date_range[1], datetime.max.time(), tzinfo=timezone.utc)
elif time_window == "All data":
    start_dt = min_ts
    end_dt = max_ts
else:
    end_dt = max_ts
    delta = {
        "Last 24h": timedelta(days=1),
        "Last 7d": timedelta(days=7),
        "Last 30d": timedelta(days=30),
    }[time_window]
    start_dt = end_dt - delta

event_types_all = sorted(audit_df["event_type"].dropna().unique().tolist())
categories = sorted({evt.split(".")[0] for evt in event_types_all})
core_categories = ["event", "pr", "report", "verify", "agent", "policy", "error"]
category_mode = st.sidebar.radio(
    "Category focus",
    ["Core", "All"],
    index=0,
)
if category_mode == "Core":
    selected_categories = [c for c in core_categories if c in categories]
else:
    selected_categories = categories

repos = sorted(audit_df["repo_key"].dropna().unique().tolist())
selected_repos = st.sidebar.multiselect(
    "Repos",
    repos,
    default=repos,
)
granularity = st.sidebar.selectbox("Time granularity", ["Hour", "Day"], index=1)
timeline_mode = st.sidebar.selectbox("Timeline view", ["All events", "By event type"], index=0)
top_n = st.sidebar.slider("Top event types", min_value=5, max_value=20, value=10, step=1)

base_filtered = audit_df.copy()
if "ts" in base_filtered.columns:
    base_filtered = base_filtered[(base_filtered["ts"] >= start_dt) & (base_filtered["ts"] <= end_dt)]
if selected_categories:
    base_filtered = base_filtered[
        base_filtered["event_type"].apply(lambda x: str(x).split(".")[0] in selected_categories)
    ]
if selected_repos:
    base_filtered = base_filtered[base_filtered["repo_key"].isin(selected_repos)]

event_types = sorted(base_filtered["event_type"].dropna().unique().tolist()) or event_types_all
event_counts = base_filtered["event_type"].value_counts()
default_events = [
    evt for evt in [
        "event.received",
        "pr.created",
        "report.saved",
        "verify.finished",
        "agent.failed",
        "policy.decided",
        "error.classified",
    ]
    if evt in event_types
]
if not default_events:
    default_events = event_counts.head(10).index.tolist() if not event_counts.empty else event_types[:10]

show_all_events = st.sidebar.checkbox("Show all event types", value=False)
selected_events = st.sidebar.multiselect(
    "Event types",
    event_types,
    default=event_types if show_all_events else default_events,
)

filtered = base_filtered.copy()
if selected_events:
    filtered = filtered[filtered["event_type"].isin(selected_events)]

st.markdown(
    f"<div class='section-note'>Showing {len(filtered)} of {len(audit_df)} events "
    f"from {start_dt.date()} to {end_dt.date()}.</div>",
    unsafe_allow_html=True,
)
if filtered.empty:
    st.info("No data for the selected filters. Try switching Time window to All data.")

# High-level metrics
st.subheader("Key Metrics")

pr_created = filtered[filtered["event_type"] == "pr.created"]
report_saved = filtered[filtered["event_type"] == "report.saved"]
agent_failed = filtered[filtered["event_type"] == "agent.failed"]
policy_blocked = filtered[(filtered["event_type"] == "policy.decided") & (filtered["payload.decision.allowed"] == False)]
verify_finished = filtered[filtered["event_type"] == "verify.finished"]
verify_ok = verify_finished[verify_finished["payload.ok"] == True]
verify_failed = verify_finished[verify_finished["payload.ok"] == False]

received_count = len(filtered[filtered["event_type"] == "event.received"])
patch_success_rate = (len(pr_created) / max(1, received_count)) * 100
rca_rate = (len(report_saved) / max(1, received_count)) * 100

col1, col2, col3, col4, col5 = st.columns(5)
col1.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>Events</div>"
    f"<div class='kpi-value'>{received_count}</div>"
    f"<div class='kpi-sub'>Total received</div></div>",
    unsafe_allow_html=True,
)
col2.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>PRs Created</div>"
    f"<div class='kpi-value'>{len(pr_created)}</div>"
    f"<div class='kpi-sub'>Patch proposals</div></div>",
    unsafe_allow_html=True,
)
col3.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>RCA Reports</div>"
    f"<div class='kpi-value'>{len(report_saved)}</div>"
    f"<div class='kpi-sub'>Root-cause docs</div></div>",
    unsafe_allow_html=True,
)
col4.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>Agent Failed</div>"
    f"<div class='kpi-value'>{len(agent_failed)}</div>"
    f"<div class='kpi-sub'>Needs review</div></div>",
    unsafe_allow_html=True,
)
col5.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>Policy Blocked</div>"
    f"<div class='kpi-value'>{len(policy_blocked)}</div>"
    f"<div class='kpi-sub'>Guardrail stops</div></div>",
    unsafe_allow_html=True,
)

col6, col7, col8, col9, col10 = st.columns(5)
col6.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>Verify OK</div>"
    f"<div class='kpi-value'>{len(verify_ok)}</div>"
    f"<div class='kpi-sub'>Sandbox pass</div></div>",
    unsafe_allow_html=True,
)
col7.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>Verify Failed</div>"
    f"<div class='kpi-value'>{len(verify_failed)}</div>"
    f"<div class='kpi-sub'>Potential false fix</div></div>",
    unsafe_allow_html=True,
)
col8.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>Patch Success Rate</div>"
    f"<div class='kpi-value'>{patch_success_rate:.1f}%</div>"
    f"<div class='kpi-sub'>PRs / events</div></div>",
    unsafe_allow_html=True,
)
col9.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>RCA Rate</div>"
    f"<div class='kpi-value'>{rca_rate:.1f}%</div>"
    f"<div class='kpi-sub'>RCA / events</div></div>",
    unsafe_allow_html=True,
)
col10.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>False Fix Signal</div>"
    f"<div class='kpi-value'>{len(verify_failed)}</div>"
    f"<div class='kpi-sub'>Verify failures</div></div>",
    unsafe_allow_html=True,
)

st.caption("False Fix Signal = count of verify failures")

st.subheader("Time to Remediation")
received = filtered[filtered["event_type"] == "event.received"]["ts"].reset_index(drop=True)
pr_ts = pr_created["ts"].reset_index(drop=True)
if not received.empty and not pr_ts.empty:
    min_len = min(len(received), len(pr_ts))
    ttr = (pr_ts.iloc[:min_len] - received.iloc[:min_len]).dt.total_seconds() / 60.0
    st.metric("Median Minutes to PR", f"{ttr.median():.1f} min")
elif filtered.empty:
    st.caption("No events available in the selected filters.")
else:
    st.caption("Not enough paired events to compute median time.")

st.markdown("<div class='chart-card'>", unsafe_allow_html=True)
st.markdown("<div class='chart-title'>Event Timeline</div>", unsafe_allow_html=True)
time_df = filtered.copy()
if "ts" in time_df.columns and not time_df.empty:
    rule = "H" if granularity == "Hour" else "D"
    time_df = time_df.dropna(subset=["ts"]).set_index("ts")
    if timeline_mode == "All events":
        time_df = (
            time_df.resample(rule).size().reset_index(name="count")
        )
        timeline_chart = (
            alt.Chart(time_df)
            .mark_area(line={"color": "#60a5fa"}, color="#1d4ed8", opacity=0.35)
            .encode(
                x=alt.X("ts:T", title="Time"),
                y=alt.Y("count:Q", title="Events"),
                tooltip=["ts:T", "count:Q"],
            )
            .properties(height=260)
        )
    else:
        time_df = (
            time_df.groupby("event_type")
            .resample(rule)
            .size()
            .reset_index(name="count")
        )
        timeline_chart = (
            alt.Chart(time_df)
            .mark_line(point=True, strokeWidth=2)
            .encode(
                x=alt.X("ts:T", title="Time"),
                y=alt.Y("count:Q", title="Events"),
                color=alt.Color("event_type:N", scale=alt.Scale(scheme="tableau10")),
                tooltip=["event_type", "ts:T", "count:Q"],
            )
            .properties(height=280)
        )
    st.altair_chart(timeline_chart, use_container_width=True)
elif filtered.empty:
    st.info("No events match the current filters.")
else:
    st.info("Timeline is unavailable because timestamps are missing.")
st.markdown("</div>", unsafe_allow_html=True)

# Events by type
st.markdown("<div class='chart-card'>", unsafe_allow_html=True)
st.markdown("<div class='chart-title'>Event Volume by Type</div>", unsafe_allow_html=True)
counts = filtered["event_type"].value_counts().reset_index()
counts.columns = ["event_type", "count"]
if len(counts) > top_n:
    top = counts.head(top_n)
    other = pd.DataFrame(
        [{"event_type": "Other", "count": counts["count"].iloc[top_n:].sum()}]
    )
    counts = pd.concat([top, other], ignore_index=True)
chart = (
    alt.Chart(counts)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("count:Q", title="Count"),
        y=alt.Y("event_type:N", sort="-x", title="Event Type"),
        color=alt.Color("event_type:N", scale=alt.Scale(scheme="tableau10")),
        tooltip=["event_type", "count"],
    )
    .properties(height=280)
)
st.altair_chart(chart, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

# Repo selection
st.markdown("<div class='chart-card'>", unsafe_allow_html=True)
st.markdown("<div class='chart-title'>Repo Selection</div>", unsafe_allow_html=True)
repo_selected = filtered[filtered["event_type"] == "repo.selected"]
if not repo_selected.empty:
    repo_counts = repo_selected["payload.repo_key"].value_counts().reset_index()
    repo_counts.columns = ["repo", "count"]
    chart = (
        alt.Chart(repo_counts)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("repo:N", title="Repository"),
            y=alt.Y("count:Q", title="Selections"),
            color=alt.Color("repo:N", scale=alt.Scale(scheme="category20")),
            tooltip=["repo", "count"],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.caption("No repo selections in the current filters.")
st.markdown("</div>", unsafe_allow_html=True)

# Classifications
st.markdown("<div class='chart-card'>", unsafe_allow_html=True)
st.markdown("<div class='chart-title'>Error Classification</div>", unsafe_allow_html=True)
class_df = filtered[filtered["event_type"] == "error.classified"]
if not class_df.empty:
    class_counts = class_df["payload.classification"].value_counts().reset_index()
    class_counts.columns = ["classification", "count"]
    chart = (
        alt.Chart(class_counts)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("classification:N", title="Classification"),
            y=alt.Y("count:Q", title="Count"),
            color=alt.Color("classification:N", scale=alt.Scale(scheme="set2")),
            tooltip=["classification", "count"],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.caption("No classified errors in the current filters.")
st.markdown("</div>", unsafe_allow_html=True)

# Evidence summary (if available)
if not evidence_df.empty:
    st.subheader("Evidence Summary")
    preferred_cols = ["repo_name", "scenario_id", "patchit_action", "patch_summary"]
    available_cols = [col for col in preferred_cols if col in evidence_df.columns]
    if available_cols:
        display_df = evidence_df[available_cols].rename(
            columns={"patchit_action": "patchit_action"}
        )
        st.dataframe(display_df)
    else:
        st.info("Evidence summary columns not found in current evidence data.")

# Raw audit viewer
with st.expander("Raw Audit Log"):
    st.dataframe(filtered.tail(200))
