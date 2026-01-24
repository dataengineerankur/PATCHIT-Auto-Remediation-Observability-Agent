import json
from datetime import datetime, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
SWE_BENCH_LOGS = ROOT.parents[0] / "SWE-bench" / "logs" / "run_evaluation"

st.set_page_config(page_title="PATCHIT SWE-bench", layout="wide")
st.markdown(
    "<h1 style='margin-bottom:0'>PATCHIT SWE-bench Dashboard</h1>",
    unsafe_allow_html=True,
)
st.markdown(
    "<div class='section-note' style='margin-bottom:16px'>"
    "Objective patch quality metrics from SWE-bench evaluations."
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
    .kpi-card {
        background: linear-gradient(135deg, #0f172a 0%, #111827 100%);
        border: 1px solid #1f2937;
        border-radius: 14px;
        padding: 16px 18px;
        box-shadow: 0 4px 12px rgba(15, 23, 42, 0.2);
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
    .chart-card {
        background: #0b1220;
        border: 1px solid #1f2937;
        border-radius: 16px;
        padding: 10px 16px 4px 16px;
        box-shadow: 0 6px 16px rgba(15, 23, 42, 0.24);
    }
    .chart-title {
        color: #cbd5f5;
        font-size: 14px;
        letter-spacing: 0.3px;
        text-transform: uppercase;
        margin: 8px 0 4px 0;
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


alt.themes.register("patchit_swe", patchit_theme)
alt.themes.enable("patchit_swe")


def load_swebench_reports() -> pd.DataFrame:
    if not SWE_BENCH_LOGS.exists():
        return pd.DataFrame()
    rows = []
    for report in SWE_BENCH_LOGS.glob("**/report.json"):
        try:
            payload = json.loads(report.read_text())
        except Exception:
            continue
        run_id = report.parents[2].name
        ts = datetime.fromtimestamp(report.stat().st_mtime, tz=timezone.utc)
        for instance_id, details in payload.items():
            rows.append(
                {
                    "instance_id": instance_id,
                    "repo": instance_id.split("__")[0],
                    "resolved": details.get("resolved"),
                    "patch_applied": details.get("patch_successfully_applied"),
                    "patch_exists": details.get("patch_exists"),
                    "run_id": run_id,
                    "run_ts": ts,
                }
            )
    return pd.DataFrame(rows)


swe_df = load_swebench_reports()
if swe_df.empty:
    st.warning("No SWE-bench reports found yet.")
    st.stop()

total = len(swe_df)
resolved = int(swe_df["resolved"].sum())
applied = int(swe_df["patch_applied"].sum())
resolved_rate = (resolved / max(1, total)) * 100

col1, col2, col3, col4 = st.columns(4)
col1.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>Instances</div>"
    f"<div class='kpi-value'>{total}</div><div class='kpi-sub'>Evaluated</div></div>",
    unsafe_allow_html=True,
)
col2.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>Resolved</div>"
    f"<div class='kpi-value'>{resolved}</div><div class='kpi-sub'>Fix verified</div></div>",
    unsafe_allow_html=True,
)
col3.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>Resolved Rate</div>"
    f"<div class='kpi-value'>{resolved_rate:.1f}%</div><div class='kpi-sub'>Resolved/Total</div></div>",
    unsafe_allow_html=True,
)
col4.markdown(
    f"<div class='kpi-card'><div class='kpi-label'>Patch Applied</div>"
    f"<div class='kpi-value'>{applied}</div><div class='kpi-sub'>Applied cleanly</div></div>",
    unsafe_allow_html=True,
)

st.markdown("<div class='chart-card'>", unsafe_allow_html=True)
st.markdown("<div class='chart-title'>Resolved by Run</div>", unsafe_allow_html=True)
run_counts = swe_df.groupby("run_id")["resolved"].sum().reset_index()
run_chart = (
    alt.Chart(run_counts)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("run_id:N", title="Run ID"),
        y=alt.Y("resolved:Q", title="Resolved"),
        color=alt.Color("run_id:N", scale=alt.Scale(scheme="tableau10")),
        tooltip=["run_id", "resolved"],
    )
    .properties(height=260)
)
st.altair_chart(run_chart, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div class='chart-card'>", unsafe_allow_html=True)
st.markdown("<div class='chart-title'>Resolved by Repository</div>", unsafe_allow_html=True)
repo_counts = swe_df.groupby("repo")["resolved"].sum().reset_index()
repo_chart = (
    alt.Chart(repo_counts)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("repo:N", title="Repository"),
        y=alt.Y("resolved:Q", title="Resolved"),
        color=alt.Color("repo:N", scale=alt.Scale(scheme="set2")),
        tooltip=["repo", "resolved"],
    )
    .properties(height=260)
)
st.altair_chart(repo_chart, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div class='chart-card'>", unsafe_allow_html=True)
st.markdown("<div class='chart-title'>Resolved Over Time</div>", unsafe_allow_html=True)
time_counts = (
    swe_df.groupby(pd.Grouper(key="run_ts", freq="D"))["resolved"]
    .sum()
    .reset_index()
)
time_chart = (
    alt.Chart(time_counts)
    .mark_area(line={"color": "#38bdf8"}, color="#0284c7", opacity=0.35)
    .encode(
        x=alt.X("run_ts:T", title="Run Date"),
        y=alt.Y("resolved:Q", title="Resolved"),
        tooltip=["run_ts:T", "resolved"],
    )
    .properties(height=240)
)
st.altair_chart(time_chart, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

with st.expander("Latest SWE-bench Results"):
    st.dataframe(
        swe_df.sort_values("run_ts", ascending=False)[
            ["instance_id", "repo", "resolved", "patch_applied", "run_id", "run_ts"]
        ]
    )
