# PATCHIT Observability Dashboard

This is a lightweight Streamlit dashboard that reads PATCHIT audit logs and evidence files to show remediation KPIs.

## Run
```bash
cd /Users/ankurchopra/repo_projects/PATCHIT-Auto-Remediation-Observability-Agent/observability_dashboard
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m streamlit run app.py --server.address 127.0.0.1 --server.port 8504
```

The dashboard reads:
- `var/audit/patchit_audit.jsonl`
- `var/evidence/*.json`
- `var/reports/*.md`
