# PATCHIT (product-only) deployment

This folder contains a **PATCHIT-only** `docker-compose` so you can run PATCHIT without starting Airflow/dbt demo services.

## First-time setup (fresh laptop)

```bash
git clone <your-repo>
cd PATCHIT-Auto-Remediation-Observability-Agent

# Create local env (DO NOT commit it)
cp deploy/patchit.env.example deploy/patchit.env

# Start PATCHIT (no Airflow)
docker compose -f deploy/docker-compose.patchit.yml --env-file deploy/patchit.env up -d --build
```

Open:
- UI: `http://localhost:8089/ui`
- API: `http://localhost:8088/docs`

If those ports are already in use (e.g. you're also running the demo Airflow stack),
edit `deploy/patchit.env` and set:
- `PATCHIT_PORT=18088`
- `PATCHIT_UI_PORT=18089`
- `PATCHIT_AGENT_PORT=18098`

## Keeping / losing history

PATCHIT stores history in `var/` (audit/evidence/reports/memory).

- **Fresh laptop, no history**: do nothing (empty `var/` ⇒ empty UI).
- **Keep history across restarts**: this compose mounts `../var` into the container, so history persists locally.
- **Move history to another laptop**: copy the `var/` folder to the same repo path on the new laptop.

## Local-only network exposure

Ports are bound to `127.0.0.1` only, so PATCHIT is accessible **only from the local machine** (safe for VPN/laptop use).

