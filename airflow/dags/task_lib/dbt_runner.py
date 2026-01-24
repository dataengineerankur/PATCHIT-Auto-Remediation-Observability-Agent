from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DbtResult:
    ok: bool
    stdout: str


def run_dbt_build(*, project_dir: str, profiles_dir: str, duckdb_path: str) -> DbtResult:
    """
    Runs `dbt build` against DuckDB at `duckdb_path`.
    Writes a dynamic profiles.yml into profiles_dir.
    """
    Path(profiles_dir).mkdir(parents=True, exist_ok=True)
    profiles_yml = os.path.join(profiles_dir, "profiles.yml")
    with open(profiles_yml, "w", encoding="utf-8") as f:
        f.write(
            f"""
patchit:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: {duckdb_path}
      threads: 1
"""
        )

    env = dict(os.environ)
    env["DBT_PROFILES_DIR"] = profiles_dir

    p = subprocess.run(
        ["dbt", "build", "--project-dir", project_dir, "--profiles-dir", profiles_dir],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    out = (p.stdout or "") + "\n" + (p.stderr or "")
    if p.returncode != 0:
        # Let Airflow capture stack trace; include dbt output in exception message (important for PATCHIT parsing)
        raise RuntimeError(f"dbt build failed (rc={p.returncode}). Output:\n{out}")
    return DbtResult(ok=True, stdout=out)



