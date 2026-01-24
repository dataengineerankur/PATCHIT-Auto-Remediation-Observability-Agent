from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from patchit.models import PatchProposal


@dataclass(frozen=True)
class VerificationResult:
    ok: bool
    summary: str
    output: str
    attempted_commands: List[str]


def _repo_root_path(repo_root: str) -> str:
    # Accept either absolute or relative-to-cwd repo roots.
    if os.path.isabs(repo_root):
        return repo_root
    return os.path.abspath(os.path.join(os.getcwd(), repo_root))


def _infer_verification_commands(
    *,
    patch: PatchProposal,
    duckdb_path: str | None,
    tmp_root: str,
    dst_repo: str,
    event: Dict[str, Any] | None = None,
    error: Dict[str, Any] | None = None,
    verify_cmd_rules_json: str | None = None,
) -> List[List[str]]:
    """
    Best-effort verification suite based on touched files.
    The goal is to catch obvious breakages and re-run relevant build steps (e.g. dbt).
    """
    paths = [f.path for f in patch.files]
    cmds: List[List[str]] = []

    # Always run a light Python syntax compile for any python changes.
    if any(p.endswith(".py") for p in paths):
        cmds.append(["python", "-m", "compileall", "-q", "repo"])

    # If PATCHIT core code changes, run unit tests (fast-ish).
    if any(p.startswith("patchit/") for p in paths):
        cmds.append(["pytest", "-q", "repo/tests/unit"])

    # -------- Spark harness (best-effort) --------
    # We can't run a full Spark cluster in the verifier, but we can at least run repo-provided tests if present.
    msg = ""
    try:
        msg = str((error or {}).get("message") or "")[:10_000]
    except Exception:
        msg = ""
    looks_spark = any(
        ("spark" in (p or "").lower())
        or (p or "").lower().endswith(".scala")
        or ("pyspark" in (p or "").lower())
        for p in paths
    ) or any(x in msg.lower() for x in ["sparkexception", "spark-submit", "pyspark", "spark:"])
    if looks_spark:
        spark_tests_dir = os.path.join(dst_repo, "tests", "spark")
        if os.path.isdir(spark_tests_dir):
            cmds.append(["pytest", "-q", "repo/tests/spark"])

    # -------- API harness (best-effort) --------
    # Same idea: if the repo provides integration tests, run them; otherwise avoid "fake passing".
    looks_api = any(x in msg.lower() for x in ["httperror", "requests.", "timeout", "connectionerror", "429", "502", "503"]) or any(
        x in (p or "").lower() for x in ["client", "api", "http"]
        for p in paths
    )
    if looks_api:
        it_dir = os.path.join(dst_repo, "tests", "integration")
        if os.path.isdir(it_dir):
            cmds.append(["pytest", "-q", "repo/tests/integration"])

    # -------- Config-driven extra commands --------
    # Allows extending verification for Spark/API/DBT/etc per-repo without changing PATCHIT core.
    # This is intentionally conservative: rules only ADD commands; they do not suppress built-ins.
    try:
        raw = json.loads(verify_cmd_rules_json) if verify_cmd_rules_json else None
    except Exception:
        raw = None
    if isinstance(raw, list):
        pipeline_id = None
        try:
            pipeline_id = (event or {}).get("pipeline_id")
        except Exception:
            pipeline_id = None
        for rule in raw:
            if not isinstance(rule, dict):
                continue
            when = rule.get("when") if isinstance(rule.get("when"), dict) else {}
            touched_re = when.get("touched_path_regex") if isinstance(when.get("touched_path_regex"), str) else None
            err_re = when.get("error_regex") if isinstance(when.get("error_regex"), str) else None
            pipe_re = when.get("pipeline_id_regex") if isinstance(when.get("pipeline_id_regex"), str) else None

            def _match(regex: str | None, text: str | None) -> bool:
                if not regex or not text:
                    return False
                import re

                try:
                    return re.search(regex, text) is not None
                except re.error:
                    return False

            ok = True
            if touched_re:
                ok = ok and any(_match(touched_re, p) for p in paths)
            if err_re:
                ok = ok and _match(err_re, msg)
            if pipe_re:
                ok = ok and _match(pipe_re, str(pipeline_id or ""))
            if not ok:
                continue

            cmds_spec = rule.get("cmds")
            if not isinstance(cmds_spec, list):
                continue
            for c in cmds_spec:
                if isinstance(c, str) and c.strip():
                    cmds.append(["bash", "-lc", c.strip()])

    return cmds


def _discover_dbt_project_dirs(*, tmp_root: str, repo_dir: str, touched_paths: list[str]) -> list[str]:
    """
    Discover dbt projects in a repo-agnostic way by walking upward from touched files
    until we find a `dbt_project.yml`.

    Returns project dirs as paths relative to `tmp_root` (so they can be used as --project-dir).
    Example return value: ["repo/dbt_retail", "repo/dbt_finance"].
    """
    out: list[str] = []
    seen: set[str] = set()

    repo_dir_abs = os.path.abspath(repo_dir)
    tmp_root_abs = os.path.abspath(tmp_root)

    for p in touched_paths:
        if not p:
            continue
        # Patch paths are repo-relative (e.g., "dbt_retail/models/...").
        abs_fp = os.path.abspath(os.path.join(repo_dir_abs, p))
        cur = abs_fp if os.path.isdir(abs_fp) else os.path.dirname(abs_fp)

        # Walk up until repo root.
        while True:
            if not cur.startswith(repo_dir_abs):
                break
            marker = os.path.join(cur, "dbt_project.yml")
            if os.path.exists(marker):
                rel = os.path.relpath(cur, tmp_root_abs)
                if rel not in seen:
                    seen.add(rel)
                    out.append(rel)
                break
            parent = os.path.dirname(cur)
            if parent == cur:
                break
            cur = parent

    return out


def _extract_duckdb_path(artifact_uris: List[str]) -> Optional[str]:
    for uri in artifact_uris:
        if not uri:
            continue
        if uri.startswith("file://"):
            p = uri[len("file://") :]
        elif uri.startswith("file:"):
            p = uri[len("file:") :]
        else:
            continue
        if p.endswith(".duckdb") and os.path.exists(p):
            return p
    return None


class SandboxVerifier:
    """
    Applies the unified diff inside a throwaway temp workspace and runs verification commands.

    NOTE: This is "sandbox-ish" isolation (separate workspace) but executes code locally.
    For Phase-1 MVP it is sufficient to validate patches before creating PRs.
    """

    def __init__(self, *, repo_root: str, timeout_s: float = 180.0) -> None:
        self.repo_root = repo_root
        self.timeout_s = float(timeout_s)

    def verify(
        self,
        *,
        patch: PatchProposal,
        artifact_uris: List[str],
        event: Dict[str, Any] | None = None,
        error: Dict[str, Any] | None = None,
        dbt_vars_json: str | None = None,
        replay_enabled: bool = True,
        converge_to_green: bool = False,
        runtime_verify_cmd: str | None = None,
        runtime_verify_required: bool = False,
        on_event: Callable[[str, Dict[str, Any]], None] | None = None,
        orchestration_runtime_required: bool = False,
        verify_cmd_rules_json: str | None = None,
        verify_extra_pythonpaths_json: str | None = None,
    ) -> VerificationResult:
        repo_src = _repo_root_path(self.repo_root)
        if not os.path.isdir(repo_src):
            return VerificationResult(
                ok=False,
                summary=f"repo_root_not_found: {repo_src}",
                output="",
                attempted_commands=[],
            )

        duckdb_path = _extract_duckdb_path(artifact_uris)

        tmp = tempfile.mkdtemp(prefix="patchit_sandbox_")
        attempted: List[str] = []
        out_chunks: List[str] = []
        try:
            def _emit(name: str, payload: Dict[str, Any]) -> None:
                if on_event is None:
                    return
                try:
                    on_event(name, payload)
                except Exception:
                    return

            def _run(cmd: List[str], *, label: str, cwd: str, env: Dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
                started = time.monotonic()
                _emit(
                    "verify.cmd_started",
                    {
                        "label": label,
                        "cmd": cmd,
                    },
                )
                p = subprocess.run(
                    cmd,
                    cwd=cwd,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=self.timeout_s,
                )
                dur = round(time.monotonic() - started, 3)
                tail = ((p.stdout or "") + "\n" + (p.stderr or ""))[-4000:]
                _emit(
                    "verify.cmd_finished",
                    {
                        "label": label,
                        "cmd": cmd,
                        "returncode": p.returncode,
                        "duration_s": dur,
                        "output_tail": tail,
                    },
                )
                return p

            # Create temp workspace with repo under ./repo so `patch -p1` works with `a/repo/...` paths.
            dst_repo = os.path.join(tmp, "repo")
            _copy_repo(repo_src, dst_repo)
            _link_top_level_paths(tmp_root=tmp, repo_dir=dst_repo)

            # Ensure profiles dir exists (inside repo tree for convenience).
            profiles_dir = os.path.join(dst_repo, "airflow", "data", "_patchit_verify_profiles")
            os.makedirs(profiles_dir, exist_ok=True)
            # Always provide a dbt profiles.yml for parse/build. Even `dbt parse` requires the project's configured
            # profile name to exist, but it doesn't need a live DB connection. We use a local duckdb profile for speed.
            profiles_duckdb = duckdb_path or os.path.join(profiles_dir, "patchit_verify.duckdb")
            # Collect profile names from any discovered dbt projects (profile field in dbt_project.yml).
            profile_names: list[str] = []
            try:
                import yaml  # type: ignore

                for root, _, files in os.walk(dst_repo):
                    if "dbt_project.yml" in files:
                        p = os.path.join(root, "dbt_project.yml")
                        try:
                            data = yaml.safe_load(open(p, "r", encoding="utf-8")) or {}
                            if isinstance(data, dict):
                                prof = (data.get("profile") or "").strip()
                                if prof:
                                    profile_names.append(prof)
                        except Exception:
                            continue
            except Exception:
                profile_names = []

            _write_dbt_profiles(profiles_dir=profiles_dir, duckdb_path=profiles_duckdb, profile_names=profile_names)

            # Apply patch
            # Write the diff inside the repo root and apply from there.
            # LLMs often emit repo-relative paths like `a/dbt_retail/...`; applying from `tmp/repo` makes those resolve.
            diff_path = os.path.join(dst_repo, "patch.diff")
            diff_text = patch.diff_unified or ""
            if diff_text and not diff_text.endswith("\n"):
                diff_text += "\n"
            diff_text = _canonicalize_diff_paths(diff_text)
            Path(diff_path).write_text(diff_text, encoding="utf-8")

            # Prefer git-style apply for unified diffs; initialize a temporary repo for consistent behavior.
            attempted.append("git init (sandbox)")
            _ = _run(["git", "init", "-q"], label="git init (sandbox)", cwd=dst_repo)

            # If the patch is already present in the current codebase, do NOT proceed with verification.
            # This prevents PATCHIT from opening redundant PRs after a fix was already merged/applied.
            attempted.append("git apply -R --check --unsafe-paths --whitespace=nowarn patch.diff (already-applied check)")
            already = _run(
                ["git", "apply", "-R", "--check", "--unsafe-paths", "--whitespace=nowarn", "patch.diff"],
                label="git apply -R --check (already-applied check)",
                cwd=dst_repo,
            )
            if already.returncode == 0:
                out_chunks.append("== already-applied check ==")
                out_chunks.append((already.stdout or "") + "\n" + (already.stderr or ""))
                return VerificationResult(
                    ok=False,
                    summary="patch_already_applied",
                    output="\n".join(out_chunks)[-30_000:],
                    attempted_commands=attempted,
                )

            attempted.append("git apply --unsafe-paths --whitespace=nowarn patch.diff")
            apply = _run(
                ["git", "apply", "--unsafe-paths", "--whitespace=nowarn", "patch.diff"],
                label="git apply patch.diff",
                cwd=dst_repo,
            )

            out_chunks.append("== git apply ==")
            out_chunks.append((apply.stdout or "") + "\n" + (apply.stderr or ""))
            if apply.returncode != 0:
                # Fallback: try GNU patch for slightly malformed diffs.
                attempted.append("patch -p1 --batch --forward < patch.diff (fallback)")
                p2 = _run(
                    ["patch", "-p1", "--batch", "--forward", "-i", "patch.diff"],
                    label="patch apply (fallback)",
                    cwd=dst_repo,
                )
                out_chunks.append("== patch apply (fallback) ==")
                out_chunks.append((p2.stdout or "") + "\n" + (p2.stderr or ""))
                if p2.returncode != 0:
                    return VerificationResult(
                        ok=False,
                        summary="patch_apply_failed",
                        output="\n".join(out_chunks)[-30_000:],
                        attempted_commands=attempted,
                    )

            # Run verification commands
            cmds = _infer_verification_commands(
                patch=patch,
                duckdb_path=duckdb_path,
                tmp_root=tmp,
                dst_repo=dst_repo,
                event=event,
                error=error,
                verify_cmd_rules_json=verify_cmd_rules_json,
            )

            # DBT verification (repo-agnostic): if patch touches a file inside a dbt project (dbt_project.yml),
            # run dbt parse (and build if we have a runnable profile via duckdb artifact).
            touched_paths = [f.path for f in (patch.files or [])]
            dbt_projects = _discover_dbt_project_dirs(tmp_root=tmp, repo_dir=dst_repo, touched_paths=touched_paths)
            for proj in dbt_projects:
                if duckdb_path:
                    cmds.append(
                        [
                            "dbt",
                            "build",
                            "--project-dir",
                            proj,
                            "--profiles-dir",
                            "repo/airflow/data/_patchit_verify_profiles",
                            *(
                                ["--vars", dbt_vars_json]
                                if isinstance(dbt_vars_json, str) and dbt_vars_json.strip()
                                else []
                            ),
                        ]
                    )
                else:
                    cmds.append(
                        [
                            "dbt",
                            "parse",
                            "--project-dir",
                            proj,
                            "--profiles-dir",
                            "repo/airflow/data/_patchit_verify_profiles",
                            *(
                                ["--vars", dbt_vars_json]
                                if isinstance(dbt_vars_json, str) and dbt_vars_json.strip()
                                else []
                            ),
                        ]
                    )

            for cmd in cmds:
                attempted.append(" ".join(cmd))
                env = dict(os.environ)
                # Make src-layout repos testable by default:
                # - include repo root (some projects import from repo/)
                # - include repo/src (common Python layout)
                # - allow injecting extra PYTHONPATH entries for multi-repo shared libs
                extra: list[str] = []
                try:
                    raw = json.loads(verify_extra_pythonpaths_json) if verify_extra_pythonpaths_json else None
                    if isinstance(raw, list):
                        for p in raw:
                            if isinstance(p, str) and p.strip():
                                extra.append(p.strip())
                except Exception:
                    extra = []
                py_parts = [
                    os.path.join(tmp, "repo"),
                    os.path.join(tmp, "repo", "src"),
                    *extra,
                ]
                # de-dupe while preserving order
                seen = set()
                py_parts = [p for p in py_parts if p and not (p in seen or seen.add(p))]
                env["PYTHONPATH"] = os.pathsep.join(py_parts)
                # Force dbt to use the sandbox profiles dir if present.
                env["DBT_PROFILES_DIR"] = os.path.join(tmp, "repo", "airflow", "data", "_patchit_verify_profiles")
                p = _run(cmd, label=" ".join(cmd), cwd=tmp, env=env)
                out_chunks.append(f"== {' '.join(cmd)} ==")
                out_chunks.append((p.stdout or "") + "\n" + (p.stderr or ""))
                if p.returncode != 0:
                    return VerificationResult(
                        ok=False,
                        summary=f"verify_failed: {' '.join(cmd)}",
                        output="\n".join(out_chunks)[-30_000:],
                        attempted_commands=attempted,
                    )

            # Optional: runtime-faithful verification command (e.g., docker compose Airflow task run).
            # This is the recommended “true sandbox fidelity” hook.
            if runtime_verify_required and not runtime_verify_cmd:
                out_chunks.append("== runtime_verify_cmd ==")
                out_chunks.append(
                    (
                        "runtime_verify_cmd is required but is not configured.\n"
                        + (
                            "This patch touches orchestration code (Airflow DAGs / triggers), so runtime verification is mandatory.\n"
                            if orchestration_runtime_required
                            else ""
                        )
                        + "Set PATCHIT_RUNTIME_VERIFY_CMD and ensure the verifier can run it (e.g., mount docker socket + docker cli)."
                    )
                )
                return VerificationResult(
                    ok=False,
                    summary="runtime_verify_required_missing",
                    output="\n".join(out_chunks)[-30_000:],
                    attempted_commands=attempted,
                )

            if runtime_verify_cmd:
                attempted.append("runtime_verify_cmd")
                cmd_str = (runtime_verify_cmd or "").replace("{sandbox}", tmp).replace("{repo}", os.path.join(tmp, "repo"))
                p_rt = _run(["bash", "-lc", cmd_str], label="runtime_verify_cmd", cwd=tmp)
                out_chunks.append("== runtime_verify_cmd ==")
                out_chunks.append((p_rt.stdout or "") + "\n" + (p_rt.stderr or ""))
                if p_rt.returncode != 0 and runtime_verify_required:
                    return VerificationResult(
                        ok=False,
                        summary="runtime_verify_failed",
                        output="\n".join(out_chunks)[-30_000:],
                        attempted_commands=attempted,
                    )

            # Optional: replay the failing function in the sandbox (task-agnostic for Python failures).
            if replay_enabled and event and error:
                attempted.append("python repro_replay.py repro_payload.json")
                repro_payload = {"event": event, "error": error, "artifact_uris": artifact_uris}
                Path(os.path.join(tmp, "repro_payload.json")).write_text(
                    __import__("json").dumps(repro_payload, indent=2),
                    encoding="utf-8",
                )
                Path(os.path.join(tmp, "repro_replay.py")).write_text(_REPRO_REPLAY_SCRIPT, encoding="utf-8")
                env = dict(os.environ)
                # Match Airflow's import behavior: it puts DAGS_FOLDER on sys.path.
                env["PYTHONPATH"] = os.pathsep.join(
                    [
                        os.path.join(tmp, "repo"),
                        os.path.join(tmp, "repo", "src"),
                        os.path.join(tmp, "repo", "airflow", "dags"),
                        os.path.join(tmp, "repo", "airflow_extra_dags"),
                    ]
                )
                p = _run(
                    ["python", "repro_replay.py", "repro_payload.json"],
                    label="python repro_replay.py repro_payload.json",
                    cwd=tmp,
                    env=env,
                )
                out_chunks.append("== python repro_replay.py repro_payload.json ==")
                out_chunks.append((p.stdout or "") + "\n" + (p.stderr or ""))
                if p.returncode != 0:
                    # Best-effort only: replay is useful when it works, but is NOT a gating signal
                    # for verification in the general (pipeline-agnostic) case.
                    #
                    # Reasons replay can fail while a patch is still correct:
                    # - Missing artifacts in the sandbox (mount/path mismatch, upstream task never ran)
                    # - Runtime-only args required (Airflow context, external services)
                    # - The failing function is not directly replayable (operator wrappers, closures)
                    #
                    # In converge-to-green mode we still want strictness elsewhere (chaos suite),
                    # but replay itself should remain informational.
                    out_chunks.append(
                        "== replay skipped ==\nreplay failed (best-effort). Continuing verification; see traceback above."
                    )

            # Converge-to-green: run a deterministic suite for the chaos DAG helpers and require no exceptions.
            if converge_to_green and event and (event.get("pipeline_id") in ("patchit_chaos_dag", "patchit_complex_test_dag")):
                attempted.append("python chaos_green_suite.py")
                Path(os.path.join(tmp, "chaos_green_suite.py")).write_text(_CHAOS_GREEN_SUITE_SCRIPT, encoding="utf-8")
                env = dict(os.environ)
                env["PYTHONPATH"] = os.pathsep.join(
                    [
                        os.path.join(tmp, "repo"),
                        os.path.join(tmp, "repo", "airflow", "dags"),
                        os.path.join(tmp, "repo", "airflow_extra_dags"),
                    ]
                )
                p = _run(["python", "chaos_green_suite.py"], label="python chaos_green_suite.py", cwd=tmp, env=env)
                out_chunks.append("== python chaos_green_suite.py ==")
                out_chunks.append((p.stdout or "") + "\n" + (p.stderr or ""))
                if p.returncode != 0:
                    return VerificationResult(
                        ok=False,
                        summary="converge_suite_failed",
                        output="\n".join(out_chunks)[-30_000:],
                        attempted_commands=attempted,
                    )

            return VerificationResult(
                ok=True,
                summary="verified",
                output="\n".join(out_chunks)[-30_000:],
                attempted_commands=attempted,
            )
        except subprocess.TimeoutExpired as e:
            out_chunks.append(f"== timeout ==\n{e}")
            return VerificationResult(
                ok=False,
                summary="verify_timeout",
                output="\n".join(out_chunks)[-30_000:],
                attempted_commands=attempted,
            )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


def _copy_repo(src: str, dst: str) -> None:
    def _ignore(_dir: str, names: List[str]) -> set[str]:
        # Keep it lean while preserving code needed for verification.
        ignore = {
            ".git",
            ".venv",
            "venv",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            ".mock_github",
            "var",  # audit logs
            "patchit_var",  # host-mounted runtime state in postgres-etl-pipeline (worktrees/audit/etc)
            "worktrees",  # extra safety: avoid copying nested git worktrees (may contain symlinks)
            "pgdata",  # docker-compose postgres volume (can be huge)
        }
        # Don't copy Airflow logs (huge). We also don't need Airflow data; dbt build points to real duckdb path.
        if _dir.endswith(os.path.join("airflow", "logs")):
            return set(names)
        if _dir.endswith(os.path.join("airflow")):
            ignore |= {"logs", "data", "pgdata", "patchit_var"}
        return {n for n in names if n in ignore}

    shutil.copytree(src, dst, dirs_exist_ok=True, ignore=_ignore)


def _write_dbt_profiles(*, profiles_dir: str, duckdb_path: str, profile_names: list[str] | None = None) -> None:
    profiles_yml = os.path.join(profiles_dir, "profiles.yml")
    names = [n for n in (profile_names or []) if isinstance(n, str) and n.strip()]
    # Always include a fallback profile name for internal use.
    if "patchit" not in names:
        names.append("patchit")
    # De-dupe keep order
    seen = set()
    names = [n for n in names if not (n in seen or seen.add(n))]

    blocks: list[str] = []
    for name in names:
        blocks.append(
            f"""
{name}:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: {duckdb_path}
      threads: 1
""".lstrip()
        )
    Path(profiles_yml).write_text("\n".join(blocks).rstrip() + "\n", encoding="utf-8")


def _link_top_level_paths(*, tmp_root: str, repo_dir: str) -> None:
    """
    Make sandbox robust to diffs that reference either:
      - repo/<path> (our preferred convention), OR
      - <path> (top-level), as many LLMs emit.

    We create symlinks at tmp_root/<top> -> tmp_root/repo/<top>.
    """
    for name in ("airflow", "airflow_extra_dags", "patchit", "dbt", "mock_api", "tests"):
        src = os.path.join(repo_dir, name)
        dst = os.path.join(tmp_root, name)
        if os.path.exists(dst):
            continue
        if os.path.exists(src):
            os.symlink(src, dst)

    # Also link common single files
    for fname in ("README.md",):
        src = os.path.join(repo_dir, fname)
        dst = os.path.join(tmp_root, fname)
        if os.path.exists(dst):
            continue
        if os.path.exists(src):
            os.symlink(src, dst)

    # Canonicalize Airflow's mounted extra_dags path inside the sandbox too.
    # Map repo/airflow/dags/extra_dags -> repo/airflow_extra_dags when possible.
    try:
        airflow_dags = os.path.join(repo_dir, "airflow", "dags")
        extra_target = os.path.join(airflow_dags, "extra_dags")
        extra_src = os.path.join(repo_dir, "airflow_extra_dags")
        if os.path.exists(extra_src):
            os.makedirs(airflow_dags, exist_ok=True)
            if not os.path.exists(extra_target):
                os.symlink(extra_src, extra_target)
    except Exception:
        # Best-effort only; sandbox should still work for canonical paths.
        pass


_REPRO_REPLAY_SCRIPT = r'''
import importlib.util
import inspect
import json
import os
import re
import sys
import traceback


def _uri_to_path(u: str) -> str | None:
    if not u:
        return None
    if u.startswith("file://"):
        return u[len("file://"):]
    if u.startswith("file:"):
        return u[len("file:"):]
    return None


def _pick_artifact(artifact_uris: list[str], suffix: str) -> str | None:
    for u in artifact_uris or []:
        p = _uri_to_path(u)
        if p and p.endswith(suffix) and os.path.exists(p):
            return p
    return None


def _map_container_path_to_sandbox_repo(fp: str) -> str | None:
    # fp may be like /opt/airflow/dags/extra_dags/chaos_lib/steps.py
    if not fp:
        return None
    if fp.startswith("/opt/airflow/dags/extra_dags/"):
        rel = fp[len("/opt/airflow/dags/extra_dags/"):]
        return os.path.join(os.getcwd(), "repo", "airflow_extra_dags", rel)
    if fp.startswith("/opt/airflow/dags/"):
        rel = fp[len("/opt/airflow/dags/"):]
        return os.path.join(os.getcwd(), "repo", "airflow", "dags", rel)
    if fp.startswith("/opt/patchit/repo/"):
        rel = fp[len("/opt/patchit/repo/"):]
        return os.path.join(os.getcwd(), "repo", rel)
    return None


def _load_module_from_path(path: str):
    spec = importlib.util.spec_from_file_location("patchit_repro_mod", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot_load_module: {path}")
    mod = importlib.util.module_from_spec(spec)
    # Critical: register module before exec_module so decorators (e.g. dataclasses)
    # can resolve sys.modules[__module__] during import-time evaluation.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


def _extract_missing_path(msg: str) -> str | None:
    # "[Errno 2] No such file or directory: '/opt/airflow/data/...txt'"
    m = re.search(r"No such file or directory: '([^']+)'", msg or "")
    return m.group(1) if m else None


def main():
    payload_path = sys.argv[1]
    data = json.load(open(payload_path, "r", encoding="utf-8"))
    err = data.get("error") or {}
    artifact_uris = data.get("artifact_uris") or []
    stack = (err.get("stack") or [])
    # Find innermost frame in our repo
    target = None
    for fr in reversed(stack):
        fp = fr.get("file_path") or ""
        sandbox_fp = _map_container_path_to_sandbox_repo(fp)
        if sandbox_fp and os.path.exists(sandbox_fp):
            target = (sandbox_fp, fr.get("function"))
            break
    if not target:
        print("no_replay_target")
        return 0

    sandbox_fp, func_name = target
    if not func_name:
        print("no_function_name")
        return 0

    mod = _load_module_from_path(sandbox_fp)
    fn = getattr(mod, func_name, None)
    if fn is None or not callable(fn):
        print("function_not_found")
        return 0

    sig = None
    try:
        sig = inspect.signature(fn)
    except Exception:
        sig = None

    kwargs = {}
    if sig:
        for pname in sig.parameters.keys():
            if pname in ("path", "final_path"):
                missing = _extract_missing_path(err.get("message") or "")
                # Only pass a missing path if it actually exists in this runtime.
                # Many pipeline failures are caused by upstream artifacts not being present
                # (or not mounted into the verifier container). In those cases, passing
                # the path forces a FileNotFoundError which is not helpful for replay.
                if missing and os.path.exists(missing):
                    kwargs[pname] = missing
            if pname in ("raw_path",):
                p = _pick_artifact(artifact_uris, ".json")
                if p:
                    kwargs[pname] = p
            if pname in ("schema_path",):
                # Best-effort: repo schema location
                cand = os.path.join(os.getcwd(), "repo", "airflow", "dags", "schemas", "api_payload.schema.json")
                if os.path.exists(cand):
                    kwargs[pname] = cand
            if pname in ("payload",):
                p = _pick_artifact(artifact_uris, ".json")
                if p and os.path.exists(p):
                    kwargs[pname] = json.load(open(p, "r", encoding="utf-8"))

    try:
        fn(**kwargs) if kwargs else fn()
        return 0
    except Exception:
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
'''


_CHAOS_GREEN_SUITE_SCRIPT = r'''
import json
import os
import tempfile

from airflow_extra_dags.chaos_lib import steps


def main() -> int:
    # Ensure basic helpers do not raise
    run_id = "patchit_converge_suite"
    art = steps.make_paths(run_id)
    os.makedirs(os.path.dirname(art.raw_path), exist_ok=True)
    os.makedirs(os.path.dirname(art.out_path), exist_ok=True)

    # write_input_json / read_json should succeed
    steps.write_input_json(path=art.raw_path, payload={"run_id": run_id, "ok": True})
    _ = steps.read_json(art.raw_path)

    # write_malformed_json should NOT poison the system in converge mode: read_json should still not crash.
    bad_path = os.path.join(os.path.dirname(art.raw_path), "bad.json")
    steps.write_malformed_json(path=bad_path)
    try:
        _ = steps.read_json(bad_path)
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"read_json must not raise in converge_to_green mode: {e}") from e

    # do_key_error should not raise
    try:
        _ = steps.do_key_error({"run_id": run_id})
    except Exception as e:
        raise RuntimeError(f"do_key_error must not raise: {e}") from e

    # do_file_not_found should not raise (use an existing temp file)
    fd, p = tempfile.mkstemp()
    os.close(fd)
    with open(p, "w", encoding="utf-8") as f:
        f.write("ok")
    try:
        _ = steps.do_file_not_found(p)
    except Exception as e:
        raise RuntimeError(f"do_file_not_found must not raise: {e}") from e

    # do_type_error / do_attribute_error / do_division_by_zero should not raise
    for fn in (steps.do_type_error, steps.do_attribute_error, steps.do_division_by_zero):
        try:
            _ = fn()
        except Exception as e:
            raise RuntimeError(f"{fn.__name__} must not raise: {e}") from e

    # write_result should succeed
    steps.write_result(art.out_path, {"ok": True})
    json.load(open(art.out_path, "r", encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
'''


def _canonicalize_diff_paths(diff: str) -> str:
    if not diff:
        return diff
    repls = [
        ("a/repo/airflow/dags/extra_dags/", "a/repo/airflow_extra_dags/"),
        ("b/repo/airflow/dags/extra_dags/", "b/repo/airflow_extra_dags/"),
        ("--- a/repo/airflow/dags/extra_dags/", "--- a/repo/airflow_extra_dags/"),
        ("+++ b/repo/airflow/dags/extra_dags/", "+++ b/repo/airflow_extra_dags/"),
        ("diff --git a/repo/airflow/dags/extra_dags/", "diff --git a/repo/airflow_extra_dags/"),
        (" b/repo/airflow/dags/extra_dags/", " b/repo/airflow_extra_dags/"),
    ]
    out = diff
    for a, b in repls:
        out = out.replace(a, b)
    return out


