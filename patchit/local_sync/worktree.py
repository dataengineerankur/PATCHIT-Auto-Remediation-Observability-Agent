from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass


@dataclass(frozen=True)
class LocalSyncResult:
    ok: bool
    worktree_path: str | None = None
    branch: str | None = None
    stage: str | None = None
    returncode: int | None = None
    output: str | None = None


def _fmt_cmd(template: str, *, repo: str, sandbox: str, patch_id: str, pipeline_id: str | None, task_id: str | None) -> str:
    # Support both:
    # - {patch_id} (full)
    # - {patch_id:0:8} (common “short id” convenience)
    out = (template or "").replace("{repo}", repo).replace("{sandbox}", sandbox).replace("{patch_id}", patch_id)
    out = out.replace("{patch_id:0:8}", (patch_id or "")[:8])
    if pipeline_id:
        out = out.replace("{pipeline_id}", pipeline_id)
    if task_id:
        out = out.replace("{task_id}", task_id)
    return out


def _normalize_unified_diff_for_worktree(diff_text: str) -> str:
    """
    PATCHIT commonly produces diffs that reference files under `repo/...` because the
    target repository is mounted at `/opt/patchit/repo` in containers.

    A git worktree checkout does NOT contain a `repo/` directory; it's already the repo root.
    To apply the patch to a worktree, we rewrite diff headers to strip the leading `repo/`.
    """
    if not diff_text:
        return ""

    # Basic sanitization: strip markdown fences/sentinels and normalize CRLF.
    # (LLMs sometimes wrap diffs in ```diff fences; those can break git apply.)
    raw_lines = []
    for ln in (diff_text or "").splitlines():
        s = (ln or "").rstrip("\r")
        if s.strip().startswith("```"):
            continue
        if s.startswith("*** Begin Patch") or s.startswith("*** End Patch"):
            continue
        if s.startswith("*** End of File"):
            continue
        raw_lines.append(s)

    s = "\n".join(raw_lines).strip()
    if not s:
        return ""

    # Ensure the diff starts at the first recognizable header line.
    for marker in ("diff --git ", "--- a/"):
        idx = s.find(marker)
        if idx != -1:
            s = s[idx:].lstrip()
            break

    out_lines: list[str] = []
    for line in s.splitlines():
        # Support common unified diff header forms
        line = line.replace("diff --git a/repo/", "diff --git a/").replace("diff --git b/repo/", "diff --git b/")
        line = line.replace("--- a/repo/", "--- a/").replace("+++ b/repo/", "+++ b/")
        line = line.replace("--- repo/", "--- ").replace("+++ repo/", "+++ ")
        out_lines.append(line)

    out = "\n".join(out_lines)
    # Ensure final newline so patch/git apply don't complain.
    if s.endswith("\n") and not out.endswith("\n"):
        out += "\n"
    if out and not out.endswith("\n"):
        out += "\n"
    return out


def apply_patch_to_worktree(
    *,
    repo_path: str,
    worktree_root: str,
    patch_id: str,
    diff_text: str,
    base_ref: str = "HEAD",
    validate_cmd: str | None = None,
    validate_timeout_s: float | None = 420.0,
    pipeline_id: str | None = None,
    task_id: str | None = None,
) -> LocalSyncResult:
    """
    Create a git worktree and apply the diff there. Designed for “no merge/pull loop”.

    - Does NOT change the current working tree.
    - Uses `git worktree add -b <branch> <path> <base_ref>`.
    """
    repo_path = os.path.abspath(repo_path)
    worktree_root = os.path.abspath(worktree_root)
    os.makedirs(worktree_root, exist_ok=True)

    if not os.path.isdir(os.path.join(repo_path, ".git")):
        return LocalSyncResult(ok=False, output=f"local_sync_repo_not_git: {repo_path}")

    branch = f"patchit/local/{patch_id[:8]}"
    wt_path = os.path.join(worktree_root, branch.replace("/", "__"))

    # Recreate worktree path (idempotent)
    if os.path.exists(wt_path):
        try:
            subprocess.run(["git", "worktree", "remove", "--force", wt_path], cwd=repo_path, check=False, capture_output=True, text=True)
        except Exception:
            pass
        try:
            shutil.rmtree(wt_path, ignore_errors=True)
        except Exception:
            pass

    out: list[str] = []
    try:
        # Make branch creation idempotent (git worktree add -b fails if branch exists)
        try:
            subprocess.run(["git", "branch", "-D", branch], cwd=repo_path, check=False, capture_output=True, text=True)
        except Exception:
            pass

        p = subprocess.run(
            ["git", "worktree", "add", "-b", branch, wt_path, base_ref],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=False,
        )
        out.append("== git worktree add ==")
        out.append((p.stdout or "") + "\n" + (p.stderr or ""))
        if p.returncode != 0:
            return LocalSyncResult(
                ok=False,
                worktree_path=wt_path,
                branch=branch,
                stage="git_worktree_add",
                returncode=p.returncode,
                output="\n".join(out),
            )

        # Write diff to a temp file and apply
        td = tempfile.mkdtemp(prefix="patchit_local_patch_")
        try:
            patch_fp = os.path.join(td, "patch.diff")
            with open(patch_fp, "w", encoding="utf-8") as f:
                normalized = _normalize_unified_diff_for_worktree(diff_text or "")
                f.write(normalized)
                if normalized and not normalized.endswith("\n"):
                    f.write("\n")

            p2 = subprocess.run(
                ["git", "apply", "--unsafe-paths", "--whitespace=nowarn", patch_fp],
                cwd=wt_path,
                capture_output=True,
                text=True,
                check=False,
            )
            out.append("== git apply (worktree) ==")
            out.append((p2.stdout or "") + "\n" + (p2.stderr or ""))
            if p2.returncode != 0:
                # `git apply` is stricter than `patch`; Cursor-generated diffs sometimes fail git apply
                # but are still valid unified patches. Mirror sandbox verifier behavior here.
                p2b = subprocess.run(
                    ["patch", "-p1", "--batch", "--forward", "-i", patch_fp],
                    cwd=wt_path,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                out.append("== patch apply (worktree fallback) ==")
                out.append((p2b.stdout or "") + "\n" + (p2b.stderr or ""))
                if p2b.returncode != 0:
                    return LocalSyncResult(
                        ok=False,
                        worktree_path=wt_path,
                        branch=branch,
                        stage="git_apply_worktree",
                        returncode=p2.returncode,
                        output="\n".join(out),
                    )

            # Optional local validation command (best effort gating if provided)
            if validate_cmd:
                cmd = _fmt_cmd(
                    validate_cmd,
                    repo=wt_path,
                    sandbox=wt_path,
                    patch_id=patch_id,
                    pipeline_id=pipeline_id,
                    task_id=task_id,
                )
                try:
                    p3 = subprocess.run(
                        ["bash", "-lc", cmd],
                        cwd=wt_path,
                        capture_output=True,
                        text=True,
                        check=False,
                        timeout=(float(validate_timeout_s) if validate_timeout_s else None),
                    )
                except subprocess.TimeoutExpired as e:
                    out.append("== local_validate_cmd ==")
                    out.append(f"local_validate_timeout_s={validate_timeout_s}")
                    # Best-effort capture of partial output
                    try:
                        so = (e.stdout.decode("utf-8", errors="replace") if isinstance(e.stdout, (bytes, bytearray)) else (e.stdout or ""))
                        se = (e.stderr.decode("utf-8", errors="replace") if isinstance(e.stderr, (bytes, bytearray)) else (e.stderr or ""))
                        out.append((so or "") + "\n" + (se or ""))
                    except Exception:
                        pass

                    # Best-effort cleanup for our standard validation stacks.
                    # This assumes the validate_cmd uses PROJ="patchit_val_{patch_id:0:8}" and produces patchit_validate.compose.yml.
                    proj = f"patchit_val_{(patch_id or '')[:8]}"
                    compose_fp = os.path.join(wt_path, "patchit_validate.compose.yml")
                    if os.path.exists(compose_fp):
                        try:
                            p4 = subprocess.run(
                                ["docker-compose", "-p", proj, "-f", compose_fp, "down", "-v", "--remove-orphans"],
                                cwd=wt_path,
                                capture_output=True,
                                text=True,
                                check=False,
                                timeout=60,
                            )
                            out.append("== local_validate_cleanup ==")
                            out.append((p4.stdout or "") + "\n" + (p4.stderr or ""))
                        except Exception as ce:  # noqa: BLE001
                            out.append(f"local_validate_cleanup_failed: {ce}")

                    return LocalSyncResult(
                        ok=False,
                        worktree_path=wt_path,
                        branch=branch,
                        stage="local_validate_timeout",
                        returncode=124,
                        output="\n".join(out),
                    )
                out.append(f"== local_validate_cmd ==")
                out.append((p3.stdout or "") + "\n" + (p3.stderr or ""))
                if p3.returncode != 0:
                    # In many demo stacks PATCHIT runs inside a container without access to a Docker daemon,
                    # so validate_cmds that invoke docker/docker-compose cannot run here.
                    # Treat "command not found" as a soft-skip (do not block PR), but keep the output for humans.
                    so = (p3.stdout or "").lower()
                    se = (p3.stderr or "").lower()
                    cmd_not_found = (p3.returncode == 127) or ("command not found" in so) or ("command not found" in se)
                    missing_dockerish = cmd_not_found and (
                        ("docker-compose" in so)
                        or ("docker-compose" in se)
                        or ("docker compose" in so)
                        or ("docker compose" in se)
                        or ("docker:" in se)
                        or ("docker:" in so)
                    )
                    if missing_dockerish:
                        out.append("== local_validate_skipped ==")
                        out.append(
                            "local_validate_cmd could not run because docker/docker-compose is unavailable in this runtime. "
                            "Skipping local runtime validation (PR not blocked)."
                        )
                        return LocalSyncResult(
                            ok=True,
                            worktree_path=wt_path,
                            branch=branch,
                            stage="ok_validate_skipped_missing_docker",
                            returncode=p3.returncode,
                            output="\n".join(out),
                        )
                    return LocalSyncResult(
                        ok=False,
                        worktree_path=wt_path,
                        branch=branch,
                        stage="local_validate_cmd",
                        returncode=p3.returncode,
                        output="\n".join(out),
                    )

            stage = "ok"
            if p2.returncode != 0:
                stage = "ok_patch_fallback"
            return LocalSyncResult(ok=True, worktree_path=wt_path, branch=branch, stage=stage, returncode=0, output="\n".join(out))
        finally:
            shutil.rmtree(td, ignore_errors=True)
    except Exception as e:  # noqa: BLE001
        out.append(f"local_sync_exception: {e}")
        return LocalSyncResult(ok=False, worktree_path=wt_path, branch=branch, stage="exception", returncode=None, output="\n".join(out))


