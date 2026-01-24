from __future__ import annotations

import json
import os
import html
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class MockPr:
    pr_number: int
    title: str
    body: str
    branch: str
    repo: str
    diff_text: str


def load_mock_pr(mock_root: str, pr_number: int) -> MockPr:
    pr_dir = os.path.join(mock_root, "prs")
    meta_path = os.path.join(pr_dir, f"{pr_number}.json")
    if not os.path.exists(meta_path):
        raise FileNotFoundError(f"mock PR metadata not found: {meta_path}")

    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    diff_path = meta.get("diff_path")
    diff_text = ""
    if diff_path and os.path.exists(diff_path):
        with open(diff_path, "r", encoding="utf-8", errors="replace") as f:
            diff_text = f.read()

    return MockPr(
        pr_number=int(meta.get("pr_number", pr_number)),
        title=str(meta.get("title", "")),
        body=str(meta.get("body", "")),
        branch=str(meta.get("branch", "")),
        repo=str(meta.get("repo", "")),
        diff_text=diff_text,
    )


def render_mock_pr_html(pr: MockPr) -> str:
    body = pr.body or ""
    diff = pr.diff_text or ""
    diff_html = render_diff_html(diff)
    return f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Mock PR #{pr.pr_number}</title>
    <style>
      body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto; margin: 16px; }}
      .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas; }}
      .pill {{ font-size: 12px; padding: 2px 8px; border-radius: 999px; background: #eee; }}
      pre {{ background: #0b1020; color: #e5e7eb; padding: 12px; border-radius: 8px; overflow: auto; }}
      .diff {{ background: #0b1020; border-radius: 8px; padding: 12px; overflow:auto; }}
      .line {{ white-space: pre; }}
      .add {{ background: rgba(46, 160, 67, 0.18); color: #d1fae5; }}
      .del {{ background: rgba(248, 81, 73, 0.18); color: #fee2e2; }}
      .hunk {{ color: #93c5fd; }}
      .meta {{ color: #a1a1aa; }}
      .file {{ color: #fbbf24; }}
      a {{ color: #2563eb; text-decoration: none; }}
      .row {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }}
    </style>
  </head>
  <body>
    <div class="row">
      <h2 style="margin:0">Mock PR #{pr.pr_number}: {pr.title}</h2>
      <span class="pill">mode=mock</span>
      <a href="/ui" target="_blank">PATCHIT Event Console</a>
    </div>
    <p><b>repo</b>: <span class="mono">{pr.repo}</span> &nbsp; <b>branch</b>: <span class="mono">{pr.branch}</span></p>

    <h3>Description</h3>
    <pre class="mono">{body}</pre>

    <h3>Proposed diff</h3>
    <div class="diff mono">{diff_html}</div>
  </body>
</html>"""


def render_diff_html(diff_text: str) -> str:
    lines = diff_text.splitlines()
    out = []
    for ln in lines:
        esc = html.escape(ln)
        cls = "line"
        if ln.startswith("+++ ") or ln.startswith("--- ") or ln.startswith("diff --git"):
            cls += " file"
        elif ln.startswith("@@"):
            cls += " hunk"
        elif ln.startswith("+") and not ln.startswith("+++"):
            cls += " add"
        elif ln.startswith("-") and not ln.startswith("---"):
            cls += " del"
        elif ln.startswith("index ") or ln.startswith("new file") or ln.startswith("deleted file"):
            cls += " meta"
        out.append(f'<div class="{cls}">{esc}</div>')
    return "\n".join(out)

