from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


@dataclass(frozen=True)
class ArtifactSummary:
    uri: str
    kind: str
    ok: bool
    summary: str
    details: Optional[Dict[str, Any]] = None


def _read_file_limited(path: str, max_bytes: int) -> str:
    with open(path, "rb") as f:
        data = f.read(max_bytes + 1)
    if len(data) > max_bytes:
        data = data[:max_bytes]
    return data.decode("utf-8", errors="replace")


def summarize_artifacts(artifact_uris: List[str], *, max_bytes: int = 20_000) -> List[ArtifactSummary]:
    """
    Safety: reads are size-limited; only file:// and local paths are supported in Phase-1.
    """
    out: List[ArtifactSummary] = []
    for uri in artifact_uris:
        parsed = urlparse(uri)
        path = parsed.path if parsed.scheme == "file" else uri
        if parsed.scheme not in ("", "file"):
            out.append(ArtifactSummary(uri=uri, kind="unsupported", ok=False, summary=f"unsupported scheme: {parsed.scheme}"))
            continue
        if not os.path.exists(path):
            out.append(ArtifactSummary(uri=uri, kind="missing", ok=False, summary="artifact not found"))
            continue

        text = _read_file_limited(path, max_bytes=max_bytes)
        lower = path.lower()
        if lower.endswith(".json"):
            try:
                obj = json.loads(text)
                keys = sorted(list(obj.keys()))[:20] if isinstance(obj, dict) else None
                out.append(
                    ArtifactSummary(
                        uri=uri,
                        kind="json",
                        ok=True,
                        summary="json_ok",
                        details={"top_level_type": type(obj).__name__, "keys": keys},
                    )
                )
            except json.JSONDecodeError as e:
                out.append(
                    ArtifactSummary(
                        uri=uri,
                        kind="json",
                        ok=False,
                        summary=f"json_decode_error: {e}",
                        details={"head": text[:200]},
                    )
                )
            continue

        if lower.endswith(".csv"):
            head = "\n".join(text.splitlines()[:5])
            out.append(ArtifactSummary(uri=uri, kind="csv", ok=True, summary="csv_head", details={"head": head}))
            continue

        out.append(ArtifactSummary(uri=uri, kind="text", ok=True, summary="text_head", details={"head": text[:200]}))

    return out



