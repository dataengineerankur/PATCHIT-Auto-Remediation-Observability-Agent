from __future__ import annotations

import json

from patchit.context.artifacts import summarize_artifacts


def test_summarize_artifacts_detects_bad_json(tmp_path) -> None:
    bad = tmp_path / "payload.json"
    bad.write_text("{", encoding="utf-8")
    res = summarize_artifacts([f"file://{bad}"])
    assert res[0].ok is False
    assert "json_decode_error" in res[0].summary


def test_summarize_artifacts_ok_json(tmp_path) -> None:
    p = tmp_path / "payload.json"
    p.write_text(json.dumps({"a": 1, "b": 2}), encoding="utf-8")
    res = summarize_artifacts([f"file://{p}"])
    assert res[0].ok is True
    assert res[0].details and "keys" in res[0].details



