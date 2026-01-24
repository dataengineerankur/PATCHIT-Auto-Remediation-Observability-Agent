from __future__ import annotations

from patchit.parsers.stacktrace import parse_python_traceback


def test_parse_python_traceback_extracts_frames_and_error() -> None:
    tb = "\n".join(
        [
            "INFO something",
            "Traceback (most recent call last):",
            "  File \"/opt/airflow/dags/task_lib/validate.py\", line 10, in validate",
            "    payload = json.load(f)",
            "json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)",
        ]
    )
    parsed = parse_python_traceback(tb)
    assert parsed is not None
    assert parsed.error_type == "JSONDecodeError"
    assert "Expecting value" in parsed.message
    assert parsed.frames
    assert parsed.frames[0].file_path.endswith("validate.py")



