from __future__ import annotations

from patchit.parsers.airflow_log_parser import extract_error_excerpt


def test_extract_error_excerpt_prefers_last_traceback() -> None:
    log = "\n".join(
        [
            "INFO - something",
            "Traceback (most recent call last):",
            "  File \"a.py\", line 1, in x",
            "ValueError: first",
            "INFO - more",
            "Traceback (most recent call last):",
            "  File \"b.py\", line 2, in y",
            "KeyError: second",
        ]
    )
    res = extract_error_excerpt(log, max_lines=100)
    assert res.has_traceback is True
    assert "b.py" in res.excerpt
    assert "a.py" not in res.excerpt



