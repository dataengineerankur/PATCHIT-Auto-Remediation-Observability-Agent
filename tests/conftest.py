from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Keep tests deterministic and isolated from developer machine env.
    """
    for k in list(os.environ.keys()):
        if k.startswith("PATCHIT_"):
            monkeypatch.delenv(k, raising=False)



