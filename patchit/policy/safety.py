from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List


_INJECTION_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"ignore\s+previous\s+instructions", re.IGNORECASE),
    re.compile(r"system\s+prompt", re.IGNORECASE),
    re.compile(r"BEGIN\s+SYSTEM", re.IGNORECASE),
    re.compile(r"END\s+SYSTEM", re.IGNORECASE),
    re.compile(r"you\s+are\s+chatgpt", re.IGNORECASE),
    re.compile(r"exfiltrat(e|ion)", re.IGNORECASE),
    re.compile(r"api[_ -]?key", re.IGNORECASE),
    re.compile(r"print\s+your\s+instructions", re.IGNORECASE),
    re.compile(r"do\s+not\s+follow\s+policy", re.IGNORECASE),
]


@dataclass(frozen=True)
class InjectionSignal:
    ok: bool
    matches: List[str]


def detect_prompt_injection(text: str | None, *, max_matches: int = 5) -> InjectionSignal:
    """
    Heuristic detector for prompt-injection strings embedded in logs/artifacts.
    This is intentionally conservative: any match triggers a safety stop.
    """
    t = text or ""
    hits: list[str] = []
    for pat in _INJECTION_PATTERNS:
        m = pat.search(t)
        if not m:
            continue
        hits.append(pat.pattern)
        if len(hits) >= max_matches:
            break
    return InjectionSignal(ok=(len(hits) == 0), matches=hits)



