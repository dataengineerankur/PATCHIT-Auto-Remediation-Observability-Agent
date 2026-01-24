from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional


class AuditLogger:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def new_correlation_id(self) -> str:
        return uuid.uuid4().hex

    def write(
        self,
        correlation_id: str,
        event_type: str,
        payload: Dict[str, Any],
        *,
        actor: str = "patchit",
        timestamp: Optional[str] = None,
    ) -> None:
        record = {
            "ts": timestamp or datetime.utcnow().isoformat() + "Z",
            "correlation_id": correlation_id,
            "actor": actor,
            "event_type": event_type,
            "payload": payload,
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")



