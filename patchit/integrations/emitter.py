from __future__ import annotations

import json
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import httpx


@dataclass(frozen=True)
class IntegrationEvent:
    schema: str
    emitted_at_unix: float
    event_type: str
    correlation_id: str
    payload: Dict[str, Any]


def _parse_urls(urls_json: str | None) -> List[str]:
    if not urls_json:
        return []
    try:
        raw = json.loads(urls_json)
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    for u in raw:
        if isinstance(u, str) and u.strip():
            out.append(u.strip())
    return out


def _parse_event_types(types_json: str | None) -> Optional[set[str]]:
    if not types_json:
        return None
    try:
        raw = json.loads(types_json)
    except Exception:
        return None
    if not isinstance(raw, list):
        return None
    out: set[str] = set()
    for t in raw:
        if isinstance(t, str) and t.strip():
            out.add(t.strip())
    return out or None


class IntegrationEmitter:
    """
    Best-effort outbound event emitter (typically to n8n).
    Non-blocking: failures must never break remediation.
    """

    def __init__(
        self,
        *,
        webhook_urls_json: str | None,
        emit_event_types_json: str | None = None,
        timeout_s: float = 6.0,
        max_queue: int = 500,
    ) -> None:
        self._urls = _parse_urls(webhook_urls_json)
        self._allowed_types = _parse_event_types(emit_event_types_json)
        self._timeout_s = float(timeout_s)
        self._q: "queue.Queue[IntegrationEvent]" = queue.Queue(maxsize=max_queue)
        self._stop = threading.Event()
        self._t: threading.Thread | None = None

    def start(self) -> None:
        if self._t is not None:
            return
        if not self._urls:
            return

        def _run() -> None:
            with httpx.Client(timeout=self._timeout_s) as client:
                while not self._stop.is_set():
                    try:
                        ev = self._q.get(timeout=0.25)
                    except queue.Empty:
                        continue
                    try:
                        data = {
                            "schema": ev.schema,
                            "emitted_at_unix": ev.emitted_at_unix,
                            "event_type": ev.event_type,
                            "correlation_id": ev.correlation_id,
                            "payload": ev.payload,
                        }
                        for url in self._urls:
                            try:
                                client.post(url, json=data)
                            except Exception:
                                # Best-effort: swallow.
                                continue
                    finally:
                        try:
                            self._q.task_done()
                        except Exception:
                            pass

        self._t = threading.Thread(target=_run, name="patchit_integration_emitter", daemon=True)
        self._t.start()

    def stop(self) -> None:
        self._stop.set()

    def emit(self, *, event_type: str, correlation_id: str, payload: Dict[str, Any]) -> None:
        if not self._urls:
            return
        if self._allowed_types is not None and event_type not in self._allowed_types:
            return
        try:
            ev = IntegrationEvent(
                schema="patchit.integration_event.v1",
                emitted_at_unix=time.time(),
                event_type=event_type,
                correlation_id=correlation_id,
                payload=payload or {},
            )
            self._q.put_nowait(ev)
        except Exception:
            return


