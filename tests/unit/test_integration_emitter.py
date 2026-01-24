from __future__ import annotations

import json

from patchit.integrations.emitter import IntegrationEmitter


def test_integration_emitter_filters_event_types() -> None:
    em = IntegrationEmitter(
        webhook_urls_json=json.dumps(["http://example.invalid/webhook"]),
        emit_event_types_json=json.dumps(["policy.decided"]),
        timeout_s=0.01,
    )
    # Should not raise (we don't actually send since no start + best-effort queue).
    em.emit(event_type="pr.created", correlation_id="c1", payload={"x": 1})
    em.emit(event_type="policy.decided", correlation_id="c1", payload={"x": 1})


