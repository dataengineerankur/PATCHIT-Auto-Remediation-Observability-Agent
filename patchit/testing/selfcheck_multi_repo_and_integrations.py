from __future__ import annotations

import json

from patchit.integrations.emitter import IntegrationEmitter
from patchit.repos.registry import select_repo_context
from patchit.settings import Settings


def main() -> None:
    # ---- Multi-repo selection ----
    s = Settings()
    s.repo_registry_json = json.dumps(
        [
            {"key": "repoA", "pipeline_id_regex": "^alpha_", "repo_root": "repos/a", "github_repo": "org/a"},
            {"key": "repoB", "pipeline_id_regex": "^beta_", "repo_root": "repos/b", "github_repo": "org/b"},
        ]
    )
    a = select_repo_context(settings=s, pipeline_id="alpha_payments")
    assert a.key == "repoA"
    assert a.github_repo == "org/a"

    b = select_repo_context(settings=s, pipeline_id="beta_risk")
    assert b.key == "repoB"
    assert b.github_repo == "org/b"

    # ---- Integration emitter filtering ----
    em = IntegrationEmitter(
        webhook_urls_json=json.dumps(["http://example.invalid/webhook"]),
        emit_event_types_json=json.dumps(["policy.decided"]),
        timeout_s=0.01,
    )
    # Best-effort: should never raise.
    em.emit(event_type="pr.created", correlation_id="c1", payload={"x": 1})
    em.emit(event_type="policy.decided", correlation_id="c1", payload={"x": 1})

    print("selfcheck ok")


if __name__ == "__main__":
    main()


