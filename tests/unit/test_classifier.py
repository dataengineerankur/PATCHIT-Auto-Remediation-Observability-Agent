from __future__ import annotations

from patchit.classifier.rules import RuleBasedClassifier
from patchit.models import NormalizedErrorObject


def test_classifier_partial_json() -> None:
    err = NormalizedErrorObject(error_id="e1", error_type="JSONDecodeError", message="Expecting value", stack=[])
    out = RuleBasedClassifier().classify(err)
    assert out.classification == "partial_or_malformed_json"


def test_classifier_schema_drift_contract_error() -> None:
    err = NormalizedErrorObject(
        error_id="e1",
        error_type="DataContractError",
        message="Missing required field userId (schema drift likely: field renamed to user_id)",
        stack=[],
    )
    out = RuleBasedClassifier().classify(err)
    assert out.classification == "schema_drift_missing_field"



