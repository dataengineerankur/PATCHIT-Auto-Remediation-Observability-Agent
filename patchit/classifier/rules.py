from __future__ import annotations

from dataclasses import dataclass

from patchit.models import NormalizedErrorObject


@dataclass(frozen=True)
class RuleBasedClassifier:
    """
    Phase-1: deterministic rules only. Designed to be extended with ML later.
    """

    def classify(self, err: NormalizedErrorObject) -> NormalizedErrorObject:
        msg = (err.message or "").lower()
        excerpt = (err.raw_excerpt or "").lower()

        # Generic python exception buckets (used by heuristic patcher too)
        if err.classification is None:
            et = (err.error_type or "").lower()
            if "keyerror" in et:
                err.classification = "python_key_error"
                err.likely_root_cause = "Code attempted to access a missing key in a dict (schema drift / contract mismatch)."
                err.confidence = max(err.confidence, 0.55)
                return err
            if "filenotfounderror" in et:
                err.classification = "python_file_not_found"
                err.likely_root_cause = "Code attempted to read/write a file path that does not exist."
                err.confidence = max(err.confidence, 0.52)
                return err
            if "attributeerror" in et:
                err.classification = "python_attribute_error"
                err.likely_root_cause = "Code attempted to access an attribute that doesn't exist (often NoneType)."
                err.confidence = max(err.confidence, 0.5)
                return err

        # Explicit contract failures from our demo pipeline
        if err.error_type.lower() in ("datacontracterror",) or "contract" in msg:
            if "schema drift" in msg or "renamed" in msg or "missing required field" in msg:
                err.classification = "schema_drift_missing_field"
                err.likely_root_cause = "Contract failure due to upstream schema drift or missing field."
                err.confidence = max(err.confidence, 0.75)
                return err

        # Malformed / partial JSON
        if "jsondecodeerror" in err.error_type.lower() or "expecting value" in msg:
            err.classification = "partial_or_malformed_json"
            err.likely_root_cause = "Upstream wrote malformed/partial JSON or downstream read before write completed."
            err.confidence = max(err.confidence, 0.75)
            return err

        # Missing column / schema drift
        if "keyerror" in err.error_type.lower() and ("missing" in msg or "keyerror" in msg):
            err.classification = "schema_drift_missing_field"
            err.likely_root_cause = "API schema drift or contract violation (field missing/renamed)."
            err.confidence = max(err.confidence, 0.7)
            return err

        if "column" in msg and ("not found" in msg or "does not exist" in msg):
            err.classification = "dbt_missing_column"
            err.likely_root_cause = "dbt model references missing column due to upstream schema drift."
            err.confidence = max(err.confidence, 0.75)
            return err

        # dbt run wrapper errors often hide the adapter message in the log excerpt, not the exception message.
        if ("dbt" in excerpt or "dbt run" in excerpt or "dbt build" in excerpt) and ("column" in excerpt) and (
            "does not exist" in excerpt or "not found" in excerpt
        ):
            err.classification = "dbt_missing_column"
            err.likely_root_cause = "dbt failed due to missing column (see dbt adapter/database error in logs)."
            err.confidence = max(err.confidence, 0.72)
            return err

        # dbt build wrapper errors often hide the underlying binder message in the log excerpt
        if "dbt build failed" in msg or "dbt build failed" in excerpt:
            if ("binder error" in excerpt or "column" in excerpt) and ("not found" in excerpt or "does not exist" in excerpt):
                err.classification = "dbt_missing_column"
                err.likely_root_cause = "dbt build failed due to missing column (DuckDB binder error)."
                err.confidence = max(err.confidence, 0.75)
                return err
            err.classification = "dbt_failure"
            err.likely_root_cause = "dbt build failed; see dbt output in the log excerpt."
            err.confidence = max(err.confidence, 0.6)
            return err

        # Intermittent API error payload
        if "error payload" in msg or ("status_code" in msg and "500" in msg):
            err.classification = "intermittent_api_error"
            err.likely_root_cause = "Temporal upstream API failure returning error payload instead of expected schema."
            err.confidence = max(err.confidence, 0.7)
            return err

        err.classification = err.classification or "unknown"
        err.likely_root_cause = err.likely_root_cause or "Could not confidently classify root cause."
        err.confidence = min(err.confidence, 0.55)
        return err


