"""
Prompting / agent methodology implementation.

This package implements PATCHIT's multi-stage prompt pipeline:
- Meta prompt (identity + safety)
- Engineered sequencing (RCA -> Fix -> Validate -> Decide)
- Variant prompting + scoring + selection
- Modality switching on low-quality outputs
- Playbook tightening from failures
"""


