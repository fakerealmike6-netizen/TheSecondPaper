# Stage1B-R4 review source

Nine repairs, durable bounded read recovery, and same-input context validation.
External acceptance: PENDING_REVIEW. Stop at CHECKPOINT_1B_R4_REACHED.

The fixed parent is R3 commit 83507f2660462508233121e1069478a05298a866; main is not the baseline. Publication is append-only.

After extracting the public bundle, run:
```
python -B src/validate_review_bundle_r4.py --tree . --kind public --output ../r4_public_validation
```
For the private MIN use `--kind min`. Output must be a new directory outside the frozen input. The validator removes credentials, blocks network access, guards Python children and restricts writes to that output. Python plus requirements.txt dependencies must already be installed.

Windows Python 3.14.3 was actually tested. Linux was not available and is not claimed tested. The public bundle contains code, synthetic tests and sanitized summaries; real response evidence and account accounting are private. Both source copies are explicitly hash mapped by PUBLIC_LOCAL_EQUIVALENCE.json.

Begin with 00_REVIEW_INDEX.md and 03_REPAIR_CLOSURE_MATRIX.json. Final commit, downloaded public asset hash and extracted-ZIP validation are in the external publication/validation receipts, avoiding hash cycles.
