# Offline R4-R1 bundle validation

Use a fresh output directory outside the extracted tree. Existing NumPy/SciPy dependencies are listed in requirements.txt. No live provider credentials are needed.

```text
python -B src/validate_review_bundle_r4_r1.py --tree . --output ../r4-r1-min-validation --kind min
python -B src/validate_review_bundle_r4_r1.py --tree . --output ../r4-r1-public-validation --kind public
```

Run the command matching the inner ZIP. The inherited strict guard removes real credentials, disables networking, propagates to legitimate Python children and restricts writes. Public runs all synthetic tests and 12/32 controlled LP checks. MIN additionally replays saved R4 context using the preserved R4 engine/config schema, compares R2/R3 and exact R4 context snapshots, runs independent audits and verifies the new original-wire material closure. Missing private files cannot be silently treated as verified wire. Existing R4 root reports/config names are historical or explicitly reused input schemas, not renewed spend authorization.

The new validator's four actual CLI failure controls (mismatch/nonzero/missing/malformed result) must FAIL with exit 1; its normal expected-gap control must PASS with exit 0. These use explicitly synthetic children and never count mocked fixture totals as real tests. Linux is not claimed tested in this revision.
