# R4 review index

1. `03_REPAIR_CLOSURE_MATRIX.json` / `.md`: F01–F09 code, regressions, callers and historical scope.
2. `04_RETRY_POLICY_AND_FAULT_TESTS.md`: E01 durable recovery and retained risk.
3. `05_SAME_INPUT_REPLAY_AND_AMOUNT_DIFF.md`: original R3, current evidence-to-model and nested relaxation.
4. `06_HISTORICAL_LABEL_AND_CACHE_IMPACT.md`: F03/F06/F07/F08 historical corrections versus unresolved support.
5. `07_WETH_REMAINING_GAPS_OR_CERTIFICATION.md`: actual fixed-component proof and limits.
6. `08_BUDGET_AND_REQUEST_LEDGER.md`, `09_TEST_RESULTS.json`: cumulative accounting and actual tests.
7. `src/validate_review_bundle_r4.py`: no-network/no-credential reproduction entry point.

MIN additionally includes exact saved response closures, anchors, account ledgers, fees, constraint mappings, endpoint witnesses, original counterexamples/results and code diffs. It excludes the full label library. The original inner review ZIP containers are not published.

External `PUBLICATION_RECEIPT.json`, `FINAL_VALIDATION_RESULTS.json` and `REVIEW_HANDOFF.md` bind final public identity, both ZIPs and their actual extracted validation. External acceptance remains PENDING_REVIEW.
