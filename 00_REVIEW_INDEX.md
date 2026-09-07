# Stage1B-R2 review index

This revision continues the existing R1 run and stops at CHECKPOINT_1B_R2_REACHED. External acceptance is PENDING_REVIEW.

Read 01 for the checkpoint, 03 for the three directed fixes, 04 for cumulative accounting, 05 for actual collection, 06 for balance/gas/WETH, 07 for amounts, 08 for label/reference changes, and 09 for test status. Fixed publication identity and final ZIP validation are resolved by the external PUBLICATION_RECEIPT.json, FINAL_VALIDATION_RESULTS.json and REVIEW_HANDOFF.md.

The MIN includes finite saved pages and receipts, exact two seeds, final collectors/graphs/LP inputs and witnesses, four old graph baselines, a 35-row final label slice, new label observations, and the finite affected reference proof. It excludes full historical reference/label libraries and account databases. Public review includes safe original source, synthetic fixtures/tests, public rules and aggregate reports. Its private replay is explicitly omitted.

Commands after extracting into a fresh directory (outputs must be outside that directory):

```text
python -B src/validate_review_bundle_r2.py --tree <extracted> --output <fresh-output> --kind min
python -B src/validate_review_bundle_r2.py --tree <public-extracted> --output <fresh-output> --kind public
```

MIN row-level audit: derived/pilot_metrics_final/{events,intervals,jobs,pages,requests,frontiers_and_stops,target_amounts,gas_candidate_context}.csv. Candidate/context membership can overlap across arrival windows; physical events are deduplicated before LP capacity. The metrics collector never performs network work.
