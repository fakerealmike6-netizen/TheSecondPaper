# Stage1B-R3 fixed review index

Stage1B-R3 reaches CHECKPOINT_1B_R3_REACHED with the two ETH context pipelines complete within their declared model scope. Overall: COMPLETED_WITH_RECORDED_GAPS. External acceptance: PENDING_REVIEW.

Baseline is commit `88773d0a74187dfc4bf74e962ac9e9d72819ad1f`, tree `5e5321f7feff14a4893c8f57ea97dbaea517069d`. Review this revision at tag `stage1b-r3-20260907T151753+0800_stage1b_r3`, never by assuming the default branch or main is R2/R3.

Read 01 for the checkpoint, 03 for the completion matrix, 04 for evidence and reconciliation, 05 for constraint mapping, 06 for three-model amounts, 07 for the separate WETH limitation, 08 for cumulative accounting, and 09 for tests. Fixed commit/download identities and final archive tests are recorded externally in PUBLICATION_RECEIPT.json, FINAL_VALIDATION_RESULTS.json, and REVIEW_HANDOFF.md.

The MIN contains finite original responses, exact baseline graphs and LPs, 55 anchors, account ledgers, fee roles, complete constraints and witnesses. Public contains corresponding source, synthetic tests and sanitized reports; it deliberately cannot replay excluded private real data.

After extracting, write outputs into a fresh directory outside the extracted tree:

```text
python -B src/validate_review_bundle_r3.py --tree <extracted-public> --output <fresh-output> --kind public
python -B src/validate_review_bundle_r3.py --tree <extracted-MIN> --output <fresh-output> --kind min
```

MIN audit entries: configs/CONTEXT_REPLAY_R3.json; derived/context_pipeline/<pilot>/; derived/context_amounts/<pilot>/; derived/amount_comparison_r3/; derived/usage_r3/. The validator reconstructs anchors from raw RPC request/response bindings and checks Dune pages before computing amounts. No credentials or network are used in validation.
