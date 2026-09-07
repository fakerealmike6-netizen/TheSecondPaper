# Stage1B-R4-R1 checkpoint report

R4-F05-R1 is FIXED_AND_TESTED. The only modified existing production file is src/provider_receipts.py. All 46 existing test files are byte-identical to R4; the original 660 tests and 53 new tests pass. Additional source files only supply revision packaging/validation, failure controls and verification of the already saved shared RPC body.

The two existing ETH models, 55 anchors, 69 value events and 61 fees are reused; evidence-to-ledger-to-LP replay matches the fixed R4 results. No labels, windows, seeds, depths, source initialization or WETH predicates were changed. All research-provider requests, usage requests and new fees are zero. Frozen payload reports direct final ZIP/publication results to the external receipts. Stop at CHECKPOINT_1B_R4_R1_REACHED; external acceptance is PENDING_REVIEW.
