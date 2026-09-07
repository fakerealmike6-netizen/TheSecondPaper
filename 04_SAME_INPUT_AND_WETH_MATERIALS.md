# Same inputs and WETH materials

| Probe | Accounts / anchors / value events / fees | Joint ETH interval |
|---|---|---|
| Atomic | 5 / 10 / 12 / 11 | [359.495,359.495] |
| Harmony | 22 / 45 / 57 / 50 | [504.907298683760128997,1554.999042315467482] |

Both are FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE with no new conflicts/residuals. The actual evidence-to-ledger-to-LP replay matches fixed R4 inputs; R2/R3 baselines and matched same-graph balance relaxation also pass. Relaxed lower bounds remain zero with unchanged upper bounds; no outcome was imposed as a solver constraint.

The existing shared HTTP body was found by its accepted R4 manifest's one R3 batch reference. Its original 25,319 bytes have SHA-256 c22430d52fd472458dc477c28a862986b0e2b54d2d4971770f78287033d006e7. Five request/response members were checked by ID against original split artifacts: four successes and one preserved trace RPC error. Original timestamps and hashes are preserved; redacted identity/receipt views have separate hashes and are not represented as original wire bytes. No response was reconstructed or re-requested. Raw body and account material are private MIN only.

src/verify_weth_shared_wire_r4_r1.py is a separate material check in MIN validation. The existing fixed 150 ETH local WETH component remains 12/12 with unchanged predicates; it does not certify the full bridge/protocol. This material supplement is separate from the F05 code correction. The old Free-tier trace denial and 582 unrelated historical label-support gaps remain recorded, with no retries or label lookup.
