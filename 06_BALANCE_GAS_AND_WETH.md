# Balance, gas, order and WETH

Atomic has six unknown initial address-ETH balances; Harmony has 29. No unknown was replaced with zero. Candidate gas is observed for all 12 and 50 transactions, respectively: 5976984603492000 and 23515433358228000 wei. Actual transaction gas spend is distinct from its source-taint share. The current conditional LP does not enforce complete transaction/address gas history or source attribution.

All 62 candidate transfers retain observed tx_index and pass the shared necessary-order guard. Candidate block hashes are absent; the report does not claim independent block-finality verification. No needed relative order is synthesized from hashes.

The real fixed WETH transaction received one scoped trace SQL and a complete five-row export. Canonical deposit input 0xd0e30db0, 150 ETH, the credited caller and observed ancestry resolve the WETH path to [0,0,0,0]. Legacy trace:1 remains the historical response ordinal; it was not silently redefined as a trace path. Delegatecall records are not independent value capacity. Output data remains unknown and observed lack of descendants is not a certified zero refund.

Real certification remains NOT_CERTIFIED (6/12); historical runtime/source and log-to-frame evidence are still insufficient. Conversion remains disabled. The controlled WETH/LP fixtures are a separate successful synthetic result. No unsupported bridge-wide WETH-only claim is made.

The previously failing historical PublicNode path had no demonstrated capability change and was not retried. Current Alchemy allowance and BigQuery entitlement were insufficiently established for new requests. The documented Dune balances tables require Enterprise access; this trial task did not upgrade or query them. See docs/R2_CONTEXT_EVIDENCE_PLAN.md and docs/R2_WETH_CONTEXT_RESULT.md for source evidence and precise missing predicates.
