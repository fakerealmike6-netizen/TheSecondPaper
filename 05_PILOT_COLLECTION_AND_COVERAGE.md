# Actual two-probe collection

The exact original seeds/query IDs and UTC bounds are preserved in 02_FINAL_STATUS.json and the policy. Atomic uses blocks 17395962–17427582, depth 2; Harmony uses 16402635–16403140, depth 5. The active start was Atomic one and Harmony three pending arrivals; subsequent real events generated further arrivals. Collection continued until every legal in-scope frontier was completed or stopped by the frozen depth/first-service rules.

| Probe | Old rows reused | New rows | New physical IDs | Candidates old→new | Intervals old+new | Frontier |
|---|---:|---:|---:|---:|---:|---:|
| atomic_simple_transfer | 18 | 3 | 1 | 11→12 | 4+1 | 0 |
| harmony_high_branch | 5 | 117 | 52 | 4→50 | 1+26 | 0 |

Atomic's five completed intervals queried top/internal/ERC20; internal returned zero in all five, while inherited ERC20 context supplied four raw rows. Harmony's 27 completed intervals also queried all three surfaces; internal and ERC20 returned zero in their actual queried windows. These are successful indexed-window zero results, not unqueried rows filled with zero, nor a proof that all historical ETH state/internal/fees are complete. Standard Transfer filtering covers event endpoints, not only transaction signers.

New candidate batches returned Atomic 3 and Harmony 14/49/49/5 rows; all had complete one-page exports. Reused old rows were not redownloaded. Across each probe, raw occurrences and physical IDs are distinct units: Atomic 21→16, Harmony 122→57. Candidate capacities are deduplicated; context-only facts do not become propagation edges. There are no normalization gaps, conflicting facts, quarantined facts or remaining failed/inflight candidate jobs.

Per-arrival 90-day windows intersect the original scope, strict event ordering, same-asset propagation, first-identified-service stops and acquisition-depth limits remain active. Harmony has 13 service arrival-state stops but only eight unique target entry events. Context entries can recur across arrival views: 95 Harmony entries represent 49 physical context IDs. They are not 95 extra transfers. LP adds no acquisition-depth or source-age restriction to combinations already present in the fixed graph.
