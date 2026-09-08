# Methods and fair input boundary

All seven methods use identical frozen observed physical facts, target domain, event order, asset scope, actual fees and available balances. Baselines cannot read hidden allocations, Oracle answers, references or FULL endpoints. Reference evaluation occurs after method results are saved.

| Method | Output and interpretation |
| --- | --- |
| FULL_INTERVAL | Exact-certified event/address/joint feasible source intervals |
| BOUNDED_REACHABILITY | Temporal/asset address-event set; no amount |
| POISON | Persistent boolean marking and nominal raw physical volume |
| HAIRCUT | Independent exact-rational proportional replay; explicit B_min boundary convention when needed |
| NO_CROSS_TARGET_COUPLING | Product of independent target-specific complete source-allocation copies |
| NO_PROTOCOL_CONTINUATION | Supported protocol net source terminates at input boundary |
| BALANCE_INFORMATION_REMOVED | Same physical graph with selected balance information relaxed |

Haircut is not chosen by optimizing the primary LP. The primary constraints only verify its already-constructed allocation. H_BMIN_BOUNDARY_V1 supplies the minimum non-source outside balance needed by the observed chronology; it is a baseline assumption, not an observed balance. Unknown modeled balances outside this convention are reported unsupported. WETH refund examples use a synthetic gross/refund envelope around the canonical net deposit; WETH9.deposit itself is not claimed to refund.

See METHOD_SPEC_EFFECTIVE.md, EXPERIMENT_FREEZE.json and STATISTICAL_DICTIONARY.json for frozen semantics, code identities, and metric units.
