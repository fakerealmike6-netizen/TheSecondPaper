# Fixed-graph amounts and separated changes

SAME_INPUT_FIXED_CODE: four R1 saved graphs/labels retain the same graph semantics and all LP endpoints. The independent old same-asset check covers 23 intervals / 46 endpoint witnesses.

SAME_RAW_UPDATED_LABELS: the five old successful query exports were replayed with the final new labels. Atomic remains [0,287.395] ETH and Harmony remains NO_OBSERVED_TARGET on those old facts. Thus new labels alone do not manufacture a missing observed service entry in the old graph. Intermediate new-data replays before/after labels separately retain the causal change records.

NEW_OBSERVED_GRAPH: the real new Atomic event contributes 72.1 ETH of additional target capacity, updating its joint upper to 359.495. New Harmony observed facts plus seven matched service identities produce eight unique entries and seven address-asset groups.

| Probe / address–ETH | Conditional lower ETH | Conditional joint upper ETH | Unique entry events |
|---|---:|---:|---:|
| atomic / 0x579a7f3c9de8da595fcbf93342874260ebb23d3a | 0 | 359.495 | 6 |
| harmony / 0xd21c356b252212297f55187c4d3d001f1278c5ff | 0 | 550 | 2 |
| harmony / 0x1fba259f529849a718f970fa0ce15bd70d17c2db | 0 | 249.997530999558463 | 1 |
| harmony / 0x09c964c75a27a1d296108e5afe7108f98993b2f5 | 0 | 268 | 1 |
| harmony / 0xf69f340cf699be3e0169558230d51d45d01d0216 | 0 | 225 | 1 |
| harmony / 0x7fa9128f778d551236186ab1dfb5aace36af12d9 | 0 | 275 | 1 |
| harmony / 0x5cad5e57551dc4e91e092a4fc5364860cb092473 | 0 | 251 | 1 |
| harmony / 0x9d753eeae3291e4d5d5f67d5308b2c378bade935 | 0 | 5 | 1 |

All targets have event-level and address-asset joint results. For a one-event group the joint result is also its event interval, so an empty separate entry_intervals map is an explicit alias rather than a missing computation. Different addresses' upper bounds were optimized separately and are not added into a claimed all-target joint optimum. All amounts are raw integer wei in the saved data, with exact model endpoint certificates/witnesses where applicable.

Results are ASSUMPTION_CONDITIONAL on the observed graph and missing initial-balance/gas/conversion assumptions. They are not full-chain attributed amounts. Model inputs reconstruct the constraint matrix; solver status, primal/dual endpoint artifacts and witnesses are supplied. Independent integer maximum-flow checks apply only to eligible same-asset model assumptions and are not extended to unobserved conversions or full gas constraints.
