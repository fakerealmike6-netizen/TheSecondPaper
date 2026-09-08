# CONTROLLED_V1 results

All six prespecified families contain ten newly constructed samples. The historical twelve scenarios and thirty-two comparisons are separate regression evidence. Hidden feasible allocations and independent Oracle extrema serve different roles; neither constitutes external blind truth.

| Family | Queries | Passed | Mean normalized union width | Positive / zero lower | Haircut feasible | Poison > full upper |
| --- | --- | --- | --- | --- | --- | --- |
| normal_mixing | 1 | 1 | 0.500000 | 1 / 0 | 1 | 1 |

Query mean normalized union width: 0.500000. Exact endpoint errors by asset: {"ETH": "0"}. Hidden coverage by statistical unit: {"addresses_total": 1, "addresses_covered": 1, "events_total": 1, "events_covered": 1, "joint_by_asset_total": 1, "joint_by_asset_covered": 1}.

| Method | Query macro address recall | Eligible positive queries / all | False-positive query/address/asset pairs |
| --- | --- | --- | --- |
| FULL_INTERVAL | 1.000000 | 1 / 1 | 0 |
| BOUNDED_REACHABILITY | 1.000000 | 1 / 1 | 0 |
| POISON | 1.000000 | 1 / 1 | 0 |
| HAIRCUT | 1.000000 | 1 / 1 | 0 |
| NO_CROSS_TARGET_COUPLING | 1.000000 | 1 / 1 | 0 |
| NO_PROTOCOL_CONTINUATION | 1.000000 | 1 / 1 | 0 |
| BALANCE_INFORMATION_REMOVED | 1.000000 | 1 / 1 | 0 |

Haircut query mean normalized distance to one hidden assignment: 0.250000. This distance does not imply that a different feasible proportional allocation is wrong. Exact whole-assignment feasibility is assessed separately. 1 prespecified tiny crosschecks are retained; the dedicated evidence file reports objective counts and full enumeration status.

Per-query and per-objective results are preserved in the machine-readable batch output. All failures and unsupported cases remain in STATISTICS.json and METHOD_SUPPORT_MATRIX.csv.
