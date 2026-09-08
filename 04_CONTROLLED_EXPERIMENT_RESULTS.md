# CONTROLLED_V1 results

All six prespecified families contain ten newly constructed samples. The historical twelve scenarios and thirty-two comparisons are separate regression evidence. Hidden feasible allocations and independent Oracle extrema serve different roles; neither constitutes external blind truth.

| Family | Queries | Passed | Mean normalized union width | Positive / zero lower | Haircut feasible | Poison > full upper |
| --- | --- | --- | --- | --- | --- | --- |
| canonical_weth_1to1 | 10 | 10 | 0.101786 | 10 / 0 | 10 | 10 |
| gas_background_and_anchors | 10 | 10 | 0.293571 | 8 / 2 | 10 | 10 |
| missing_information_and_boundary_controls | 10 | 10 | 0.466667 | 4 / 6 | 4 | 6 |
| normal_mixing | 10 | 10 | 0.553095 | 5 / 5 | 10 | 5 |
| repeated_entries_timed_returns | 10 | 10 | 0.000000 | 10 / 0 | 10 | 10 |
| split_merge_shared_targets | 10 | 10 | 0.000000 | 10 / 0 | 10 | 10 |

Query mean normalized union width: 0.235853. Exact endpoint errors by asset: {"ETH": "0", "TOK": "0", "WETH": "0"}. Hidden coverage by statistical unit: {"addresses_total": 97, "addresses_covered": 97, "events_total": 117, "events_covered": 117, "joint_by_asset_total": 60, "joint_by_asset_covered": 60}.

| Method | Query macro address recall | Eligible positive queries / all | False-positive query/address/asset pairs |
| --- | --- | --- | --- |
| FULL_INTERVAL | 1.000000 | 58 / 60 | 0 |
| BOUNDED_REACHABILITY | 1.000000 | 58 / 60 | 0 |
| POISON | 1.000000 | 58 / 60 | 0 |
| HAIRCUT | 1.000000 | 52 / 60 | 0 |
| NO_CROSS_TARGET_COUPLING | 1.000000 | 58 / 60 | 0 |
| NO_PROTOCOL_CONTINUATION | 0.827586 | 58 / 60 | 0 |
| BALANCE_INFORMATION_REMOVED | 1.000000 | 58 / 60 | 0 |

Haircut query mean normalized distance to one hidden assignment: 0.160527. This distance does not imply that a different feasible proportional allocation is wrong. Exact whole-assignment feasibility is assessed separately. 12 prespecified tiny crosschecks are retained; the dedicated evidence file reports objective counts and full enumeration status.

Per-query and per-objective results are preserved in the machine-readable batch output. All failures and unsupported cases remain in STATISTICS.json and METHOD_SUPPORT_MATRIX.csv.
