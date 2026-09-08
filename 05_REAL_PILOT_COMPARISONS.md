# Real pilot comparisons

The two ETH graphs are the accepted full contexts: 55 anchors, 69 value events and 61 fees in total. They are previously developed cases. No real source-amount truth is available; no real amount accuracy or precision is reported.

| Case | Method | Joint ETH / point / nominal | Positive addresses | Positive entries | Known-positive address recall |
| --- | --- | --- | --- | --- | --- |
| Atomic | FULL_INTERVAL | [359.495000000000000000, 359.495000000000000000] | 1 | 6 | 1.000000 |
| Atomic | BOUNDED_REACHABILITY | No amount | 1 | 6 | 1.000000 |
| Atomic | POISON | 359.495000000000000000 | 1 | 6 | 1.000000 |
| Atomic | HAIRCUT | 359.495000000000000000 | 1 | 6 | 1.000000 |
| Atomic | NO_CROSS_TARGET_COUPLING | [359.495000000000000000, 359.495000000000000000] | 1 | 6 | 1.000000 |
| Atomic | NO_PROTOCOL_CONTINUATION | [359.495000000000000000, 359.495000000000000000] | 1 | 6 | 1.000000 |
| Atomic | BALANCE_INFORMATION_REMOVED | [0.000000000000000000, 359.495000000000000000] | 1 | 6 | 1.000000 |
| Harmony | FULL_INTERVAL | [504.907298683760128997, 1554.999042315467482000] | 7 | 8 | NA |
| Harmony | BOUNDED_REACHABILITY | No amount | 7 | 8 | NA |
| Harmony | POISON | 1831.000000000000000000 | 7 | 8 | NA |
| Harmony | HAIRCUT | 1068.613261375416928213 | 7 | 8 | NA |
| Harmony | NO_CROSS_TARGET_COUPLING | [60.302368224708810999, 1823.997530999558463000] | 7 | 8 | NA |
| Harmony | NO_PROTOCOL_CONTINUATION | [504.907298683760128997, 1554.999042315467482000] | 7 | 8 | NA |
| Harmony | BALANCE_INFORMATION_REMOVED | [0.000000000000000000, 1554.999042315467482000] | 7 | 8 | NA |

Atomic has one independently retained known-positive address and six entries. Harmony has no currently verified reference positives after first-service correction; its runtime targets are not a reference denominator. Recall is null there. Unknown labels and reference-external outputs are not negative examples.

Atomic: Haircut complete assignment feasible=True; B_min convention adopted=False, B0=0.000000000000000000 ETH. B0 is an assumed completion, not an observed fact. The two real ETH graphs contain no connected supported WETH conversion, so the protocol-ablation feature is absent.

Harmony: Haircut complete assignment feasible=True; B_min convention adopted=True, B0=2499.993785860695088000 ETH. B0 is an assumed completion, not an observed fact. The two real ETH graphs contain no connected supported WETH conversion, so the protocol-ablation feature is absent.

Amounts displayed to 18 decimals; exact rational raw quantities remain in STATISTICS.json. Public reporting omits real account/transaction identities and third-party reference rows; MIN retains detailed per-address and per-event results. Query and incident macro summaries report their eligible denominators, with global address, address-asset, entry-event and known-actor counts deduplicated separately.
