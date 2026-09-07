# R2A shared observed-event order

`cache_probe.fixed_graph` now calls `event_order.order_observed_transfers` after physical-fact reconciliation. `dune_observed_replay.live_fixed_graph` uses the same builder and guard; its former separate pairwise implementation is removed without removing the shared-balance rejection it supplied. LP arithmetic, balances, capacity constraints, terminal treatment and collection policy are unchanged.

Block order, transaction index and the already supported verified intra-transaction execution/log positions determine the presentation through a topological sort. Missing transaction indices never determine shared-balance chronology through a hash. A same-block pair touching the same address and asset with no established relative order marks the entire affected graph `ORDER_UNRESOLVED_MODEL_NOT_SOLVABLE`; the existing LP rejection remains active. Diagnostic ordinals in such a rejected graph are not asserted chain evidence. Disjoint address/asset operations do not need otherwise irrelevant relative-order requests. This helper is restricted to the current top/internal/ERC-20 transfer builders, and does not authorize or certify conversion semantics.

Seven new synthetic tests cover both cache/live entry points, both hash presentations of the external shared-balance counterexample, one missing index, the incoming-first upper bound 100, the outgoing-first upper bound 10, independent operations, supported verified execution positions and receipt log positions. The two unknown-order variants are rejected in both entry points, with no numeric interval returned. The external probe is retained with import/output adaptation and explicit handling of the newly correct cache rejection; the original four event facts and cases are unchanged. Its complete adaptation diff is in `checks/order_r2/check_order_interface_r2.diff`.

Windows 11 / Python 3.14.3 executed 75 selected tests: the 7 new tests plus 68 existing physical-fact, collector and LP regressions. All 75 passed; zero failures, errors or skips. This check used credential-variable removal and blocked socket access. It does not replace the final extracted-bundle validator, and it does not claim a Linux run.

Four R1 saved collections were rebuilt with precisely the same observed facts and labels. All four rebuilt graph JSON semantics and all event/joint interval endpoints equal their R1 counterparts. The extra order audit appears in `model_scope.json`; no graph assumption, objective, capacity or interval changed. The four graphs contain 85 event entries in total, which are not 85 distinct new events. An independent standard-library maximum-flow and rational-witness replay passed 23 intervals and 46 endpoint witnesses. These are mathematical checks of supported fixed graphs, not complete-chain or acquisition-completeness claims.

| Same-input graph | Events | Outcome |
|---|---:|---|
| Atomic live Dune | 11 | [0, 287.395] ETH, unchanged |
| Harmony live Dune | 4 | NO_OBSERVED_TARGET, unchanged |
| Atomic inherited cache | 12 | [0, 359.495] ETH, unchanged |
| Harmony inherited cache | 58 | Three prior joint groups and their event intervals unchanged |

Evidence is recorded in `checks/order_r2/targeted_regression_tests.json`, `order_interface_results_r2.json`, `same_input_replay.json`, `independent_fixed_graphs.json`, and the per-graph rebuilds under `checks/order_r2/graphs/`. Source and input SHA-256 values are included in the corresponding receipts. All activity in this subtask was offline; no chain data or labels were added, and R1 artifacts were only read.

Portable commands from the R2 tree (with the recorded R1 subset available at `<baseline-root>`):

```text
python -B -m unittest discover -s tests -p test_event_order_r2.py -v
python -B src/replay_order_same_input_r2.py --baseline <baseline-root> --output <separate-output>
python -B src/verify_fixed_graph_independent.py --root <separate-output> --derived-subdir graphs --output <audit-output.json>
```

The R2A source diff is `checks/order_r2/R2A_SOURCE_DIFF.patch`; newly authored files are supplied directly. External acceptance remains PENDING_REVIEW.
