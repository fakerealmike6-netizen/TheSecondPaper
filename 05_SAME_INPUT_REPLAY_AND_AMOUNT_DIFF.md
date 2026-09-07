# Same-input ETH comparison

|Query|R3 baseline / R4 best context joint ETH|Same enhanced graph with balance information removed|
|---|---|---|
|Atomic|[359.495, 359.495]|[0, 359.495]|
|Harmony|[504.907298683760128997, 1554.999042315467482]|[0, 1554.999042315467482]|

Atomic reuses 5 accounts, 10 anchors, 12 value events and 11 fees; Harmony reuses 22 accounts, 45 anchors, 57 value events and 50 fees. All account reconciliations have zero residual and no fact conflict. Unknown external incoming source shares remain modeled, rather than silently treated as normal funds or new source injections. Known actual gas is counted once and source gas shares are solved.

The source evidence is the same. F01/F02 normalization and F03 dual-domain coverage now reject the supplied invalid counterexamples; the actual valid model JSON and all endpoint amounts remain unchanged. The historical provenance patches do not intersect either probe. WETH's separate adapter does not alter either ETH graph.

Each entry event, address–asset group and all-service union is optimized on the joint model. Independent upper bounds are not added. Both informed models exclude the all-downstream-zero allocation; matched information relaxation makes it feasible. Monotonicity is asserted only within the nested same-input graph pair. R2 sparse baseline graphs are replayed separately. Reviewer-supplied independent routines check 48 intervals, 100 rational witnesses and 1130 saved-evidence assertions.
