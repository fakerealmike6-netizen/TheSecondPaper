# Amount comparison and explanation

All units below are ETH. Entries, address-assets and all-service unions are distinct objectives; independent upper endpoints are not added as a joint bound. Source/target/rules are identical only for the enhanced vs information-relaxed pair.

|Query|R2_BASELINE joint|BEST_AVAILABLE_CONTEXT joint|MATCHED_INFORMATION_RELAXED joint|
|---|---|---|---|
|atomic_simple_transfer|[0, 359.495]|[359.495, 359.495]|[0, 359.495]|
|harmony_high_branch|[0, 1554.999042315467482]|[504.907298683760128997, 1554.999042315467482]|[0, 1554.999042315467482]|

Atomic has five genuinely observed zero initial balances, one source injection, no additional incoming native flow,11 payer fees and exact closing balances. Its seed359.505457948863919325ETH equals service359.495 + actual modeledGas0.005482101864876 + final balances0.004975846999043325. The exact model thus excludes an all-zero downstream allocation.

Harmony retains57 actual flows:50 frozen candidate events, six external incoming flows with unknown conserved source shares, and one related-account context flow. It keeps normal-capital capacity rather than treating every observed incoming amount as source. Its positive joint lower bound is a conservation/actual-capacity consequence over the whole enhanced ledger. Each target can have a different lower bound: most independent target lower bounds remain0 even though the all-service lower bound is positive.

|Query / target alias|R2 address-asset interval|Enhanced address-asset interval|
|---|---|---|
|atomic_simple_transfer T01|[0, 359.495]|[359.495, 359.495]|
|harmony_high_branch T01|[0, 550]|[60.302368224708810999, 550]|
|harmony_high_branch T02|[0, 249.997530999558463]|[0, 249.997530999558463]|
|harmony_high_branch T03|[0, 268]|[0, 268]|
|harmony_high_branch T04|[0, 225]|[0, 225]|
|harmony_high_branch T05|[0, 275]|[0, 275]|
|harmony_high_branch T06|[0, 251]|[0, 251]|
|harmony_high_branch T07|[0, 5]|[0, 5]|

Aliases follow the private COMPARISON.json row order. Complete event intervals, alias-to-address identities, raw-unit endpoints, exact witnesses, dual constraint provenance and all-zero infeasibility certificates are in the MIN. Enhanced and relaxed models have the same variables/equalities; only actual-balance upper bounds are removed, so the nesting proof is structural. There is no asserted endpoint monotonicity between different old/new graphs, even though these particular upper bounds happen to agree. Candidate membership and labels are unchanged; newly integrated background data, actual balances, fees and timing explain the changes.
