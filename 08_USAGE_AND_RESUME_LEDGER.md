# Cumulative usage and resume ledger

Authorization STAGE1B_R3_CONTEXT_FIRST_V1 carries the same Dune STAGE1B_R2_CAP20_CUMULATIVE100_V1 grant. Single execution20 is USER_CONFIRMED. All original Stage1B/R1/R2/R3 risk shares100;80 is a warning only. No extra credits, upgrade, card or overage action was taken.

|Item|Credits|
|---|---:|
|Inherited risk|39.587758616|
|Inherited reliable known charge lower bound|21.005321989|
|R3 terminal execution charges|1.840029414|
|R3 export reserve upper, not invoice|5|
|R3 provisional/terminal discrepancy retained|0.740085063|
|R3 incremental total risk|7.580114477|
|Cumulative reliable known charges|22.845351403|
|Cumulative export upper, not invoice|15.000000000|
|Cumulative unresolved risk|9.322521690|
|Cumulative total risk upper|47.167873093|
|Remaining of100|52.832126907|

Reliable terminal fees replace the corresponding unknown20 component; export reserves are based on the complete actual result metadata and inherited rate evidence. The underlying legacy reserved column also holds reliable component fees, so it is not an amount of wholly unknown spending. Provisional execution peaks above terminal fees are carried as discrepancy risk, without relabeling those peaks as actual invoices. Account usage was read at startup and collection end; rounded account differences were not treated as precise per-job bills.

Three Small/Medium-authorized jobs actually ran on Medium and fully exported24,114,2 rows. The last job covered only a missing31-block single-account window. There is no new fixed1 export restriction: the conservative whole-result envelopes were1,3,1 from actual shapes.

RPC/REST cumulative160/500 individual operations (R3 new155, including22 interrupted attempts retained and22 successful once-only recovery calls). Alchemy documented-method bound3100CU, cumulative cap50000; final account CU invoice is unavailable, so actual CU is not claimed. The included allowance was user-confirmed; no billing-toggle verification is claimed. Historical archive balance access succeeded. WETH debug trace was Free-tier denied and was not retried or upgraded. BigQuery and MetaSleuth added0 calls.

New context clocks: Atomic32.688399s; Harmony105.927641s, each below3600s. Original candidate clocks remain3586.162299s and3640.705699s. Conservative cumulative raw occupancy10076683 bytes /536870912, including uncertainty from the interrupted read. No clock, successful page or grant is reset by restart.

Per-job and per-batch accounting is in derived/usage_r3/. Saved successful evidence must be reused. External acceptance PENDING_REVIEW.
