# Migration report category addendum

The gate-bound `checks/BUDGET_MIGRATION_CHECK.json` retains its original SHA-256 `9a61189ba37d6ba2afc3326a1ff601d4615ec3af9944a294f322f5d77fc40917`. Its initial snapshot predates the reporting-only separation of an inherited proven export upper from unclassified pending risk. No cost, reservation, original ledger, or authorization was changed.

The authoritative gate snapshot and current `budget_r2.py` categorization identify 1 credit of the inherited residual as the already documented, closed R1 label export upper. Thus the inherited state is known actual lower bound 7.116882357, export upper 1, other unknown/pending residual 8.316205879, total risk 16.433088236. The original migration report groups the latter two together as 9.316205879. The total and remaining83.566911764 are identical under both presentations.

The implementation providing this classification is `src/budget_r2.py`, SHA-256 `43c46b369aa98ee4bdaf0703901de611518ce91aaeee9aa9311fae6ba00b4e7c`, already bound by `CONTINUATION_GATE.json`. Its inherited evidence is the unchanged R1 `dune_risk` row with status `BOUNDED_ACCOUNTING_NOT_FINAL`; it does not infer an invoice from an upper bound.
