# R4 bounded read recovery

Three total read attempts use one persistent logical selector across process restart, new run names and batch splitting. Each attempt has durable intent, retry_of, outcome, evidence and a separate cost reservation. Failed/unknown responses never overwrite successful cache entries. Empty successful results remain reusable. Unproven old failure totals do not receive fresh attempts.

HTTP 408/429/500/502/503/504, timeout, incomplete read, reset and temporary DNS/provider errors use bounded full-jitter exponential backoff. Retry-After is not shortened; a delay beyond the remaining deadline is persisted as DEFERRED. Permanent authentication, entitlement, schema and physical-fact errors are explicit failures. All fault injection uses synthetic transport and clocks, not paid endpoints.

Historical RPC batch responses are matched by exact ID. Only missing/failed members are retried; successful siblings and valid zero balances are cached. Each dispatched member counts an operation and retains its CU upper bound. The 500-operation / 50000-CU cumulative limits do not reset.

Dune known executions continue under the same ID. Successful status observations can continue beyond three polls; each failed read chain has its own maximum of three total attempts. Unknown SQL creation cannot be automatically re-POSTed. A known terminal failed SQL permits at most one justified retry under the same work-unit lineage. Results pin execution, offset, limit and columns; no guessed next page or SQL reexecution for download. Old uncertain export risk and each added retry reservation are additive, even when the old bound exceeds the current page estimate.

Independent integration review added two regressions: mismatched old RPC plan/envelope and old export risk 10 plus new request bound 3. Both were fixed before gate. F09 uses real validator subprocess faults (mismatch, nonzero exit, missing output, malformed JSON), retains failed outputs, then runs the normal expected-gap positive control. Required SKIP or unknown receipt schemas cannot be aggregated as PASS.
