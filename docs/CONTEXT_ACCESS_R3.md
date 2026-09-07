# R3 context access and accounting

`src/context_access_r3.py` provides the actual R3 finite context access entry point.
Import and migration make no provider requests. The root worker owns all live
calls; test transports produce `SYNTHETIC_TRANSPORT` evidence and cannot establish
real-provider capability.

The migration uses SQLite read-only backup snapshots of the stopped R2 budget
and request-attempt journal. All 13 original tables, including 24 amount rows,
23 job rows, 17 execution/export components and 61 observations, are preserved
exactly. R3 has one new writer database. The cumulative risk was read from the
actual ledger as 39.587758616 credits: known terminal execution charges
21.005321989, documented export upper bounds 10, and other uncertainty
8.582436627. These are distinct classifications of the same occupied pool.
The cap remains 100; this continuation grants no additional 100 credits.

The established R2 Dune financial and page schemas remain in use. New R3 job
records are separately identified by `r3_journal`, while the compatibility
directory is named `private/dune_r2_jobs`. The snapshot field `r2_risk` therefore
continues to mean all jobs using that schema; revision reports must select R3
jobs through the R3 journal or compare against the immutable migration snapshot.
Unknown executions reserve 20 and reliable terminal observations reduce the
execution component. Full-result metadata and the inherited rate evidence bound
every export before dispatch. Page attempts and uncertain requests survive
restarts and cannot be silently resubmitted.

The R3 context clock is an explicitly separate 3600-second budget per query.
Shared necessary context time counts against both queries. Each serial live
work unit has an exclusive file lock and durable start/end record. An unclosed
record blocks restart until inspected. Old candidate collection time is retained
unchanged. Raw-byte accounting carries the original conservative occupancy and
adds new saved artifacts and uncertainty from failed bounded reads.

Alchemy uses only the already configured `ALCHEMY_API_KEY`, in memory in the
official HTTPS request; neither the credential nor its authenticated URL is
stored, printed, hashed or followed through redirects. The root records current
included allowance separately from unverified account settings. Permission to
incur additional paid overage is false. The confirmed included allowance and
the cumulative 50000-CU/500-operation limits jointly bound each request.
Every individual RPC member, including an HTTP failure, counts as an attempted
operation. Documented method CU costs are reserved as upper bounds, and are not
misreported as an independently observed final account bill. A documented
zero-CU `eth_chainId` still counts as one RPC operation.

One provider has at most five capability operations. Real bulk history requires
successful mainnet identity and fixed historical balance capability first.
`latest`, transaction sending, signing, arbitrary methods and unbounded block
transaction bodies are rejected. Successful zero balances remain evidence;
missing and malformed results remain missing. Normal collection reuses cached
operations instead of submitting the same logical request again.

Entry points are `migrate`, `snapshot`, `usage`, `rpc` and `dune`. RPC input is a
finite JSON list of exact `method`/`params` pairs. A Dune freeze has
`r3_context_scope: true`, the exact `sql_sha256` and an `export_plan`. All live
entry points require a source-bound `CONTINUATION_GATE_R3.json` and a unique
work-unit clock. Resume uses a saved accepted execution and verified next page;
it never sends replacement SQL just to retrieve successful saved results.

Twenty directed Windows/Python 3.14.3 tests cover migration identity, retained
risk, clock recovery, no duplicate dispatch, finite capability limits, known
zero balances, zero-CU operations, failed-request accounting, credential echo
withholding and allowance confirmation that cannot refill a budget. The initial
test-only SQLite handle cleanup failure was retained and fixed with an explicit
connection close; no isolation test or protection was removed.

The root may explicitly select one optimized retry of an actual `TimeoutError`
or `IncompleteRead` with no persisted response body. This is a read-only RPC
exception: at most five operations per split retry, unchanged method/parameters,
one retry per original identity, durable original-to-retry linkage, a documented
reason and separate reservations. Original operations, CU and raw uncertainty
remain occupied. Successful evidence is never fetched again through this route.
The Dune uncertain submission/export prohibition remains unchanged.
