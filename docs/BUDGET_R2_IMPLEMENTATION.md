# Stage1B-R2 unified budget and transport

The effective authorization is `STAGE1B_R2_CAP20_CUMULATIVE100_V1`: the cumulative original Stage1B plus all revisions ceiling is 100 credits. Ordinary Small/Medium SQL has a 20-credit execution ceiling. The 80-credit threshold reports progress and does not pause. Historical authorization records remain unchanged; their former 10/20 parent/subbucket and 1/2/5/6 per-job controls are not consulted for new R2 jobs. No account setting is changed by this code.

The user's account confirmation is recorded as `USER_CONFIRMED`, with the R2 receipt time; actual account setting modification time is unknown. It is not API or browser verification. The account's trial eligibility/current included allowance remains separate from the project authorization. No paid overage, card, upgrade, or replenishment is authorized.

## Migration evidence

`private/BUDGET_MIGRATION.json` and `checks/BUDGET_MIGRATION_CHECK.json` bind the original ledger and attempt database through SHA-256 and SQLite consistent backups. Before migration the R1 checkpoint states that new network submissions are disabled, and `Get-Process python,python3,pythonw` returned no processes. This is a process-name check, not a claim that privileged command-line inspection succeeded.

The original SQLite files were read through read-only connections. The SQLite backup API incorporated committed state. Before/after source hashes match. Every old table is identical in the new ledger except the active Dune limit row, whose cap becomes 100; old `r1_meta`, `dune_risk`, exception, job, amount and observation rows remain exact. New accounting uses separate `r2_*` tables. Non-Dune allowances and usage are retained, including five RPC operations. Four existing durable request attempts are copied, with zero unresolved attempts at migration. Older original-run successful page files that predate that journal remain source-bound cached evidence; the migration does not invent journal history for them.

The same destination initialization is idempotent. Another sibling revision already holding the authorization causes migration refusal. Source ledger files and R1 tags are never rewritten. The root orchestrator also uses a single exclusive network worker lock; the ledger transaction refuses a second in-flight SQL reservation.

At the migration boundary, the exact ledger values are:

| Quantity | Credits |
|---|---:|
| Cumulative risk retained | 16.433088236 |
| Final actual job totals | 2 |
| Known actual lower bound including execution receipts | 7.116882357 |
| Documented export upper, not actual | 1 |
| Unknown/pending residual beyond known actual and documented upper | 8.316205879 |
| Available under cumulative100 | 83.566911764 |

These are migration values, not final post-collection values. The five original jobs retain their aggregate 10-credit whole-job risk. The closed R1 label job retains its execution actual 3.433088236 plus export upper 1. The two failed R1 SQL attempts retain their final actual total 2. Known actual is contained within each whole-job reservation and is not added again.

## Request and export accounting

New jobs reserve execution20 and export0 before submit; a frozen export plan is mandatory. Candidate jobs additionally verify the complete `dune_batch_r1` interval/label/SQL freeze and exact SQL digest. Accepted submit and each page are journaled before dispatch. Unknown submit/page responses cannot be retried with changed SQL, job label, offset, or page limit.

A successful status response must explicitly name the saved execution ID. Reliable terminal execution costs atomically replace the execution20 component immediately, before an export or subsequent SQL. Nonterminal costs are observations and do not prematurely reduce the execution risk. Observed execution cost above20 is retained in full and halts new chargeable operations.

Before every page, the transport reserves the whole required result stream. The envelope uses server total row/byte/column metadata and the inherited documented Free20 credits per decimal MB plus datapoint evidence. It takes the maximum published scheme, conservatively rounds each possible request upward to a whole credit, and uses the entire known result metadata to bound any one page when metering scope is ambiguous. This is an upper bound, not a provider minimum or final bill. Page limits up to1000 reduce unnecessary requests. Verified page metadata can expand the envelope; it can never silently release an already dispatched or uncertain page's risk. An observed expansion beyond the remaining pool is still recorded in full and halts further dispatch.

If necessary export cannot fit the cumulative pool, execution identity and result metadata are retained and export is not called. When all pages verify and no unknown attempts remain, the job closes as `BOUNDED_ACCOUNTING_NOT_FINAL`; final export actual remains null. A terminal execution with no exported pages may close at the reported execution actual only. No usage delta is used as a precise job invoice, and no repeated usage reads are made to wait for absent billing fields.

All page validation retains execution identity, exact offsets, frozen page size, total rows, schema, page content hashes, and terminal-page invariants. A cached completed stream returns cached progress without another export. Direct low-level SQL/results invocation is guarded. Only Small/Medium and normal Dune account cap behavior are used. Each HTTP response read is bounded at16MiB, with a cumulative512MiB pre-request check when the root's baseline resource mapping is present; a response reaching the read bound is retained as uncertain, never parsed as a complete result.

## Focused verification and interface

21 unique budget/transport synthetic tests passed on Windows with Python3.14.3 using `python -m unittest test_budget_r2 test_dune_r2 -q` and `PYTHONPATH=src;tests`. They cover idempotent migration initialization, retained old risk, new jobs above old subbucket capacity, immediate terminal20 reduction, nonterminal retention, export above1,80 warning vs100 refusal, observed overruns, zero Meta authorization, duplicate jobs, no-retry submit/page failures, gate refusal, missing candidate freeze, missing status identity, missing export plan, and Large rejection. Linux was not exercised by this subtask; final package verification is reported by the root validation task.

Use `RevisionLive(work).submit(sqlpath,label,kind,freeze_manifest,performance)`; it returns `job_folder`. `poll(folder)` returns the state/cost/result metadata summary. `export(folder,limit=1000,offset=0)` returns a `progress` object containing `complete` and `next_offset`. `settle(folder)` returns final/bounded accounting status. CLI actions are `migrate`, `usage`, `submit`, `poll`/`status`, `export`, `settle`, and `snapshot`.

`src/budget_r1.py`, `src/dune_r1.py`, historical tests, original ledgers, original delivery packages, and original tags are unchanged. Real network execution belongs to the root worker; this implementation subtask did not make a provider request.
