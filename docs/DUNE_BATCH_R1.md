# Frozen actual-frontier union and offline replay

This module performs no network or ledger operations. The root alone submits SQL,
polls and exports through the separately budgeted R1 runner. Preparation requires
the latest label update and the actual unfinished frontier obtained by replaying
that frozen label snapshot against the same raw data first.

From the revision workspace:

```text
python -B src/dune_batch_r1.py prepare --frontier <latest-replay>/next_actual_frontier.json --name <fixed-probe-name> --label-manifest <latest-label-apply-manifest.json> --policy bootstrap/config/STAGE1B_R1_POLICY.json --project-root <project-root> --output <new-private-plan-directory>
python -B src/dune_batch_r1.py verify --manifest <new-private-plan-directory>/freeze_manifest.json
```

`--label-manifest` accepts the frontier-label module's applied snapshot manifest
(`label_snapshot_path`, relative to project root, and `registry_sha256`). A portable
wrapper may instead contain `registry_path`, relative to its own directory, and
the exact registry SHA-256. The wrapper must identify the same already frozen
snapshot; it is not permission to invent or edit labels. Registry resolution must
stay inside project root. Failed/unqueried identities remain gaps and continue as
UNKNOWN. Successful historical empty responses remain observations, not failures.

Preparation reads no reference-neighbor or target table. It selects only the named
probe's `INTERVAL_INCOMPLETE` entries whose depth permits another acquisition and
whose frozen label does not stop the branch. Every interval retains its exact
arrival, block/time bounds and original scope. The end time must equal the minimum
of arrival plus 90 days and the original fixed global end. Each complete reviewed
`build_interval_sql` fragment gains only matching raw-table date partitions. The
outer `UNION ALL` adds a deterministic `interval_id`; there is no LIMIT, Top-K or
other candidate filter. The SQL itself has not been tested against a real Dune
provider by this module's author; successful synthetic controls do not claim that.

The new plan directory contains `query.sql`, complete fragment SQL files,
`freeze_manifest.json`, source frontier/policy copies, the exact label manifest,
and a small slice of the frozen labels actually used. It pins the full registry
SHA without duplicating its complete third-party contents. Directory overwrite is
rejected. The root passes `query.sql` and `freeze_manifest.json` to
`RevisionLive.submit(..., kind='candidate', freeze_manifest=...)`. The root retains
all inherited budgets, clocks and resource counters; this preparation makes no
new allowance or assertion about current provider permissions.

After all actual export pages are saved:

```text
python -B src/dune_batch_r1.py load --folder <saved-R1-job> --work . --manifest <frozen-plan>/freeze_manifest.json --output <new-private-load-check.json>
```

`load_completed_batch` validates the job-pinned frozen manifest, reproduces every
SQL fragment and the entire union, verifies raw submission and terminal-status
receipt hashes and execution IDs, then applies `page_contract` to the complete
unfiltered page chain. Each saved page must equal its original raw response and
match its receipt hash, byte count, execution and offset/limit. Unknown interval
IDs and rows outside their bound address/time/block/asset/chain scope are rejected.
Only a complete whole-job export proves that a selected interval returned zero
rows. Missing pages never produce successful empty intervals.

Rows use the existing repaired `normalize_rows` and `PhysicalFactRegistry`.
`BatchSavedDuneProvider` inherits the existing `SavedDuneProvider` and rebuilds the
fact registry across old and new intervals before any interval is selected. A
conflicting physical fact cannot win through match ordering. The old gate source
`dune_observed_replay.py` is not modified.

```text
python -B src/dune_batch_r1.py replay --policy bootstrap/config/STAGE1B_R1_POLICY.json --events <exact-seed-event-pool> --members <query-members.csv> --label-manifest <latest-label-apply-manifest.json> --project-root <project-root> --jobs private/dune_live_jobs --batch-jobs private/dune_r1_jobs --work . --output <new-replay-directory>
```

Replay reads only the exact inherited seed from its source event pool. All other
candidate data come from validated saved Dune jobs. It writes collection, request
plan, fixed graph, model scope, source manifest, summary and next actual frontier.
Unfinished/non-batch candidate jobs are recorded as rejected, not filled with old
reference-derived caches. Output directories must be new. It uses the same label
registry hash for service termination and future-frontier generation. Both missing
registry rows and explicit `UNQUERIED`, `LOOKUP_FAILED`, `BUDGET_BLOCKED` remain
label gaps; valid empty outcomes continue UNKNOWN without being made into services.
Lookup statuses such as `LOOKUP_FAILED_RESOURCE_CAP` map to the Collector's
`LOOKUP_FAILED` while preserving the full detailed status. Pure ownership conflicts
map to UNKNOWN and add `LABEL_CONFLICT`; they never imply a protocol boundary.
This label conflict is separate from physical-event conflicts that quarantine facts
and block the LP. The original gate replay source remains unchanged.

`live_query_interval_count` counts matched intervals;
`live_logical_job_count` counts distinct parent SQL jobs;
`live_exported_rows` counts each used batch's complete exported row set once;
`live_interval_rows` counts rows across its used interval partitions. These may
include duplicate physical events across overlapping intervals. Distinct events
are reconciled independently and never inflated into algorithm performance.
Execution cost is a parent-job field and must not be summed once per interval.
Replay itself issues zero requests; actual cost/counts belong to the root ledger.
Complete provider intervals still do not certify initial balances, all labels,
unsupported components, global chain coverage or unconditional real LP bounds.

The saved `scope_freeze_path` may be absolute in the root runner's local receipt.
For a relocated review tree, pass its explicit relocated manifest to
`load_completed_batch(folder, work, freeze_path)`, or construct
`BatchSavedDuneProvider(old_jobs, work, batch_specs=[(folder, freeze_path), ...])`.
All manifest contents still require the original job-pinned SHA-256.

Offline validation:

```text
python -B -m unittest discover -s tests -p "test_dune_batch_r1.py" -v
```

All 14 test cases use synthetic fixtures and temporary project-local files. They
cover SQL/scope freeze tampering, policy bounds, label boundaries, zero-row proof,
missing/unsafe pagination, source/interval mismatch, physical conflict quarantine,
exact decimal wire-to-root persistence, and exact-seed end-to-end replay. No real
provider was invoked by these tests.
