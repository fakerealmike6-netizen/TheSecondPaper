# R1 actual-frontier label import

`src/frontier_labels_r1.py` has no network client and never updates a budget.
The root worker owns authorization, SQL submission, polling, exports and cost
settlement. The frozen queue comes exclusively from actual collection states
whose identity status is `UNQUERIED`. Successful previous MetaSleuth responses,
including empty responses, and completed local source opportunities prevent
repeat queries. A reference target list is never an input to selection.

```
python -B src/frontier_labels_r1.py freeze --work WORK --project-root PROJECT_ROOT --batch batch_01
python -B src/frontier_labels_r1.py apply --work WORK --project-root PROJECT_ROOT --batch batch_01 --job-dir SAVED_JOB
```

Optional `--collection-root` and `--base-snapshot` select an explicitly saved
later actual-frontier state and the preceding frozen label version. They do not
grant extra addresses or jobs. Root enforces the revision-wide 40-address,
three-label-job limits, including failed executions and optimized SQL jobs.

The queue and source rules are frozen before submission. Each output address
has four complete JSON arrays and four original match counts. There is no SQL
row limit, `DISTINCT`, array slicing, or match truncation. Column types come from
previously saved successful schema results. Varbinary values are hex encoded;
other selected primitive values are strings, and null remains JSON null. The
four source-table matches may include multiple rows and duplicates; all are
retained. A zero-count array means no match in that source at this execution,
not a globally unidentified or non-service address.

The importer verifies frozen-file hashes, SQL identity, final execution state,
every saved result receipt and raw response hash, exact parsed/raw equality,
execution and pagination metadata, all frozen addresses, all four sources, and
each array's match count. Failed or incomplete jobs cannot create labels. A
completed empty lookup creates durable context observations and an `UNKNOWN`
registry row with `lookup_status=COMPLETED_FOUR_TABLE_OPPORTUNITY`. UNKNOWN still
continues through the collector.

Dune's internally consistent explicit attribution keeps the inherited priority.
Persona, usage and deployer labels do not establish control. Missing generation
methods remain null; `static` is not relabeled manual. A programmatic deposit
claim retains its disclosed programmatic origin. Owner roles use the inherited
role mapping and already approved local owner-details cache; an unseen owner
name cannot by itself become a service. Missing owner category metadata is
recorded. Raw other-source and Dune internal conflicts survive adoption.

Successful import writes an independent full snapshot under
`derived/label_snapshots/`, plus new observations, per-source opportunities,
per-address adoption delta and row-by-row B/C delta under the batch directory.
`apply_manifest.json` provides `label_snapshot_path` relative to PROJECT_ROOT,
`registry_sha256`, `observations_sha256`, `label_snapshot_version`, and actual
provider execution identity. The root must replay the same saved raw pilot
facts with that version before further collection. Only affected finite
reference queries are re-evaluated; unaffected relations retain their prior
state. Label growth is not algorithm performance and does not finalize a paper
denominator.

One specifically reviewed optimization can be prepared after a recorded
resource-cap failure:

```
python -B src/frontier_labels_r1.py optimize-static --work WORK --project-root PROJECT_ROOT --job-dir FAILED_JOB
```

It preserves the original frozen version and identical queue/source-rule
bytes, then replaces each dynamic join filter by a literal address `IN` filter
to permit earlier predicate pushdown. The optimized version records the failed
execution, raw SQL hash and line-ending normalization. It is a new chargeable
logical job, not an automatic retry. Performance is unverified until root
executes it. A second cap failure pauses label SQL; this module neither raises
the cap nor submits another attempt.

When both reviewed capped attempts have failed, their saved final status and
raw receipt hashes can be recorded without claiming any source result:

```
python -B src/frontier_labels_r1.py record-failure --work WORK --project-root PROJECT_ROOT --job-dirs FIRST_FAILED_JOB SECOND_FAILED_JOB
```

This writes 36 failed source opportunities for the initial nine-address cohort,
zero new label observations, and a distinct registry version containing nine
UNKNOWN context rows with `lookup_status=LOOKUP_FAILED_RESOURCE_CAP`. The
observation source is reused by `observations_source_path` and its SHA-256;
there is no copied or fabricated observation file in this context-only version.
The finite B/C reference pool is replayed and compared row by row. A failed
lookup remains a recorded gap and does not stop an UNKNOWN collector frontier.
The same saved pilot facts must be replayed under the new lookup version before
continuation. This path performs no network request or ledger mutation.

If the user separately authorizes one further capped execution of the same
frozen SQL, successful result import is compatible with a new authorization
logical job ID and a directory unrelated to the SQL hash. The saved SQL,
execution identity, complete pages and raw receipts must still match exactly.
No such authorization or execution is created by this module. If that third
execution also fails, pass all three saved job directories to `record-failure`.
It creates `failed_lookup_revision_02` and a second independent lookup snapshot,
retains each execution's actual observed charge, and preserves revision 01.

The MIN bundle's private replay inputs can be assembled from any final applied
manifest without altering it:

```
python -B src/frontier_labels_r1.py portable --work WORK --project-root PROJECT_ROOT --label-manifest FINAL_APPLY_MANIFEST --output NEW_PRIVATE_DIRECTORY
```

The portable manifest uses a relative `registry_path`, pins the slice hash and
the original full-registry hash, and records the source observation hash without
copying the full observations. Selection covers immutable finite-reference and
all inherited canonical-cache endpoints, registered reference target rows,
actual collector states/context, and the initial frontier cohort. Missing
registry rows remain missing; existing rows, including failed-lookup status,
retain every column. Selection is solely for an identity replay slice and never
supplies candidate-provider neighbors.

Five immutable reference fact/membership inputs are copied byte for byte.
Earlier identity slices are excluded; a new identity slice uses the final
registry. Per-address and per-relation differences retain original paths and
SHA-256 in a file mapping. Offline validation replays all finite reference rows
and compares full versus sliced identities on both saved live-provider pilots
and both sparse-cache pilots. A later final label version must use a new output
directory and a fresh validation. These inputs and differences are private;
they do not belong in the public package.

Public tests are synthetic and offline:

```
python -B -m unittest discover -s tests -p test_frontier_labels_r1.py -v
```

Queues, owner-details caches, full labels, actual job pages, reference subsets,
and private manifests must stay out of the public source export.
