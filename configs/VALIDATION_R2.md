# Stage1B-R2 portable extracted-bundle validation

Use a fresh validation output directory outside the frozen extracted tree:

```text
python -B src/validate_review_bundle_r2.py --tree <public-extracted> --output <new-public-output> --kind public
python -B src/validate_review_bundle_r2.py --tree <min-extracted> --output <new-min-output> --kind min
```

The R2 entry point reuses `validate_review_bundle_r1.Validator`'s repaired isolation machinery: a fresh code/test/fixture mirror, credential removal at every Python dispatch, the inherited network/DNS guard, a write boundary confined to the new output, and the Python-command allowlist. It does not alter or disable those controls. Original extracted input hashes are recorded before validation and checked afterward.

Both bundle kinds verify the R2 file manifest and their side of the frozen payload mapping, run the current synthetic tests with zero failures/errors/skips, and run 12 controlled LP scenarios with 32 Oracle comparisons. Test counts are obtained from actual execution. Public review explicitly omits private replay; an omitted private replay is not described as passed.

MIN additionally rebuilds four R1 saved collection snapshots with current code, compares graph semantics and all event/joint endpoints, runs the existing independent same-asset proof, and replays the latest saved Dune intervals/batches plus the exact seeds. The latest comparison checks candidate/context physical facts, processed states, service stops, remaining frontiers, coverage and page/source hashes, labels or the verified slice source chain, fixed graphs, and event/joint LP results. Only provenance/location text and runtime timing measurements are excluded. Seed raw-evidence identity remains checked; compact seed/member CSV byte identities are recorded separately.

MIN does not require the unchanged complete B/C reference tables or the older R1 mandatory `private_checks` sequence. Those omissions are explicit scope decisions for this R2 portable subset, not skipped failures of a claimed complete R1 review.

The default private descriptor is `configs/PRIVATE_VALIDATION_R2.json`. `--manifest` can select another relative descriptor within the frozen tree. Every path is portable and must resolve within that tree. The descriptor has these keys:

```json
{
  "schema_version": "stage1b-r2-private-validation-v1",
  "baseline_root": "baseline/r1",
  "policy": "configs/STAGE1B_R2_POLICY.json",
  "seeds": "private/seed_inputs/value_events_normalized.csv.gz",
  "members": "private/seed_inputs/reference_query_members.csv",
  "registry": "private/labels/address_registry.csv.gz",
  "label_manifest": "private/labels/portable_label_manifest.json",
  "label_slice_mapping": {
    "source_apply_manifest": "private/labels/source_apply_manifest.json"
  },
  "old_jobs": "private/dune_live_jobs",
  "new_jobs": "private/dune_r2_jobs",
  "batch_specs": [
    {
      "folder": "private/dune_r2_jobs/example_completed_candidate",
      "freeze": "private/frozen_batches/example_batch/freeze_manifest.json"
    }
  ],
  "expected_graph_root": "derived/new_observed_graph",
  "work": "."
}
```

`label_slice_mapping` is optional only when a slice is not being used. A slice uses the existing R1 verified source-apply-manifest mapping format; the original apply manifest is included byte-identically and its hash binds the full-registry identity. This checks the supplied provenance chain and equal replay outputs, not membership against a withheld full registry. `registry` bytes must match the portable label manifest's `registry_sha256`.

`reference_delta` is an optional in-tree directory containing a finite reference delta payload. When declared, the validator runs `reference_delta_r2.py verify --output <directory>` under the same isolation guard. This read-only command checks the delta payload hashes, replays affected queries under both identity slices, and recomposes metrics using the unchanged projection. It requires no complete baseline reference library; its verification result is recorded separately from the four graph replays.

Every `QUERY_STATE_COMPLETED` job of `kind=candidate` directly inside `new_jobs` must be named exactly once in `batch_specs`. Failed, incomplete or noncandidate jobs may be retained and are recorded as excluded evidence, never accepted as complete interval data. The batch's original `job.json`, SQL, pages, receipt files, entire frozen batch input set, and referenced raw responses remain byte-identical. Relocated freezes are passed explicitly; historical absolute paths embedded as metadata are not followed. The original five completed jobs remain under `old_jobs` with their referenced raw paths relative to `work`.

The baseline subset contains these R1 relative folders beneath `baseline_root`:

```text
derived/label_update_success_same_raw/atomic_simple_transfer/
derived/label_update_success_same_raw/harmony_high_branch/
derived/bugfix_only_same_input/cache_probe/atomic_simple_transfer/
derived/bugfix_only_same_input/cache_probe/harmony_high_branch/
```

Each folder needs `fixed_graph.json`, `lp/lp_fixed_graph_result.json`, and `collection.json` for live graphs or `cached_collection.json` for cache graphs. No raw historical reference pool is needed for this same-input graph rebuild.

`expected_graph_root` needs `source_manifest.json`, `next_actual_frontier.json`, and each pilot's `collection.json`, `fixed_graph.json`, `status.json`, and `lp/lp_fixed_graph_result.json`. These are the frozen outputs against which current code is checked. Expected LP JSON must bind its graph's bytes. Missing mandatory data, rejected required jobs or differing facts/endpoints cause a failure; incomplete real collection may replay successfully while remaining scientifically PARTIAL.

The optional public descriptor, if used, must contain exactly:

```json
{"schema_version": "stage1b-r2-public-validation-v1"}
```

It cannot add arbitrary commands or private data mappings. Both receipts identify the actual host platform and Python version; no unexecuted platform is inferred. Final extracted-bundle validation and remote publication are separate from the seven synthetic orchestration tests of the R2 wrapper. External acceptance remains PENDING_REVIEW.
