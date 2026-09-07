# Portable R3 validation

Run `src/validate_review_bundle_r3.py --tree <extracted-tree> --output <new-sibling-output> --kind public` or `--kind min`.
The output directory must be new and outside the extracted input. The validator
copies code/tests into that output before running them. It reuses the repaired
R1 environment, Python child, `posix_spawn`, network and filesystem guards.
Validation cannot send provider requests, access credentials through children,
or rewrite the frozen input. The platform and Python version in the receipt
describe only the environment actually used for this invocation.

Public validation verifies the R3 file manifest and payload correspondence,
the exact public allowlist/content scan, all bundled synthetic unit tests and
the original 12-scenario/32-objective controlled LP suite. Private replay is
explicitly excluded from the public artifact and is never reported as executed.

MIN also requires `configs/PRIVATE_VALIDATION_R3.json` with exactly these keys:

```json
{
  "schema_version": "stage1b-r3-private-validation-v1",
  "context_manifest": "configs/CONTEXT_REPLAY_R3.json",
  "r2_baseline": [
    {"name": "atomic_simple_transfer", "graph": "baseline/r2/atomic/fixed_graph.json", "expected_lp": "baseline/r2/atomic/lp_fixed_graph_result.json"},
    {"name": "harmony_high_branch", "graph": "baseline/r2/harmony/fixed_graph.json", "expected_lp": "baseline/r2/harmony/lp_fixed_graph_result.json"}
  ],
  "expected_context": [
    {"name": "atomic_simple_transfer", "ledger": "derived/atomic/ASSEMBLED_CONTEXT.json", "amounts": "derived/atomic/CONTEXT_AMOUNT_RESULTS.json"},
    {"name": "harmony_high_branch", "ledger": "derived/harmony/ASSEMBLED_CONTEXT.json", "amounts": "derived/harmony/CONTEXT_AMOUNT_RESULTS.json"}
  ]
}
```

The example relative locations are illustrative; the actual bundle config binds
its actual paths. Exactly the two fixed pilot names are required. No custom
command, arbitrary Python expression, absolute path or path traversal is accepted.

`context_manifest` uses `stage1b-r3-context-replay-v1`, the same actual input
format as `context_ledger_r3.replay_manifest`. Each source entry has its portable
relative path and SHA-256. The worker verifies Dune frozen SQL/scope, accepted
execution, terminal status, complete pagination and original response bytes;
it verifies RPC intent, operation identity, response envelope and original body.
Failed requests remain evidence gaps. Normalized balance facts alone cannot
replace those source responses in the private replay.

The `ledger` file is the complete object returned by `replay_manifest`, including
model input, anchors, reconstructed ledger, reconciliation, fact/constraint
provenance, gaps, exclusions, completion status and used-source manifest. The
worker rebuilds this object from evidence and compares all facts. Only source
manifest path relocation is excluded; file hashes/lengths and scientific facts
remain part of the comparison.

The worker solves the enhanced and same-graph information-relaxed models again,
compares every event/address/all-service joint endpoint and model status, and
audits every exact witness in both the package and new result through independent
integer/rational ledger replay. Different valid optimal witnesses across solver
versions are allowed. Missing witnesses cannot retain an exact result claim.
Unknown solver results are never promoted to proof. R2 baseline graphs are
separately recomputed with the original graph runner; no cross-graph monotonicity
is asserted.

The strict optional `weth` object has only `manifest` and `expected_result`
relative paths. The manifest must be named `INPUT_MANIFEST.json` and use the
fixed `stage1b-r3-weth-replay-input-v1` schema. The MIN worker verifies each
listed source's SHA-256/length, calls the fixed offline
`weth_source_adapter_r3.replay_weth_manifest` entry point, and compares the full
reproduced component result with the bundled result. There is no custom command
field. The real remaining log-to-frame gap and disabled conversion must survive
replay; a source/hash check alone is not reported as a component replay.

Receipts are written to the external validation output. An extracted-package
PASS establishes the checks actually listed, not current RPC availability,
current quota, whole-chain truth or external scientific acceptance. External
acceptance remains `PENDING_REVIEW`.
