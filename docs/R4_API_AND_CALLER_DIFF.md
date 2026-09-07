# R4 API and caller diff audit

Only R4 changed interfaces and their current src/tests callers; not a generic repository review.

The immutable R3 baseline has 65 source files. This snapshot has 80; 38 source and 13 test files differ. Existing named test functions removed: 0. Original R3 bytes remain unchanged.

| Group | Changed source | Caller/CLI closure |
|---|---|---|
| F01 | `context_ledger_r3.py`, `exact_fields_r4.py` | Required exact amount/status parsing feeds normalize_rows; replay and audits use the same parser. Unknown receipt metadata cannot create a zero-value fact. |
| F02 | `context_ledger_r3.py` | normalize_rows compares physical top/root identity and known status before failed-frame filtering; conflicting model facts are rejected by LP. |
| F03 | `audit_context_contracts_r4.py`, `context_ledger_r3.py`, `context_queries_r3.py`, `dune_r4.py`, `freeze_context_replay_r3.py` | build/freeze now require verified block time coverage. freeze_context_replay, evidence replay, and R4 Dune submission call verify_frozen_scope before coverage/completion or network dispatch. |
| F04 | `exact_fields_r4.py`, `provider_etherscan.py` | Etherscan fetch_interval still calls normalize_rows; malformed raw/status fields add gaps and prevent complete status. Provider transport remains injected, with no current real Etherscan activation. |
| F05 | `provider_receipts.py` | ReceiptEnricher __call__ is the injected detail_enricher; it compares known locators and explicit log identities before filling missing values. |
| F06 | `frontier_labels_r1.py`, `frontier_labels_r2.py`, `labels_history_r4.py`, `labels_policy.py` | Both frontier apply callers pass prior observation contents through baseline_observations. The historical helper outputs separate corrections; actor/role/service-stop values are preserved. |
| F07 | `labels_history_r4.py`, `labels_import.py` | parse retains its observation/outcome pair but emits batch/address failures; CLI adds frozen requested-address input, exits nonzero on failure and preserves old successful observation bytes. |
| F08 | `provider_etherscan.py`, `read_retry_r4.py` | Etherscan _page uses persistent ReadRetryStore; failure attempts and successful cache are separate; FetchResult counters/coverage propagate real attempts and incompleteness. |
| F09 | `validate_review_bundle_r1.py`, `validate_review_bundle_r2.py`, `validate_review_bundle_r3.py`, `validate_review_bundle_r4.py`, `validation_faults_r4.py`, `validation_result_r4.py` | All validator check/skip/failure aggregation goes through reserved validation fields; required commands and nonzero/malformed receipts cannot be summarized as PASS. |
| E01 | `budget.py`, `budget_r2.py`, `context_access_r3.py`, `context_access_r4.py`, `dune_live.py`, `dune_r1.py`, `dune_r2.py`, `dune_r4.py`, `legacy_guard_r4.py`, `network.py`, `pilot_actions_r2.py`, `provider_etherscan.py`, `read_retry_r4.py`, `rpc_context_r1.py` | R4 RPC/Dune use one migrated risk ledger, per-identity persistent attempts and clocked dispatch. R3 CLI routes active R4 work to new entrypoints. Legacy live entry restrictions are separately audited below. |
| WETH | `review_replay_r4.py`, `weth_component.py`, `weth_evidence.py`, `weth_trace_adapter_r4.py` | R4 replay calls replay_weth_r4, first reproduces R3 baseline, then validates original Dune chain and sealed unique execution-context/log proof. Old R3 adapter remains unchanged and 11/12 is separately reproducible. |
| DELIVERY_AND_INDEPENDENT_REPLAY | `independent_amount_audit_r4.py`, `independent_evidence_audit_r4.py`, `package_review_r4.py`, `review_replay_r4.py`, `validate_review_bundle_r4.py` | Safe publication, independent evidence/amount checks, and R4 private replay integration. |

The complete per-file JSON inventory includes exact old/new SHA-256, definitions, import callers and CLI presence. Unified source/test diffs are under `manifests/r4_code_diffs/`, with combined `ALL_SRC_TESTS.patch`. This includes added files and the three adjusted old test fixtures: verified boundary headers; a receipt positive control that first preserves/asserts rejection of the original conflicting locator; and an explicit unmarked historical work parameter for the old RPC transport positive control. Original response payloads and relevant assertions are retained.

## Live boundary

Old live constructors originally admitted active R4 work through independent legacy ledgers; reported to root before final packaging.
Current status: **CLOSED_SUPPORTED_R4_ENTRYPOINTS_GUARDED**. The final guard receipt binds all guard source/test hashes; actual Windows results are 11 new refusal/positive controls plus 12 existing RPC tests. Normal old constructors, live functions, migrations and standard legacy ledger paths reject active R4 private or public-policy workspaces before side effects; old R3 public policy alone is not blocked. Unscoped PublicNode transport checks explicit work or current module/cwd scope. New R4 costed RPC/Dune positive controls and unmarked historical workspaces remain available. No real credentials, provider request or private ledger modification was used for this audit.

Provider Etherscan/receipt APIs accept caller-injected transports and have no independent live credentials. Their current in-repository callers are controlled tests. Enabling them still requires the authorized cumulative transport wrapper; their reusable parser/retry APIs are not a separate grant.

F06/F07 historical results are derivative patches. Full source scan indexes remain local; the MIN package needs only summary/affected IDs and relevant two-probe rows. No full label library is copied by this audit.

WETH retains the original twelve scientific checks, the old R3 replay and unchanged raw evidence. The R4 adapter supplies an independently inspectable unique log-emitter proof without inventing frame logs or the missing old explorer request.

This is source/caller closure evidence. Final executable tests, finite real replay, artifact integrity and actual Windows/Linux platform results are recorded separately by final validation receipts. External acceptance remains PENDING_REVIEW.
