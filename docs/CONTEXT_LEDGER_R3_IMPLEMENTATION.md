# R3 evidence-to-ledger implementation

The actual execution entry is `context_ledger_r3.replay_manifest`, called by
`python src/context_ledger_r3.py replay --manifest configs/CONTEXT_REPLAY_R3.json --bundle-root . --name <pilot> --output <fresh-directory>`.
It performs zero network operations. The manifest contains relative paths and
SHA-256 identities; a relocated private review bundle is sufficient.

`targets` independently regrouped R2 physical facts into 5 Atomic and 22 Harmony
nonterminal ETH accounts. Both frozen graph hashes and all mechanical baseline
address/window/count fields matched. The original target queue remains unchanged.
`CONTEXT_TARGETS_ENHANCED.json` records one justified 31-block context extension:
an already modeled Harmony account made a real transfer to another modeled
account before its original first candidate receipt. The extra prior anchor and
missing interval were obtained, preserving one shared source variable. Its old
initial anchor became an additional intermediate constraint instead of being
discarded.

`replay_manifest` verifies each RPC dispatch request against its saved envelope,
the actual response body, member hash, receipt, request ID, and historical block
response. It rejects latest-state substitutions and missing results. Failed
transport receipts remain identifiable evidence of unsuccessful attempts and
never become balance facts. Dune replay checks the frozen SQL and scope hash,
job identity, actual submit/status responses, every unfiltered page, offsets,
pagination metadata, raw response sizes and hashes. Completeness is determined
for every required native data type and each account's contiguous interval.

`normalize_rows` uses integer wei, merges the same transaction across saved
windows and sources, merges root trace value with top-level value, removes
DELEGATECALL/CALLCODE/STATICCALL value as nonphysical capacity, and propagates
failed-ancestor rollback. Available trace child counts are checked against the
saved tree. Create and selfdestruct recipient fields have explicit adapters.
Withdrawals retain protocol block-end ordering. A fee-recipient or unsupported
protocol-credit hit requires additional accounting; it cannot silently establish
complete context. Actual R3 account windows contained no such hits.

`assemble_model` initializes each account from its own exact prior-block-end
balance, replays actual incoming, outgoing, and one physical fee per transaction,
and checks its post-window balance in integer wei. No balance is replaced with
zero when absent. Every retained anchor and gas fact receives a constraint
mapping or an explicit reason for exclusion. Fees paid by seed or background
senders outside modeled accounts are not charged to recipients. Ordinary
21000-gas ETH transfers use a transaction-begin net-fee constraint; the model
chooses the source-funded share. A receipt cross-check additionally bound the
Atomic seed's saved transaction identity and fee facts to RPC evidence.

The actual integrated inputs are:

| Query | Initial balances | Total anchors | Physical value flows | Modeled actual fees | Account/transaction rows | Zero-residual accounts |
|---|---:|---:|---:|---:|---:|---:|
| Atomic | 5 known | 10 | 12 | 11 | 17 | 5 of 5 |
| Harmony | 22 known | 45 | 57 | 50 | 99 | 22 of 22 |

The 55 anchors all reach the model: 27 initial balances, 27 closing constraints,
and one intermediate Harmony constraint. The 69 observed transaction fee facts
have 61 modeled payer fees and 8 explicit external-payer exclusions. Historical
code observations are empty for all 27 required accounts, plus the earlier
Harmony alignment state. Every actual account ledger has full declared native
coverage and zero residual; neither assertion is inferred from the other.

Harmony's seven added context movements consist of one transfer between modeled
accounts and six external inflows with known actual amounts. Their unknown
source shares are bounded variables in a conserved external reservoir, not
missing actual data or new source injections. They are reported separately in
`attribution_uncertainties.json`. Complete context is claimed only inside the
declared single-source, finite-window, first-service-stop model. It is not a
claim about all Ethereum paths or a unique legal attribution.

Tests in `test_context_ledger_r3.py` start from saved Dune and JSON-RPC response
shapes. The full hash-bound RPC/envelope/body adapter-to-ledger-to-LP test obtains
the analytical [70,80] result for other balance 20, seed 80, and transfer 90;
matched removal of balance information yields [0,80]. Other tests cover raw
tampering, position semantics, missing results, conflicting physical facts,
failed transactions and ancestors, one fee across duplicate traces/windows,
create/selfdestruct adapters, withdrawals, missing pages and interval holes,
zero residual without coverage, external fee payers, retained other inflows,
and deterministic repeated offline reconstruction. All 26 passed on the actual
Windows Python runtime. Final extracted-bundle validation is reported separately.
