# F06/F07 label provenance and failed-import repair

`labels_policy.resolve_address` now retains a prior actor and role together with
actual prior supporting observations. Prior IDs alone do not establish what an
observation says. The optional `baseline_observations` argument carries the frozen
old group; R1 and R2 frontier callers capture it before appending new observations.
The fallback for older callers uses only saved baseline IDs whose actual contents
are present. Different-actor or different-role observations are not adopted as
support. Missing old support is `PROVENANCE_UNRESOLVED` with
`HISTORICAL_EVIDENCE_GAP`; it does not invent an ID or change the retained actor.

Dune's explicit internally consistent priority, the approved BitPay lexical alias,
internal conflicts, first-service termination and targeted-source fallback remain.
Registry and delta serialization preserve the supporting source versions and new
provenance fields. The label policy has an explicit R4 version.

`labels_import.parse` keeps its observations/outcomes return pair. Provider failure,
malformed success, transport failure and missing/duplicate/unexpected requested
members now produce explicit failed outcomes. A frozen roster produces address
outcomes; absence of a roster preserves a batch failure without guessing addresses.
A genuine empty address record is `SUCCESS_EMPTY`, while an omitted expected
address is unresolved. The CLI returns 2 for a failed/partial batch, 0 for success,
appends failure outcomes, and never overwrites an existing successful observation
with a failed parse. Conflicting reuse of an observation ID is rejected.

The unmodified R3 F06/F07 examples were reproduced from the immutable baseline.
Desired-behavior regressions include actual CLI subprocesses, both retained-ID
positive and negative controls, partial batches, immutable success preservation,
historical pointer remediation and a synthetic misleading historical outcome.
The original label/frontier regressions remain in the suite. Provider verification
for these repaired fallback contracts is `OFFLINE_CONTRACT_VERIFIED`.

The project history scan inspected 382 candidate material files, representing 182
distinct byte versions and 210,072,493 bytes on disk. It examined 644,146 physical
materialization rows, including 583,142 occurrences of the retained-resolution
branches. There were 97,058 distinct retained row variants across 97,027 addresses.
Original files were hash-checked again after processing and remained unchanged.

1,110 addresses need a provenance patch or an explicit evidence-gap annotation.
528 pointer corrections consist of clearing adoption on 519 already-conflicted
identities and restoring actual supporting observations for 9 retained bridge
identities. Another 582 named non-service boundary/protocol identities lack
supporting old attribution evidence under the current predicate and remain
`HISTORICAL_EVIDENCE_GAP`. No actor, role or service-stop classification changed.
Neither fixed probe's candidate graph addresses nor its service targets intersect
these 1,110 addresses. Amount effects are checked separately by the same-input
context/LP replay; no expected amount is enforced by this remediation.

The Meta history pass inspected 70 saved JSON material files and identified five
distinct label responses, each with its original frozen 10-address roster. All five
were successful. The 210 existing hash-bound outcome rows (including copies) did
not contain a failed response represented as successful. The repair writes 50
explicit reclassified address outcomes into a separate R4 directory. A saved
chain-list response is a capability response, not label-import data; its verified
endpoint and response hash exclude it from this denominator. The preliminary
scope pass is retained locally, and the final Meta result is the v2 directory.
This finite scan does not reconstruct a failure for which no historical response
or record survived. MetaSleuth calls added by this work: **0**.

Full historical indexes and row patches remain private in the project. The MIN
needs the scan summaries, affected-ID index, the fixed-probe intersection result
and any actually relevant row patches; it does not need the full historical label
library. Public delivery contains this aggregate explanation, code and synthetic
tests. External acceptance remains `PENDING_REVIEW`.
