# R3 context amount model

`context_lp_r3.py` consumes the self-contained `stage1b-r3-context-model-v1`
document produced by `context_ledger_r3.assemble_model`. It does not read labels,
reference answers, hidden allocations, credentials, provider endpoints or files
outside the supplied input. The accepted R2 builder and its sparse conditional
results remain unchanged.

The document supplies each account's own initialization position and physical
balance, ordered transactions, physical flow roles, one actual fee per
transaction, aligned anchors, objective groups and explicit gaps. A block-end
initial balance must precede every affected event in that block; a later balance
cannot be moved back to the seed time. An anchor including the seed cannot be
used and then injected again. Event-pre initialization requires a separately
verified adapter and is rejected by this block/transaction interface.

Every modeled source balance is initially zero. The single frozen seed injects
its defined amount once at its receiver. Physical initial balances and later
anchors bound source balances; they do not create source. Known normal incoming
funds affect physical state and therefore the possible source allocation, while
their source-zero role requires an explicit causal basis. Related modeled
accounts share one transfer variable. The builder must not infer normality from
the absence of a reference path or from a context label.

For an observed external outflow, an aggregate external reservoir receives the
same source variable. An observed possible return spends only source already
present there. This pooling relaxes unobserved external account identities while
preserving chronology and total source capacity. It cannot create source or
borrow future exits; returns before any exit consequently have source zero.
It may retain source and return external normal funds, so observed return value
does not force source to return. This is an explicit finite model boundary,
not a claim to reconstruct unobserved Ethereum activity. First service entries
are absorbing terminals and never feed this reservoir.

Actual net fees are paid once from the actual payer at transaction start.
Their source shares are endogenous bounded variables. Debit states precede
credits, so a later internal inflow cannot finance the earlier fee. For an
ordinary transfer, the fee and top-level value thus share the beginning
balance. Ordered internal transfers may use earlier internal receipts.
Contract gas prepayment and refund timing is not proved merely by positioning
the net fee first; affected evidence must carry the stated local timing gap.
Fee source is an absorbing model sink, retaining the prior model's fee scope.
Seed sender gas is not silently charged to the recipient.

Integer physical state is replayed and every known source state is bounded by
the resulting actual state. Negative actual balances, mismatched aligned
anchors, duplicate physical identities, reverted value, unknown transaction
order and explicit fact conflicts are rejected. Missing evidence remains a
specific gap; one missing initial balance does not remove other known balances.
Facts are never repaired with an invented balancing flow or infinite slack.

`BEST_AVAILABLE_CONTEXT` uses every supplied reliable balance capacity.
`MATCHED_INFORMATION_RELAXED` keeps identical variables, source, targets,
ordering and equality rows and removes only actual balance upper bounds.
The structural inclusion check precedes the endpoint monotonicity check.
The separate R2 graph can have different events and is not subject to this
same-graph monotonicity assertion.

Each event, address–asset group and union of all first service entries is solved
as an objective on the same coupled model. The union is not a sum of separately
optimized upper bounds. The minimum sum of all nonseed candidate source ports
checks whether every downstream candidate source can simultaneously be zero.
If its exact lower endpoint is positive, nonzero dual bound provenance identifies
the seed and physical capacities supporting that exclusion.

HiGHS proposes solutions. Original exact rational equalities, bounds and dual
conditions certify endpoints. A sparse exact active-bound recovery extends the
existing small-model recovery to larger context models without changing the
original rows. A separate source/physical ledger replayer audits every reported
endpoint directly from the document and event shares, without consulting LP
rows. Numeric or certificate failures return unresolved endpoints.

Run one self-contained real model:

```
python -B src/context_lp_r3.py --input model_input.json --output fresh_results
```

Run the targeted synthetic controls:

```
python -B -m unittest discover -s tests -p test_context_lp_r3.py -v
```

The initial control run exposed an incorrect analytical test expectation that
all source leaving to an external reservoir had to return with observed funds.
The expected lower bound was corrected to zero; the upper bound remains the
single prior source capacity. The initial failure and corrected run are retained.

These controls prove the exercised behaviors. Actual context completeness and
the external acceptance decision are reported separately; external review
remains `PENDING_REVIEW`.
