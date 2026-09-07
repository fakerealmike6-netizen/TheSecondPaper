# ReadRetryStore R4 contract

`src/read_retry_r4.py` is a small SQLite claim and attempt journal for idempotent reads. It does not call a provider, own credentials, reserve a financial budget or release historical risk. Dune SQL creation/submission is excluded. Caller wrappers must perform at most one transport per `mark_dispatched` and disable nested SDK retries.

Stable identity is a dictionary with `provider`, `method`, and all nonsecret selectors. Include chain, historical block/hash, execution identity and pagination fields as appropriate; omit run, authentication and transport-only request IDs. `logical_key(identity)` hashes canonical JSON. State observation generations belong in a known execution's status-read identity only after a valid pending/running result, never as a way to reset a failed request.

```python
store = ReadRetryStore(sqlite_path, clock=clock, rng=rng)
claim = store.claim(identity, deadline=absolute_deadline)
# CACHE_HIT contains payload and receipt; no transport is needed.
# DEFERRED contains persistent next_eligible_at; do not shorten Retry-After.
# Only CLAIMED authorizes the caller to attempt budget reservation.
accounting = reserve_each_attempt(claim)  # caller owns the cumulative ledger
store.mark_dispatched(claim['attempt_id'], accounting=accounting)
response = one_authorized_read()
store.finish(claim['attempt_id'], 'SUCCESS', payload=response, receipt=proof)
```

On classified failure call `finish` with RETRYABLE_FAILURE, PERMANENT_FAILURE or UNRESOLVED, plus `error_class`, `retry_after` and immutable receipt evidence. `classify_failure` accepts exceptions, `http_status`, provider error dictionaries or complete JSON-RPC error envelopes, and explicit caller category mappings. Retry-After supports seconds or HTTP dates; its lower bound overrides the jittered backoff, including waits longer than 30 seconds. A caller can return DEFERRED or wait in bounded increments. Every dispatched attempt remains counted after a later success.

`claim` persists intent under a SQLite write transaction. Concurrent callers cannot both claim the same key. Recovery of an in-flight request requires both lease expiry and proof the old owning process is no longer alive. Recovery retains the previous attempt as UNRESOLVED and counts it against the maximum three. A process with an active claim cannot replace itself merely by constructing a new store instance. `abandon_before_dispatch` is allowed only before dispatch is marked and proves no transport occurred; it does not itself release any financial reservation.

`import_attempt(identity, legacy_id, outcome=..., payload=..., accounting=..., receipt=...)` imports each distinct evidenced historical dispatch exactly once. Successful legacy evidence populates the cache. Conflicting repeated imports are rejected, and legacy accounting remains unchanged. Imported failures plus current attempts share the same three-attempt cap; more old history can be retained but never creates new allowance.

`migrate_cache(identity, old_path, classification, payload=..., history=...)` hashes and indexes old bytes without changing them. For failed old artifacts `history` is the complete evidence-backed list of dictionaries containing `legacy_id` and `import_attempt` keyword fields. Without it the request remains HISTORICAL_ATTEMPT_COUNT_UNRESOLVED and cannot dispatch. Successful artifacts can be reused directly, while their unknown old attempt total and unchanged risk remain explicit. Once imported, proof of migration remains durable and need not be supplied on every restart.

Etherscan's optional `attempt_hook(claim, params)` supplies one reservation/accounting record per actual dispatch; the injected transport remains responsible for the caller's already authorized provider and resource conditions. The constructor's `legacy_failure_history` maps the historical parameter SHA-256 to complete attempt evidence lists. Fetch and receipt counters count each actual request, including errors and retries; replay cache hits make no new request. All current Etherscan call sites are the synthetic contract tests; activation requires the existing authorized access wrapper, not a new allowance.
