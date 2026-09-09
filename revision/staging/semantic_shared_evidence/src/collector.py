"""Bounded event-state candidate acquisition; no reference graph or attribution.

Amounts remain arbitrary-precision raw integers. Depth/window constrain requests,
not later LP source age. Providers must state interval completeness explicitly.
"""
from __future__ import annotations
from dataclasses import asdict, dataclass, field
from datetime import datetime
import hashlib
import heapq
import json
import time
from pathlib import Path
from physical_facts import PhysicalFactRegistry, canonical_event, CONFLICT_STATUS

NATIVE = "native:eip155:1"
QUERY_WINDOW_MODE = "QUERY_ARRIVAL_WINDOW_SECONDS_V1"

def utc_seconds(value):
    return int(value) if isinstance(value, (int, float)) else int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())

@dataclass(frozen=True)
class Event:
    event_id: str
    tx_hash: str
    sender: str
    recipient: str
    asset: str
    amount_raw: int
    block: int
    tx_index: int | None
    timestamp: int
    kind: str = "top"
    log_index: int | None = None
    trace_address: str | None = None
    execution_index: int | None = None
    success: bool = True
    provenance: str = ""
    gas_raw: int | None = None
    chain_id: str = 'eip155:1'
    block_hash: str | None = None
    gas_used: int | None = None
    gas_price: int | None = None

    def __post_init__(self):
        if not self.event_id:
            raise ValueError("exact event identity and nonnegative integer amount required")
        for name,value in canonical_event(self).items():object.__setattr__(self,name,value)

    def stable_key(self):
        # Deterministic presentation only; never used to claim trace/log chronology.
        return (self.block, self.tx_index if self.tx_index is not None else -1, self.timestamp, self.event_id)

def strictly_after(event, arrival):
    """True/False/None; None means the evidence does not establish order."""
    if event.event_id == arrival.event_id:
        return False
    if event.block != arrival.block:
        return event.block > arrival.block
    if event.tx_hash != arrival.tx_hash:
        if event.tx_index is None or arrival.tx_index is None:
            return None
        return event.tx_index > arrival.tx_index
    if event.execution_index is not None and arrival.execution_index is not None:
        return event.execution_index > arrival.execution_index
    if arrival.kind == "top" and event.kind in ("internal", "erc20"):
        return True
    if event.kind == "top":
        return False
    if event.kind in ("erc20", "semantic_log") and arrival.kind in ("erc20", "semantic_log") and event.log_index is not None and arrival.log_index is not None:
        return event.log_index > arrival.log_index
    # Trace IDs alone cannot establish successful value execution vs receipt logs.
    return None

@dataclass(frozen=True)
class Scope:
    query_id: str
    name: str
    start_block: int
    end_block: int
    start_time: int
    end_time: int
    max_depth: int
    local_window_seconds: int | None = 90 * 86400
    window_mode: str | None = None
    scope_id: str | None = None

    def __post_init__(self):
        if self.start_block is None or self.end_block is None:
            raise ValueError("exact resolved block bounds required before freezing scope")
        if self.start_block > self.end_block or self.start_time > self.end_time or self.max_depth < 0:
            raise ValueError("inverted scope or negative depth")
        if self.window_mode not in (None, "REFERENCE_FULL", "ARRIVAL_90D", QUERY_WINDOW_MODE):
            raise ValueError("unknown window mode")
        if self.window_mode == "REFERENCE_FULL":
            if self.local_window_seconds is not None:
                raise ValueError("REFERENCE_FULL requires an explicit null local cap")
        elif self.window_mode == QUERY_WINDOW_MODE:
            if type(self.local_window_seconds) is not int or self.local_window_seconds <= 0:
                raise ValueError("QUERY_ARRIVAL_WINDOW_SECONDS_V1 requires exact positive integer seconds")
        elif self.window_mode == "ARRIVAL_90D" and self.local_window_seconds != 90 * 86400:
            raise ValueError("ARRIVAL_90D requires exactly 90 days")
        elif self.local_window_seconds is not None and self.local_window_seconds < 0:
            raise ValueError("negative local window")
        if self.window_mode is not None:
            identity = {k: v for k, v in asdict(self).items() if k != "scope_id"}
            expected = "scope:" + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
            if self.window_mode == QUERY_WINDOW_MODE and self.scope_id not in (None, expected):
                raise ValueError("Custom arrival window scope_id differs from its frozen identity")
            if self.scope_id is None: object.__setattr__(self, "scope_id", expected)

    def local_end(self, arrival):
        arrival_time = arrival.timestamp if isinstance(arrival, Event) else utc_seconds(arrival)
        return self.end_time if self.local_window_seconds is None else min(self.end_time, arrival_time + self.local_window_seconds)

    def freeze_dict(self):
        value = asdict(self)
        # Old inputs retain the old serialized identity and 90-day semantics.
        if self.window_mode is None and self.scope_id is None:
            value.pop("window_mode"); value.pop("scope_id")
        return value

    @property
    def scope_hash(self):
        return hashlib.sha256(json.dumps(self.freeze_dict(), sort_keys=True).encode()).hexdigest()

    @classmethod
    def from_policy(cls, policy):
        mode = policy.get("window_mode", policy.get("primary_window_mode"))
        if mode == QUERY_WINDOW_MODE:
            if policy.get('primary_window_mode', mode) != mode:
                raise ValueError('Conflicting custom arrival window modes')
            keys = [k for k in ('local_window_seconds', 'primary_local_window_seconds') if k in policy]
            if not keys or any(type(policy[k]) is not int or policy[k] <= 0 for k in keys) or any(policy[k] != policy[keys[0]] for k in keys):
                raise ValueError('Missing or conflicting custom arrival window seconds')
        if mode is None:
            # Historical from_policy ignored optional local-cap fields. Preserve
            # that historical interpretation for every freeze missing a mode.
            cap = 90 * 86400
        else:
            cap = policy.get("local_window_seconds", policy.get("primary_local_window_seconds", None if mode in ("REFERENCE_FULL", QUERY_WINDOW_MODE) else 90 * 86400))
            if mode == "REFERENCE_FULL" and any(policy.get(k) is not None for k in ('local_window_seconds', 'primary_local_window_seconds')):
                raise ValueError("REFERENCE_FULL must explicitly disable the local cap")
        return cls(policy["query_id"], policy["name"], policy["start_block"], policy["end_block"], utc_seconds(policy["start_time_utc"]), utc_seconds(policy["end_time_utc"]), policy["max_acquisition_depth"], cap, mode, policy.get("scope_id"))

@dataclass(frozen=True)
class State:
    query_id: str
    address: str
    asset: str
    arrival: Event
    depth: int
    local_end: int
    protocol_context: str = "ordinary"
    provenance: str = "observed_candidate_arrival"

    def key(self):
        return (self.query_id, self.address, self.asset, self.arrival.event_id, self.depth, self.local_end, self.protocol_context)

@dataclass
class FetchResult:
    events: list[Event] = field(default_factory=list)
    coverage: list[dict] = field(default_factory=list)
    complete: bool = False
    gaps: list[dict] = field(default_factory=list)
    new_raw_bytes: int = 0
    real_requests: int = 0
    cache_hits: int = 0
    fact_conflicts: list[dict] = field(default_factory=list)
    quarantined_facts: list[dict] = field(default_factory=list)

@dataclass
class Limits:
    max_events: int = 25000
    max_expanded_addresses: int = 1000
    max_online_seconds: int = 5400

@dataclass
class CollectionResult:
    query_id: str
    status: str
    candidate_events: list[dict]
    context_events: list[dict]
    states: list[dict]
    stops: list[dict]
    unresolved_frontier: list[dict]
    coverage: list[dict]
    gaps: list[dict]
    metrics: dict
    fact_conflicts: list[dict] = field(default_factory=list)
    quarantined_facts: list[dict] = field(default_factory=list)
    invalidated_evidence: dict = field(default_factory=dict)
    semantic_units: list[dict] = field(default_factory=list)
    semantic_membership: list[dict] = field(default_factory=list)
    semantic_evidence_context: dict = field(default_factory=dict)

    def write(self, path):
        data = asdict(self)
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        return hashlib.sha256(path.read_bytes()).hexdigest()

class Collector:
    """label_resolver(address)-> {kind, actor?, status?, acquisition_scope?}.

    kind is SERVICE, BRIDGE, MIXER, UNSUPPORTED_PROTOCOL, or UNKNOWN/ORDINARY.
    A failed lookup is recorded as a gap and UNKNOWN still expands.
    fetch_interval is injected, permitting serial externally budgeted transport.
    """
    def __init__(self, provider, label_resolver, limits=None, checkpoint_path=None, *, semantic_resolver=None):
        self.provider = provider
        self.label_resolver = label_resolver
        self.limits = limits or Limits()
        self.checkpoint_path = Path(checkpoint_path) if checkpoint_path else None
        self.semantic_resolver = semantic_resolver

    def run(self, scope, seed):
        if not seed.success or seed.amount_raw <= 0 or not (scope.start_block <= seed.block <= scope.end_block) or not (scope.start_time <= seed.timestamp <= scope.end_time):
            raise ValueError("exact positive successful seed must lie inside declared scope")
        heap, seen, expanded, candidates, context = [], set(), set(), {seed.event_id: seed}, {}
        registry=PhysicalFactRegistry();registry.add(seed)
        fact_conflicts=[];quarantined=[]
        processed, stops, pending, coverage, gaps, membership = [], [], [], [], [], []
        semantic_units, semantic_membership = {}, []
        online_seconds, provider_wall_seconds, requests, hits, raw_bytes = 0.0, 0.0, 0, 0, 0
        prior_online_seconds = 0.0
        if self.checkpoint_path and self.checkpoint_path.exists():
            prior = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
            if prior.get("query_id") != scope.query_id or prior.get("scope") not in (scope.freeze_dict(), asdict(scope)):
                raise ValueError("checkpoint belongs to another query/scope; use new path")
            if prior.get('semantic_resolver_sha256') != getattr(self.semantic_resolver,'identity',None):
                raise ValueError('checkpoint belongs to another semantic evidence catalogue')
            prior_online_seconds = float(prior.get("cumulative_online_seconds", 0))
        if hasattr(self.provider, "bind_scope"):
            self.provider.bind_scope(scope)
        interrupted = None

        def push(arrival, depth):
            st = State(scope.query_id, arrival.recipient, arrival.asset, arrival, depth, scope.local_end(arrival))
            if st.key() not in seen:
                seen.add(st.key())
                heapq.heappush(heap, ((depth,) + arrival.stable_key() + (st.key(),), st))

        def checkpoint():
            if self.checkpoint_path:
                payload = {"query_id": scope.query_id, "scope": scope.freeze_dict(), "cumulative_online_seconds": prior_online_seconds + online_seconds, "pending": [asdict(x[1]) for x in heap] + pending, "processed": processed, "candidate_event_ids": sorted(candidates), "coverage": coverage, "resume_strategy": "deterministic restart with immutable page cache; partial pages retain cursors"}
                if scope.window_mode is not None:
                    payload.update(scope_id=scope.scope_id, scope_hash=scope.scope_hash, window_mode=scope.window_mode)
                if self.semantic_resolver is not None:
                    payload.update(semantic_resolver_sha256=self.semantic_resolver.identity,
                        semantic_units=semantic_units,semantic_membership=semantic_membership)
                self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
                temp = self.checkpoint_path.with_suffix(".tmp")
                temp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
                temp.replace(self.checkpoint_path)

        push(seed, 0)
        while heap:
            _, state = heapq.heappop(heap)
            state_resolver = getattr(self.label_resolver, 'resolve_state', None)
            identity = (state_resolver(state) if callable(state_resolver) else self.label_resolver(state.address)) or {"kind": "UNKNOWN", "status": "UNKNOWN"}
            if identity.get("status") in ("LOOKUP_FAILED", "BUDGET_BLOCKED", "UNQUERIED"):
                gaps.append({"address": state.address, "reason": "LABEL_" + identity["status"], "continues_as_unknown": True})
            base = {"state": asdict(state), "identity": identity}
            processed.append(base)
            if identity.get("kind") == "SERVICE":
                stops.append({**base, "reason": "FIRST_IDENTIFIED_SERVICE", "entry_event_id": state.arrival.event_id})
                checkpoint()
                continue
            if identity.get('branch_action') == 'SUPPORTED_OPERATION_RESOLVE':
                # Validated operations are consumed while expanding their actual
                # reachable caller. A remaining contract-recipient state is an
                # unresolved instance, never authority to crawl all WETH users.
                stops.append({**base,'reason':'SUPPORTED_COMPONENT_INSTANCE_BOUNDARY','entry_event_id':state.arrival.event_id})
                gaps.append({'reason':'COMPONENT_INSTANCE_EVIDENCE_REQUIRED' if self.semantic_resolver is not None else 'SEMANTIC_INTEGRATION_GATE_CLOSED',
                    'address':state.address,'arrival_event_id':state.arrival.event_id})
                checkpoint();continue
            if identity.get("kind") in ("BRIDGE", "MIXER", "UNSUPPORTED_PROTOCOL"):
                stops.append({**base, "reason": "PROTOCOL_BOUNDARY", "entry_event_id": state.arrival.event_id})
                checkpoint()
                continue
            if state.depth >= scope.max_depth:
                stops.append({**base, "reason": "DECLARED_DEPTH_BOUNDARY"})
                checkpoint()
                continue
            time_limit = prior_online_seconds + online_seconds >= self.limits.max_online_seconds and not getattr(self.provider, "replay_only", False)
            if time_limit or (state.address not in expanded and len(expanded) >= self.limits.max_expanded_addresses) or len(candidates) >= self.limits.max_events:
                interrupted = "INCOMPLETE_RESOURCE_LIMIT"
                pending.append({**base, "reason": interrupted})
                break
            expanded.add(state.address)
            started = time.monotonic()
            try:
                result = self.provider.fetch_interval(state.address, state.asset, state.arrival.block, scope.end_block, start_time=state.arrival.timestamp, end_time=state.local_end, global_end_time=scope.end_time)
            except Exception as exc:
                result = FetchResult(gaps=[{"reason": "PROVIDER_EXCEPTION", "exception_type": type(exc).__name__}])
            elapsed = time.monotonic() - started
            provider_wall_seconds += elapsed
            # Warm cache replay is measured independently from online waiting.
            if result.real_requests or (not result.cache_hits and not result.complete):
                online_seconds += elapsed
            requests += result.real_requests
            hits += result.cache_hits
            raw_bytes += result.new_raw_bytes
            coverage.extend([{**row, "state_key": list(state.key())} for row in result.coverage])
            gaps.extend([{**row, "address": state.address, "arrival_event_id": state.arrival.event_id} for row in result.gaps])
            # Validate the whole batch against every earlier interval before
            # allowing any candidate/stop derived from this response.
            for event in result.events:registry.add(event)
            fact_snapshot=registry.snapshot(include_events=False)
            fact_conflicts=result.fact_conflicts+fact_snapshot['conflicts']
            fact_conflicts += [g for g in result.gaps if g.get('reason') in ('PHYSICAL_FACT_CONFLICT','DUNE_EVENT_IDENTITY_CONFLICT') and g not in fact_conflicts]
            if fact_conflicts:
                quarantined=result.quarantined_facts+fact_snapshot['quarantined_versions']
                for conflict in fact_conflicts:
                    for version in conflict.get('versions',[]):
                        if version not in quarantined:quarantined.append(version)
                interrupted=CONFLICT_STATUS
                pending.append({**base,'reason':CONFLICT_STATUS})
                break
            if not result.complete:
                pending.append({**base, "reason": "INTERVAL_INCOMPLETE", "coverage": result.coverage})
            batch={registry.get(e.event_id)['event_id']:Event(**registry.get(e.event_id)) for e in result.events}
            semantic = []
            if self.semantic_resolver is not None:
                try:
                    semantic = self.semantic_resolver.resolve(state,list(batch.values()),scope,identity)
                    for item in semantic:
                        if 'gap' in item:
                            gaps.append(dict(item['gap'],address=state.address));continue
                        registry.add(item['native_event'])
                    added_snapshot=registry.snapshot(include_events=False)
                    if added_snapshot['conflicts']:
                        fact_conflicts=added_snapshot['conflicts'];quarantined=added_snapshot['quarantined_versions']
                        interrupted=CONFLICT_STATUS;pending.append({**base,'reason':CONFLICT_STATUS});break
                except (ValueError,TypeError,KeyError) as exc:
                    fact_conflicts=[{'reason':'SEMANTIC_CURRENT_EVIDENCE_CONFLICT','detail':str(exc)}]
                    interrupted=CONFLICT_STATUS;pending.append({**base,'reason':CONFLICT_STATUS});break
            replaced={item['native_event'].event_id for item in semantic if 'unit' in item and item['replace_native_recipient_push']}
            for event in sorted(batch.values(), key=Event.stable_key):
                # Keep all received facts; failed transfers and gas are context only.
                reason = None
                if not event.success:
                    reason = "FAILED_VALUE_TRANSFER_GAS_CONTEXT"
                elif event.amount_raw == 0:
                    reason = "ZERO_VALUE_CONTEXT"
                elif event.asset != state.asset:
                    reason = "OTHER_ASSET_CONTEXT_NO_CERTIFIED_CONVERSION"
                elif event.sender != state.address:
                    reason = "EXTERNAL_INFLOW_CONTEXT_NO_RENEWAL"
                elif event.recipient == state.address:
                    reason = "SELF_TRANSFER_CONTEXT_NO_RENEWAL"
                elif not (scope.start_block <= event.block <= scope.end_block and state.arrival.timestamp <= event.timestamp <= state.local_end):
                    reason = "OUTSIDE_ARRIVAL_WINDOW_CONTEXT"
                else:
                    order = strictly_after(event, state.arrival)
                    if order is None:
                        reason = "ORDER_UNRESOLVED"
                        gaps.append({"reason": reason, "event_id": event.event_id, "arrival_event_id": state.arrival.event_id})
                    elif not order:
                        reason = "NOT_STRICTLY_AFTER_ARRIVAL"
                if reason:
                    context[(event.event_id, state.arrival.event_id, reason)] = {**asdict(event), "context_only": True, "context_reason": reason, "arrival_event_id": state.arrival.event_id}
                    continue
                candidates[event.event_id] = event
                membership.append({"event_id": event.event_id, "arrival_event_id": state.arrival.event_id, "depth": state.depth + 1})
                if event.event_id not in replaced:push(event, state.depth + 1)
            for item in semantic:
                if 'unit' not in item:continue
                unit=item['unit'];semantic_units[unit['unit_id']]=unit
                if item['membership'] not in semantic_membership:semantic_membership.append(item['membership'])
                native=item['native_event']
                candidates[native.event_id]=native
                push(item['arrival'],state.depth+1)
            # The entire response is retained even if it takes the count over cap.
            if len(candidates) > self.limits.max_events or (prior_online_seconds + online_seconds >= self.limits.max_online_seconds and not getattr(self.provider, "replay_only", False)):
                interrupted = "INCOMPLETE_RESOURCE_LIMIT"
                break
            checkpoint()
        pending.extend({"state": asdict(st), "reason": interrupted or "UNPROCESSED_FRONTIER"} for _, st in heap)
        heap.clear()
        if interrupted:
            status = interrupted
        elif any("BUDGET" in g.get("reason", "") for g in gaps):
            status = "INCOMPLETE_BUDGET_LIMIT"
        elif pending:
            status = "INCOMPLETE_PROVIDER_OR_DATA_GAP"
        elif any(g.get("reason") in ("ORDER_UNRESOLVED", "EVENT_IDENTITY_UNRESOLVED", "INTERNAL_STATUS_UNRESOLVED",
                "SEMANTIC_OPERATION_ORDER_UNRESOLVED","COMPONENT_INSTANCE_EVIDENCE_REQUIRED","SEMANTIC_INTEGRATION_GATE_CLOSED") for g in gaps):
            status = "COMPLETED_WITH_RECORDED_DATA_GAPS"
        else:
            status = "COMPLETED_WITHIN_DECLARED_SCOPE"
        invalidated={}
        if fact_conflicts:
            # A prior branch may already have produced a service stop before a
            # later interval contradicted it. Preserve it as invalid evidence,
            # never as an active target or a usable candidate capacity.
            invalidated={'candidate_events':[asdict(e) for e in candidates.values()],
                         'context_events':list(context.values()),'states':processed,
                         'stops':stops,'membership':membership,'coverage':coverage}
            if semantic_units:invalidated.update(semantic_units=list(semantic_units.values()),semantic_membership=semantic_membership)
            candidates={};context={};processed=[];stops=[];membership=[]
            semantic_units={};semantic_membership=[]
            coverage=[{**c,'complete':False,'invalidated_reason':CONFLICT_STATUS} for c in coverage]
            gaps.append({'reason':'PHYSICAL_FACT_CONFLICT','conflict_count':len(fact_conflicts),'query_invalidated':True})
        else:
            candidates={registry.get(eid)['event_id']:Event(**registry.get(eid)) for eid in candidates}
        checkpoint()
        facts = [asdict(e) | {"context_only": False} for e in sorted(candidates.values(), key=Event.stable_key)]
        stable = {"candidate_events": facts, "stops": stops, "membership": membership, "coverage": [{k: v for k, v in c.items() if k not in ("cache_hit", "raw_path")} for c in coverage]}
        if self.semantic_resolver is not None:
            stable.update(semantic_units=semantic_units,semantic_membership=semantic_membership,
                semantic_resolver_sha256=self.semantic_resolver.identity)
        digest = hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest()
        result = CollectionResult(scope.query_id, status, facts, list(context.values()), processed, stops, pending, coverage, gaps, {"candidate_event_count": len(candidates), "candidate_transaction_count": len({e.tx_hash for e in candidates.values()}), "expanded_address_count": len(expanded), "candidate_address_count": len({e.recipient for e in candidates.values()}), "state_count": len(processed), "service_entry_state_count": sum(s["reason"] == "FIRST_IDENTIFIED_SERVICE" for s in stops), "service_address_count": len({s["state"]["address"] for s in stops if s["reason"] == "FIRST_IDENTIFIED_SERVICE"}), "real_requests": requests, "cache_hits": hits, "new_raw_bytes": raw_bytes, "online_collection_seconds": online_seconds, "cumulative_online_collection_seconds": prior_online_seconds + online_seconds, "provider_total_wall_seconds": provider_wall_seconds, "candidate_stop_coverage_sha256": digest, "candidate_membership": membership,'fact_validation_status':'CONFLICT' if fact_conflicts else 'CONSISTENT_OBSERVED_FACTS'},fact_conflicts,quarantined,invalidated)
        if self.semantic_resolver is not None:
            from stage1d_semantic_units import SEMANTIC_VERSION, digest as semantic_digest
            result.semantic_units=[semantic_units[k] for k in sorted(semantic_units)]
            result.semantic_membership=semantic_membership
            used_contexts={u['evidence_context_id'] for u in result.semantic_units}
            from stage1d_shared_evidence import serialize_contexts
            result.semantic_evidence_context=serialize_contexts(self.semantic_resolver.contexts,used_contexts)
            result.metrics.update(semantic_version=SEMANTIC_VERSION,
                semantic_resolver_sha256=self.semantic_resolver.identity,
                semantic_scope_id='semantic_scope:'+semantic_digest({'query_id':scope.query_id,'scope_hash':scope.scope_hash,
                    'version':SEMANTIC_VERSION,'unit_ids':sorted(semantic_units)}),
                semantic_unit_count=len(semantic_units),semantic_membership_count=len(semantic_membership),
                semantic_feature_status='ADMITTED_REACHABLE_COMPONENTS' if semantic_units else 'NO_REACHABLE_ELIGIBLE_COMPONENT')

        if scope.window_mode is not None:
            result.metrics.update(scope_id=scope.scope_id, scope_hash=scope.scope_hash,
                                  window_mode=scope.window_mode, local_window_seconds=scope.local_window_seconds,
                                  scope_freeze=scope.freeze_dict())
        return result
