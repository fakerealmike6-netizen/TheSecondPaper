"""State-scoped cost decisions projected onto the existing conserved boundary.

Pure document adapter. No identity lookup, provider, filesystem or solver calls.
The root binds the policy/evidence files; this layer rechecks the saved decisions
and never turns a task decision into a service identity or source-zero fact.
"""
from __future__ import annotations
from collections import defaultdict
import copy
from dataclasses import asdict, is_dataclass
import hashlib
import json
import re

from collector import Scope
from context_ledger_r3 import EvidenceConflict

SCHEMA = 'stage1d-cost-boundary-context-v1'
AUTH = 'STAGE1D_UNKNOWN_CONTRACT_OR_RATE20_BOUNDARY_V1'
POLICY_SCHEMA = 'stage1d-unknown-cost-policy-v1'
STOP_REASONS = frozenset(('UNKNOWN_CODE_COST_BOUNDARY',
    'UNKNOWN_NO_CODE_HIGH_OUTFLOW_COST_BOUNDARY'))
DEFERRED = frozenset(('STOP', 'PENDING'))
ASSUMPTION = ('CONDITIONAL_OBSERVED_SCOPE: actual observed nonservice exits fund the existing '
    'shared per-asset outside reservoir, initially with zero source; observed returns may draw '
    'only from prior conserved exits. External locations are pooled. Unobserved downstream '
    'transport, returns, conversions and alternative paths are not certified absent.')


def canonical(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
        ensure_ascii=False).encode()).hexdigest()


def record(value):
    return asdict(value) if is_dataclass(value) else value


def account_key(address, value):
    from stage1d_multiasset_context import asset
    return str(address).lower() + '|' + asset(value)


def validate_overlay(query, collection, *, expected_policy_sha256=None):
    """Validate exact state/list correspondence using the current pure core gate."""
    policy = collection.get('cost_boundary_policy')
    rows = collection.get('states', [])
    decisions = collection.get('cost_boundary_decisions', [])
    overlays = [row['cost_boundary'] for row in rows if 'cost_boundary' in row]
    if policy is None:
        if decisions or overlays:
            raise EvidenceConflict('Cost decisions require their explicit policy')
        return None
    if (not isinstance(policy, dict) or policy.get('schema_version') != POLICY_SCHEMA
            or policy.get('authorization_id') != AUTH or type(policy.get('enabled')) is not bool
            or not re.fullmatch('[0-9a-f]{64}', str(policy.get('policy_sha256', '')))):
        raise EvidenceConflict('Unknown or malformed cost boundary policy')
    if expected_policy_sha256 is not None and policy['policy_sha256'] != expected_policy_sha256:
        raise EvidenceConflict('Current adopted cost policy SHA differs')
    if not isinstance(decisions, list) or overlays != decisions:
        raise EvidenceConflict('Cost state overlays and ordered collection decisions differ')
    if policy['enabled'] and len(overlays) != len(rows):
        raise EvidenceConflict('Enabled cost policy requires a decision or priority BYPASS for every saved state')
    if not policy['enabled'] and any(d.get('action') != 'BYPASS' for d in decisions):
        raise EvidenceConflict('Disabled cost policy cannot stop or defer a state')
    from stage1d_unknown_cost_boundary import validate_decision, state_key
    scope = Scope.from_policy(query)
    if scope.scope_hash != query.get('scope_hash'):
        raise EvidenceConflict('Cost context query scope hash differs')
    by_account = defaultdict(list)
    cross = {'STOP': defaultdict(list), 'PENDING': defaultdict(list)}
    for action, table in (('STOP', collection.get('stops', [])),
                          ('PENDING', collection.get('unresolved_frontier', []))):
        for saved in table:
            if action == 'STOP' and saved.get('reason') in STOP_REASONS and 'cost_boundary' not in saved:
                raise EvidenceConflict('Cost stop is missing its state decision evidence')
            if saved.get('reason') in STOP_REASONS and saved.get('cost_boundary', {}).get('action') != 'STOP':
                raise EvidenceConflict('Cost stop reason requires a STOP decision')
            if 'cost_boundary' in saved and saved['cost_boundary'].get('action') in DEFERRED:
                cross[action][saved['cost_boundary'].get('state_key')].append(saved)
    used = set()
    expected_cross = {'STOP': set(), 'PENDING': set()}
    for row in rows:
        state = row['state']
        key = state_key(state, scope)
        if key in used:
            raise EvidenceConflict('Duplicate current cost arrival state')
        used.add(key)
        decision = row.get('cost_boundary')
        if decision is not None:
            validate_decision(state, scope, decision, policy['policy_sha256'])
            if decision.get('base_identity', row['identity']) != row['identity']:
                raise EvidenceConflict('Cost decision cannot change the base identity')
            if decision['action'] == 'STOP' and decision['reason'] not in STOP_REASONS:
                raise EvidenceConflict('Unknown cost stop reason')
            if decision['action'] == 'STOP' and (
                    row['identity'].get('kind') in ('SERVICE', 'BRIDGE', 'MIXER', 'UNSUPPORTED_PROTOCOL')
                    or row['identity'].get('branch_action') in ('USER_REQUESTED_BRANCH_HOLD', 'SUPPORTED_OPERATION_RESOLVE')):
                raise EvidenceConflict('Existing user/role/semantic boundary has priority over generic cost stop')
            if decision['action'] in DEFERRED:
                expected_cross[decision['action']].add(key)
                matches = cross[decision['action']].get(key, [])
                if (len(matches) != 1 or matches[0].get('state') != state
                        or matches[0].get('identity') != row['identity']
                        or matches[0].get('reason') != decision['reason']
                        or matches[0].get('cost_boundary') != decision):
                    raise EvidenceConflict('Cost decision does not match its STOP/frontier replay row')
                if decision['action'] == 'STOP' and matches[0].get('entry_event_id') != state['arrival']['event_id']:
                    raise EvidenceConflict('Cost stop lost its physical arrival entry')
        by_account[account_key(state['address'], state['asset'])].append({
            'state_key': key, 'state': state, 'decision': decision})
    if any(set(cross[action]) != expected_cross[action] for action in DEFERRED):
        raise EvidenceConflict('Extra cost STOP/frontier row outside the current decision set')
    return {'policy': copy.deepcopy(policy), 'decisions': copy.deepcopy(decisions),
        'decisions_sha256': canonical(decisions), 'by_account': dict(by_account)}


def _finite_unit_continuation(unit, query, collection):
    """Recheck the exact saved cost state and physical semantic membership.

    This preserves a unit's ledger dependency, never its holder's discovery
    permission. The portable certificate is independently verified as usual.
    """
    if not any(row.get('finite_semantic_continuation') is not None for row in collection.get('states', [])):
        return False
    if not isinstance(unit, dict) or not isinstance(unit.get('unit_id'), str):
        raise EvidenceConflict('Finite cost-state continuation requires an exact unit identity')
    from collector import Event, strictly_after
    from stage1d_semantic_units import validate_semantic_unit, WETH, TOKEN
    from stage1d_unknown_cost_boundary import state_key
    scope = Scope.from_policy(query)
    unit_ids = {u['unit_id'] for u in collection.get('semantic_units', [])}
    for row in collection.get('states', []):
        info = row.get('finite_semantic_continuation')
        if info is None or unit['unit_id'] not in info.get('unit_ids', []):
            continue
        state, decision = row['state'], row.get('cost_boundary', {})
        ids = info.get('unit_ids')
        if (info.get('schema_version') != 'stage1d-cost-finite-semantic-continuation-v1'
                or not isinstance(ids, list) or ids != sorted(set(ids)) or not set(ids) <= unit_ids
                or info.get('state_key') != state_key(state, scope)
                or info.get('decision_sha256') != decision.get('decision_sha256')
                or decision.get('action') not in DEFERRED
                or not info.get('semantic_resolver_sha256')
                or info['semantic_resolver_sha256'] != collection.get('metrics', {}).get('semantic_resolver_sha256')
                or info.get('ordinary_discovery_allowed') is not False
                or info.get('final_ledger_complete_claimed') is not False
                or state['address'] != unit['holder'] or state['asset'] != unit['input']['asset']
                or state['depth'] >= scope.max_depth
                or row['identity'].get('kind') in ('SERVICE', 'BRIDGE', 'MIXER', 'UNSUPPORTED_PROTOCOL')
                or row['identity'].get('branch_action') in ('USER_REQUESTED_BRANCH_HOLD', 'SUPPORTED_OPERATION_RESOLVE')):
            raise EvidenceConflict('Invalid finite cost-state semantic continuation binding')
        check = validate_semantic_unit(unit, evidence_context=collection.get('semantic_evidence_context'))
        if not check['passed']:
            raise EvidenceConflict('Finite cost-state portable unit failed: ' + check['reason'])
        native = Event(**unit['native_event'])
        arrival = Event(**state['arrival'])
        ordering = native if unit['kind'] == 'DEPOSIT' else Event(
            unit['raw_log_event_id'], unit['tx_hash'], state['address'], WETH, TOKEN,
            int(unit['input']['amount_raw']), unit['block_number'], unit['tx_index'], unit['timestamp'],
            kind='semantic_log', log_index=unit['exact_log_locator'], block_hash=unit['block_hash'])
        output_id = unit['virtual_ports']['output'] if unit['kind'] == 'DEPOSIT' else native.event_id
        expected = {'query_id': scope.query_id, 'scope_id': scope.scope_id, 'scope_hash': scope.scope_hash,
            'unit_id': unit['unit_id'], 'input_arrival_event_id': arrival.event_id,
            'input_depth': state['depth'], 'output_arrival_event_id': output_id,
            'output_depth': state['depth'] + 1, 'window_end': scope.local_end(unit['timestamp']),
            'physical_hops': 1, 'extra_virtual_port_hops': 0}
        if (not (scope.start_block <= unit['block_number'] <= scope.end_block
                 and arrival.timestamp <= unit['timestamp'] <= state['local_end'])
                or strictly_after(ordering, arrival) is not True
                or expected not in collection.get('semantic_membership', [])):
            raise EvidenceConflict('Finite cost-state unit lacks exact legal arrival membership')
        candidates = [record(e) for e in collection.get('candidate_events', [])
                      if record(e)['event_id'] == native.event_id]
        from physical_facts import PhysicalFactRegistry
        facts = PhysicalFactRegistry(); facts.add(native)
        for candidate in candidates:
            facts.add(Event(**{k: v for k, v in candidate.items() if k in Event.__dataclass_fields__}))
        if len(candidates) != 1 or facts.snapshot(include_events=False)['conflicts']:
            raise EvidenceConflict('Finite cost-state unit lost its exact physical native candidate')
        return True
    return False


def project_plan(query, collection, plan):
    """Defer only accounts whose every saved arrival is independently stopped.

    Any historical BYPASS, other live arrival, depth state or supported conversion
    preserves its ordinary finite ledger. No event/amount/coverage is edited.
    """
    overlay = validate_overlay(query, collection)
    if overlay is None:
        return plan
    if plan.get('query_id') != query['query_id']:
        raise EvidenceConflict('Cost context plan belongs to another query')
    from stage1d_multiasset_context import ASSETS, ETH, WETH
    candidates = [record(e) for e in collection.get('candidate_events', [])]
    by_id = {e['event_id']: e for e in candidates}
    if len(by_id) != len(candidates):
        raise EvidenceConflict('Duplicate physical candidate identity in cost projection')
    protected = set()
    for unit in collection.get('semantic_units', []):
        holder_rows = [row for key, rows in overlay['by_account'].items()
                       if key.rsplit('|', 1)[0] == unit['holder'] for row in rows]
        finite = _finite_unit_continuation(unit, query, collection)
        if not finite and (not holder_rows or all(row['decision'] and row['decision']['action'] in DEFERRED for row in holder_rows)):
            raise EvidenceConflict('COST_BOUNDARY_SUPPORTED_UNIT_REACHABILITY_OPEN: no other current holder arrival supports the unit')
        protected.update(account_key(unit['holder'], a) for a in ASSETS)
    # A retained ordinary token account still requires its real native gas
    # ledger, even when another ETH arrival at that address was cost-stopped.
    protected.update(account_key(key.rsplit('|', 1)[0], ETH)
        for key, arrivals in overlay['by_account'].items()
        if key.rsplit('|', 1)[1] == WETH and any(
            not r['decision'] or r['decision']['action'] not in DEFERRED for r in arrivals))
    eligible = set()
    retained = []
    for key, arrivals in overlay['by_account'].items():
        affected = [r for r in arrivals if r['decision'] and r['decision']['action'] in DEFERRED]
        if not affected:
            continue
        if len(affected) != len(arrivals) or key in protected:
            retained.append({'account_id': key, 'affected_state_keys': [r['state_key'] for r in affected],
                'retained_state_keys': [r['state_key'] for r in arrivals if r not in affected],
                'reason': 'OTHER_ASSET_GAS_OR_SUPPORTED_CONVERSION_LEDGER_REQUIRED' if key in protected else 'OTHER_LEGAL_ARRIVAL_OR_HISTORICAL_BYPASS'})
            continue
        for row in affected:
            entry = by_id.get(row['state']['arrival']['event_id'])
            if entry is None or account_key(entry['recipient'], entry['asset']) != key:
                raise EvidenceConflict('Cost boundary lacks its actual retained candidate arrival')
            if entry['event_id'] == query['seed_event_id']:
                raise EvidenceConflict('COST_BOUNDARY_SEED_RECEIVER_MODEL_OPEN: preserve source injection; dedicated interface required')
        if any(account_key(e['sender'], e['asset']) == key for e in candidates if e['event_id'] != query['seed_event_id']):
            raise EvidenceConflict('Cost-only candidate continuation requires collection replay; do not delete it')
        eligible.add(key)
    # Token-only holder fee rows are ancillary ledger requirements. A separate
    # ETH arrival or supported conversion still protects the native ledger.
    auxiliary = set()
    for key in eligible:
        address, value = key.rsplit('|', 1)
        native = account_key(address, ETH)
        if value == WETH and native not in overlay['by_account'] and native not in protected:
            auxiliary.add(native)
    excluded = eligible | auxiliary
    copied = copy.deepcopy(plan)
    removed = [row for row in copied['rows'] if row['account_id'] in excluded]
    copied['rows'] = [row for row in copied['rows'] if row['account_id'] not in excluded]
    stopped = [d for d in overlay['decisions'] if d['action'] == 'STOP']
    pending = [d for d in overlay['decisions'] if d['action'] == 'PENDING']
    copied['cost_boundary_context'] = {'schema_version': SCHEMA,
        'policy': overlay['policy'], 'decisions_sha256': overlay['decisions_sha256'],
        'has_cost_boundary': bool(stopped), 'has_pending_identity_or_type': bool(pending),
        'stopped_state_keys': [d['state_key'] for d in stopped],
        'pending_state_keys': [d['state_key'] for d in pending],
        'deferred_account_ids': sorted(excluded), 'deferred_context_rows': removed,
        'retained_due_to_other_arrival_or_conversion': retained,
        'conditional_assumptions': [ASSUMPTION] if stopped or pending else [],
        'entry_semantics': 'EXISTING_R3_CANDIDATE_TO_OUTSIDE_RESERVOIR_NON_TARGET',
        'observed_return_semantics': 'EXISTING_R3_UNKNOWN_EXTERNAL_INCOMING_SHARED_PRIOR_EXITS',
        'candidate_events_deleted': False, 'coverage_promoted': False,
        'service_objectives_changed': False, 'new_source_pool_created': False,
        'individual_boundary_upper_bounds_addable': False,
        'policy_evidence_trust': 'ROOT_SOURCE_BOUND_POLICY_AND_CORE_VALIDATION; NO_NEW_IDENTITY_CHECK'}
    finite = [copy.deepcopy(row['finite_semantic_continuation']) for row in collection.get('states', [])
              if row.get('finite_semantic_continuation')]
    if finite:
        copied['cost_boundary_context']['finite_semantic_continuations'] = finite
        copied['cost_boundary_context']['finite_semantic_continuation_grants_ordinary_discovery'] = False
        copied['cost_boundary_context']['finite_semantic_continuation_claims_complete_ledger'] = False
    return copied


def current_prior_rows(collection, current_rows, prior_rows):
    """Under an enabled policy, history cannot add accounts outside current need.

    Current rows already include retained ordinary accounts and true native gas
    dependencies. For those accounts the caller still widens historical windows
    exactly as before. Removed rows remain losslessly available in the audit.
    """
    if collection.get('cost_boundary_policy', {}).get('enabled') is not True:
        return prior_rows, []
    domain = {row['account_id'] for row in current_rows}
    kept, deferred = [], []
    for row in prior_rows:
        if row['account_id'] in domain:
            kept.append(row)
        else:
            deferred.append({'original_row': copy.deepcopy(row), 'original_row_sha256': canonical(row),
                'reason': 'PRIOR_ACCOUNT_OUTSIDE_CURRENT_REPLAYED_CANDIDATE_AND_NECESSARY_DEPENDENCY_DOMAIN',
                'original_material_deleted': False})
    return kept, deferred


def annotate_result(result, plan):
    """Attach explicit conditions without changing flow variables or constraints."""
    info = plan.get('cost_boundary_context')
    if info is None:
        return result
    document = result.get('model_input')
    expected = {r['account_id'] for r in plan['rows']}
    reconciled = {r.get('account_id'): r for r in result.get('ledger_reconciliation', [])}
    # Native reconciliation has an exact retained-account contract. Token-only
    # completeness is not inferred from the ETH subset or a cost stop.
    ledger_complete = (not result.get('fact_conflicts') and expected == set(reconciled)
        and all(r.get('completion_status') == 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE'
                for r in reconciled.values()))
    from stage1d_gap_sequence import gap_count
    has_gaps = bool(gap_count(document.get('gaps', []))) if document is not None else True
    status = {'scope_complete': bool(ledger_complete and not has_gaps),
        'scope_complete_definition': 'RETAINED_CONTEXT_RECONCILED_WITH_NO_RECORDED_UNRESOLVED_GAPS',
        'retained_ordinary_ledger_reconciliation_complete': bool(ledger_complete),
        'has_recorded_context_gaps': has_gaps,
        'scope_complete_not_inferred_for_missing_token_reconciliation': True,
        'has_cost_boundary': info['has_cost_boundary'],
        'has_pending_identity_or_type': info['has_pending_identity_or_type'],
        'conditional_assumptions': copy.deepcopy(info['conditional_assumptions']),
        'untruncated_scope_strict_interval_claimed': False,
        'amount_scope': 'CONDITIONAL_OBSERVED_SCOPE' if info['conditional_assumptions'] else 'UNCHANGED_OBSERVED_SCOPE'}
    result['cost_boundary_scope'] = status
    if document is not None:
        document['cost_boundary_context'] = copy.deepcopy(info)
        document['cost_boundary_scope'] = copy.deepcopy(status)
        for assumption in info['conditional_assumptions']:
            if assumption not in document['assumptions']:
                document['assumptions'].append(assumption)
    return result
