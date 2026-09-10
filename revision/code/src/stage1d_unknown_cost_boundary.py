"""Pure state-scoped cost-boundary policy; no I/O and no acquisition side effects.

The structural decision validator does not certify external evidence. The root
resolver must verify source provenance and normalization before supplying its
bound evidence. ``verify_source_bytes`` additionally checks exact original bytes;
it is not a substitute for that provider-specific validation.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict, is_dataclass, dataclass, fields
import hashlib
import json
import re

from collector import Event, Scope, strictly_after
from physical_facts import PhysicalFactRegistry, canonical_event

AUTH = 'STAGE1D_UNKNOWN_CONTRACT_OR_RATE20_BOUNDARY_V1'
VERSION = 'stage1d-unknown-cost-boundary-v1'
POLICY_SCHEMA = 'stage1d-unknown-cost-policy-v1'
DECISION_SCHEMA = 'stage1d-unknown-cost-decision-v1'
AUTHORITY_SHA256 = 'PRIVATE_AUTHORITY_SHA256'
NATIVE = 'native:eip155:1'
WETH = 'erc20:eip155:1:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
STOP_REASONS = frozenset(('UNKNOWN_CODE_COST_BOUNDARY',
                         'UNKNOWN_NO_CODE_HIGH_OUTFLOW_COST_BOUNDARY'))
_SHA = re.compile(r'[0-9a-f]{64}')
_ADDRESS = re.compile(r'0x[0-9a-f]{40}')
_TX = re.compile(r'0x[0-9a-f]{64}')
_TOKEN = object()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _dict(value):
    return asdict(value) if is_dataclass(value) else dict(value)


def _integer(value, name):
    if type(value) is not int or value < 0:
        raise ValueError('Exact nonnegative integer required: ' + name)
    return value


def _asset(value):
    value = str(value).lower()
    if value == 'eth':
        value = NATIVE
    if value not in (NATIVE, WETH):
        raise ValueError('Unsupported chain-bound state asset')
    return value


def _scope(value):
    if isinstance(value, Scope):
        return value
    raw = _dict(value)
    if 'start_time_utc' in raw:
        result = Scope.from_policy(raw)
    else:
        result = Scope(**{f.name: raw[f.name] for f in fields(Scope) if f.name in raw})
    if raw.get('scope_hash') not in (None, result.scope_hash):
        raise ValueError('Explicit scope hash differs from frozen scope')
    return result


def _event(value):
    row = canonical_event(_dict(value))
    return Event(**{f.name: row[f.name] for f in fields(Event) if f.name in row})


def state_binding(state, scope):
    s, q = _dict(state), _scope(scope)
    a = _event(s['arrival'])
    address = str(s['address']).lower()
    if not _ADDRESS.fullmatch(address) or not a.event_id or not _TX.fullmatch(a.tx_hash):
        raise ValueError('Full state/arrival identity required')
    if a.chain_id != 'eip155:1' or s.get('chain_id', 'eip155:1') not in ('eip155:1', 1, '1'):
        raise ValueError('State outside Ethereum mainnet')
    asset = _asset(s['asset'])
    if _asset(a.asset) != asset or a.recipient != address:
        raise ValueError('Arrival is not a transfer into this state asset/address')
    if a.success is not True or type(a.amount_raw) is not int or a.amount_raw <= 0:
        raise ValueError('A legal successful positive arrival is required')
    if s['query_id'] != q.query_id:
        raise ValueError('State belongs to another query')
    if s.get('scope_hash') not in (None, q.scope_hash):
        raise ValueError('State carries another explicit scope hash')
    depth = _integer(s['depth'], 'depth')
    end = _integer(s['local_end'], 'local_end')
    if depth > q.max_depth or not (q.start_block <= a.block <= q.end_block):
        raise ValueError('State outside frozen block/depth scope')
    if not (q.start_time <= a.timestamp <= q.end_time) or end != q.local_end(a):
        raise ValueError('State window differs from active scope')
    context = s.get('protocol_context', 'ordinary')
    if not isinstance(context, str) or not context:
        raise ValueError('Invalid protocol context')
    return dict(query_id=q.query_id, chain_id=a.chain_id, address=address, asset=asset,
                arrival_event_id=a.event_id, depth=depth, local_end=end,
                protocol_context=context, scope_hash=q.scope_hash)


def state_key(state, scope):
    return digest(state_binding(state, scope))


def validate_policy(policy, *, scope=None):
    """Validate the immutable policy body; caller separately hashes its bytes."""
    if not isinstance(policy, dict) or policy.get('schema_version') != POLICY_SCHEMA or policy.get('authorization_id') != AUTH:
        raise ValueError('Unknown cost policy version/authority')
    if type(policy.get('enabled')) is not bool or type(policy.get('threshold_daily_strict_gt')) is not int or policy['threshold_daily_strict_gt'] != 20:
        raise ValueError('Frozen strict threshold/enable flag changed')
    if policy.get('authority_source_sha256') != AUTHORITY_SHA256:
        raise ValueError('Cost authority bytes differ')
    hashes = policy.get('query_scope_hashes')
    if not isinstance(hashes, dict) or len(hashes) != 4 or any(not isinstance(k, str) or not isinstance(v, str) or not _SHA.fullmatch(v) for k, v in hashes.items()):
        raise ValueError('Exactly four frozen query scope hashes required')
    if scope is not None:
        q = _scope(scope)
        if hashes.get(q.query_id, hashes.get(q.name)) != q.scope_hash:
            raise ValueError('Scope is not authorized by this policy')
    return True


def _refs(values):
    result, seen = [], set()
    for value in values:
        if not isinstance(value, dict) or not isinstance(value.get('path'), str) or not value['path']:
            raise ValueError('Evidence requires a path and exact SHA-256')
        if not isinstance(value.get('sha256'), str) or not _SHA.fullmatch(value['sha256']):
            raise ValueError('Invalid evidence SHA-256')
        key = (value['path'], value['sha256'])
        if key not in seen:
            result.append(deepcopy(value)); seen.add(key)
    return result


class VerifiedSources:
    """Opaque, in-memory byte verification result. Never deserialize this object."""
    def __init__(self, blobs, token):
        if token is not _TOKEN:
            raise ValueError('Use verify_source_bytes')
        self._blobs = blobs
        self._token = token

    def require(self, refs):
        refs = _refs(refs)
        if not refs:
            raise ValueError('At least one original evidence source is required')
        for ref in refs:
            if (ref['path'], ref['sha256']) not in self._blobs:
                raise ValueError('Original source bytes were not verified')
        return refs

    def json(self, ref):
        self.require([ref])
        return json.loads(self._blobs[(ref['path'], ref['sha256'])].decode('utf-8-sig'))


def verify_source_bytes(items):
    """items: iterable of (bound_ref, original_bytes), no filesystem access."""
    blobs = {}
    for ref, payload in items:
        ref = _refs([ref])[0]
        if not isinstance(payload, bytes) or hashlib.sha256(payload).hexdigest() != ref['sha256']:
            raise ValueError('Original source SHA-256 mismatch')
        blobs[(ref['path'], ref['sha256'])] = payload
    return VerifiedSources(blobs, _TOKEN)


@dataclass(frozen=True)
class BoundEvidence(Mapping):
    """Produced by this module only; .to_dict() is a report, not a capability."""
    _data: dict
    _token: object

    def __post_init__(self):
        if self._token is not _TOKEN:
            raise ValueError('Evidence must be rebuilt from verified inputs')

    def __getitem__(self, key):
        return deepcopy(self._data[key])

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def to_dict(self):
        return deepcopy(self._data)


def _bound(kind, data, sources=None, refs=()):
    refs = _refs(refs)
    verified = isinstance(sources, VerifiedSources) and sources._token is _TOKEN
    if verified:
        refs = sources.require(refs)
    result = dict(data, evidence_kind=kind, evidence_refs=refs,
                  source_bytes_verified=verified, implementation_version=VERSION)
    result['evidence_sha256'] = digest(result)
    return BoundEvidence(result, _TOKEN)


def activity(state, scope, events, *, sources=None, evidence_refs=(), previous=None):
    """Reliable observed lower bound, independent of candidate membership.

    ``events`` must be current-authorized normalized actual facts; all previously
    admitted observations may be passed on every replay. A previous BoundEvidence
    can retain its witnesses after candidate projection changes. The function
    never claims account completeness, including when its observed rate is low.
    """
    binding = state_binding(state, scope)
    q, arrival = _scope(scope), _event(_dict(state)['arrival'])
    registry = PhysicalFactRegistry()
    registry.add(asdict(arrival), source='current_arrival')
    excluded = Counter()
    material = []
    refs = list(evidence_refs)
    if previous is not None:
        if not isinstance(previous, BoundEvidence) or previous.get('evidence_kind') != 'activity' or previous.get('state_key') != state_key(state, scope):
            raise ValueError('Previous activity belongs to another frozen state')
        material.extend(previous['witness_events'])
        refs.extend(previous['evidence_refs'])
    material.extend(events)
    for raw in material:
        try:
            raw = _dict(raw)
            if raw.get('semantic_virtual') or raw.get('is_virtual') or str(raw.get('event_id', '')).startswith('semport:') or str(raw.get('kind', '')).lower() not in ('top', 'internal', 'erc20'):
                excluded['NOT_PHYSICAL_VALUE_TRANSFER'] += 1; continue
            if any(raw.get(k) is False for k in ('tx_success', 'ancestor_success', 'ancestors_success', 'effective_success')) or raw.get('reverted') is True:
                # Feed the contrary physical outcome into reconciliation rather
                # than discard it and accidentally trust a successful duplicate.
                raw['success'] = False
            row = canonical_event(raw)
            # An absent/empty-string internal location is not an explicit root.
            if row['kind'] == 'internal' and (raw.get('trace_address') is None or raw.get('trace_address') == ''):
                excluded['PHYSICAL_POSITION_UNRESOLVED'] += 1; continue
            if row['kind'] == 'erc20' and row.get('log_index') is None:
                excluded['PHYSICAL_POSITION_UNRESOLVED'] += 1; continue
            registry.add(row, source='observed_activity')
        except (TypeError, ValueError, KeyError):
            excluded['MALFORMED_FACT'] += 1
    snap = registry.snapshot()
    canonical_arrival = registry.get(arrival.event_id)
    available = canonical_arrival is not None
    witnesses, txs, counterparts = [], set(), set()
    for row in snap['events']:
        try:
            e = _event(row)
            if e.event_id == arrival.event_id:
                continue
            if e.chain_id != binding['chain_id'] or e.asset != binding['asset']:
                excluded['OTHER_CHAIN_OR_ASSET'] += 1; continue
            if (e.asset == NATIVE and e.kind not in ('top', 'internal')) or (e.asset == WETH and e.kind != 'erc20'):
                excluded['ASSET_EVENT_KIND_MISMATCH'] += 1; continue
            if e.success is not True or type(e.amount_raw) is not int or e.amount_raw <= 0:
                excluded['FAILED_ZERO_OR_MISSING_AMOUNT'] += 1; continue
            if e.sender != binding['address'] or e.recipient == e.sender or not _ADDRESS.fullmatch(str(e.recipient)):
                excluded['NOT_QUALIFYING_OUTFLOW'] += 1; continue
            if not _TX.fullmatch(e.tx_hash) or e.block is None or e.timestamp is None:
                excluded['MISSING_CHAIN_POSITION'] += 1; continue
            if not (q.start_block <= e.block <= q.end_block and arrival.timestamp <= e.timestamp <= binding['local_end']):
                excluded['OUTSIDE_WINDOW'] += 1; continue
            after = strictly_after(e, arrival)
            if after is not True:
                excluded['ORDER_UNKNOWN' if after is None else 'NOT_AFTER_ARRIVAL'] += 1; continue
            witnesses.append(asdict(e)); txs.add((e.chain_id, e.tx_hash)); counterparts.add(e.recipient)
        except (TypeError, ValueError, KeyError):
            excluded['UNUSABLE_RECONCILED_FACT'] += 1
    duration = binding['local_end'] - arrival.timestamp
    count = len(txs) if available else None
    high = available and duration > 0 and count * 86400 > 20 * duration
    result = dict(state_key=state_key(state, scope), state_binding=binding,
                  arrival_timestamp=arrival.timestamp, t_end=binding['local_end'],
                  D_seconds=duration, N_obs=count, actual_count=None,
                  observed_rate_numerator=None if count is None or duration == 0 else count * 86400,
                  observed_rate_denominator=duration if available and duration > 0 else None,
                  strict_gt20_observed=bool(high), rate_truth='TRUE' if high else ('UNKNOWN' if available and duration > 0 else 'UNAVAILABLE'),
                  coverage='OBSERVED_LOWER_BOUND' if available else 'UNAVAILABLE', full_coverage=False,
                  short_window_rate_unstable=0 < duration < 86400,
                  physical_event_count=len(witnesses) if available else None,
                  unique_counterpart_count=len(counterparts) if available else None,
                  transaction_keys=[list(x) for x in sorted(txs)] if available else [],
                  witness_events=witnesses if available else [],
                  excluded_counts=dict(sorted(excluded.items())), conflicts=snap['conflicts'])
    return _bound('activity', result, sources, refs)


def code_status(value):
    if not isinstance(value, str) or re.fullmatch(r'0x(?:[0-9a-fA-F]{2})*', value) is None:
        return 'TYPE_UNRESOLVED'
    return 'NO_RUNTIME_CODE_AT_BLOCK' if value == '0x' else 'CODE_PRESENT'


def classify_code(state, scope, request, response, *, sources=None, evidence_refs=(), conflict=False):
    """Inspect an exact historical RPC pair; root verifies its provider binding."""
    binding = state_binding(state, scope)
    arrival = _event(_dict(state)['arrival'])
    status = 'TYPE_UNRESOLVED'
    params = request.get('params', []) if isinstance(request, dict) else []
    exact = (isinstance(request, dict) and request.get('method') == 'eth_getCode' and
             len(params) == 2 and str(params[0]).lower() == binding['address'] and
             isinstance(params[1], str) and re.fullmatch(r'0x[0-9a-fA-F]+', params[1]) is not None and
             int(params[1], 16) == arrival.block)
    if exact and not conflict and isinstance(response, dict) and not response.get('error'):
        status = code_status(response.get('result'))
    return _bound('historical_code', dict(chain_id=binding['chain_id'], address=binding['address'],
                  block=arrival.block, block_hash=arrival.block_hash, status=status,
                  request=deepcopy(request), response=deepcopy(response),
                  request_sha256=digest(request), response_sha256=digest(response),
                  same_block_conflict=bool(conflict)), sources, evidence_refs)


def bind_identity(state, scope, channels, *, sources, evidence_refs):
    """Bind root-normalized finite check statuses to verified original sources.

    Each channel has a status and bound evidence_refs. A reused result retains
    its original successful status, with optional reused=True. This API does not
    turn arbitrary labels, a nonempty name, or checked=True into an identity.
    """
    b = state_binding(state, scope)
    allowed = {'local': 'LOCAL_CHECKED', 'online': 'ONLINE_CHECKED_NO_ADOPTABLE_ROLE',
               'exact_search': 'EXACT_SEARCH_CHECKED'}
    pending = {'UNQUERIED', 'LOOKUP_FAILED', 'ACCESS_BLOCKED', 'ROLE_CONFLICT'}
    normalized = {}
    for key, success in allowed.items():
        row = deepcopy(channels.get(key, {'status': 'UNQUERIED', 'evidence_refs': []}))
        if not isinstance(row, dict) or row.get('status') not in pending | {success}:
            raise ValueError('Invalid finite identity check status: ' + key)
        if row['status'] == success:
            if not isinstance(sources, VerifiedSources):
                raise ValueError('Successful check requires verified original sources')
            row['evidence_refs'] = sources.require(row.get('evidence_refs', []))
        else:
            row['evidence_refs'] = _refs(row.get('evidence_refs', []))
        normalized[key] = row
    return _bound('identity', dict(chain_id=b['chain_id'], address=b['address'], channels=normalized,
                  checked_unknown=all(normalized[k]['status'] == v for k, v in allowed.items())),
                  sources, evidence_refs)


def make_decision(state, scope, *, policy_sha256, action, reason, evidence_refs=(), **details):
    b = state_binding(state, scope)
    value = dict(schema_version=DECISION_SCHEMA, authorization_id=AUTH,
                 policy_sha256=policy_sha256, state_key=state_key(state, scope),
                 query_id=b['query_id'], scope_hash=b['scope_hash'], action=action,
                 reason=reason, evidence_refs=_refs(evidence_refs))
    if set(details) & (set(value) | {'decision_sha256'}):
        raise ValueError('Details cannot override bound decision identity')
    value.update(deepcopy(details))
    value['decision_sha256'] = digest(value)
    validate_decision(state, scope, value, policy_sha256)
    return value


def validate_decision(state, scope, decision, policy_sha256):
    """Pure structural/domain/hash validation only; external proofs stay root-owned."""
    b = state_binding(state, scope)
    if not isinstance(decision, dict) or not isinstance(policy_sha256, str) or not _SHA.fullmatch(policy_sha256):
        raise ValueError('Invalid decision/policy identity')
    expected = dict(schema_version=DECISION_SCHEMA, authorization_id=AUTH,
                    policy_sha256=policy_sha256, state_key=state_key(state, scope),
                    query_id=b['query_id'], scope_hash=b['scope_hash'])
    if any(decision.get(k) != v for k, v in expected.items()):
        raise ValueError('Decision differs from frozen state/policy binding')
    action, reason = decision.get('action'), decision.get('reason')
    if action not in ('STOP', 'PENDING', 'CONTINUE', 'BYPASS') or not isinstance(reason, str) or not reason:
        raise ValueError('Invalid decision action/reason')
    if (action == 'STOP') != (reason in STOP_REASONS):
        raise ValueError('Cost STOP requires an exact authorized reason')
    refs = _refs(decision.get('evidence_refs', []))
    if action == 'STOP' and not refs:
        raise ValueError('Cost STOP requires bound evidence references')
    if any(decision.get(k) is True for k in ('chain_verified', 'source_zero', 'certified_zero', 'is_service', 'full_coverage')):
        raise ValueError('Cost policy does not certify identity, zero, or downstream coverage')
    body = {k: v for k, v in decision.items() if k != 'decision_sha256'}
    if decision.get('decision_sha256') != digest(body):
        raise ValueError('Decision digest mismatch')
    return True


def _evidence(value, kind, binding):
    if not isinstance(value, BoundEvidence) or value._token is not _TOKEN or value.get('evidence_kind') != kind or not value.get('source_bytes_verified'):
        return None
    data = value.to_dict()
    if kind == 'activity':
        if data['state_binding'] != binding:
            raise ValueError('Activity belongs to another state/scope')
    elif data['chain_id'] != binding['chain_id'] or data['address'] != binding['address']:
        raise ValueError('Evidence belongs to another chain/address')
    return data


def _activity_summary(value):
    if value is None:
        return None
    # Raw observations belong in the persistent evidence ledger, not repeated in
    # every collection row and decision overlay. The evidence digest binds them.
    keys = ('state_key', 'evidence_sha256', 'evidence_refs', 'N_obs', 'D_seconds',
            'arrival_timestamp', 't_end', 'observed_rate_numerator',
            'observed_rate_denominator', 'strict_gt20_observed', 'rate_truth',
            'coverage', 'full_coverage', 'short_window_rate_unstable',
            'physical_event_count', 'unique_counterpart_count')
    return {k: deepcopy(value[k]) for k in keys}


def decide(state, scope, base_identity, *, policy_sha256, identity=None, code=None,
           observed_activity=None, enabled=True, supported_operation=False):
    """Return a flat overlay, never modify identity; root persists it per state.

    supported_operation must be an already adopted role's certificate reference
    dictionary, not True. Instance certification remains the existing resolver's
    responsibility and cannot be inferred from WETH's address alone.
    """
    b, q = state_binding(state, scope), _scope(scope)
    original = deepcopy(base_identity)
    refs = []
    def result(action, reason, **details):
        return make_decision(state, scope, policy_sha256=policy_sha256, action=action,
                             reason=reason, evidence_refs=refs, base_identity=original,
                             implementation_version=VERSION, **details)
    if type(enabled) is not bool:
        raise ValueError('Policy enabled must be boolean')
    if not enabled:
        return result('BYPASS', 'POLICY_DISABLED')
    if original.get('branch_action') == 'USER_REQUESTED_BRANCH_HOLD':
        return result('BYPASS', 'EXISTING_USER_REQUESTED_BRANCH_HOLD')
    if original.get('kind') in ('SERVICE', 'BRIDGE', 'MIXER', 'UNSUPPORTED_PROTOCOL', 'ORDINARY'):
        return result('BYPASS', 'EXISTING_ADOPTED_ROLE')
    if original.get('kind') == 'SUPPORTED_PROTOCOL' and original.get('branch_action') == 'SUPPORTED_OPERATION_RESOLVE':
        certificate_ids = original.get('role_certificate_ids')
        if isinstance(certificate_ids, list) and certificate_ids and all(isinstance(x, str) and x for x in certificate_ids):
            # These are the existing TechnicalRoles.resolve output references,
            # already source-reverified by that resolver. Instance resolution is
            # still mandatory and remains outside this generic cost policy.
            return result('BYPASS', 'EXISTING_SUPPORTED_OPERATION')
    if supported_operation:
        if not isinstance(supported_operation, dict) or not supported_operation.get('evidence_refs'):
            raise ValueError('Supported operation requires adopted certificate references')
        refs.extend(_refs(supported_operation['evidence_refs']))
        return result('BYPASS', 'EXISTING_SUPPORTED_OPERATION')
    if b['depth'] >= q.max_depth or b['local_end'] <= _event(_dict(state)['arrival']).timestamp:
        return result('BYPASS', 'EXISTING_DEPTH_OR_TIME_BOUNDARY')
    if original.get('kind') not in (None, 'UNKNOWN'):
        return result('PENDING', 'IDENTITY_CHECK_PENDING')
    identity_data = _evidence(identity, 'identity', b)
    code_data = _evidence(code, 'historical_code', b)
    activity_data = _evidence(observed_activity, 'activity', b)
    for item in (identity_data, code_data, activity_data):
        if item:
            refs.extend(item['evidence_refs'])
    if identity_data is None:
        return result('PENDING', 'IDENTITY_CHECK_PENDING')
    channels = identity_data['channels']
    if channels['local']['status'] != 'LOCAL_CHECKED' or channels['online']['status'] != 'ONLINE_CHECKED_NO_ADOPTABLE_ROLE' or channels['exact_search']['status'] in ('LOOKUP_FAILED', 'ACCESS_BLOCKED', 'ROLE_CONFLICT'):
        return result('PENDING', 'IDENTITY_CHECK_PENDING', identity_checks=channels)
    arrival = _event(_dict(state)['arrival'])
    if code_data is None or code_data['block'] != arrival.block or code_data['status'] == 'TYPE_UNRESOLVED':
        return result('PENDING', 'TYPE_UNRESOLVED', identity_checks=channels)
    high = activity_data is not None and activity_data['strict_gt20_observed']
    if code_data['status'] == 'CODE_PRESENT' or high:
        if not identity_data['checked_unknown']:
            return result('PENDING', 'IDENTITY_CHECK_PENDING', identity_checks=channels, code_status=code_data['status'])
        reason = 'UNKNOWN_CODE_COST_BOUNDARY' if code_data['status'] == 'CODE_PRESENT' else 'UNKNOWN_NO_CODE_HIGH_OUTFLOW_COST_BOUNDARY'
        return result('STOP', reason, identity_checks=channels, code_status=code_data['status'],
                      historical_block=arrival.block, activity=_activity_summary(activity_data),
                      scope_complete=False, has_cost_boundary=True)
    return result('CONTINUE', 'NO_CODE_RATE_NOT_PROVEN_ABOVE_20', identity_checks=channels,
                  code_status=code_data['status'], activity=_activity_summary(activity_data),
                  scope_complete=False, has_cost_boundary=False)
