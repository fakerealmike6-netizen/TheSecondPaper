"""Read-only, current-state authorization of candidate discovery rectangles.

This is a request guard, not provider coverage or a context-ledger permission.
No evidence acquisition, state replay, database, counter or request key is changed.
The caller must run it again at the actual dispatch boundary under its existing
single-writer discipline; this function deliberately acquires no writer lock.
"""
from __future__ import annotations

from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re

from collector import Scope
from stage1d_cost_boundary_context import validate_overlay
from stage1d_unknown_cost_boundary import state_key, state_binding
from stage1d_window import missing_rectangles

SCHEMA = 'stage1d-cost-candidate-request-guard-v1'
FIELDS = ('start_block', 'end_block', 'start_time', 'end_time')
NATIVE = 'native:eip155:1'
WETH = 'erc20:eip155:1:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
_SHA = re.compile(r'[0-9a-f]{64}')
_ADDRESS = re.compile(r'0x[0-9a-f]{40}')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
        ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _path(work, relative):
    if not isinstance(relative, str) or not relative or '\\' in relative:
        raise ValueError('Guard requires a code-relative POSIX path')
    rel = Path(relative)
    if rel.is_absolute() or ':' in relative or any(p in ('.', '..') for p in relative.split('/')):
        raise ValueError('Guard path escapes the current code')
    target = work / rel
    for p in (target, *target.parents):
        if p == work:
            break
        if p.is_symlink() or (hasattr(p, 'is_junction') and p.is_junction()):
            raise ValueError('Guard input may not redirect through a link')
    if not target.resolve().is_relative_to(work):
        raise ValueError('Guard input is outside the current code')
    return target


def _sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _read_collection(work, query, entry):
    expected_path = 'derived/stage1d/queries/' + query['name'] + '/collection.json'
    if entry.get('collection_path') != expected_path:
        raise ValueError('Candidate guard requires the current collection alias, not an old snapshot')
    expected_sha = entry.get('collection_sha256')
    if not isinstance(expected_sha, str) or not _SHA.fullmatch(expected_sha):
        raise ValueError('Candidate collection requires its exact original SHA')
    path = _path(work, expected_path)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha:
        raise ValueError('Candidate entry collection is stale or changed')
    return path, json.loads(raw.decode('utf-8-sig'))


def _asset(value):
    if value == 'ETH':
        return NATIVE
    if value not in (NATIVE, WETH):
        raise ValueError('Candidate rectangle asset is outside the exact current asset domain')
    return value


def _rectangle(row, query, scope):
    if not isinstance(row, dict):
        raise ValueError('Candidate rectangle must be an object')
    for key, expected in (('query_name', query['name']), ('query_id', scope.query_id),
                          ('scope_hash', scope.scope_hash), ('scope_id', scope.scope_id)):
        if key in row and row[key] != expected:
            raise ValueError('Candidate rectangle has another query or scope: ' + key)
    if row.get('chain_id', 'eip155:1') != 'eip155:1':
        raise ValueError('Candidate rectangle has another chain')
    if row.get('direction', 'OUTGOING') != 'OUTGOING':
        raise ValueError('This guard only authorizes candidate outgoing discovery')
    if not isinstance(row.get('address'), str) or not _ADDRESS.fullmatch(row['address']):
        raise ValueError('Candidate rectangle requires a full normalized address')
    value = dict(address=row['address'], asset=_asset(row.get('asset')))
    for key in FIELDS:
        if type(row.get(key)) is not int:
            raise ValueError('Candidate rectangle bounds require exact integers: ' + key)
        value[key] = row[key]
    if not (scope.start_block <= value['start_block'] <= value['end_block'] <= scope.end_block
            and scope.start_time <= value['start_time'] <= value['end_time'] <= scope.end_time):
        raise ValueError('Candidate rectangle is inverted or outside the frozen domain')
    return value


def _allowed(collection, scope):
    # A saved stop is stronger than a BYPASS label. In particular a user hold is
    # an UNKNOWN identity plus a task boundary, never permission to keep crawling.
    stopped = set()
    for row in collection.get('stops', []):
        stopped.add(state_key(row['state'], scope))
    allowed = defaultdict(list)
    state_keys = []
    excluded = defaultdict(int)
    for row in collection['states']:
        state, identity, decision = row['state'], row['identity'], row['cost_boundary']
        binding = state_binding(state, scope)
        key = decision['state_key']
        reason = None
        if decision['action'] not in ('CONTINUE', 'BYPASS'):
            reason = decision['action']
        elif key in stopped:
            reason = 'EXISTING_STOP'
        elif identity.get('kind') in ('SERVICE', 'BRIDGE', 'MIXER', 'UNSUPPORTED_PROTOCOL'):
            reason = 'EXISTING_ROLE_BOUNDARY'
        elif identity.get('branch_action') in ('USER_REQUESTED_BRANCH_HOLD',
                'SUPPORTED_OPERATION_RESOLVE', 'UNSUPPORTED_PROTOCOL_STOP', 'FIRST_SERVICE_STOP'):
            reason = 'EXISTING_ROLE_OR_TASK_ACTION'
        elif state['depth'] >= scope.max_depth or state.get('protocol_context', 'ordinary') != 'ordinary':
            reason = 'DEPTH_OR_COMPONENT_BOUNDARY'
        if reason:
            excluded[reason] += 1
            continue
        # complete=True is consumed solely as a geometric union mask by the
        # existing exact rectangle algebra. It is NOT an acquisition claim and
        # is never emitted as provider coverage or saved in the content cache.
        rectangle = dict(address=binding['address'], asset=binding['asset'],
            start_block=state['arrival']['block'], end_block=scope.end_block,
            start_time=state['arrival']['timestamp'], end_time=state['local_end'], complete=True)
        allowed[(rectangle['address'], rectangle['asset'])].append(rectangle)
        state_keys.append(key)
    return allowed, state_keys, dict(excluded)


def validate_candidate_entry(work, query, entry):
    """Reject stale identity or requests outside the union of live arrivals.

    ``entry`` uses the existing boundary fields collection_path/sha256,
    query_name/query_id/scope_hash and needed_ranges. A pending-only caller may
    supply pending_intervals instead. When both lists exist both are verified;
    pending_intervals must be an actual list, never a count masquerading as data.
    An absent registry/policy preserves the previous pipeline unchanged.
    """
    from stage1d_unknown_cost_registry import Registry
    work = Path(work).resolve()
    if not isinstance(query, dict) or not re.fullmatch(r'[A-Za-z0-9_-]+', str(query.get('name', ''))):
        raise ValueError('Invalid current query name')
    scope = Scope.from_policy(query)
    registry = Registry(work)
    policy = registry.policy_for_scope(scope)
    if policy is None:
        # Existing callers already validate their own entry in the old path.
        # Do not upgrade that path to a new, undeclared scope or budget.
        alias = _path(work, 'derived/stage1d/queries/' + query['name'] + '/collection.json')
        if alias.exists():
            # Only inspect an existing collection when the registry is absent:
            # removing CURRENT must not silently disable a previously active gate.
            old = json.loads(alias.read_text(encoding='utf-8-sig'))
            if old.get('cost_boundary_policy', {}).get('enabled') is True:
                raise ValueError('Current collection declares a now-missing cost registry policy')
        if Registry(work).identity != registry.identity:
            raise ValueError('Cost registry changed during old-scope guard')
        return {'schema_version': SCHEMA, 'status': 'NOT_APPLICABLE_UNCHANGED',
                'query_id': scope.query_id, 'scope_hash': scope.scope_hash,
                'provider_coverage_claimed': False}
    if query.get('scope_hash') != scope.scope_hash or query.get('scope_id') != scope.scope_id:
        raise ValueError('Candidate query frozen scope identity differs')
    if not isinstance(entry, dict):
        raise ValueError('Candidate entry must be an object')
    for key, expected in (('query_name', query['name']), ('query_id', scope.query_id), ('scope_hash', scope.scope_hash)):
        if entry.get(key) != expected:
            raise ValueError('Candidate entry belongs to another query or scope')
    if entry.get('scope_id', scope.scope_id) != scope.scope_id:
        raise ValueError('Candidate entry scope ID differs')
    path, collection = _read_collection(work, query, entry)
    if collection.get('query_id') != scope.query_id:
        raise ValueError('Current collection belongs to another query')
    metrics = collection.get('metrics', {})
    if metrics.get('scope_hash') != scope.scope_hash or metrics.get('scope_freeze') != scope.freeze_dict():
        raise ValueError('Current collection has another frozen scope or window')
    if collection.get('cost_boundary_policy') != policy:
        raise ValueError('Current collection does not bind the exact adopted cost policy metadata')
    if metrics.get('cost_boundary_resolver_sha256') != registry.identity:
        raise ValueError('Current collection does not bind the exact current cost registry identity')
    validated = validate_overlay(query, collection, expected_policy_sha256=policy['policy_sha256'])
    if validated is None:
        raise ValueError('Current policy has no corresponding saved state decisions')
    evidence_refs = [ref for decision in validated['decisions'] for ref in decision['evidence_refs']]
    registry.verify_evidence_refs(evidence_refs)
    allowed, state_keys, excluded = _allowed(collection, scope)
    checked = {}
    if not any(key in entry for key in ('needed_ranges', 'pending_intervals')):
        raise ValueError('Candidate guard requires explicit request rectangles')
    for field in ('needed_ranges', 'pending_intervals'):
        if field not in entry:
            continue
        if not isinstance(entry[field], list):
            raise ValueError('Candidate request rectangles must be a list: ' + field)
        rectangles = []
        for original in entry[field]:
            rectangle = _rectangle(original, query, scope)
            outside = missing_rectangles(rectangle, allowed.get((rectangle['address'], rectangle['asset']), ()))
            if outside:
                raise ValueError('Candidate rectangle exceeds current CONTINUE/BYPASS arrival union: '
                    + field + ' ' + digest(original) + ' uncovered=' + json.dumps(outside[:1], sort_keys=True))
            rectangles.append(rectangle)
        checked[field] = {'count': len(rectangles), 'rectangles_sha256': digest(rectangles)}
    source_recheck = registry.assert_source_snapshot_unchanged()
    if _sha(path) != entry['collection_sha256']:
        raise ValueError('Current collection changed during candidate guard')
    result = {'schema_version': SCHEMA, 'status': 'CURRENT_COST_STATE_REQUEST_DOMAIN_VERIFIED',
        'query_name': query['name'], 'query_id': scope.query_id, 'scope_hash': scope.scope_hash,
        'collection_ref': {'path': entry['collection_path'], 'sha256': entry['collection_sha256']},
        'policy_sha256': policy['policy_sha256'], 'registry_identity_sha256': registry.identity,
        'decisions_sha256': validated['decisions_sha256'], 'allowed_state_count': len(state_keys),
        'allowed_state_keys_sha256': digest(state_keys), 'excluded_state_counts': excluded,
        'request_sets': checked, 'authorization_domain': 'CANDIDATE_DISCOVERY_ONLY',
        'source_recheck': source_recheck,
        'provider_coverage_claimed': False, 'source_zero_claimed': False,
        'request_keys_changed': False, 'resource_counters_changed': False}
    return result | {'guard_sha256': digest(result)}


def validate_bq_candidate_request(work, query, document):
    """Guard only new batch-discovery preparation/submission, never context jobs.

    Actual supported shapes are the schema-less CURRENT_NEEDED_RECTANGLES
    document, batch-binding PREPARATION and its versioned dryrun spec. The
    caller invokes this before a new dryrun/job reservation, not while reading
    submitted/uncertain/complete jobs. No saved request identity is rewritten.
    """
    if not isinstance(document, dict):
        raise ValueError('BQ request document must be an object')
    schema = document.get('schema_version')
    marker = document.get('batch_binding_sql_version')
    prep = 'stage1d-batch-binding-preparation-v1'
    has_marker = 'batch_binding_sql_version' in document
    discovery = has_marker or schema == prep or 'need_rectangles' in document
    if not discovery:
        return {'schema_version': SCHEMA, 'status': 'NOT_BATCH_DISCOVERY_UNCHANGED'}
    if (has_marker and marker != 'stage1d-batch-binding-route-v1'
            or schema not in (None, prep, 'stage1d-bigquery-dryrun-spec-v1')):
        raise ValueError('Unknown or conflicting BQ discovery schema/version')
    if schema == 'stage1d-bigquery-dryrun-spec-v1' and not has_marker:
        raise ValueError('BQ discovery dryrun requires its explicit batch SQL version')
    if document.get('query_id') != query['query_id'] or document.get('scope_hash') != query['scope_hash']:
        raise ValueError('BQ discovery document belongs to another query/scope')
    if not isinstance(document.get('need_rectangles'), list):
        raise ValueError('BQ discovery requires exact logical need rectangles')
    root = Path(work).resolve()
    relative = 'derived/stage1d/queries/' + query['name'] + '/collection.json'
    path = _path(root, relative)
    # Old batch preparation can predate a saved collection. Let the same core
    # guard decide the no-policy case before requiring that new artifact; a
    # present/malformed/new policy still fails closed on its missing SHA.
    current_sha = _sha(path) if path.is_file() else None
    entry = {'query_name': query['name'], 'query_id': query['query_id'],
        'scope_hash': query['scope_hash'], 'collection_path': relative,
        'collection_sha256': current_sha, 'needed_ranges': document['need_rectangles']}
    return validate_candidate_entry(root, query, entry)
