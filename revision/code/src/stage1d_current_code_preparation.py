"""Validate bounded historical code demands against current reachable states.

No network, cache mutation, graph update or initial snapshot replacement.
"""
from pathlib import Path
import hashlib
import json
from collector import Scope
from stage1d_closure_scope import active_batch
from stage1d_runtime import Runtime
from read_retry_r4 import logical_key
from stage1d_unknown_cost_boundary import AUTH, state_key, validate_decision

SCHEMA = 'stage1d-current-pending-historical-code-preparation-v1'
PROVIDER = 'ALCHEMY_ETH_MAINNET_EXISTING'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def checked(root, ref, *, decode=True):
    root = Path(root).resolve()
    path = (root / ref['path']).resolve()
    if not path.is_relative_to(root):
        raise ValueError('Current demand reference escapes revision')
    for part in (path, *path.parents):
        if part == root: break
        if part.is_symlink() or getattr(part, 'is_junction', lambda: False)():
            raise ValueError('Linked current demand input')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != ref['sha256']:
        raise ValueError('Current demand source changed: ' + ref['path'])
    return json.loads(raw) if decode else raw


def validate(work, preparation_ref, *, runtime=None):
    """Return exact original states, scopes and request bindings for one batch."""
    work = Path(work).resolve(); root = work.parent
    runtime = runtime or Runtime()
    doc = checked(root, preparation_ref)
    if doc.get('schema_version') != SCHEMA or doc.get('authorization_id') != AUTH:
        raise ValueError('Current code preparation schema/authority differs')
    sources = doc['source_and_current_input_sha256']
    for path, value in sources.items(): checked(root, {'path': path, 'sha256': value}, decode=False)
    # Detect newly added label inputs too; hashing only the old list is insufficient.
    label_paths = {p.relative_to(root).as_posix() for p in (work/'derived/stage1d/labels').glob('*.json')}
    if label_paths != {p for p in sources if p.startswith('code/derived/stage1d/labels/') and p.endswith('.json')}:
        raise ValueError('Current label inventory changed')
    queries = {q['name']: q for q in active_batch(work)['queries']}
    if set(doc['current_collection_refs']) != set(queries):
        raise ValueError('Current query collection set differs')
    scopes = {name: Scope.from_policy(q) for name, q in queries.items()}
    collections = {}
    for name, ref in doc['current_collection_refs'].items():
        expected = 'code/derived/stage1d/queries/' + name + '/collection.json'
        if ref['path'] != expected or sources.get(expected) != ref['sha256']:
            raise ValueError('Demand is not bound to the live collection')
        collections[name] = checked(root, ref)
    groups = {}; rows = []
    for group in doc['code_groups']:
        key = digest([group['chain_id'], group['address'], group['arrival_block']])
        if key != group['group_key'] or key in groups or group['chain_id'] != 'eip155:1':
            raise ValueError('Original physical code group differs')
        if group['request'] != {'method':'eth_getCode', 'params':[group['address'], hex(group['arrival_block'])]}:
            raise ValueError('Historical code selector differs from arrival')
        bound = []
        for member in group['states']:
            name = member['query_name']; scope = scopes[name]; co = collections[name]
            record = co['states'][member['current_row_index']]
            state = record['state']; decision = record['cost_boundary']
            validate_decision(state, scope, decision, co['cost_boundary_policy']['policy_sha256'])
            if (state_key(state, scope) != member['state_key'] or
                    decision['decision_sha256'] != member['saved_decision_sha256'] or
                    member['collection_ref'] != doc['current_collection_refs'][name] or
                    member['scope_hash'] != scope.scope_hash or member['query_id'] != scope.query_id or
                    state['address'] != group['address'] or state['arrival']['block'] != group['arrival_block']):
                raise ValueError('Current arrival/decision binding differs')
            if (decision['action'] != 'PENDING' or decision['reason'] not in ('TYPE_UNRESOLVED', 'IDENTITY_CHECK_PENDING') or
                    state['depth'] >= scope.max_depth or state['local_end'] <= state['arrival']['timestamp']):
                raise ValueError('Historical code request targets a nonpending or stopped state')
            if any(v.get('status') in ('LOOKUP_FAILED','ACCESS_BLOCKED','ROLE_CONFLICT') for v in decision.get('identity_checks', {}).values()):
                raise ValueError('Finite identity failure may not be silently reopened')
            identity = member['current_identity']
            if (identity.get('kind') != 'UNKNOWN' or identity.get('branch_action') not in (None, 'NORMAL_ACCOUNT_EXPAND') or
                    group['label_status']['online_label_status'] != 'SUCCESS'):
                raise ValueError('Current finite label/role requirement differs')
            bound.append({'query_name':name, 'state':state})
        if not bound: raise ValueError('Physical selector has no current reachable state')
        groups[key] = {'group':group, 'states':bound}
        rows.extend(bound)
    nominated = doc['next_batch']['requests']; seen = set()
    if not 0 <= len(nominated) <= 100: raise ValueError('Finite batch exceeds bound')
    for need in nominated:
        entry = groups[need['first_group_key']]; group = entry['group']
        plan = runtime.validate_rpc(need['request'])
        allowed = (group['request'], {'method':'eth_getBlockByNumber','params':[hex(group['arrival_block']),False]})
        key = logical_key(runtime.rpc_identity(PROVIDER, plan))
        if plan not in allowed or key != need['logical_key'] or key in seen:
            raise ValueError('Current request key/selector differs or duplicates')
        if group['block_binding_status'] == 'BLOCK_HASH_CONFLICT':
            raise ValueError('Conflicting physical block cannot authorize requests')
        if set(need['query_names']) != {s['query_name'] for s in entry['states']}:
            raise ValueError('Original clock owners differ')
        seen.add(key)
    return doc, rows, {s.query_id:s for s in scopes.values()}, groups
