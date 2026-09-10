"""Incremental adoption of already successful exact historical-code members."""
from copy import deepcopy
from pathlib import Path
import json
from collector import Scope
from context_access_r3 import read, sha
from page_attempts import atomic_json
from read_retry_r4 import logical_key
from stage1d_runtime import Runtime
from stage1d_unknown_cost_registry import Registry, CODE_SCHEMA, CURRENT_PATH, PROVIDER
from stage1d_transfers_acquisition import cached_member
from stage1d_unknown_cost_boundary import state_key


def _immutable(path, document):
    if path.exists():
        if read(path) != document:
            raise ValueError('Immutable successful-code admission differs')
    else:
        atomic_json(path, document)


def admit_operation(work, operation_ref, *, apply=False, current_preparation_ref=None):
    """No network or ledger writes; the CURRENT pointer changes only after proof.

    Every member retains its original operation, SUCCESS key and request/body.
    The derived historical-code observation does not assert an identity or stop.
    """
    work=Path(work).resolve()
    if (work/'private/network_worker.lock').exists():
        raise ValueError('Existing writer has not reached its safe point')
    prepared=None
    if current_preparation_ref is not None:
        from stage1d_current_code_preparation import validate
        prepared, current_rows, current_scopes, _ = validate(work,current_preparation_ref)
    registry=Registry(work)
    operation=registry.read(operation_ref)
    current_path=work/CURRENT_PATH
    previous_sha=sha(current_path)
    current=read(current_path)
    snapshot=registry.read(current['initial_snapshot_ref'])
    scopes={q['query']['query_id']:Scope.from_policy(q['query']) for q in snapshot['query_snapshots']}
    state_rows=current_rows if prepared is not None else snapshot['state_screen_rows']
    if prepared is not None:scopes=current_scopes
    admitted=[];gaps=[];affected=[]
    new_current=deepcopy(current)
    for batch in operation.get('results',[]):
        needs=batch['needs'];members=batch['result']['members']
        if len(needs)!=len(members):raise ValueError('Operation member count differs')
        for need,saved in zip(needs,members):
            plan=need['request']
            if plan['method']!='eth_getCode':continue
            key=logical_key(Runtime().rpc_identity(PROVIDER,plan))
            if key!=need['logical_key']:raise ValueError('Operation request identity changed')
            member=cached_member(work,plan)
            if member is None:
                gaps.append({'logical_key':key,'reason':'NO_EXISTING_SUCCESS','new_requests':0});continue
            if (saved.get('status')!='SUCCESS_VALIDATED' or saved['artifact_sha256']!=member['artifact_sha256']
                    or saved['artifact_path']!=member['artifact_path']):
                raise ValueError('Operation does not bind the exact successful member')
            ref={'path':member['artifact_path'],'sha256':member['artifact_sha256']}
            request,response=registry._rpc(ref)
            if {k:request[k] for k in ('method','params')}!=plan:
                raise ValueError('Successful member selector differs')
            address,block=plan['params'][0],int(plan['params'][1],16)
            states=[r for r in state_rows if r['state']['address']==address
                    and r['state']['arrival']['block']==block]
            if not states:
                gaps.append({'logical_key':key,'reason':'NO_BOUND_CURRENT_ARRIVAL' if prepared is not None else 'NO_BOUND_INITIAL_ARRIVAL','new_requests':0});continue
            hashes={r['state']['arrival'].get('block_hash') for r in states if r['state']['arrival'].get('block_hash')}
            if len(hashes)>1:raise ValueError('Initial arrival block conflict')
            header_ref=None
            if prepared is not None or not hashes or any(not r['state']['arrival'].get('block_hash') for r in states):
                hp={'method':'eth_getBlockByNumber','params':[hex(block),False]}
                hm=cached_member(work,hp)
                if hm is None:
                    gaps.append({'logical_key':key,'reason':'EXACT_HEADER_BINDING_MISSING','request':hp,'new_requests':0});continue
                header_ref={'path':hm['artifact_path'],'sha256':hm['artifact_sha256']}
                _,hr=registry._rpc(header_ref);hashes.add(hr['result']['hash'])
            if len(hashes)!=1:raise ValueError('Exact header and arrival disagree')
            env=registry.read(ref)
            body=registry._path(ref['path']).parent/'response_body.bin'
            descriptor={'schema_version':CODE_SCHEMA,'chain_id':'eip155:1','address':address,
                'arrival_block_hash':next(iter(hashes)),'request_ref':ref,'response_ref':ref,
                'original_body_ref':registry._ref(body),'same_block_conflict':False}
            if header_ref is not None:descriptor['header_ref']=header_ref
            if prepared is not None:
                from stage1d_current_code_preparation import digest
                filename=key+'_'+digest(descriptor)+'.json'
            else:filename=key+'.json'
            target=work/'private/stage1d_roles/code_cache_admission'/filename
            _immutable(target,descriptor);dref=registry._ref(target)
            existing=[v for v in new_current['historical_codes'] if v==dref]
            if not existing:
                registry._codes.setdefault(address,[]).append((dref,descriptor))
                new_current['historical_codes'].append(dref)
            for row in states:
                state=row['state'];scope=scopes[state['query_id']]
                observation=registry._code(state,scope)
                if observation is None:raise ValueError('Descriptor did not reach the real cost consumer')
                affected.append({'query_name':row['query_name'],'state_key':state_key(state,scope),
                                 'code_observation':observation.to_dict()})
            admitted.append({'logical_key':key,'descriptor':dref,'code_result':response['result'],
                'new_descriptor':not bool(existing),'same_success_requeried':False})
    if sha(current_path)!=previous_sha:raise ValueError('Concurrent Registry writer detected')
    if apply and new_current!=current:
        backup=work/'private/stage1d_roles/catalogues'/f'{previous_sha}.json'
        backup.parent.mkdir(parents=True,exist_ok=True)
        if backup.exists() and sha(backup)!=previous_sha:raise ValueError('Prior catalog backup differs')
        if not backup.exists():backup.write_bytes(current_path.read_bytes())
        atomic_json(current_path,new_current)
    return {'status':'ADOPTED_REPLAY_REQUIRED' if apply else 'VALIDATED_NOT_ADOPTED',
        'original_operation':operation_ref,'current_preparation':current_preparation_ref,'previous_catalog_sha256':previous_sha,
        'current_catalog_sha256':sha(current_path),'admitted':admitted,'gaps':gaps,
        'affected_states':affected,'affected_queries':sorted({r['query_name'] for r in affected}),
        'identity_or_cost_stop_not_inferred_from_empty_code':True,'new_requests':0,
        'budget_attempts_and_online_sessions_modified':False}
