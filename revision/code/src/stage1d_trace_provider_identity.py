"""Reconcile complete provider trees with different nonvalue precompile paths.

No amount-based fuzzy matching is permitted. Both complete trees must have the
same ordered rooted structure after removing successful, childless, zero-value
CALL/STATICCALL frames at the two observed precompile addresses (1 and 4).
Their full original representations and the ordered bijection remain evidence.
The selected Dune representation retains even these nonvalue frames.
"""
import copy,hashlib,json
from stage1d_raw_fields import exact_uint,exact_trace_path as trace_path,validate_raw_member

SCHEMA='stage1d-complete-provider-trace-identity-v1'

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def _tree(rows,tx):
    tree={}
    for original in rows:
        if original.get('record_type')!='trace' or original.get('tx_hash')!=tx:continue
        validate_raw_member(original,physical=True)
        row=copy.deepcopy(original);path=tuple(trace_path(row['trace_address']))
        if path in tree:raise ValueError('Duplicate provider trace path')
        for field in ('block_number','tx_index','subtraces'):
            row[field]=exact_uint(row.get(field))
        if row.get('value_raw') is not None:row['value_raw']=str(exact_uint(row['value_raw']))
        if not row.get('evidence_ids'):raise ValueError('Provider trace lacks original evidence')
        tree[path]=row
    if () not in tree:raise ValueError('Complete provider root missing')
    for path,row in tree.items():
        children=sorted(p for p in tree if p and p[:-1]==path)
        if path and path[:-1] not in tree:raise ValueError('Provider trace ancestor missing')
        if [p[-1] for p in children]!=list(range(row['subtraces'])):
            raise ValueError('Provider full tree count/order is incomplete')
        if type(row.get('success')) is not bool or type(row.get('tx_success')) is not bool:
            raise ValueError('Provider success evidence unresolved')
    return tree

def _nonvalue_leaf(row):
    return (row.get('call_type') in ('call','staticcall')
            and row.get('to_address') in ('0x'+'0'*39+'1','0x'+'0'*39+'4')
            and (row.get('value_raw')=='0' or row.get('call_type')=='staticcall' and row.get('value_raw') is None)
            and row['subtraces']==0 and row['success'] is True and row['tx_success'] is True
            and not row.get('error'))

def _signature(row):
    result={k:row.get(k) for k in ('block_number','block_hash','tx_index','from_address','to_address',
        'value_raw','call_type','trace_type','success','tx_success','error','created_address','refund_address')}
    if result['call_type']=='staticcall' and result['value_raw'] in (None,'0'):
        result['value_raw']='NONVALUE_STATICCALL_ORIGINAL_VALUE_RETAINED'
    return result

def reconcile(material,supplement):
    """Return a derived material and independently recomputed full-tree proof."""
    if supplement.get('provider')!='DUNE' or supplement.get('coverage')!=[]:
        raise ValueError('Only complete Dune point trees, with no range coverage, are accepted')
    targets=supplement.get('complete_transaction_trees',[])
    if not targets or len(set(targets))!=len(targets):raise ValueError('Exact distinct tree identities required')
    if any(r.get('tx_hash') not in targets for r in supplement['events']):raise ValueError('Unrequested supplement transaction')
    original=material['events'];replacements=[];proofs=[]
    for tx in targets:
        left=_tree(original,tx);right=_tree(supplement['events'],tx);pairs=[]
        def walk(a,b):
            if _signature(left[a])!=_signature(right[b]):
                raise ValueError('Provider tree physical facts disagree at '+tx+':'+str((a,b)))
            pairs.append((a,b))
            ac=sorted(p for p in left if p and p[:-1]==a and not _nonvalue_leaf(left[p]))
            bc=sorted(p for p in right if p and p[:-1]==b and not _nonvalue_leaf(right[p]))
            if len(ac)!=len(bc):raise ValueError('Provider ordered tree structures disagree')
            for x,y in zip(ac,bc):walk(x,y)
        walk((),())
        if len(pairs)!=sum(not _nonvalue_leaf(r) for p,r in left.items()) or len(pairs)!=sum(not _nonvalue_leaf(r) for p,r in right.items()):
            raise ValueError('Incomplete provider tree bijection')
        proof={'schema_version':SCHEMA,'tx_hash':tx,'status':'ORDERED_COMPLETE_TREE_EQUIVALENT_MODULO_NONVALUE_PRECOMPILE_LEAVES',
            'left_original_tree_sha256':digest([original for original in material['events'] if original.get('record_type')=='trace' and original.get('tx_hash')==tx]),
            'right_original_tree_sha256':digest([original for original in supplement['events'] if original.get('record_type')=='trace' and original.get('tx_hash')==tx]),
            'left_to_right_paths':[{'left':list(a),'right':list(b)} for a,b in pairs],
            'left_nonvalue_leaves':[list(p) for p,r in left.items() if _nonvalue_leaf(r)],
            'right_nonvalue_leaves':[list(p) for p,r in right.items() if _nonvalue_leaf(r)],
            'selected_representation':'DUNE_FULL_ORIGINAL_TREE','all_positive_value_calls_preserved':True,
            'source_variables_merged':False,'new_account_coverage':False}
        proof_id='TRACE_PROVIDER_EQUIVALENCE:'+digest(proof);reverse={b:a for a,b in pairs}
        for row in supplement['events']:
            if row.get('record_type')!='trace' or row.get('tx_hash')!=tx:continue
            selected=copy.deepcopy(row);path=tuple(trace_path(row['trace_address']))
            ids=selected['evidence_ids']+[proof_id]
            if path in reverse:ids+=left[reverse[path]]['evidence_ids']
            selected['evidence_ids']=sorted(set(ids));replacements.append(selected)
        proofs.append(proof)
    result=copy.deepcopy(material)
    result['events']=[r for r in result['events'] if not(r.get('record_type')=='trace' and r.get('tx_hash') in targets)]+replacements
    result['trace_provider_identity_proofs']=copy.deepcopy(material.get('trace_provider_identity_proofs',[]))+proofs
    return result,proofs
