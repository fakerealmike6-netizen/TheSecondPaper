"""Verified paid context ranges supply only their proven native discovery part.

No provider calls or global-completeness inference. Each physical source family
is checked once per caller-owned proof memo; unrelated unbound BQ rows cannot
force an already proved current interval to be downloaded again.
"""
from collections import defaultdict
from pathlib import Path
from copy import deepcopy
from collector import Scope,NATIVE,Event
import stage1d_bq_context_prepare as h
import stage1d_current_context_paid_bq as paid
import stage1d_batch_binding_route as batch
from stage1d_closure_context import requirements
from stage1d_cost_request_guard import _rectangle,_allowed,validate_candidate_entry
from stage1d_cost_boundary_context import validate_overlay
from stage1d_window import missing_rectangles

VERSION='stage1d-paid-context-native-projection-v1'
FIELDS=('start_block','end_block','start_time','end_time')

def _intersect(left,right):
    return paid.union((max(a,c),min(b,d)) for a,b in left for c,d in right if a<=d and c<=b)

def project(query,collection,admission,needs):
    """Pure intersection after verified admission; retain full necessary families."""
    scope=Scope.from_policy(query)
    policy=collection.get('cost_boundary_policy')
    if policy:
        validate_overlay(query,collection,expected_policy_sha256=policy['policy_sha256'])
        allowed,_,_=_allowed(collection,scope)
    else:allowed=None
    families=defaultdict(list);by_address=defaultdict(set)
    for row in admission['rows']:
        tx=row.get('tx_hash')
        if not tx:continue
        families[tx].append(row)
        for address in (row.get('from_address'),row.get('to_address'),row.get('created_address'),row.get('refund_address')):
            if address:by_address[address].add((int(row['block_number']),tx))
    records=[];used_rows={};used_events={};failures=[]
    for original in needs:
        need=_rectangle(original,query,scope)
        if need['asset']!=NATIVE:continue
        if allowed is not None and missing_rectangles(need,allowed.get((need['address'],NATIVE),())):
            raise ValueError('Paid projection exceeds frozen lawful CONTINUE/BYPASS arrivals')
        blocks=[[need['start_block'],need['end_block']]]
        for kind in paid.KINDS:
            intervals=[(r['start_block'],r['end_block']) for r in admission['coverage']
                if r.get('address')==need['address'] and r.get('data_type')==kind
                and r.get('status')=='COMPLETE' and r.get('pagination_complete') is True
                and r.get('evidence_ids') and r.get('date_domain_verified') is True
                and r.get('block_domain_verified') is True and r.get('source_bound_kind_validation') is True]
            blocks=_intersect(blocks,intervals)
        for lo,hi in blocks:
            ids=sorted(tx for b,tx in by_address.get(need['address'],()) if lo<=b<=hi)
            rows=[r for tx in ids for r in families[tx]]
            validated=batch.validate_transaction_rows(rows)
            events,event_gaps=batch._events(rows,validated)
            kinds,details=paid._classify(rows,dict(address=need['address'],start_block=lo,end_block=hi))
            gaps=validated['gaps']+event_gaps
            if not all(kinds.values()):gaps.append(dict(reason='PAID_PROJECTED_KIND_RECHECK_FAILED',details=details))
            if gaps:
                failures.append(dict(need=need,start_block=lo,end_block=hi,gaps=gaps));continue
            selected=[]
            for event in events:
                key=event['event_id']
                if key in used_events and used_events[key]!=event:raise ValueError('Projected physical event conflict')
                used_events[key]=event;selected.append(key)
            for tx in ids:used_rows[tx]=families[tx]
            records.append(dict(need, start_block=lo,end_block=hi,event_ids=sorted(selected),
                full_family_ids=ids,complete=True,normalization_gaps=[],native_gas_operands_verified=True))
    # Multiple legal arrivals may ask the same exact rectangle. This is physical
    # source reuse, not merging source variables or refreshing unreachable states.
    records=list({h.digest(r):r for r in records}.values())
    return dict(records=records,events=sorted(used_events.values(),key=lambda e:Event(**e).stable_key()),
        rows=[r for tx in sorted(used_rows) for r in used_rows[tx]],failures=failures,
        full_context_claimed=False,source_zero_claimed=False,all_asset_export_complete=False)

def replay_admission(work,document):
    """Check immutable original source pages and exact recorded point members."""
    query=document['query'];inputs=document['inputs']
    frozen=h.read(h.checked(work,inputs['freeze']))
    if [q for q in frozen['queries'] if q['query_id']==query['query_id']]!=[query]:
        raise ValueError('Paid projection query differs from its exact frozen version')
    collection=h.read(h.checked(work,inputs['collection']));labels=h.read(h.checked(work,inputs['labels']))
    saved=h.read(h.checked(work,document['admission']))
    if saved['query_id']!=query['query_id'] or saved['scope_hash']!=query['scope_hash']:
        raise ValueError('Paid projection query/scope differs')
    if any(saved['current_input_refs'][k]['sha256']!=inputs[k]['sha256'] for k in ('collection','labels','freeze')):
        raise ValueError('Paid projection input versions differ')
    demand=requirements(query,collection,labels)
    if h.digest(demand['context_plan'])!=h.digest(saved['context_plan']):
        raise ValueError('Paid projection context requirements changed')
    inventory=h.read(h.checked(work,document['source_inventory']))['sources']
    sources=[];seen=set()
    for item in inventory:
        key=h.digest(dict(preparation_ref=item['preparation_ref'],job_states=item['job_states']))
        if key not in seen:sources.append(paid._load_source(work,item));seen.add(key)
    points=paid._Points(work,saved['used_point_bindings'],allow_paid_top_fields=True)
    actual=paid._admit_plan(Scope.from_policy(query),demand['context_plan'],sources,points)
    for key in ('rows','coverage','binding_gaps','sources','point_binding_requests'):
        if h.digest(actual[key])!=h.digest(saved[key]):raise ValueError('Paid source replay differs: '+key)
    return query,collection,actual

def install(work,query,entry,admission_path,source_inventory,output):
    """Root invokes at a safe point with the current actual request guard."""
    work=Path(work).resolve()
    if (work/'private/network_worker.lock').exists():raise ValueError('Existing writer must finish normally')
    guard=validate_candidate_entry(work,query,entry)
    admission=h.read(h.inside(work,admission_path));out=h.inside(work,output)
    if out.exists():raise ValueError('Use a new immutable paid projection revision')
    out.mkdir(parents=True)
    document=dict(version=VERSION,query=deepcopy(query),inputs=admission['current_input_refs'],
        admission=h.dep(work,admission_path),source_inventory=h.dep(work,source_inventory),
        needs=deepcopy(entry['needed_ranges']),original_guard=guard)
    if admission['current_input_refs']['collection']['sha256']!=entry['collection_sha256']:
        raise ValueError('Paid projection is stale for the current candidate graph')
    q,col,replayed=replay_admission(work,document)
    result=project(q,col,replayed,document['needs']);document['result_sha256']=h.digest(result)
    h.save(out/'PROOF.json',document);h.save(out/'EVENTS.json',result['events']);h.save(out/'PROJECTED_ROWS.json',result['rows'])
    h.save(out/'RESULT.json',result)
    proof_ref=h.dep(work,out/'PROOF.json');event_ref=h.dep(work,out/'EVENTS.json');records=[]
    for segment in result['records']:
        record=dict(acquisition_adapter=VERSION,provider='CLASSIC_BIGQUERY',evidence_id=proof_ref['sha256'],
            addresses=[segment['address']],asset=NATIVE,**{k:segment[k] for k in FIELDS},
            complete=True,native_scope_complete=True,coverage_capability='NATIVE_INDEX_ONLY',
            all_asset_export_complete=False,context_complete=False,normalization_gaps=[],
            paid_projection_path=proof_ref['path'],paid_projection_sha256=proof_ref['sha256'],
            events_path=event_ref['path'],events_sha256=event_ref['sha256'],
            query_id_at_acquisition=query['query_id'],scope_id_at_acquisition=query['scope_id'],scope_hash_at_acquisition=query['scope_hash'],
            coverage_basis='CURRENT_LAWFUL_NEED_INTERSECT_VERIFIED_PAID_TOP_INTERNAL_FEE_RANGES')
        target=work/'derived/stage1d/intervals'/('paid_'+h.digest(record)+'.coverage.json')
        h.save(target,record);records.append(h.dep(work,target))
    receipt=dict(status='CURRENT_PAID_NATIVE_INTERVALS_INSTALLED',proof=proof_ref,coverage_records=records,
        events=len(result['events']),rows=len(result['rows']),unadopted_segments=result['failures'],
        new_external_requests=0,full_context_claimed=False,source_zero_claimed=False)
    h.save(out/'RECEIPT.json',receipt);return receipt

def verify_interval_record(work,record,*,memo=None):
    work=Path(work).resolve();memo={} if memo is None else memo
    if (record.get('acquisition_adapter')!=VERSION or record.get('asset')!=NATIVE
        or record.get('provider')!='CLASSIC_BIGQUERY'
        or record.get('coverage_basis')!='CURRENT_LAWFUL_NEED_INTERSECT_VERIFIED_PAID_TOP_INTERNAL_FEE_RANGES'
        or record.get('coverage_capability')!='NATIVE_INDEX_ONLY' or record.get('complete') is not True
        or record.get('native_scope_complete') is not True or record.get('all_asset_export_complete') is not False
        or record.get('context_complete') is not False or record.get('normalization_gaps')!=[]):
        raise ValueError('Exact native-only paid projection required')
    ref=dict(path=record['paid_projection_path'],sha256=record['paid_projection_sha256'])
    key=('PAID_NATIVE_PROJECTION',str(work),ref['path'],ref['sha256'])
    if key not in memo:
        doc=h.read(h.checked(work,ref))
        if doc['version']!=VERSION:raise ValueError('Paid projection version differs')
        query,collection,admitted=replay_admission(work,doc)
        result=project(query,collection,admitted,doc['needs'])
        if h.digest(result)!=doc['result_sha256']:raise ValueError('Paid candidate projection does not regenerate')
        ep=h.checked(work,dict(path=record['events_path'],sha256=record['events_sha256']))
        if h.read(ep)!=result['events']:raise ValueError('Paid candidate event bytes differ')
        memo[key]=(doc,result,record['events_path'],record['events_sha256'])
    doc,result,path,digest=memo[key]
    if (record['evidence_id']!=ref['sha256'] or record['events_path']!=path or record['events_sha256']!=digest
        or any(record.get(k+'_at_acquisition')!=doc['query'][k] for k in ('query_id','scope_id','scope_hash'))
        or not any(record['addresses']==[r['address']] and all(record[k]==r[k] for k in FIELDS) for r in result['records'])):
        raise ValueError('Paid coverage exceeds the exact verified native intersection')
    return result
