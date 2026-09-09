"""Bounded current native-material inventory; no DB/provider/model/solver writes."""
from pathlib import Path
from collections import Counter
import hashlib,json,sys,time
from unittest.mock import patch

work=Path(sys.argv[1]).resolve();out=Path(__file__).resolve().parent
sys.path.insert(0,str(work/'src'))
from stage1d_closure_context import validate_current,requirements,_project
from stage1d_closure_scope import active_batch
from stage1d_context import necessary_context_windows,_event,_native_rows,_headers,_unwrap,_hash
from context_ledger_r3 import normalize_rows,coverage_complete,integer
from stage1d_final_context_prepare import gaps_for_kinds,merge_address_ranges
from stage1d_multiasset_context import asset,ETH
sources={}
def read(path):
    path=Path(path);data=path.read_bytes();sources[path.relative_to(work).as_posix()]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
    return json.loads(data)
def saved(name,value):(out/name).write_text(json.dumps(value,indent=2,ensure_ascii=False,default=str)+'\n',encoding='utf-8')
def vals(value):return list(value.values()) if isinstance(value,dict) else value

def run():
    wall,cpu=time.perf_counter(),time.process_time()
    queries={q['name']:q for q in active_batch(work)['queries']}
    materials=[];native=[];coverage=[];headers=[];balances=[];receipts=[];providers=[]
    for name in ('txphish_src002','txphish_src001'):
        root=work/'derived/stage1d/queries'/name/'context'
        folder=root/'round_500_all_01'
        files={key:folder/filename for key,filename in [('events','ledger_rows.json'),('coverage','coverage.json'),('headers','headers.json'),('balances','balances.json'),('receipts','receipts.json')]}
        part={k:read(p) for k,p in files.items()};native+=part['events'];coverage+=part['coverage'];headers+=vals(part['headers']);balances+=vals(part['balances']);receipts+=vals(part['receipts'])
        for key,target in [('headers',headers),('balances',balances),('receipts',receipts)]:
            if (root/(key+'.json')).exists():target+=vals(read(root/(key+'.json')))
        oldplan=read(folder/'context_plan.json');acq=read(folder/'acquisition.json');jobkey=Path(acq['job_folder']).name
        job=read(work/'private/dune_r2_jobs'/jobkey/'job.json')
        proof={'job_key':jobkey,'execution_id':job.get('execution_id'),'export_status':job.get('export_status'),
               'sql_sha256':job.get('sql_sha256'),'verified_pages':[],'note':'Original inherited context job, no new execution or present-scope FULL inference.'}
        sql=work/'private/stage1d_sql'/jobkey/'query.sql'
        if sql.exists():
            data=sql.read_bytes();sources[sql.relative_to(work).as_posix()]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
            proof['original_sql_sha_matches_job']=hashlib.sha256(data).hexdigest()==job.get('sql_sha256')
        for item in job.get('r4_verified_pages',{}).values():
            checked={}
            for kind in ('page','receipt'):
                p=work/item[kind+'_path'];data=p.read_bytes();digest=hashlib.sha256(data).hexdigest();sources[p.relative_to(work).as_posix()]={'sha256':digest,'bytes':len(data)}
                checked[kind+'_sha_matches']=digest==item[kind+'_sha256']
            proof['verified_pages'].append(checked)
        providers.append(proof)
        materials.append({'query':name,'files':{k:p.relative_to(work).as_posix() for k,p in files.items()},
            'native_rows':len(part['events']),'native_transactions':len({_event(r).get('tx_hash') for r in part['events'] if _event(r).get('tx_hash')}),
            'raw_record_types':dict(Counter(r.get('record_type') for r in part['events'])),
            'coverage_rows':len(part['coverage']),'coverage_accounts':sorted({r['address'] for r in part['coverage']}),
            'old_plan_accounts':len(oldplan['rows']),'original_scope_is_not_current_coverage_proof':True})
    minimal=work/'private/stage1d_bq_recovery_minimal/root_binding/root_equivalence.json'
    bq=None
    if minimal.exists():
        bq=read(minimal)
        if bq.get('status')=='ROOT_EQUIVALENT_BOUND' and bq.get('full_context_claimed') is False and bq.get('covered_ranges')==0:
            native+=bq['rows']+[bq['current_top_row']]
        else:raise ValueError('Tiny known BQ family changed; no inferred binding')
    # Exact raw repeats are deduplicated only by whole-row canonical content.
    native=list({_hash(r):r for r in native}.values())
    material={'events':native,'coverage':coverage,'headers':headers,'balances':balances,'receipts':receipts}
    header_index=_headers(headers)
    balance_index={_hash({k:v['request'][k] for k in ('method','params')}) for v in balances if v.get('request') and v.get('response',{}).get('result') is not None}
    receipt_index=set()
    for v in receipts:
        r=_unwrap(v);r=r.get('result',r)
        if isinstance(r,dict) and r.get('transactionHash'):receipt_index.add(r['transactionHash'].lower())
    report={'status':'CURRENT_DYNAMIC_SNAPSHOT_NOT_FINAL','queries':{},'materials':materials,'provider_metadata_verification':providers,
        'known_points_only_not_global_cache_absence':True,'global_rpc_cache_examined':False,'new_network_requests':0,'solver_calls':0,'registration_calls':0,
        'bq_minimal':None if bq is None else {'path':minimal.relative_to(work).as_posix(),'rows':len(bq['rows']),'top_rows':1,'covered_ranges':0,'full_context_claimed':False},
        'qualification':'Ledger difference uses strict existing coverage metadata for these named files only. It is not a model FULL certificate or a command to fetch all missing RPC selectors.'}
    pair_missing=[]
    for name in ('txphish_src002','txphish_src001'):
        folder=work/'derived/stage1d/queries'/name
        collection=read(folder/'collection.json');labels=read(folder/'label_snapshot.json')
        binding=validate_current(queries[name],collection,labels)
        plan=necessary_context_windows(queries[name],collection,(),labels)
        nativeplan={**plan,'rows':[r for r in plan['rows'] if asset(r['asset'])==ETH]}
        selected=_project(native,nativeplan)
        per_account=[]
        for r in nativeplan['rows']:
            missing={kind:gaps_for_kinds(r,coverage,(kind,)) for kind in r['required_coverage']}
            complete,detail=coverage_complete(coverage,r['address'],r['ledger_start_block'],r['ledger_end_block'],r['required_coverage'])
            per_account.append({'account_id':r['account_id'],'address':r['address'],'start_block':r['ledger_start_block'],
                'end_block':r['ledger_end_block'],'before_anchor_block':r['before_anchor_block'],'after_anchor_block':r['after_anchor_block'],
                'existing_material_coverage_complete':complete,'coverage_by_type':detail,'missing_by_type':missing})
            for lo,hi in gaps_for_kinds(r,coverage,tuple(r['required_coverage'])):pair_missing.append({'address':r['address'],'start_block':lo,'end_block':hi})
        raw,_,excluded,treechecks,treeconflicts=_native_rows(selected)
        normalized=normalize_rows(raw)
        needed=None;need_error=None
        try:needed=requirements(queries[name],collection,labels,material)
        except Exception as exc:need_error=type(exc).__name__+': '+str(exc)
        point_detail=[]
        point_scope='FULL_CURRENT_REQUIREMENTS' if needed else 'NATIVE_ANCHORS_ONLY_REQUIREMENTS_BLOCKED'
        requests=needed['point_requests'] if needed else list({_hash(r):r for row in nativeplan['rows'] for b in (row['before_anchor_block'],row['after_anchor_block']) for r in ({'method':'eth_getBalance','params':[row['address'],hex(b)]},{'method':'eth_getBlockByNumber','params':[hex(b),False]})}.values())
        if requests:
            for request in requests:
                method,params=request['method'],request['params']
                present=(_hash(request) in balance_index if method in ('eth_getBalance','eth_call') else
                         integer(params[0]) in header_index if method=='eth_getBlockByNumber' else
                         params[0].lower() in receipt_index if method=='eth_getTransactionReceipt' else
                         params[0].lower() in {r['tx_hash'] for r in normalized['transactions']})
                point_detail.append({'request':request,'present_in_named_context_material':present})
        result={'binding':binding,'candidate_status':collection.get('status'),'candidate_events':len(collection.get('candidate_events',[])),
            'context_events':len(collection.get('context_events',[])),'semantic_units':len(collection.get('semantic_units',[])),
            'modeled_native_accounts':len(nativeplan['rows']),'current_native_plan':nativeplan,
            'fully_covered_accounts_in_named_material':sum(r['existing_material_coverage_complete'] for r in per_account),
            'accounts_with_some_coverage':sum(any(any(x.get('address')==r['address'] and x['start_block']<=r['end_block'] and x['end_block']>=r['start_block'] for x in coverage) for _ in [0]) for r in per_account),
            'accounts':per_account,'selected_original_family_rows':len(selected),'selected_physical_txs':len({_event(r).get('tx_hash') for r in selected if _event(r).get('tx_hash')}),
            'native_normalizer_conflicts':normalized['conflicts']+treeconflicts,'native_exclusions_by_reason':dict(Counter(x['reason'] for x in excluded+normalized['excluded'])),
            'normalized_top_transactions':len(normalized['transactions']),'normalized_effective_flows':len(normalized['flows']),
            'top_fees_present':sum(r.get('fee_raw') is not None for r in normalized['transactions']),
            'requirements_error':need_error,'point_map_scope':point_scope,
            'context_asset_counts':dict(Counter(r.get('asset') for r in collection.get('context_events',[]))),
            'point_counts_by_method':dict(Counter(r['request']['method'] for r in point_detail)),
            'missing_in_named_material_by_method':dict(Counter(r['request']['method'] for r in point_detail if not r['present_in_named_context_material'])),
            'point_need_map_file':name+'_POINT_NEEDS.json','ledger_need_map_file':name+'_LEDGER_NEEDS.json',
            'old_models_not_reused':True,'source_or_candidate_change_requires_recompute':True}
        report['queries'][name]=result
        saved(name+'_POINT_NEEDS.json',{'query':name,'binding':binding,'scope':'Named context files only; exact current RPC cache lookup still required before any request. '+point_scope,'requests':point_detail})
        saved(name+'_LEDGER_NEEDS.json',{'query':name,'binding':binding,'accounts':per_account})
        print(json.dumps({'query':name,'accounts':result['modeled_native_accounts'],'fully_covered':result['fully_covered_accounts_in_named_material'],
            'some_coverage':result['accounts_with_some_coverage'],'selected_rows':len(selected),'normalization_conflicts':len(result['native_normalizer_conflicts']),
            'requirements_error':need_error,'known_material_missing_points':result['missing_in_named_material_by_method']}),flush=True)
    report['shared_missing_native_ranges']=merge_address_ranges(pair_missing)
    report['shared_missing_native_addresses']=len({r['address'] for r in report['shared_missing_native_ranges']})
    report['shared_clock']='SHARED existing Runtime policy charges all applicable query clocks; no new clock or SQL job here'
    report['input_sources']=sources
    report['inputs_unchanged_at_end']=all(hashlib.sha256((work/p).read_bytes()).hexdigest()==v['sha256'] for p,v in sources.items())
    report['source_sha256']={n:hashlib.sha256((work/'src'/n).read_bytes()).hexdigest() for n in ['stage1d_closure_context.py','stage1d_context.py','stage1d_final_context_prepare.py','context_ledger_r3.py']}
    report['wall_seconds']=time.perf_counter()-wall;report['cpu_seconds']=time.process_time()-cpu
    saved('INVENTORY.json',report)
    saved('SHARED_NATIVE_LEDGER_NEEDS.json',{'status':report['status'],'source_bindings':{n:v['binding'] for n,v in report['queries'].items()},
        'ranges':report['shared_missing_native_ranges'],'accounts':report['shared_missing_native_addresses'],'not_a_frozen_sql_or_actual_dryrun':True})
    print(json.dumps({'status':report['status'],'shared_missing_addresses':report['shared_missing_native_addresses'],'inputs_unchanged':report['inputs_unchanged_at_end'],'wall_seconds':report['wall_seconds']}))

if __name__=='__main__':
    with patch('socket.create_connection',side_effect=AssertionError('offline inventory')),patch('socket.socket.connect',side_effect=AssertionError('offline inventory')):run()
