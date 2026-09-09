"""One-pass, read-only current BQ gap triage. No normalization/certification."""
from pathlib import Path
import collections,json,hashlib,sqlite3,sys,time,datetime,gzip
R=Path(__file__).resolve().parents[2];C=R/'code';OUT=Path(__file__).resolve().parent
P=C/'private/stage1d_batch_binding/proofs/c14c1cae359a08fe7c9ee4dca576b628a8181cf08e8636ded2c35862fd33d965'
WETH='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2';NATIVE='native:eip155:1'
start=time.perf_counter();cpu=time.process_time();inputs=[]
def read(path,hash_once=False):
    before=path.stat();raw=path.read_bytes();data=json.loads(raw);after=path.stat()
    inputs.append({'path':path.relative_to(R).as_posix(),'bytes':len(raw),'mtime_ns':after.st_mtime_ns,'read_once':True,'stable_during_read':(before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns),'sha256':hashlib.sha256(raw).hexdigest() if hash_once else None})
    return data
def number(v):return int(v,16) if isinstance(v,str) and v.startswith('0x') else int(v)
def stamp(v):return int(datetime.datetime.fromisoformat(v.replace('Z','+00:00')).timestamp())
def save(name,value):(OUT/name).write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')

proof=read(P/'proof.json');shape={'proof_keys':list(proof),'result_keys':list(proof.get('result',{}))}
result=proof.get('result',proof);gaps=result.get('gaps',[])
if not gaps:gaps=proof.get('gaps',[])
rootgaps={g['tx_hash']:g for g in gaps if g.get('reason',g.get('type'))=='ROOT_EQUIVALENCE_UNRESOLVED_GAP'}
shape['gap_reasons']=dict(collections.Counter(g.get('reason',g.get('type')) for g in gaps));shape['root_gap_count']=len(rootgaps)
shape['preparation']=proof.get('preparation');shape['sample_gap']=next(iter(rootgaps.values()),None)
save('INPUT_SHAPES.json',shape)
if not rootgaps:raise ValueError('Root gap shape differed; stop before reading ledger again')
del proof,result,gaps
batch=read(C/'private/STAGE1D_CLOSURE_BATCH.json',True);queries={q['name']:q for q in batch['queries']}
querydata={}
for qname in ('txphish_src001','txphish_src002'):
    path=C/'derived/stage1d/queries'/qname/'collection.json';co=read(path,True);collection_ref=inputs[-1].copy()
    pending=collections.defaultdict(list)
    for row in co.get('unresolved_frontier',[]):
        s=row.get('state',{})
        if s.get('asset')==NATIVE and s.get('depth',999)<queries[qname]['max_acquisition_depth']:pending[s['address']].append(s)
    known={e['tx_hash'] for e in co['candidate_events']};weth={e['tx_hash'] for e in co['candidate_events'] if e.get('recipient')==WETH and e.get('asset')==NATIVE and e.get('success') is True and number(e.get('amount_raw',0))>0}
    context=collections.defaultdict(list);plan_path=C/'private/stage1d_current_context_plans'/qname/(collection_ref['sha256']+'.json')
    context_status='NO_EXACT_CURRENT_COLLECTION_PLAN_FOUND';plan_keys=[]
    if plan_path.is_file():
        plan=read(plan_path,True);plan_keys=list(plan)
        for row in plan.get('rows',plan.get('plan',{}).get('rows',[])):
            if row.get('asset',NATIVE) in (NATIVE,'ETH'):context[row['address']].append(row)
        context_status='CURRENT_PLAN_ROWS_AVAILABLE' if context else 'CURRENT_PLAN_SHAPE_NOT_INTERPRETED'
    querydata[qname]={'pending':pending,'known':known,'weth':weth,'context':context,'context_status':context_status,'context_plan_keys':plan_keys,'collection':collection_ref,
        'scope_hash':co.get('metrics',{}).get('scope_hash'),'states':len(co.get('states',[])),'pending_count':sum(map(len,pending.values()))}
    del co
rows=read(P/'ledger_rows.json');shape['ledger_container']=type(rows).__name__
if isinstance(rows,dict):shape['ledger_keys']=list(rows);rows=rows.get('rows',rows.get('ledger_rows',[]))
if not isinstance(rows,list) or not rows:raise ValueError('Ledger row container differs')
shape['ledger_row_count']=len(rows);shape['row_fields']=sorted(set().union(*(r.keys() for r in rows[:20])))
facts={tx:{'tx_hash':tx,'row_count':0,'trace_count':0,'null_trace_count':0,'explicit_root_count':0,'tops':[],'positive_rows':0,'queries':{q:{'pending_outgoing':False,'pending_touched':False,'strict_order_unknown':False,'context_touched':False,'samples':[]} for q in querydata}} for tx in rootgaps}
field_presence=collections.Counter();top_fields=collections.Counter()
for row in rows:
    tx=row.get('tx_hash')
    if tx not in facts:continue
    f=facts[tx];f['row_count']+=1;f['block']=number(row['block_number']);f['block_hash']=row.get('block_hash');f['tx_index']=row.get('tx_index');f['block_time']=row.get('block_time')
    typ=row.get('record_type');path=row.get('trace_address');success=row.get('success');value=number(row.get('value_raw') or 0);positive=success is True and value>0
    if typ=='trace':
        f['trace_count']+=1;f['null_trace_count']+=path is None;f['explicit_root_count']+=path in ('[]',[])
        if path is None:f['null_root']={k:row.get(k) for k in ('trace_type','call_type','subtraces','from_address','to_address','value_raw','success','input_data')}
    if typ=='transaction':
        f['tops'].append({k:row.get(k) for k in ('from_address','to_address','created_address','value_raw','success','gas_used','effective_gas_price','gas_price','gas_limit','input_data','transaction_type','receipt_blob_gas_used','receipt_blob_gas_price')})
        top_fields.update(k for k,v in row.items() if v is not None)
    if positive:f['positive_rows']+=1
    when=stamp(row['block_time']);sender=row.get('from_address');recipient=row.get('to_address');index=number(row['tx_index'])
    for qname,qd in querydata.items():
        fq=f['queries'][qname]
        for addr in {sender,recipient,row.get('created_address')}:
            for state in qd['pending'].get(addr,[]):
                arrival=state['arrival']
                if arrival['block']<=f['block']<=queries[qname]['end_block'] and arrival['timestamp']<=when<=state['local_end']:
                    fq['pending_touched']=True
                    if sender==addr and recipient!=addr and positive:
                        order=True if f['block']>arrival['block'] else index>arrival['tx_index'] if tx!=arrival['tx_hash'] and arrival.get('tx_index') is not None else None
                        if tx==arrival['tx_hash'] and arrival['kind']=='top' and typ=='trace' and path not in (None,'[]',[]):order=True
                        if order is True:
                            fq['pending_outgoing']=True
                            if len(fq['samples'])<2:fq['samples'].append({'address':addr,'arrival_event_id':arrival['event_id'],'arrival_block':arrival['block'],'local_end':state['local_end'],'record_type':typ,'trace_address':path,'value_raw':str(value)})
                        elif order is None:fq['strict_order_unknown']=True
            for cr in qd['context'].get(addr,[]):
                if cr.get('ledger_start_block',cr.get('start_block',-1))<=f['block']<=cr.get('ledger_end_block',cr.get('end_block',-1)):fq['context_touched']=True
del rows
sys.path.insert(0,str(C/'src'));from read_retry_r4 import logical_key
cache={};con=sqlite3.connect((C/'private/read_retry_r4.sqlite').resolve().as_uri()+'?mode=ro',uri=True);con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
try:
    for f in facts.values():
        f['cache']={}
        for method,params in [('eth_getTransactionByHash',[f['tx_hash']]),('eth_getTransactionReceipt',[f['tx_hash']]),('eth_getBlockByNumber',[hex(f['block']),False])]:
            identity={'provider':'ALCHEMY_ETH_MAINNET_EXISTING','chain':1,'method':method,'params':params};key=logical_key(identity)
            if key not in cache:
                row=con.execute('SELECT state,identity_json,success_receipt FROM read_requests WHERE logical_key=?',(key,)).fetchone()
                status=row[0] if row else 'ABSENT';receipt=json.loads(row[2]) if row and row[2] else {}
                if row and json.loads(row[1])!=identity:status='IDENTITY_CONFLICT'
                cache[key]={'method':method,'params':params,'state':status,'artifact_path':receipt.get('artifact_path'),'artifact_sha256':receipt.get('artifact_sha256'),'original_not_reread':True}
            f['cache'][method]=cache[key]
finally:con.rollback();con.close()
for tx,f in facts.items():
    for qname,qd in querydata.items():f['queries'][qname].update(current_candidate_tx=tx in qd['known'],current_weth_entry_tx=tx in qd['weth'])
    root=f.get('null_root',{});tops=f['tops'];f['top_null_basic_fields_agree']=len(tops)==1 and all(root.get(k)==tops[0].get(k) for k in ('from_address','to_address','value_raw','success'))
    f['all_three_current_success_index']=all(r['state']=='SUCCESS' for r in f['cache'].values())
    f['any_query_pending_outgoing']=any(x['pending_outgoing'] for x in f['queries'].values())
    f['any_query_weth_entry']=any(x['current_weth_entry_tx'] for x in f['queries'].values())
with gzip.open(OUT/'TRANSACTION_TRIAGE.jsonl.gz','wt',encoding='utf-8') as out:
    for tx in sorted(facts):out.write(json.dumps(facts[tx],separators=(',',':'))+'\n')
summary={'schema_version':'stage1d-current-bq-root-gap-triage-v1','status':'READ_ONLY_TRIAGE_NOT_COVERAGE_CERTIFICATE','inputs':inputs,'input_shapes':shape,'unresolved_physical_transactions':len(facts),
    'unique_blocks':len({f['block'] for f in facts.values()}),'ledger_transaction_top_fields_nonnull':dict(top_fields),
    'basic_top_null_fields_agree':sum(f['top_null_basic_fields_agree'] for f in facts.values()),'one_null_trace':sum(f['null_trace_count']==1 for f in facts.values()),
    'all_three_current_success_index':sum(f['all_three_current_success_index'] for f in facts.values()),'cache_unique_point_states':dict(collections.Counter((p['method']+'|'+p['state']) for p in cache.values())),
    'queries':{},'sample_priority_transactions':[f for f in facts.values() if f['any_query_pending_outgoing']][:10],
    'external_requests':0,'raw_originals_read':0,'normalizer_runs':0,'complete_verifier_runs':0,'large_file_sha_recomputed':False,'cpu_seconds':time.process_time()-cpu,'wall_seconds':time.perf_counter()-start}
for q,qd in querydata.items():
    selected=[f for f in facts.values() if f['queries'][q]['pending_outgoing']]
    summary['queries'][q]={k:qd[k] for k in ('collection','scope_hash','states','pending_count','context_status','context_plan_keys')}
    summary['queries'][q].update({key:sum(f['queries'][q][key] for f in facts.values()) for key in ('pending_outgoing','pending_touched','strict_order_unknown','context_touched','current_candidate_tx','current_weth_entry_tx')})
    summary['queries'][q]['pending_outgoing_all_three_success_index']=sum(f['all_three_current_success_index'] for f in selected)
    summary['queries'][q]['pending_outgoing_point_states']=dict(collections.Counter(m+'|'+c['state'] for f in selected for m,c in f['cache'].items()))
save('TRIAGE.json',summary);save('INPUT_SHAPES.json',shape)
print(json.dumps({k:summary[k] for k in ('unresolved_physical_transactions','unique_blocks','basic_top_null_fields_agree','one_null_trace','all_three_current_success_index','cache_unique_point_states','queries','cpu_seconds','wall_seconds')},indent=2))
