"""Bounded, read-only current WETH selector inventory. Outputs audits only."""
import collections, datetime, hashlib, json, pathlib, sqlite3, sys, time

C = pathlib.Path(__file__).resolve().parents[2]
R = C.parent
OUT = C / 'checks/semantic_units'
sys.path.insert(0, str(C / 'src'))
from stage1d_transfers_acquisition import verified_member
from read_retry_r4 import logical_key

WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def ref(p): return {'path':p.relative_to(R).as_posix(),'sha256':sha(p),'bytes':p.stat().st_size}
def integer(v):
    if type(v) is int:return v
    if isinstance(v,str):return int(v,16) if v.startswith('0x') else int(v)
    raise ValueError('Missing integral physical binding')

def main():
    start=time.perf_counter();cpu=time.process_time();sources=[];query_events={};tx_events={};blocks={};matches=[]
    req_path=R/'CURRENT_FINITE_REQUIREMENTS.json';sources.append(ref(req_path));req=read(req_path)
    for q in req['queries']:
        if not q['weth_events']:continue
        p=C/'derived/stage1d/queries'/q['query_name']/'collection.json';sources.append(ref(p));co=read(p)
        candidates={e['event_id']:e for e in co['candidate_events'] if e['recipient']==WETH and e['asset']=='native:eip155:1' and e['success'] is True and e['amount_raw']>0}
        arrivals={s['state']['arrival']['event_id'] for s in co['states'] if s['state']['address']==WETH}
        requested={e['event_id']:e for e in q['weth_events']}
        if set(requested)!=set(candidates) or not set(requested)<=arrivals:raise ValueError('Requirements/current reachable entries differ: '+q['query_name'])
        for eid,e in requested.items():
            for k in ('tx_hash','block','timestamp','tx_index','sender','recipient','amount_raw','kind','trace_address'):
                if e.get(k)!=candidates[eid].get(k):raise ValueError('Current event differs: '+eid+':'+k)
        es=list(candidates.values());query_events[q['query_name']]=es
        matches.append({'query_name':q['query_name'],'scope_hash':q['scope_hash'],'event_count':len(es),'unique_physical_transactions':len({e['tx_hash'] for e in es}),'unique_physical_blocks':len({e['block'] for e in es}),'all_entries_have_actual_arrival':True,'semantic_unit_count':len(co.get('semantic_units',[]))})
        for e in es:
            tx_events.setdefault(e['tx_hash'],{})[e['event_id']]=e
            blocks.setdefault(e['block'],[]).append(e)
    plans={}
    for tx,es in tx_events.items():
        for method in ('eth_getTransactionByHash','eth_getTransactionReceipt'):plans[(method,tx)]={'method':method,'params':[tx]}
    for block in blocks:
        plans[('eth_getBlockByNumber',block)]={'method':'eth_getBlockByNumber','params':[hex(block),False]}
        plans[('eth_getCode',block)]={'method':'eth_getCode','params':[WETH,hex(block)]}
    db=C/'private/read_retry_r4.sqlite';db_before={'path':db.relative_to(R).as_posix(),'bytes':db.stat().st_size,'mtime_ns':db.stat().st_mtime_ns}
    con=sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True);con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
    selectors=[];result_map={};records={}
    try:
        state_counts=dict(con.execute('SELECT state,count(*) FROM read_requests GROUP BY state'))
        for (method,selector),plan in sorted(plans.items(),key=lambda z:(z[0][0],str(z[0][1]))):
            key=logical_key({'provider':'ALCHEMY_ETH_MAINNET_EXISTING','chain':1,**plan})
            row=con.execute('SELECT state,identity_json,success_payload,success_receipt FROM read_requests WHERE logical_key=?',(key,)).fetchone()
            item={'method':method,'selector':selector,'plan':plan,'logical_key':key,'state':row[0] if row else 'ABSENT_FROM_CURRENT_SUCCESS_INDEX','reusable':False,'evidence_refs':[]}
            if row and row[0]=='SUCCESS':
                try:
                    identity=json.loads(row[1]);expected={'provider':'ALCHEMY_ETH_MAINNET_EXISTING','chain':1,**plan}
                    if identity!=expected:raise ValueError('Stored request identity differs')
                    member=dict(json.loads(row[3]),result=json.loads(row[2]),cache_hit=True,status='SUCCESS_VALIDATED')
                    value=verified_member(C,plan,member)
                    # The production verifier validates original request/response; enforce the current physical event binding too.
                    if method in ('eth_getTransactionByHash','eth_getTransactionReceipt'):
                        ev=list(tx_events[selector].values());b={e['block'] for e in ev};ti={e['tx_index'] for e in ev}
                        if {integer(value['blockNumber'])}!=b or {integer(value['transactionIndex'])}!=ti:raise ValueError('Current block/transaction-index binding differs')
                        if method=='eth_getTransactionReceipt' and integer(value['status'])!=1:raise ValueError('Current successful entry has failed receipt')
                    elif method=='eth_getBlockByNumber':
                        if integer(value['number'])!=selector or {integer(value['timestamp'])}!={e['timestamp'] for e in blocks[selector]}:raise ValueError('Header current number/timestamp differs')
                        if not isinstance(value.get('transactions'),list) or len(set(value['transactions']))!=len(value['transactions']):raise ValueError('Header full transaction hash list missing')
                        for e in blocks[selector]:
                            if value['transactions'][e['tx_index']].lower()!=e['tx_hash'].lower():raise ValueError('Header transaction-index hash differs')
                    elif not isinstance(value,str) or value=='0x':raise ValueError('Historical canonical WETH runtime missing')
                    p=C/member['artifact_path'];env=read(p);item['evidence_refs'].append(ref(p));original=C/env['original_artifact']['path'] if env.get('original_artifact') else p
                    if original!=p:item['evidence_refs'].append(ref(original))
                    raw=original.parent/'response_body.bin'
                    if env.get('evidence_kind')=='REAL_CHAIN_LEGACY_RAW_REUSE':
                        item['evidence_contract']='STRICT_ADMITTED_LEGACY_POINT_ORIGINAL_HTTP_UNKNOWN'
                        for field in ('admission_path',):
                            if member.get(field):item['evidence_refs'].append(ref(C/member[field]))
                        # legacy_source holds historical locator strings only.
                        # Read the verified, already admitted current copies.
                        for field in ('raw_path','manifest_path'):
                            if env.get(field):item['evidence_refs'].append(ref(C/env[field]))
                    elif raw.is_file():item['evidence_contract']='CURRENT_HTTP200_ENVELOPE_AND_ORIGINAL_BODY';item['evidence_refs'].append(ref(raw))
                    else:item['evidence_contract']='ACCEPTED_CURRENT_SUCCESS_ENVELOPE_SHA_EXACT_REQUEST_RESPONSE_NO_BODY'
                    item['reusable']=True;item['result_content_sha256']=hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                    result_map[(method,selector)]=value
                    item['member']=member.copy();item['member'].pop('result',None)
                except Exception as ex:item['verification_error']=type(ex).__name__+': '+str(ex)
            selectors.append(item);records[(method,selector)]=item
    finally:con.rollback();con.close()
    cross=[]
    for tx,es in tx_events.items():
        e=next(iter(es.values()));values={m:result_map.get((m,tx)) for m in ('eth_getTransactionByHash','eth_getTransactionReceipt')};h=result_map.get(('eth_getBlockByNumber',e['block']))
        hashes=[x['blockHash'].lower() for x in values.values() if x is not None]+([h['hash'].lower()] if h else [])
        if len(set(hashes))>1:cross.append({'tx_hash':tx,'reason':'CONFLICTING_ORIGINAL_BLOCK_HASHES'})
    def counts(allowed=None):
        rr=[a for a in selectors if allowed is None or (a['method'],a['selector']) in allowed]
        return {m:{'required_unique_selectors':len(x:=[a for a in rr if a['method']==m]),'reusable_success':sum(a['reusable'] for a in x),'absent':sum(a['state']=='ABSENT_FROM_CURRENT_SUCCESS_INDEX' for a in x),'known_non_success':sum(a['state'] not in ('SUCCESS','ABSENT_FROM_CURRENT_SUCCESS_INDEX') for a in x),'success_but_unverified':sum(a['state']=='SUCCESS' and not a['reusable'] for a in x),'evidence_contracts':dict(collections.Counter(a.get('evidence_contract') for a in x if a['reusable']))} for m in sorted({a['method'] for a in rr})}
    for q in matches:
        es=query_events[q['query_name']];ts={e['tx_hash'] for e in es};bs={e['block'] for e in es};allowed={(m,t) for m in ('eth_getTransactionByHash','eth_getTransactionReceipt') for t in ts}|{(m,b) for m in ('eth_getBlockByNumber','eth_getCode') for b in bs};q['point_counts']=counts(allowed)
    stable=all(ref(R/s['path'])==s for s in sources)
    report={'schema_version':'stage1d-current-weth-evidence-gap-audit-v1','snapshot_kind':'DYNAMIC_READ_ONLY_AUDIT','created_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'inputs':sources,'input_sources_stable':stable,'sqlite_read_contract':'URI mode=ro; query_only; one read transaction; exact primary logical keys','db_snapshot':db_before,'db_state_counts':state_counts,'queries':matches,'unique_physical_transactions':len(tx_events),'unique_physical_blocks':len(blocks),'counts':counts(),'cross_binding_conflicts':cross,'selectors':selectors,'no_new_external_requests':True,'no_production_writes':True,'no_formal_algorithm':True,'not_an_adoption_or_certificate':True,'cpu_seconds':time.process_time()-cpu,'wall_seconds':time.perf_counter()-start}
    (OUT/'CURRENT_WETH_POINT_INVENTORY.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:report[k] for k in ('input_sources_stable','unique_physical_transactions','unique_physical_blocks','counts','cross_binding_conflicts','cpu_seconds','wall_seconds')},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
