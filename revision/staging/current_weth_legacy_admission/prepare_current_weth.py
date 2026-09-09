"""Current-demand-only WETH point reuse. Review by default; root-only --apply.

No network adapter, RPC dispatcher, provider client, solver, legacy scan or index
builder is imported or called. Current LegacyPointImporter owns any actual import.
"""
import argparse,collections,copy,datetime,hashlib,json,pathlib,re,sqlite3,sys
from contextlib import closing

WETH='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
NATIVE='native:eip155:1'
METHODS=('eth_getTransactionByHash','eth_getTransactionReceipt','eth_getBlockByNumber','eth_getCode')
INDEX_SHA='041ad4c4fb9bd9f2a65e84872ae25bd2d1ea8785075b93d6ecf8f8e4853238b1'
INDEX_BYTES=71970816
SEAL_SHA='5049e7b6ff00ed0ae1fd8f194ef7f09c7f3b9b37cb885307ef549ba6824ed710'
VERSION='stage1d-current-weth-legacy-demand-v1'

def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def digest(value):return sha(encoded(value))
def integer(v):
    if type(v) is int:return v
    if isinstance(v,str) and re.fullmatch(r'0x[0-9a-fA-F]+|[0-9]+',v):return int(v,16) if v.startswith('0x') else int(v)
    raise ValueError('Exact non-boolean integer required')
def stamp(path):
    s=path.stat();return (s.st_size,s.st_mtime_ns,s.st_ino)
def read_stable(path):
    before=stamp(path);raw=path.read_bytes()
    if stamp(path)!=before:raise ValueError('Input changed during read: '+str(path))
    return raw,before
def inside(root,name):
    p=root/name;resolved=p.resolve()
    if not resolved.is_relative_to(root.resolve()):raise ValueError('Path escaped declared root')
    for part in [p,*p.parents]:
        if part==root:break
        if part.is_symlink() or getattr(part,'is_junction',lambda:False)():raise ValueError('Linked path is not admitted')
    return resolved
def reference(root,path,raw):return {'path':path.relative_to(root).as_posix(),'sha256':sha(raw),'bytes':len(raw)}
def write_immutable(path,raw):
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():
        if path.read_bytes()!=raw:raise ValueError('Immutable current demand differs')
    else:
        with path.open('xb') as handle:handle.write(raw)
    return raw
def ro(path):
    if not path.is_file():raise ValueError('Existing adopted SQLite database required')
    con=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True);con.execute('PRAGMA query_only=ON');return con

def safe_point(work):
    if (work/'private/network_worker.lock').exists():raise ValueError('Active writer lock: wait for normal safe point')
    for folder in ('private/stage1d_sessions','private/context_sessions_r4'):
        for path in (work/folder).glob('*.json'):
            if json.loads(path.read_bytes()).get('closed') is not True:raise ValueError('Unclosed measured session: '+str(path))
    with closing(ro(work/'private/read_retry_r4.sqlite')) as con:
        pending=con.execute("SELECT count(*) FROM read_requests WHERE state IN ('IN_FLIGHT','INFLIGHT')").fetchone()[0]
        pending+=con.execute("SELECT count(*) FROM read_attempts WHERE outcome IN ('IN_FLIGHT','INFLIGHT')").fetchone()[0]
        if pending:raise ValueError('In-flight read intent must remain untouched; wait for root resolution')
        totals=dict(zip(('attempt_rows','dispatched_rows','unknown_rows'),con.execute("SELECT count(*),coalesce(sum(dispatched_at IS NOT NULL),0),coalesce(sum(outcome LIKE '%UNKNOWN%'),0) FROM read_attempts").fetchone()))
    return totals

def verify_index_copy(work):
    revision=work.parent;project=revision.parents[3]
    path=inside(revision,'recovery/legacy_metadata_index/COPY_VERIFICATION_RECEIPT.json');raw,_=read_stable(path)
    if sha(raw)!=SEAL_SHA:raise ValueError('Exact current copy-verification receipt changed')
    seal=json.loads(raw);ref=seal['copy'];index=inside(project,ref['path'])
    expected=inside(revision,'recovery/legacy_metadata_index/legacy_metadata.sqlite')
    if (index!=expected or seal.get('byte_identity_equal') is not True or seal.get('index_rebuilt') is not False
            or ref['sha256']!=INDEX_SHA or ref['bytes']!=INDEX_BYTES or seal['source']['sha256']!=INDEX_SHA):
        raise ValueError('Sealed current index identity differs')
    before=stamp(index);h=hashlib.sha256()
    with index.open('rb') as handle:
        for block in iter(lambda:handle.read(1024*1024),b''):h.update(block)
    if before!=stamp(index) or before[0]!=INDEX_BYTES or h.hexdigest()!=INDEX_SHA:raise ValueError('Current copied index bytes changed')
    return index,{'path':path.relative_to(revision).as_posix(),'sha256':SEAL_SHA,'index_sha256':INDEX_SHA,'index_bytes':INDEX_BYTES}

def derive_query(query,collection,collection_ref,freeze_ref):
    """Re-derive from current adopted membership; never reuse an old event list."""
    from collector import Scope,Event,strictly_after
    scope=Scope.from_policy(query)
    if query.get('scope_id')!=scope.scope_id or query.get('scope_hash')!=scope.scope_hash:raise ValueError('Frozen scope does not regenerate')
    metrics=collection.get('metrics',{})
    if metrics.get('scope_id')!=scope.scope_id or metrics.get('scope_hash')!=scope.scope_hash:raise ValueError('Collection is not the current exact query scope')
    if collection.get('fact_conflicts') or collection.get('quarantined_facts'):raise ValueError('Current collection has unresolved physical conflicts')
    all_events=collection.get('candidate_events',[])
    if len({e['event_id'] for e in all_events})!=len(all_events):raise ValueError('Duplicate current candidate identities')
    state_rows=collection.get('states',[]);states=collections.defaultdict(list)
    for row in state_rows:
        state=row['state']
        if state['query_id']!=query['query_id']:raise ValueError('Cross-query state')
        states[state['arrival']['event_id']].append(row)
    stopped={digest(row['state']) for row in collection.get('stops',[])}
    members=collections.defaultdict(list)
    membership=metrics.get('candidate_membership',collection.get('membership',[]))
    if 'candidate_membership' in metrics and 'membership' in collection and metrics['candidate_membership']!=collection['membership']:
        raise ValueError('Conflicting current membership aliases')
    if not isinstance(membership,list):raise ValueError('Current candidate membership list required')
    for m in membership:members[m['event_id']].append(m)
    events=[];proofs={};source_states={}
    for e in all_events:
        if not (e.get('recipient')==WETH and e.get('asset')==NATIVE and e.get('success') is True and integer(e.get('amount_raw',0))>0):continue
        if e.get('sender')==WETH:raise ValueError('Ordinary self-transfer cannot originate a WETH demand')
        for field,size in [('sender',40),('recipient',40),('tx_hash',64)]:
            if not re.fullmatch('0x[0-9a-f]{'+str(size)+'}',e[field]):raise ValueError('Exact current physical identity required')
        for field in ('block','timestamp','tx_index'):
            if type(e.get(field)) is not int or e[field]<0:raise ValueError('Exact current block/time/index required')
        if e.get('block_hash') is not None and not re.fullmatch('0x[0-9a-f]{64}',e['block_hash']):raise ValueError('Invalid current block hash')
        if e.get('kind') not in ('top','internal'):raise ValueError('Only observed native physical entry events')
        event=Event(**e);links=[]
        for m in members.get(e['event_id'],[]):
            for row in states.get(m['arrival_event_id'],[]):
                st=row['state'];arrival=Event(**st['arrival'])
                if (digest(st) in stopped or st['address']!=e['sender'] or st['asset']!=NATIVE
                    or arrival.recipient!=st['address'] or st['depth']>=scope.max_depth or m['depth']!=st['depth']+1
                    or st['local_end']!=scope.local_end(arrival)
                    or not scope.start_block<=e['block']<=scope.end_block
                    or not arrival.timestamp<=e['timestamp']<=st['local_end']
                    or strictly_after(event,arrival) is not True):continue
                state_id=digest(row);source_states[state_id]=copy.deepcopy(row)
                links.append({'membership':copy.deepcopy(m),'source_state_id':state_id})
        if not links:raise ValueError('WETH candidate lacks current legal forward membership: '+e['event_id'])
        events.append(copy.deepcopy(e));proofs[e['event_id']]=links
    events.sort(key=lambda e:(e['block'],e['tx_index'],e['tx_hash'],e['event_id']))
    document={'schema_version':VERSION,'query':copy.deepcopy(query),'query_id':query['query_id'],'scope_id':scope.scope_id,'scope_hash':scope.scope_hash,
        'current_collection':collection_ref,'current_freeze':freeze_ref,'events':events,'forward_membership':proofs,'source_states':source_states,
        'event_set_sha256':digest(sorted(e['event_id'] for e in events)),'event_facts_sha256':digest(events),
        'old_inventory_used_to_authorize_demand':False,'demand_basis':'CURRENT_FROZEN_QUERY_ACTUAL_REACHABLE_NATIVE_WETH_FORWARD_MEMBERSHIP'}
    txs=collections.defaultdict(list);blocks=collections.defaultdict(list)
    for e in events:txs[e['tx_hash']].append(e);blocks[e['block']].append(e)
    needs=[]
    def add(method,params,es):
        if len({e['block'] for e in es})!=1 or len({e['timestamp'] for e in es})!=1 or len({e.get('block_hash') for e in es if e.get('block_hash')})>1:raise ValueError('Current block physical evidence conflicts')
        if method in METHODS[:2] and len({(e['block'],e['tx_index']) for e in es})!=1:raise ValueError('Current transaction has conflicting block/index')
        block=es[0]['block'];binding={'block':block,'timestamp':es[0]['timestamp'],'block_hash':next((e['block_hash'] for e in es if e.get('block_hash')),None),
            'transactions':sorted({(e['tx_hash'],e['tx_index']) for e in es})}
        need={'query_id':query['query_id'],'scope_id':scope.scope_id,'scope_hash':scope.scope_hash,'method':method,'params':params,
            'expected_block':block,'reason':'CURRENT_REACHABLE_WETH_ENTRY_FINITE_POINT_EVIDENCE_ONLY',
            'event_ids':sorted(e['event_id'] for e in es),'physical_binding':binding,'evidence_refs':[collection_ref,freeze_ref]}
        needs.append(need)
    for tx,es in sorted(txs.items()):
        for method in METHODS[:2]:add(method,[tx],es)
    for block,es in sorted(blocks.items()):
        add('eth_getBlockByNumber',[hex(block),False],es);add('eth_getCode',[WETH,hex(block)],es)
    if len(needs)>10000:raise ValueError('Finite importer request bound exceeded')
    return document,needs

def verify_cross_query_facts(documents):
    transactions={};blocks={};positions={}
    for document in documents:
        for e in document['events']:
            tx=e['tx_hash'];fixed=(e['block'],e['tx_index'],e['timestamp'])
            if tx in transactions and transactions[tx]!=fixed:raise ValueError('Cross-query physical transaction conflict')
            transactions[tx]=fixed;block=e['block'];known=blocks.setdefault(block,{'timestamp':e['timestamp'],'hash':None})
            if known['timestamp']!=e['timestamp']:raise ValueError('Cross-query block timestamp conflict')
            h=e.get('block_hash')
            if h and known['hash'] and h!=known['hash']:raise ValueError('Cross-query block hash conflict')
            if h:known['hash']=h
            pos=(block,e['tx_index'])
            if pos in positions and positions[pos]!=tx:raise ValueError('Cross-query block transaction position conflict')
            positions[pos]=tx

def validate_point(plan,value,binding):
    method=plan['method']
    if method not in METHODS:raise ValueError('Driver allows only tx/receipt/header/code; no balances or eth_call')
    if method=='eth_getCode':
        if plan['params']!=[WETH,hex(binding['block'])] or not isinstance(value,str) or not re.fullmatch(r'0x(?:[0-9a-fA-F]{2})+',value):raise ValueError('Needed canonical historical code point absent or differs')
        return
    if not isinstance(value,dict):raise ValueError('Expected exact physical point object')
    block_field='number' if method=='eth_getBlockByNumber' else 'blockNumber'
    if integer(value[block_field])!=binding['block']:raise ValueError('Current event block conflict')
    block_hash=value['hash'] if method=='eth_getBlockByNumber' else value['blockHash']
    if binding['block_hash'] is not None and block_hash.lower()!=binding['block_hash']:raise ValueError('Current event block hash conflict')
    if method=='eth_getBlockByNumber':
        if plan['params']!=[hex(binding['block']),False] or integer(value['timestamp'])!=binding['timestamp']:raise ValueError('Current header time/selector conflict')
        hashes=value.get('transactions')
        if not isinstance(hashes,list) or any(not isinstance(x,str) for x in hashes) or len(set(hashes))!=len(hashes):raise ValueError('Complete unique header hash list required')
        for tx,index in binding['transactions']:
            if index>=len(hashes) or hashes[index].lower()!=tx:raise ValueError('Current transaction/header position conflict')
    else:
        field='hash' if method=='eth_getTransactionByHash' else 'transactionHash'
        if value[field].lower()!=plan['params'][0] or (plan['params'][0],integer(value['transactionIndex'])) not in [tuple(t) for t in binding['transactions']]:raise ValueError('Current exact transaction/index conflict')
        if method=='eth_getTransactionReceipt' and integer(value['status'])!=1:raise ValueError('Current successful WETH entry has failed receipt')

def importer_type(base):
    class CurrentWethImporter(base):
        def prepare_one(self,query,need):
            if need.get('method') not in METHODS:raise ValueError('Driver point method outside whitelist')
            for path,expected in self.current_input_stamps.items():
                if stamp(path)!=expected:raise ValueError('Current demand input changed; stop at same point')
            prepared=super().prepare_one(query,need)
            try:
                if prepared['status']=='ADMISSIBLE_POINT_PENDING_ROOT_APPLY':value=prepared['_response']['result']
                elif prepared['status']=='CURRENT_SUCCESS_REUSED_LEGACY_NOT_READ':value=self._current_success(prepared['plan'])['payload']
                else:return prepared
                validate_point(prepared['plan'],value,need['physical_binding'])
                prepared['current_weth_physical_binding_validated']=True
                return prepared
            except (ValueError,KeyError,TypeError,IndexError) as ex:
                return {'status':'POINT_ADMISSION_QUARANTINED','query_id':query['query_id'],'need':copy.deepcopy(need),
                    'reason':str(ex),'error_class':type(ex).__name__,'new_network_requests':0,'covered_ranges':0}
    return CurrentWethImporter

def compact_result(row):
    keep=('status','query_id','scope_id','scope_hash','logical_key','plan','request_sha256','freeze_sha256','index_sha256',
        'legacy_source','current_receipt','receipt','reason','error_class','response_sha256_variants','imported_storage_added_bytes','current_weth_physical_binding_validated')
    result={k:row[k] for k in keep if k in row};need=row.get('need',{})
    result['demand']={k:need[k] for k in ('query_id','scope_id','scope_hash','method','params','expected_block','event_ids','evidence_refs') if k in need}
    result.update(new_network_requests=0,new_provider_dispatches=0,complete_intervals_added=0,historical_request_attempt_total='UNKNOWN_PRESERVED_NOT_INFERRED')
    return result

def run(work,out,*,apply=False):
    work=pathlib.Path(work).resolve();out=pathlib.Path(out).resolve();revision=work.parent
    if work.name!='code' or out==work or out in work.parents or out.is_relative_to(work) or not out.is_relative_to(revision):raise ValueError('Report output must be outside production code but inside current revision')
    if out.exists() and any(out.iterdir()):raise ValueError('Use a new empty output directory')
    before=safe_point(work)  # No lock/session/attempt recovery or cleanup is performed.
    sys.path.insert(0,str(work/'src'))
    from stage1d_closure_scope import active_batch_path
    from stage1d_legacy_rpc_import import LegacyPointImporter
    freeze=active_batch_path(work);freeze_raw,freeze_stamp=read_stable(freeze);batch=json.loads(freeze_raw);freeze_ref=reference(work,freeze,freeze_raw)
    queries=batch['queries'];snapshots=[];stamps={freeze:freeze_stamp};allplans=set();alltx=set();allblocks=set()
    for query in queries:
        name=query['name']
        if not re.fullmatch(r'[a-z0-9_]+',name):raise ValueError('Invalid exact frozen query name')
        path=inside(work,'derived/stage1d/queries/'+name+'/collection.json');raw,st=read_stable(path);stamps[path]=st
        document,needs=derive_query(query,json.loads(raw),reference(work,path,raw),freeze_ref)
        snapshots.append((query,document,needs))
        allplans.update(digest({'method':n['method'],'params':n['params']}) for n in needs)
        alltx.update(e['tx_hash'] for e in document['events']);allblocks.update(e['block'] for e in document['events'])
    verify_cross_query_facts([d for q,d,n in snapshots])
    index,seal=verify_index_copy(work)
    importer=importer_type(LegacyPointImporter)(work,index);importer.current_input_stamps=stamps
    if importer.index_sha!=INDEX_SHA:raise ValueError('Importer sees a different sealed index')
    for path,st in stamps.items():
        if stamp(path)!=st:raise ValueError('Current source changed before demand preparation')
    safe_point(work)
    out.mkdir(parents=True,exist_ok=True);rows=[];snapshot_refs=[];completed=[]
    run_id=digest({'freeze':freeze_ref,'queries':[d for q,d,n in snapshots]})
    summary={'schema_version':VERSION,'mode':'APPLY' if apply else 'REVIEW_ONLY','created_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'status':'IN_PROGRESS','run_id':run_id,'freeze':freeze_ref,'sealed_index':seal,'queries':[],
        'unique_physical_transactions':len(alltx),'unique_physical_blocks':len(allblocks),'unique_physical_selectors':len(allplans),
        'cross_query_demand_count':sum(len(n) for q,d,n in snapshots),'source_variables_merged':False,'old_inventory_used_to_authorize_demand':False,
        'external_requests':0,'financial_ledger_mutated':False,'new_fee_pool':False,'covered_ranges':0,'attempts_before':before}
    try:
        for query,document,needs in snapshots:
            safe_point(work)
            for path,st in stamps.items():
                if stamp(path)!=st:raise ValueError('Current source changed; preserve exact local state')
            raw=encoded(document)
            if len(raw)>64*1024*1024:raise ValueError('Compact demand exceeds existing importer evidence bound; preserve snapshot without widening it')
            if apply:
                dest=inside(work,'private/stage1d_current_weth_demands/'+run_id+'/'+query['name']+'/DEMAND.json')
                write_immutable(dest,raw);ref=reference(work,dest,raw)
                for need in needs:need['evidence_refs']=[ref,freeze_ref]
            else:ref={'path':'review_snapshots/'+query['name']+'/DEMAND.json','sha256':sha(raw),'bytes':len(raw)}
            write_immutable(out/'review_snapshots'/query['name']/'DEMAND.json',raw);snapshot_refs.append(ref)
            if apply and needs:
                result=importer.apply_many(query,needs);results=result['results']
            else:results=[importer.prepare_one(query,need) for need in needs]
            query_rows=[compact_result(row) for row in results];rows.extend(query_rows)
            with (out/'POINT_RESULTS.jsonl').open('a',encoding='utf-8') as handle:
                for row in query_rows:handle.write(encoded(row).decode()+'\n')
            completed.append(query['name']);summary['queries'].append({'query_name':query['name'],'query_id':query['query_id'],'scope_id':query['scope_id'],'scope_hash':query['scope_hash'],
                'current_event_set_sha256':document['event_set_sha256'],'current_event_facts_sha256':document['event_facts_sha256'],'events':len(document['events']),
                'demand_count':len(needs),'snapshot':ref,'statuses':dict(collections.Counter(row['status'] for row in query_rows))})
        summary['status']='COMPLETED_LOCAL_ADMISSION' if apply else 'COMPLETED_READ_ONLY_REVIEW'
    except Exception as ex:
        summary.update(status='STOPPED_LOCAL_STATE_PRESERVED',error_class=type(ex).__name__,reason=str(ex),completed_queries=completed,
            persistent_partial_import_receipts='private/stage1d_legacy_import/calls')
        raise
    finally:
        summary['statuses']=dict(collections.Counter(row['status'] for row in rows));summary['demand_snapshots']=snapshot_refs
        summary['imported_physical_points']=sum(row['status']=='IMPORTED_POINT_SUCCESS' for row in rows)
        summary['copied_raw_bytes']=sum(row.get('imported_storage_added_bytes',0) for row in rows)
        try:
            after=safe_point(work);summary['attempts_after']=after
            if after['dispatched_rows']!=before['dispatched_rows'] or after['unknown_rows']!=before['unknown_rows']:raise ValueError('Historical dispatch/unknown attempt totals changed unexpectedly')
        except Exception as ex:summary['postcondition_issue']=type(ex).__name__+': '+str(ex);summary['status']='STOPPED_LOCAL_STATE_PRESERVED'
        write_immutable(out/'RESULT.json',encoded(summary))
    return summary

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--work',required=True);ap.add_argument('--output',required=True);ap.add_argument('--apply',action='store_true');args=ap.parse_args()
    result=run(args.work,args.output,apply=args.apply)
    print(json.dumps({k:result[k] for k in ('status','mode','unique_physical_transactions','unique_physical_blocks','unique_physical_selectors','statuses','copied_raw_bytes')},indent=2))
    return 0 if result['status'].startswith('COMPLETED_') else 2
if __name__=='__main__':raise SystemExit(main())
