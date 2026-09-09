"""Finite context scheduling with immutable replay evidence and partial recovery."""
import argparse,json,hashlib,re,sqlite3,copy
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha
from datetime import datetime,timezone
from stage1d_runtime import rpc,execute_sql,result_rows,Runtime
from stage1d_acquisition import Labels,save_sql
from stage1d_context import required_context_windows,necessary_context_windows,build_document,REQUIRED
from context_queries_r3 import _build_sql
from context_access_r3 import read,sha
from context_access_r4 import db_path,retry_path
from context_ledger_r3 import EvidenceConflict
from read_retry_r4 import logical_key
from page_attempts import atomic_json
from stage1d_context_coverage import merge_coverage


def _number(value):
    if isinstance(value,bool):raise ValueError('Boolean is not an exact block or amount')
    return int(value,16) if isinstance(value,str) and value.startswith('0x') else int(value)


def _semantic(key,value):
    if value is None:return None
    if key in {'block_number','tx_index','value_raw','amount_raw','gas_used','gas_limit','gas_price','effective_gas_price','subtraces','withdrawal_index'}:
        return _number(value)
    if key=='trace_address':return tuple(json.loads(value) if isinstance(value,str) else value)
    if key in {'success','tx_success'} and isinstance(value,str):return value.lower()=='true' if value.lower() in {'true','false'} else value
    if isinstance(value,str) and value.startswith('0x'):return value.lower()
    return value


def _merge(left,right):
    result=copy.deepcopy(left)
    for key,value in right.items():
        old=result.get(key)
        if key=='evidence_ids':result[key]=sorted(set(old or [])|set(value or []));continue
        if value is None:continue
        if old is None:result[key]=copy.deepcopy(value);continue
        if isinstance(old,dict) and isinstance(value,dict):result[key]=_merge(old,value);continue
        if key=='transactions' and isinstance(old,list) and isinstance(value,list):
            hashes=lambda rows:[(r.get('hash') if isinstance(r,dict) else r).lower() for r in rows]
            if hashes(old)!=hashes(value):raise EvidenceConflict('Context header transaction identities disagree')
            if value and isinstance(value[0],dict):result[key]=copy.deepcopy(value)
            continue
        if key in {'id','jsonrpc','request_id','provenance','provider_alias'}:continue
        if _semantic(key,old)!=_semantic(key,value):raise EvidenceConflict('Context physical evidence disagrees at '+key)
    return result


def _result(value):
    response=value.get('response',value) if isinstance(value,dict) else value
    return response.get('result',response) if isinstance(response,dict) else response


def _row_key(row):
    kind=row.get('record_type',row.get('kind','transaction'))
    tx=str(row.get('tx_hash',row.get('hash','')) or '').lower()
    if kind in {'transaction','top','tx'}:return ('transaction',tx)
    if kind in {'trace','internal'}:return ('trace',tx,_semantic('trace_address',row.get('trace_address')))
    block=_number(row.get('block_number',row.get('block')))
    if kind=='withdrawal':return (kind,block,_number(row['withdrawal_index']))
    if kind in {'fee_recipient','block'}:return (kind,block)
    if kind=='protocol_credit':return (kind,block,_semantic('trace_address',row.get('trace_address')),
        row.get('from_address'),row.get('to_address'),row.get('created_address'),row.get('refund_address'))
    raise ValueError('Unknown cached native ledger row kind: '+str(kind))


def _merge_rows(rows,additional):
    found={}
    for row in list(rows)+list(additional):
        key=_row_key(row)
        if key[0] in {'transaction','trace'} and not re.fullmatch('0x[0-9a-f]{64}',key[1]):raise EvidenceConflict('Cached ledger lacks physical transaction identity')
        found[key]=_merge(found[key],row) if key in found else copy.deepcopy(row)
    return [found[key] for key in sorted(found,key=repr)]


def _merge_rpc(headers,balances,receipts,plan,value,evidence):
    method,params=plan['method'],plan['params']
    if method=='eth_getBlockByNumber':
        block=_number(params[0])
        if not isinstance(value,dict) or _number(value['number'])!=block or not re.fullmatch('0x[0-9a-fA-F]{64}',value.get('hash','')):
            raise EvidenceConflict('Cached header block identity differs')
        headers[block]=_merge(headers[block],value) if block in headers else copy.deepcopy(value)
    else:
        key=params[0].lower()+':'+str(_number(params[1])) if method=='eth_getBalance' else params[0].lower()
        target=balances if method=='eth_getBalance' else receipts
        if method=='eth_getTransactionReceipt' and (not isinstance(value,dict) or value.get('transactionHash','').lower()!=key):
            raise EvidenceConflict('Cached receipt transaction identity differs')
        env={'request':plan,'response':{'result':value},'evidence_ids':list(evidence)}
        target[key]=_merge(target[key],env) if key in target else env


def _load_context(work,root,folder):
    headers,balances,receipts,ledger,coverage,prior_rows,sources={},{},{},[],[],[],[]
    directories=[root]+sorted(p for p in root.iterdir() if p.is_dir() and re.fullmatch(r'(?:v[0-9]+|round_[A-Za-z0-9_-]+)',p.name))
    paths=[p/name for p in directories for name in ('headers.json','balances.json','receipts.json','ledger_rows.json','coverage.json','context_plan.json','context_result.json') if (p/name).is_file()]
    seed=work/'private/stage1d_inputs/seed_receipts.json'
    if seed.exists():paths.insert(0,seed)
    for path in paths:
        if path.is_symlink() or getattr(path.parent,'is_junction',lambda:False)():raise ValueError('Linked context evidence is not accepted')
        data=path.read_bytes();digest=hashlib.sha256(data).hexdigest();value=json.loads(data)
        saved=folder/'input_evidence'/(digest+'.json');saved.parent.mkdir(parents=True,exist_ok=True)
        if saved.exists() and sha(saved)!=digest:raise EvidenceConflict('Immutable context evidence copy changed')
        if not saved.exists():saved.write_bytes(data)
        if sha(path)!=digest:raise EvidenceConflict('Context input changed while being read')
        sources.append({'path':path.relative_to(work).as_posix(),'sha256':digest,'snapshot_path':saved.relative_to(work).as_posix()})
        evidence=['sha256:'+digest]
        if path.name=='headers.json':
            for key,item in value.items():
                result=_result(item)
                _merge_rpc(headers,balances,receipts,{'method':'eth_getBlockByNumber','params':[hex(_number(key)),False]},result,evidence)
        elif path.name in {'balances.json','receipts.json','seed_receipts.json'}:
            for key,item in value.items():
                result=_result(item)
                if result is None or isinstance(item,dict) and item.get('response',{}).get('error'):continue
                if path.name=='balances.json':
                    address,block=key.rsplit(':',1);expected={'method':'eth_getBalance','params':[address,hex(_number(block))]}
                else:expected={'method':'eth_getTransactionReceipt','params':[key]}
                if item.get('request') and item['request'].get('params')!=expected['params']:raise EvidenceConflict('Saved context RPC key differs from request')
                _merge_rpc(headers,balances,receipts,expected,result,list(item.get('evidence_ids',[]))+evidence)
        elif path.name=='ledger_rows.json':ledger=_merge_rows(ledger,[dict(row,evidence_ids=sorted(set(row.get('evidence_ids',[])+evidence))) for row in value])
        elif path.name=='coverage.json':coverage=merge_coverage(coverage,value)
        else:prior_rows.extend((value.get('context_plan',{}) if path.name=='context_result.json' else value).get('rows',[]))
    return headers,balances,receipts,ledger,coverage,prior_rows,sources


def _cached_rpc(work,plans):
    path=retry_path(work)
    if not path.exists():return []
    runtime=Runtime();found=[]
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        for plan in plans:
            identity=runtime.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',plan)
            row=db.execute("SELECT success_payload,success_receipt FROM read_requests WHERE logical_key=? AND state='SUCCESS'",(logical_key(identity),)).fetchone()
            if not row:continue
            value,receipt=json.loads(row[0]),json.loads(row[1]);artifact=(work/receipt['artifact_path']).resolve()
            if not artifact.is_relative_to(work) or not artifact.is_file() or sha(artifact)!=receipt['artifact_sha256']:raise EvidenceConflict('Persistent RPC success artifact changed')
            env=read(artifact);request=env.get('request',{});response=env.get('response',{})
            if ({k:request.get(k) for k in ('method','params')}!=plan or runtime.rpc_result_status(request,response)!='SUCCESS_VALIDATED' or response.get('result')!=value):
                raise EvidenceConflict('Persistent RPC successful response binding differs')
            found.append((plan,value,['sha256:'+receipt['artifact_sha256']]))
    return found


def _rpc_capacity(work):
    snapshot=Runtime().ledger_factory(db_path(work)).snapshot()
    rates=read(work/'private/alchemy_permission_r3.json')['method_cu_upper_bounds']
    return snapshot,rates


def _complete(row,coverage):
    for kind in REQUIRED:
        cursor=row['ledger_start_block']
        intervals=sorted((r['start_block'],r['end_block']) for r in coverage if r.get('address')==row['address'] and r.get('data_type')==kind and r.get('status')=='COMPLETE' and r.get('pagination_complete') is True)
        for lo,hi in intervals:
            if lo>cursor:break
            cursor=max(cursor,hi+1)
        if cursor<=row['ledger_end_block']:return False
    return True


def _dates(work,query,rows,headers):
    frozen=active_batch_path(work);bounds=None
    if frozen.exists():
        match=next((q for q in read(frozen)['queries'] if q['query_id']==query['query_id']),None)
        fields=('start_block','end_block','scope_id','scope_hash')
        if match and all(k in match and match[k]==query.get(k) for k in fields):
            # Production batch freezes carry integer times in the nested Scope;
            # the display-only outer *_time_utc labels never define SQL bounds.
            scope=match.get('scope',match);given=query.get('scope',query)
            if all(k in scope and scope[k]==given.get(k) for k in ('start_block','end_block','start_time','end_time')):
                if _number(scope['start_block'])<=_number(scope['end_block']) and _number(scope['start_time'])<=_number(scope['end_time']):
                    bounds={k:_number(scope[k]) for k in ('start_block','end_block','start_time','end_time')}
    ready,lo,hi,gaps=[],[],[],[]
    for row in rows:
        start,end=row['ledger_start_block'],row['ledger_end_block']
        if bounds and bounds['start_block']<=start<=end<=bounds['end_block']:
            a,b=bounds['start_time'],bounds['end_time']
        elif start in headers and end in headers:
            a,b=_number(headers[start]['timestamp']),_number(headers[end]['timestamp'])
        else:
            gaps.append({'type':'CONTEXT_DATE_DOMAIN_UNPROVED','address':row['address'],'start_block':start,'end_block':end});continue
        if a>b:raise EvidenceConflict('Inverted context timestamp domain')
        ready.append(row);lo.append(a);hi.append(b)
    dated=lambda t:datetime.fromtimestamp(t,timezone.utc).date().isoformat()
    domain={'basis':'FROZEN_QUERY_FULL_UTC_DATES_OR_EXACT_HEADERS','rows':ready,'unready':gaps,
        'start_date_utc':dated(min(lo)) if lo else None,'end_date_utc':dated(max(hi)) if hi else None,
        'batch_freeze_sha256':sha(frozen) if bounds else None,'query_global_bounds':{k:bounds[k] for k in ('start_block','end_block','start_time','end_time')} if bounds else None}
    return ready,domain


def run_context(work,query,*,fetch_ledger=True,output_variant=None,account_selection='debit_first',fetch_anchor_headers=True,rpc_batch_size=50):
    if type(rpc_batch_size) is not int or not 1<=rpc_batch_size<=50:raise ValueError('Finite RPC outer batch size must be an integer in 1..50')
    work=Path(work).resolve();qdir=work/'derived/stage1d/queries'/query['name'];collection=read(qdir/'collection.json')
    if account_selection not in {'all','debit_first'}:raise ValueError('Unknown finite context account selection')
    if output_variant is not None and not re.fullmatch(r'round_[A-Za-z0-9_-]{1,64}',output_variant):raise ValueError('New context variant must be a safe round name')
    root=qdir/'context';root.mkdir(exist_ok=True);folder=root/output_variant if output_variant else root;folder.mkdir(exist_ok=True)
    headers,balances,receipts,ledger,coverage,prior_rows,sources=_load_context(work,root,folder)
    labels_reader=Labels(work)
    addresses={s['state']['address'] for s in collection['states']}
    addresses.update(s['state']['address'] for s in collection.get('stops',[]))
    label_snapshot={address:labels_reader(address) for address in sorted(addresses)}
    atomic_json(folder/'label_snapshot.json',label_snapshot)
    plan=necessary_context_windows(query,collection,ledger,label_snapshot,prior_rows=prior_rows)
    active={query['seed_to']}|{e['sender'] for e in collection['candidate_events'] if e['event_id']!=query['seed_event_id']}
    selected=sorted((r for r in plan['rows'] if account_selection=='all' or r['address'] in active),key=lambda r:(r['address'] not in active,r['address']))
    plan.update(online_selected_accounts=[r['address'] for r in selected],account_selection=account_selection,
        selection_reason='Deterministic observed debit accounts first, then other finite non-service model accounts; no result-dependent choice.')
    atomic_json(folder/'context_plan.json',plan);atomic_json(folder/'input_evidence_manifest.json',{'sources':sources})
    plans=[]
    for row in selected:
        for key in ('before_anchor_block','after_anchor_block'):
            block=row[key]
            if row['address']+':'+str(block) not in balances:plans.append({'method':'eth_getBalance','params':[row['address'],hex(block)]})
        if fetch_anchor_headers:
            plans.extend({'method':'eth_getBlockByNumber','params':[hex(row[key]),False]} for key in ('before_anchor_block','after_anchor_block') if row[key] not in headers)
    ledger_fee_txs={r.get('tx_hash',r.get('hash')) for r in ledger if r.get('gas_used') is not None}
    plans.extend({'method':'eth_getTransactionReceipt','params':[tx]} for tx in sorted({e['tx_hash'] for e in collection['candidate_events'] if e['sender'] in active}-set(receipts)-ledger_fee_txs))
    plans=list({json.dumps(p,sort_keys=True):p for p in plans}.values())
    cached=_cached_rpc(work,plans)
    for p,value,evidence in cached:_merge_rpc(headers,balances,receipts,p,value,evidence)
    cached_keys={json.dumps(p,sort_keys=True) for p,_,_ in cached};pending=[p for p in plans if json.dumps(p,sort_keys=True) not in cached_keys]
    results=[];rpc_errors=[];resource_gaps=[]
    while pending:
        snapshot,rates=_rpc_capacity(work);remaining=snapshot['rpc_operations']['remaining'];cu=snapshot['alchemy_cu']['remaining']
        ops=0 if remaining is None else int(Decimal(remaining));units=Decimal(0) if cu is None else Decimal(cu);chosen=[]
        for p in pending[:rpc_batch_size]:
            cost=Decimal(rates[p['method']])
            if ops<1 or units<cost:break
            chosen.append(p);ops-=1;units-=cost
        if not chosen:
            resource_gaps.append({'type':'CONTEXT_RPC_RESOURCE_GAP','unprocessed_plans':pending,'remaining_rpc_operations':remaining,'remaining_alchemy_cu':cu});break
        try:
            batch=rpc(work,chosen,query['name'],'context_anchors_'+query['name']);results.append(batch)
            if len(batch['members'])!=len(chosen):raise EvidenceConflict('RPC batch member count differs')
            for p,m in zip(chosen,batch['members']):
                if m['status']=='SUCCESS_VALIDATED':_merge_rpc(headers,balances,receipts,p,m['result'],['sha256:'+m['artifact_sha256']])
                else:rpc_errors.append({'plan':p,'status':m['status'],'reason':'Specific member remains unavailable'})
        except Exception as exc:
            rpc_errors.append({'unprocessed_plans':chosen,'error_class':type(exc).__name__,'reason':str(exc)})
            if isinstance(exc,EvidenceConflict):raise
        for p,value,evidence in _cached_rpc(work,chosen):_merge_rpc(headers,balances,receipts,p,value,evidence)
        pending=pending[len(chosen):]
        atomic_json(folder/'rpc_results.json',{'plans':plans,'batches':results,'errors':rpc_errors,'resource_gaps':resource_gaps,'shared_success_cache_hits':len(cached),'rpc_batch_size':rpc_batch_size})
    atomic_json(folder/'rpc_results.json',{'plans':plans,'batches':results,'errors':rpc_errors,'resource_gaps':resource_gaps,'shared_success_cache_hits':len(cached),'rpc_batch_size':rpc_batch_size})
    for name,value in [('headers',headers),('balances',balances),('receipts',receipts)]:atomic_json(folder/(name+'.json'),value)
    needed=[r for r in selected if not _complete(r,coverage)];ready,domain=_dates(work,query,needed,headers)
    resource_gaps.extend(domain['unready']);atomic_json(folder/'date_domain.json',domain)
    from stage1d_context_recovery import _cap
    cap=_cap(work)
    outcome={'status':'NOT_REQUESTED' if not fetch_ledger else 'COMPLETE_CONTEXT_CACHE_REUSED' if selected and not needed else 'NO_SELECTED_ACCOUNTS'}
    if ready and fetch_ledger and len(ledger)<=cap:
        sql=_build_sql({'rows':[dict(r,role='NON_TERMINAL_MODEL_ACCOUNT') for r in ready]},domain['start_date_utc'],domain['end_date_utc'])
        deps=[];stable=work/'private/stage1d_context_dependencies'/sha(folder/'context_plan.json');stable.mkdir(parents=True,exist_ok=True)
        for name in ('context_plan.json','date_domain.json','headers.json','label_snapshot.json'):
            source=folder/name;target=stable/(sha(source)+'_'+name)
            if not target.exists():target.write_bytes(source.read_bytes())
            if sha(target)!=sha(source):raise EvidenceConflict('Frozen context dependency changed')
            deps.append({'path':target.relative_to(work).as_posix(),'sha256':sha(target)})
        intervals=[{'query_id':query['query_id'],'kind':'context','address':r['address'],'start_block':r['ledger_start_block'],'end_block':r['ledger_end_block']} for r in ready]
        freeze=save_sql(work,sql,'context',query,intervals,deps)
        try:outcome=execute_sql(work,freeze,query['name'],'context_'+query['name'])
        except Exception as exc:outcome={'status':'CONTEXT_ACQUISITION_PARTIAL','error_class':type(exc).__name__,'reason':str(exc)}
        if outcome.get('status')=='COMPLETED_EXPORTED':
            ledger=_merge_rows(ledger,[dict(row,evidence_ids=sorted(set(row.get('evidence_ids',[])+['sha256:'+sha(freeze)]))) for row in result_rows(work,outcome['job_folder'])])
            for row in ready:
                coverage.extend({'address':row['address'],'data_type':kind,'start_block':row['ledger_start_block'],'end_block':row['ledger_end_block'],
                    'status':'COMPLETE','pagination_complete':True,'provider_frozen_scope':True,'date_domain_verified':True,'block_domain_verified':True,'evidence_ids':['sha256:'+sha(freeze)]} for kind in REQUIRED)
    elif needed and not ready and fetch_ledger:outcome={'status':'MISSING_PROVABLE_CONTEXT_DATE_DOMAINS','gaps':domain['unready']}
    if len(ledger)>cap:resource_gaps.append({'type':'CONTEXT_PHYSICAL_EVENT_CAP_EXCEEDED','observed_unique_physical_records':len(ledger),'cap':cap,'raw_retained':True,'not_truncated':True})
    coverage=merge_coverage(coverage)
    for name,value in [('ledger_rows',ledger),('coverage',coverage),('acquisition',outcome)]:atomic_json(folder/(name+'.json'),value)
    if output_variant is None:atomic_json(qdir/'label_snapshot.json',label_snapshot)
    if len(ledger)>cap:result={'completion_status':'CONTEXT_RESOURCE_LIMIT_MODEL_BLOCKED','evidence_gaps':resource_gaps,'reason':'Retained physical context exceeds the fixed per-query cap'}
    else:
        try:result=build_document(query,collection,ledger,balances,headers,receipts,label_snapshot,coverage=coverage,context_plan=plan)
        except Exception as exc:result={'completion_status':'EVIDENCE_CONFLICT_MODEL_BLOCKED' if 'Conflict' in type(exc).__name__ else 'CONTEXT_ADAPTER_ERROR','error_class':type(exc).__name__,'reason':str(exc)}
    hash_gaps=[{'type':'CONTEXT_ANCHOR_BLOCK_HASH_MISSING','address':r['address'],'block_number':r[k]} for r in selected for k in ('before_anchor_block','after_anchor_block') if r['address']+':'+str(r[k]) in balances and r[k] not in headers]
    gaps=resource_gaps+hash_gaps
    if gaps:
        result.setdefault('evidence_gaps',[]).extend(gaps)
        if result.get('model_input'):result['model_input'].setdefault('gaps',[]).extend(gaps)
        if result['completion_status']=='FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE':result['completion_status']='PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS'
    result['context_acquisition_metadata']={'output_variant':output_variant,'account_selection':account_selection,'rpc_batch_size':rpc_batch_size,'physical_context_records':len(ledger),'context_event_cap':cap,'raw_never_truncated':True,'inherited_evidence_files':len(sources),'resource_gaps':resource_gaps,'rpc_errors':rpc_errors}
    atomic_json(folder/'context_result.json',result)
    if result.get('model_input'):atomic_json(folder/'model_input.json',result['model_input'])
    return {'query':query['name'],'context_status':result['completion_status'],'context_folder':folder.relative_to(work).as_posix(),'accounts':len(plan['rows']),'queried_accounts':len(selected),'known_balance_anchors':len(balances),'ledger_rows':len(ledger),'context_event_cap':cap,'resource_gaps':resource_gaps,'acquisition':outcome,'error':result.get('reason')}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--query',required=True);p.add_argument('--no-ledger',action='store_true');p.add_argument('--output-variant');p.add_argument('--account-selection',choices=('all','debit_first'),default='debit_first');p.add_argument('--no-anchor-headers',action='store_true');p.add_argument('--rpc-batch-size',type=int,default=50);a=p.parse_args()
    q=next(q for q in active_batch(a.work)['queries'] if q['name']==a.query)
    print(json.dumps(run_context(a.work,q,fetch_ledger=not a.no_ledger,output_variant=a.output_variant,account_selection=a.account_selection,fetch_anchor_headers=not a.no_anchor_headers,rpc_batch_size=a.rpc_batch_size),indent=2))
