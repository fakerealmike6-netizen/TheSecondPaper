"""Independent saved-evidence -> balances/ledger/model cross-check.
No imports from reviewed modules and no chain/provider calls. A saved HTTP
response hash proves artifact consistency, not an independently queried chain fact.
"""
from pathlib import Path
from collections import Counter,defaultdict
from decimal import Decimal
import json,hashlib,re,argparse,zipfile

def load(p):return json.loads(p.read_bytes(),parse_float=Decimal)
def h(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def num(v):return int(v,16) if isinstance(v,str) and v.startswith('0x') else int(v)
def key(a):return a.lower()+'|ETH' if a else None
def status_equivalent(raw, saved):
 raw=dict(raw);saved=dict(saved)
 for d in (raw,saved):
  if d.get('execution_cost_credits') is not None:d['execution_cost_credits']=Decimal(str(d['execution_cost_credits']))
 return raw==saved

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--tree',type=Path,required=True);ap.add_argument('--output',type=Path,required=True);a=ap.parse_args();r=a.tree;checks=[]
 def ck(name,ok,details=None):
  checks.append({'name':name,'passed':bool(ok),'details':details});assert ok,(name,details)
 def verified(e):
  p=r/e['path'];ck('sha:'+e['path'],p.resolve().is_relative_to(r.resolve()) and h(p)==e['sha256']);return load(p)
 rpc=[];failed=[];rpcops=0;cu=Decimal(0);bals={};blocks={};codes=[];receipts={};members=Counter();retried_original=[]
 for f in sorted((r/'raw/rpc_r3').glob('*/receipt.json')):
  rec=load(f);intent=load(f.parent/'dispatch_intent.json');rpcops+=rec['rpc_operations_actual'];cu+=Decimal(str(rec['cu_upper_bound_not_actual']))
  requests=intent['requests'];ck('rpc_count:'+rec['job'],len(requests)==rec['rpc_operations_actual']==len(rec['members']))
  rp=r/rec['raw_path'];ck('rpc_body_hash:'+rec['job'],rp.stat().st_size==rec['raw_bytes'] and h(rp)==rec['raw_sha256'])
  if rec['error_class'] or rec['http_status']!=200:
   failed.append({'job':rec['job'],'error':rec['error_class'],'http':rec['http_status'],'ops':len(requests),'raw_bytes':rec['raw_bytes']});continue
  responses=load(rp);ck('rpc_batch_ids:'+rec['job'],isinstance(responses,list) and len(responses)==len(requests) and {x['id'] for x in responses}=={x['id'] for x in requests})
  byid={x['id']:x for x in responses};member_by_art={x['artifact_path']:x for x in rec['members']}
  for i,req in enumerate(requests):
   path=(f.parent/f'envelope_{i}.json').relative_to(r).as_posix();me=member_by_art[path];env=load(r/path);res=byid[req['id']]
   ck('rpc_envelope:'+path,h(r/path)==me['artifact_sha256'] and env['request']==req and env['response']==res and env['raw_body_sha256']==rec['raw_sha256'])
   members[env['status']]+=1;rpc.append((req,res,env,me))
   if env['status']!='SUCCESS_VALIDATED':continue
   method=req['method'];par=req['params'];val=res['result']
   if method=='eth_chainId':ck('ethereum_chain',num(val)==1)
   elif method=='eth_getBlockByNumber':
    bn=num(par[0]);ck('block_identity:'+str(bn),num(val['number'])==bn)
    if bn in blocks:ck('repeat_block_hash:'+str(bn),blocks[bn]['hash']==val['hash'])
    blocks[bn]=val
   elif method=='eth_getBalance':
    identity=(key(par[0]),num(par[1]));amount=num(val)
    if identity in bals:ck('repeat_balance:'+str(identity),bals[identity]['balance']==amount)
    bals[identity]={'balance':amount,'request':req,'envelope':path,'evidence_digest':h(r/path)}
   elif method=='eth_getCode':codes.append((key(par[0]),num(par[1]),val))
   elif method=='eth_getTransactionReceipt':
    ck('receipt_tx:'+par[0],val['transactionHash'].lower()==par[0].lower());receipts[par[0].lower()]=val
  if rec.get('retry_of'):
   ck('retained_failed_risk:'+rec['job'],rec['old_attempt_risk_released'] is False);retried_original+=rec['retry_of']
 ck('single_retry_each_failed_id',len(retried_original)==len(set(retried_original))==22)
 accounting=load(r/'derived/usage_r3/RPC_REQUEST_ACCOUNTING.json');logged_ops=sum(x['rpc_operations_actual'] for x in accounting);raw_jobs={f.parent.name for f in (r/'raw/rpc_r3').glob('*/receipt.json')};not_raw=[x for x in accounting if x['job'] not in raw_jobs];ck('rpc_reconcile_logged_and_included',rpcops+sum(x['rpc_operations_actual'] for x in not_raw)==logged_ops==155)
 report={'rpc_operations_with_full_raw_batch_in_this_min':rpcops,'total_rpc_operations_in_accounting':logged_ops,'additional_weth_batch_ledger_entries':not_raw,'rpc_batches':len(list((r/'raw/rpc_r3').glob('*/receipt.json'))),'rpc_member_statuses':dict(members),'transport_failures':failed,'retried_read_operations':len(retried_original),'rpc_cu_risk_bound_not_invoice':str(cu),'independent_historical_balances':len(bals),'independent_block_headers':len(blocks),'historical_code_records':len(codes)}
 spec=load(r/'configs/CONTEXT_REPLAY_R3.json');targets=verified(spec['targets']);window_docs={q['name']:q for q in targets['queries']};job_rows={};queries=[];cost_terminal=Decimal(0);cost_peak=Decimal(0);newrows=0;used_balance_keys=set();all_code_byquery=[]
 for q in spec['queries']:
  name=q['name'];win={x['account_id']:x for x in window_docs[name]['rows']};g=verified(q['fixed_graph']);coll=verified(q['collection']);raw=[];coverage=defaultdict(list)
  for ce in q['context_jobs']:
   freeze=verified(ce['freeze']);job=verified(ce['job']);folder=(r/ce['job']['path']).parent
   sql=(folder/'query.sql').read_text();ck('sql_hash:'+job['logical_job_id'],hashlib.sha256(sql.encode()).hexdigest()==job['sql_sha256']==freeze['sql_sha256'])
   ck('query_shape:'+job['logical_job_id'],all(t in sql for t in ['ethereum.transactions','ethereum.traces','ethereum.withdrawals','ethereum.blocks','UNION ALL','top_keys','trace_keys']) and not re.search(r'\bLIMIT\s+\d',sql,re.I))
   for rw in freeze['account_windows']:
    ck('SQL_scope:'+rw['address']+':'+job['logical_job_id'],f"({rw['address']},{rw['ledger_start_block']},{rw['ledger_end_block']})" in sql)
    for typ in rw['required_coverage']:coverage[rw['account_id'],typ].append((rw['ledger_start_block'],rw['ledger_end_block']))
   status=job['latest_status_response'];ck('job_terminal_identity:'+job['logical_job_id'],status['execution_id']==job['execution_id'] and status['state']==job['state']=='QUERY_STATE_COMPLETED')
   for label in ['submit','status','latest_status']:
    rec=job[label+'_receipt'];p=r/rec['raw_path'];ck('Dune_'+label+':'+job['logical_job_id'],p.stat().st_size==rec['raw_bytes'] and h(p)==rec['sha256'] and status_equivalent(load(p),job[label+'_response']))
   cost_terminal+=Decimal(str(status['execution_cost_credits']));cost_peak+=Decimal(str(job['execution_cost_credits']))
   seenrows=[];offset=0
   for off in job['export_offsets']:
    ck('page_offset',off==offset)
    page=load(folder/f'page_{off}.json');rec=load(folder/f'page_{off}_receipt.json');p=r/rec['raw_path'];rows=page['result']['rows'];md=page['result']['metadata']
    ck('page_binding:'+job['logical_job_id'],h(p)==rec['sha256'] and p.stat().st_size==rec['raw_bytes'] and load(p)==page and page['execution_id']==job['execution_id'] and page['state']=='QUERY_STATE_COMPLETED')
    ck('page_count:'+job['logical_job_id'],len(rows)==md['row_count'] and md['total_row_count']==status['result_metadata']['total_row_count']);offset+=len(rows);seenrows+=rows
    if offset==md['total_row_count']:ck('terminal_page',page.get('next_offset') is None and page.get('next_uri') is None)
   ck('export_complete:'+job['logical_job_id'],len(seenrows)==status['result_metadata']['total_row_count']);raw+=seenrows;newrows+=len(seenrows);job_rows[job['logical_job_id']]=len(seenrows)
  txs={};roottraces=[];other=[]
  for row in raw:
   if row['record_type']=='transaction':
    tx=row['tx_hash'];fact={k:row[k] for k in ['block_number','block_hash','tx_index','from_address','to_address','value_raw','success','gas_used','effective_gas_price','gas_limit','input_data']}
    if tx in txs:ck('crossjob_same_tx:'+tx,txs[tx]==fact)
    txs[tx]=fact
   elif row['record_type']=='trace':roottraces.append(row)
   else:other.append(row)
  ck('observed_shape_supported:'+name,not other and all(x['trace_address']=='[]' and x['subtraces']==0 and x['trace_type']=='call' and x['call_type']=='call' and x['success'] is True and x['tx_success'] is True and not x['error'] for x in roottraces))
  for tr in roottraces:
   tx=txs[tr['tx_hash']];ck('top_root_identity:'+tr['tx_hash'],all(tr[k]==tx[k] for k in ['block_number','block_hash','tx_index','from_address','to_address','value_raw','success']))
  ck('all_top_transactions_simple:'+name,all(t['success'] is True and num(t['value_raw'])>0 and num(t['gas_used'])==21000 and t['input_data']=='0x' for t in txs.values()))
  doc=load(r/'derived/context_pipeline'/name/'model_input.json');accounts={x['account_id']:x for x in doc['accounts']};mdtx={x['tx_id']:x for x in doc['transactions']};candidates={x['id'] for x in g['events']};oldnative={e['event_id'] for e in coll['candidate_events']+coll['context_events'] if e['asset']=='native:eip155:1'};ids={'eip155:1:tx:'+t+':top' for t in txs}
  ck('69_value_events_reused_not_new:'+name,ids<=oldnative and set(mdtx)==set(txs))
  ck('model_objective_set_unchanged:'+name,doc['objective_groups']==g['objective_groups'])
  for tx,rawtx in txs.items():
   m=mdtx[tx];eid='eip155:1:tx:'+tx+':top';f=m['flows'][0]
   ck('model_tx_identity:'+tx,len(m['flows'])==1 and m['block_number']==rawtx['block_number'] and m['tx_index']==rawtx['tx_index'] and f['event_id']==eid and num(f['amount_raw'])==num(rawtx['value_raw']))
   b=rawtx['block_number'];fr=key(rawtx['from_address']);to=key(rawtx['to_address']);active=lambda k:k in win and win[k]['ledger_start_block']<=b<=win[k]['ledger_end_block']
   expected_fr=fr if active(fr) else None;expected_to=to if active(to) or to in doc['objective_groups'] and eid in candidates else None
   if eid==doc['seed_event_id']:expected_fr=None;role='SEED'
   elif eid in candidates:role='CANDIDATE'
   elif expected_fr and expected_to:role='MODELED_INTERNAL'
   elif expected_fr:role='BOUNDARY_OUTFLOW'
   else:
    seedtx=txs[doc['seed_event_id'].split(':')[3]];role='BACKGROUND_NORMAL' if (b,rawtx['tx_index'])<(seedtx['block_number'],seedtx['tx_index']) else 'UNKNOWN_EXTERNAL_INCOMING'
   ck('model_ports_role:'+tx,(f['from_account'],f['to_account'],f['role'])==(expected_fr,expected_to,role))
   fee=num(rawtx['gas_used'])*num(rawtx['effective_gas_price'])
   ck('gas_once_payer:'+tx,(len(m['fees'])==1 and m['fees'][0]['payer_account']==fr and num(m['fees'][0]['amount_raw'])==fee) if active(fr) else not m['fees'])
   if b in blocks:
    block=blocks[b];ck('raw_tx_block_hash:'+tx,rawtx['block_hash']==block['hash'])
    hashes=block.get('transactions',[])
    if hashes:ck('raw_tx_block_position:'+tx,(hashes[rawtx['tx_index']].get('hash') if isinstance(hashes[rawtx['tx_index']],dict) else hashes[rawtx['tx_index']])==tx)
   if tx in receipts:
    rec=receipts[tx];ck('cross_provider_fee:'+tx,num(rec['gasUsed'])*num(rec['effectiveGasPrice'])==fee and rec['blockHash']==rawtx['block_hash'] and num(rec['transactionIndex'])==rawtx['tx_index'])
  anchors=load(r/'derived/context_pipeline'/name/'balance_anchors.json');used=len(doc['anchors'])+len(accounts);reconcile=load(r/'derived/context_pipeline'/name/'ledger_reconciliation.json');byrec={x['account_id']:x for x in reconcile};gas_sum=0;zero_initial=0
  for an in anchors:
   k=(an['account_id'],an['block_number']);used_balance_keys.add(k);ck('anchor_value:'+an['anchor_id'],k in bals and bals[k]['balance']==num(an['actual_balance_raw']) and an['position']=='BLOCK_END' and an['block_number'] in blocks and an['block_hash']==blocks[an['block_number']]['hash'])
  for k,w in win.items():
   start=bals[k,w['before_anchor_block']]['balance'];actual=start;zero_initial+=start==0
   ck('initial_position:'+k,accounts[k]['initial_position']['block_number']==w['before_anchor_block'] and accounts[k]['initial_position']['phase']=='BLOCK_END' and num(accounts[k]['initial_actual_balance_raw'])==start and w['before_anchor_block']<w['ledger_start_block'])
   for typ in w['required_coverage']:
    cursor=w['ledger_start_block']
    for lo,hi in sorted(coverage[k,typ]):
     if lo>cursor:break
     if hi>=cursor:cursor=hi+1
    ck('contiguous_'+typ+':'+k,cursor>w['ledger_end_block'])
   for tx,rawtx in sorted(txs.items(),key=lambda kv:(kv[1]['block_number'],kv[1]['tx_index'])):
    bn=rawtx['block_number'];fr,to=key(rawtx['from_address']),key(rawtx['to_address'])
    if not w['ledger_start_block']<=bn<=w['ledger_end_block'] or k not in (fr,to):continue
    mt=mdtx[tx];ck('pre_balance:'+k+':'+tx,num(mt['pre_actual_balances'][k])==actual)
    amt=num(rawtx['value_raw']);fee=num(rawtx['gas_used'])*num(rawtx['effective_gas_price']) if fr==k else 0
    if fr==k:ck('prepaid_value_fee_affordable:'+k+':'+tx,actual>=amt+fee);actual-=amt+fee;gas_sum+=fee
    if to==k:actual+=amt
    ck('post_balance:'+k+':'+tx,num(mt['post_actual_balances'][k])==actual)
   final=bals[k,w['after_anchor_block']]['balance'];ck('closing_balance:'+k,actual==final==num(byrec[k]['observed_final_balance_raw']) and num(byrec[k]['difference_raw'])==0)
   for an in anchors:
    if an['account_id']!=k or not w['before_anchor_block']<an['block_number']<w['after_anchor_block']:continue
    middle=start
    for tx,rawtx in txs.items():
     if not w['ledger_start_block']<=rawtx['block_number']<=an['block_number']:continue
     amt=num(rawtx['value_raw'])
     if key(rawtx['from_address'])==k:middle-=amt+num(rawtx['gas_used'])*num(rawtx['effective_gas_price'])
     if key(rawtx['to_address'])==k:middle+=amt
    ck('intermediate_balance:'+an['anchor_id'],middle==num(an['actual_balance_raw']))
  queries.append({'name':name,'accounts':len(accounts),'raw_rows':len(raw),'physical_transactions':len(txs),'physical_value_events':len(ids),'new_value_event_ids':len(ids-oldnative),'anchors_acquired':len(anchors),'anchors_used':used,'zero_initial_balance_accounts':zero_initial,'modeled_fees':sum(len(t['fees']) for t in doc['transactions']),'modeled_actual_gas_wei':str(gas_sum),'source_role_counts':dict(Counter(f['role'] for t in doc['transactions'] for f in t['flows'])),'closing_zero_residual_accounts':len(reconcile),'context_gaps':doc['gaps'],'fact_conflicts':doc['fact_conflicts']})
 ck('all_55_balances_used',len(used_balance_keys)==len(bals)==55)
 ck('new_140_rows',newrows==140)
 ck('terminal_execution_cost',cost_terminal==Decimal('1.840029414'))
 report.update({'status':'PASS','checks':len(checks),'check_details':checks,'queries':queries,'dune_job_rows':job_rows,'dune_new_terminal_execution_cost':str(cost_terminal),'dune_retained_peak_execution':str(cost_peak),'network_calls':0,'scope':'Independent consistency and ledger reconstruction from saved responses; endpoint/source coverage statements remain within declared provider indexes and finite model.'})
 a.output.write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ['check_details']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
