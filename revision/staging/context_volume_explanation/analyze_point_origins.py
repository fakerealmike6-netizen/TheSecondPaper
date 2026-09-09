"""Read only the still-SHA-matching Tx1 collection referenced by fixed PREP."""
from analyze_fixed_preparation import R,C,S,BASE,PREP_SHA,read,event,asset,digest
import collections, hashlib,json,time

t=time.perf_counter()
p=read(BASE/'PREPARATION_RESULT.json',PREP_SHA);q=p['queries']['txphish_src001']
col=read(C/q['inputs']['collection']['path'],q['inputs']['collection']['sha256'])
windows={r['address']:r for r in q['context_plan']['rows']}
known={i for r in windows.values() for i in r['known_physical_event_ids']}
known.update(i for b in q['context_plan']['protocol_boundary_entries'] for i in b['entry_event_ids'])
known.update(i for ids in q['context_plan']['objective_groups'].values() for i in ids)
known_tx={i.split(':tx:',1)[1].split(':',1)[0] for i in known if ':tx:' in i}
pts={m:{r['request']['params'][0] for r in q['point_requests'] if r['request']['method']==m} for m in ('eth_getTransactionReceipt','eth_getTransactionByHash')}
facts=col['candidate_events']+col['context_events'];txs=collections.defaultdict(list)
for orig in facts:
 e=event(orig)
 if e.get('tx_hash'):txs[e['tx_hash']].append(e)
details=[]
for txid in sorted(pts['eth_getTransactionReceipt']-known_tx):
 rows=txs.get(txid,[]);touch=[]
 for e in rows:
  for a in {e.get('sender'),e.get('recipient')}:
   w=windows.get(a)
   if w and w['ledger_start_block']<=e['block']<=w['ledger_end_block']:touch.append((a,e))
 details.append({'tx_hash':txid,'current_collection_rows':len(rows),'assets':sorted({e['asset'] for e in rows}),
  'native_rows':sum(asset(e['asset'])=='ETH' for e in rows),'weth_rows':sum(asset(e['asset'])==e['asset'] and e['asset']!='ETH' for e in rows),
  'foreign_token_rows_touching_current_native_window':sum(asset(e['asset']) is None for a,e in touch),
  'matching_addresses':sorted({a for a,e in touch}),'transaction_point_required':txid in pts['eth_getTransactionByHash']})
groups=collections.Counter()
for d in details:
 if not d['current_collection_rows']:groups['NOT_IN_COLLECTION_MAY_BE_SELECTED_MATERIAL']+=1
 elif d['foreign_token_rows_touching_current_native_window'] and not d['native_rows'] and not d['weth_rows']:groups['FOREIGN_TOKEN_TX_TOUCHES_NATIVE_WINDOW_FOR_FEES']+=1
 elif d['native_rows']:groups['NATIVE_COLLECTION_FACT_OUTSIDE_PLAN_KNOWN_LIST']+=1
 elif d['weth_rows']:groups['WETH_COLLECTION_FACT']+=1
 else:groups['UNCLASSIFIED']+=1
account=collections.Counter(a for d in details if d['foreign_token_rows_touching_current_native_window'] for a in d['matching_addresses'])
out={'schema_version':'stage1d-fixed-context-point-origin-diagnosis-v1','preparation_sha256':PREP_SHA,
 'collection_ref':q['inputs']['collection'],'collection_counts':{'candidate':len(col['candidate_events']),'context':len(col['context_events'])},
 'receipt_requests_outside_frozen_plan_known_txs':len(details),'group_counts':dict(groups),
 'foreign_token_transaction_account_overlapping_counts':dict(account.most_common()),'details':details,'seconds':time.perf_counter()-t,
 'limitation':'Material-family txs outside known event IDs are classified only as absent from collection; fixed main diagnosis separately proves the selected material projection.'}
f=S/'POINT_ORIGINS.json'
with f.open('x',encoding='utf-8') as w:json.dump(out,w,ensure_ascii=False,sort_keys=True,indent=2);w.write('\n')
print(json.dumps({'sha256':hashlib.sha256(f.read_bytes()).hexdigest(),'group_counts':dict(groups),'top_accounts':account.most_common(5),'seconds':out['seconds']}))
