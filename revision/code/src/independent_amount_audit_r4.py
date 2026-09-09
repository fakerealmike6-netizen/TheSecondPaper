"""Independent bounded time-state min-cost-flow + rational witness audit.
No imports from project LP/Oracle, numpy/scipy/networkx; no network requests.
This checks the explicitly declared finite single-source ETH model, not full-chain truth.
"""
from pathlib import Path
from collections import deque
from fractions import Fraction
import json,argparse,time

OUTSIDE='@outside_reservoir|ETH'
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def cap(v,P,relaxed):return P if v is None or relaxed else min(P,int(v))
class Net:
 def __init__(self):self.adj=[];self.forward=[]
 def node(self):self.adj.append([]);return len(self.adj)-1
 def edge(self,u,v,n,c=0,label=None):
  assert n>=0
  x=[v,len(self.adj[v]),n,c,n,label];y=[u,len(self.adj[u]),0,-c,0,None]
  self.adj[u].append(x);self.adj[v].append(y);self.forward.append((u,len(self.adj[u])-1))
 def send(self,s,t,required):
  sent=0;cost=0;augmentations=0
  while sent<required:
   dist=[None]*len(self.adj);par=[None]*len(self.adj);dist[s]=0;q=deque([s]);active={s}
   while q:
    u=q.popleft();active.discard(u)
    for i,e in enumerate(self.adj[u]):
     v,_,n,c,*_=e
     if n and (dist[v] is None or dist[v]>dist[u]+c):
      dist[v]=dist[u]+c;par[v]=(u,i)
      if v not in active:q.append(v);active.add(v)
   if dist[t] is None:raise AssertionError(f'Cannot route full source: {sent}/{required}')
   amount=required-sent;v=t
   while v!=s:u,i=par[v];amount=min(amount,self.adj[u][i][2]);v=u
   v=t
   while v!=s:
    u,i=par[v];e=self.adj[u][i];e[2]-=amount;self.adj[v][e[1]][2]+=amount;v=u
   sent+=amount;cost+=amount*dist[t];augmentations+=1
  return cost,augmentations

def ports(flow,accounts,terminals):
 role=flow['role'];fr,to=flow.get('from_account'),flow.get('to_account')
 if role=='SEED':return None,to
 if role=='BACKGROUND_NORMAL':return None,to
 if role=='UNKNOWN_EXTERNAL_INCOMING':return OUTSIDE,to
 assert fr in accounts and fr not in terminals
 return fr,to if to in accounts or to in terminals else OUTSIDE

def make_network(doc,selected,maximize=False,relaxed=False):
 accounts={x['account_id']:x for x in doc['accounts']};terminals=set(doc['objective_groups']);txs=doc['transactions']
 seeds=[f for tx in txs for f in tx['flows'] if f['role']=='SEED'];assert len(seeds)==1
 P=int(seeds[0]['amount_raw']);net=Net();S=net.node();T=net.node();last={};actual={k:int(a['initial_actual_balance_raw']) if a['initial_actual_balance_raw'] is not None else None for k,a in accounts.items()}
 actual.update({k:None for k in terminals|{OUTSIDE}})
 def state(k,balance):
  a=net.node();b=net.node();net.edge(a,b,cap(balance,P,relaxed),label='state:'+k);return a,b
 for k in actual:_,last[k]=state(k,actual[k])
 anchors={}
 for a in doc['anchors']:anchors.setdefault((a['tx_id'],a['when']),[]).append(a)
 def align(tx,phase):
  values=list(tx.get(phase+'_actual_balances',{}).items())+[(x['account_id'],x['actual_balance_raw']) for x in anchors.get((tx['tx_id'],phase),[])]
  for k,v in values:
   if v is None:continue
   v=int(v);assert actual[k] is None or actual[k]==v,(k,phase,'actual anchor mismatch')
   actual[k]=v;before=last[k];inn,new=state(k,v);net.edge(before,inn,P);last[k]=new
 def operation(eid,fr,to,n,role):
  if fr is None:origin=S if role=='SEED' else None
  else:
   origin=last[fr];rem=None if actual[fr] is None else actual[fr]-n;assert rem is None or rem>=0
   actual[fr]=rem;inn,new=state(fr,rem);net.edge(origin,inn,P);last[fr]=new
  if to is None:dest=T
  else:
   origin_hold=last[to];total=None if actual[to] is None else actual[to]+n;actual[to]=total
   inn,new=state(to,total);net.edge(origin_hold,inn,P);last[to]=new;dest=inn
  # Normal background adds actual capacity but no source injection.
  if origin is not None:net.edge(origin,dest,min(n,P),(-1 if maximize else 1) if eid in selected else 0,label=eid)
 for tx in txs:
  align(tx,'pre')
  for f in tx['fees']:operation(f['fee_id'],f['payer_account'],None,int(f['amount_raw']),'FEE')
  for f in tx['flows']:
   fr,to=ports(f,accounts,terminals);operation(f['event_id'],fr,to,int(f['amount_raw']),f['role'])
  align(tx,'post')
 for node in last.values():net.edge(node,T,P)
 cost,it=net.send(S,T,P)
 return -cost if maximize else cost,it

def verify_witness(doc,witness,selected,value,relaxed):
 A={x['account_id']:x for x in doc['accounts']};terms=set(doc['objective_groups']);keys=set(A)|terms|{OUTSIDE}
 src={k:Fraction(0) for k in keys};bal={k:(None if k not in A or A[k]['initial_actual_balance_raw'] is None else int(A[k]['initial_actual_balance_raw'])) for k in keys}
 ops={f['event_id'] for tx in doc['transactions'] for f in tx['flows']}|{f['fee_id'] for tx in doc['transactions'] for f in tx['fees']}
 assert set(witness)==ops
 injected=Fraction(0);lost=Fraction(0)
 anchors={}
 for a in doc['anchors']:anchors.setdefault((a['tx_id'],a['when']),[]).append(a)
 def bounds(k):
  assert src[k]>=0
  if bal[k] is not None:assert bal[k]>=0
  if not relaxed and bal[k] is not None:assert src[k]<=bal[k]
 def align(tx,when):
  entries=list(tx.get(when+'_actual_balances',{}).items())+[(a['account_id'],a['actual_balance_raw']) for a in anchors.get((tx['tx_id'],when),[])]
  for k,v in entries:
   if v is None:continue
   v=int(v);assert bal[k] is None or bal[k]==v;bal[k]=v;bounds(k)
 for tx in doc['transactions']:
  align(tx,'pre')
  for f in tx['fees']:
   n=int(f['amount_raw']);x=Fraction(witness[f['fee_id']]);assert 0<=x<=n;k=f['payer_account']
   if k in A:
    src[k]-=x;bal[k]=None if bal[k] is None else bal[k]-n;bounds(k);lost+=x
   else:assert x==0
  for f in tx['flows']:
   fr,to=ports(f,A,terms);n=int(f['amount_raw']);x=Fraction(witness[f['event_id']]);assert 0<=x<=n
   if f['role']=='SEED':assert x==n;injected+=x
   elif f['role']=='BACKGROUND_NORMAL':assert x==0
   if fr is not None:src[fr]-=x;bal[fr]=None if bal[fr] is None else bal[fr]-n;bounds(fr)
   if to is not None:src[to]+=x;bal[to]=None if bal[to] is None else bal[to]+n;bounds(to)
  align(tx,'post');assert sum(src.values())+lost==injected
 assert sum(Fraction(witness[e]) for e in selected)==Fraction(value)
 return {'source_injected_raw':str(injected),'source_fees_raw':str(lost),'end_source_total_raw':str(sum(src.values()))}

def main():
 p=argparse.ArgumentParser();p.add_argument('--tree',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();started=time.time();records=[];wcount=0;zero_checks=[]
 for folder in sorted((a.tree/'derived/context_pipeline').iterdir()):
  if not folder.is_dir():continue
  doc=load(folder/'model_input.json');saved=load(a.tree/'derived/context_amounts'/folder.name/'CONTEXT_AMOUNT_RESULTS.json')
  for variant,obj in saved['variants'].items():
   relaxed=variant=='MATCHED_INFORMATION_RELAXED';objectives=[]
   for cat in ['entry_intervals','address_asset_intervals']:
    objectives.extend((cat,key,rec) for key,rec in obj[cat].items())
   objectives.append(('all_service_joint','ALL_FIRST_SERVICE_ENTRIES|ETH',obj['all_service_joint']))
   for cat,key,rec in objectives:
    ids=set(rec['objective_events']);lo,itlo=make_network(doc,ids,False,relaxed);hi,ithi=make_network(doc,ids,True,relaxed)
    assert Fraction(rec['lower_raw'])==lo and Fraction(rec['upper_raw'])==hi,(folder.name,variant,key,lo,hi,rec['lower_raw'],rec['upper_raw'])
    traces={}
    for side,val in [('lower',lo),('upper',hi)]:
     traces[side]=verify_witness(doc,rec['endpoints'][side]['witness_event_source_raw'],ids,val,relaxed);wcount+=1
    records.append({'query':folder.name,'variant':variant,'category':cat,'objective':key,'lower_raw':str(lo),'upper_raw':str(hi),'matches_saved':True,'augmentations':itlo+ithi,'conservation':traces})
  # extra all-downstream-zero certificate by minimization of sum of all nonseed candidate variables
  candidate={f['event_id'] for tx in doc['transactions'] for f in tx['flows'] if f['role']=='CANDIDATE'}
  for relaxed in [False,True]:
   value,_=make_network(doc,candidate,False,relaxed)
   obj=saved['variants']['MATCHED_INFORMATION_RELAXED' if relaxed else 'BEST_AVAILABLE_CONTEXT']['all_downstream_zero'];assert bool(obj['feasible'])==(value==0)
   assert Fraction(obj['minimum_sum_of_downstream_source_raw'])==value
   proof=obj['witness'] if obj['feasible'] else obj['exclusion_certificate'];verify_witness(doc,proof['witness_event_source_raw'],candidate,value,relaxed);wcount+=1
   zero_checks.append({'query':folder.name,'information_relaxed':relaxed,'minimum_candidate_source_sum_raw':str(value),'all_zero_feasible':value==0,'not_an_independent_money_total':True})
 output={'status':'PASS','implementation':'Reviewer-authored exact integer time-state min-cost max-flow and separate Fraction ledger witness replay','project_solver_oracle_numpy_scipy_imported':False,'network_requests':0,'intervals_verified':len(records),'endpoint_witnesses_verified':wcount,'all_zero_checks':zero_checks,'checks':records,'elapsed_seconds':round(time.time()-started,3),'scope':'Declared single-source finite ETH model with modeled initial source 0 and conserved pooled external boundary; not source uniqueness on all Ethereum.'}
 a.output.write_text(json.dumps(output,ensure_ascii=False,indent=2)+'\n');print(json.dumps({k:v for k,v in output.items() if k!='checks'},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
