"""Read-only independent Fraction replay and boolean path marking.
Uses only stdlib and frozen observations, never main LP/Oracle/results to choose
allocation. Saved results are opened AFTER independent construction.
"""
from pathlib import Path
from fractions import Fraction as F
from collections import defaultdict,Counter
import json
R=Path(__file__).parent
import argparse
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--tree',type=Path,required=True)
parser.add_argument('--batch',type=Path,required=True)
parser.add_argument('--reports',type=Path,required=True)
parser.add_argument('--baseline-tree',type=Path,required=True)
parser.add_argument('--output-dir',type=Path,required=True)
args=parser.parse_args()
ROOT=args.tree.resolve();BATCH=args.batch.resolve();REPORTS=args.reports.resolve()
BASELINE=args.baseline_tree.resolve();OUT=args.output_dir.resolve()
for protected in (ROOT,BATCH,REPORTS,BASELINE):
 if OUT==protected or protected in OUT.parents:raise ValueError('Independent check outputs must be separate from read-only inputs')
OUT.mkdir(parents=True,exist_ok=True)
def batch_path(relative):
 p=(BATCH/relative).resolve()
 if not p.is_relative_to(BATCH):raise ValueError('Result path escapes supplied batch')
 return p
def batch_index():
 index=json.loads((BATCH/'RESULTS_INDEX.json').read_text(encoding='utf-8'))
 if index.get('schema_version')=='stage1c-r1-package-navigation-index-v1':
  import hashlib
  rows=[]
  for batch in index['batches']:
   subroot=batch_path(batch['path']);path=subroot/'RESULTS_INDEX.json'
   assert hashlib.sha256(path.read_bytes()).hexdigest()==batch['index_sha256'],'Package child index SHA mismatch'
   child=json.loads(path.read_text(encoding='utf-8'))
   assert len(child['method_results_index'])==batch['queries'],'Package child sample count mismatch'
   for row in child['method_results_index']:
    resolved=(subroot/row['path']).resolve()
    if not resolved.is_relative_to(subroot):raise ValueError('Child result path escapes declared batch')
    rows.append({**row,'path':resolved.relative_to(BATCH).as_posix()})
  assert len({(r['kind'],r['sample_id']) for r in rows})==len(rows),'Duplicate package sample identity'
  return {**index,'method_results_index':rows}
 if not isinstance(index.get('method_results_index'),list):raise ValueError('Unsupported batch index schema')
 return index
def sample_result(sample_id,kind):
 index=batch_index()
 rows=[x for x in index['method_results_index'] if x['sample_id']==sample_id and x['kind']==kind]
 assert len(rows)==1,('Expected exactly one independently indexed sample',sample_id,kind)
 return batch_path(rows[0]['path'])
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def key(a,asset):return str(a)+'|'+asset
O='@outside_reservoir|ETH'
def compile_observation(g):
 ops=[]
 if g.get('schema_version')=='stage1b-r3-context-model-v1':
  groups=g['objective_groups'];accounts={a['account_id']:a for a in g['accounts']};initial={k:None if a['initial_actual_balance_raw'] is None else F(a['initial_actual_balance_raw']) for k,a in accounts.items()};initial.update({k:F(0) for k in groups});initial[O]=F(0)
  amap=defaultdict(dict)
  for a in g.get('anchors',[]):amap[(a['tx_id'],a['when'])][a['account_id']]=F(a['actual_balance_raw'])
  for tx in g['transactions']:
   pre=amap[(tx['tx_id'],'pre')]|{k:F(v) for k,v in tx.get('pre_actual_balances',{}).items() if v is not None};ops.append(('anchor',pre))
   for f in tx.get('fees',[]):
    payer=f['payer_account'];ops.append(('flow',f['fee_id'],payer if payer in accounts else None,None,F(f['amount_raw']),None if payer in accounts else F(0)))
   for f in tx['flows']:
    a,b=f.get('from_account'),f.get('to_account');role=f['role'];fixed=None
    if role=='SEED':a=None;fixed=F(f['amount_raw'])
    elif role=='BACKGROUND_NORMAL':a=None;fixed=F(0)
    elif role=='UNKNOWN_EXTERNAL_INCOMING':a=O
    elif b not in accounts and b not in groups:b=O
    ops.append(('flow',f['event_id'],a,b,F(f['amount_raw']),fixed))
   post=amap[(tx['tx_id'],'post')]|{k:F(v) for k,v in tx.get('post_actual_balances',{}).items() if v is not None};ops.append(('anchor',post))
  cumulative=F(0);minimum=F(0)
  for op in ops:
   if op[0]=='flow':
    if op[2]==O:cumulative-=op[4]
    elif op[3]==O:cumulative+=op[4]
    minimum=min(minimum,cumulative)
  initial[O]=-minimum;return initial,ops,groups,-minimum
 initial={k:None if v is None else F(v) for k,v in g['initial_balances'].items()};groups=defaultdict(list);targets=set(g['target_accounts'])
 for e in sorted(g['events'],key=lambda x:x['order']):
  i=e['id'];kind=e['kind'];asset=e['asset']
  if kind in ('seed','normal_incoming','transfer','gas','boundary_outflow'):
   a=key(e['from'],asset) if kind in ('transfer','gas','boundary_outflow') else None
   b=key(e['to'],asset) if kind in ('seed','normal_incoming','transfer') else None
   n=F(e['amount_raw']);fixed=n if kind=='seed' else F(0) if kind=='normal_incoming' else None
   ops.append(('flow',i,a,b,n,fixed))
   if e.get('to') in targets and b is not None:groups[b].append(i)
  elif kind=='conversion':
   a=key(e['from'],asset);b=key(e['to'],e['output_asset']);refundto=key(e.get('refund_to',e['from']),asset);gross=F(e['gross_raw']);refund=F(e.get('refund_raw',0));net=F(e['output_raw']);assert net==gross-refund
   ops.append(('wrap',i,a,b,gross,refund,refundto,net))
   if e['to'] in targets:groups[b].append(i+':output')
   if refund and e.get('refund_to',e['from']) in targets:groups[refundto].append(i+':refund')
  elif kind=='multioutput':
   a=key(e['from'],asset);outs=[(key(o['to'],asset),F(o['amount_raw'])) for o in e['outputs']];ops.append(('split',i,a,F(e['gross_raw']),outs))
   for j,o in enumerate(e['outputs']):
    if o['to'] in targets:groups[key(o['to'],asset)].append(i+f':output{j}')
  if e.get('balance_anchors_after'):ops.append(('anchor',{k:F(v) for k,v in e['balance_anchors_after'].items()}))
 for k in groups:
  if initial.get(k) is None:initial[k]=F(0)
 return initial,ops,dict(groups),F(0)
def independent(g):
 b,ops,groups,bmin=compile_observation(g);s=defaultdict(F);active=set();marks={};capacity={};h={};not_applicable=False
 def debit(k,n):
  nonlocal not_applicable
  if k is None:return F(0)
  old=b[k]
  if old is None:
   if s[k] and n:not_applicable=True;return None
   x=F(0)
  else:
   assert n<=old;x=n*s[k]/old if n else F(0);b[k]-=n
  s[k]-=x;return x
 def credit(k,n,x):
  if k is None:return
  if b[k] is not None:b[k]+=n
  s[k]+=x
 for op in ops:
  kind=op[0]
  if kind=='anchor':
   if not not_applicable:
    for k,n in op[1].items():assert b.get(k) is None or b[k]==n;assert s[k]<=n;b[k]=n
   continue
  i=op[1]
  if kind=='flow':
   _,_,a,d,n,fixed=op;marked=n>0 and (fixed is not None and fixed>0 or fixed is None and a in active)
   marks[i]=bool(marked);capacity[i]=n
   if marked and d is not None:active.add(d)
   if not not_applicable:
    x=fixed if fixed is not None else debit(a,n)
    if x is not None:h[i]=x;credit(d,n,x)
  elif kind=='wrap':
   _,_,a,d,gross,refund,rt,net=op
   for name,n,recv in [(i+':input',gross,None),(i+':refund',refund,rt),(i+':net',net,None),(i+':output',net,d)]:
    marks[name]=a in active and n>0;capacity[name]=n
    if marks[name] and recv is not None:active.add(recv)
   if not not_applicable:
    x=debit(a,gross)
    if x is not None:
     xr=x*refund/gross;xn=x-xr;h.update({i+':input':x,i+':refund':xr,i+':net':xn,i+':output':xn});credit(rt,refund,xr);credit(d,net,xn)
  else:
   _,_,a,n,outs=op;flag=a in active and n>0;marks[i+':input']=flag;capacity[i+':input']=n
   for j,(dst,amount) in enumerate(outs):
    name=i+f':output{j}';marks[name]=flag and amount>0;capacity[name]=amount
    if marks[name]:active.add(dst)
   if not not_applicable:
    x=debit(a,n)
    if x is not None:
     h[i+':input']=x
     for j,(dst,amount) in enumerate(outs):v=x*amount/n if n else F(0);h[i+f':output{j}']=v;credit(dst,amount,v)
 return {'groups':groups,'marks':marks,'capacity':capacity,'haircut':None if not_applicable else h,'B0':bmin}

def main():
 inp=read(ROOT/'EXPERIMENT_INPUTS.json');rows=inp['controlled']+read(ROOT/'private/REAL_EXPERIMENT_INPUTS.json');result=[];compared=0
 for row in rows:
  doc=read(ROOT/row['observed_path']);got=independent(doc);kind=row.get('kind','controlled');dest=sample_result(row['sample_id'],kind);saved=read(dest/'METHOD_RESULTS.json')
  assert got['groups']==doc['objective_groups']
  for method in ['POISON','BOUNDED_REACHABILITY']:
   r=saved[method];expected_positive={a for a,es in got['groups'].items() if any(got['marks'][e] for e in es)};assert set(r['positive_addresses'])==expected_positive
   for e,v in r['events'].items():
    assert v['supported']==got['marks'][e];compared+=1
    if method=='POISON':assert F(v['nominal_raw'])==(got['capacity'][e] if got['marks'][e] else 0);compared+=1
  r=saved['HAIRCUT'];status='NOT_APPLICABLE' if got['haircut'] is None else 'COMPLETED';assert r['status']==status
  if got['haircut'] is not None:
   assert set(got['haircut'])==set(r['allocation_raw'])
   for e,n in got['haircut'].items():assert F(r['allocation_raw'][e])==n;compared+=1
   assert F(r['boundary_completion']['B0_out_raw'])==got['B0']
   for a,v in r['addresses'].items():assert F(v['point_raw'])==sum(got['haircut'][e] for e in got['groups'][a]);compared+=1
  if got['haircut'] is not None:
   for asset,rr in r['joint_by_asset'].items():assert F(rr['point_raw'])==sum(got['haircut'][e] for a,es in got['groups'].items() if a.rsplit('|',1)[1]==asset for e in es);compared+=1
  detail={'sample_id':row['sample_id'],'kind':kind,'passed':True,'haircut_status':status,'B0_out_raw':str(got['B0'])}
  if kind=='real':
   detail['haircut_joint_eth']=str(sum(got['haircut'][e] for es in got['groups'].values() for e in es)/10**18)
   detail['poison_joint_eth']=str(sum(got['capacity'][e] if got['marks'][e] else 0 for es in got['groups'].values() for e in es)/10**18)
  result.append(detail)
 report={'status':'PASS','samples':len(rows),'fact_or_allocation_checks':compared,'algorithm':'Independent stdlib boolean-state and Fraction balance replay; no project/Oracle/LP imports. Same stipulated H_BMIN completion, not independently observed outside balance.','rows':result}
 (OUT/'independent_baseline_results.json').write_text(json.dumps(report,indent=2));print({k:v for k,v in report.items() if k!='rows'});print([x for x in result if x['kind']=='real'])
if __name__=='__main__':main()
