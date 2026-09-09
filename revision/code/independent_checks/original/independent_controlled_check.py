"""External review DP, no project imports, solver/Oracle calls, numpy or scipy.
Enumerates integer local allocations and coalesces equal source-balance states,
retaining separate extrema for each requested additive objective. This is not
naive enumeration of every path, nor a count of independent real cases.
Applies to supplied small integer, same-asset/1:1 controlled operations only.
"""
from pathlib import Path
from collections import defaultdict,Counter
from fractions import Fraction as F
from itertools import product
import json,hashlib
R=Path(__file__).parent;ROOT=R/'inputs/min'
def read(p):return json.loads(p.read_text())
def ports(e):
 k=e['kind'];i=e['id']
 if k=='conversion':return [i+':input',i+':refund',i+':net',i+':output']
 if k=='multioutput':return [i+':input']+[i+f':output{j}' for j in range(len(e['outputs']))]
 return [i]
def key(a,b):return a+'|'+b

def analyze(g,variant='FULL_INTERVAL'):
 keys=sorted(g['initial_balances']);pos={k:i for i,k in enumerate(keys)};evs=sorted(g['events'],key=lambda e:e['order']);q=int(next(e['amount_raw'] for e in evs if e['kind']=='seed'))
 initial=[None if g['initial_balances'][k] is None else int(g['initial_balances'][k]) for k in keys]
 groups=g['objective_groups'];objectives={'event:'+p:{p:1} for e in evs for p in ports(e)}
 objectives.update({'address:'+a:{e:1 for e in v} for a,v in groups.items()})
 assets=sorted({a.rsplit('|',1)[1] for a in groups}) or [next(e['asset'] for e in evs if e['kind']=='seed')]
 objectives.update({'joint:'+a:{e:1 for grp,es in groups.items() if grp.rsplit('|',1)[1]==a for e in es} for a in assets})
 objnames=list(objectives);N=len(objnames)
 dp={tuple(0 for _ in keys):([0]*N,[0]*N)};actual=initial;peak=1;transitions=0
 for e in evs:
  k=e['kind'];i=e['id'];a=e['asset'];sender=pos.get(key(e.get('from','@'),a));recv=pos.get(key(e.get('to','@'),e.get('output_asset',a)))
  gross=int(e.get('gross_raw',e.get('amount_raw',0)));after=list(actual)
  # Accurate physical debit BEFORE any refund/credit, even for no-protocol variant.
  residue=None
  if k in ('transfer','gas','boundary_outflow','conversion','multioutput'):
   residue=None if actual[sender] is None else actual[sender]-gross
   assert residue is None or residue>=0
   after[sender]=residue
  def add(idx,n):
   after[idx]=None if after[idx] is None else after[idx]+n
  if k in ('seed','normal_incoming','transfer'):add(recv,int(e['amount_raw']))
  elif k=='conversion':
   refund=int(e.get('refund_raw',0));net=int(e['output_raw']);assert gross-refund==net
   refund_idx=pos[key(e.get('refund_to',e['from']),a)];add(refund_idx,refund);add(recv,net)
  elif k=='multioutput':
   caps=[int(z['amount_raw']) for z in e['outputs']];assert sum(caps)==gross
   for z in e['outputs']:add(pos[key(z['to'],a)],int(z['amount_raw']))
  for kk,v in e.get('balance_anchors_after',{}).items():
   j=pos[kk];assert after[j] is None or after[j]==int(v);after[j]=int(v)
  nd={}
  for state,(oldlo,oldhi) in dp.items():
   if k in ('seed','normal_incoming'):
    options=[{i:int(e['amount_raw']) if k=='seed' else 0}]
   else:
    lo=max(0,state[sender]-residue) if residue is not None and variant!='BALANCE_INFORMATION_REMOVED' else 0
    hi=min(state[sender],gross);options=[]
    for x in range(lo,hi+1):
     if k=='conversion':
      for r in range(max(0,x-net),min(x,refund)+1):options.append({i+':input':x,i+':refund':r,i+':net':x-r,i+':output':0 if variant=='NO_PROTOCOL_CONTINUATION' else x-r})
     elif k=='multioutput':
      for vals in product(*(range(min(x,c)+1) for c in caps)):
       if sum(vals)==x:options.append({i+':input':x,**{i+f':output{j}':v for j,v in enumerate(vals)}})
     else:options.append({i:x})
   for vals in options:
    state2=list(state)
    if k in ('seed','normal_incoming','transfer'):state2[recv]+=vals[i]
    if k in ('transfer','gas','boundary_outflow'):state2[sender]-=vals[i]
    elif k=='conversion':
     state2[sender]-=vals[i+':input'];state2[refund_idx]+=vals[i+':refund'];state2[recv]+=vals[i+':output']
    elif k=='multioutput':
     state2[sender]-=vals[i+':input']
     for j,o in enumerate(e['outputs']):state2[pos[key(o['to'],a)]]+=vals[i+f':output{j}']
    if any(v<0 or (variant!='BALANCE_INFORMATION_REMOVED' and after[j] is not None and v>after[j]) for j,v in enumerate(state2)):continue
    delta=[sum(vals.get(eid,0)*coef for eid,coef in objectives[on].items()) for on in objnames]
    lo2=[x+y for x,y in zip(oldlo,delta)];hi2=[x+y for x,y in zip(oldhi,delta)];ss=tuple(state2);transitions+=1
    if ss in nd:
     aa,bb=nd[ss];nd[ss]=([min(x,y) for x,y in zip(aa,lo2)],[max(x,y) for x,y in zip(bb,hi2)])
    else:nd[ss]=(lo2,hi2)
   if len(nd)>200000:raise ValueError('Review DP state limit exceeded; not silently skipping')
  dp=nd;actual=after;peak=max(peak,len(dp));assert dp,('infeasible',g['scenario_id'],i,variant)
 bounds={name:(min(lo[j] for lo,hi in dp.values()),max(hi[j] for lo,hi in dp.values())) for j,name in enumerate(objnames)}
 return bounds,{'peak_states':peak,'final_states':len(dp),'enumerated_local_transitions':transitions}

def check_witness(g,values,variant='FULL_INTERVAL'):
 b={k:None if v is None else F(v) for k,v in g['initial_balances'].items()};s={k:F(0) for k in b}
 def debit(k,n,x):
  assert 0<=x<=n and s[k]>=x;s[k]-=x
  if b[k] is not None:
   b[k]-=n;assert b[k]>=0
   if variant!='BALANCE_INFORMATION_REMOVED':assert s[k]<=b[k]
 def credit(k,n,x):
  s[k]+=x
  if b[k] is not None:b[k]+=n
 for e in sorted(g['events'],key=lambda e:e['order']):
  i=e['id'];k=e['kind'];a=e['asset'];v={p:F(values[p]) for p in ports(e)}
  if k in ('seed','normal_incoming'):
   x=F(e['amount_raw']) if k=='seed' else F(0);assert v[i]==x;credit(key(e['to'],a),F(e['amount_raw']),x)
  elif k in ('transfer','gas','boundary_outflow'):
   debit(key(e['from'],a),F(e['amount_raw']),v[i])
   if k=='transfer':credit(key(e['to'],a),F(e['amount_raw']),v[i])
  elif k=='conversion':
   gg=F(e['gross_raw']);rr=F(e.get('refund_raw',0));nn=F(e['output_raw']);debit(key(e['from'],a),gg,v[i+':input'])
   assert 0<=v[i+':refund']<=rr and 0<=v[i+':net']<=nn and v[i+':input']==v[i+':refund']+v[i+':net']
   assert v[i+':output']==(0 if variant=='NO_PROTOCOL_CONTINUATION' else v[i+':net'])
   credit(key(e.get('refund_to',e['from']),a),rr,v[i+':refund']);credit(key(e['to'],e['output_asset']),nn,v[i+':output'])
  elif k=='multioutput':
   debit(key(e['from'],a),F(e['gross_raw']),v[i+':input']);assert sum(v[i+f':output{j}'] for j in range(len(e['outputs'])))==v[i+':input']
   for j,o in enumerate(e['outputs']):
    x=v[i+f':output{j}'];assert 0<=x<=F(o['amount_raw']);credit(key(o['to'],a),F(o['amount_raw']),x)
  for kk,bb in e.get('balance_anchors_after',{}).items():
   assert b[kk] is None or b[kk]==F(bb);b[kk]=F(bb)
  assert all(v>=0 and (variant=='BALANCE_INFORMATION_REMOVED' or b[kk] is None or v<=b[kk]) for kk,v in s.items())
 return True

def main():
 manifest=read(ROOT/'controlled_v1/MANIFEST.json');out=[];cmp=0;wit=0
 for row in manifest['samples']:
  g=read(ROOT/row['observed_path']);h=read(ROOT/row['hidden_path']);assert check_witness(g,h['event_source_amounts_raw']);wit+=1
  f=ROOT/'results/controlled/samples'/row['sample_id'].replace('/','_')/'METHOD_RESULTS.json';results=read(f);ev=read(f.parent/'EVALUATION.json')
  details={}
  for variant in ('FULL_INTERVAL','BALANCE_INFORMATION_REMOVED','NO_PROTOCOL_CONTINUATION','NO_CROSS_TARGET_COUPLING'):
   value=results[variant];assert value['status']=='COMPLETED'
   assert set(value['addresses'])==set(g['objective_groups'])
   assert set(value['events'])=={e for es in g['objective_groups'].values() for e in es}
   assert set(value['joint_by_asset'])=={k.rsplit('|',1)[1] for k in g['objective_groups']}
   assert set(value['positive_addresses'])=={k for k,v in value['addresses'].items() if F(v['upper_raw'])>0}
  for variant in ('FULL_INTERVAL','BALANCE_INFORMATION_REMOVED','NO_PROTOCOL_CONTINUATION'):
   bounds,stats=analyze(g,variant);count=0
   for cat,catkey in [('addresses','address'),('events','event'),('joint_by_asset','joint')]:
    for name,r in results[variant][cat].items():
     expect=bounds[catkey+':'+name];assert (F(r['lower_raw']),F(r['upper_raw']))==expect,(row['sample_id'],variant,cat,name,expect,r['lower_raw'],r['upper_raw']);count+=1;cmp+=1
     for endpoint in ('lower','upper'):
      if 'endpoints' in r:
       w=r['endpoints'][endpoint]['witness_event_source_raw'];assert check_witness(g,w,variant)
       selected=g['objective_groups'][name] if cat=='addresses' else [name] if cat=='events' else [e for grp,es in g['objective_groups'].items() if grp.rsplit('|',1)[1]==name for e in es]
       assert sum(F(w[e]) for e in selected)==F(r[endpoint+'_raw']);wit+=1
   if variant=='FULL_INTERVAL':
    for cat,catkey in [('addresses','address'),('events','event'),('joint_by_asset','joint')]:
     for name,r in ev['oracle'][cat].items():assert (F(r['lower_raw']),F(r['upper_raw']))==bounds[catkey+':'+name];cmp+=1
    for a,joint in results['NO_CROSS_TARGET_COUPLING']['joint_by_asset'].items():
     gs=[grp for grp in g['objective_groups'] if grp.rsplit('|',1)[1]==a];e=(sum(bounds['address:'+grp][0] for grp in gs),sum(bounds['address:'+grp][1] for grp in gs));assert (F(joint['lower_raw']),F(joint['upper_raw']))==e;cmp+=1
   details[variant]={'objective_intervals_compared':count,**stats}
  cut=results['HAIRCUT']
  if cut.get('allocation_raw') is not None:assert check_witness(g,cut['allocation_raw']);wit+=1
  out.append({'sample_id':row['sample_id'],'family':row['family'],'details':details,'passed':True})
 report={'status':'PASS','samples':len(out),'project_imports':False,'method':'Independent chronological integer DP with exact min/max state aggregation; matches continuous extrema for these integer 1:1 flow polytopes, not arbitrary nonlinear protocols.','interval_comparisons':cmp,'allocation_witness_checks':wit,'results':out,'source_file':Path(__file__).name}
 (R/'independent_controlled_results.json').write_text(json.dumps(report,indent=2));print({k:v for k,v in report.items() if k!='results'})
if __name__=='__main__':main()
