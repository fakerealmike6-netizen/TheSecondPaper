"""Replay ALL inherited canonical events through actual-frontier-only collector.

This is an explicitly incomplete historical cache, never an address-history
index and never a substitute for a completed real online probe.
"""
import argparse,csv,gzip,hashlib,json
from pathlib import Path
from collections import defaultdict
from dataclasses import asdict
from collector import Collector,Event,FetchResult,Scope,utc_seconds
from collector_inputs import load_exact_seed

def rows(path):
    opener=gzip.open if path.suffix=='.gz' else open
    with opener(path,'rt',encoding='utf-8-sig',newline='') as h: return list(csv.DictReader(h))

class InheritedSparseCache:
    replay_only=True
    def __init__(self,records,source_sha):
        self.records=records;self.source_sha=source_sha;self.requests=[]
    def fetch_interval(self,address,asset,start_block,end_block,**times):
        request=dict(address=address,asset=asset,start_block=start_block,end_block=end_block,**times)
        self.requests.append(request)
        selected=[e for e in self.records if address in (e.sender,e.recipient) and start_block<=e.block<=end_block]
        coverage={**request,'complete':False,'source_sha256':self.source_sha,'returned_events':len(selected),'basis':'INHERITED_REFERENCE_TARGETED_SPARSE_CHAIN_CACHE_NOT_COMPLETE_ADDRESS_INTERVAL'}
        return FetchResult(selected,[coverage],False,[{'reason':'HISTORICAL_CACHE_INTERVAL_INCOMPLETE','source_sha256':self.source_sha}],cache_hits=1)

def normalized_event(r):
    kind={'ETH_TOP_LEVEL':'top','ETH_INTERNAL':'internal','ERC20_TRANSFER':'erc20'}[r['event_type']]
    return Event(r['event_id'],r['tx_hash'],r['from_address'],r['to_address'],r['asset_key'],int(r['amount_raw']),int(r['block_number']),int(r['transaction_index']) if r['transaction_index'] else None,utc_seconds(r['block_timestamp']),kind,int(r['log_index']) if r['log_index'] else None,r['trace_address'] or None,success=r['transaction_status']=='SUCCESS',provenance='INHERITED_CANONICAL_CACHE:'+r['raw_evidence_sha256'])

def run(policy_path,events_path,members_path,registry_path,out):
    out.mkdir(parents=True,exist_ok=True)
    policy=json.loads(policy_path.read_text()); records=[normalized_event(r) for r in rows(events_path)]
    registry={r['address']:r for r in rows(registry_path)}
    def identity(address):
        r=registry.get(address)
        if not r: return {'kind':'UNKNOWN','status':'BUDGET_BLOCKED','acquisition_scope':'NEW_FRONTIER_SAME_POLICY_LOOKUP_PENDING'}
        kind={'SERVICE':'SERVICE','BRIDGE_BOUNDARY':'BRIDGE','MIXER_BOUNDARY':'MIXER','DEX_OR_PROTOCOL':'UNSUPPORTED_PROTOCOL','CONFLICTED_IDENTITY':'UNSUPPORTED_PROTOCOL'}.get(r['identity_class'],'UNKNOWN')
        return {'kind':kind,'actor':r.get('actor'),'status':'FROZEN_LOCAL_OBSERVATION','identity_class':r['identity_class'],'acquisition_scope':'REFERENCE_TARGETED_OR_INHERITED_CACHE'}
    metrics=[];manifests=[]
    for pilot in policy['query_pilots']:
        seed,manifest=load_exact_seed(pilot,members_path,events_path); manifests.append(manifest)
        provider=InheritedSparseCache(records,hashlib.sha256(events_path.read_bytes()).hexdigest())
        result=Collector(provider,identity).run(Scope.from_policy(pilot),seed)
        folder=out/pilot['name'];folder.mkdir(exist_ok=True)
        result.write(folder/'cached_collection.json')
        (folder/'request_plan.json').write_text(json.dumps(provider.requests,indent=2))
        warm=Collector(InheritedSparseCache(records,provider.source_sha),identity).run(Scope.from_policy(pilot),seed)
        assert warm.metrics['candidate_stop_coverage_sha256']==result.metrics['candidate_stop_coverage_sha256']
        graph, gap=fixed_graph(result,seed)
        (folder/'fixed_graph.json').write_text(json.dumps(graph,indent=2))
        (folder/'model_scope.json').write_text(json.dumps(gap,indent=2))
        metrics.append({'query_id':pilot['query_id'],'name':pilot['name'],'status':result.status,'acquisition_mode':'INHERITED_SPARSE_CACHE_REPLAY','new_online_candidate_events':0,'warm_replay_identical':True,**{k:v for k,v in result.metrics.items() if k!='candidate_membership'},'unresolved_frontier_count':len(result.unresolved_frontier)})
    (out/'cache_metrics.json').write_text(json.dumps(metrics,indent=2));(out/'seed_input_manifest.json').write_text(json.dumps(manifests,indent=2))
    print(json.dumps(metrics,indent=2))

def fixed_graph(result,seed):
    events=result.candidate_events; bytx=defaultdict(list)
    for e in events: bytx[e['tx_hash']].append(e)
    ambiguous=[tx for tx,ee in bytx.items() if len(ee)>1 and not all(e['kind']=='erc20' for e in ee) and not (len(ee)==2 and any(e['kind']=='top' for e in ee))]
    targets=sorted({s['state']['address'] for s in result.stops if s['reason']=='FIRST_IDENTIFIED_SERVICE'})
    actual=[];groups=defaultdict(list); balances={}
    for i,e in enumerate(sorted(events,key=lambda e:(e['block'],e['tx_index'] if e['tx_index'] is not None else -1,0 if e['kind']=='top' else 1,e.get('log_index') or 0,e['event_id']))):
        asset='ETH' if e['asset']=='native:eip155:1' else e['asset']
        kind='seed' if e['event_id']==seed.event_id else 'transfer'
        actual.append({'id':e['event_id'],'order':i,'kind':kind,'asset':asset,'amount_raw':str(e['amount_raw']),'from':e['sender'],'to':e['recipient']})
        balances[e['recipient']+'|'+asset]=None
        if kind!='seed': balances[e['sender']+'|'+asset]=None
        if e['recipient'] in targets: groups[e['recipient']+'|'+asset].append(e['event_id'])
    scope={'status':'ASSUMPTION_CONDITIONAL' if not ambiguous else 'ORDER_UNRESOLVED_MODEL_NOT_SOLVABLE','ambiguous_transactions':ambiguous,'online_complete':False,'missing_balance_anchors':True,'gas_context_complete':False,'unobserved_flows_zero_is_not_claimed':True,'real_world_conservative_guarantee':False}
    graph={'scenario_id':result.query_id,'scope':scope['status'],'initial_balances':balances,'target_accounts':targets,'events':actual,'objective_groups':dict(groups),'assumptions':['Only actual-frontier-reachable inherited observed candidate edges are modeled; cache acquisition was reference-targeted and interval-incomplete.','All actual initial balance bounds unknown; no missing balance is set to zero.','Unobserved source-bearing routes, gas, and unsupported conversions are not represented. Endpoints apply only to this fixed graph, not all possible real chain flows.','No acquisition depth or 90-day source-age constraint is added to the fixed-graph LP.']}
    return graph,scope

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--policy',type=Path,required=True);p.add_argument('--events',type=Path,required=True);p.add_argument('--members',type=Path,required=True);p.add_argument('--registry',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    run(a.policy,a.events,a.members,a.registry,a.output)
