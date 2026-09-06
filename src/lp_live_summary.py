"""Summarize already-solved live-Dune fixed graphs without any network calls."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


def read(path):return json.loads(path.read_text(encoding='utf-8'))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def summarize(root):
    output={'schema_version':'stage1b-live-dune-lp-summary-1.0','created_at_utc':datetime.now(timezone.utc).isoformat(),
        'analysis_status':'COMPLETED_WITH_RECORDED_GAPS','external_acceptance_status':'PENDING_REVIEW',
        'source_scope':'LIVE_DUNE_CANDIDATE_RESULTS_PLUS_EXACT_INHERITED_SEED',
        'inherited_reference_targeted_cache_used_to_fill_missing_edges':False,
        'solver_network_requests':0,'full_collection_complete':False,
        'inherited_cache_lp_outputs_overwritten':False,'probes':[]}
    for name in ('atomic_simple_transfer','harmony_high_branch'):
        folder=root/'derived/dune_live_replay'/name
        graph_path=folder/'fixed_graph.json';result_path=folder/'lp/lp_fixed_graph_result.json'
        graph=read(graph_path);result=read(result_path);collection=read(folder/'status.json')
        if result['graph_file_sha256']!=sha(graph_path):
            raise RuntimeError('LP result does not match current fixed graph: '+name)
        groups=[]
        for target,data in result['results'].items():
            joint=data['joint'];entries=data['entry_intervals']
            groups.append({'target_group':target,'asset':joint['asset'],'status':joint['status'],
                'lower_raw':joint['lower_raw'],'upper_raw':joint['upper_raw'],'positive_support':joint['positive_support'],
                'observed_entry_event_count':len(joint['objective_events']),
                'extra_entry_interval_solves':len(entries),
                'all_entry_solves_exact_certified':all(x['status']=='OPTIMAL_EXACT_CERTIFIED' for x in entries.values())})
        status='NO_OBSERVED_TARGET' if not groups else 'CONDITIONAL_INTERVALS_EXACT_CERTIFIED' if all(g['status']=='OPTIMAL_EXACT_CERTIFIED' for g in groups) else 'PARTIAL_SOLVER_OR_NUMERICAL_GAP'
        output['probes'].append({'name':name,'query_id':graph['scenario_id'],'lp_status':status,'scope':result['scope'],
            'model_construction_status':'BUILT','target_optimization_status':'NOT_RUN_NO_OBSERVED_TARGET' if not groups else 'ATTEMPTED',
            'candidate_events_including_seed':len(graph['events']),'model_variables':result['model']['variables'],
            'model_equalities':result['model']['equalities'],'balance_status':result['model']['balance_status'],
            'observed_target_groups':len(groups),'target_intervals':groups,
            'collection_status':collection['status'],'live_query_interval_count':collection['live_query_interval_count'],
            'live_exported_rows':collection['live_exported_rows'],'unresolved_frontier_count':collection['unresolved_frontier_count'],
            'assumptions':result['assumptions'],'no_target_is_not_a_zero_amount_claim':not groups,
            'fixed_graph_path':graph_path.relative_to(root).as_posix(),'fixed_graph_sha256':sha(graph_path),
            'lp_result_path':result_path.relative_to(root).as_posix(),'lp_result_sha256':sha(result_path),
            'fixed_graph_unchanged_by_lp':True})
    dest=root/'derived/dune_live_lp_summary.json'
    dest.write_text(json.dumps(output,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print(json.dumps({'output':dest.relative_to(root).as_posix(),'statuses':{p['name']:p['lp_status'] for p in output['probes']}},indent=2))


if __name__=='__main__':summarize(Path(__file__).resolve().parents[1])
