"""Compare frozen R2, enhanced context, and its nested information relaxation."""
from decimal import Decimal
import hashlib,json
from pathlib import Path
from lp_model import build_model,solve_interval


def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def eth(x):return format(Decimal(x)/Decimal(10**18),'f') if x is not None else None
def interval(x):return {k:x.get(k) for k in ('status','lower_raw','upper_raw')}


def compare(work,output):
    work,output=Path(work),Path(output);output.mkdir(parents=True,exist_ok=True)
    result={'schema_version':'stage1b-r3-amount-comparison-v1','external_acceptance':'PENDING_REVIEW','queries':[],
            'old_new_monotonicity_claimed':False,'nested_monotonicity_scope':'BEST_AVAILABLE_CONTEXT vs MATCHED_INFORMATION_RELAXED only'}
    for name in ('atomic_simple_transfer','harmony_high_branch'):
        base=work/'baseline/r2/derived/new_observed_graph'/name
        graph_path=base/'fixed_graph.json'
        if not graph_path.exists():
            candidates=list(base.glob('**/*fixed*graph*.json'));candidates=[p for p in candidates if 'result' not in p.name]
            if len(candidates)!=1:raise ValueError('Unique exact baseline graph required')
            graph_path=candidates[0]
        graph=read(graph_path);original=read(base/'lp/lp_fixed_graph_result.json')
        assert original['graph_file_sha256']==hashlib.sha256(graph_path.read_bytes()).hexdigest()
        names=sorted({e for group in graph['objective_groups'].values() for e in group})
        union=solve_interval(build_model(graph),names)
        assert union['status']=='OPTIMAL_EXACT_CERTIFIED'
        amounts=read(work/'derived/context_amounts'/name/'CONTEXT_AMOUNT_RESULTS.json')
        document=read(work/'derived/context_pipeline'/name/'model_input.json')
        best=amounts['variants']['BEST_AVAILABLE_CONTEXT'];relaxed=amounts['variants']['MATCHED_INFORMATION_RELAXED']
        rows=[]
        for target,old in original['results'].items():
            rows.append({'address_asset':target,'R2_BASELINE':interval(old['joint']),
                         'BEST_AVAILABLE_CONTEXT':interval(best['address_asset_intervals'][target]),
                         'MATCHED_INFORMATION_RELAXED':interval(relaxed['address_asset_intervals'][target])})
        oldevents={e['id'] for e in graph['events']};newflows={f['event_id'] for t in document['transactions'] for f in t['flows']}
        row={'name':name,'r2_graph_sha256':original['graph_file_sha256'],'candidate_events_unchanged':oldevents.issubset(newflows),
             'original_candidate_events':len(oldevents),'enhanced_native_flows':len(newflows),'additional_context_flows':sorted(newflows-oldevents),
             'labels_changed':False,'extra_seed_injections':0,'address_asset_rows':rows,
             'all_service_joint':{'R2_BASELINE':interval(union),'BEST_AVAILABLE_CONTEXT':interval(best['all_service_joint']),
                                  'MATCHED_INFORMATION_RELAXED':interval(relaxed['all_service_joint'])},
             'all_zero_downstream':{v:amounts['variants'][v]['all_downstream_zero']['feasible'] for v in amounts['variants']},
             'same_graph_nested_comparisons_passed':amounts['all_same_graph_comparisons_passed'],
             'sum_of_independent_best_upper_raw':str(sum(int(x['upper_raw']) for x in best['address_asset_intervals'].values())),
             'independent_upper_sum_is_not_joint':True}
        (output/(name+'_R2_BASELINE_ALL_SERVICE_JOINT.json')).write_text(json.dumps({'baseline_graph_sha256':original['graph_file_sha256'],'objective_events':names,'result':union},indent=2),encoding='utf-8')
        result['queries'].append(row)
    (output/'COMPARISON.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    return result


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    result=compare(a.work,a.output)
    print(json.dumps({r['name']:r['all_service_joint'] for r in result['queries']},indent=2))
