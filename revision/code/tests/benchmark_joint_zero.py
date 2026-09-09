"""Bounded synthetic on/off measurement; never consumes a real query graph."""
import copy
import json
from pathlib import Path
import statistics
import sys
from time import perf_counter
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(BASE/'src'),str(BASE/'tests')]
from scipy.optimize import linprog
from stage1c_intervals import METHODS, run_interval
from stage1c_zero_derivation import digest
from test_stage1c_joint_zero import zero_graph, amounts


def main():
    document=zero_graph(); rows=[]
    for method in METHODS:
        modes={}; observations={}
        for enabled in (False,True):
            timings=[]; counts=[]; endpoints=None
            for repetition in range(6):
                # Input copy is common preprocessing, outside method timing.
                argument=copy.deepcopy(document)
                with patch('scipy.optimize.linprog',wraps=linprog) as calls:
                    start=perf_counter()
                    result=run_interval(argument,method,joint_zero_optimization=enabled)
                    elapsed=perf_counter()-start
                    count=calls.call_count
                current=amounts(result)
                if endpoints is not None and current!=endpoints:
                    raise AssertionError('Non-deterministic exact endpoints')
                endpoints=current
                if count!=result['task_counts']['endpoint_optimizations']:
                    raise AssertionError('Reported LP count differs from actual linprog calls')
                timings.append(elapsed);counts.append(count)
            mode='joint_first' if enabled else 'legacy_schedule'
            modes[mode]={'warmup_seconds':timings[0],'timed_seconds':timings[1:],
                'median_seconds':statistics.median(timings[1:]),'actual_linprog_calls_per_run':counts,
                'task_counts':result['task_counts'],'endpoints_sha256':digest(endpoints)}
            observations[mode]=endpoints
        equal=observations['joint_first']==observations['legacy_schedule']
        if not equal:raise AssertionError('Optimization changed scientific output')
        rows.append({'method':method,'exact_domain_asset_endpoints_equal':equal,**modes,
            'actual_endpoint_calls_saved_per_run':modes['legacy_schedule']['actual_linprog_calls_per_run'][0]
                -modes['joint_first']['actual_linprog_calls_per_run'][0]})
    names=['src/lp_model.py','src/stage1c_intervals.py','src/stage1c_zero_derivation.py',
        'src/stage1c_output_contract.py','tests/test_stage1c_joint_zero.py','tests/benchmark_joint_zero.py']
    import hashlib
    report={'status':'PASS','input_sha256':digest(document),'synthetic_fixture':document,
        'warmup_per_mode':1,'timed_repetitions_per_mode':5,'rows':rows,
        'source_sha256':{name:hashlib.sha256((BASE/name).read_bytes()).hexdigest() for name in names},
        'measurement':'Fresh run_interval model and run-level objective/parent memo each repetition. '
            'Wall time includes model construction, exact proof generation, solver and source-output construction; '
            'input copy, import and receiver checks excluded. unittest.mock call accounting is included equally. '
            'This small synthetic measurement is not a real-query profile or provider cost saving.'}
    output=BASE/'checks'/'joint_zero';output.mkdir(parents=True,exist_ok=True)
    (output/'SYNTHETIC_ON_OFF_BENCHMARK.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'status':'PASS','rows':[{'method':r['method'],'before_calls':r['legacy_schedule']['actual_linprog_calls_per_run'][0],
        'after_calls':r['joint_first']['actual_linprog_calls_per_run'][0],
        'before_median_seconds':r['legacy_schedule']['median_seconds'],
        'after_median_seconds':r['joint_first']['median_seconds']} for r in rows]},indent=2))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
