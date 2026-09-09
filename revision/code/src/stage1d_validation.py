"""Final Stage1D package gate: inherited offline guard plus fresh real replay."""
from __future__ import annotations
import argparse, json, platform
from pathlib import Path
from stage1d_experiments import read, write, safe, validate_saved_batch

def validate_package(tree,batch_path,results_path,output,kind='min'):
    """Validate a fresh extracted tree, never mutate it, and execute without keys.

    ``batch_path`` and ``results_path`` are relative in-package locations. Public
    packages pass a synthetic four-source batch and never claim private replay.
    A saved PASS alone cannot satisfy either of the two independent subprocesses.
    """
    from validate_review_bundle_r1 import Validator, tree_hashes
    from run_stage1c import semantic_result
    tree=Path(tree).resolve();batch=safe(tree,batch_path);saved=safe(tree,results_path)
    if kind not in ('min','public'):raise ValueError('Explicit min/public package kind required')
    validator=Validator(tree,Path(output).resolve(),kind)
    validator.command('stage1d_saved_gate','src/stage1d_experiments.py',
        ['--tree',tree,'--batch',batch_path,'--action','gate','--results',saved,'--output',validator.out/'SAVED_GATE.json'])
    replay=validator.out/'fresh_method_replay'
    replay_ok=validator.command('stage1d_fresh_seven_method_replay','src/stage1d_experiments.py',
        ['--tree',tree,'--batch',batch_path,'--action','run','--output',replay])
    if replay_ok:
        validator.command('stage1d_replay_saved_gate','src/stage1d_experiments.py',
            ['--tree',tree,'--batch',batch_path,'--action','gate','--results',replay,'--output',validator.out/'REPLAY_GATE.json'])
        try:
            expected=read(saved/'RESULTS_INDEX.json');actual=read(replay/'RESULTS_INDEX.json')
            # Receipt hashes can differ when raw native returns contain measured
            # timing. Compare the full scientific identity/result projection.
            def project(value):
                if isinstance(value,list):return [project(v) for v in value]
                if isinstance(value,dict):return {k:project(v) for k,v in value.items() if k!='query_acceptance_sha256'}
                return value
            same=project(expected['method_results_index'])==project(actual['method_results_index']) and project(expected['query_progress'])==project(actual['query_progress'])
            for row in actual['method_results_index']:
                a=read(safe(saved,row['path'])/'METHOD_RESULTS.json');b=read(safe(replay,row['path'])/'METHOD_RESULTS.json')
                if {m:semantic_result(v) for m,v in a.items()}!={m:semantic_result(v) for m,v in b.items()}:same=False
            validator.check('fresh_replay_scientific_equivalence',same,model_count=actual['model_count'])
        except Exception as exc:validator.check('fresh_replay_scientific_equivalence',False,error=type(exc).__name__+': '+str(exc))
    unchanged=validator.before==tree_hashes(tree)
    validator.check('input_tree_unchanged',unchanged)
    required=('stage1d_saved_gate','stage1d_fresh_seven_method_replay','stage1d_replay_saved_gate','fresh_replay_scientific_equivalence','input_tree_unchanged')
    failures=validator.validation_failures(required)
    receipt={'schema_version':'stage1d-package-validation-v1','passed':not failures,'status':'PASS' if not failures else 'FAIL',
             'kind':kind,'commands':validator.commands,'network_disabled':True,'credentials_removed_from_child_environment':True,
             'guard':'Inherited Stage1C-R1 OFFLINE_SITE with DNS/socket/child-process controls',
             'input_tree_unchanged':unchanged,'private_real_data_replay_requested':kind=='min',
             'platform_actually_tested':platform.platform(),'python':platform.python_version(),
             'external_acceptance':'PENDING_REVIEW'}
    write(validator.out/'PACKAGE_VALIDATION.json',receipt)
    return receipt

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--tree',type=Path,required=True);p.add_argument('--batch',required=True)
    p.add_argument('--results',required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--kind',choices=('min','public'),required=True)
    a=p.parse_args()
    try:value=validate_package(a.tree,a.batch,a.results,a.output,a.kind)
    except Exception as exc:
        print(json.dumps({'status':'ERROR','error':type(exc).__name__+': '+str(exc)}));return 1
    print(json.dumps({'status':value['status'],'platform':value['platform_actually_tested']}));return 0 if value['passed'] else 1

if __name__=='__main__':raise SystemExit(main())
