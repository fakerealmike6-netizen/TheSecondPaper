"""Offline F09 end-to-end fault injection with explicitly synthetic child outputs.

Exercises the real validator CLI, subprocess dispatch, result parsing and exit
code. Synthetic receipt counts here are controls, never production test results.
"""
from pathlib import Path
import argparse, hashlib, json, shutil, subprocess, sys
from validate_review_bundle_r1 import clean_environment


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2)+'\n', encoding='utf-8')


def fixture(root, mode, stage='r3'):
    if stage == 'r4': from package_review_r4 import freeze_payloads, write_manifest
    elif stage == 'r3': from package_review_r3 import freeze_payloads, write_manifest
    else: raise ValueError('Explicit validator stage required')
    public = root/'public'
    (public/'src').mkdir(parents=True)
    (public/'fixtures/controlled').mkdir(parents=True)
    (public/'src/run_tests.py').write_text(
        "import argparse,json,pathlib\np=argparse.ArgumentParser();p.add_argument('--output');a=p.parse_args();pathlib.Path(a.output).write_text(json.dumps({'success':True,'tests_run':3,'passed':3,'failed':0,'errors':0,'skipped':0,'network_attempts':[]}))\n", encoding='utf-8')
    (public/'src/lp_run.py').write_text(
        "import argparse,json,pathlib\np=argparse.ArgumentParser();p.add_argument('--fixtures');p.add_argument('--output');a=p.parse_args();o=pathlib.Path(a.output);o.mkdir(parents=True);(o/'lp_verification_results.json').write_text(json.dumps({'scenario_count':12,'objective_comparisons':32,'all_passed':True}))\n", encoding='utf-8')
    private = root/'min'
    shutil.copytree(public, private)
    config = {'schema_version':'stage1b-r3-private-validation-v1','context_manifest':'configs/context.json',
              'r2_baseline':[], 'expected_context':[],
              'weth':{'manifest':'synthetic/INPUT_MANIFEST.json','expected_result':'synthetic/weth.json'}}
    for name in ('atomic_simple_transfer','harmony_high_branch'):
        config['r2_baseline'].append({'name':name,'graph':f'synthetic/{name}/graph.json','expected_lp':f'synthetic/{name}/lp.json'})
        config['expected_context'].append({'name':name,'ledger':f'synthetic/{name}/ledger.json','amounts':f'synthetic/{name}/amounts.json'})
        for suffix in ('graph','lp','ledger','amounts'):write(private/f'synthetic/{name}/{suffix}.json',{})
    write(private/'configs/context.json',{})
    write(private/'synthetic/INPUT_MANIFEST.json',{})
    write(private/'synthetic/weth.json',{})
    if stage == 'r4':
        config['schema_version']='stage1b-r4-private-validation-v1'
        config['weth']['manifest']='synthetic/R4_INPUT_MANIFEST.json'
        write(private/'synthetic/R4_INPUT_MANIFEST.json',{})
        config['r3_baseline']=[dict(x) for x in config['expected_context']]
        matrix={'findings':[{'id':'F%02d'%i,'status':'FIXED_AND_TESTED','code':['src/run_tests.py'],'regression_tests':['SYNTHETIC_FAULT_FIXTURE'],'callers':['SYNTHETIC_FAULT_FIXTURE']} for i in range(1,10)],
                'E01':{'status':'IMPLEMENTED_AND_OFFLINE_TESTED'},'external_acceptance_status':'PENDING_REVIEW',
                'source_sha256':{'src/run_tests.py':hashlib.sha256((private/'src/run_tests.py').read_bytes()).hexdigest()}}
        write(private/'03_REPAIR_CLOSURE_MATRIX.json',matrix)
        for filename,data in (
            ('independent_evidence_audit_r4.py',{'status':'PASS','network_calls':0,'queries':[{},{}]}),
            ('independent_amount_audit_r4.py',{'status':'PASS','network_requests':0,'intervals_verified':48,'endpoint_witnesses_verified':100}),
            ('audit_context_contracts_r4.py',{'all_block_and_date_domains_verified':True,'top_root_fact_conflicts':0,'network_requests':0})):
            (private/'src'/filename).write_text("import argparse,pathlib\np=argparse.ArgumentParser();p.add_argument('--tree');p.add_argument('--output');a=p.parse_args();pathlib.Path(a.output).write_text("+repr(json.dumps(data))+")\n",encoding='utf-8')
    write(private/('configs/PRIVATE_VALIDATION_'+stage.upper()+'.json'),config)
    result={'passed':True,'context_comparisons':[{},{}],'r2_baseline_comparisons':[{},{}],'network_requests':0,
            'weth_comparison':{'passed':mode=='normal_expected_gap','status':'OBSERVED_LOCAL_DEPOSIT_PATTERN_WITH_GAPS',
                               'real_component_certified':False,'real_conversion_enabled':False}}
    if stage == 'r4':result['r3_baseline_comparisons']=[{},{}]
    child = "import argparse,json,pathlib,sys\np=argparse.ArgumentParser();p.add_argument('--tree');p.add_argument('--config');p.add_argument('--output');a=p.parse_args();o=pathlib.Path(a.output);o.mkdir(parents=True)\n"
    if mode == 'child_nonzero':child += 'raise SystemExit(7)\n'
    elif mode == 'missing_result':child += 'pass\n'
    elif mode == 'malformed_result':child += "(o/'R3_REPLAY_COMPARISON.json').write_text('{invalid')\n"
    else:child += "(o/'R3_REPLAY_COMPARISON.json').write_text("+repr(json.dumps(result))+")\n"
    child=child.replace('R3_REPLAY_COMPARISON',stage.upper()+'_REPLAY_COMPARISON')
    (private/('src/review_replay_'+stage+'.py')).write_text(child, encoding='utf-8')
    freeze_payloads(private, public)
    write_manifest(private);write_manifest(public)
    return private


def run_case(root, mode, stage='r3'):
    tree=fixture(root,mode,stage); output=root/'validation'
    validator=Path(__file__).with_name('validate_review_bundle_'+stage+'.py')
    result=subprocess.run([sys.executable,'-B',str(validator),'--tree',str(tree),'--kind','min','--output',str(output)],
                          capture_output=True,text=True,encoding='utf-8',errors='replace',env=clean_environment(__import__('os').environ),timeout=120)
    (root/'process_stdout.txt').write_text(result.stdout,encoding='utf-8')
    (root/'process_stderr.txt').write_text(result.stderr,encoding='utf-8')
    receipt=json.loads((output/'validation_receipt.json').read_text(encoding='utf-8'))
    expected='PASS' if mode=='normal_expected_gap' else 'FAIL'
    row={'mode':mode,'validator_stage':stage,'synthetic_fault_only':True,'process_exit_code':result.returncode,
         'receipt_status':receipt['status'],'expected_status':expected,
         'passed':receipt['status']==expected and (result.returncode==0)==(expected=='PASS'),
         'weth_check':next((c for c in receipt['commands'] if c['name']==stage+'_weth_actual_input_replay_receipt'),None)}
    write(root/'INJECTION_RESULT.json',row)
    return row


def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--output',type=Path,required=True);ap.add_argument('--stage',choices=('r3','r4'),default='r4');args=ap.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    cases=[run_case(args.output/mode,mode,args.stage) for mode in ('weth_mismatch','child_nonzero','missing_result','malformed_result','normal_expected_gap')]
    result={'schema_version':'stage1b-r4-validator-faults-v1','status':'PASS' if all(c['passed'] for c in cases) else 'FAIL',
            'synthetic_child_outputs_not_production_science':True,'cases':cases,'real_provider_requests':0}
    write(args.output/'FAULT_INJECTION_RESULTS.json',result)
    print(json.dumps(result,indent=2))
    return 0 if result['status']=='PASS' else 1


if __name__=='__main__':raise SystemExit(main())
