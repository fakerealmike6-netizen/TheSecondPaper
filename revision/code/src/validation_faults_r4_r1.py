"""R4-R1 real-CLI negative controls with explicitly synthetic child results.

The inherited R4 fault fixtures and source stay unchanged. Only fresh synthetic
fixture metadata is adapted to the R4-R1 package and required material check.
Mocked test counts in these fixtures are not production test results.
"""
from pathlib import Path
import argparse,hashlib,json,os,subprocess,sys
from validation_faults_r4 import fixture,write
from validate_review_bundle_r1 import clean_environment
from package_review_r4_r1 import freeze_payloads,write_manifest

def run_case(root,mode):
    root=Path(root); tree=fixture(root,mode,'r4');public=root/'public'
    # These are newly generated synthetic fixture maps, never an old artifact.
    for folder in (tree,public):
        for name in ('PUBLIC_LOCAL_EQUIVALENCE.json','12_FILE_HASHES.txt'):
            (folder/name).unlink()
    for folder in (tree,public):
        path=folder/'src/run_tests.py'
        path.write_text(path.read_text().replace("'tests_run':3,'passed':3", "'tests_run':660,'passed':660"),encoding='utf-8')
    (tree/'tests').mkdir();(tree/'tests/test_synthetic.py').write_text('# synthetic inventory control only\n',encoding='utf-8')
    script=tree/'src/verify_weth_shared_wire_r4_r1.py'
    script.write_text("import argparse,pathlib,json\np=argparse.ArgumentParser();p.add_argument('--tree');p.add_argument('--output');a=p.parse_args();pathlib.Path(a.output).write_text(json.dumps({'status':'PASS','network_requests':0,'synthetic_fault_control':True}))\n",encoding='utf-8')
    closure={'id':'R4-F05-R1','status':'FIXED_AND_TESTED','same_fixture_old_failure_reproduced':True,
        'required_erc20_and_global_checks_preserved':True,'original_test_count':660,'total_test_count':660,
        'unchanged_original_test_sha256':{'tests/test_synthetic.py':hashlib.sha256((tree/'tests/test_synthetic.py').read_bytes()).hexdigest()},
        'source_sha256':{'src/run_tests.py':hashlib.sha256((tree/'src/run_tests.py').read_bytes()).hexdigest()},
        'external_acceptance_status':'PENDING_REVIEW','synthetic_fault_fixture_only':True}
    write(tree/'REPAIR_CLOSURE.json',closure)
    freeze_payloads(tree,public);write_manifest(tree);write_manifest(public)
    output=root/'revision_validation'
    result=subprocess.run([sys.executable,'-B',str(Path(__file__).with_name('validate_review_bundle_r4_r1.py')),
        '--tree',str(tree),'--kind','min','--output',str(output)],capture_output=True,text=True,
        encoding='utf-8',errors='replace',env=clean_environment(os.environ),timeout=120)
    (root/'revision_process_stdout.txt').write_text(result.stdout,encoding='utf-8')
    (root/'revision_process_stderr.txt').write_text(result.stderr,encoding='utf-8')
    receipt=json.loads((output/'validation_receipt.json').read_text())
    expected='PASS' if mode=='normal_expected_gap' else 'FAIL'
    row={'mode':mode,'synthetic_child_outputs_not_science':True,'process_exit_code':result.returncode,
        'receipt_status':receipt['status'],'expected_status':expected,
        'passed':receipt['status']==expected and result.returncode==(0 if expected=='PASS' else 1)}
    write(root/'REVISION_INJECTION_RESULT.json',row);return row

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    rows=[run_case(args.output/mode,mode) for mode in ('weth_mismatch','child_nonzero','missing_result','malformed_result','normal_expected_gap')]
    result={'schema_version':'stage1b-r4-r1-fault-controls-v1','status':'PASS' if all(r['passed'] for r in rows) else 'FAIL',
            'cases':rows,'synthetic_child_outputs_not_production_science':True,'new_research_requests':0}
    write(args.output/'FAULT_INJECTION_RESULTS.json',result);print(json.dumps(result,indent=2))
    return 0 if result['status']=='PASS' else 1
if __name__=='__main__':raise SystemExit(main())
