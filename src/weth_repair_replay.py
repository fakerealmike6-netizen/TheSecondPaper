"""Replay preserved synthetic WETH faults against an explicitly selected module.

No network. Inputs remain read-only. Suitable for original and repaired module
comparison; a reproduced old defect is never counted as a repaired test pass.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--module',type=Path,required=True)
    parser.add_argument('--fixtures',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--expect-fixed',action='store_true')
    args=parser.parse_args();sys.dont_write_bytecode=True
    sys.path.insert(0,str(args.module.resolve().parent))
    spec=importlib.util.spec_from_file_location('weth_replay_subject',args.module)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    cases=[]
    for file in sorted(args.fixtures.glob('*.json')):
        fixture=json.loads(file.read_text())
        if 'input' not in fixture or not fixture.get('synthetic_only'):
            continue
        value=module.verify_component(**fixture['input'])
        is_control=fixture['case_id'].endswith('_control')
        correct=(not value['semantic_unit']['certified'] and
                 (is_control or value['status']=='COMPONENT_EVIDENCE_INCONSISTENT'))
        cases.append({'case_id':fixture['case_id'],'fixture_sha256':hashlib.sha256(file.read_bytes()).hexdigest(),
                      'expected_fixed_behavior_observed':correct,'result':value})
    result={'schema_version':'weth-fix04-replay-1','subject_source_sha256':hashlib.sha256(args.module.read_bytes()).hexdigest(),
            'synthetic_only':True,'network_requests':0,'expect_fixed':args.expect_fixed,'cases':cases,
            'case_count':len(cases),'fixed_expectations_passed':sum(c['expected_fixed_behavior_observed'] for c in cases)}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k!='cases'},indent=2))
    return 1 if args.expect_fixed and (not cases or any(not c['expected_fixed_behavior_observed'] for c in cases)) else 0


if __name__=='__main__':
    raise SystemExit(main())
