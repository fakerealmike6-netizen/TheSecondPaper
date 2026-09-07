"""Bounded offline historical F01/F02/F03 check against an immutable R3 MIN."""
import argparse
import hashlib
import json
from pathlib import Path

from context_queries_r3 import load_verified_block_headers, verify_frozen_scope
from context_ledger_r3 import normalize_rows, read_json, write_json


def audit(tree, output):
    tree=Path(tree).resolve()
    headers=load_verified_block_headers(tree)
    jobs=[]; all_rows=[]
    spec=read_json(tree/'configs/CONTEXT_REPLAY_R3.json')
    for query in spec['queries']:
        for selected in query['context_jobs']:
            freeze=tree/selected['freeze']['path']
            result=verify_frozen_scope(freeze,tree,block_headers=headers)
            result['query']=query['name']
            result['freeze_path']=selected['freeze']['path']
            result['freeze_sha256']=hashlib.sha256(freeze.read_bytes()).hexdigest()
            jobs.append(result)
            jobpath=tree/selected['job']['path']; job=read_json(jobpath)
            for offset in job['export_offsets']:
                all_rows.extend(read_json(jobpath.parent/f'page_{offset}.json')['result']['rows'])
    normalized=normalize_rows(all_rows)
    transactions=normalized['transactions']
    status={
        'schema_version':'stage1b-r4-context-historical-impact-v1',
        'scope':'Only the saved three R3 native-context jobs and their block evidence; no bulk acquisition',
        'saved_block_headers_verified':len(headers),
        'historical_jobs':len(jobs),'account_window_rows':sum(j['window_count'] for j in jobs),
        'all_block_and_date_domains_verified':all(j['date_domain_verified'] and j['block_domain_verified'] for j in jobs),
        'normalized_transactions':len(transactions),'normalized_value_flows':len(normalized['flows']),
        'present_exact_positive_values':sum(int(t['amount_raw'])>0 for t in transactions),
        'top_root_fact_conflicts':len(normalized['conflicts']),
        'historical_source_files_modified':False,'network_requests':0,
        'new_gap_collection_required':False,'queries':jobs,
        'expected_fixed_r3_scope':{'headers':43,'jobs':3,'windows':28,'value_flows':69},
        'decision':'SAVED_R3_INPUTS_REUSABLE_UNDER_VERIFIED_DUAL_DOMAIN_COVERAGE'
            if (not normalized['conflicts'] and all(j['status']=='PASS' for j in jobs)
                and len(headers)==43 and len(jobs)==3 and sum(j['window_count'] for j in jobs)==28
                and len(normalized['flows'])==69) else 'DEPENDENT_EVIDENCE_REPAIR_REQUIRED'}
    write_json(output,status)
    return status


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tree',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args(argv);result=audit(args.tree,args.output)
    print(json.dumps({k:v for k,v in result.items() if k!='queries'},indent=2))
    return 0 if (result.get('all_block_and_date_domains_verified') is True
                 and result.get('top_root_fact_conflicts') == 0
                 and result.get('decision') == 'SAVED_R3_INPUTS_REUSABLE_UNDER_VERIFIED_DUAL_DOMAIN_COVERAGE') else 1


if __name__=='__main__':
    raise SystemExit(main())
