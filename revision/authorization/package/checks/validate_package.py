"""Offline integrity/consistency check for the execution package, not project validation."""
from __future__ import annotations
import argparse,hashlib,json,zipfile
from pathlib import Path,PurePosixPath
from decimal import Decimal
from datetime import datetime

def h(b: bytes) -> str: return hashlib.sha256(b).hexdigest()
def pairs(text: str):
    for line in text.splitlines():
        if not line.strip() or line.startswith('#'): continue
        digest, name=line.split(None,1)
        name=name.strip().lstrip('*');p=PurePosixPath(name)
        if len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest) or p.is_absolute() or '..' in p.parts or '\\' in name:
            raise ValueError('Unsafe or invalid manifest entry')
        yield digest,name

def check(root: Path) -> dict:
    root=root.resolve(); manifest=root/'PACKAGE_FILE_HASHES.txt'; count=0; seen=set()
    for digest,name in pairs(manifest.read_text(encoding='utf-8')):
        if name.casefold() in seen: raise ValueError('Duplicate manifest name')
        seen.add(name.casefold()); file=(root/name).resolve()
        if not file.is_relative_to(root) or not file.is_file() or h(file.read_bytes())!=digest:
            raise ValueError('Input hash mismatch: '+name)
        count+=1
    p=json.loads((root/'config/ACTIVE_POLICY.json').read_text(encoding='utf-8'))
    qs=json.loads((root/'config/QUERY_SCOPES.json').read_text(encoding='utf-8'))['queries']
    old=json.loads((root/'inputs/ORIGINAL_QUERY_SCOPES_HISTORICAL.json').read_text(encoding='utf-8'))['queries']
    old={q['name']:q for q in old}; expected={'xscam_src001':4818570,'lifi_src001':None,'txphish_src001':1409808,'txphish_src002':1409808}
    assert len(qs)==4 and {q['name'] for q in qs}==set(expected)
    assert p['windows']['values']==expected
    for q in qs:
        a=old[q['name']]
        for key in ('query_id','seed_event_id','seed_tx_hash','seed_asset','seed_amount_raw','seed_from','seed_to','start_time_utc','end_time_utc','max_acquisition_depth'):
            assert q[key]==a[key],key
        assert q['primary_local_window_seconds']==expected[q['name']]
        assert q['primary_window_mode']==('REFERENCE_FULL' if q['name']=='lifi_src001' else 'QUERY_ARRIVAL_WINDOW_SECONDS_V1')
        assert q['fallback_window_mode'] is None and q['fallback_local_window_seconds'] is None
        if q['name']!='lifi_src001':
            assert isinstance(q['primary_local_window_seconds'],int) and q['primary_local_window_seconds']>0
            end=datetime.fromisoformat(q['end_time_utc'].replace('Z','+00:00'));start=datetime.fromisoformat(q['start_time_utc'].replace('Z','+00:00'))
            assert q['primary_local_window_seconds']<=(end-start).total_seconds()
    r=p['resource_policy']; baseline=json.loads((root/'inputs/RECOVERY_RESOURCE_BASELINE_EXTRACT.json').read_text(encoding='utf-8'))['fields']
    numeric_keys={
        'dune':['execution_cap_credits','shared_cumulative_risk_cap_credits','warning_credits'],
        'bigquery':['cumulative_scan_risk_bytes','per_job_maximum_bytes_billed','cumulative_warning_bytes','monthly_safety_room_bytes'],
        'rpc':['cumulative_individual_operation_cap','operation_warning','alchemy_cumulative_cu_cap','alchemy_warning_cu'],
        'labels':['batch_distinct_chain_address_cap','warning'],
        'resources':['per_query_online_soft_seconds','per_query_online_hard_seconds_all_modes','candidate_events_per_query_soft','candidate_events_per_query_hard','expanded_addresses_per_query_soft','expanded_addresses_per_query_hard','context_events_per_query_soft','context_events_per_query_hard','new_raw_logical_unique_bytes_hard','new_raw_logical_unique_warning_bytes','workspace_min_free_disk_bytes']}
    n=0
    for section,keys in numeric_keys.items():
        for key in keys: assert r[section][key]==baseline[section][key],(section,key);n+=1
    assert r['dune']['execution_cap_credits']=='50' and r['dune']['shared_cumulative_risk_cap_credits']=='2000'
    assert r['rpc']['cumulative_individual_operation_cap']==100000 and r['rpc']['alchemy_cumulative_cu_cap']==5000000
    assert r['bigquery']['cumulative_scan_risk_bytes']==800*2**30 and r['bigquery']['per_job_maximum_bytes_billed']==100*2**30
    assert r['resources']['per_query_online_hard_seconds_all_modes']==12*3600
    assert r['resources']['new_raw_logical_unique_bytes_hard']==8*2**30
    assert p['retry_clarifications']['no_new_retry_grant'] is True
    assert p['retry_clarifications']['existing_recovery_grant_id']==r['retry']['one_time_recovery_id']
    assert p['checkpoint']=='CHECKPOINT_1D_REACHED' and p['external_review']=='PENDING_REVIEW'
    assert len(p['method_contract']['methods'])==7 and p['method_contract']['warmup']==1 and p['method_contract']['timed_repetitions']==5
    s=p['last_paused_snapshot'];assert sum(Decimal(s[k]) for k in ['dune_known_fee_lower','dune_export_risk_upper','dune_other_unknown'])==Decimal(s['dune_total_risk'])
    assert Decimal('2000')-Decimal(s['dune_total_risk'])==Decimal(s['dune_remaining'])
    source_name='inputs/Stage1D_Paused_Optimization_Assessment_2026-09-09.zip';inner_count=0
    with zipfile.ZipFile(root/source_name) as z:
        if z.testzip() is not None: raise ValueError('Inner ZIP CRC failed')
        for digest,name in pairs(z.read('FILE_HASHES.txt').decode()):
            assert h(z.read(name))==digest;inner_count+=1
    report={'status':'PASS','outer_manifest_entries':count,'inner_manifest_entries':inner_count,'numeric_resource_fields_unchanged':n,'queries':4,'explicit_W_seconds':expected,'original_seeds_global_times_depths_unchanged':True,'fund_age_constraint_added':False,'new_retry_grant':False,'live_provider_calls':0,'production_tests_executed':False}
    return report
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);a=parser.parse_args()
    try: print(json.dumps(check(a.root),ensure_ascii=False,indent=2))
    except Exception as exc:
        print(json.dumps({'status':'FAIL','error_type':type(exc).__name__,'message':str(exc)},ensure_ascii=False));raise SystemExit(1)
