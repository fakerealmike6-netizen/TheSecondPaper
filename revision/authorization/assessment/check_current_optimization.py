"""Read only the uploaded paused snapshot. No production imports or online calls.
Counts prove properties of saved snapshots, not chain completeness or cost savings.
"""
from pathlib import Path
from collections import Counter
import hashlib,json,csv
R=Path(__file__).resolve().parent
I=R/'input'
def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
WETH='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
queries={p.parent.name:read(p) for p in (I/'evidence/core/derived/stage1d/queries').glob('*/collection.json')}
expected={'xscam_src001':895,'lifi_src001':1,'txphish_src001':4265,'txphish_src002':3977}
obs_by_address={}
for p in (I/'evidence/roles/inputs').glob('*.json'):
    v=read(p)
    if not isinstance(v,dict):continue
    for o in v.get('observations',[]):
        obs_by_address.setdefault(o['address'],{})[o['observation_id']]=(o,p)
lookup=read(I/'evidence/roles/PERSISTED_CURRENT_ADDRESS_LOOKUP.json')['lookup']
results=[];heavy=[];wsets={}
for name in sorted(queries):
    d=queries[name];events=d['candidate_events'];states=d['states']
    assert len(events)==expected[name]
    assert len(events)==len({e['event_id'] for e in events})
    assert not d['fact_conflicts']
    roles={s['state']['address']:s['identity'] for s in states}
    stops={}
    for kind in ['FIRST_IDENTIFIED_SERVICE','PROTOCOL_BOUNDARY','DECLARED_DEPTH_BOUNDARY']:
        ss=[s for s in d['stops'] if s['reason']==kind]
        stops[kind]={'state_records':len(ss),'unique_addresses':len({s['state']['address'] for s in ss}),
                     'unique_entry_events':len({s['entry_event_id'] for s in ss if s.get('entry_event_id')})}
    we=[e for e in events if e['recipient']==WETH];wsets[name]={e['event_id'] for e in we}
    sidecar=I/f'evidence/core/derived/stage1d/queries/{name}/label_snapshot.json'
    rawlabels=read(sidecar)
    results.append({'query':name,'candidate_events':len(events),'candidate_transactions':len({e['tx_hash'] for e in events}),
       'candidate_assets':dict(Counter(e['asset'] for e in events)),'arrival_states':len(states),
       'adopted_role_addresses':len(roles),'label_sidecar_top_level_entries':len(rawlabels),
       'unqueried_or_failed_role_addresses':len({a for a,v in roles.items() if v.get('status') in ('UNQUERIED','LOOKUP_FAILED')}),
       'stops':stops,'weth_recipient_events':len(we),'weth_recipient_transactions':len({e['tx_hash'] for e in we}),
       'weth_event_ids':sorted(wsets[name]),'weth_counts_are_not_certified_components':True,
       'source_collection_path':f'evidence/core/derived/stage1d/queries/{name}/collection.json',
       'source_collection_sha256':sha(I/f'evidence/core/derived/stage1d/queries/{name}/collection.json')})
    for address,n in Counter(e['sender'] for e in events).most_common(5):
        if roles.get(address,{}).get('kind')!='UNKNOWN':continue
        rr=[]
        for oid,(o,p) in obs_by_address.get(address,{}).items():
            record=o.get('source_metadata',{}).get('raw_source_record')
            if not record:continue
            if isinstance(record,str):
                try:record=json.loads(record)
                except ValueError:continue
            rr.append({'observation_id':oid,'raw_name':record.get('name'),
                       'category':record.get('category'),'label_type':record.get('label_type'),
                       'current_classification_rule':o.get('source_metadata',{}).get('classification_rule'),
                       'source':p.relative_to(I).as_posix(),'source_sha256':sha(p)})
        heavy.append({'query':name,'address':address,'observed_candidate_out_events':n,
              'actual_role':roles[address],'lookup_status':lookup.get(address,{}).get('registry_row',{}).get('lookup_status'),
              'observations':rr,'interpretation':'Prioritize verification, never stop based only on degree, name or a persona label.'})
shared=wsets['txphish_src001']&wsets['txphish_src002']
report={'basis':'Uploaded paused local source and saved candidate graphs; NOT unpublished live code on disk and NOT GitHub current HEAD',
    'research_platform_requests':0,'production_changes':0,'source_modules_in_snapshot':len(list((I/'source_snapshot/production_src').glob('*.py'))),
    'all_modules_semantically_audited':False,'full_test_suite_run':False,
    'queries':results,'txphish_weth_unique_event_union':len(wsets['txphish_src001']|wsets['txphish_src002']),
    'txphish_weth_event_intersection':len(shared),'heavy_unknown_role_clues':heavy}
(R/'CURRENT_OPTIMIZATION_CHECKS.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
with (R/'ROLE_CLUES_TO_VERIFY.csv').open('w',encoding='utf-8-sig',newline='') as f:
    fields=['query','address','observed_candidate_out_events','lookup_status','raw_contract_names','recommendation']
    w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
    for row in heavy:
        names=[o['raw_name'] for o in row['observations'] if o['label_type']=='identifier' and o['category']=='contracts']
        w.writerow({k:row[k] for k in fields[:4]}|{'raw_contract_names':json.dumps(names,ensure_ascii=False),
          'recommendation':'VERIFY_CHAIN_AND_HISTORICAL_ROLE_THEN_REPLAY_NOT_AUTOMATIC_PRUNING' if names else 'KEEP_UNKNOWN_UNLESS_NEW_ADMISSIBLE_EVIDENCE'})
# Track exact scopes of the source review, not a full-repository claim.
segments={
 'collector.py':['Scope.__post_init__','Scope.from_policy','Scope.local_end','State.key','Collector.run'],
 'stage1d_context.py':['required_context_windows','exclude_verified_protocol_windows','necessary_context_windows','build_document'],
 'stage1c_intervals.py':['is_context','build_variant','run_interval','nested solve_all'],
 'weth_component.py':['audit_component / semantic_unit and next_state return fields'],
 'stage1d_experiments.py':['scope registration','register_document'],
 'stage1d_final_context_prepare.py':['prepare_current','point_needs','gaps_for_kinds']}
(R/'SOURCE_CHECK_SCOPE.json').write_text(json.dumps({'depth':'Targeted static path checks, not complete semantic audit',
 'files':[{'path':'source_snapshot/production_src/'+n,'sha256':sha(I/'source_snapshot/production_src'/n),'checked_interfaces':s} for n,s in segments.items()]},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print('saved',len(results),'queries;',len(heavy),'heavy UNKNOWN records; shared WETH events',len(shared))
for q in results:
 print(q['query'],q['candidate_events'],q['stops'],q['weth_recipient_events'])
