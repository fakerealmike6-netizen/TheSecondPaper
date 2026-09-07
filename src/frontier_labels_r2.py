"""Offline R2 actual-frontier label batches. No networking or budget writer.

Reuses R1 source schemas/classification and successful static-predicate SQL.
The R1+R2 unique cohort cap is40; old three-job and cap5 controls do not apply.
"""
import argparse,hashlib,json,re
from collections import defaultdict,Counter
from pathlib import Path
from datetime import datetime,timezone
from frontier_labels_r1 import build_sql,parse_result_rows,observations_from_parsed,read,TABLES
from labels_policy import full_address,resolve_address
from reference_core import digest,dump,write_csv,truth
from reference_recompute import rows
from dune_batch_r1 import registry_from_manifest,verified_raw
from page_contract import initial_progress,validate_page

VERSION='stage1b-r2-frontier-labels-1.0'
def now():return datetime.now(timezone.utc).isoformat()
def inside(root,path):
    p=Path(path).resolve()
    if not p.is_relative_to(Path(root).resolve()):raise ValueError('Input outside project')
    return p
def record(root,path,role):
    p=inside(root,path);return {'path':p.relative_to(root).as_posix(),'sha256':digest(p),'bytes':p.stat().st_size,'role':role}
def optimized_sql(addresses,schema):
    sql=build_sql(addresses,schema)
    original="  INNER JOIN frontier f ON s.address = f.address\n  WHERE s.blockchain = 'ethereum'"
    static="  WHERE s.blockchain = 'ethereum'\n    AND s.address IN (\n      "+',\n      '.join(sorted(addresses))+'\n    )'
    if sql.count(original)!=4:raise ValueError('Inherited SQL shape changed')
    return '-- Reuse successful static address-predicate shape for R2 actual new frontiers.\n'+sql.replace(original,static).replace('Stage1B-R1','Stage1B-R2')
def select_addresses(frontiers,registry,prior,cap=40):
    prior={full_address(a) for a in prior};actual={full_address(a) for a in frontiers}
    reusable={a for a,row in registry.items() if not (row.get('identity_class')=='UNKNOWN' and str(row.get('lookup_status','')).startswith('UNQUERIED'))}
    eligible=sorted(actual-reusable-prior)
    selected=eligible[:max(0,cap-len(prior))]
    review=[{'address':a,'eligible':a in selected,'reason':'NEW_ACTUAL_FRONTIER' if a in selected else 'PRIOR_FROZEN_OR_SUCCESS_REUSED' if a in prior else 'LOCAL_REGISTRY_REUSED' if a in reusable else 'CUMULATIVE40_LABEL_GAP_UNKNOWN'} for a in sorted(actual)]
    return selected,review
def unqueried_arrivals(collection,max_depth):
    """Every actual arrival needs label opportunity, including depth-boundary states."""
    selected=[]
    for index,item in enumerate(collection['states']):
        state=item['state']
        if not 0<=state['depth']<=max_depth:raise ValueError('Collection arrival depth outside fixed probe scope')
        if item['identity'].get('status')=='UNQUERIED':selected.append((index,state))
    return selected
def observation_sources(root,manifest_path,manifest):
    if manifest.get('observation_sources'):
        sources=manifest['observation_sources']
    else:
        base=inside(root,root/manifest['label_snapshot_path'])
        source=inside(root,root/manifest['observations_source_path']) if manifest.get('observations_source_path') else base/'label_observations.csv.gz'
        sources=[{'path':source.relative_to(root).as_posix(),'sha256':manifest['observations_sha256']}]
    for source in sources:
        p=inside(root,root/source['path'])
        if digest(p)!=source['sha256']:raise ValueError('Label observation source changed')
    return sources
def verify_frozen(path):
    path=Path(path);m=read(path)
    if m.get('schema_version')!=VERSION:raise ValueError('Unknown R2 label freeze version')
    for item in m['frozen_files']:
        p=inside(path.parent,path.parent/item['name'])
        if digest(p)!=item['sha256']:raise ValueError('Frozen label input changed')
    if digest(path.parent/'query.sql')!=m['sql_sha256']:raise ValueError('Label SQL hash differs')
    if not m['external_addresses'] or len(m['external_addresses'])!=len(set(m['external_addresses'])):raise ValueError('Invalid frozen cohort')
    return m
def freeze(work,root,replay,label_manifest,baseline_work,batch='labels_001'):
    root=Path(root).resolve();work=inside(root,work);replay=inside(root,replay);label_manifest=inside(root,label_manifest);baseline=inside(root,baseline_work)
    if not re.fullmatch(r'labels_[0-9]{3}',batch):raise ValueError('Safe batch name required')
    dest=work/'private/frozen_batches'/batch
    if (dest/'freeze_manifest.json').exists():return verify_frozen(dest/'freeze_manifest.json')
    if dest.exists():raise ValueError('Incomplete prior freeze; do not overwrite')
    registry_path,base=registry_from_manifest(label_manifest,root);registry={r['address']:r for r in rows(registry_path)}
    policy=read(work/'configs/STAGE1B_R2_POLICY.json');pilots={p['name']:p for p in policy['query_pilots']}
    frontier_file=replay/'next_actual_frontier.json';frontier=read(frontier_file);active=defaultdict(list)
    for item in frontier:
        p=pilots.get(item['name'])
        if p is None or item['query_id']!=p['query_id'] or not 0<=item['depth']<=p['max_acquisition_depth']:raise ValueError('Frontier is outside the two fixed probes')
        active[full_address(item['address'])].append(item)
    collection_inputs=[];occurrences=defaultdict(list)
    for name in pilots:
        path=replay/name/'collection.json';c=read(path);collection_inputs.append(record(root,path,'ACTUAL_COLLECTION_STATES'))
        if c['query_id']!=pilots[name]['query_id']:raise ValueError('Collection query mismatch')
        for index,state in unqueried_arrivals(c,pilots[name]['max_acquisition_depth']):
            address=full_address(state['address'])
            occurrences[address].append({'pilot':name,'query_id':c['query_id'],'state_index':index,'depth':state['depth'],'arrival_event_id':state['arrival']['event_id'],'source_sha256':digest(path)})
    rules_path=baseline/'derived/frontier_labels/batch_01_optimized_v1/source_rules.json';old_queue=baseline/'derived/frontier_labels/batch_01_optimized_v1/queue.json'
    prior=set(read(old_queue)['external_addresses']);prior_files=[record(root,old_queue,'R1_SUCCESSFUL_NINE_ADDRESS_COHORT')]
    for p in sorted((work/'private/frozen_batches').glob('labels_*/freeze_manifest.json')):
        previous=verify_frozen(p);prior.update(previous['external_addresses']);prior_files.append(record(root,p,'R2_PREVIOUSLY_FROZEN_COHORT_INCLUDING_UNCERTAIN'))
    selected,review=select_addresses(occurrences,registry,prior)
    if not selected:raise ValueError('No new eligible actual frontier labels within cumulative40')
    rules=read(rules_path);rules['version']=VERSION;rules['future_batches']='Same frozen classifier; actual new frontier only; R1+R2 max40 distinct addresses; no fixed number of jobs'
    sources=observation_sources(root,label_manifest,base)
    inputs=collection_inputs+[record(root,frontier_file,'ACTUAL_PENDING_FRONTIER'),record(root,label_manifest,'BASE_LABEL_MANIFEST'),record(root,registry_path,'BASE_REGISTRY'),record(root,rules_path,'INHERITED_CLASSIFIER_AND_VERIFIED_SCHEMA')]+prior_files
    sql=optimized_sql(selected,rules['table_schema']);dest.mkdir(parents=True)
    dump(dest/'source_rules.json',rules);dump(dest/'base_label_manifest.json',base)
    queue={'version':VERSION,'batch':batch,'external_addresses':selected,'actual_state_occurrences':{a:occurrences[a] for a in selected},'selection_review':review,'prior_unique_addresses':len(prior),'cumulative_unique_addresses':len(prior|set(selected)),'address_count':len(selected),'source_opportunities':4*len(selected),'frozen_at_utc':now(),'network_calls':0,'no_match_is_not_nonservice':True}
    dump(dest/'queue.json',queue);(dest/'query.sql').write_text(sql,encoding='utf-8',newline='\n')
    manifest={'schema_version':VERSION,'batch':batch,'external_addresses':selected,'sql_sha256':digest(dest/'query.sql'),'source_inputs':inputs,'base_registry':record(root,registry_path,'BASE_REGISTRY'),'base_label_manifest':record(root,label_manifest,'BASE_LABEL_MANIFEST'),'observation_sources':sources,'queue_sha256':digest(dest/'queue.json'),'cumulative_unique_addresses':queue['cumulative_unique_addresses'],'export_plan':{'columns':['address']+[k+s for k in TABLES for s in ('_json','_count')],'expected_rows':len(selected),'all_source_matches_required':True,'all_pages_required':True,'arrays_never_truncated':True},'phase':'FROZEN_NOT_SUBMITTED','network_calls':0,'frozen_files':[{'name':p.name,'sha256':digest(p),'bytes':p.stat().st_size} for p in sorted(dest.iterdir()) if p.is_file()]}
    dump(dest/'freeze_manifest.json',manifest);return verify_frozen(dest/'freeze_manifest.json')
def saved_rows(work,freeze_path,jobdir):
    work=Path(work);freeze_path=Path(freeze_path);jobdir=Path(jobdir);manifest=verify_frozen(freeze_path);job=read(jobdir/'job.json')
    if job.get('kind')!='frontier_labels' or job.get('sql_sha256')!=manifest['sql_sha256'] or digest(jobdir/'query.sql')!=manifest['sql_sha256'] or job.get('scope_freeze_sha256')!=digest(freeze_path):raise ValueError('Job not bound to this frozen label cohort')
    status=job.get('status_response') or {}
    if job.get('state')!='QUERY_STATE_COMPLETED' or status.get('state')!='QUERY_STATE_COMPLETED' or status.get('execution_id')!=job.get('execution_id'):raise ValueError('Label execution incomplete or wrong identity')
    if verified_raw(job['status_receipt'],work)!=status:raise ValueError('Label status raw differs')
    offsets=job.get('export_offsets',[])
    if len(offsets)!=job.get('export_requests') or len(offsets)!=len(set(offsets)):raise ValueError('Invalid page history')
    progress=initial_progress(status['result_metadata']['total_row_count']);all_rows=[];evidence=[]
    for offset in offsets:
        page=read(jobdir/f'page_{offset}.json');receipt=read(jobdir/f'page_{offset}_receipt.json');params=receipt['parameters']
        if verified_raw(receipt,work)!=page:raise ValueError('Label page raw differs')
        progress=validate_page(page,execution_id=job['execution_id'],offset=offset,limit=params['limit'],progress=progress,status_metadata=status['result_metadata'],receipt=receipt,parameters=params)
        all_rows+=page['result']['rows'];evidence.append({'offset':offset,'page_sha256':digest(jobdir/f'page_{offset}.json'),'receipt_sha256':digest(jobdir/f'page_{offset}_receipt.json'),'raw_sha256':receipt['sha256'],'raw_path':receipt['raw_path'],'rows':len(page['result']['rows'])})
    if not progress['complete']:raise ValueError('Label full result not exported')
    return parse_result_rows(all_rows,manifest['external_addresses']),job,evidence
def apply(work,root,freeze_path,jobdir):
    root=Path(root).resolve();work=inside(root,work);freeze_path=inside(work,freeze_path);jobdir=inside(work,jobdir);manifest=verify_frozen(freeze_path);batch=manifest['batch'];dest=work/'derived/frontier_labels'/batch
    if (dest/'apply_manifest.json').exists():
        old=read(dest/'apply_manifest.json')
        if old['job_sha256']!=digest(jobdir/'job.json'):raise ValueError('Applied job mutated')
        return old
    parsed,job,pages=saved_rows(work,freeze_path,jobdir);rules=read(freeze_path.parent/'source_rules.json')
    registry_path=inside(root,root/manifest['base_registry']['path'])
    if digest(registry_path)!=manifest['base_registry']['sha256']:raise ValueError('Base registry changed')
    oldreg=rows(registry_path);index={r['address']:r for r in oldreg};groups=defaultdict(list)
    for source in manifest['observation_sources']:
        p=inside(root,root/source['path'])
        if digest(p)!=source['sha256']:raise ValueError('Base observation source changed')
        for row in rows(p):
            if row['address'] in parsed:groups[row['address']].append(row)
    dest.mkdir(parents=True);evidence_path=dest/'applied_page_evidence.json';dump(evidence_path,{'execution_id':job['execution_id'],'job_path':jobdir.relative_to(root).as_posix(),'pages':pages})
    acquired=job['status_response'].get('execution_ended_at') or job['status_receipt'].get('utc')
    if not acquired:raise ValueError('Acquisition timestamp absent')
    added,opportunities=observations_from_parsed(parsed,rules,job['execution_id'],acquired,evidence_path.relative_to(root).as_posix(),digest(evidence_path))
    for row in added:groups[row['address']].append(row)
    version=work.name+'_'+batch;changes=[]
    for address in sorted(parsed):
        before=index.get(address,{'chain_id':'1','address':address,'identity_class':'UNKNOWN','actor':''});after=resolve_address(groups[address],before)
        if any(truth(o.get('record_conflict')) for o in groups[address]):after['preserved_conflict']=True
        after.update(acquisition_scope=rules['acquisition_scope'],lookup_status='COMPLETED_FOUR_TABLE_OPPORTUNITY',last_frontier_label_execution_id=job['execution_id'],label_snapshot_version=version,distinct_label_facts=len({o['semantic_label_key'] for o in groups[address]}),actor_keys=sorted({o.get('actor_key') for o in groups[address] if o.get('actor_key')}))
        index[address]=after;changes.append({'address':address,'old_identity_class':before.get('identity_class'),'new_identity_class':after['identity_class'],'old_actor':before.get('actor') or '','new_actor':after.get('actor') or '','identity_or_actor_changed':(before.get('identity_class'),before.get('actor') or '')!=(after['identity_class'],after.get('actor') or ''),'lookup_status':after['lookup_status'],'resolution_rule':after['resolution_rule'],'preserved_conflict':after['preserved_conflict'],'new_observations':sum(o['address']==address for o in added)})
    snapshot=work/'derived/label_snapshots'/version
    if snapshot.exists():raise ValueError('Snapshot exists; no overwrite')
    snapshot.mkdir(parents=True);columns=list(oldreg[0])
    for r in index.values():
        for k in r:
            if k not in columns:columns.append(k)
    write_csv(snapshot/'address_registry.csv.gz',[index[k] for k in sorted(index)],columns);write_csv(snapshot/'new_observations.csv.gz',added);write_csv(dest/'source_opportunities.csv',opportunities);write_csv(dest/'label_resolution_delta.csv',changes)
    sources=manifest['observation_sources']+[record(root,snapshot/'new_observations.csv.gz','R2_NEW_LABEL_OBSERVATIONS')]
    result={'version':VERSION,'label_snapshot_version':version,'label_snapshot_path':snapshot.relative_to(root).as_posix(),'registry_sha256':digest(snapshot/'address_registry.csv.gz'),'registry_rows':len(index),'observation_sources':sources,'observation_storage':'Immutable inherited sources plus this version delta; no full historical observation copy','base_registry_sha256':manifest['base_registry']['sha256'],'base_label_manifest_sha256':manifest['base_label_manifest']['sha256'],'frontier_address_count':len(parsed),'source_opportunities':len(opportunities),'new_observation_rows':len(added),'identity_changes':sum(c['identity_or_actor_changed'] for c in changes),'match_status_counts':dict(Counter(o['status'] for o in opportunities)),'cumulative_unique_addresses':manifest['cumulative_unique_addresses'],'execution_id':job['execution_id'],'sql_sha256':manifest['sql_sha256'],'job_sha256':digest(jobdir/'job.json'),'freeze_sha256':digest(freeze_path),'scope':'LABEL_UPDATE_SAME_RAW; replay both fixed probes with this manifest before continuation','old_inputs_unchanged':digest(registry_path)==manifest['base_registry']['sha256'],'network_calls':0,'reference_delta_status':'ROOT_FINITE_REFERENCE_REPLAY_PENDING'}
    dump(snapshot/'source_manifest.json',result);dump(snapshot/'label_policy.json',{'version':VERSION,'source_rules_sha256':digest(freeze_path.parent/'source_rules.json'),'unknown_intermediate_blocks_chain':False,'inherited_classifier_unchanged':True});dump(dest/'apply_manifest.json',result);return result
def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=('freeze','apply','verify'));p.add_argument('--work',type=Path,required=True);p.add_argument('--project-root',type=Path,required=True);p.add_argument('--replay',type=Path);p.add_argument('--label-manifest',type=Path);p.add_argument('--baseline-work',type=Path);p.add_argument('--batch',default='labels_001');p.add_argument('--freeze',type=Path);p.add_argument('--job-dir',type=Path);a=p.parse_args()
    result=freeze(a.work,a.project_root,a.replay,a.label_manifest,a.baseline_work,a.batch) if a.action=='freeze' else apply(a.work,a.project_root,a.freeze,a.job_dir) if a.action=='apply' else verify_frozen(a.freeze)
    print(json.dumps(result,indent=2))
if __name__=='__main__':main()
