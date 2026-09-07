"""Finite reference policy delta for actual R2 label changes; strictly offline.

Only registered queries whose existing events/targets involve changed labels
are recomputed. No reference event is a discovery neighbor or amount estimate.
"""
import argparse,json,hashlib
from pathlib import Path
from datetime import datetime,timezone
from reference_recompute import rows,recompute,metrics
from reference_core import digest,dump,write_csv,stable,truth
from dune_batch_r1 import registry_from_manifest

VERSION='stage1b-r2-finite-reference-label-delta-1.0'
FIELDS=('actor','target_identity_class','task_reference_status','change_reason','collection_90d_status','verified_numeric_depth','unknown_intermediate_addresses','witness_id')
PROJECTION=('old_relation_id','query_id','target_address','entry_event_id','actor','task_reference_status','collection_90d_status','change_reason','old_requestable_pair')
def norm(value):return stable(value) if isinstance(value,(list,dict)) else '' if value is None else str(value)
def canonical_hash(records):return hashlib.sha256(stable(records).encode('utf-8')).hexdigest()
def checked(root,path):
    p=Path(path).resolve()
    if not p.is_relative_to(Path(root).resolve()):raise ValueError('Reference input outside project')
    return p
def source(root,path,role):
    p=checked(root,path);return {'path':p.relative_to(root).as_posix(),'sha256':digest(p),'bytes':p.stat().st_size,'role':role}
def affected_queries(changed,events,members,queries,baseline):
    affected_events={e['event_id'] for e in events if e['from_address'] in changed or e['to_address'] in changed}
    incidents={r['incident_id'] for r in members if r['event_id'] in affected_events}
    affected={q['query_id'] for q in queries if q['incident_id'] in incidents}
    affected.update(r['query_id'] for r in baseline if r['target_address'] in changed)
    return affected,affected_events,incidents
def delta_rows(before,after):
    prior={r['old_relation_id']:r for r in before}
    if set(prior)!={r['old_relation_id'] for r in after}:raise ValueError('Affected reference relation identities changed')
    result=[]
    for r in after:
        old=prior[r['old_relation_id']];changes={k:{'before':norm(old.get(k)),'after':norm(r.get(k))} for k in FIELDS if norm(old.get(k))!=norm(r.get(k))}
        result.append({'old_relation_id':r['old_relation_id'],'query_id':r['query_id'],'target_address':r['target_address'],'changed':bool(changes),'field_changes':changes,'old_B':old['task_reference_status']=='VERIFIED_TASK_REFERENCE','new_B':r['task_reference_status']=='VERIFIED_TASK_REFERENCE','old_C':old['task_reference_status']=='VERIFIED_TASK_REFERENCE' and old['collection_90d_status']=='WITHIN_PER_ARRIVAL_90D','new_C':r['task_reference_status']=='VERIFIED_TASK_REFERENCE' and r['collection_90d_status']=='WITHIN_PER_ARRIVAL_90D'})
    return result
def run(work,root,baseline_work,label_manifests,output):
    root=Path(root).resolve();work=checked(root,work);baseline_work=checked(root,baseline_work);output=checked(work,output);label_manifests=[checked(work,p) for p in label_manifests]
    if not label_manifests:raise ValueError('Applied label manifest list required')
    if output.exists():
        prior=read(output/'summary.json')
        if prior['applied_manifest_sha256']!=[digest(p) for p in label_manifests]:raise ValueError('Output version exists with different label versions')
        verify(output);return prior
    baseline_path=baseline_work/'private/portable_label_reference_final_v1/diffs/reference_relations_updated.csv.gz'
    base_summary_path=baseline_work/'private/portable_label_reference_final_v1/diffs/reference_delta_summary.json'
    portable=read(baseline_work/'private/portable_label_reference_final_v1/portable_manifest.json')
    old_registry=checked(root,root/portable['source_registry_path'])
    if digest(old_registry)!=portable['source_registry_sha256']:raise ValueError('R1 baseline registry changed')
    new_registry,final_label=registry_from_manifest(label_manifests[-1],root)
    changed=set();label_sources=[];changes_rows=[]
    for path in label_manifests:
        manifest=read(path);delta=path.parent/'label_resolution_delta.csv'
        if manifest.get('old_inputs_unchanged') is not True:raise ValueError('Label source changed')
        label_sources.extend([source(root,path,'APPLIED_ACTUAL_FRONTIER_LABEL_VERSION'),source(root,delta,'ACTUAL_LABEL_RESOLUTION_DELTA')])
        for r in rows(delta):
            if truth(r['identity_or_actor_changed']):changed.add(r['address']);changes_rows.append(r|{'applied_manifest_sha256':digest(path)})
    baseline=rows(baseline_path);prior_metrics=metrics(baseline);expected=read(base_summary_path)['after']
    if prior_metrics!=expected:raise ValueError('Baseline reference metrics differ from R1 receipt')
    data=baseline_work/'private/reference_replay';names=('reference_event_slice.csv.gz','reference_query_members.csv','reference_incident_event_membership.csv.gz','B_reference_relation_recheck.csv.gz','A_original_reference_entries.csv.gz')
    all_inputs={name:rows(data/name) for name in names}
    affected,events_with_label,incidents_with_label=affected_queries(changed,all_inputs[names[0]],all_inputs[names[2]],all_inputs[names[1]],baseline)
    qs=[q for q in all_inputs[names[1]] if q['query_id'] in affected];incidents={q['incident_id'] for q in qs}
    memberships=[r for r in all_inputs[names[2]] if r['incident_id'] in incidents]
    event_ids={r['event_id'] for r in memberships}|{q['seed_event_id'] for q in qs}
    events=[r for r in all_inputs[names[0]] if r['event_id'] in event_ids]
    original_relations=[r for r in all_inputs[names[3]] if r['query_id'] in affected];source_rows={r['old_source_row'] for r in original_relations}
    original_entries=[r for r in all_inputs[names[4]] if r['old_source_row'] in source_rows]
    before=[r for r in baseline if r['query_id'] in affected];unchanged=[r for r in baseline if r['query_id'] not in affected]
    addresses={e[k] for e in events for k in ('from_address','to_address')}|{r['target_address'] for r in before}|changed
    old_ids={r['address']:r for r in rows(old_registry) if r['address'] in addresses};new_ids={r['address']:r for r in rows(new_registry) if r['address'] in addresses}
    output.mkdir(parents=True);subset=output/'affected_inputs';subset.mkdir()
    input_rows={names[0]:events,names[1]:qs,names[2]:memberships,names[3]:original_relations,names[4]:original_entries}
    for name,items in input_rows.items():write_csv(subset/name,items,list(all_inputs[name][0]))
    write_csv(subset/'old_identity_slice.csv.gz',list(old_ids.values()));write_csv(subset/'new_identity_slice.csv.gz',list(new_ids.values()))
    # Verify unchanged code under the old labels before attributing any effect.
    old_replayed,_,_=recompute(subset,old_ids) if affected else ([],[],{})
    baseline_mismatches=[r for r in delta_rows(before,old_replayed) if r['changed']]
    if baseline_mismatches:
        write_csv(output/'old_input_replay_mismatches.csv.gz',baseline_mismatches)
        raise ValueError('Old-label affected replay changed; isolate before claiming new label delta')
    after,witnesses,_=recompute(subset,new_ids) if affected else ([],[],{})
    deltas=delta_rows(before,after);after_by_id={r['old_relation_id']:r for r in after};combined=[after_by_id.get(r['old_relation_id'],r) for r in baseline]
    write_csv(output/'affected_before.csv.gz',before,list(baseline[0]));write_csv(output/'affected_after.csv.gz',after,list(baseline[0]));write_csv(output/'affected_decision_delta.csv.gz',deltas);write_csv(output/'affected_new_witnesses.csv.gz',witnesses)
    write_csv(output/'changed_label_evidence.csv',changes_rows)
    projection=[{k:r.get(k,'') for k in PROJECTION} for r in unchanged];write_csv(output/'unchanged_metric_projection.csv.gz',projection,list(PROJECTION))
    write_csv(output/'unchanged_row_hashes.csv.gz',[{'old_relation_id':r['old_relation_id'],'canonical_row_sha256':canonical_hash(r)} for r in unchanged])
    sources=label_sources+[source(root,baseline_path,'R1_REFERENCE_POLICY_BASELINE'),source(root,base_summary_path,'R1_REFERENCE_COUNTS_RECEIPT'),source(root,old_registry,'R1_LABEL_REGISTRY'),source(root,new_registry,'LATEST_R2_LABEL_REGISTRY')]+[source(root,data/name,'UNCHANGED_REGISTERED_REFERENCE_FACTS') for name in names]
    report={'version':VERSION,'generated_at_utc':datetime.now(timezone.utc).isoformat(),'applied_manifest_sha256':[digest(p) for p in label_manifests],'changed_label_addresses':sorted(changed),'affected_query_ids':sorted(affected),'affected_query_count':len(affected),'affected_incident_ids':sorted(incidents_with_label),'registered_events_touching_changed_labels':len(events_with_label),'affected_registered_event_rows':len(events),'affected_reference_rows':len(before),'unchanged_reference_rows':len(unchanged),'full_reference_row_count':len(baseline),'old_label_replay_mismatches':len(baseline_mismatches),'changed_reference_rows':sum(r['changed'] for r in deltas),'before':prior_metrics,'after':metrics(combined),'unchanged_full_rows_canonical_sha256':canonical_hash(unchanged),'baseline_full_rows_canonical_sha256':canonical_hash(baseline),'after_full_rows_canonical_sha256':canonical_hash(combined),'source_inputs':sources,'network_calls':0,'new_chain_data_rows':0,'denominator_finalized':False,'all_sample_or_stage1a_reexecution':False,'scope':'Finite registered reference policy views affected by new labels only; identical unchanged remainder inherited by hash and minimal metric projection','unchanged_remainder_not_recomputed':True,'reference_data_used_as_provider_neighbors':False,'full_baseline_reference_library_copied':False}
    dump(output/'summary.json',report)
    payload=[{'path':p.relative_to(output).as_posix(),'sha256':digest(p),'bytes':p.stat().st_size} for p in sorted(output.rglob('*')) if p.is_file()]
    dump(output/'payload_manifest.json',{'version':VERSION,'files':payload,'excluded_from_self_hash':['payload_manifest.json','verification.json']})
    verified=verify(output);dump(output/'verification.json',verified);return report
def read(path):return json.loads(Path(path).read_text(encoding='utf-8'))
def verify(output):
    """Rerun only included affected slices; original library is unnecessary."""
    output=Path(output);manifest=read(output/'payload_manifest.json')
    for item in manifest['files']:
        path=checked(output,output/item['path'])
        if digest(path)!=item['sha256']:raise ValueError('Reference delta payload hash mismatch')
    report=read(output/'summary.json');subset=output/'affected_inputs';before=rows(output/'affected_before.csv.gz');after=rows(output/'affected_after.csv.gz');projection=rows(output/'unchanged_metric_projection.csv.gz')
    for phase,saved in [('old',before),('new',after)]:
        identities={r['address']:r for r in rows(subset/(phase+'_identity_slice.csv.gz'))}
        replayed,_,_=recompute(subset,identities) if report['affected_query_count'] else ([],[],{})
        if any(r['changed'] for r in delta_rows(saved,replayed)):raise ValueError('Portable affected '+phase+' reference replay differs')
    if metrics(projection+before)!=report['before'] or metrics(projection+after)!=report['after']:raise ValueError('Projection+affected reference metrics differ')
    return {'passed':True,'verified_payload_files':len(manifest['files']),'affected_queries_replayed_under_each_label_version':report['affected_query_count'],'old_and_new_affected_reference_rows':len(before)+len(after),'unchanged_projection_rows':len(projection),'network_calls':0,'requires_full_baseline_library':False,'denominator_finalized':False}
def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=('run','verify'));p.add_argument('--work',type=Path);p.add_argument('--project-root',type=Path);p.add_argument('--baseline-work',type=Path);p.add_argument('--label-manifests',type=Path,nargs='+');p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    result=run(a.work,a.project_root,a.baseline_work,a.label_manifests,a.output) if a.action=='run' else verify(a.output)
    print(json.dumps(result,indent=2))
if __name__=='__main__':main()
