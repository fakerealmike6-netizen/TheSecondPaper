"""Deterministic one-off lookup queue; all data supplied by caller."""
import datetime as dt
import json
from collections import defaultdict, Counter
from labels_policy import historical_successes, VERSION
from reference_core import truth, utc, dump, digest, write_csv

def freeze_queue(work, results, old, policy, histories):
    path=work/'derived/label_lookup_queue.csv'
    if path.exists():
        manifest=json.loads((work/'manifests/label_lookup_queue_freeze.json').read_text())
        assert digest(path)==manifest['queue_sha256']; return manifest
    success=historical_successes(histories)
    known_b={r['query_id'] for r in results if r['task_reference_status']=='VERIFIED_TASK_REFERENCE'}
    pilots={q['query_id'] for q in policy['query_pilots']}
    by=defaultdict(list)
    for r in results:
        if r['target_identity_class']=='UNKNOWN' and r['change_reason']=='TARGET_SERVICE_IDENTITY_UNRESOLVED': by[r['target_address']].append(r)
    pool=[]
    for address,rr in sorted(by.items()):
        structural=[r for r in rr if truth(r['policy_chain_with_unknowns_observed'])]
        qids=sorted({r['query_id'] for r in structural}); incidents=sorted({r['incident_id'] for r in structural})
        pilot=bool(pilots & set(qids)); unlock=sorted(set(qids)-known_b)
        eligible=bool(structural) and address not in success
        pool.append({'address':address,'eligible':eligible,'previous_success_including_empty':address in success,'exclusion_reason':'PREVIOUS_SUCCESS_INCLUDING_EMPTY' if address in success else ('NO_CURRENT_POLICY_STRUCTURE_WITNESS' if not structural else ''),'priority_tier':0 if pilot else (1 if unlock else 2),'query_address_pairs':len(qids),'query_ids':qids,'incident_ids':incidents,'rotation_incident':incidents[0] if incidents else '', 'pilot_identity_gap':pilot,'zero_B_query_ids':unlock,'relation_rows_descriptive_only':len(rr),'structural_relation_rows':len(structural)})
    candidates=[r for r in pool if r['eligible']]; selected=[]; count=Counter()
    # Pair coverage dominates incident rotation. Equal tiers/counts round-robin
    # incidents, each address lexically ordered. First pass <= 3 per incident.
    for relax in (False,True):
        for key in sorted({(r['priority_tier'],-r['query_address_pairs']) for r in candidates}):
            groups=defaultdict(list)
            for r in candidates:
                if (r['priority_tier'],-r['query_address_pairs'])==key and r['address'] not in {s['address'] for s in selected}: groups[r['rotation_incident']].append(r)
            while any(groups.values()) and len(selected)<10:
                for incident in sorted(groups):
                    if not groups[incident]: continue
                    row=groups[incident].pop(0)
                    if not relax and any(count[i]>=3 for i in row['incident_ids']):continue
                    row=dict(row,rank=len(selected)+1,incident_cap_relaxed=relax,selection_reason=['FIXED_PILOT_IDENTITY_GAP','FIRST_B_TARGET_POTENTIAL','DISTINCT_QUERY_ADDRESS_PAIR_COVERAGE'][row['priority_tier']]+(';INSUFFICIENT_CANDIDATES_UNDER_3_PER_INCIDENT' if relax else ''))
                    selected.append(row)
                    for i in row['incident_ids']: count[i]+=1
                    if len(selected)==10:break
    # Compare by address after copying selected rows above.
    seen=set(); selected=[r for r in selected if not (r['address'] in seen or seen.add(r['address']))]
    now=dt.datetime.now(dt.timezone.utc).isoformat()
    for r in selected:r['frozen_at']=now;r['acquisition_scope']='REFERENCE_TARGETED';r['previous_query_status']='NOT_PREVIOUSLY_SUCCESSFULLY_QUERIED'
    write_csv(path,selected);write_csv(work/'derived/label_lookup_candidate_pool.csv',pool)
    last=sorted({r['queried_at'] for r in histories if r.get('queried_at')})
    recent=[r for r in histories if r.get('queried_at') and utc(r['queried_at'])>dt.datetime.now(dt.timezone.utc)-dt.timedelta(hours=24)]
    freeze={'frozen_at':now,'queue_sha256':digest(path),'queue_path':'derived/label_lookup_queue.csv','selected_addresses':len(selected),'all_current_unknown_addresses':len(pool),'current_unknown_with_policy_witness':sum(r['structural_relation_rows']>0 for r in pool),'eligible_after_success_exclusion':len(candidates),'historical_unique_success_addresses':len(success),'historical_empty_success_addresses':len({r['address'] for r in histories if r.get('api_response_status')=='SUCCESS' and r.get('label_result_status')=='EMPTY_LABEL_RESULT'}),'latest_historical_submission_utc':last[-1] if last else None,'local_last_24h_submitted_addresses':len({(r.get('request_id'),r['address']) for r in recent if r.get('api_submission_status','').startswith('SUBMITTED')}),'local_only_does_not_certify_provider_entitlement':True,'policy_version':VERSION,'incident_selection_counts':dict(count),'queue_replacement_after_results_allowed':False}
    dump(work/'manifests/label_lookup_queue_freeze.json',freeze);return freeze
