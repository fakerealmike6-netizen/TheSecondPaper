"""Recompute the accepted finite reference pool under a supplied label snapshot.

This is a reference certificate audit, never a discovery neighbor source or
fund attribution algorithm. Inputs are portable private review slices.
"""
from pathlib import Path
from collections import defaultdict, Counter
import csv, gzip
from reference_core import *

def rows(path):
    with (gzip.open if Path(path).suffix=='.gz' else open)(path,'rt',encoding='utf-8-sig',newline='') as f:
        return list(csv.DictReader(f))

def recompute(data_dir, identities):
    d=Path(data_dir)
    ev={e['event_id']:e for e in rows(d/'reference_event_slice.csv.gz')}
    qs=defaultdict(list); incidents=defaultdict(set)
    for q in rows(d/'reference_query_members.csv'): qs[q['query_id']].append(q)
    for r in rows(d/'reference_incident_event_membership.csv.gz'): incidents[r['incident_id']].add(r['event_id'])
    original={r['old_source_row']:r for r in rows(d/'A_original_reference_entries.csv.gz')}
    old=rows(d/'B_reference_relation_recheck.csv.gz'); cache={}
    for qid,qrows in qs.items():
        seeds=[q['seed_event_id'] for q in qrows if q['seed_match_status']=='EXACT_EVENT_MATCH' and q['seed_event_id'] in ev]
        pool=[ev[e] for e in sorted(incidents[qrows[0]['incident_id']] | set(seeds))]
        cache[qid]=(reference_certificates(pool,seeds,identities),reference_certificates(pool,seeds,identities,policy=True),reference_certificates(pool,seeds,identities,policy=True,window_days=90))
    result=[]; witnesses=[]
    for before in old:
        r=dict(before); qid=r['query_id']; qrows=qs[qid]; eid=r['entry_event_id']; target=r['target_address']
        source=original[r['old_source_row']]; c,p,w=cache[qid]; cert=p.get(eid); causal=c.get(eid); window=w.get(eid)
        ident=identities.get(target,{'identity_class':'UNKNOWN'}); role=ident.get('identity_class','UNKNOWN')
        ready=all(q['seed_match_status']=='EXACT_EVENT_MATCH' and q['seed_time'] for q in qrows)
        early=ready and source['event_match_status']=='EXACT_EVENT_MATCH' and source['entry_time'] and (min(utc(q['seed_time']) for q in qrows)-utc(source['entry_time'])).total_seconds()>0.000864
        status='UNRESOLVED'; reason=''
        if early: status,reason='REJECTED_ASSOCIATION','ENTRY_BEFORE_EARLIEST_SEED'
        elif not ready: reason='EXACT_SEED_UNRESOLVED'
        elif source['event_match_status']!='EXACT_EVENT_MATCH' or eid not in ev: reason='EXACT_ENTRY_UNRESOLVED'
        elif any(amount_status(ev[q['seed_event_id']].get('amount_raw'))[0]!='AMOUNT_POSITIVE' for q in qrows if q['seed_event_id'] in ev): reason='SEED_POSITIVE_AMOUNT_UNRESOLVED'
        elif amount_status(ev[eid].get('amount_raw'))[0]!='AMOUNT_POSITIVE': reason='ENTRY_'+amount_status(ev[eid].get('amount_raw'))[0]
        elif role in ('BRIDGE_BOUNDARY','MIXER_BOUNDARY','DEX_OR_PROTOCOL','NON_SERVICE_SENTINEL'): status,reason='OUTSIDE_CURRENT_SERVICE_TARGET_SCOPE',role
        elif role=='CONFLICTED_IDENTITY': reason='TARGET_IDENTITY_CONFLICT'
        elif role!='SERVICE' or not ident.get('actor'): reason='TARGET_SERVICE_IDENTITY_UNRESOLVED'
        elif cert:
            if qrows[0]['seed_rule']=='S3': reason='S3_INJECTION_DISJOINTNESS_UNRESOLVED'
            else: status,reason='VERIFIED_TASK_REFERENCE',('EXACT_ZERO_HOP_SERVICE_ENTRY' if cert['depth']==0 else 'STRICT_POSITIVE_SAME_ASSET_FIRST_IDENTIFIED_SERVICE')
        elif causal:
            blockers=[identities.get(ev[x]['to_address'],{}).get('identity_class','UNKNOWN') for x in causal['path'][:-1] if identities.get(ev[x]['to_address'],{}).get('identity_class') in STOP_ROLES]
            reason='KNOWN_FIRST_SERVICE_OR_BOUNDARY_BLOCKS_AVAILABLE_CERTIFICATES' if blockers and all(x in ('SERVICE','BRIDGE_BOUNDARY','MIXER_BOUNDARY') for x in blockers) else 'KNOWN_PROTOCOL_OR_CONFLICT_BOUNDARY_ON_AVAILABLE_CERTIFICATE'
        else:
            reason='PROTOCOL_SEMANTICS_UNRESOLVED' if r['entry_asset'] not in {q['seed_asset'] for q in qrows if q['seed_asset']} else 'NO_STRICT_SAME_ASSET_REFERENCE_CHAIN'
        issues=[]
        if qrows[0]['seed_rule']=='S3': issues.append('S3_DISJOINTNESS_NOT_PROVEN')
        if status=='UNRESOLVED': issues.append(reason)
        r.update(actor=ident.get('actor'),target_identity_class=role,task_reference_status=status,change_reason=reason,
            collection_90d_status=('WITHIN_PER_ARRIVAL_90D' if window else 'OUTSIDE_90D_IN_REGISTERED_EVIDENCE') if status=='VERIFIED_TASK_REFERENCE' else 'NOT_APPLICABLE_TO_UNRESOLVED_OR_EXCLUDED_B',
            verified_numeric_depth=cert['depth'] if status=='VERIFIED_TASK_REFERENCE' else None,
            strict_same_asset_chain_observed=bool(causal),policy_chain_with_unknowns_observed=bool(cert),
            unknown_intermediate_addresses=cert['unknown_intermediates'] if cert else [], unknown_identity_is_blocker=False,
            diagnostic_policy_90d_reachable=bool(window) if cert else None,unresolved_issues=issues,
            additional_issues=issues+(['UNKNOWN_INTERMEDIATE_IDENTITIES:'+str(len(cert['unknown_intermediates']))] if cert and cert['unknown_intermediates'] else []),
            identity_caveats=['UNKNOWN_INTERMEDIATES_NOT_CONFIRMED_EOA_OR_NON_SERVICE'] if cert and cert['unknown_intermediates'] else [])
        diagnostic={'reason':None,'entry_sender':ev.get(eid,{}).get('from_address'),'causal_predecessor_event_ids':[],'known_boundary_addresses_on_witness':[]}
        if eid in ev and ready:
            if causal:
                diagnostic['reason']='STRICT_SAME_ASSET_CERTIFICATE_EXISTS'
                diagnostic['known_boundary_addresses_on_witness']=[ev[x]['to_address'] for x in causal['path'][:-1] if identities.get(ev[x]['to_address'],{}).get('identity_class') in STOP_ROLES]
            else:
                e=ev[eid];predecessors=[ev[x] for x in c if ev[x]['to_address']==e['from_address'] and ev[x]['asset_key']==e['asset_key']]
                diagnostic['causal_predecessor_event_ids']=[x['event_id'] for x in predecessors]
                if not predecessors:diagnostic['reason']='NO_CERTIFIED_SAME_ASSET_ARRIVAL_AT_ENTRY_SENDER'
                elif any(strictly_before(x,e) is None for x in predecessors):diagnostic['reason']='INTRA_TRANSACTION_EXECUTION_ORDER_UNRESOLVED'
                elif all(strictly_before(x,e) is False for x in predecessors):diagnostic['reason']='ALL_CERTIFIED_ARRIVALS_AFTER_OR_EQUAL_ENTRY'
                else:diagnostic['reason']='REFERENCE_CERTIFICATE_INCOMPLETE'
        r['causal_diagnostic']=diagnostic
        r['witness_id']='wit:'+r['old_relation_id'][4:] if cert or causal else None
        result.append(r)
        chosen=cert or causal
        if chosen:
            witnesses.append({'old_relation_id':r['old_relation_id'],'query_id':qid,'entry_event_id':eid,'target_address':target,'event_ids':chosen['path'],'window_witness_event_ids':window['path'] if window else [],'unknown_intermediate_addresses':chosen['unknown_intermediates'],'witness_type':'FROZEN_LABEL_FIRST_IDENTIFIED_SERVICE_POLICY' if cert else 'CAUSAL_ONLY_BOUNDARIES_NOT_PASSED'})
    return result,witnesses,cache

def metrics(records):
    b=[r for r in records if r['task_reference_status']=='VERIFIED_TASK_REFERENCE']
    c=[r for r in b if r['collection_90d_status']=='WITHIN_PER_ARRIVAL_90D']
    def units(rs):return {'relations':len(rs),'queries':len({r['query_id'] for r in rs}),'addresses':len({r['target_address'] for r in rs}),'pairs':len({(r['query_id'],r['target_address']) for r in rs}),'entry_events':len({r['entry_event_id'] for r in rs}),'actors':len({r['actor'] for r in rs})}
    return {'B':units(b),'C':units(c),'status_counts':dict(Counter(r['task_reference_status'] for r in records)),'reason_counts':dict(Counter(r['change_reason'] for r in records)),'early_old_core':sum(r['change_reason']=='ENTRY_BEFORE_EARLIEST_SEED' and truth(r['old_requestable_pair']) for r in records),'denominator_finalized':False}
