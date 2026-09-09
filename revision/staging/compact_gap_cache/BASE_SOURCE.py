"""Actual-frontier Stage1D collection, immutable SQL pages and local replay.

Reference relations are deliberately absent from the provider API.
"""
from dataclasses import asdict
from datetime import datetime, timezone
import argparse, csv, gzip, hashlib, json, re, time
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha
from collector import Collector, Event, Scope, FetchResult, Limits, NATIVE
from provider_dune import build_interval_sql, normalize_rows, sql_time, exact_hex
from stage1d_runtime import Runtime, AUTH, execute_sql, result_rows
from context_access_r3 import read, sha, now
from page_attempts import atomic_json

def rows(path):
    op=gzip.open if str(path).endswith('.gz') else open
    with op(path,'rt',encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))

def interval_sql(addresses, start_block, end_block, start_time, end_time):
    addresses=sorted({exact_hex(a,20) for a in addresses})
    if not 1<=len(addresses)<=1000:raise ValueError('Finite expanded address set')
    first=addresses[0]
    sql=build_interval_sql(first,NATIVE,start_block,end_block,start_time=start_time,end_time=end_time)
    if len(addresses)>1:sql=re.sub(r'\s*=\s*'+first,r' IN ('+','.join(addresses)+')',sql)
    dates=f"block_date BETWEEN DATE '{sql_time(start_time)[:10]}' AND DATE '{sql_time(end_time)[:10]}' AND "
    for table in ('ethereum.transactions','ethereum.traces'):
        sql=sql.replace('FROM '+table+' WHERE ','FROM '+table+' WHERE '+dates)
    # Exact timestamp/block predicates remain; the same raw trace ancestor check
    # and physical normalizer own capacity. A batch shares a conservative ledger
    # interval, and per-arrival membership is checked again by Collector.
    return sql

def save_sql(work, sql, kind, query, intervals, dependencies, addresses=None):
    digest=hashlib.sha256(sql.encode()).hexdigest();folder=Path(work)/'private/stage1d_sql'/digest
    frozen={'schema_version':'stage1d-sql-freeze-v1','authorization_id':AUTH,'kind':kind,
        'query_ids':[query['query_id']],'scope_id':query['scope_id'],'scope_hash':query['scope_hash'],
        'sql_sha256':digest,'intervals':intervals,'dependencies':dependencies,
        'addresses':addresses or [],'export_plan':{'all_pages_required':True,'no_limit_or_sampling':True}}
    if (folder/'freeze_manifest.json').exists():
        old=read(folder/'freeze_manifest.json')
        if old['sql_sha256']!=digest:raise ValueError('Immutable SQL identity conflict')
        if old.get('scope_id')==query['scope_id'] and old.get('scope_hash')==query['scope_hash']:
            return folder/'freeze_manifest.json'
        # New observation scope, same successful SQL content/job identity. Keep
        # the historical FULL freeze immutable and bind a separate local claim.
        folder=folder/'scope_claims'/query['scope_hash']
        if (folder/'freeze_manifest.json').exists():
            if read(folder/'freeze_manifest.json')!=frozen:raise ValueError('Existing scope claim differs')
            return folder/'freeze_manifest.json'
    folder.mkdir(parents=True,exist_ok=False)
    (folder/'query.sql').write_text(sql,encoding='utf8',newline='\n');atomic_json(folder/'freeze_manifest.json',frozen)
    return folder/'freeze_manifest.json'

def _normalization_gap_suffix(original_gaps, matching):
    """Index only the pre-comprehension list; retain every ordered suffix copy.

    Equal ordinary JSON dictionaries with a string reason have the same reason.
    A bucket is only a negative lookup accelerator: Python list membership still
    decides equality, including nested/numeric values. Other shapes use the
    original list. Newly appended suffix values never enter this index because
    the original list comprehension also cannot see its own pending output.
    """
    by_reason, fallback = {}, []
    for gap in original_gaps:
        reason = gap.get('reason') if type(gap) is dict else None
        if type(reason) is str:
            by_reason.setdefault(reason, []).append(gap)
        else:
            fallback.append(gap)
    suffix = []
    for record in matching:
        for gap in record.get('normalization_gaps', []):
            reason = gap.get('reason') if type(gap) is dict else None
            if type(reason) is str:
                if gap not in by_reason.get(reason, ()) and gap not in fallback:
                    suffix.append(gap)
            elif gap not in original_gaps:
                suffix.append(gap)
    return suffix


class CachedIntervals:
    replay_only=True
    def __init__(self, work):
        self.work=Path(work);self.pending=[];self.records=[];self.scope=None
        content_memo={}
        for p in sorted((self.work/'derived/stage1d/intervals').glob('*.coverage.json')):
            r=read(p)
            if r.get('acquisition_adapter') == 'stage1d-transfers-acquisition-v1':
                from stage1d_transfers_acquisition import verify_interval_record
                verify_interval_record(self.work, r)
            elif r.get('acquisition_adapter') == 'stage1d-batch-binding-route-v1':
                from stage1d_batch_binding_route import verify_interval_record
                verify_interval_record(self.work, r, memo=content_memo)
            elif r.get('acquisition_adapter') == 'stage1d-canonical-weth-log-index-v1':
                from stage1d_weth_log_index import verify_interval_record
                verify_interval_record(self.work, r)
            elif r.get('coverage_capability') == 'NATIVE_INDEX_ONLY':
                from stage1d_native_candidate import verify_capability_record
                verify_capability_record(self.work, r)
            data=(self.work/r['events_path']).resolve()
            if not data.is_relative_to(self.work.resolve()) or data.is_symlink():
                raise ValueError('Cached event path escapes workspace')
            content_key=(str(data),r['events_sha256'])
            if content_key not in content_memo:
                if sha(data)!=r['events_sha256']:raise ValueError('Cached events changed')
                events=[Event(**e) for e in read(data)]
                by_address={}
                for event in events:
                    # One self-transfer appears once per address. Every list
                    # preserves the original physical input order exactly.
                    for address in dict.fromkeys((event.sender,event.recipient)):
                        by_address.setdefault(address,[]).append(event)
                content_memo[content_key]=(events,by_address)
            r['events'],r['events_by_address']=content_memo[content_key]
            r['coverage_sha256']=sha(p)
            self.records.append(r)

    def bind_scope(self,scope):
        # Identity is a claim about this replay, never a content-cache key.
        self.scope=scope

    def fetch_interval(self,address,asset,start_block,end_block,*,start_time,end_time,global_end_time):
        from stage1d_window import missing_rectangles
        from physical_facts import PhysicalFactRegistry
        requested={'address':address,'asset':asset,'start_block':start_block,'end_block':end_block,
            'start_time':start_time,'end_time':min(end_time,global_end_time)}
        # Legacy Stage1D records without an asset field were emitted by the
        # native index SQL; do not extend their completeness to token states.
        matching=[r for r in self.records if address in r['addresses']
                  and r.get('asset',NATIVE)==asset
                  and r['end_block']>=start_block and r['start_block']<=end_block
                  and r['end_time']>=start_time and r['start_time']<=requested['end_time']]
        completed=[{k:r[k] for k in ('start_block','end_block','start_time','end_time')}
                   |{'address':address,'asset':asset,'complete':r.get('complete') is True}
                   for r in matching]
        missing=missing_rectangles(requested,completed)
        # All old and new pages keep one physical capacity, even when obtained
        # by another query, an overlapping batch, or a narrower window version.
        registry=PhysicalFactRegistry()
        for r in matching:
            for e in r['events_by_address'].get(address,()):
                if address in (e.sender,e.recipient) and start_block<=e.block<=end_block and start_time<=e.timestamp<=requested['end_time']:
                    registry.add(e,source='INTERVAL:'+r['evidence_id'])
        snap=registry.snapshot()
        explicit_conflicts=[g for r in matching for g in r.get('normalization_gaps',[])
                            if g.get('reason') in ('PHYSICAL_FACT_CONFLICT','DUNE_EVENT_IDENTITY_CONFLICT')]
        conflicts=snap['conflicts']+explicit_conflicts
        complete=not missing and not conflicts
        for part in missing:
            if part not in self.pending:self.pending.append(part)
        claim={'scope_id':self.scope.scope_id,'scope_hash':self.scope.scope_hash,
               'query_id':self.scope.query_id,'window_mode':self.scope.window_mode} if self.scope else {}
        evidence=[{'evidence_id':r['evidence_id'],'coverage_sha256':r['coverage_sha256'],
                   'events_sha256':r['events_sha256'],'complete':r.get('complete') is True,
                   **{k:r[k] for k in ('coverage_capability','native_scope_complete','all_asset_export_complete',
                       'native_decision_path','native_decision_sha256') if k in r},
                   **{k:r[k] for k in ('start_block','end_block','start_time','end_time')}} for r in matching]
        gaps=[{'reason':'UNCOLLECTED_ADDRESS_INTERVAL',**part} for part in missing]+conflicts
        if missing:gaps += _normalization_gap_suffix(gaps, matching)
        return FetchResult(events=[] if conflicts else [Event(**e) for e in snap['events']],
            complete=complete,cache_hits=int(bool(matching)),
            coverage=[{**requested,**claim,'provider':','.join(sorted({r.get('provider','Dune') for r in matching})) or 'Dune','complete':complete,
                'interval_evidence':[r['evidence_id'] for r in matching],
                'verified_content_intervals':evidence,'uncovered_intervals':missing,
                **({'coverage_capability':'NATIVE_INDEX_ONLY','all_asset_export_complete':False}
                   if any(r.get('coverage_capability')=='NATIVE_INDEX_ONLY' for r in matching) else {}),
                'content_identity_separate_from_scope':True}],gaps=gaps,
            fact_conflicts=conflicts,quarantined_facts=snap['quarantined_versions'])

class Labels:
    def __init__(self,work):
        self.work=Path(work);base=self.work/'private/stage1d_inputs/address_registry.csv.gz'
        self.technical_roles=None
        self.task_boundaries=None
        if (self.work/'private/stage1d_authority/CLOSURE_SCOPE_ADOPTION.json').exists():
            from stage1d_role_adoption import TechnicalRoles
            self.technical_roles=TechnicalRoles(self.work)
            from stage1d_task_boundaries import TaskBoundaries
            self.task_boundaries=TaskBoundaries(self.work)
        self.registry={r['address']:r for r in rows(base)} if base.exists() else {}
        for p in sorted((self.work/'derived/stage1d/labels').glob('*.json')):
            for r in read(p).get('rows',[]):self.registry[r['address']]=r
        p=self.work/'private/stage1d_inputs/HISTORICAL_LABEL_SUCCESS.json'
        self.success=set(read(p)['addresses']) if p.exists() else set()
    def __call__(self,address):
        r=self.registry.get(address)
        if r:
            role=r.get('identity_class','UNKNOWN');status=r.get('lookup_status','LOCAL_FROZEN')
            kind={'SERVICE':'SERVICE','BRIDGE_BOUNDARY':'BRIDGE','MIXER_BOUNDARY':'MIXER',
                'DEX_OR_PROTOCOL':'UNSUPPORTED_PROTOCOL','CONFLICTED_IDENTITY':'UNSUPPORTED_PROTOCOL'}.get(role,'UNKNOWN')
            if kind!='UNKNOWN' and r.get('provenance_status')=='PROVENANCE_UNRESOLVED':
                return {'kind':'UNKNOWN','status':'UNQUERIED','reason':'HISTORICAL_EVIDENCE_GAP_REQUIRES_ACTUAL_FRONTIER_CHECK'}
            return {'kind':kind,
                'status':'LOOKUP_FAILED' if str(status).startswith('FAILED') else 'LOCAL_FROZEN',
                'actor':r.get('actor'),'provenance_status':r.get('provenance_status'),'observation_ids':r.get('adopted_observation_ids'),
                'acquisition_scope':r.get('acquisition_scope')}
        return {'kind':'UNKNOWN','status':'SUCCESS_EMPTY_CACHED' if address in self.success else 'UNQUERIED'}

    def resolve_state(self,state):
        identity=self(state.address)
        identity=self.technical_roles.resolve(state,identity) if self.technical_roles else identity
        return self.task_boundaries.resolve(state,identity) if self.task_boundaries else identity

def replay(work,query):
    work=Path(work).resolve();provider=CachedIntervals(work);scope=Scope.from_policy(query)
    runtime=Runtime()
    limits=Limits(max_online_seconds=runtime.resource_cap(work,'per_query_online_hard_seconds_all_modes',10800),
        max_events=runtime.resource_cap(work,'candidate_events_per_query_hard',25000),
        max_expanded_addresses=runtime.resource_cap(work,'expanded_addresses_per_query_hard',1000))
    from stage1d_semantic_catalogue import load_current_resolver
    seed=Event(**query['seed_event']);collection=Collector(provider,Labels(work),limits,
        semantic_resolver=load_current_resolver(work)).run(scope,seed)
    folder=work/'derived/stage1d/queries'/query['name'];folder.mkdir(parents=True,exist_ok=True)
    collection.write(folder/'collection.json');atomic_json(folder/'pending_intervals.json',provider.pending)
    return asdict(collection),provider.pending

def acquire_labels(work,query,collection):
    from frontier_labels_r2 import optimized_sql
    from frontier_labels_r1 import parse_result_rows, observations_from_parsed
    from labels_policy import resolve_address
    from stage1d_label_opportunities import snapshot as label_snapshot, address_key
    work=Path(work).resolve(); labels=Labels(work)
    actual=list(dict.fromkeys(s['state']['address'] for s in collection['states']))
    unresolved={a for a in actual if labels(a)['status'] in ('UNQUERIED','LOOKUP_FAILED')}
    cohorts=[(p,read(p)) for p in (work/'private/stage1d_label_cohorts').glob('*.json')]
    quota=label_snapshot(work,cohorts)
    chain=str(query.get('seed_event',{}).get('chain_id') or 'eip155:1')
    spent=set(quota['allocated_addresses_by_chain'].get(chain,[]))
    recoverable=[]
    for p,r in cohorts:
        if not unresolved.intersection(r['addresses']):continue
        completed=work/'derived/stage1d/labels'/p.name
        if completed.exists():
            saved=read(completed);resolved=saved.get('rows',[])
            if len(resolved)!=len(r['addresses']) or {v.get('address') for v in resolved}!=set(r['addresses']) or any(v.get('lookup_status')!='COMPLETED_FOUR_TABLE_OPPORTUNITY' for v in resolved):raise ValueError('Saved label cohort resolution is incomplete or inconsistent')
            # A successful empty result remains cached even if a historical
            # unsupported service claim still has unresolved provenance.
            continue
        recoverable.append((p,r))
    recoverable.sort(key=lambda pair:(pair[1].get('previous_batch_distinct',0),pair[0].name))
    recovered=bool(recoverable)
    if recovered:
        path,record=recoverable[0];selected=list(record['addresses'])
        digest=hashlib.sha256(json.dumps(record,sort_keys=True).encode()).hexdigest()
        if path.stem!=digest:raise ValueError('Immutable label cohort identity changed')
        if record['query_id']!=query['query_id']:
            owners=[q for q in active_batch(work)['queries'] if q['query_id']==record['query_id']]
            if len(owners)!=1:raise ValueError('Original label cohort query owner is unavailable')
            owner=owners[0]
        else:owner=query
    else:
        selected=[a for a in actual if a in unresolved and address_key(a) not in spent][:min(100 if quota['cap']==2000 else 40,quota['remaining'])]
        if not selected:return {'status':'NO_NEW_LABEL_OPPORTUNITY','new_addresses':0,'labels_updated':False,'label_opportunity_budget':quota}
        owner=query
        record={'query_id':query['query_id'],'addresses':selected,'actual_arrivals':collection['states'],
            'previous_batch_distinct':quota['allocated_distinct'],'stage_distinct_after':quota['allocated_distinct']+len({address_key(a) for a in selected}),
            'basis':'ACTUAL_FRONTIER_ORDER_LOCAL_FIRST_NO_REFERENCE_WHITELIST'}
        if not quota['legacy']:
            record.update(chain_id=chain,quota_authorization_id=quota['authorization_id'],quota_authority_ref=quota['authority_ref'])
        digest=hashlib.sha256(json.dumps(record,sort_keys=True).encode()).hexdigest()
        path=work/'private/stage1d_label_cohorts'/(digest+'.json');atomic_json(path,record)
    from stage1d_owner_roles import effective_rules
    rules=effective_rules(work)
    dependency={'path':path.relative_to(work).as_posix(),'sha256':sha(path)}
    matches=[]
    if recovered:
        for candidate in (work/'private/stage1d_sql').glob('*/freeze_manifest.json'):
            frozen=read(candidate)
            deps=[d for d in frozen.get('dependencies',[]) if d.get('path')==dependency['path']]
            if not deps:continue
            if deps!=[dependency] or frozen.get('kind')!='frontier_labels' or frozen.get('addresses')!=selected or frozen.get('query_ids')!=[record['query_id']]:raise ValueError('Existing label SQL freeze conflicts with immutable cohort')
            if sha(candidate.parent/'query.sql')!=frozen.get('sql_sha256'):raise ValueError('Original label SQL bytes changed')
            matches.append(candidate)
    if len(matches)>1:raise ValueError('Ambiguous SQL executions for one immutable label cohort')
    if matches:freeze=matches[0]
    else:
        # A crash or budget refusal may precede SQL freeze/submission. Rebuild
        # only the already charged cohort; no new address opportunity is spent.
        sql=(optimized_sql(selected,rules['table_schema'],maximum_addresses=100) if quota['cap']==2000
             else optimized_sql(selected,rules['table_schema']))
        freeze=save_sql(work,sql,'frontier_labels',owner,[],[dependency],selected)
    frozen=read(freeze);job_folder=work/'private/dune_r2_jobs'/frozen['sql_sha256']
    metadata={'new_addresses':0 if recovered else len(selected),'opportunities_spent_now':0,
        'opportunities_reserved_now':0 if recovered else len(selected),'opportunities_confirmed_used_now':0,
        'cohort_reused':recovered,'opportunities_reused':len(selected) if recovered else 0,'cohort_id':digest,'labels_updated':False}
    if job_folder.exists() and (not (job_folder/'job.json').exists() or not read(job_folder/'job.json').get('execution_id')):
        return {'status':'UNRESOLVED_EXISTING_SUBMISSION','reason':'Saved submission identity is unknown; original job/risk retained and no POST attempted',
                'job_folder':str(job_folder),'label_opportunity_budget':label_snapshot(work),**metadata}
    # execute_sql owns the existing execution's poll/page/retry journal. An
    # existing failed/pending job is never replaced with another SQL submission.
    outcome=execute_sql(work,freeze,owner['name'],'labels_'+owner['name'])
    if outcome.get('status')=='COMPLETED_EXPORTED':
        job=read(Path(outcome['job_folder'])/'job.json');raw=result_rows(work,outcome['job_folder'])
        parsed=parse_result_rows(raw,selected)
        obs,opps=observations_from_parsed(parsed,rules,job['execution_id'],now(),freeze.relative_to(work).as_posix(),sha(freeze))
        prior_path=work/'private/stage1d_inputs/label_observations.csv.gz'
        prior=rows(prior_path) if prior_path.exists() else []
        resolved=[]
        for address in selected:
            old=[o for o in prior if o['address']==address];added=[o for o in obs if o['address']==address]
            r=resolve_address(old+added,labels.registry.get(address,{'address':address,'identity_class':'UNKNOWN'}),baseline_observations=old)
            r.update(lookup_status='COMPLETED_FOUR_TABLE_OPPORTUNITY',acquisition_scope='STAGE1D_ACTUAL_FRONTIER')
            resolved.append(r)
        atomic_json(work/'derived/stage1d/labels'/(digest+'.json'),{'rows':resolved,'observations':obs,'opportunities':opps,'sql_freeze_sha256':sha(freeze)})
        metadata['labels_updated']=True
    after_quota=label_snapshot(work)
    newly_confirmed=max(0,after_quota['confirmed_used_distinct']-quota['confirmed_used_distinct'])
    metadata['opportunities_confirmed_used_now']=newly_confirmed
    metadata['opportunities_spent_now']=newly_confirmed if not recovered else 0
    outcome.update(metadata,label_opportunity_budget=after_quota)
    return outcome

def acquire_pending(work,query,pending,collection_path):
    work=Path(work).resolve();collection_path=Path(collection_path).resolve()
    if not pending:return {'status':'NO_PENDING_INTERVALS'}
    # Batch only identical uncovered rectangles: a min/max envelope would
    # accidentally re-request already successful content between disjoint gaps.
    fields=('start_block','end_block','start_time','end_time')
    rectangle=tuple(pending[0][k] for k in fields)
    same=[p for p in pending if tuple(p[k] for k in fields)==rectangle]
    addresses=list(dict.fromkeys(p['address'] for p in same))[:32]
    intervals=[p for p in same if p['address'] in addresses]
    start_block=min(p['start_block'] for p in intervals);end_block=max(p['end_block'] for p in intervals)
    t0=min(p['start_time'] for p in intervals);t1=max(p['end_time'] for p in intervals)
    sql=interval_sql(addresses,start_block,end_block,t0,t1)
    dep={'path':collection_path.relative_to(work).as_posix(),'sha256':sha(collection_path)}
    # Copy the changing collection into an immutable input for this SQL freeze.
    frozen_dep=work/'private/stage1d_frontier_evidence'/(dep['sha256']+'.json')
    if not frozen_dep.exists():frozen_dep.parent.mkdir(parents=True,exist_ok=True);frozen_dep.write_bytes(collection_path.read_bytes())
    dep['path']=frozen_dep.relative_to(work).as_posix()
    freeze=save_sql(work,sql,'candidate',query,[{**p,'query_id':query['query_id'],'kind':'candidate'} for p in intervals],[dep],addresses)
    outcome=execute_sql(work,freeze,query['name'],'candidate_'+query['name'])
    if outcome.get('status')=='COMPLETED_EXPORTED':
        rawrows=result_rows(work,outcome['job_folder']);events,gaps=normalize_rows(rawrows)
        folder=work/'derived/stage1d/intervals';folder.mkdir(parents=True,exist_ok=True)
        eid=sha(freeze.parent/'query.sql');ep=folder/(eid+'.events.json')
        atomic_json(ep,[asdict(e) for e in events])
        record={'evidence_id':eid,'addresses':addresses,'start_block':start_block,'end_block':end_block,
            'start_time':t0,'end_time':t1,'complete':not gaps,'normalization_gaps':gaps,'raw_rows':len(rawrows),
            'events_path':ep.relative_to(work).as_posix(),'events_sha256':sha(ep),'query_id_at_acquisition':query['query_id'],
            'scope_id_at_acquisition':query['scope_id'],'job_folder':str(Path(outcome['job_folder']).relative_to(work))}
        atomic_json(folder/(eid+'.coverage.json'),record)
        outcome.update(raw_rows=len(rawrows),unique_events=len(events),normalization_gaps=gaps)
    return outcome

def run_query(work,query,*,max_batches=100):
    work=Path(work).resolve();folder=work/'derived/stage1d/queries'/query['name'];folder.mkdir(parents=True,exist_ok=True)
    attempts=read(folder/'acquisition_attempts.json') if (folder/'acquisition_attempts.json').exists() else []
    for _ in range(max_batches):
        collection,pending=replay(work,query)
        try:
            label_result=acquire_labels(work,query,collection)
            if label_result.get('new_addresses') or label_result.get('labels_updated'):
                attempts.append({'phase':'labels',**label_result});atomic_json(folder/'acquisition_attempts.json',attempts)
                collection,pending=replay(work,query)
        except Exception as exc:
            attempts.append({'phase':'labels','status':'LABEL_LOOKUP_FAILED_OR_RESOURCE_BLOCKED','error_class':type(exc).__name__,'reason':str(exc)})
            atomic_json(folder/'acquisition_attempts.json',attempts)
        if not pending:break
        try:result=acquire_pending(work,query,pending,folder/'collection.json')
        except Exception as exc:
            result={'status':'ACQUISITION_PARTIAL','error_class':type(exc).__name__,'reason':str(exc)}
        attempts.append(result);atomic_json(folder/'acquisition_attempts.json',attempts)
        if result.get('status')!='COMPLETED_EXPORTED':break
        if Runtime().raw_risk(work)>=Runtime().raw_limit(work):break
    collection,pending=replay(work,query)
    return {'name':query['name'],'query_id':query['query_id'],'scope_id':query['scope_id'],'window_mode':query['window_mode'],
        'max_depth':query['max_acquisition_depth'],'collection_status':collection['status'],'pending_intervals':len(pending),
        'candidate_events':len(collection['candidate_events']),'attempts':attempts}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--query',required=True);p.add_argument('--max-batches',type=int,default=100);a=p.parse_args()
    query=next(q for q in active_batch(a.work)['queries'] if q['name']==a.query)
    outcome=run_query(a.work,query,max_batches=a.max_batches);atomic_json(a.work/'derived/stage1d/queries'/a.query/'ACQUISITION_STATUS.json',outcome)
    print(json.dumps(outcome,indent=2,default=str))
