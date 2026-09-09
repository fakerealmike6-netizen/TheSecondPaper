"""Recomputable R2 report tables from saved evidence, without live collection.

Rows, requests, intervals, unique physical events and graph candidates are
different units. SQLite is opened read-only; account-wide fields are excluded.
"""
import argparse
import csv
import hashlib
import json
import sqlite3
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

from context_evidence_r2 import inside, read, receipt_payload, sha, write
from page_contract import initial_progress, validate_page
from provider_dune import normalize_rows


KINDS=('top','internal','erc20')


def text_json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,default=str)


def csv_file(path, rows, fields=None):
    fields=fields or sorted(set().union(*(row.keys() for row in rows))) if rows else fields or ['status']
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',encoding='utf-8',newline='') as handle:
        writer=csv.DictWriter(handle,fieldnames=fields);writer.writeheader()
        for row in rows:
            writer.writerow({k:text_json(v) if isinstance(v,(dict,list,tuple)) else v for k,v in row.items()})


def query_surfaces(sql, kind):
    # A surface is only claimed for the reviewed full indexed-event template.
    normal=' '.join(sql.split())
    template=kind=='candidate' and 'stage1b-dune-index-adapter-1.0' in normal and 'SELECT * FROM indexed_events' in normal
    return {k:bool(template and token in normal) for k,token in
            [('top',"'top' AS event_kind"),('internal',"SELECT 'internal'"),('erc20',"SELECT 'erc20'")]}


def amount_objectives(data):
    """Expose singleton entry aliases without claiming an additional solve."""
    joint=data['joint'];entries=data['entry_intervals'];events=joint['objective_events']
    rows=[('ADDRESS_ASSET_JOINT',None,joint,False)]
    rows.extend(('ENTRY_EVENT',eid,result,False) for eid,result in entries.items())
    if len(events)==1 and not entries:
        rows.append(('ENTRY_EVENT',events[0],joint,True))
    elif set(entries)!=set(events):
        raise ValueError('Joint target events lack corresponding entry intervals')
    return rows


def read_jobs(work):
    jobs={}; pages=[]; gaps=[]
    for group,origin in [('dune_live_jobs','INHERITED'),('dune_r1_jobs','INHERITED'),('dune_r2_jobs','R2_NEW')]:
        for path in sorted((work/'private'/group).glob('*/job.json')):
            job=read(path);folder=path.parent;logical=job['logical_job_id']
            if logical in jobs:raise ValueError('Duplicate logical job artifact')
            kind=job.get('kind') or 'candidate';sql=(folder/'query.sql').read_text(encoding='utf-8')
            if hashlib.sha256(sql.encode()).hexdigest()!=job['sql_sha256']:
                raise ValueError('Submitted SQL differs from saved text')
            if job.get('scope_freeze_path'):
                freeze_path=inside(work,job['scope_freeze_path'])
                if sha(freeze_path)!=job['scope_freeze_sha256']:raise ValueError('Scope freeze changed')
            execution=job.get('execution_id');sources=[]
            for phase in ('submit','status'):
                receipt=job.get(phase+'_receipt');payload=job.get(phase+'_response')
                if receipt:
                    actual,source=receipt_payload(work,receipt,payload);sources.append(source)
                    if execution and isinstance(actual,dict) and actual.get('execution_id')!=execution:
                        raise ValueError('Saved job execution binding differs')
            progress=initial_progress();data=[];md=(job.get('status_response') or {}).get('result_metadata') or {}
            for offset in job.get('export_offsets',[]):
                page_path=folder/f'page_{offset}.json';receipt_path=folder/f'page_{offset}_receipt.json'
                if not page_path.exists() or not receipt_path.exists():
                    gaps.append({'job_id':logical,'reason':'ATTEMPTED_PAGE_ARTIFACT_UNAVAILABLE','offset':offset});continue
                page=read(page_path);receipt=read(receipt_path)
                _,source=receipt_payload(work,receipt,page);sources.append(source)
                params=receipt.get('parameters') or {'limit':1000,'offset':offset}
                progress=validate_page(page,execution_id=execution,offset=offset,limit=params['limit'],progress=progress,
                                       status_metadata=md,receipt=receipt,parameters=params)
                data.extend(page['result']['rows'])
                pages.append({'job_id':logical,'origin':origin,'kind':kind,'execution_id':execution,'offset':offset,
                              'request_id':receipt['request_id'],'rows':len(page['result']['rows']),
                              'raw_bytes':source['bytes'],'raw_sha256':source['sha256'],'raw_path':source['path'],
                              'saved_page_sha256':sha(page_path),'page_contract_complete_after_page':progress['complete']})
            jobs[logical]={'job':job,'folder':folder,'path':path.relative_to(work).as_posix(),'sha256':sha(path),
                           'origin':origin,'kind':kind,'rows':data,'progress':progress,'sources':sources,
                           'surfaces':query_surfaces(sql,kind)}
    return jobs,pages,gaps


def terminal_execution_costs(jobs):
    """Use only COMPLETED status bodies already hash-checked by read_jobs."""
    costs={}
    for logical,item in jobs.items():
        job=item['job'];status=job.get('status_response') or {};receipt=job.get('status_receipt') or {}
        if job.get('state')!='QUERY_STATE_COMPLETED' or status.get('state')!='QUERY_STATE_COMPLETED':continue
        if not status.get('is_execution_finished') or not receipt or receipt.get('http_status')!=200:continue
        raw_cost=status.get('execution_cost_credits')
        if raw_cost is None:continue
        cost=Decimal(str(raw_cost))
        if not cost.is_finite() or cost<0:raise ValueError('Invalid terminal execution cost')
        costs[logical]={'credits':str(cost),'execution_id':job['execution_id'],
                        'terminal_state':status['state'],'job_path':item['path'],'job_sha256':item['sha256'],
                        'status_request_id':receipt['request_id'],'status_raw_path':receipt['raw_path'],
                        'status_raw_sha256':receipt['sha256']}
    return costs


def classify_fee_components(reserved,actual,ledger_execution_known,export_actual,bounded_accounting,terminal_execution=None):
    """Reclassify provisional peaks without releasing their occupied risk."""
    risk=Decimal(actual if actual is not None else reserved)
    peak=Decimal(ledger_execution_known or '0');export=Decimal(export_actual or '0')
    execution=Decimal(terminal_execution) if terminal_execution is not None else peak
    known=Decimal(actual) if actual is not None else execution+export
    # The pre-existing export bound is calculated against its ledger peak.
    # A later lower terminal cost does not silently enlarge that export bound.
    bounded=max(Decimal(0),risk-peak-export) if actual is None and bounded_accounting else Decimal(0)
    pending=risk-known-bounded
    discrepancy=max(Decimal(0),peak-execution) if actual is None else Decimal(0)
    if min(risk,known,bounded,pending)<0 or discrepancy>pending:
        raise ValueError('Execution evidence cannot partition retained fee occupancy')
    return {'risk':risk,'known':known,'bounded':bounded,'pending':pending,'discrepancy':discrepancy,'execution':execution}


def budget_snapshot(work,jobs):
    terminal=terminal_execution_costs(jobs)
    path=work/'private/shared_budget_r2.sqlite'
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True) as db:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        limits=dict(db.execute('SELECT unit,cap FROM limits'))
        amounts={(r[0],r[1]):r[2:] for r in db.execute('SELECT job,unit,reserved,actual FROM amounts')}
        rows=[];total=known=bounded=unknown=discrepancy_total=provisional_peak_total=Decimal(0);legacy=new=Decimal(0)
        for job,origin,er,ek,xr,xa,status,evidence in db.execute('SELECT * FROM r2_components ORDER BY origin,job'):
            reserved,actual=amounts[(job,'dune_credits')]
            history=json.loads(evidence).get('historical_risk') if origin=='INHERITED' else None
            proven=status=='BOUNDED_ACCOUNTING_NOT_FINAL' or bool(history and history[3]=='BOUNDED_ACCOUNTING_NOT_FINAL')
            evidence_cost=terminal.get(job)
            classified=classify_fee_components(reserved,actual,ek,xa,proven,evidence_cost['credits'] if evidence_cost else None)
            risk=classified['risk'];identified=classified['known'];bound_part=classified['bounded'];unknown_part=classified['pending']
            row={'job_id':job,'origin':origin,'known_execution_credits':str(classified['execution']),'known_export_credits':xa,
                 'final_actual_total_credits':actual,'risk_occupancy_credits':str(risk),'known_actual_lower_bound_credits':str(identified),
                 'bounded_export_not_actual_credits':str(bound_part),'unknown_or_pending_credits':str(unknown_part),'accounting_status':status,
                 'ledger_execution_known_raw':ek,'observed_provisional_peak_execution_credits':ek,
                 'execution_cost_discrepancy_pending_credits':str(classified['discrepancy']),
                 'execution_cost_evidence_basis':'VERIFIED_COMPLETED_STATUS' if evidence_cost else 'INHERITED_RECORDED_EVIDENCE',
                 'terminal_execution_cost_source':evidence_cost}
            rows.append(row);total+=risk;known+=identified;bounded+=bound_part;unknown+=unknown_part
            discrepancy_total+=classified['discrepancy'];provisional_peak_total+=Decimal(ek or '0')
            if origin=='INHERITED':legacy+=risk
            else:new+=risk
        units={}
        for unit in ('rpc_operations','bigquery_bytes','alchemy_cu','meta_requests','meta_addresses'):
            actual=sum((Decimal(a) for (j,u),(r,a) in amounts.items() if u==unit and a is not None),Decimal(0))
            reserved=sum((Decimal(r) for (j,u),(r,a) in amounts.items() if u==unit and a is None),Decimal(0))
            units[unit]={'actual':str(actual),'unsettled_reserved':str(reserved),'policy_cap':limits.get(unit)}
        meta=dict(db.execute("SELECT key,value FROM r2_meta WHERE key IN ('authorization_id','halt','initialized_utc')"))
        serialized={'limits':limits,'jobs':rows,'other_units':units,'authorization':meta}
        snapshot_hash=hashlib.sha256(text_json(serialized).encode()).hexdigest()
    if known+bounded+unknown!=total:raise ValueError('Fee components do not partition cumulative risk')
    return {'source':'READ_ONLY_TRANSACTION_SNAPSHOT','snapshot_sha256':snapshot_hash,
            'authorization_id':meta.get('authorization_id'),'cap_credits':limits['dune_credits'],
            'cumulative_risk_credits':str(total),'known_actual_lower_bound_credits':str(known),
            'bounded_export_not_actual_credits':str(bounded),'unknown_or_pending_risk_credits':str(unknown),
            'observed_provisional_peak_execution_total_credits':str(provisional_peak_total),
            'execution_cost_discrepancy_pending_credits':str(discrepancy_total),
            'unknown_or_pending_excluding_execution_discrepancy_credits':str(unknown-discrepancy_total),
            'ledger_risk_released_by_terminal_reclassification':False,
            'legacy_risk_credits':str(legacy),'r2_risk_credits':str(new),'remaining_stage_policy_credits':str(Decimal(limits['dune_credits'])-total),
            'warning_at_80':total>=80,'warning_requires_pause':False,'halt_reason':meta.get('halt'),
            'other_units':units,'metasleuth_new_requests_authorized':0,'account_wide_allowance_fields_excluded':True,'jobs':rows}


def receipt_inventory(work, jobs):
    copied={r['destination'] for r in read(work/'manifests/R1_INPUT_MAPPING.json')
            if r['status']=='COPIED_IDENTICAL' and r['destination'].startswith('raw/')}
    records={};seen_sources=[]
    for path in sorted((work/'logs').glob('*.json')):
        try:value=read(path)
        except (ValueError,UnicodeError):continue
        if isinstance(value,dict) and value.get('request_id') and value.get('raw_path') and value.get('sha256'):
            seen_sources.append((path,value))
    for item in jobs.values():
        for key,value in item['job'].items():
            if key.endswith('_receipt') and isinstance(value,dict) and value.get('request_id') and value.get('raw_path'):
                seen_sources.append((work/item['path'],value))
        for path in item['folder'].glob('page_*_receipt.json'):
            value=read(path)
            if value.get('request_id'):seen_sources.append((path,value))
    for source,value in seen_sources:
        rid=value['request_id'];path=inside(work,value['raw_path'])
        if sha(path)!=value['sha256'] or path.stat().st_size!=value['raw_bytes']:
            raise ValueError('Request evidence bytes changed')
        origin='INHERITED_COPY' if value['raw_path'] in copied else 'R2_NEW'
        row={k:value.get(k) for k in ('request_id','operation','execution_id','utc','elapsed_seconds','http_status','error_class','raw_path','raw_bytes','sha256')}
        row['origin']=origin
        if rid in records:
            old=records[rid]
            if any(old[k]!=row[k] for k in ('raw_bytes','sha256','operation','raw_path')):
                raise ValueError('Same request identity has contradictory evidence')
            old['receipt_sources'].append(source.relative_to(work).as_posix())
        else:records[rid]=row|{'receipt_sources':[source.relative_to(work).as_posix()]}
    raw_files=[p for p in (work/'raw').rglob('*') if p.is_file()]
    new_files=[p for p in raw_files if p.relative_to(work).as_posix() not in copied]
    new_requests=[r for r in records.values() if r['origin']=='R2_NEW']
    named_raw={r['raw_path'] for r in new_requests}
    unmatched=[p.relative_to(work).as_posix() for p in new_files if p.relative_to(work).as_posix() not in named_raw]
    base=read(work/'baseline/FINAL_RESOURCE_ADDENDUM.json')['bytes']
    raw={'baseline_physical_upper_bytes':base['conservative_physical_directory_upper_with_unknown_read_reserve'],
         'baseline_logical_download_bytes':base['logical_acquired_payload_bytes'],
         'r2_new_physical_raw_bytes':sum(p.stat().st_size for p in new_files),
         'r2_downloaded_body_bytes_by_request':sum(r['raw_bytes'] for r in new_requests),
         'r2_unique_request_count':len(new_requests),'inherited_copy_file_bytes_not_new_download':sum(p.stat().st_size for p in raw_files if p.relative_to(work).as_posix() in copied),
         'r2_unknown_read_limit_hits':[r['request_id'] for r in new_requests if r.get('error_class')],
         'unmatched_new_raw_files':unmatched,'deduplication_unit':'request_id and verified file identity; never body SHA alone',
         'cap_bytes':536870912,'old_legacy_unrecorded_read_uncertainty_preserved':base['legacy_URLError_saved_zero_bytes_are_not_proof_of_zero_unrecorded_transfer']}
    raw['cumulative_conservative_raw_occupancy_bytes']=raw['baseline_physical_upper_bytes']+raw['r2_new_physical_raw_bytes']
    raw['cumulative_logical_download_bytes_observed']=raw['baseline_logical_download_bytes']+raw['r2_downloaded_body_bytes_by_request']
    raw['within_raw_policy']=raw['cumulative_conservative_raw_occupancy_bytes']<=raw['cap_bytes']
    return sorted(records.values(),key=lambda r:r['request_id']),raw


def dt(value):return datetime.fromisoformat(value.replace('Z','+00:00'))


def uncovered_seconds(start,end,ranges):
    pieces=[(start,end)]
    for left,right in ranges:
        revised=[]
        for a,b in pieces:
            if right<=a or left>=b:revised.append((a,b));continue
            if a<left:revised.append((a,min(b,left)))
            if b>right:revised.append((max(a,right),b))
        pieces=revised
    return sum((Decimal(str((b-a).total_seconds())) for a,b in pieces),Decimal(0))


def online_clocks(work,receipts,names):
    base=read(work/'baseline/FINAL_RESOURCE_ADDENDUM.json');sessions=[];ranges=[]
    for path in sorted((work/'private/online_sessions').glob('*.json')):
        s=read(path)
        if not s.get('closed'):raise RuntimeError('Cannot finalize metrics with an open online session')
        record={k:s.get(k) for k in ('label','probe','kind','started_at_utc','ended_at_utc','elapsed_seconds','status','closed')}
        record.update(source=path.relative_to(work).as_posix(),sha256=sha(path));sessions.append(record)
        ranges.append((dt(s['started_at_utc']),dt(s['ended_at_utc'])))
    # HTTP time inside any session is already included in that lifecycle wall
    # envelope. Only the uncovered part of an out-of-session request is additive.
    extra=[];missing=[]
    for r in receipts:
        if r['origin']!='R2_NEW':continue
        if r.get('utc') is None or r.get('elapsed_seconds') is None:
            missing.append(r['request_id']);continue
        start=dt(r['utc']);end=start+timedelta(seconds=float(r['elapsed_seconds']))
        residual=uncovered_seconds(start,end,ranges)
        if residual>0:extra.append({'request_id':r['request_id'],'operation':r['operation'],'uncovered_http_seconds':str(residual),'shared_charge_to_each_probe':True})
    shared=sum((Decimal(str(s['elapsed_seconds'])) for s in sessions if s['probe']=='SHARED'),Decimal(0))
    extra_total=sum((Decimal(r['uncovered_http_seconds']) for r in extra),Decimal(0))
    # Distinct serial sessions should not overlap; a repeat envelope would not
    # be a legitimate second charge. Fail instead of silently double-counting.
    ordered=sorted(ranges)
    if any(a[1]>b[0] for a,b in zip(ordered,ordered[1:])):raise ValueError('Overlapping online session envelopes')
    probes={}
    for name in names:
        direct=sum((Decimal(str(s['elapsed_seconds'])) for s in sessions if s['probe']==name),Decimal(0))
        total=Decimal(base['per_probe_cumulative_upper_seconds'])+direct+shared+extra_total
        probes[name]={'baseline_cumulative_upper_seconds':base['per_probe_cumulative_upper_seconds'],
                      'r2_direct_sessions_seconds':str(direct),'r2_shared_sessions_seconds':str(shared),
                      'r2_extra_uncovered_http_seconds':str(extra_total),'cumulative_upper_seconds':str(total),
                      'remaining_seconds':str(Decimal(5400)-total),'within_90_minutes':total<=5400,
                      'legacy_unknown_durations_preserved_in_baseline_upper':True}
    return {'probes':probes,'sessions':sessions,'non_session_request_parts':extra,'missing_new_request_timing':missing,
            'offline_replay_zero_not_used_to_reset_clock':True,'http_and_server_times_not_added_again_inside_sessions':True}


def kind_counts(rows):return {kind:sum(r.get('event_kind')==kind for r in rows) for kind in KINDS}


def normalized(rows):
    events,gaps=normalize_rows(rows)
    return {e.event_id:asdict(e) for e in events},gaps


def gas_table(name,candidates,contexts):
    cand_ids={e['tx_hash'] for e in candidates};context_ids={e['tx_hash'] for e in contexts}
    rows=[]
    for tx in sorted(cand_ids|context_ids):
        facts=[e for e in candidates+contexts if e['tx_hash']==tx and e['kind']=='top']
        values={(e.get('gas_used'),e.get('gas_price'),e.get('gas_raw')) for e in facts if e.get('gas_used') is not None and e.get('gas_price') is not None}
        row={'probe':name,'tx_hash':tx,'candidate_membership':tx in cand_ids,'context_membership':tx in context_ids,
             'status':'MISSING_TOP_TRANSACTION_GAS','gas_used':None,'gas_price':None,'gas_raw':None,
             'source_share':None,'scope':'OBSERVED_TRANSACTION_CONTEXT_ONLY','lp_enforcement_claimed':False}
        if len(values)>1:row['status']='CONFLICTING_GAS_FACTS'
        elif values:
            used,price,fee=next(iter(values));product=int(used)*int(price)
            row.update(status='OBSERVED' if fee in (None,product) else 'CONFLICTING_GAS_PRODUCT',gas_used=str(used),gas_price=str(price),gas_raw=str(product))
        rows.append(row)
    return rows


def metrics(work,replay,output):
    work,replay,output=map(lambda p:Path(p).resolve(),(work,replay,output))
    if not replay.is_relative_to(work) or not output.is_relative_to(work):raise ValueError('Metrics work/replay/output must stay inside revision')
    expected_outputs={'METRICS.json','events.csv','intervals.csv','frontiers_and_stops.csv',
                      'jobs.csv','pages.csv','target_amounts.csv','gas_candidate_context.csv','requests.csv'}
    if output.exists() and any(p.name not in expected_outputs or not p.is_file() for p in output.iterdir()):
        raise ValueError('Existing metrics directory contains unrelated files')
    output.mkdir(parents=True,exist_ok=True)
    policy=read(work/'configs/STAGE1B_R2_POLICY.json');sources=read(replay/'source_manifest.json')
    source_by_name={r['query']:r for r in sources};statuses={r['name']:r for r in read(replay/'summary.json')}
    jobs,page_rows,job_gaps=read_jobs(work);budget=budget_snapshot(work,jobs)
    receipts,raw=receipt_inventory(work,jobs);clocks=online_clocks(work,receipts,[p['name'] for p in policy['query_pilots']])
    events_csv=[];interval_csv=[];frontier_csv=[];amount_csv=[];gas_csv=[];probes=[];normalization_gaps=[]
    used_job_probes={}
    for pilot in policy['query_pilots']:
        name=pilot['name'];folder=replay/name;source=source_by_name[name];status=statuses[name]
        collection=read(folder/'collection.json');graph=read(folder/'fixed_graph.json');scope=read(folder/'model_scope.json')
        baseline=read(work/'baseline/r1_observed_graph'/name/'collection.json')
        baseline_status=read(work/'baseline/r1_observed_graph'/name/'status.json')
        used=source['used_jobs'];raw_old=[];raw_new=[];all_raw=[];seen_batches=set();seen_intervals=set();normal_rows=[]
        per_kind={k:{'queried_intervals':0,'complete_intervals':0,'success_zero_row_intervals':0,'observed_rows':0} for k in KINDS}
        interval_old=interval_new=0
        for use in used:
            parent=use.get('batch_parent_logical_job_id',use['logical_job_id']);item=jobs[parent]
            used_job_probes.setdefault(parent,set()).add(name)
            if sha(item['folder']/'job.json')!=use['job_file_sha256']:
                raise ValueError('Replay used job artifact no longer matches source manifest')
            interval_id=use.get('interval_id')
            rows=[r for r in item['rows'] if interval_id is None or r.get('interval_id')==interval_id]
            if len(rows)!=use['exported_rows']:raise ValueError('Source manifest interval row count differs from actual pages')
            key=(parent,interval_id)
            if key in seen_intervals:raise ValueError('Duplicate source acquisition interval')
            seen_intervals.add(key);normal_rows.extend(rows)
            if item['origin']=='R2_NEW':interval_new+=1
            else:interval_old+=1
            if parent not in seen_batches:
                seen_batches.add(parent);all_raw.extend(item['rows'])
                (raw_new if item['origin']=='R2_NEW' else raw_old).extend(item['rows'])
            counts=kind_counts(rows)
            line={'record_type':'ACQUIRED_SOURCE_INTERVAL','probe':name,'query_id':pilot['query_id'],
                  'job_id':parent,'interval_id':interval_id,'origin':item['origin'],'execution_id':use['execution_id'],
                  **use['scope'],'export_complete':bool(use['export_complete'] and item['progress']['complete']),
                  'raw_interval_rows':len(rows),'normalization_gap_count':len(use.get('normalization_gaps',[])),
                  'sql_sha256':use['sql_sha256'],'scope_is_candidate_window_not_full_address_history':True}
            for kind in KINDS:
                queried=item['surfaces'][kind];complete=queried and line['export_complete']
                line[kind+'_queried']=queried;line[kind+'_observed_rows']=counts[kind]
                line[kind+'_coverage_status']='SUCCESS_NO_ROWS_IN_QUERIED_SCOPE' if complete and counts[kind]==0 else 'COMPLETE_DECLARED_INDEX_ROWS' if complete else 'INCOMPLETE_OR_UNQUERIED'
                per_kind[kind]['queried_intervals']+=queried;per_kind[kind]['complete_intervals']+=complete
                per_kind[kind]['success_zero_row_intervals']+=complete and counts[kind]==0;per_kind[kind]['observed_rows']+=counts[kind]
            interval_csv.append(line)
        norm_old,old_gaps=normalized(raw_old);norm_new,new_gaps=normalized(raw_new);norm_all,all_gaps=normalized(normal_rows)
        normalization_gaps.extend({'probe':name,'origin':origin,**gap} for origin,gs in [('INHERITED',old_gaps),('R2_NEW',new_gaps),('ALL_OBSERVED',all_gaps)] for gap in gs)
        candidate_ids={e['event_id'] for e in collection['candidate_events']};context_ids={e['event_id'] for e in collection['context_events']}
        baseline_candidates={e['event_id'] for e in baseline['candidate_events']}
        baseline_contexts={e['event_id'] for e in baseline['context_events']}
        by_id=dict(norm_all)
        for e in collection['candidate_events']+collection['context_events']:by_id.setdefault(e['event_id'],e)
        for eid,event in sorted(by_id.items()):
            events_csv.append({'probe':name,'query_id':pilot['query_id'],'candidate':eid in candidate_ids,'context':eid in context_ids,
                               'seen_in_inherited_exports':eid in norm_old,'seen_in_r2_exports':eid in norm_new,
                               'new_physical_event_not_in_inherited_exports':eid in norm_new and eid not in norm_old,
                               'new_candidate_vs_r1':eid in candidate_ids and eid not in baseline_candidates,
                               **{k:event.get(k) for k in ('event_id','tx_hash','sender','recipient','asset','amount_raw','block','tx_index','timestamp','kind','log_index','trace_address','execution_index','success','block_hash','gas_raw','gas_used','gas_price','provenance')}})
        for coverage in collection['coverage']:
            interval_csv.append({'record_type':'ARRIVAL_COVERAGE','probe':name,'query_id':pilot['query_id'],
                                 **{k:coverage.get(k) for k in ('address','asset','start_block','end_block','start_time','end_time','complete','basis','execution_id','logical_job_id','state_key','returned_events')},
                                 'scope_is_candidate_window_not_full_address_history':True})
        for state_kind,rows in [('STATE',collection['states']),('STOP',collection['stops']),('UNRESOLVED_FRONTIER',collection['unresolved_frontier'])]:
            for row in rows:
                state=row['state'];arrival=state['arrival'];identity=row.get('identity',{})
                frontier_csv.append({'probe':name,'query_id':pilot['query_id'],'record_type':state_kind,
                                     'address':state['address'],'asset':state['asset'],'depth':state['depth'],'local_end':state['local_end'],
                                     'arrival_event_id':arrival['event_id'],'arrival_timestamp':arrival['timestamp'],
                                     'reason':row.get('reason'),'identity_kind':identity.get('kind'),'identity_class':identity.get('identity_class'),
                                     'actor':identity.get('actor'),'lookup_status':identity.get('lookup_status',identity.get('status'))})
        gas=gas_table(name,collection['candidate_events'],collection['context_events']);gas_csv.extend(gas)
        lp_path=folder/'lp/lp_fixed_graph_result.json';lp_status='NOT_EXECUTED';group_count=entry_count=alias_count=0;target_event_ids=set()
        if lp_path.exists():
            lp=read(lp_path)
            if lp['graph_file_sha256']!=sha(folder/'fixed_graph.json'):raise ValueError('Amount result references a different fixed graph')
            lp_status='NO_OBSERVED_TARGET' if not lp['results'] else 'CONDITIONAL_FIXED_GRAPH_INTERVALS'
            for target,data in lp['results'].items():
                group_count+=1
                target_event_ids.update(data['joint']['objective_events'])
                for level,eid,result,alias in amount_objectives(data):
                    if level=='ENTRY_EVENT':
                        if alias:alias_count+=1
                        else:entry_count+=1
                    amount_csv.append({'probe':name,'query_id':pilot['query_id'],'level':level,'target_group':target,'event_id':eid,
                                       'asset':result['asset'],'status':result['status'],'lower_raw':result.get('lower_raw'),'upper_raw':result.get('upper_raw'),
                                       'positive_support':result.get('positive_support'),'objective_events':result.get('objective_events'),
                                       'singleton_joint_alias':alias,'additional_solve_claimed':False if alias else None,
                                       'graph_sha256':sha(folder/'fixed_graph.json'),'lp_result_sha256':sha(lp_path),'model_scope':lp['scope'],
                                       'global_case_truth_claimed':False})
        complete=(collection['status']=='COMPLETED_WITHIN_DECLARED_SCOPE' and not collection['unresolved_frontier']
                  and all(c['complete_intervals']==len(used) for c in per_kind.values()) and not all_gaps and not collection.get('fact_conflicts'))
        probes.append({'name':name,'query_id':pilot['query_id'],'seed_event_id':pilot['seed_event_id'],
                       'declared_scope':{k:pilot[k] for k in ('start_block','end_block','start_time_utc','end_time_utc','max_acquisition_depth')},
                       'provider_candidate_window_status':'COMPLETED_WITHIN_DECLARED_SCOPE' if complete else 'PARTIAL',
                       'collector_status':collection['status'],'raw_export_rows_inherited_reused':len(raw_old),'raw_export_rows_r2_downloaded':len(raw_new),
                       'raw_export_rows_total_unique_jobs':len(all_raw),'raw_interval_row_occurrences':len(normal_rows),
                       'normalized_unique_events_in_inherited_exports':len(norm_old),'normalized_unique_events_in_r2_exports':len(norm_new),
                       'new_observed_physical_event_ids':len(set(norm_new)-set(norm_old)),'normalized_unique_observed_events_total':len(norm_all),
                       'raw_kind_counts':kind_counts(all_raw),'inherited_kind_counts':kind_counts(raw_old),'new_kind_counts':kind_counts(raw_new),
                       'duplicate_raw_occurrences_over_unique_events':len(normal_rows)-len(norm_all) if not all_gaps else None,
                       'candidate_events':len(candidate_ids),'baseline_candidate_events':len(baseline_candidates),
                       'new_candidate_event_ids':len(candidate_ids-baseline_candidates),'removed_candidate_event_ids':len(baseline_candidates-candidate_ids),
                       'context_event_entries':len(collection['context_events']),'distinct_context_event_ids':len(context_ids),
                       'candidate_context_overlap_event_ids':len(candidate_ids&context_ids),'baseline_context_entries':len(baseline['context_events']),
                       'new_context_event_ids':len(context_ids-baseline_contexts),'stops_by_reason':dict(Counter(r['reason'] for r in collection['stops'])),
                       'states':len(collection['states']),'unresolved_frontiers':len(collection['unresolved_frontier']),
                       'inherited_acquisition_intervals':interval_old,'new_acquisition_intervals':interval_new,'queried_addresses':len({u['scope']['address'] for u in used}),
                       'coverage_by_event_kind':per_kind,'normalization_gaps':len(all_gaps),'fact_conflicts':len(collection.get('fact_conflicts',[])),
                       'quarantined_facts':len(collection.get('quarantined_facts',[])),'rejected_or_inflight_jobs':source.get('rejected_or_inflight_jobs',[]),
                       'all_ethereum_state_or_global_ledger_complete_claimed':False,'balance_gas_amount_scope':scope['status'],
                       'balance_unknown_pairs':sum(v is None for v in graph['initial_balances'].values()),'balance_known_zero_pairs':sum(v in (0,'0') for v in graph['initial_balances'].values()),
                       'gas_candidate_transactions_observed':sum(r['candidate_membership'] and r['status']=='OBSERVED' for r in gas),
                       'gas_candidate_transaction_context_wei':str(sum(int(r['gas_raw']) for r in gas if r['candidate_membership'] and r['status']=='OBSERVED')),
                       'gas_source_attribution_known':False,'lp_status':lp_status,'target_joint_intervals':group_count,
                       'target_event_intervals':entry_count+alias_count,'explicit_target_event_intervals':entry_count,
                       'singleton_joint_event_aliases':alias_count,'unique_target_event_count':len(target_event_ids),
                       'separately_solved_objectives':group_count+entry_count,'cross_address_sum_optimized':False,
                       'new_data_delta_source':'R1 observed graph vs selected R2 replay; not code-only or label-only attribution',
                       'saved_status_comparison':{'reported_raw_rows':status['live_exported_rows'],'reported_normalized_unique_events':status['live_normalized_distinct_events'],
                                                  'raw_rows_match':status['live_exported_rows']==len(all_raw),'normalized_unique_match':status['live_normalized_distinct_events']==len(norm_all)},
                       'clock':clocks['probes'][name],'input_sha256':{f:sha(folder/f) for f in ('collection.json','fixed_graph.json','model_scope.json','status.json')}})
    budget_by_id={r['job_id']:r for r in budget['jobs']};job_csv=[]
    for logical,item in jobs.items():
        job=item['job'];job_csv.append({'job_id':logical,'origin':item['origin'],'kind':item['kind'],'query_label':job.get('query_label'),
                                       'probes_used_by_replay':sorted(used_job_probes.get(logical,[])),'execution_id':job.get('execution_id'),'state':job['state'],
                                       'saved_artifact':item['path'],'job_sha256':item['sha256'],'sql_sha256':job['sql_sha256'],
                                       'saved_export_rows':len(item['rows']),'complete_page_chain':item['progress']['complete'],
                                       'pages':item['progress']['successful_pages'],'used_by_selected_replay':logical in used_job_probes,
                                       **{k:v for k,v in budget_by_id.get(logical,{}).items() if k not in ('job_id','origin')}})
    for logical,row in budget_by_id.items():
        if logical not in jobs:job_csv.append(row|{'saved_artifact':None,'kind':'HISTORICAL_LEDGER_ONLY','execution_id':None,'note':'Execution/page artifacts not in this local minimal revision; historical risk retained.'})
    newjob=[j for j in jobs.values() if j['origin']=='R2_NEW']
    summary={'schema_version':'stage1b-r2-pilot-metrics-1','created_at_utc':datetime.now(timezone.utc).isoformat(),
             'run_id':work.name,'replay':replay.relative_to(work).as_posix(),'external_acceptance_status':'PENDING_REVIEW',
             'probes':probes,'new_jobs':{'total':len(newjob),'by_kind':dict(Counter(j['kind'] for j in newjob)),
                                      'execution_states':dict(Counter(j['job']['state'] for j in newjob)),
                                      'complete_exports':sum(j['progress']['complete'] for j in newjob),
                                      'actual_pages':sum(j['progress']['successful_pages'] for j in newjob),
                                      'all_kinds_downloaded_rows':sum(len(j['rows']) for j in newjob)},
             'budget':budget,'raw_resources':raw,'online_time':clocks,'job_artifact_gaps':job_gaps,
             'normalization_gaps':normalization_gaps,'source_manifest_sha256':sha(replay/'source_manifest.json'),
             'baseline_resource_sha256':sha(work/'baseline/FINAL_RESOURCE_ADDENDUM.json'),
             'code_sha256':sha(Path(__file__)),'live_requests_by_metrics':0,'collector_reexecuted':False,
             'read_only_budget_snapshot':True,'replay_counts_not_used_as_actual_network_performance':True}
    for name,rows in [('events.csv',events_csv),('intervals.csv',interval_csv),('frontiers_and_stops.csv',frontier_csv),
                      ('jobs.csv',job_csv),('pages.csv',page_rows),('target_amounts.csv',amount_csv),('gas_candidate_context.csv',gas_csv),
                      ('requests.csv',receipts)]:csv_file(output/name,rows)
    summary['tables']={p.name:{'bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(output.glob('*.csv'))}
    write(output/'METRICS.json',summary)
    return summary


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('work','replay','output'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args();result=metrics(args.work,args.replay,args.output)
    print(json.dumps({'output':str(args.output),'probes':[{k:p[k] for k in ('name','provider_candidate_window_status','candidate_events','new_observed_physical_event_ids','unresolved_frontiers')} for p in result['probes']]}))


if __name__=='__main__':main()
