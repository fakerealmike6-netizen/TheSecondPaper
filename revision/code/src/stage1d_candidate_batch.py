"""Batch exact uncollected rectangles without claiming their bounding envelope.

The inherited native index, transaction-local token evidence and trace ancestor
rules are unchanged. Each successful rectangle owns a separate coverage claim;
all claims reference one immutable physical event file. No reference is read.
"""
import argparse, hashlib, json
from dataclasses import asdict
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha
from collector import NATIVE
from provider_dune import exact_hex, integer, sql_time, timestamp, normalize_rows
from context_access_r3 import read, sha
from page_attempts import atomic_json
from stage1d_acquisition import interval_sql, save_sql, replay, acquire_labels
from stage1d_runtime import Runtime, execute_sql, result_rows

FIELDS=('start_block','end_block','start_time','end_time')


def rectangles(pending,maximum=32):
    if isinstance(maximum,bool) or not isinstance(maximum,int) or not 1<=maximum<=32:
        raise ValueError('At most32 exact uncovered rectangles per execution')
    out=[]
    for p in pending:
        if p.get('asset',NATIVE)!=NATIVE:raise ValueError('This batch retains native index scope only')
        item={'address':exact_hex(p['address'],20),'asset':NATIVE,
              'start_block':integer(p['start_block']),'end_block':integer(p['end_block']),
              'start_time':timestamp(p['start_time']),'end_time':timestamp(p['end_time'])}
        if item['start_block']>item['end_block'] or item['start_time']>item['end_time']:
            raise ValueError('Inverted uncovered rectangle')
        if item not in out:out.append(item)
        if len(out)==maximum:break
    return out


def build_sql(pending,maximum=32):
    selected=rectangles(pending,maximum)
    if not selected:raise ValueError('No pending rectangles')
    first=selected[0]['address']
    low=min(p['start_block'] for p in selected);high=max(p['end_block'] for p in selected)
    t0=min(p['start_time'] for p in selected);t1=max(p['end_time'] for p in selected)
    sql=interval_sql([first],low,high,t0,t1)
    values=',\n  '.join("(%s,%d,%d,TIMESTAMP '%s',TIMESTAMP '%s')" %
        (p['address'],p['start_block'],p['end_block'],sql_time(p['start_time']),sql_time(p['end_time'])) for p in selected)
    prefix='WITH request_windows(address,first_block,last_block,first_time,last_time) AS (VALUES\n  '+values+'\n), tx_window AS ('
    sql=sql.replace('WITH tx_window AS (',prefix,1)
    def guard(alias,block,time,fields):
        return ('EXISTS (SELECT 1 FROM request_windows w WHERE '
            +alias+'.'+block+' BETWEEN w.first_block AND w.last_block AND '
            +alias+'.'+time+' BETWEEN w.first_time AND w.last_time AND ('
            +' OR '.join(field+'=w.address' for field in fields)+'))')
    substitutions={
        f't."from" = {first} OR t."to" = {first}':guard('t','block_number','block_time',['t."from"','t."to"']),
        f'(t."from"={first} OR COALESCE(t."to",t.address)={first} OR t.refund_address={first})':
            guard('t','block_number','block_time',['t."from"','COALESCE(t."to",t.address)','t.refund_address']),
        f'(e."from"={first} OR e."to"={first})':guard('e','evt_block_number','evt_block_time',['e."from"','e."to"'])}
    for before,after in substitutions.items():
        if sql.count(before)!=1:raise ValueError('Inherited index template changed; batch transformation must be reviewed')
        sql=sql.replace(before,after,1)
    return sql,selected


def acquire(work,query,pending,collection_path,maximum=32):
    work=Path(work).resolve();collection_path=Path(collection_path).resolve()
    if not collection_path.is_relative_to(work):raise ValueError('Frontier evidence outside workspace')
    sql,selected=build_sql(pending,maximum)
    for p in selected:
        if not (query['start_block']<=p['start_block']<=p['end_block']<=query['end_block']):
            raise ValueError('Uncovered blocks exceed frozen query scope')
        if not (query['scope']['start_time']<=p['start_time']<=p['end_time']<=query['scope']['end_time']):
            raise ValueError('Uncovered time exceeds frozen query scope')
    digest=sha(collection_path);frozen_dep=work/'private/stage1d_frontier_evidence'/(digest+'.json')
    frozen_dep.parent.mkdir(parents=True,exist_ok=True)
    if not frozen_dep.exists():frozen_dep.write_bytes(collection_path.read_bytes())
    if sha(frozen_dep)!=digest:raise ValueError('Immutable frontier evidence changed')
    dep={'path':frozen_dep.relative_to(work).as_posix(),'sha256':digest}
    intervals=[dict(p,query_id=query['query_id'],kind='candidate') for p in selected]
    freeze=save_sql(work,sql,'candidate',query,intervals,[dep],sorted({p['address'] for p in selected}))
    result=execute_sql(work,freeze,query['name'],'candidate_exact_rectangles_'+query['name'])
    if result.get('status')=='COMPLETED_EXPORTED':
        raw=result_rows(work,result['job_folder']);events,gaps=normalize_rows(raw)
        folder=work/'derived/stage1d/intervals';folder.mkdir(parents=True,exist_ok=True)
        eid=sha(freeze.parent/'query.sql');ep=folder/(eid+'.events.json')
        payload=[asdict(e) for e in events]
        if ep.exists() and read(ep)!=payload:raise ValueError('Immutable batched physical events changed')
        if not ep.exists():atomic_json(ep,payload)
        for p in selected:
            key=hashlib.sha256(json.dumps({'sql_sha256':eid,'rectangle':p},sort_keys=True).encode()).hexdigest()
            target=folder/(key+'.coverage.json')
            record={'evidence_id':eid,'addresses':[p['address']],'asset':NATIVE,
                **{k:p[k] for k in FIELDS},'complete':not gaps,'normalization_gaps':gaps,'raw_rows':len(raw),
                'events_path':ep.relative_to(work).as_posix(),'events_sha256':sha(ep),
                'query_id_at_acquisition':query['query_id'],'scope_id_at_acquisition':query['scope_id'],
                'job_folder':Path(result['job_folder']).relative_to(work).as_posix(),
                'coverage_basis':'ONE_EXACT_REQUEST_RECTANGLE_NOT_BATCH_ENVELOPE',
                'shared_physical_content_identity':eid,'batch_rectangle_count':len(selected)}
            if target.exists() and read(target)!=record:raise ValueError('Immutable exact coverage changed')
            if not target.exists():atomic_json(target,record)
        result.update(raw_rows=len(raw),unique_events=len(events),normalization_gaps=gaps,exact_rectangles=len(selected))
    return result


def run(work,query,max_batches=1,maximum=32):
    work=Path(work).resolve();folder=work/'derived/stage1d/queries'/query['name']
    attempts=read(folder/'acquisition_attempts.json') if (folder/'acquisition_attempts.json').exists() else []
    for _ in range(max_batches):
        collection,pending=replay(work,query)
        try:
            label=acquire_labels(work,query,collection)
            if label.get('new_addresses') or label.get('labels_updated'):
                attempts.append(dict(label,phase='labels'));atomic_json(folder/'acquisition_attempts.json',attempts)
                collection,pending=replay(work,query)
        except Exception as exc:
            attempts.append({'phase':'labels','status':'LABEL_LOOKUP_FAILED_OR_RESOURCE_BLOCKED','error_class':type(exc).__name__,'reason':str(exc)})
            atomic_json(folder/'acquisition_attempts.json',attempts)
        if not pending:break
        try:result=acquire(work,query,pending,folder/'collection.json',maximum)
        except Exception as exc:result={'status':'ACQUISITION_PARTIAL','error_class':type(exc).__name__,'reason':str(exc)}
        attempts.append(result);atomic_json(folder/'acquisition_attempts.json',attempts)
        if result.get('status')!='COMPLETED_EXPORTED' or Runtime().raw_risk(work)>=536870912:break
    collection,pending=replay(work,query)
    result={'name':query['name'],'query_id':query['query_id'],'scope_id':query['scope_id'],
        'window_mode':query['window_mode'],'max_depth':query['max_acquisition_depth'],
        'collection_status':collection['status'],'pending_intervals':len(pending),
        'candidate_events':len(collection['candidate_events']),'attempts':attempts}
    atomic_json(folder/'ACQUISITION_STATUS.json',result)
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--query',required=True)
    p.add_argument('--max-batches',type=int,default=1);p.add_argument('--max-rectangles',type=int,default=32);a=p.parse_args()
    q=next(q for q in active_batch(a.work)['queries'] if q['name']==a.query)
    result=run(a.work,q,a.max_batches,a.max_rectangles)
    print(json.dumps({k:v for k,v in result.items() if k!='attempts'}))
