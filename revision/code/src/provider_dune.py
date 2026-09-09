"""Dune candidate-index adapter with injected, externally budgeted callbacks.

No networking, authentication or billing changes occur in this module. Callers
must reserve each SQL execution plus ALL exports as one logical job, enforce
provider caps and retain actual over-reservation consumption. SQL has no LIMIT,
reference target list or financial pruning. API export pages alone are bounded.

Schema evidence: docs.dune.com Ethereum raw tx/traces pages and the official
duneanalytics/spellbook transfers_base macro, documented in the schema manifest.
"""
from __future__ import annotations
import hashlib,json,re
from datetime import datetime,timezone
from pathlib import Path
from collector import Event,FetchResult,NATIVE
from page_attempts import AttemptStore, RequestBlocked
from page_contract import validate_page, initial_progress

VERSION='stage1b-dune-index-adapter-1.0'

def exact_hex(value,size):
    if not isinstance(value,str) or not re.fullmatch(r'0x[0-9a-fA-F]{'+str(size*2)+r'}',value):
        raise ValueError('complete hexadecimal locator required')
    return value.lower()

def integer(value,*,optional=False):
    if value in (None,'') and optional:return None
    if isinstance(value,bool) or not isinstance(value,(str,int)):raise ValueError('exact integer required')
    if isinstance(value,str) and not re.fullmatch(r'[0-9]+',value):raise ValueError('decimal raw integer required')
    out=int(value)
    if out<0:raise ValueError('negative integer')
    return out

def timestamp(value):
    if isinstance(value,int):return value
    if not isinstance(value,str):raise ValueError('timestamp required')
    value=value.removesuffix(' UTC').replace('Z','+00:00')
    d=datetime.fromisoformat(value)
    return int((d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d).timestamp())

def sql_time(value):
    return datetime.fromtimestamp(timestamp(value),timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def build_interval_sql(address,asset,start_block,end_block,*,start_time,end_time):
    """Bounded actual-address interval; include native/gas context for token state.

    References define only caller-supplied outer time/block bounds. Incoming rows
    provide context and must not independently create candidate arrival states.
    Native states include standard token logs for transaction-local context;
    unsupported conversions remain collector/component boundaries.
    """
    address=exact_hex(address,20);start_block=integer(start_block);end_block=integer(end_block)
    if start_block>end_block or timestamp(start_time)>timestamp(end_time):raise ValueError('inverted interval')
    token=None
    if asset!=NATIVE:
        if not asset.startswith('erc20:eip155:1:'):raise ValueError('unsupported asset')
        token=exact_hex(asset.rsplit(':',1)[1],20)
    t0,t1=sql_time(start_time),sql_time(end_time)
    bounds=f"block_number BETWEEN {start_block} AND {end_block} AND block_time BETWEEN TIMESTAMP '{t0}' AND TIMESTAMP '{t1}'"
    ercbounds=f"e.evt_block_number BETWEEN {start_block} AND {end_block} AND e.evt_block_time BETWEEN TIMESTAMP '{t0}' AND TIMESTAMP '{t1}'"
    token_filter=f' AND e.contract_address = {token}' if token else ''
    # CAST raw amounts to VARCHAR avoids JSON numeric rounding. Root trace is
    # excluded because ethereum.transactions already owns its physical capacity.
    return f'''-- {VERSION}; ordinary acquisition, no reference neighbors
WITH tx_window AS (
  SELECT hash, "from", "to", value, block_number, "index", block_time,
         success, gas_used, gas_price
  FROM ethereum.transactions WHERE {bounds}
), trace_window AS (
  SELECT tx_hash, "from", "to", address, refund_address, value, block_number,
         tx_index, block_time, trace_address, success, tx_success, type, call_type
  FROM ethereum.traces WHERE {bounds}
), indexed_events AS (
  SELECT 'top' AS event_kind, t.hash AS tx_hash, t."from" AS sender,
         t."to" AS recipient, CAST(t.value AS VARCHAR) AS amount_raw,
         t.block_number, t."index" AS tx_index, t.block_time,
         CAST(NULL AS BIGINT) AS log_index, CAST(NULL AS ARRAY(BIGINT)) AS trace_address,
         t.success, CAST(NULL AS VARBINARY) AS contract_address,
         CAST(t.gas_used AS VARCHAR) AS gas_used, CAST(t.gas_price AS VARCHAR) AS gas_price,
         CAST(NULL AS VARCHAR) AS trace_type, CAST(NULL AS VARCHAR) AS call_type
  FROM tx_window t WHERE t."from" = {address} OR t."to" = {address}
  UNION ALL
  SELECT 'internal', t.tx_hash, t."from",
         CASE WHEN t.type = 'suicide' THEN t.refund_address ELSE COALESCE(t."to",t.address) END,
         CAST(t.value AS VARCHAR), t.block_number, CAST(t.tx_index AS BIGINT), t.block_time,
         CAST(NULL AS BIGINT), t.trace_address,
         t.success AND t.tx_success AND NOT EXISTS (
           SELECT 1 FROM trace_window a WHERE a.tx_hash=t.tx_hash
           AND a.block_number=t.block_number AND a.success=false
           AND cardinality(a.trace_address)<cardinality(t.trace_address)
           AND slice(t.trace_address,1,cardinality(a.trace_address))=a.trace_address
         ),
         CAST(NULL AS VARBINARY), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),t.type,t.call_type
  FROM trace_window t
  WHERE cardinality(t.trace_address)>0 AND t.value>UINT256 '0'
    AND (t.call_type IS NULL OR t.call_type NOT IN ('delegatecall','callcode','staticcall'))
    AND (t."from"={address} OR COALESCE(t."to",t.address)={address} OR t.refund_address={address})
  UNION ALL
  SELECT 'erc20', e.evt_tx_hash, e."from", e."to", CAST(e.value AS VARCHAR),
         e.evt_block_number, t."index", e.evt_block_time, e.evt_index,
         CAST(NULL AS ARRAY(BIGINT)), t.success, e.contract_address,
         CAST(NULL AS VARCHAR),CAST(NULL AS VARCHAR),CAST(NULL AS VARCHAR),CAST(NULL AS VARCHAR)
  FROM erc20_ethereum.evt_Transfer e LEFT JOIN tx_window t
    ON t.hash=e.evt_tx_hash AND t.block_number=e.evt_block_number
  WHERE {ercbounds} AND (e."from"={address} OR e."to"={address}){token_filter}
)
SELECT * FROM indexed_events
ORDER BY block_number,tx_index,tx_hash,event_kind,log_index,trace_address
'''

def build_scope_interval_sql(scope,address,asset,start_block,*,start_time,end_time=None,end_block=None):
    """Stage1D SQL using the frozen per-arrival end and matching UTC dates.

    SQL identity is content identity, so equivalent short-window modes can
    share successful pages. Scope/version identity is persisted separately.
    """
    end_block = scope.end_block if end_block is None else end_block
    if not (scope.start_block <= start_block <= end_block <= scope.end_block):
        raise ValueError('request exceeds frozen block bounds')
    if not scope.start_time <= timestamp(start_time) <= scope.end_time:
        raise ValueError('request starts outside frozen timestamp bounds')
    local_end = scope.local_end(start_time)
    if end_time is not None and timestamp(end_time) > local_end:
        raise ValueError('request exceeds frozen arrival window')
    local_end = local_end if end_time is None else timestamp(end_time)
    sql = build_interval_sql(address,asset,start_block,end_block,
                             start_time=start_time,end_time=local_end)
    d0,d1 = sql_time(start_time)[:10],sql_time(local_end)[:10]
    return sql.replace('WHERE block_number BETWEEN',
        "WHERE block_date BETWEEN DATE '%s' AND DATE '%s' AND block_number BETWEEN" % (d0,d1))

def normalize_rows(rows):
    from physical_facts import PhysicalFactRegistry
    registry=PhysicalFactRegistry();gaps=[]
    for n,r in enumerate(rows):
        try:
            tx=exact_hex(r['tx_hash'],32);kind=r['event_kind']
            if r.get('success') not in (True,False):raise ValueError('transaction/trace success unresolved')
            sender=exact_hex(r['sender'],20);recipient=exact_hex(r['recipient'],20)
            trace=None;log=None;gas=None;asset=NATIVE
            if kind=='top':
                suffix='top'
                if r.get('gas_used') is not None and r.get('gas_price') is not None:gas=integer(r['gas_used'])*integer(r['gas_price'])
            elif kind=='internal':
                a=r.get('trace_address')
                if not isinstance(a,list) or not a:raise ValueError('non-root trace locator required')
                trace='_'.join(str(integer(x)) for x in a);suffix='trace:'+trace
                if r.get('call_type') in ('delegatecall','callcode','staticcall'):raise ValueError('non-value trace kind')
            elif kind=='erc20':
                log=integer(r['log_index']);suffix='log:'+str(log)
                asset='erc20:eip155:1:'+exact_hex(r['contract_address'],20)
            else:raise ValueError('unknown event kind')
            event=Event('eip155:1:tx:'+tx+':'+suffix,tx,sender,recipient,asset,integer(r['amount_raw']),integer(r['block_number']),integer(r.get('tx_index'),optional=True),timestamp(r['block_time']),kind,log,trace,integer(r.get('execution_index'),optional=True),r['success'],'DUNE_INDEX_'+VERSION,gas,
                        chain_id=r.get('chain_id','eip155:1'),block_hash=exact_hex(r['block_hash'],32) if r.get('block_hash') else None,
                        gas_used=integer(r.get('gas_used'),optional=True),gas_price=integer(r.get('gas_price'),optional=True))
            registry.add(event,raw=r)
        except (KeyError,TypeError,ValueError) as exc:
            gaps.append({'reason':'DUNE_NORMALIZATION_UNRESOLVED','row_index':n,'exception_type':type(exc).__name__})
    snapshot=registry.snapshot();gaps.extend(snapshot['conflicts'])
    events=[Event(**e) for e in snapshot['events']]
    # Deterministic presentation retains the historical top/trace/log grouping;
    # this key is not evidence of trace-versus-log execution chronology.
    events.sort(key=lambda e:(e.block,e.tx_index if e.tx_index is not None else -1,e.tx_hash,
                             {'top':0,'internal':1,'erc20':2}[e.kind],e.event_id))
    return events,gaps

def atomic_json(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.tmp');temp.write_text(json.dumps(data,sort_keys=True,separators=(',',':')),encoding='utf-8');temp.replace(path)

class DuneProvider:
    """execute_and_wait(sql,job_id) and export_page(execution_id,params,job_id).

    Both callbacks must use the SAME persisted logical-job budget reservation.
    The execution callback returns a completed Dune execution status object; the
    page callback returns unfiltered JSON from the official results endpoint.
    Metadata/status polling request counts can be returned in `_request_count`.
    Callbacks may raise a budget denial; uncertain submissions are never retried.
    """
    def __init__(self,execute_and_wait,export_page,cache_dir,*,page_size=50,max_pages=50,max_rows=25000,replay_only=False,raw_limit_bytes=536870912,account_context_ref='INJECTED_CALLER_ACCOUNT_CONTEXT'):
        if not 1<=page_size<=1000:raise ValueError('page_size must be 1..1000')
        self.execute_and_wait=execute_and_wait;self.export_page=export_page;self.cache=Path(cache_dir)
        self.cache.mkdir(parents=True,exist_ok=True);self.page_size=page_size;self.max_pages=max_pages;self.max_rows=max_rows
        self.replay_only=replay_only;self.raw_limit_bytes=raw_limit_bytes;self.new_raw_bytes=0;self.request_log=[]
        self.account_context_ref=account_context_ref
        self.attempts=AttemptStore(self.cache/'request_attempts.sqlite')
        from physical_facts import PhysicalFactRegistry
        self.fact_registry=PhysicalFactRegistry();self._fact_returns=[]
        self.bound_scope=None
        # Rebuild facts only from hash-validated successful request receipts.
        # This read-only replay prevents a fresh provider instance from losing
        # contradictions learned in earlier jobs of the same account context.
        for record in self.attempts.rows():
            identity=json.loads(record['identity'])
            if record['state']=='SUCCESS_VALIDATED' and identity['operation']=='results' and identity['account_context_ref']==self.account_context_ref:
                response,_,_=self.attempts.cached(identity)
                self._remember_facts(response['result']['rows'],identity['logical_job_id']+':'+identity['execution_id'])

    def bind_scope(self,scope):
        self.bound_scope=scope

    def _remember_facts(self,rows,source):
        events,gaps=normalize_rows(rows)
        for event in events:self.fact_registry.add(event,source=self.account_context_ref+':'+source)
        for gap in gaps:
            for version in gap.get('versions',[]):
                self.fact_registry.add(version['facts']|{'event_id':version['event_id'],'provenance':json.dumps(version.get('provenance',[]))},source=self.account_context_ref+':'+source,raw=version.get('raw'))
        ids={e.event_id for e in events}|{eid for g in gaps for eid in g.get('event_ids',[])}
        return ids,gaps

    def fetch_interval(self,address,asset,start_block,end_block,*,start_time,end_time,global_end_time):
        end_time=min(end_time,global_end_time)
        if self.bound_scope is not None and self.bound_scope.window_mode is not None:
            if end_block>self.bound_scope.end_block or global_end_time!=self.bound_scope.end_time:
                raise ValueError('provider request differs from bound scope')
            sql=build_scope_interval_sql(self.bound_scope,address,asset,start_block,start_time=start_time,end_time=end_time,end_block=end_block)
        else:
            sql=build_interval_sql(address,asset,start_block,end_block,start_time=start_time,end_time=end_time)
        known=[c for c in self.fact_registry.snapshot()['conflicts'] if any(
            address.lower() in (v['facts']['sender'],v['facts']['recipient']) and
            integer(start_block)<=v['facts']['block']<=integer(end_block) and timestamp(start_time)<=v['facts']['timestamp']<=min(timestamp(end_time),timestamp(global_end_time))
            for v in c['versions'])]
        if known:
            return FetchResult([], [{'provider':'Dune','complete':False,'basis':'PHYSICAL_FACT_CONFLICT','address':address}],False,known,
                               cache_hits=1,fact_conflicts=known,quarantined_facts=[v for c in known for v in c['versions']])
        jobid='dune:'+hashlib.sha256(sql.encode()).hexdigest();folder=self.cache/jobid.split(':')[1];folder.mkdir(exist_ok=True)
        sqlpath=folder/'query.sql'
        if sqlpath.exists() and sqlpath.read_text(encoding='utf-8')!=sql:raise ValueError('SQL hash collision')
        if not sqlpath.exists():sqlpath.write_text(sql,encoding='utf-8')
        if self.bound_scope is not None and self.bound_scope.window_mode is not None:
            atomic_json(folder/'scope_requests'/(self.bound_scope.scope_hash+'.json'),
                {'scope':self.bound_scope.freeze_dict(),'scope_hash':self.bound_scope.scope_hash,
                 'content_sql_sha256':jobid.split(':')[1],'start_block':start_block,'end_block':end_block,
                 'start_time':start_time,'end_time':end_time,'scope_identity_is_not_content_identity':True})
        jobpath=folder/'job.json';coverage=[];gaps=[];allrows=[];requests=0;hits=0;newbytes=0;complete=False
        progress=initial_progress();execution_identity=None
        try:
            execution_identity=AttemptStore.identity(self.account_context_ref,jobid,'SQL:'+jobid.split(':')[1],'execute',{'sql_sha256':jobid.split(':')[1]})
            if jobpath.exists():
                job=json.loads(jobpath.read_text());hits+=1
                if job.get('state')!='QUERY_STATE_COMPLETED' or not job.get('execution_id'):
                    return FetchResult(gaps=[{'reason':'DUNE_PRIOR_JOB_UNRESOLVED_NO_AUTOMATIC_RESUBMISSION','logical_job_id':jobid}],cache_hits=hits)
            else:
                if self.replay_only:raise RuntimeError('DUNE_CACHE_MISS_REPLAY_ONLY')
                # SQLite commits the unique right to dispatch before any callback.
                aid=self.attempts.dispatch(execution_identity)
                try:
                    atomic_json(jobpath,{'state':'SUBMITTING_OR_UNCERTAIN','logical_job_id':jobid})
                    requests+=1
                    job=self.execute_and_wait(sql,jobid)
                    requests+=max(0,int(job.get('_request_count',1))-1)
                    receipt={'operation':'execute','logical_job_id':jobid,'request_identity':execution_identity,'_callback_returned':True}
                    self.attempts.save_response(aid,job,receipt,jobpath,folder/'execution_receipt.json')
                    if job.get('state')!='QUERY_STATE_COMPLETED' or not job.get('execution_id'):
                        self.attempts.mark(aid,'INVALID_RESPONSE','Execution was not completed and explicitly bound')
                    else:self.attempts.validated(aid,{'complete':True,'next_offset':None})
                except BaseException as exc:
                    self.attempts.mark(aid,'UNKNOWN_TRANSPORT',type(exc).__name__)
                    raise
                if job.get('state')!='QUERY_STATE_COMPLETED' or not job.get('execution_id'):
                    return FetchResult(gaps=[{'reason':'DUNE_EXECUTION_NOT_COMPLETED','logical_job_id':jobid,'state':job.get('state')}],real_requests=requests)
            execution=job['execution_id'];offset=0;seen_offsets=set();total=None
            for page in range(self.max_pages):
                if offset in seen_offsets:raise ValueError('repeated offset')
                seen_offsets.add(offset);path=folder/f'page_{offset}.json';cache_hit=path.exists()
                params={'limit':self.page_size,'offset':offset}
                identity=AttemptStore.identity(self.account_context_ref,jobid,execution,'results',params)
                if cache_hit:
                    registered=self.attempts.get(identity)
                    if registered:
                        response,receipt,_=self.attempts.cached(identity)
                    else:
                        response=json.loads(path.read_text());receipt=None
                    hits+=1
                else:
                    if self.replay_only:raise RuntimeError('DUNE_PAGE_CACHE_MISS_REPLAY_ONLY')
                    if self.new_raw_bytes+self.page_size*4096>self.raw_limit_bytes:raise RuntimeError('DUNE_RAW_BYTES_RESOURCE_LIMIT')
                    if self.attempts.get(execution_identity) is None:
                        raise RequestBlocked('Legacy completed SQL has no dispatch history; missing page is not proof of an unsubmitted request')
                    # Never set filters/sample_count/allow_partial_results or
                    # ignore_max_credits_per_request, and never trust next_uri.
                    aid=self.attempts.dispatch(identity)
                    try:
                        requests+=1
                        response=self.export_page(execution,params,jobid)
                        if isinstance(response,dict):requests+=max(0,int(response.get('_request_count',1))-1)
                        receipt={'operation':'results','execution_id':execution,'logical_job_id':jobid,'parameters':params,'http_status':200,'error_class':None,'evidence_type':'INJECTED_CALLBACK_RESPONSE'}
                        self.attempts.save_response(aid,response,receipt,path,folder/f'page_{offset}_receipt.json')
                    except BaseException as exc:
                        self.attempts.mark(aid,'UNKNOWN_TRANSPORT',type(exc).__name__)
                        raise
                    size=path.stat().st_size;newbytes+=size;self.new_raw_bytes+=size
                try:
                    next_progress=validate_page(response,execution_id=execution,offset=offset,limit=self.page_size,progress=progress,status_metadata=job.get('result_metadata'),receipt=receipt,parameters=params)
                    if not cache_hit:self.attempts.validated(aid,next_progress)
                except Exception as exc:
                    if not cache_hit:self.attempts.mark(aid,'INVALID_RESPONSE',str(exc))
                    raise
                progress=next_progress
                data=response['result'];values=data['rows'];md=data['metadata'];total=progress['total'];next_offset=progress['next_offset']
                allrows.extend(values)
                terminal=next_offset is None
                page_complete=progress['complete']
                coverage.append({'provider':'Dune','logical_job_id':jobid,'execution_id':execution,'sql_sha256':jobid.split(':')[1],'address':address,'asset':asset,'start_block':start_block,'end_block':end_block,'start_time':start_time,'end_time':end_time,'offset':offset,'returned_rows':len(values),'total_row_count':total,'next_offset':next_offset,'complete':page_complete,'raw_path':str(path),'response_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'cache_hit':cache_hit})
                if terminal:
                    if not page_complete:gaps.append({'reason':'DUNE_TOTAL_COUNT_OR_PAGINATION_GAP','downloaded_rows':len(allrows),'total_row_count':total})
                    complete=page_complete;break
                if len(allrows)>=self.max_rows:gaps.append({'reason':'DUNE_ROW_RESOURCE_LIMIT','next_offset':next_offset});break
                if self.new_raw_bytes>=self.raw_limit_bytes:gaps.append({'reason':'DUNE_RAW_BYTES_RESOURCE_LIMIT','next_offset':next_offset});break
                offset=next_offset
            else:gaps.append({'reason':'DUNE_PAGE_RESOURCE_LIMIT','next_offset':offset})
        except Exception as exc:
            gaps.append({'reason':getattr(exc,'reason','DUNE_TRANSPORT_BUDGET_OR_RESULT_UNRESOLVED'),'exception_type':type(exc).__name__,'logical_job_id':jobid})
        ids,ngaps=self._remember_facts(allrows,jobid+':'+str(locals().get('execution','UNRESOLVED')))
        gaps.extend(ngaps);snapshot=self.fact_registry.snapshot()
        conflicts=[c for c in snapshot['conflicts'] if ids.intersection(c['event_ids'])]
        gaps.extend(c for c in conflicts if c not in gaps)
        unique={self.fact_registry.get(eid)['event_id']:Event(**self.fact_registry.get(eid)) for eid in ids if self.fact_registry.get(eid) is not None}
        if conflicts:
            unique={};coverage=[{**c,'complete':False,'invalidated_reason':'PHYSICAL_FACT_CONFLICT'} for c in coverage]
        # References returned earlier by this live provider are also revoked;
        # Collector independently invalidates all query descendants and stops.
        for prior,prior_ids in self._fact_returns:
            relevant=[c for c in snapshot['conflicts'] if prior_ids.intersection(c['event_ids'])]
            if relevant:
                prior.events=[];prior.complete=False;prior.fact_conflicts=relevant
                prior.quarantined_facts=[v for c in relevant for v in c['versions']]
                prior.coverage=[{**c,'complete':False,'invalidated_reason':'PHYSICAL_FACT_CONFLICT'} for c in prior.coverage]
                prior.gaps.extend(c for c in relevant if c not in prior.gaps)
        self.request_log.extend(coverage)
        result=FetchResult(list(unique.values()),coverage,complete and not gaps,gaps,newbytes,requests,hits,
                           fact_conflicts=conflicts,quarantined_facts=[v for c in conflicts for v in c['versions']])
        self._fact_returns.append((result,ids))
        return result
