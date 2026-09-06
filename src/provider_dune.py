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

def normalize_rows(rows):
    events=[];gaps=[]
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
            events.append(Event('eip155:1:tx:'+tx+':'+suffix,tx,sender,recipient,asset,integer(r['amount_raw']),integer(r['block_number']),integer(r.get('tx_index'),optional=True),timestamp(r['block_time']),kind,log,trace,None,r['success'],'DUNE_INDEX_'+VERSION,gas))
        except (KeyError,TypeError,ValueError) as exc:
            gaps.append({'reason':'DUNE_NORMALIZATION_UNRESOLVED','row_index':n,'exception_type':type(exc).__name__})
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
    def __init__(self,execute_and_wait,export_page,cache_dir,*,page_size=50,max_pages=50,max_rows=25000,replay_only=False,raw_limit_bytes=536870912):
        if not 1<=page_size<=1000:raise ValueError('page_size must be 1..1000')
        self.execute_and_wait=execute_and_wait;self.export_page=export_page;self.cache=Path(cache_dir)
        self.cache.mkdir(parents=True,exist_ok=True);self.page_size=page_size;self.max_pages=max_pages;self.max_rows=max_rows
        self.replay_only=replay_only;self.raw_limit_bytes=raw_limit_bytes;self.new_raw_bytes=0;self.request_log=[]

    def fetch_interval(self,address,asset,start_block,end_block,*,start_time,end_time,global_end_time):
        sql=build_interval_sql(address,asset,start_block,end_block,start_time=start_time,end_time=min(end_time,global_end_time))
        jobid='dune:'+hashlib.sha256(sql.encode()).hexdigest();folder=self.cache/jobid.split(':')[1];folder.mkdir(exist_ok=True)
        sqlpath=folder/'query.sql'
        if sqlpath.exists() and sqlpath.read_text(encoding='utf-8')!=sql:raise ValueError('SQL hash collision')
        if not sqlpath.exists():sqlpath.write_text(sql,encoding='utf-8')
        jobpath=folder/'job.json';coverage=[];gaps=[];allrows=[];requests=0;hits=0;newbytes=0;complete=False
        try:
            if jobpath.exists():
                job=json.loads(jobpath.read_text());hits+=1
                if job.get('state')!='QUERY_STATE_COMPLETED' or not job.get('execution_id'):
                    return FetchResult(gaps=[{'reason':'DUNE_PRIOR_JOB_UNRESOLVED_NO_AUTOMATIC_RESUBMISSION','logical_job_id':jobid}],cache_hits=hits)
            else:
                if self.replay_only:raise RuntimeError('DUNE_CACHE_MISS_REPLAY_ONLY')
                atomic_json(jobpath,{'state':'SUBMITTING_OR_UNCERTAIN','logical_job_id':jobid})
                job=self.execute_and_wait(sql,jobid);requests+=int(job.get('_request_count',1))
                atomic_json(jobpath,job)
                if job.get('state')!='QUERY_STATE_COMPLETED' or not job.get('execution_id'):
                    return FetchResult(gaps=[{'reason':'DUNE_EXECUTION_NOT_COMPLETED','logical_job_id':jobid,'state':job.get('state')}],real_requests=requests)
            execution=job['execution_id'];offset=0;seen_offsets=set();total=None
            for page in range(self.max_pages):
                if offset in seen_offsets:raise ValueError('repeated offset')
                seen_offsets.add(offset);path=folder/f'page_{offset}.json';cache_hit=path.exists()
                if cache_hit:response=json.loads(path.read_text());hits+=1
                else:
                    if self.replay_only:raise RuntimeError('DUNE_PAGE_CACHE_MISS_REPLAY_ONLY')
                    if self.new_raw_bytes+self.page_size*4096>self.raw_limit_bytes:raise RuntimeError('DUNE_RAW_BYTES_RESOURCE_LIMIT')
                    # Never set filters/sample_count/allow_partial_results or
                    # ignore_max_credits_per_request, and never trust next_uri.
                    response=self.export_page(execution,{'limit':self.page_size,'offset':offset},jobid)
                    requests+=int(response.get('_request_count',1));atomic_json(path,response)
                    size=path.stat().st_size;newbytes+=size;self.new_raw_bytes+=size
                if response.get('execution_id')!=execution or response.get('state')!='QUERY_STATE_COMPLETED':raise ValueError('execution mismatch or incomplete result')
                data=response.get('result',{});values=data.get('rows');md=data.get('metadata',{})
                if not isinstance(values,list):raise ValueError('missing result rows')
                row_total=integer(md.get('total_row_count'))
                if total is not None and row_total!=total:raise ValueError('total row count changed')
                total=row_total;next_offset=response.get('next_offset')
                if next_offset is not None:next_offset=integer(next_offset)
                if next_offset is not None and (not values or next_offset!=offset+len(values)):raise ValueError('noncontiguous pagination')
                allrows.extend(values)
                terminal=next_offset is None
                page_complete=terminal and len(allrows)==total
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
        events,ngaps=normalize_rows(allrows);gaps.extend(ngaps);unique={}
        for e in events:
            if e.event_id in unique and e!=unique[e.event_id]:gaps.append({'reason':'DUNE_EVENT_IDENTITY_CONFLICT','event_id':e.event_id})
            else:unique[e.event_id]=e
        self.request_log.extend(coverage)
        return FetchResult(list(unique.values()),coverage,complete and not gaps,gaps,newbytes,requests,hits)
