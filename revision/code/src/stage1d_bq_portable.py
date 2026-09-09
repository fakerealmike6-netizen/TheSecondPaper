"""Independent, memory-only revalidation of finite classic BigQuery families.

The caller supplies original bytes under safe work-relative names. No filesystem,
database, authentication or transport is consulted. A complete transaction tree
is a transaction proof; it is never an account-window coverage certificate.
"""
import copy
import base64
import hashlib
import json
import re
from datetime import datetime,timezone
from pathlib import PurePosixPath
from types import MappingProxyType

SOURCE_TYPE = 'CLASSIC_BIGQUERY_FULL_TRANSACTION_FAMILY_V1'
PORTABLE_SCHEMA = 'stage1d-classic-bq-portable-family-v1'
ROOT_BINDINGS_PATH = '_portable/ROOT_RPC_BINDINGS.json'


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def safe_name(name):
    if (not isinstance(name,str) or not name or ':' in name or '\\' in name or name.startswith('/')
            or any(x in ('','.','..') for x in name.split('/'))):
        raise ValueError('Portable proof requires safe relative names')
    return name


class ValidationSession:
    """Fresh in-memory reuse of original-byte checks, never serialized trust."""
    def __init__(self):
        self._raw = {}; self._jobs = {}
        self.counts = {'job_verifications': 0, 'job_cache_hits': 0, 'tx_indexes': 0}

    def sha(self, data):
        entry = self._raw.get(id(data))
        if entry is None or entry[0] is not data:
            entry = (data, hashlib.sha256(data).hexdigest()); self._raw[id(data)] = entry
        return entry[1]


def _freeze_validation(value,memo=None):
    if memo is None:memo={}
    if isinstance(value,(dict,list)):
        if id(value) in memo:return memo[id(value)]
        result=MappingProxyType({k:_freeze_validation(v,memo) for k,v in value.items()}) if isinstance(value,dict) else \
               tuple(_freeze_validation(v,memo) for v in value)
        memo[id(value)]=result
        return result
    return value


def _copy_validation(value):
    if isinstance(value,(dict,MappingProxyType)):return {k:_copy_validation(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [_copy_validation(v) for v in value]
    return value


class Documents:
    """A finite original-byte inventory; used dependencies are recorded exactly."""
    def __init__(self,documents,session=None):
        if not isinstance(documents,dict) or not documents:
            raise ValueError('Original portable documents required')
        self.documents={safe_name(k):v for k,v in documents.items()}
        if any(type(v) is not bytes for v in self.documents.values()):
            raise ValueError('Original bytes required, not decoded or reserialized objects')
        self.used={}
        if session is not None and not isinstance(session,ValidationSession):raise ValueError("Fresh validation session required")
        self.session=session; self.family_identity=None

    def raw(self,path,sha256=None,size=None):
        path=safe_name(path)
        if path not in self.documents:raise ValueError('Portable dependency missing: '+path)
        data=self.documents[path];sha=self.session.sha(data) if self.session is not None else hashlib.sha256(data).hexdigest()
        if sha256 is not None and (not re.fullmatch('[0-9a-f]{64}',str(sha256)) or sha!=sha256):
            raise ValueError('Portable dependency SHA changed: '+path)
        if size is not None and (type(size) is not int or size!=len(data)):
            raise ValueError('Portable dependency byte count changed: '+path)
        self.used[path]={'path':path,'sha256':sha,'bytes':len(data)}
        return data

    def dep(self,dependency):
        if not isinstance(dependency,dict) or 'sha256' not in dependency:
            raise ValueError('Original dependency SHA required')
        return self.raw(dependency['path'],dependency['sha256'],dependency.get('bytes'))

    def read(self,path,sha256=None):return json.loads(self.raw(path,sha256))
    def read_dep(self,dependency):return json.loads(self.dep(dependency))


def _integer(value):
    if isinstance(value,bool) or not isinstance(value,(str,int)) or not re.fullmatch('[0-9]+',str(value)):
        raise ValueError('Exact nonnegative integer required')
    return int(value)


def _decode_rows(document):
    """Independently decode the finite flat canonical REST row contract."""
    fields=document.get('schema',{}).get('fields',[])
    if not fields or len({f['name'] for f in fields})!=len(fields):
        raise ValueError('Unique complete canonical result schema required')
    if any(f.get('mode')=='REPEATED' or f.get('type') in ('STRUCT','RECORD') for f in fields):
        raise ValueError('Only canonical scalar result columns supported')
    rows=[]
    for row in document.get('rows',[]):
        cells=row.get('f',[])
        if len(cells)!=len(fields):raise ValueError('REST row arity differs from schema')
        out={}
        for field,cell in zip(fields,cells):
            if not isinstance(cell,dict) or set(cell)!={'v'}:raise ValueError('Exact REST scalar cell required')
            value=cell['v']
            if value is not None and field['type'] in ('BOOLEAN','BOOL'):
                if type(value) is bool:pass
                elif value in ('true','false'):value=value=='true'
                else:raise ValueError('Invalid REST boolean')
            out[field['name']]=value
        rows.append(out)
    return rows


def verify_export_documents(store,state_path,expected_spec):
    if not isinstance(store,Documents):store=Documents(store)
    session=store.session
    if session is None:return _verify_export_documents_uncached(store,state_path,expected_spec)
    state_sha=session.sha(store.raw(state_path))
    key=(store.family_identity,state_path,state_sha,digest(expected_spec))
    cached=session._jobs.get(key)
    if cached is not None:
        # Same state SHA is insufficient: the complete original dependency
        # mapping must still exist with exactly the bytes previously verified.
        value,dependencies=cached
        for dep in dependencies:store.dep(dep)
        session.counts['job_cache_hits']+=1
        return value
    local=Documents(store.documents,session);local.family_identity=store.family_identity
    value=_verify_export_documents_uncached(local,state_path,expected_spec)
    index={}
    for row in value['rows']:index.setdefault(row.get('tx_hash'),[]).append(row)
    value['_tx_index']=index
    value=_freeze_validation(value)
    dependencies=list(local.used.values())
    session._jobs[key]=(value,dependencies)
    session.counts['job_verifications']+=1;session.counts['tx_indexes']+=1
    for dep in dependencies:store.dep(dep)
    return value


def _verify_export_documents_uncached(store,state_path,expected_spec):
    """Recheck spec/dryrun/job/schema/raw page chain and exact row bytes."""
    if not isinstance(store,Documents):store=Documents(store)
    state=store.read(state_path)
    if state.get('state')!='COMPLETE_EXPORTED' or state.get('complete_page_chain') is not True:
        raise ValueError('Complete exported job required')
    plan=store.read_dep({'path':state['plan_path'],'sha256':state['plan_sha256']})
    if plan.get('dry_spec_path')!=expected_spec['path'] or plan.get('dry_spec_sha256')!=expected_spec['sha256']:
        raise ValueError('Job belongs to a different prepared spec')
    spec=store.read_dep(expected_spec)
    if state['sql_sha256']!=spec['sql_sha256']:raise ValueError('Job SQL identity changed')
    sql=store.raw(spec['sql_path'],spec['sql_sha256']).decode('utf-8')
    for dependency in spec['scope_dependencies']+spec['schema_evidence']:store.dep(dependency)
    dry=store.read_dep({'path':plan['dry_receipt_path'],'sha256':plan['dry_receipt_sha256']})
    identity=dry.get('identity',{});result=dry.get('result',{})
    if (dry.get('status')!='SUCCESS_VALIDATED' or result.get('kind')!='DRY_RUN'
            or identity.get('sql_sha256')!=spec['sql_sha256']
            or identity.get('spec_sha256')!=expected_spec['sha256']
            or identity.get('scope_hash')!=spec['scope_hash']
            or result.get('actual_query_executed') is not False):
        raise ValueError('Actual dry-run does not bind prepared spec')
    _integer(result['estimated_processed_bytes'])
    project=identity['project']
    expected_job='stage1d_recovery_'+digest({'sql_sha256':spec['sql_sha256'],'project':project,'location':'US'})[:48]
    if state['job_id']!=expected_job:raise ValueError('Deterministic original job identity differs')
    terminal_path=str(PurePosixPath(safe_name(state_path)).parent/'terminal_job.json')
    terminal=store.read(terminal_path,state['terminal_job_sha256'])
    reference=terminal.get('jobReference',{});query=terminal.get('configuration',{}).get('query',{})
    if (reference.get('jobId')!=expected_job or reference.get('projectId')!=project
            or reference.get('location','US')!='US' or terminal.get('status',{}).get('state')!='DONE'
            or terminal['status'].get('errorResult') or query.get('query')!=sql
            or query.get('useLegacySql') is not False):
        raise ValueError('Original terminal job identity/status/SQL differs')
    count=_integer(state['total_rows']);rows=[];tokens=set();expected_token=None;encoded=hashlib.sha256()
    pages=state['pages']
    if not isinstance(pages,list) or not pages:raise ValueError('At least one actual result page required')
    for page in pages:
        token=page['page_token']
        if token!=expected_token or token in tokens:raise ValueError('Missing/repeated/reordered page token')
        if token is not None and (not isinstance(token,str) or not token):raise ValueError('Invalid original page token')
        tokens.add(token)
        data=store.raw(page['rows_path'],page['rows_sha256'])
        response=page['response_receipt']
        envelope=store.read(response['artifact_path'],response['artifact_sha256'])
        document=envelope.get('result',{});ident=envelope.get('identity',{});selectors=ident.get('selectors',{})
        if (envelope.get('status')!='SUCCESS_VALIDATED' or ident.get('method')!='jobs.getQueryResults'
                or ident.get('project')!=project or ident.get('location','US')!='US'
                or selectors.get('job_id')!=expected_job or selectors.get('pageToken')!=token
                or document.get('jobReference',{}).get('jobId')!=expected_job
                or document.get('jobReference',{}).get('projectId')!=project
                or document.get('jobComplete') is not True or _integer(document['totalRows'])!=count
                or document.get('pageToken')!=page['next_page_token']):
            raise ValueError('Actual result page request/identity/completeness differs')
        raw=envelope.get('raw_sources',[])
        if len(raw)!=1 or raw[0].get('complete') is not True or raw[0].get('http_status')!=200:
            raise ValueError('One complete HTTP 200 original page required')
        if store.read_dep(raw[0])!=document:raise ValueError('Raw page differs from response envelope')
        if document.get('schema') and document['schema']!=state['schema']:raise ValueError('Result schema changed between pages')
        decoded=_decode_rows(dict(document,schema=document.get('schema') or state['schema']))
        actual=[json.loads(line) for line in data.splitlines()]
        if (actual!=decoded or len(actual)!=_integer(page['row_count'])
                or any(set(row)!=set(spec['canonical_columns']) for row in actual)):
            raise ValueError('Stored rows differ from exact original REST decode')
        rows.extend(actual);encoded.update(data);expected_token=page['next_page_token']
    if expected_token is not None or len(rows)!=count:raise ValueError('Incomplete terminal page chain or row count')
    combined=store.raw(state['rows_path'],state['rows_sha256'])
    if encoded.hexdigest()!=hashlib.sha256(combined).hexdigest():raise ValueError('Combined rows differ from page bytes')
    evidence='BIGQUERY_JOB:'+expected_job+':'+store.used[state_path]['sha256']
    return {'rows':[dict(row,evidence_ids=[evidence]) for row in rows], 'evidence_id':evidence,
            'spec':spec,'sql':sql,'job_id':expected_job,'state_sha256':store.used[state_path]['sha256'],
            'page_count':len(pages),'total_rows':count}


def schema_fields_documents(store,evidence,tables):
    """Validate original get_table results and exact fields needed by the SQL."""
    result={}
    for dep in evidence:
        envelope=store.read_dep(dep);metadata=envelope.get('result',{})
        if envelope.get('status')!='SUCCESS_VALIDATED' or metadata.get('kind')!='SCHEMA':
            raise ValueError('Actual get_table schema success required')
        table=metadata['table']
        if table not in tables:continue
        if table in result:raise ValueError('Duplicate schema evidence')
        partition=metadata.get('partition') or {}
        if partition.get('field')!='block_timestamp' or partition.get('type')!='DAY':
            raise ValueError('Exact classic DAY partition evidence required')
        fields=metadata['schema']
        if len({f['name'] for f in fields})!=len(fields):raise ValueError('Duplicate schema field')
        result[table]={f['name']:f['type'] for f in fields}
    if set(result)!=set(tables):raise ValueError('Missing original table metadata')
    from stage1d_bq_context_prepare import TR
    common={'block_number':{'INTEGER','INT64'},'block_hash':{'STRING'},'block_timestamp':{'TIMESTAMP'},
        'transaction_index':{'INTEGER','INT64'},'from_address':{'STRING'},'to_address':{'STRING'},
        'value':{'NUMERIC','BIGNUMERIC','INTEGER','INT64','STRING'}}
    for table,fields in result.items():
        required=dict(common)
        required.update({'transaction_hash' if table==TR else 'hash':{'STRING'}})
        required.update({'status':{'INTEGER','INT64'},'trace_address':{'STRING'},'trace_type':{'STRING'},
            'call_type':{'STRING'},'subtraces':{'INTEGER','INT64'},'error':{'STRING'}} if table==TR
            else {'gas':{'INTEGER','INT64'},'input':{'STRING'}})
        if any(fields.get(name) not in types for name,types in required.items()):
            raise ValueError('Required exact classic table schema differs')
    return result


def portable_dependencies(documents):
    """Deterministic original-byte inventory, not a new acquisition claim."""
    store=Documents(documents)
    for name in sorted(documents):store.raw(name)
    return list(store.used.values())


def verify_rpc_member_documents(store,plan,member):
    """Preserve the accepted saved-point contract and recheck legacy admission."""
    from stage1d_rpc import Stage1DRpcValidation
    from read_retry_r4 import logical_key
    validator=Stage1DRpcValidation({})
    if set(plan)!={'method','params'} or plan['method'] not in {
            'eth_getTransactionByHash','eth_getTransactionReceipt','eth_getBlockByNumber'}:
        raise ValueError('Only exact root transaction/receipt/header points supported')
    envelope=store.read(member['artifact_path'],member['artifact_sha256'])
    request,response=envelope.get('request',{}),envelope.get('response',{})
    if (envelope.get('provider_alias')!='ALCHEMY_ETH_MAINNET_EXISTING'
            or envelope.get('status')!='SUCCESS_VALIDATED' or envelope.get('response_complete') is not True
            or {k:request.get(k) for k in ('method','params')}!=plan
            or validator.rpc_result_status(request,response)!='SUCCESS_VALIDATED'
            or member.get('result',response.get('result'))!=response.get('result')):
        raise ValueError('Original RPC point request/response differs')
    if envelope.get('evidence_kind')=='REAL_CHAIN_LEGACY_RAW_REUSE':
        schema='stage1d-legacy-rpc-point-admission-v1'
        if (envelope.get('schema')!=schema or envelope.get('original_http_status_not_preserved') is not True
                or envelope.get('http_status') is not None or envelope.get('import_is_new_provider_dispatch') is not False
                or member.get('cache_hit') is not True or member.get('local_import') is not True
                or member.get('new_network_requests')!=0 or member.get('new_alchemy_cu')!=0):
            raise ValueError('Original admitted point contract differs')
        source=envelope['legacy_source'];raw_sha=source['raw_sha256'];manifest_sha=source['manifest_sha256']
        raw_name='raw/legacy_rpc/'+raw_sha+'.json';manifest_name='private/stage1d_legacy_import/manifests/'+manifest_sha+'.json'
        if (envelope.get('raw_path')!=raw_name or envelope.get('manifest_path')!=manifest_name
                or envelope.get('raw_body_sha256')!=raw_sha or member.get('legacy_raw_sha256')!=raw_sha):
            raise ValueError('Original admitted raw identities differ')
        raw=store.raw(raw_name,raw_sha,source['raw_bytes'])
        manifest=json.loads(store.raw(manifest_name,manifest_sha,source['manifest_bytes']))
        if len(raw)>16*1024*1024 or json.loads(raw)!=response:raise ValueError('Original admitted response differs')
        admission=store.read(member['admission_path'],member['admission_sha256'])
        key=logical_key(validator.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',plan))
        if (admission.get('schema')!=schema or admission.get('status')!='ADMISSIBLE_POINT_PENDING_ROOT_APPLY'
                or admission.get('freeze_sha256')!='ba3f1368ab4f6daaac19e62607892e0a29ad19afd849facd3e7eedd7dc8f9776'
                or admission.get('logical_key')!=key or admission.get('legacy_source')!=source
                or admission.get('plan')!=plan or admission.get('request_sha256')!=digest({'chain_id':1,**plan})
                or admission.get('coverage_complete') is not False or not isinstance(admission.get('need'),dict)
                or not isinstance(admission.get('normalization'),dict)
                or any(admission.get(k)!=0 for k in ('new_network_requests','new_rpc_operations','new_alchemy_cu','new_bigquery_scanned_bytes'))):
            raise ValueError('Original durable legacy admission differs')
        admission_key=digest({'logical_key':key,'need':admission['need'],'raw_sha256':raw_sha})
        if (member['admission_path']!='private/stage1d_legacy_import/admissions/'+admission_key+'.json'
                or member['artifact_path']!='private/stage1d_legacy_import/envelopes/'+key+'_'+raw_sha+'.json'
                or envelope.get('strict_normalizer')!=admission['normalization'].get('normalizer')):
            raise ValueError('Original admitted point path/normalizer differs')
        ordinal=source['manifest_ordinal'];entries=manifest.get('entries')
        if type(ordinal) is not int or not isinstance(entries,list) or not 0<=ordinal<len(entries):
            raise ValueError('Original legacy manifest ordinal invalid')
        entry=entries[ordinal]
        if (entry.get('file','').lower()!=raw_sha+'.json' or entry.get('response_sha256','').lower()!=raw_sha
                or entry.get('bytes')!=len(raw) or entry.get('method')!=plan['method']
                or entry.get('request_sha256','').lower()!=admission['request_sha256']):
            raise ValueError('Legacy manifest request/raw binding differs')
    else:
        if envelope.get('evidence_kind')!='REAL_CHAIN' or envelope.get('http_status')!=200:
            raise ValueError('Original successful real RPC point required')
        original_path=member['artifact_path'];compressed=envelope.get('decoding')=='BOUNDED_ZSTD_FROM_SAVED_HTTP200_BYTES'
        if 'original_artifact' in envelope:
            if not compressed or envelope.get('new_http_requests')!=0:raise ValueError('Unknown recovery form')
            original=store.read_dep(envelope['original_artifact']);original_path=envelope['original_artifact']['path']
            if original.get('request')!=request or original.get('raw_body_sha256')!=envelope.get('raw_body_sha256'):
                raise ValueError('Recovered point original request binding differs')
        raw_name=str(PurePosixPath(safe_name(original_path)).parent/'response_body.bin')
        if raw_name not in store.documents:
            if member.get('cache_hit') is not True:raise ValueError('New RPC point requires original response body')
            # Existing cache success contract predates raw-body retention. Its
            # bound envelope remains a point proof, never a pagination proof.
        else:
            body=store.raw(raw_name,envelope['raw_body_sha256'])
            if compressed:
                import io
                from compression import zstd
                with zstd.ZstdFile(io.BytesIO(body)) as stream:body=stream.read(8388609)
            if len(body)>8388608:raise ValueError('Original point body exceeds bounded response')
            values=json.loads(body)
            matches=[x for x in values if isinstance(x,dict) and type(x.get('id')) is type(request.get('id')) and x.get('id')==request.get('id')] if isinstance(values,list) else []
            if len(matches)!=1 or matches[0]!=response:raise ValueError('Original raw batch does not bind exactly this point')
    return copy.deepcopy(response['result'])


def verify_transaction_family_documents(preparation_path,job_states,tx_hash,documents, *, validation_session=None):
    """Reproduce one exact full tree from original prepared SQL/result bytes."""
    from collector import Scope
    from stage1d_bq_context_prepare import TX,TR,needed_ranges,columns_for
    from stage1d_batch_binding_route import VERSION,build_binding_sql,needed_day_chunks,validate_transaction_rows
    from stage1d_bq_root_binding import _bind
    from stage1d_alchemy_transfers import bind_need_to_scope
    if not re.fullmatch('0x[0-9a-f]{64}',str(tx_hash)):raise ValueError('Exact current transaction hash required')
    store=Documents(documents,validation_session);manifest=store.read(preparation_path)
    store.family_identity=store.used[preparation_path]['sha256']
    if manifest.get('schema_version')!='stage1d-batch-binding-preparation-v1' or manifest.get('actual_query_executed') is not False:
        raise ValueError('Unknown finite BQ preparation')
    deps=manifest['scope_dependencies']
    for dep in deps:store.dep(dep)
    frozen=[dep for dep in deps if dep['sha256']==manifest['freeze_sha256']]
    if not frozen:raise ValueError('Original frozen query bytes absent')
    freeze=store.read_dep(frozen[0]);queries=[q for q in freeze['queries'] if q['query_id']==manifest['query_id']]
    if len(queries)!=1:raise ValueError('Prepared current query not uniquely frozen')
    query=queries[0];scope=Scope.from_policy(query)
    if query.get('name') not in ('txphish_src001','txphish_src002','xscam_src001'):
        raise ValueError('Finite batch family cannot expand another query')
    if manifest.get('query_name')!=query['name'] or manifest.get('scope_hash')!=query['scope_hash']:
        raise ValueError('Prepared query identity changed')
    ranges=needed_ranges(query,manifest,freeze_sha=manifest['freeze_sha256'])
    rectangles=manifest['need_rectangles']
    for need in rectangles:
        if bind_need_to_scope(need,scope)!=need:raise ValueError('Current exact need was not canonical')
        if (need.get('query_id')!=query['query_id'] or need.get('query_name')!=query['name']
                or need.get('scope_id')!=scope.scope_id or need.get('scope_hash')!=scope.scope_hash
                or need.get('asset')!='native:eip155:1'
                or not scope.start_block<=need['start_block']<=need['end_block']<=scope.end_block
                or not scope.start_time<=need['start_time']<=need['end_time']<=scope.end_time):
            raise ValueError('Actual need rectangle differs from frozen query')
    if sorted({(n['address'],n['start_block'],n['end_block']) for n in rectangles})!=sorted((r['address'],r['start_block'],r['end_block']) for r in ranges):
        raise ValueError('Prepared range/rectangle projections differ')
    dates=needed_day_chunks(rectangles,manifest['chunk_days'])
    if [list(pair) for pair in dates]!=manifest['date_chunks']:raise ValueError('Prepared date domain differs from exact current needs')
    specs=manifest['plans']
    if not specs or len({s['path'] for s in specs})!=len(specs) or set(job_states)!={s['path'] for s in specs}:
        raise ValueError('Exactly every prepared day chunk is required')
    if [(s['date_start_inclusive'],s['date_end_exclusive']) for s in specs]!=[tuple(pair) for pair in dates]:
        raise ValueError('Prepared family dates missing or reordered')
    rows=[];jobs=[];prior_fields=None
    for expected_spec in specs:
        verified=verify_export_documents(store,job_states[expected_spec['path']],expected_spec);spec=_copy_validation(verified['spec'])
        if (spec.get('template')!='transaction_and_trace' or spec.get('batch_binding_sql_version')!=VERSION
                or spec.get('canonical_columns')!=columns_for('transaction_and_trace')
                or spec.get('query_id')!=query['query_id'] or spec.get('scope_hash')!=query['scope_hash']
                or spec.get('needed_ranges')!=ranges or spec.get('need_rectangles')!=rectangles
                or spec.get('scope_dependencies')!=manifest['scope_dependencies']
                or any(spec.get(k)!=expected_spec[k] for k in ('date_start_inclusive','date_end_exclusive'))):
            raise ValueError('Prepared exact transaction/trace specification changed')
        fields=schema_fields_documents(store,spec['schema_evidence'],{TX,TR})
        if prior_fields is not None and fields!=prior_fields:raise ValueError('Schema changed within complete family')
        prior_fields=fields
        sql=build_binding_sql(ranges,fields,spec['date_start_inclusive'],spec['date_end_exclusive'])
        if sql!=verified['sql']:raise ValueError('Original SQL is not the finite complete transaction-family query')
        selected=verified['_tx_index'].get(tx_hash,[]) if '_tx_index' in verified else \
                 (row for row in verified['rows'] if row.get('tx_hash')==tx_hash)
        rows.extend(_copy_validation(row) for row in selected)
        jobs.append({k:verified[k] for k in ('evidence_id','job_id','state_sha256','page_count','total_rows')})
    if not rows:raise ValueError('Exact transaction absent from complete actual family')
    root_bindings=[]
    traces=[row for row in rows if row.get('record_type')=='trace']
    if any(row.get('trace_address') is None for row in traces):
        bindings=store.read(ROOT_BINDINGS_PATH)
        if bindings.get('schema_version')!='stage1d-portable-root-rpc-bindings-v1' or bindings.get('tx_hash')!=tx_hash:
            raise ValueError('Exact portable root point bindings absent')
        matches=[x for x in bindings['root_bindings'] if x.get('tx_hash')==tx_hash]
        if len(matches)!=1:raise ValueError('One exact root equivalence input required')
        item=matches[0];requests=item['requests']
        block_numbers={_integer(row['block_number']) for row in traces}
        if len(block_numbers)!=1:raise ValueError('Transaction trace block identity conflict')
        plans=[{'method':'eth_getTransactionByHash','params':[tx_hash]},
               {'method':'eth_getTransactionReceipt','params':[tx_hash]},
               {'method':'eth_getBlockByNumber','params':[hex(next(iter(block_numbers))),False]}]
        if [x.get('plan') for x in requests]!=plans:raise ValueError('Root point requests are not exact tx/receipt/header')
        values=[verify_rpc_member_documents(store,x['plan'],x['member']) for x in requests]
        evidence=sorted({e for row in traces for e in row['evidence_ids']}|{'sha256:'+x['member']['artifact_sha256'] for x in requests})
        bound=_bind(traces,*values,evidence)
        # Ordering of evidence IDs is immaterial; every actual source and field
        # of the root equivalence is independently recomputed from originals.
        if bound['root_binding']!=item['binding']:raise ValueError('Stored root equivalence differs from recomputed original points')
        rows=[row for row in rows if row.get('record_type')!='trace']+bound['rows']
        root_bindings.append(item)
    checked=validate_transaction_rows(rows)
    if checked.get('gaps'):raise ValueError('Complete transaction tree validation retains gaps: '+str(checked['gaps']))
    if tx_hash not in checked['full_tree_proofs']:raise ValueError('Exact full tree proof absent')
    return {'source_type':SOURCE_TYPE,'tx_hash':tx_hash,'rows':rows,
        'full_tree_proof':checked['full_tree_proofs'][tx_hash],'normalized':checked['normalized'],
        'preparation_dependency':store.used[preparation_path], 'job_states':copy.deepcopy(job_states),
        'jobs':jobs,'root_bindings':root_bindings,'evidence_dependencies':list(store.used.values()),
        'full_context_claimed':False,'covered_ranges':0}


def export_bigquery_family(work,preparation_path,job_states,tx_hash, *, blob_pool=None,validation_session=None):
    """Explicit disk adapter, called by root; retains all original byte strings."""
    from stage1d_batch_binding_route import collect_portable_dependencies
    documents=collect_portable_dependencies(work,preparation_path,job_states,tx_hash)
    return export_family_documents(preparation_path,job_states,tx_hash,documents,
                                   blob_pool=blob_pool,validation_session=validation_session)


def export_family_documents(preparation_path,job_states,tx_hash,documents, *, blob_pool=None,validation_session=None):
    """Export a finite already-read byte map; preserve per-tx local path bindings."""
    verify_transaction_family_documents(preparation_path,job_states,tx_hash,documents,
                                        validation_session=validation_session)
    return {'schema_version':PORTABLE_SCHEMA,'preparation_path':preparation_path,
        'job_states':copy.deepcopy(job_states),'tx_hash':tx_hash,'documents':{
            name:{'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),
                  'bytes_base64':blob_pool.add(raw) if blob_pool is not None else base64.b64encode(raw).decode('ascii')}
            for name,raw in sorted(documents.items())}}


def load_bigquery_family(portable, *, blob_reader=None, validation_session=None):
    if not isinstance(portable,dict) or portable.get('schema_version')!=PORTABLE_SCHEMA:
        raise ValueError('Explicit portable classic BigQuery family required')
    from stage1d_shared_evidence import decode_original
    documents={}
    for name,item in portable['documents'].items():
        safe_name(name);raw=decode_original(item['bytes_base64'],blob_reader)
        if (validation_session.sha(raw) if validation_session is not None else hashlib.sha256(raw).hexdigest())!=item['sha256'] or len(raw)!=item['bytes']:
            raise ValueError('Original portable BigQuery bytes changed')
        documents[name]=raw
    return verify_transaction_family_documents(portable['preparation_path'],portable['job_states'],portable['tx_hash'],documents,validation_session=validation_session)


def semantic_materials(family,policy):
    """Translate a verified original BQ tree; never label it an RPC/Dune trace."""
    from context_ledger_r3 import trace_path
    rows=[row for row in family['rows'] if row.get('record_type')=='trace']
    if family.get('source_type')!=SOURCE_TYPE or family.get('tx_hash')!=policy['tx_hash'] or not rows:
        raise ValueError('Exact classic complete transaction family required')
    indexed={}
    for row in rows:
        path=trace_path(row.get('trace_address'))
        if (path is None or path in indexed or row.get('tx_hash')!=policy['tx_hash']
                or _integer(row['block_number'])!=policy['block_number']
                or row.get('block_hash')!=policy['block_hash'] or _integer(row['tx_index'])!=policy['tx_index']
                or int(datetime.fromisoformat(row['block_time'].replace('Z','+00:00')).timestamp())!=policy['timestamp']
                or row.get('success') is not True or row.get('tx_success') is False or row.get('error')
                or row.get('trace_type')!='call' or row.get('call_type') not in ('call','delegatecall','staticcall','callcode')
                or not isinstance(row.get('input_data'),str) or not re.fullmatch('0x(?:[0-9a-fA-F]{2})*',row['input_data'])):
            raise ValueError('Complete semantic trace needs exact successful original calldata/identity')
        indexed[path]=row
    if () not in indexed:raise ValueError('Original complete trace root absent')
    frames={}
    for path,row in sorted(indexed.items(),key=lambda item:(len(item[0]),item[0])):
        children=sorted(p[-1] for p in indexed if len(p)==len(path)+1 and p[:-1]==path)
        if children!=list(range(_integer(row['subtraces']))) or path and path[:-1] not in indexed:
            raise ValueError('Original complete tree children/ancestors absent')
        frame={'from':row['from_address'],'to':row['to_address'],'type':row['call_type'].upper(),
            'value':hex(_integer(row['value_raw'])),'input':row['input_data'],'calls':[],
            'blockHash':row['block_hash'],'blockNumber':_integer(row['block_number']),
            'transactionHash':row['tx_hash'],'evidence_provider':SOURCE_TYPE,'logs':[]}
        frames[path]=frame
        if path:frames[path[:-1]]['calls'].append(frame)
    internal={'status':'SUCCESS_VALIDATED_INDEXED_CALLS','evidence_provider':SOURCE_TYPE,
        'result':[{'from':row['from_address'],'to':row['to_address'],'value':row['value_raw'],
            'isError':'0','blockNumber':str(row['block_number']),'blockHash':row['block_hash'],
            'hash':row['tx_hash'],'trace_address':list(path)} for path,row in sorted(indexed.items())
            if path and row['call_type']=='call']}
    return {'trace':frames[()],'internal':internal}
