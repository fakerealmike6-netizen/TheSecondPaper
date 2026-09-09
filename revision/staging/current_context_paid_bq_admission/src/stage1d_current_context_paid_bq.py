"""Read-only admission of explicitly named paid BQ sources into current context.

No request, job submission, ledger mutation, persistent cache, or solver. Source
SQL/schema/page families are checked once per invocation. Coverage is per native
ledger kind and verified subinterval; paid but unbound data never becomes a new
SQL demand. Logical discovery seconds never truncate a whole-block context row.
"""
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import re

from collector import Scope
from context_ledger_r3 import integer, normalize_rows
from stage1d_bq_fee_tree_guard import inspect_rows, fee_gaps_for_range
import stage1d_bq_context_prepare as h
import stage1d_batch_binding_route as batch
import stage1d_transfers_acquisition as transfers
from stage1d_closure_scope import active_batch, active_batch_path, batch_path_for_sha
from stage1d_closure_context import requirements
from stage1d_multiasset_context import asset, ETH

SCHEMA = 'stage1d-current-context-paid-bq-admission-v1'
CLASSIC = 'stage1d-classic-context-preparation-v1'
KINDS = (h.TOP, h.INTERNAL, h.FEES)


def union(intervals):
    result = []
    for lo, hi in sorted(intervals):
        if type(lo) is not int or type(hi) is not int or lo > hi:
            raise ValueError('Exact nonempty closed block interval required')
        if result and lo <= result[-1][1]+1:
            result[-1][1] = max(hi, result[-1][1])
        else: result.append([lo, hi])
    return result


def difference(lo, hi, covered):
    cursor = lo; result = []
    for start, end in union((max(lo,a), min(hi,b)) for a,b in covered if a <= hi and b >= lo):
        if cursor < start: result.append([cursor, start-1])
        cursor = max(cursor, end+1)
    if cursor <= hi: result.append([cursor, hi])
    return result


def _stamp(date):
    return int(datetime.fromisoformat(date+'T00:00:00+00:00').timestamp())


def _classic_preparation(work, ref, manifest):
    """Use the original classic range/date/schema/SQL contracts, unchanged IDs."""
    if manifest.get('schema_version') != CLASSIC or manifest.get('actual_query_executed') is not False:
        raise ValueError('Original complete classic preparation required')
    freeze = batch_path_for_sha(work, manifest['freeze_sha256'])
    if h.sha(freeze) != manifest['freeze_sha256']: raise ValueError('Original freeze SHA differs')
    query = next(q for q in h.read(freeze)['queries'] if q['query_id'] == manifest['query_id'])
    ranges = h.needed_ranges(query, manifest, freeze_sha=h.sha(freeze))
    specs = [p for p in manifest['plans'] if p['template'] == 'transaction_and_trace']
    expected = h.date_chunks(query, manifest['chunk_days'])
    if [(s['date_start_inclusive'],s['date_end_exclusive']) for s in specs] != expected:
        raise ValueError('Classic full date partition family missing/reordered')
    for dep in manifest['scope_dependencies']: h.checked(work,dep)
    previous = None
    for item in specs:
        spec=h.read(h.checked(work,item)); fields=h.schemas(work,spec['schema_evidence'],{h.TX,h.TR})
        if previous is not None and fields != previous: raise ValueError('Schema changes within source family')
        previous=fields
        if (spec.get('schema_version') != 'stage1d-bigquery-dryrun-spec-v1'
                or spec.get('batch_binding_sql_version') is not None
                or spec.get('template') != 'transaction_and_trace'
                or spec['canonical_columns'] != h.columns_for('transaction_and_trace')
                or spec['needed_ranges'] != ranges or spec['scope_dependencies'] != manifest['scope_dependencies']
                or spec['query_id'] != query['query_id'] or spec['scope_hash'] != query['scope_hash']
                or any(spec[k] != item[k] for k in ('date_start_inclusive','date_end_exclusive'))):
            raise ValueError('Classic spec no longer binds its original family')
        sql=h.checked(work,{'path':spec['sql_path'],'sha256':spec['sql_sha256']}).read_text(encoding='utf-8')
        if sql != h.build_sql(ranges,fields,'transaction_and_trace',item['date_start_inclusive'],item['date_end_exclusive']):
            raise ValueError('Original SQL is not the complete reviewed touched transaction family')
    return specs


def _load_source(work, source):
    ref=source['preparation_ref']; path=h.checked(work,ref); manifest=h.read(path)
    if manifest.get('schema_version') == batch.PREP_SCHEMA:
        verified,_=batch.verified_preparation(work,path)
        specs=verified['plans']
    elif manifest.get('schema_version') == CLASSIC:
        specs=_classic_preparation(work,ref,manifest)
    else: raise ValueError('Bare metadata/rows cannot certify SQL domain or complete pages')
    jobs=source['job_states']
    if set(jobs) != {s['path'] for s in specs}: raise ValueError('Exactly every original transaction-and-trace job is required')
    rows=[]; evidence=[]; states=[]
    for spec in specs:
        state_path=h.checked(work,jobs[spec['path']])
        added, identity=h.verified_export(work,state_path,spec)
        rows.extend(added);evidence.append(identity);states.append(h.dep(work,state_path))
    # Original disjoint partitions cannot duplicate a physical row.
    seen=set()
    for row in rows:
        key=(row.get('record_type'),row.get('block_hash'),row.get('tx_hash'),row.get('trace_address'))
        if row.get('tx_hash') and key in seen: raise ValueError('Duplicate physical row across original date partitions')
        seen.add(key)
    families=defaultdict(list); by_address=defaultdict(set); protocol=[]
    for row in rows:
        if row.get('tx_hash'): families[row['tx_hash']].append(row)
        else: protocol.append(row)
        for address in (row.get('from_address'),row.get('to_address'),row.get('created_address'),row.get('refund_address')):
            if address and row.get('tx_hash'): by_address[address.lower()].add((integer(row['block_number']),row['tx_hash']))
    dates=union((_stamp(p['date_start_inclusive']),_stamp(p['date_end_exclusive'])-1) for p in specs)
    return {'ref':h.dep(work,path),'job_refs':states,'manifest':manifest,'families':dict(families),
        'by_address':dict(by_address),'protocol_rows':protocol,'date_intervals':dates,
        'evidence_ids':evidence,'raw_rows':len(rows),'verified_once':True}


class _Points:
    def __init__(self,work,bindings):
        self.work=work;self.members={};self.values={};self.used=[]
        for item in bindings:
            key=h.digest(item['plan'])
            if key in self.members and self.members[key] != item: raise ValueError('Conflicting saved point bindings')
            self.members[key]=item

    def get(self,plan):
        key=h.digest(plan)
        if key not in self.members: return None
        if key not in self.values:
            member=self.members[key]['member']
            value=transfers.verified_member(self.work,plan,member)
            self.values[key]=value;self.used.append(deepcopy(self.members[key]))
        return self.values[key]


def _header_plan(block): return {'method':'eth_getBlockByNumber','params':[hex(block),False]}


def _compare_available_top(family,tx,receipt,header):
    """Any supplied verified counterevidence matters, even without a root trio."""
    tops=[r for r in family if r['record_type']=='transaction']
    if len(tops)!=1:raise ValueError('Provided point cannot match one exact BQ top')
    top=tops[0]
    def compare(field,value,numeric=False,explicit_nullable=False):
        prior=top.get(field)
        if explicit_nullable and ((prior is None)!=(value is None)):
            raise ValueError('BQ/current RPC nullable endpoint conflict: '+field)
        if prior is None or value is None:return
        equal=integer(prior)==integer(value) if numeric else str(prior).lower()==str(value).lower()
        if not equal:raise ValueError('BQ/current RPC physical field conflict: '+field)
    for value,hash_field in ((tx,'hash'),(receipt,'transactionHash')):
        if value is None:continue
        for field,rpc in (('tx_hash',hash_field),('block_hash','blockHash'),('from_address','from'),('to_address','to')):
            compare(field,value.get(rpc),explicit_nullable=field=='to_address' and rpc in value)
        for field,rpc in (('block_number','blockNumber'),('tx_index','transactionIndex'),('transaction_type','type')):
            compare(field,value.get(rpc),True)
    if tx is not None:
        compare('value_raw',tx.get('value'),True);compare('gas_limit',tx.get('gas'),True)
        compare('input_data',tx.get('input'))
    if receipt is not None:
        for field,rpc in (('gas_used','gasUsed'),('effective_gas_price','effectiveGasPrice'),
                          ('blob_gas_used','blobGasUsed'),('blob_gas_price','blobGasPrice')):
            compare(field,receipt.get(rpc),True)
        compare('created_address',receipt.get('contractAddress'),
                explicit_nullable=top.get('created_address') is not None and 'contractAddress' in receipt)
        if type(top.get('success')) is bool and receipt.get('status') is not None:
            if integer(receipt['status']) not in (0,1) or top['success'] is not bool(integer(receipt['status'])):
                raise ValueError('BQ/current receipt execution status conflict')
    if header is not None:
        compare('block_number',header.get('number'),True);compare('block_hash',header.get('hash'))
        if top.get('block_time') is not None and header.get('timestamp') is not None:
            stamp=int(datetime.fromisoformat(top['block_time'].replace('Z','+00:00')).timestamp())
            if stamp!=integer(header['timestamp']):raise ValueError('BQ/current header timestamp conflict')
        if 'transactions' in header and top.get('tx_index') is not None:
            index=integer(top['tx_index']);hashes=header['transactions']
            if not isinstance(hashes,list) or index>=len(hashes) or str(hashes[index]).lower()!=top['tx_hash'].lower():
                raise ValueError('BQ/current header transaction position conflict')


def _date_domain(source,scope,lo,hi,points):
    if scope.start_block<=lo<=hi<=scope.end_block and not difference(scope.start_time,scope.end_time,source['date_intervals']):
        return True, [], 'CURRENT_VERIFIED_QUERY_TIME_DOMAIN_CONTAINED_IN_PAID_DAY_UNION'
    # Narrow batch days need actual whole-block endpoint time evidence. A
    # logical discovery second rectangle is not that evidence.
    plans=[_header_plan(b) for b in sorted({lo,hi})]; values=[points.get(p) for p in plans]
    missing=[p for p,v in zip(plans,values) if v is None]
    if missing: return False,missing,'PAID_DATE_BLOCK_ENDPOINT_BINDING_REQUIRED'
    stamps=[]
    for p,v in zip(plans,values):
        if integer(v['number']) != integer(p['params'][0]) or not re.fullmatch('0x[0-9a-fA-F]{64}',v.get('hash','')):
            raise ValueError('Exact saved header identity differs')
        stamps.append(integer(v['timestamp']))
    if stamps != sorted(stamps): raise ValueError('Whole-block endpoint timestamps inverted')
    if not difference(stamps[0],stamps[-1],source['date_intervals']):
        return True,[], 'EXACT_VERIFIED_HEADER_TIMES_CONTAINED_IN_PAID_DAY_UNION'
    return False,[], 'PAID_DATE_DOMAIN_PARTIAL_NEEDS_EXACT_BLOCK_BRACKET; DO_NOT_RESUBMIT_WHOLE_PAID_RANGE'


def _bind_selected(families,points,evidence):
    from stage1d_bq_root_binding import _bind
    result={};gaps=[];bindings=[]
    for tx,family in families.items():
        traces=[r for r in family if r['record_type']=='trace']
        blocks={integer(r['block_number']) for r in family}
        if len(blocks)!=1: raise ValueError('Transaction family spans conflicting physical blocks')
        block=blocks.pop(); plans=[{'method':'eth_getTransactionByHash','params':[tx]},
            {'method':'eth_getTransactionReceipt','params':[tx]},_header_plan(block)]
        values=[points.get(p) for p in plans]; missing=[p for p,v in zip(plans,values) if v is None]
        if any(value is not None for value in values):
            try:_compare_available_top(family,*values)
            except (ValueError,KeyError,TypeError,IndexError) as error:
                result[tx]=family;gaps.append({'reason':'CURRENT_POINT_VS_BQ_PHYSICAL_CONFLICT','tx_hash':tx,
                    'block_number':block,'detail':str(error),'needed_rpc':[],
                    'provided_binding_evidence_failed':True});continue
        if not any(r.get('trace_address') is None for r in traces):result[tx]=family;continue
        gap={'reason':'ROOT_EQUIVALENCE_UNRESOLVED_GAP','tx_hash':tx,'block_number':block,
             'data_type':h.INTERNAL,'needed_rpc':missing}
        if not missing:
            try:
                ids=evidence+['sha256:'+points.members[h.digest(p)]['member']['artifact_sha256'] for p in plans]
                bound=_bind(traces,*values,ids)
                tops=[r for r in family if r['record_type']=='transaction']
                jointly=normalize_rows(tops+bound['rows']+[bound['current_top_row']])
                if jointly['conflicts']:
                    raise ValueError('BQ and current RPC top/fee physical evidence conflict')
                result[tx]=[r for r in family if r['record_type']!='trace']+bound['rows']
                bindings.append({'tx_hash':tx,'root_binding':bound['root_binding'],
                                 'point_request_keys':[h.digest(p) for p in plans]})
                continue
            except (ValueError,KeyError,TypeError,IndexError) as error:
                gap.update(reason='ROOT_BINDING_EVIDENCE_CONFLICT_OR_UNSUPPORTED',detail=str(error),
                           provided_binding_evidence_failed=True)
        result[tx]=family;gaps.append(gap)
    return result,gaps,bindings


def _classify(rows,need):
    """Independent ordinary top / full internal family / actual payer gas gates."""
    checked=batch.validate_transaction_rows(rows)
    normalized=checked['normalized'];review=checked['fee_review']
    conflicts=normalized['conflicts']+review['conflicts']
    if conflicts: return {k:False for k in KINDS}, {'conflicts':conflicts,'gaps':checked['gaps']}
    tops=[r for r in rows if r['record_type']=='transaction']
    top_ok=not review['top_gaps'] and all(type(r.get('success')) is bool for r in tops)
    top_ok=top_ok and all(all(r.get(k) is not None for k in ('block_number','block_hash','block_time','tx_index'))
        and re.fullmatch('0x[0-9a-f]{64}',r['block_hash']) is not None for r in tops)
    # Every exported non-protocol family selected by the account must have its
    # one top; a trace-only orphan must not prove top absence.
    txs={r['tx_hash'] for r in rows if r.get('tx_hash')}
    top_ok=top_ok and {r['tx_hash'] for r in tops}==txs and len(tops)==len(txs)
    internal_ok=top_ok and set(checked['full_tree_proofs'])==txs and not checked['gaps']
    fees_ok=top_ok and not fee_gaps_for_range(review,need)
    return {h.TOP:top_ok,h.INTERNAL:internal_ok,h.FEES:fees_ok}, {
        'conflicts':[],'gaps':checked['gaps'],'fee_gaps':fee_gaps_for_range(review,need),
        'fee_component_evidence':[x for x in review['fee_components'] if x['address']==need['address']]}


def _admit_plan(scope,plan,sources,points):
    native=[r for r in plan['rows'] if asset(r.get('asset','ETH'))==ETH]
    coverage=[];binding_gaps=[];material=[];source_summaries=[];paid=defaultdict(list)
    for source in sources:
        intersections=[]; selected=set()
        for row in native:
            lo,hi=row['ledger_start_block'],row['ledger_end_block']
            for a,b in union((max(lo,r['start_block']),min(hi,r['end_block'])) for r in source['manifest']['needed_ranges']
                if r['address']==row['address'] and r['start_block']<=hi and r['end_block']>=lo):
                paid[row['account_id']].append([a,b])
                ok,needed,reason=_date_domain(source,scope,a,b,points)
                keys={tx for block,tx in source['by_address'].get(row['address'],()) if a<=block<=b}
                selected.update(keys)
                intersections.append((row,a,b,ok,keys))
                if not ok: binding_gaps.append({'address':row['address'],'account_id':row['account_id'],
                    'start_block':a,'end_block':b,'reason':reason,'needed_rpc':needed,'source_ref':source['ref'],
                    'already_paid_source_domain_candidate':True,'new_sql_authorized':False})
        families={tx:source['families'][tx] for tx in sorted(selected)}
        families,root_gaps,root_bindings=_bind_selected(families,points,source['evidence_ids'])
        failed_binding_txs={g['tx_hash'] for g in root_gaps if g.get('provided_binding_evidence_failed')}
        binding_gaps.extend(dict(g,source_ref=source['ref']) for g in root_gaps)
        selected_rows=[r for tx in sorted(families) for r in families[tx]]
        # Protocol rows are retained as observations, never given protocol FULL.
        proto=[r for r in source['protocol_rows'] if any(row['address'] in
            (r.get('from_address'),r.get('to_address'),r.get('created_address'),r.get('refund_address'))
            and a<=integer(r['block_number'])<=b for row,a,b,_,_ in intersections)]
        material.extend(selected_rows+proto)
        for row,lo,hi,date_ok,keys in intersections:
            if not date_ok: continue
            rows=[r for tx in sorted(keys) for r in families[tx]]
            need={'address':row['address'],'start_block':lo,'end_block':hi}
            okay,details=_classify(rows,need)
            if keys & failed_binding_txs:
                okay={kind:False for kind in KINDS}
                details['provided_point_binding_failed_transactions']=sorted(keys & failed_binding_txs)
            for kind,complete in okay.items():
                if complete:
                    coverage.append(dict(need,asset=ETH,data_type=kind,status='COMPLETE',pagination_complete=True,
                        evidence_ids=source['evidence_ids'],provider_frozen_scope=True,date_domain_verified=True,
                        block_domain_verified=True,source_preparation_ref=source['ref'],
                        original_job_refs=source['job_refs'],context_directions=['INCOMING','OUTGOING'] if kind!=h.FEES else ['PAYER'],
                        source_bound_kind_validation=True))
                else:
                    field_points=[]; available=[]
                    for gap in details.get('fee_gaps',[]) if kind==h.FEES else []:
                        request={'method':'eth_getTransactionReceipt','params':[gap['tx_hash']]}
                        (field_points if points.get(request) is None else available).append(request)
                    binding_gaps.append(dict(need,account_id=row['account_id'],data_type=kind,
                        reason='PAID_EXPORTED_DOMAIN_REQUIRES_FIELD_OR_FAMILY_BINDING',source_ref=source['ref'],
                        details=details,needed_rpc=field_points,
                        verified_existing_points_still_require_field_reconciliation=available,new_sql_authorized=False))
        source_summaries.append({'preparation_ref':source['ref'],'job_refs':source['job_refs'],
            'verified_rows':source['raw_rows'],'selected_transaction_families':len(families),
            'selected_rows_including_full_ancestors_siblings_zero_failed_gas':len(selected_rows),
            'selected_protocol_observation_rows':len(proto),'root_bindings':root_bindings,
            'source_verified_once':True,'request_hash_equality_required':False})
    # Keep source versions; merge known equal rows only by existing physical
    # reconciliation. Conflicts are errors, not a source preference.
    from stage1d_context_online import _merge_rows
    material=_merge_rows([],material)
    normalized=normalize_rows(material)
    if normalized['conflicts']:
        coverage=[];binding_gaps.append({'reason':'CROSS_SOURCE_PHYSICAL_CONFLICT','details':normalized['conflicts']})
    missing_source=[];paid_pending=[];remaining=[]
    for row in native:
        lo,hi=row['ledger_start_block'],row['ledger_end_block'];known=union(paid[row['account_id']])
        for kind in KINDS:
            complete=[(r['start_block'],r['end_block']) for r in coverage if r['address']==row['address'] and r['data_type']==kind]
            for a,b in difference(lo,hi,complete):
                remaining.append({'account_id':row['account_id'],'address':row['address'],'data_type':kind,'start_block':a,'end_block':b})
            for a,b in difference(lo,hi,known):
                missing_source.append({'account_id':row['account_id'],'address':row['address'],'asset':ETH,
                    'data_type':kind,'start_block':a,'end_block':b,'reason':'NO_VERIFIED_PAID_SOURCE_IN_EXPLICIT_INVENTORY',
                    'new_query_may_be_prepared_only_after_existing_cache_and_submission_guard':True})
            for a,b in known:
                for x,y in difference(a,b,complete):
                    paid_pending.append({'account_id':row['account_id'],'address':row['address'],'data_type':kind,
                        'start_block':x,'end_block':y,'reason':'PAID_SOURCE_ALREADY_EXPORTED_BINDING_OR_DATE_PROOF_PENDING','new_sql_authorized':False})
    point_needs={h.digest(p):p for g in binding_gaps for p in g.get('needed_rpc',[])}
    return {'rows':material,'coverage':coverage,'binding_gaps':binding_gaps,
        'remaining_by_kind':remaining,'paid_pending_binding_by_kind':paid_pending,'missing_paid_source_by_kind':missing_source,
        'point_binding_requests':list(point_needs.values()),'sources':source_summaries,
        'protocol_requirements':[dict(address=r['address'],start_block=r['ledger_start_block'],end_block=r['ledger_end_block'],
            data_type=h.PROTOCOL,status='OPEN_NOT_PROVED_BY_TRANSACTION_TRACE_FAMILY') for r in native],
        'non_native_plan_rows_preserved':[r for r in plan['rows'] if asset(r.get('asset','ETH'))!=ETH],
        'full_context_claimed':False,'coverage_installed':False,'new_requests_executed':0,
        'inventories_complete_for_all_existing_providers':False,'actual_savings':None}


def admit_current(work,query_name,collection_ref,label_ref,source_inventory,*,point_bindings=()):
    """Public disk entry; inputs stay original, returned material is not installed.

    source_inventory = [{preparation_ref:{path,sha256},
       job_states:{original_spec_path:{path:original_job_json,sha256}}}]
    point_bindings = [{plan:{method,params}, member:<existing verified RPC member>}]
    No cache database is opened; missing members become exact point needs.
    """
    work=Path(work).resolve()
    if (work/'private/network_worker.lock').exists(): raise ValueError('Single writer must reach a safe point first')
    freeze_ref=h.dep(work,active_batch_path(work)); query=next(q for q in active_batch(work)['queries'] if q['name']==query_name)
    current=work/'derived/stage1d/queries'/query_name
    if h.sha(current/'collection.json')!=collection_ref['sha256'] or h.sha(current/'label_snapshot.json')!=label_ref['sha256']:
        raise ValueError('Explicit current context inputs differ from actual current aliases')
    collection=h.read(h.checked(work,collection_ref)); labels=h.read(h.checked(work,label_ref))
    demand=requirements(query,collection,labels)
    sources=[];seen=set()
    for source in source_inventory:
        key=h.digest({'preparation_ref':source['preparation_ref'],'job_states':source['job_states']})
        if key not in seen: sources.append(_load_source(work,source));seen.add(key)
    points=_Points(work,point_bindings)
    result=_admit_plan(Scope.from_policy(query),demand['context_plan'],sources,points)
    # Preserve the original unresolved material/point requirements. This helper
    # does not claim that a paid source or a new COMPLETE kind closes the model.
    result.update(schema_version=SCHEMA,query_id=query['query_id'],scope_hash=query['scope_hash'],
        current_input_refs={'collection':collection_ref,'labels':label_ref,'freeze':freeze_ref},
        context_plan=demand['context_plan'],current_requirements_binding=demand['binding'],
        original_current_point_requests=demand['point_requests'],used_point_bindings=points.used,
        current_semantic_units=len(collection.get('semantic_units',[])),evidence_trust='ORIGINAL_SQL_SCHEMA_DRY_JOB_RAW_PAGE_AND_EXACT_MEMBER_REVERIFICATION')
    for ref in (collection_ref,label_ref,freeze_ref):h.checked(work,ref)
    for source in source_inventory:
        h.checked(work,source['preparation_ref'])
        for ref in source['job_states'].values():h.checked(work,ref)
    if (work/'private/network_worker.lock').exists():raise ValueError('Writer appeared during read-only admission')
    return result
