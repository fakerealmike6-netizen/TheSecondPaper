"""Offline R1 actual-frontier label queue, complete Dune import and label replay.

This module never opens a socket or updates the shared budget. The root worker
must reserve and execute the frozen SQL. Successful empty observations are
retained; no result-dependent queue substitution is supported.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from labels_policy import full_address, resolve_address
from page_contract import initial_progress, validate_page
from reference_core import actor_key, canonical_actor, digest, dump, sid, stable, truth, write_csv
from reference_recompute import metrics, recompute, rows

VERSION = 'stage1b-r1-actual-frontier-labels-1.0'
BASE_VERSION = '20260906T181303+0800_stage1b_stage1b'
TABLES = {
    'cex_addresses': ('cex.addresses', 'Q040_TARGET_ALL_CEX_ADDRESSES.sql',
        ['cex_name', 'distinct_name', 'added_by', 'added_date', 'blockchain']),
    'labels_addresses': ('labels.addresses', 'Q041_TARGET_ALL_LABELS_ADDRESSES.sql',
        ['name', 'category', 'contributor', 'source', 'created_at', 'updated_at', 'model_name', 'label_type', 'blockchain']),
    'owner_addresses': ('labels.owner_addresses', 'Q042_TARGET_ALL_LABELS_OWNER_ADDRESSES.sql',
        ['owner_key', 'custody_owner', 'account_owner', 'contract_name', 'contract_version', 'eoa',
         'factory_contract', 'source', 'identifying_transaction', 'algorithm_name', 'source_website',
         'source_evidence', 'created_at', 'created_by', 'updated_at', 'updated_by', 'blockchain']),
    'deposit_addresses': ('cex.deposit_addresses', 'Q043_TARGET_ALL_CEX_DEPOSIT_ADDRESSES.sql',
        ['cex_name', 'first_deposit_token_standard', 'first_deposit_token_address', 'deposit_first_block_time',
         'consolidation_first_block_time', 'deposit_count', 'consolidation_count', 'amount_deposited',
         'consolidation_unique_key', 'deposit_unique_key', 'blockchain']),
}

def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def now():
    return datetime.now(timezone.utc).isoformat()

def checked(root, path):
    p = Path(path).resolve()
    if not p.is_relative_to(root.resolve()):
        raise ValueError('Path outside PROJECT_ROOT: ' + str(p))
    return p

def snapshot_observations(root, snapshot):
    """A failed-lookup version reuses the original observations by reference."""
    local=snapshot/'label_observations.csv.gz'
    if local.exists():return local
    manifest=read(snapshot/'source_manifest.json')
    source=checked(root,root/manifest['observations_source_path'])
    if digest(source)!=manifest['observations_sha256']:raise ValueError('Reused observation source changed')
    return source

def file_record(root, path, role):
    p = checked(root, path)
    return dict(path=p.relative_to(root).as_posix(), sha256=digest(p), bytes=p.stat().st_size, role=role)

def build_sql(addresses, schema, *, maximum_addresses=40):
    """Nine output rows can contain arbitrarily many complete source matches.

    Map values use documented source types and lossless textual representations
    of the provider's values. Nulls remain JSON null. Individual source records
    and duplicates are preserved by array_agg; this is not a DISTINCT export.
    """
    if type(maximum_addresses) is not int or maximum_addresses not in (40, 100):
        raise ValueError('Only the inherited40 or explicit Stage1D100 SQL batch ceiling is supported')
    addresses = [full_address(x) for x in addresses]
    if not addresses or len(addresses) > maximum_addresses:
        raise ValueError('Expected 1..' + str(maximum_addresses) + ' frozen actual-frontier addresses')
    if len(addresses) != len(set(addresses)):
        raise ValueError('Duplicate frozen actual-frontier addresses')
    addresses = sorted(addresses)
    types = {(r['table_schema']+'.'+r['table_name'], r['column_name']): r['data_type'] for r in schema}
    ctes = ['frontier(address) AS (\n  VALUES\n    ' + ',\n    '.join('('+a+')' for a in addresses) + '\n)']
    select = ["lower(concat('0x', to_hex(f.address))) AS address"]
    joins = []
    for alias, (table, _, fields) in TABLES.items():
        exprs = []
        for field in fields:
            typ = types.get((table, field))
            if typ is None:
                raise ValueError('Unverified schema column '+table+'.'+field)
            if typ == 'varbinary':
                exprs.append("lower(concat('0x', to_hex(s."+field+')))' )
            elif typ.startswith(('varchar', 'timestamp', 'date', 'bigint', 'double', 'boolean')):
                exprs.append('CAST(s.'+field+' AS VARCHAR)')
            else:
                raise ValueError('Unsupported verified schema type: '+typ)
        map_sql = 'map(ARRAY[' + ', '.join("'"+x+"'" for x in fields) + '],\n      ARRAY[' + ', '.join(exprs) + '])'
        ctes.append(alias+" AS (\n  SELECT s.address, count(*) AS match_count,\n    json_format(CAST(array_agg("+map_sql+") AS JSON)) AS matches_json\n  FROM "+table+" s\n  INNER JOIN frontier f ON s.address = f.address\n  WHERE s.blockchain = 'ethereum'\n  GROUP BY s.address\n)")
        select += ["COALESCE("+alias+".matches_json, '[]') AS "+alias+'_json',
                   'COALESCE('+alias+'.match_count, 0) AS '+alias+'_count']
        joins.append('LEFT JOIN '+alias+' ON '+alias+'.address = f.address')
    return ('-- Stage1B-R1 frozen actual-frontier label opportunities; no reference-target whitelist.\n'
            '-- Complete source matches are JSON aggregated without LIMIT, DISTINCT or array truncation.\n'
            '-- Four independent address-table opportunities; empty arrays certify only this execution.\n'
            'WITH '+',\n'.join(ctes)+'\nSELECT\n  '+',\n  '.join(select)+'\nFROM frontier f\n'+'\n'.join(joins)+'\nORDER BY address\n')

def freeze(work, root, batch='batch_01', collection_root=None, base_snapshot=None):
    root = Path(root).resolve(); work = checked(root, work)
    if not re.fullmatch(r'batch_0[1-3]', batch):
        raise ValueError('Only batch_01 through batch_03 are authorized')
    dest = work/'derived/frontier_labels'/batch
    if (dest/'manifest.json').exists():
        manifest = read(dest/'manifest.json')
        for rec in manifest['frozen_files']:
            if digest(dest/rec['name']) != rec['sha256']:
                raise ValueError('Frozen batch changed: '+rec['name'])
        return manifest
    if dest.exists():
        raise ValueError('Incomplete prior freeze directory; inspect, do not overwrite')
    source_dir = checked(root, collection_root or work/'baseline_derived/dune_live_replay')
    snap = checked(root, base_snapshot or root/'02_sources/public_labels/consolidated/snapshots'/BASE_VERSION)
    inputs = []; frontiers = defaultdict(list)
    for p in sorted(source_dir.glob('*/collection.json')):
        c = read(p); inputs.append(file_record(root,p,'ACTUAL_COLLECTION_STATES'))
        for index, item in enumerate(c['states']):
            if item['identity'].get('status') != 'UNQUERIED':
                continue
            state = item['state']; address = full_address(state['address'])
            frontiers[address].append(dict(query_id=c['query_id'], pilot=p.parent.name,
                state_index=index, depth=state['depth'], arrival_event_id=state['arrival']['event_id'],
                source_path=p.relative_to(root).as_posix(), source_sha256=digest(p)))
    if not frontiers:
        raise ValueError('No actual UNQUERIED collection states')
    registry_path = snap/'address_registry.csv.gz'; observation_path = snapshot_observations(root,snap)
    inputs += [file_record(root,p,'INHERITED_FROZEN_LABEL_SNAPSHOT') for p in (registry_path, observation_path, snap/'label_policy.json')]
    local_registry = {r['address']:r for r in rows(registry_path) if r['address'] in frontiers}
    local_obs = defaultdict(list); meta_success = set(); meta_empty = set()
    with gzip.open(observation_path,'rt',encoding='utf-8-sig',newline='') as f:
        for o in csv.DictReader(f):
            if o['address'] in frontiers:local_obs[o['address']].append(o)
            if o['platform']=='MetaSleuth':
                # These snapshots contain only successful parsed Meta responses,
                # including context-only empty response observations.
                meta_success.add(o['address'])
                if 'EMPTY' in o.get('source_metadata',''):meta_empty.add(o['address'])
    for p in sorted((root/'03_workspaces/metasleuth_daily/runs').glob('*/ledger_rows.json')):
        inputs.append(file_record(root,p,'HISTORICAL_META_SUCCESS_INCLUDING_EMPTY_LEDGER'))
        obj=read(p)
        for r in obj.get('rows',[]) if isinstance(obj,dict) else obj:
            if r.get('api_response_status')=='SUCCESS':
                meta_success.add(full_address(r['address']))
                if 'EMPTY' in r.get('label_result_status',''):meta_empty.add(full_address(r['address']))
    # The historical uniform match CSV retains no-match rows that were not
    # source observations; do not mistake absent observation for unqueried.
    matchfile=root/'03_workspaces/stage0B1L2/derived/stage0b1l2_dune_terminal_match_records.csv'
    inputs.append(file_record(root,matchfile,'HISTORICAL_FOUR_TABLE_MATCH_AND_NO_MATCH'))
    old_dune=defaultdict(set)
    for r in rows(matchfile):
        if r['address'] in frontiers:old_dune[r['address']].add(r['table_name'])
    already=set()
    for p in sorted((work/'derived/frontier_labels').glob('batch_*/queue.json')):
        q=read(p); already.update(q['external_addresses'])
    review=[]; selected=[]
    all_tables={v[0] for v in TABLES.values()}
    for address in sorted(frontiers):
        reason='NEW_ACTUAL_FRONTIER_NO_PRIOR_EXTERNAL_OPPORTUNITY'
        if address in already:reason='EXCLUDED_ALREADY_FROZEN_THIS_REVISION'
        elif address in meta_success:reason='EXCLUDED_PRIOR_METASLEUTH_SUCCESS_INCLUDING_EMPTY'
        elif old_dune[address]>=all_tables:reason='EXCLUDED_PRIOR_COMPLETE_FOUR_TABLE_OPPORTUNITY'
        elif address in local_registry:reason='EXCLUDED_EXISTING_LOCAL_REGISTRY_REUSED'
        eligible=reason.startswith('NEW_')
        if eligible:selected.append(address)
        review.append(dict(address=address, eligible_for_external_lookup=eligible, reason=reason,
            actual_state_occurrences=frontiers[address], local_registry_present=address in local_registry,
            local_observation_count=len(local_obs[address]), prior_metasleuth_success=address in meta_success,
            prior_metasleuth_empty_success=address in meta_empty, prior_dune_tables=sorted(old_dune[address]),
            initial_lookup_status='UNQUERIED' if eligible else 'LOCAL_CACHE_REUSED_NO_NEW_QUERY'))
    if not selected or len(set(selected)|already)>40:
        raise ValueError('No eligible frontier addresses or cumulative 40-address cap exceeded')
    schema_path=root/'03_workspaces/stage0B1L2/raw/Q015_SCHEMA_INFORMATION_ALL_REGISTERED_TABLES.csv'
    mapping_path=root/'03_workspaces/stage0B1L2/derived/LABELS_ADDRESSES_CATEGORY_MAPPING_V2.json'
    details_path=root/'03_workspaces/stage0B1L2/raw/Q044_AUX_MATCHED_OWNER_DETAILS.csv'
    schema=rows(schema_path)
    inputs += [file_record(root,p,role) for p,role in ((schema_path,'VERIFIED_FIVE_TABLE_COLUMN_TYPES'),
        (mapping_path,'FROZEN_BEHAVIOR_OWNERSHIP_ROLE_MAPPING'),(details_path,'LOCAL_OWNER_DETAILS_NO_NEW_QUERY'))]
    for _, sqlname, _ in TABLES.values():
        inputs.append(file_record(root,root/'03_workspaces/stage0B1L2/sql'/sqlname,'PREVIOUSLY_EXECUTED_FOUR_TABLE_SCHEMA'))
    inputs += [file_record(root,work/'bootstrap/config/STAGE1B_R1_POLICY.json','CURRENT_AUTHORIZATION_POLICY'),
               file_record(root,work/'src/labels_policy.py','INHERITED_DUNE_EXPLICIT_PRIORITY_RESOLVER')]
    queue=dict(version=VERSION,batch=batch,frozen_at_utc=now(),external_addresses=selected,
        address_count=len(selected),actual_unqueried_unique_addresses=len(frontiers),review=review,
        selection='Actual collector UNQUERIED states only; no reference targets or result-dependent replacements',
        historical_metasleuth_unique_successes=len(meta_success),historical_metasleuth_empty_successes=len(meta_empty),
        cumulative_previously_frozen_addresses=len(already), network_calls=0)
    rules=dict(version=VERSION,source_tables=list(TABLES),source_precedence='DUNE_EXPLICIT_INTERNALLY_CONSISTENT_PRIORITY',
        raw_conflicts_retained=True,behavior_establishes_custody=False,unknown_intermediate_blocks_chain=False,
        undisclosed_generation_mechanism=None,static_does_not_imply_manual=True,
        labels_addresses_mapping=read(mapping_path),
        owner_details_cache={r['owner_key'].lower():r for r in rows(details_path) if truth(r.get('matched'))},
        owner_details_missing='Retain owner raw fields and metadata gap; do not infer service from owner name',
        acquisition_scope='ACTUAL_PILOT_FRONTIER_UNIFORM_FOUR_TABLE_OPPORTUNITY',
        table_schema=schema, output_encoding='Four JSON arrays of complete maps; values VARCHAR-or-null; count is source rows',
        no_match_meaning='SUCCESS_NO_MATCH_IN_THIS_TABLE_EXECUTION_ONLY',
        future_batches='Same frozen rules; new actual UNQUERIED states only; max 3 label jobs/40 unique under root shared ledger')
    sql=build_sql(selected,schema)
    dest.mkdir(parents=True)
    dump(dest/'queue.json',queue);write_csv(dest/'queue.csv',review)
    dump(dest/'source_rules.json',rules)
    (dest/'combined_frontier_labels.sql').write_bytes(sql.encode('utf-8'))
    manifest=dict(version=VERSION,batch=batch,phase='FROZEN_NOT_SUBMITTED',queue_sha256=digest(dest/'queue.json'),
        sql_sha256=digest(dest/'combined_frontier_labels.sql'),base_snapshot=snap.relative_to(root).as_posix(),
        inherited_registry_sha256=digest(registry_path),inherited_observation_sha256=digest(observation_path),
        source_inputs=inputs,external_addresses=selected,network_calls=0,
        frozen_files=[dict(name=p.name,sha256=digest(p),bytes=p.stat().st_size) for p in sorted(dest.iterdir()) if p.is_file()])
    dump(dest/'manifest.json',manifest)
    return manifest

def optimize_static(work, root, failed_jobdir):
    """Prepare one reviewed optimization; never retry or submit the execution."""
    root=Path(root).resolve();work=checked(root,work);failed_jobdir=checked(root,failed_jobdir)
    old=work/'derived/frontier_labels/batch_01';dest=work/'derived/frontier_labels/batch_01_optimized_v1'
    manifest=read(old/'manifest.json');queue=read(old/'queue.json');job=read(failed_jobdir/'job.json')
    for rec in manifest['frozen_files']:
        if digest(old/rec['name'])!=rec['sha256']:raise ValueError('Original frozen file changed')
    if job.get('state')!='QUERY_STATE_FAILED' or (job.get('status_response') or {}).get('error',{}).get('type')!='FAILED_TYPE_RESOURCES_CAP_REACHED':
        raise ValueError('Optimization is limited to the recorded resource-cap failure')
    prior_sql=(old/'combined_frontier_labels.sql').read_text(encoding='utf-8')
    executed_sql=(failed_jobdir/'query.sql').read_text(encoding='utf-8')
    if prior_sql!=executed_sql:raise ValueError('Failed execution differs from frozen SQL beyond line endings')
    if dest.exists():raise ValueError('Optimized version already exists; no replacement')
    static="  WHERE s.blockchain = 'ethereum'\n    AND s.address IN (\n      "+',\n      '.join(queue['external_addresses'])+'\n    )'
    original="  INNER JOIN frontier f ON s.address = f.address\n  WHERE s.blockchain = 'ethereum'"
    if prior_sql.count(original)!=4:raise ValueError('Unexpected source filter count')
    sql=prior_sql.replace(original,static)
    sql='-- Static predicate optimization of the identical frozen queue after one failed capped execution.\n'+sql
    dest.mkdir(parents=True)
    for name in ('queue.json','queue.csv','source_rules.json'):(dest/name).write_bytes((old/name).read_bytes())
    (dest/'combined_frontier_labels.sql').write_bytes(sql.encode('utf-8'))
    out=dict(manifest,batch='batch_01_optimized_v1',phase='OPTIMIZATION_FROZEN_NOT_SUBMITTED',
        original_batch_manifest=old.relative_to(root).as_posix()+'/manifest.json',original_batch_manifest_sha256=digest(old/'manifest.json'),
        original_frozen_sql_sha256=manifest['sql_sha256'],original_executed_sql_sha256=job['sql_sha256'],
        original_execution_id=job['execution_id'],original_failure='FAILED_TYPE_RESOURCES_CAP_REACHED',
        original_failure_job=failed_jobdir.relative_to(root).as_posix()+'/job.json',original_failure_job_sha256=digest(failed_jobdir/'job.json'),
        original_line_ending_normalization_verified=True,sql_sha256=digest(dest/'combined_frontier_labels.sql'),
        optimization='Four dynamic join filters replaced by static binary-address IN predicates; all 9 addresses, 4 sources, complete matching arrays and row counts retained',
        optimization_performance_not_yet_verified=True,if_second_cap_failure='Pause frontier-label SQL; no automatic third retry or execution-cap increase',
        frozen_files=[dict(name=p.name,sha256=digest(p),bytes=p.stat().st_size) for p in sorted(dest.iterdir()) if p.is_file()])
    dump(dest/'manifest.json',out)
    return out

def parse_result_rows(result_rows, expected_addresses):
    """Fail closed on omitted source opportunity, truncated arrays or duplicates."""
    expected={full_address(a) for a in expected_addresses}; by={}
    required={'address'} | {k+s for k in TABLES for s in ('_json','_count')}
    for row in result_rows:
        if set(row)!=required:raise ValueError('Unexpected combined-result columns')
        address=full_address(row['address'])
        if address in by or address not in expected:raise ValueError('Duplicate or unfrozen address')
        source={}
        for alias, (table, _, fields) in TABLES.items():
            values=json.loads(row[alias+'_json'])
            count=row[alias+'_count']
            if isinstance(count,bool) or not isinstance(count,int) or count<0:raise ValueError('Invalid match count')
            if not isinstance(values,list) or len(values)!=count:raise ValueError('Truncated or invalid aggregate')
            for value in values:
                if not isinstance(value,dict) or set(value)!=set(fields):raise ValueError('Source match fields changed')
                if any(v is not None and not isinstance(v,str) for v in value.values()):raise ValueError('Unexpected source value type')
                if value['blockchain']!='ethereum':raise ValueError('Unexpected source chain')
            source[table]=sorted(values,key=stable)
        by[address]=source
    if set(by)!=expected:raise ValueError('Not every frozen address received all four opportunities')
    return by

def saved_job_rows(work, batchdir, jobdir):
    work=Path(work);jobdir=Path(jobdir);manifest=read(batchdir/'manifest.json')
    for rec in manifest['frozen_files']:
        if digest(batchdir/rec['name'])!=rec['sha256']:raise ValueError('Frozen batch file changed')
    job=read(jobdir/'job.json')
    sqlpath=jobdir/'query.sql'
    if not sqlpath.exists():sqlpath=jobdir/'job.sql'
    frozen_sql=(batchdir/'combined_frontier_labels.sql').read_text(encoding='utf-8')
    submitted_sha=hashlib.sha256(frozen_sql.encode('utf-8')).hexdigest()
    if sqlpath.read_text(encoding='utf-8')!=frozen_sql or job.get('sql_sha256')!=submitted_sha:
        raise ValueError('Saved job does not execute the frozen queue SQL')
    if job.get('state')!='QUERY_STATE_COMPLETED':raise ValueError('Job execution not completed')
    offsets=job.get('export_offsets',[])
    if len(offsets)!=job.get('export_requests') or len(offsets)!=len(set(offsets)):
        raise ValueError('Inconsistent saved export history')
    status=job.get('status_response') or {}
    if status.get('state')!='QUERY_STATE_COMPLETED' or status.get('execution_id')!=job.get('execution_id'):
        raise ValueError('Saved final status identity mismatch')
    progress=initial_progress();allrows=[];evidence=[]
    for offset in offsets:
        p=jobdir/f'page_{offset}.json';rp=jobdir/f'page_{offset}_receipt.json';page=read(p);receipt=read(rp)
        params=receipt.get('parameters') or {}
        if params.get('offset')!=offset:raise ValueError('Saved receipt offset mismatch')
        raw=checked(work,work/receipt['raw_path'])
        if digest(raw)!=receipt['sha256']:raise ValueError('Saved provider raw response hash mismatch')
        if read(raw)!=page:raise ValueError('Saved parsed page does not match original raw response')
        progress=validate_page(page,execution_id=job['execution_id'],offset=offset,limit=params.get('limit'),
            progress=progress,status_metadata=status.get('result_metadata') or {},receipt=receipt,parameters=params)
        allrows.extend(page['result']['rows'])
        evidence.append(dict(page=str(p),page_sha256=digest(p),receipt=str(rp),receipt_sha256=digest(rp),
            raw_path=receipt['raw_path'],raw_sha256=receipt['sha256'],offset=offset,rows=len(page['result']['rows'])))
    if not progress['complete']:raise ValueError('Full execution result pagination not complete')
    return parse_result_rows(allrows,manifest['external_addresses']),job,evidence

def classify(table, raw, rules):
    """Narrow inherited role mapping; no behavioral label becomes ownership."""
    actor=''; role='UNKNOWN';kind='CONTEXT';rule='UNMAPPED_RETAINED_RAW';detail={};conflict=False
    if table in ('cex.addresses','cex.deposit_addresses'):
        actor=raw.get('cex_name') or '';role='SERVICE';kind='ATTRIBUTION'
        rule='EXPLICIT_CEX_NAMED_ADDRESS' if table=='cex.addresses' else 'PROGRAMMATIC_NAMED_DEPOSIT_NOT_MANUAL_OR_LEGAL_CONFIRMATION'
    elif table=='labels.addresses':
        labeltype=(raw.get('label_type') or '').lower()
        if labeltype in ('persona','usage'):
            kind='BEHAVIOR';rule='PERSONA_OR_USAGE_NEVER_CONTROL_ATTRIBUTION'
        else:
            mapping='UNMAPPED'
            for r in rules['labels_addresses_mapping']['exact_record_rules']:
                if raw.get('category')!=r['category']:continue
                if r.get('label_type') and labeltype!=r['label_type']:continue
                if r.get('source') and (raw.get('source') or '').lower()!=r['source']:continue
                mapping=r['classification'];break
            if mapping in ('CURATED_NAMED_CUSTODIAL_IF_NON_GENERIC_ACTOR','NAMED_BRIDGE'):
                actor=raw.get('name') or '';role='SERVICE' if mapping.startswith('CURATED_') else 'BRIDGE_BOUNDARY';kind='ATTRIBUTION'
            rule='INHERITED_LABELS_ADDRESSES_V2:'+mapping
    elif table=='labels.owner_addresses':
        key=(raw.get('owner_key') or '').lower();detail=rules['owner_details_cache'].get(key,{})
        candidates=[v for v in (raw.get('custody_owner'),raw.get('account_owner'),detail.get('name')) if v]
        actors={canonical_actor(v)[1]:canonical_actor(v)[0] for v in candidates}
        conflict=len(actors)>1
        actor=' | '.join(actors[k] for k in sorted(actors)) if actors else (raw.get('owner_key') or '')
        primary=(detail.get('primary_category') or '').lower();contract=(raw.get('contract_name') or '').lower()
        blob=' '.join(str(v or '') for v in [key,actor,contract,primary,detail.get('category_tags')]).lower()
        if key=='gnosis_safe' or contract.startswith('gnosissafe'):
            role='DEX_OR_PROTOCOL';rule='OWNER_GNOSIS_SAFE_SELF_CUSTODY_PROTOCOL_GUARD'
        elif primary=='bridge' or 'bridge' in contract or key in ('arbitrum','celer_network','hop_protocol'):
            role='BRIDGE_BOUNDARY';rule='OWNER_PRIMARY_OR_CONTRACT_BRIDGE_BOUNDARY'
        elif primary=='privacy services' or any(x in blob for x in ('tornado','mixer')):
            role='MIXER_BOUNDARY';rule='OWNER_PRIVACY_OR_MIXER_BOUNDARY'
        elif primary=='decentralized exchange' or key in ('cow_protocol','opensea','sushiswap','symbiosis') or any(x in contract for x in ('router','cowswap','seaport')):
            role='DEX_OR_PROTOCOL';rule='OWNER_DEX_PROTOCOL_OR_OPENSEA_BOUNDARY'
        elif primary in ('centralized exchange','custody services','payment processing','casino','sports betting','sports betting & prediction markets'):
            role='SERVICE';rule='OWNER_DETAILS_PRIMARY_CATEGORY_CUSTODIAL_ALLOWLIST'
        else:rule='OWNER_METADATA_INSUFFICIENT_FOR_SERVICE_OR_PROTOCOL_ROLE'
        kind='ATTRIBUTION' if role!='UNKNOWN' else 'CONTEXT'
    actor,akey=canonical_actor(actor)
    if not akey or akey in {'unknown','exchange','cex','dex','institution','bridge','service','user','users','wallet'}:
        actor='';role='UNKNOWN';kind='CONTEXT' if kind=='ATTRIBUTION' else kind;rule+=';NAMED_ACTOR_REQUIRED'
    if re.search(r'\b(cex users?|dex users?|exchange users?|deployer|deployed by)\b|user of',
                 ' '.join(str(raw.get(k) or '') for k in ('name','distinct_name','contract_name')).lower()):
        actor='';role='UNKNOWN';kind='BEHAVIOR';rule+=';BEHAVIOR_OR_DEPLOYER_NOT_CONTROL'
    method='ALGORITHM_IDENTIFIED' if table=='cex.deposit_addresses' or raw.get('algorithm_name') or raw.get('source')=='query' else None
    return dict(actor=actor or None,actor_key=actor_key(actor) or None,role=role,semantic_kind=kind,
        record_conflict=conflict,generation_mechanism=method,classification_rule=rule,
        owner_details=detail,owner_metadata_missing=table=='labels.owner_addresses' and not detail)

def observations_from_parsed(parsed, rules, execution_id, acquired_at, evidence_ref, evidence_sha):
    observations=[];opportunities=[]
    for address, sources in sorted(parsed.items()):
        for table, matches in sources.items():
            opportunities.append(dict(address=address,source_table=table,execution_id=execution_id,
                match_count=len(matches),status='SUCCESS_WITH_MATCHES' if matches else 'SUCCESS_NO_MATCH_IN_THIS_TABLE_EXECUTION_ONLY'))
            for index, raw in enumerate(matches if matches else [None]):
                fields=classify(table,raw,rules) if raw is not None else dict(actor=None,actor_key=None,role='UNKNOWN',
                    semantic_kind='CONTEXT',record_conflict=False,generation_mechanism=None,
                    classification_rule='SUCCESS_NO_MATCH_IN_THIS_TABLE_EXECUTION_ONLY',owner_details={},owner_metadata_missing=False)
                rawlabel=stable(raw) if raw is not None else '[]'
                locator=address+'/'+table+'/match['+str(index)+']' if raw is not None else address+'/'+table+'/NO_MATCH'
                meta=dict(table=table,raw_source_record=raw,classification_rule=fields.pop('classification_rule'),
                    local_owner_details=fields.pop('owner_details'),owner_metadata_missing=fields.pop('owner_metadata_missing'),
                    lookup_status='SUCCESS_WITH_MATCHES' if raw is not None else 'SUCCESS_NO_MATCH_IN_THIS_TABLE_EXECUTION_ONLY',
                    no_incident_date_or_legal_subject_inference=True, original_duplicate_source_rows_preserved=True)
                o=dict(chain_id='1',address=address,raw_label=rawlabel,platform='Dune',source_version=table+'@'+execution_id,
                    acquired_at=acquired_at,collection_scope='ACTUAL_PILOT_FRONTIER_UNIFORM_FOUR_TABLE_OPPORTUNITY',
                    request_or_execution_id=execution_id,raw_evidence_ref=evidence_ref,raw_evidence_sha256=evidence_sha,
                    source_locator=locator,source_metadata=meta,**fields)
                o['generation_status']='DISCLOSED' if o['generation_mechanism'] else 'PROVIDER_METHOD_UNDISCLOSED'
                o['semantic_label_key']=sid([1,address,o['actor_key'],o['role'],o['semantic_kind'],rawlabel])
                o['observation_id']='obs:'+sid(['Dune',o['source_version'],evidence_ref,locator,address,rawlabel])
                observations.append(o)
    return observations,opportunities

def apply(work, root, batch, jobdir):
    root=Path(root).resolve();work=checked(root,work);jobdir=checked(root,jobdir)
    batchdir=work/'derived/frontier_labels'/batch
    manifest=read(batchdir/'manifest.json');rules=read(batchdir/'source_rules.json')
    if (batchdir/'apply_manifest.json').exists():
        prior=read(batchdir/'apply_manifest.json')
        if prior['job_sha256']!=digest(jobdir/'job.json'):raise ValueError('Applied job identity changed; no overwrite')
        return prior
    parsed,job,evidence=saved_job_rows(work,batchdir,jobdir)
    old=root/manifest['base_snapshot']
    old_observations=snapshot_observations(root,old)
    for path,key in [(old/'address_registry.csv.gz','inherited_registry_sha256'),(old_observations,'inherited_observation_sha256')]:
        if digest(path)!=manifest[key]:raise ValueError('Frozen inherited input changed')
    execution=job['execution_id'];version=work.name+'_labels_'+batch
    out=work/'derived/label_snapshots'/version
    if out.exists():raise ValueError('New snapshot directory already exists; inspect incomplete apply')
    oldobs=rows(old_observations);oldreg=rows(old/'address_registry.csv.gz')
    index={r['address']:r for r in oldreg};group=defaultdict(list)
    for o in oldobs:group[o['address']].append(o)
    prior_groups={address:list(items) for address,items in group.items()}
    # Evidence manifest binds every complete page to the exact saved raw SHA.
    evidencepath=batchdir/'applied_page_evidence.json'
    dump(evidencepath,dict(execution_id=execution,job=jobdir.relative_to(root).as_posix(),pages=evidence))
    acquired=(job.get('status_response') or {}).get('execution_ended_at') or (job.get('status_receipt') or {}).get('utc')
    if not acquired:raise ValueError('Actual acquisition/completion timestamp missing')
    added,opportunities=observations_from_parsed(parsed,rules,execution,acquired,evidencepath.relative_to(root).as_posix(),digest(evidencepath))
    for o in added:group[o['address']].append(o)
    changes=[]
    for address in parsed:
        before=index.get(address,{'chain_id':'1','address':address,'identity_class':'UNKNOWN','actor':''})
        after=resolve_address(group[address],before,baseline_observations=prior_groups.get(address, []))
        if any(truth(o.get('record_conflict')) for o in group[address]):
            after['preserved_conflict']=True
            if after['identity_class']=='UNKNOWN':after['conflict_status']='RAW_SOURCE_INTERNAL_CONFLICT_ROLE_UNRESOLVED'
        after.update(acquisition_scope=rules['acquisition_scope'],lookup_status='COMPLETED_FOUR_TABLE_OPPORTUNITY',
            last_frontier_label_execution_id=execution,label_snapshot_version=version,
            distinct_label_facts=len({o['semantic_label_key'] for o in group[address]}),
            actor_keys=sorted({o.get('actor_key') for o in group[address] if o.get('actor_key')}))
        index[address]=after
        changes.append(dict(address=address,old_identity_class=before.get('identity_class'),new_identity_class=after['identity_class'],
            old_actor=before.get('actor') or '',new_actor=after.get('actor') or '',
            identity_or_actor_changed=(before.get('identity_class'),before.get('actor') or '')!=(after['identity_class'],after.get('actor') or ''),
            new_observations=len([o for o in added if o['address']==address]),lookup_status=after['lookup_status'],
            resolution_rule=after['resolution_rule'],preserved_conflict=after['preserved_conflict'],
            adopted_observation_ids=after['adopted_observation_ids'],all_observation_ids=after['observation_ids'],
            adopted_sources=after['adopted_sources'],adopted_source_versions=after['adopted_source_versions'],
            provenance_status=after['provenance_status'],provenance_issues=after['provenance_issues'],
            unadopted_observation_ids=after['unadopted_observation_ids'],
            old_registry_row_present=address in {r['address'] for r in oldreg}))
    out.mkdir(parents=True)
    obscols=list(oldobs[0]);regcols=list(oldreg[0])+['lookup_status','last_frontier_label_execution_id','label_snapshot_version']
    for row in index.values():
        for key in row:
            if key not in regcols:regcols.append(key)
    write_csv(out/'label_observations.csv.gz',oldobs+added,obscols)
    write_csv(out/'address_registry.csv.gz',[index[k] for k in sorted(index)],regcols)
    dump(out/'label_policy.json',dict(read(old/'label_policy.json'),frontier_extension_version=VERSION,
        frozen_source_rules_sha256=digest(batchdir/'source_rules.json'),unknown_intermediate_blocks_chain=False))
    write_csv(batchdir/'new_observations.csv.gz',added,obscols)
    write_csv(batchdir/'source_opportunities.csv',opportunities)
    write_csv(batchdir/'label_resolution_delta.csv',changes)
    refs=reference_delta(work,batchdir,index,changes)
    result=dict(version=VERSION,label_snapshot_version=version,label_snapshot_path=out.relative_to(root).as_posix(),
        registry_sha256=digest(out/'address_registry.csv.gz'),observations_sha256=digest(out/'label_observations.csv.gz'),
        base_registry_sha256=manifest['inherited_registry_sha256'],base_observations_sha256=manifest['inherited_observation_sha256'],
        inherited_observation_rows=len(oldobs),new_observation_rows=len(added),registry_rows=len(index),
        frontier_address_count=len(parsed),source_opportunities=len(opportunities),execution_id=execution,
        queue_sha256=manifest['queue_sha256'],sql_sha256=manifest['sql_sha256'],job_sha256=digest(jobdir/'job.json'),
        match_status_counts=dict(Counter(o['status'] for o in opportunities)),
        identity_changes=sum(r['identity_or_actor_changed'] for r in changes),reference=refs,
        scope='LABEL_UPDATE_SAME_RAW; root must replay the two fixed pilots before any continuation',
        generation_mechanism_missing=sum(not o['generation_mechanism'] for o in added),network_calls=0,
        old_inputs_unchanged=(digest(old/'address_registry.csv.gz')==manifest['inherited_registry_sha256'] and
                              digest(old_observations)==manifest['inherited_observation_sha256']))
    prior_failures=sorted((work/'derived/frontier_labels').glob('failed_lookup_revision_*/apply_manifest.json'))
    if prior_failures:
        result['preceding_lookup_manifest_path']=prior_failures[-1].relative_to(root).as_posix()
        result['preceding_lookup_manifest_sha256']=digest(prior_failures[-1])
    dump(out/'source_manifest.json',result);dump(batchdir/'apply_manifest.json',result)
    return result

def record_failure(work, root, jobdirs):
    """Record failed opportunities without fabricating empty source responses."""
    root=Path(root).resolve();work=checked(root,work)
    if not 2<=len(jobdirs)<=3:raise ValueError('Expected two or three separately recorded capped attempts')
    revision_number=len(jobdirs)-1
    dest=work/'derived/frontier_labels'/('failed_lookup_revision_'+str(revision_number).zfill(2))
    if dest.exists():raise ValueError('Failure revision already exists; do not overwrite')
    original=work/'derived/frontier_labels/batch_01';mf=read(original/'manifest.json');queue=read(original/'queue.json')
    for rec in mf['frozen_files']:
        if digest(original/rec['name'])!=rec['sha256']:raise ValueError('Frozen original label input changed')
    evidence=[]
    seen=set()
    for folder in jobdirs:
        folder=checked(root,folder);job=read(folder/'job.json')
        if job.get('execution_id') in seen:raise ValueError('Duplicate failed execution')
        seen.add(job.get('execution_id'))
        status=job.get('status_response') or {};receipt=job.get('status_receipt') or {}
        if job.get('state')!='QUERY_STATE_FAILED' or status.get('state')!='QUERY_STATE_FAILED' or status.get('execution_id')!=job.get('execution_id'):
            raise ValueError('Failure execution/state identity mismatch')
        if status.get('error',{}).get('type')!='FAILED_TYPE_RESOURCES_CAP_REACHED':raise ValueError('Unexpected failure class')
        if job.get('export_requests')!=0 or job.get('export_offsets')!=[]:raise ValueError('Unexpected export after failed execution')
        if receipt.get('http_status')!=200 or receipt.get('error_class'):raise ValueError('Failure status was not received successfully')
        raw=checked(work,work/receipt['raw_path'])
        if digest(raw)!=receipt['sha256'] or read(raw)!=status:raise ValueError('Failure raw status evidence mismatch')
        scopepath=Path(job['scope_freeze_path'])
        scope=checked(root,scopepath if scopepath.is_absolute() else work/scopepath);frozen=read(scope)
        if digest(scope)!=job['scope_freeze_sha256']:raise ValueError('Failed job frozen scope hash changed')
        for rec in frozen['frozen_files']:
            if digest(scope.parent/rec['name'])!=rec['sha256']:raise ValueError('Failed job frozen input changed')
        if frozen['queue_sha256']!=mf['queue_sha256'] or frozen['external_addresses']!=mf['external_addresses']:
            raise ValueError('Failed attempts changed the original frozen address cohort')
        sql=(scope.parent/'combined_frontier_labels.sql').read_text(encoding='utf-8')
        if sql!=(folder/'query.sql').read_text(encoding='utf-8') or hashlib.sha256(sql.encode()).hexdigest()!=job['sql_sha256']:
            raise ValueError('Failed attempt SQL identity mismatch')
        evidence.append(dict(execution_id=job['execution_id'],job_path=(folder/'job.json').relative_to(root).as_posix(),
            job_sha256=digest(folder/'job.json'),scope_manifest_path=scope.relative_to(root).as_posix(),scope_manifest_sha256=digest(scope),
            sql_sha256=job['sql_sha256'],failure_type=status['error']['type'],failure_message=status['error']['message'],
            status_raw_path=raw.relative_to(root).as_posix(),status_raw_sha256=digest(raw),status_receipt=receipt,
            execution_cost_credits_observed=str(status.get('execution_cost_credits')),export_requests=0,
            authorization_id=job.get('authorization_id'),logical_job_id=job.get('logical_job_id'),
            reserved_execution_credits=job.get('reserved_execution'),
            no_source_results_returned=True))
    old=root/mf['base_snapshot'];obs=snapshot_observations(root,old)
    if digest(old/'address_registry.csv.gz')!=mf['inherited_registry_sha256'] or digest(obs)!=mf['inherited_observation_sha256']:
        raise ValueError('Inherited labels changed')
    registry=rows(old/'address_registry.csv.gz');index={r['address']:r for r in registry}
    if any(a in index for a in queue['external_addresses']):raise ValueError('Initial missing-frontier cohort unexpectedly has an inherited row')
    version=work.name+'_labels_lookup_failed_v'+str(revision_number);out=work/'derived/label_snapshots'/version
    if out.exists():raise ValueError('Failure snapshot version already exists')
    changes=[];opportunities=[]
    for address in queue['external_addresses']:
        index[address]=dict(chain_id='1',address=address,actor='',actor_candidates=[],actor_keys=[],identity_class='UNKNOWN',
            role_candidates=[],conflict_status='NO_RETURNED_LABEL_FACTS',observation_count=0,distinct_label_facts=0,
            source_platforms=[],observation_ids=[],selective_evidence_present=False,generation_method_undisclosed_present=False,
            service_status='NOT_CONFIRMED_AS_REQUESTABLE_SERVICE',temporal_control_claim='No incident-date or legal-subject identity inference',
            resolution_rule='NO_NEW_LABEL_FACTS_FAILED_DUNE_LOOKUP',label_policy_version='stage1b-dune-preferred-1.0',
            adopted_observation_ids=[],adopted_sources=[],adopted_source_versions=[],preserved_conflict=False,
            generation_method_undisclosed_observations=0,acquisition_scope='ACTUAL_PILOT_FRONTIER_LOOKUP_ATTEMPT_FAILED',
            lookup_status='LOOKUP_FAILED_RESOURCE_CAP',label_snapshot_version=version,
            last_frontier_label_execution_id=';'.join(sorted(seen)))
        changes.append(dict(address=address,old_identity_class='UNKNOWN',new_identity_class='UNKNOWN',old_actor='',new_actor='',
            identity_or_actor_changed=False,new_observations=0,lookup_status='LOOKUP_FAILED_RESOURCE_CAP',
            resolution_rule='NO_NEW_LABEL_FACTS_FAILED_DUNE_LOOKUP',preserved_conflict=False,
            adopted_observation_ids=[],all_observation_ids=[],old_registry_row_present=False))
        for table,_,_ in TABLES.values():
            opportunities.append(dict(address=address,source_table=table,status='FAILED_RESOURCE_CAP',
                attempted_in_executions=sorted(seen),planned_source_opportunity=True,successful_source_opportunity=False,
                matched=None,match_count=None,source_result_available=False,
                explanation='All recorded complete four-table SQL jobs failed at execution cap; no per-table results exported; this is not NO_LABEL'))
    dest.mkdir(parents=True);out.mkdir(parents=True)
    columns=list(dict.fromkeys(list(registry[0])+['lookup_status','last_frontier_label_execution_id','label_snapshot_version']))
    write_csv(out/'address_registry.csv.gz',[index[a] for a in sorted(index)],columns)
    policy=dict(read(old/'label_policy.json'),lookup_revision=VERSION,failed_lookup_is_no_label=False,
                unknown_intermediate_blocks_chain=False,no_new_label_observations=True)
    dump(out/'label_policy.json',policy)
    write_csv(dest/'source_opportunities.csv',opportunities)
    write_csv(dest/'label_resolution_delta.csv',changes)
    write_csv(dest/'new_observations.csv.gz',[],list(rows(obs)[0]))
    refs=reference_delta(work,dest,index,changes,verify_unchanged=True)
    result=dict(version=VERSION,status='PARTIAL',label_snapshot_version=version,label_snapshot_path=out.relative_to(root).as_posix(),
        registry_sha256=digest(out/'address_registry.csv.gz'),observations_sha256=digest(obs),
        observations_source_path=obs.relative_to(root).as_posix(),observations_reused_by_reference=True,
        base_registry_sha256=mf['inherited_registry_sha256'],base_observations_sha256=mf['inherited_observation_sha256'],
        queue_sha256=mf['queue_sha256'],frontier_address_count=len(queue['external_addresses']),
        actual_external_unique_addresses=len(queue['external_addresses']),actual_sql_attempts=len(evidence),
        successful_source_opportunities=0,failed_source_opportunities=len(opportunities),new_observation_rows=0,
        identity_changes=0,unknown_context_rows_added=len(queue['external_addresses']),registry_rows=len(index),
        label_facts_unchanged=True,unknown_intermediate_blocks_chain=False,lookup_failure_blocks_chain=False,
        metamethod='No new MetaSleuth request or address',metasleuth_new_addresses=0,
        source_evidence=evidence,reference=refs,scope='LABEL_UPDATE_SAME_RAW with changed lookup status only; retained label facts',
        must_replay_existing_pilot_raw_before_continuation=True,no_source_result_means_no_label=False,
        old_inputs_unchanged=(digest(old/'address_registry.csv.gz')==mf['inherited_registry_sha256'] and digest(obs)==mf['inherited_observation_sha256']),
        network_calls_by_this_module=0,budget_mutations_by_this_module=0)
    if revision_number>1:
        preceding=work/'derived/frontier_labels'/('failed_lookup_revision_'+str(revision_number-1).zfill(2))/'apply_manifest.json'
        if preceding.exists():
            result['preceding_lookup_manifest_path']=preceding.relative_to(root).as_posix()
            result['preceding_lookup_manifest_sha256']=digest(preceding)
    dump(out/'source_manifest.json',result);dump(dest/'apply_manifest.json',result)
    return result

def addresses_in(value):
    """Structural endpoint/state selection, not a reference discovery whitelist."""
    result=set()
    if isinstance(value,dict):
        for key,item in value.items():
            if key in ('address','sender','recipient','from_address','to_address','target_address','seed_from','seed_to'):
                if isinstance(item,str) and re.fullmatch(r'0x[0-9a-fA-F]{40}',item):result.add(item.lower())
            result.update(addresses_in(item))
    elif isinstance(value,list):
        for item in value:result.update(addresses_in(item))
    return result

def select_registry_slice(registry, required_addresses):
    """Preserve every selected row exactly; missing rows stay missing/UNQUERIED."""
    required={full_address(a) for a in required_addresses}
    index={r['address']:r for r in registry}
    if len(index)!=len(registry):raise ValueError('Contradictory/duplicate full registry address rows')
    selected=[index[a] for a in sorted(required & set(index))]
    missing=sorted(required-set(index))
    return selected,missing

def portable(work, root, label_manifest, output):
    """Assemble only private replay necessities; do not copy full observations."""
    root=Path(root).resolve();work=checked(root,work);label_manifest=checked(root,label_manifest);output=checked(root,output)
    if output.exists():raise ValueError('Portable output must be a new directory; never rewrite frozen artifacts')
    applied=read(label_manifest)
    if not applied.get('label_snapshot_path') or not applied.get('registry_sha256'):
        raise ValueError('A final applied full-snapshot manifest is required')
    full_registry=checked(root,root/applied['label_snapshot_path']/'address_registry.csv.gz')
    if digest(full_registry)!=applied['registry_sha256']:raise ValueError('Final source registry hash mismatch')
    input_dir=work/'private/reference_replay'
    if not input_dir.exists():input_dir=work/'inherited_min/private/reference_replay'
    # Original identity slices are deliberately not copied: they belong to
    # earlier label versions. The five immutable fact/membership files remain.
    reference_names=('reference_event_slice.csv.gz','reference_query_members.csv',
        'reference_incident_event_membership.csv.gz','A_original_reference_entries.csv.gz','B_reference_relation_recheck.csv.gz')
    cache_path=work/'private/cache_replay/value_events_normalized.csv.gz'
    required=defaultdict(set);reference_addresses=set();sources=[]
    def add_addresses(values,reason):
        for address in addresses_in(values):required[address].add(reason)
    for name in reference_names:
        p=checked(root,input_dir/name);r=rows(p);selected=addresses_in(r);reference_addresses.update(selected)
        add_addresses(r,'FINITE_REFERENCE_FACTS_MEMBERSHIP_OR_TARGET_ROW')
        sources.append(file_record(root,p,'IMMUTABLE_PRIVATE_REFERENCE_FACTS'))
    cache_records=rows(cache_path)
    add_addresses(cache_records,'ALL_INHERITED_CANONICAL_CACHE_ENDPOINTS')
    sources.append(file_record(root,cache_path,'COMPLETE_INHERITED_SPARSE_CACHE_INPUT_NOT_ADDRESS_HISTORY'))
    collection_paths=set()
    for folder in (work/'baseline_derived',work/'derived'):
        for name in ('collection.json','cached_collection.json'):
            collection_paths.update(folder.rglob(name))
    for p in sorted(collection_paths):
        add_addresses(read(p),'ACTUAL_COLLECTION_ENDPOINTS_STATES_AND_CONTEXT')
        sources.append(file_record(root,p,'SAVED_ACTUAL_OR_CACHE_COLLECTION_ADDRESS_SCOPE'))
    initial_queue=work/'derived/frontier_labels/batch_01/queue.json'
    for address in read(initial_queue)['external_addresses']:required[full_address(address)].add('FROZEN_INITIAL_NINE_ACTUAL_FRONTIERS')
    sources.append(file_record(root,initial_queue,'FROZEN_INITIAL_ACTUAL_FRONTIER_COHORT'))
    registry=rows(full_registry);sliced,missing=select_registry_slice(registry,required)
    reference_sliced,_=select_registry_slice(registry,reference_addresses)
    output.mkdir(parents=True);reference_output=output/'reference_replay';reference_output.mkdir()
    write_csv(output/'address_registry.csv.gz',sliced,list(registry[0]))
    write_csv(reference_output/'reference_identity_slice.csv.gz',reference_sliced,list(registry[0]))
    mappings=[]
    def copy_private(source,target,role):
        target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(source.read_bytes())
        mappings.append(dict(source=source.relative_to(root).as_posix(),source_sha256=digest(source),
            portable_path=target.relative_to(output).as_posix(),portable_sha256=digest(target),role=role,
            bytes=target.stat().st_size,byte_identical=True))
    for name in reference_names:copy_private(input_dir/name,reference_output/name,'UNCHANGED_REFERENCE_FACTS_NO_OLD_IDENTITY_SLICE')
    copy_private(label_manifest,output/'source_apply_manifest.json','ORIGINAL_FINAL_APPLY_MANIFEST_UNCHANGED')
    delta_dir=label_manifest.parent
    for name in ('label_resolution_delta.csv','source_opportunities.csv','new_observations.csv.gz',
                 'reference_relation_delta.csv.gz','reference_relations_updated.csv.gz','reference_delta_summary.json'):
        p=delta_dir/name
        if p.exists():copy_private(p,output/'diffs'/name,'FINAL_VERSION_LABEL_OR_REFERENCE_DIFF')
    write_csv(output/'address_selection.csv',[dict(address=a,required_by=sorted(required[a]),
        registry_row_included=a not in missing,missing_row_semantics='UNCHANGED_UNQUERIED' if a in missing else 'EXACT_FINAL_REGISTRY_ROW') for a in sorted(required)])
    manifest=dict(version=VERSION,type='PRIVATE_PORTABLE_LABEL_REFERENCE_SLICE',registry_path='address_registry.csv.gz',
        registry_sha256=digest(output/'address_registry.csv.gz'),label_snapshot_version=applied['label_snapshot_version'],
        source_registry_path=full_registry.relative_to(root).as_posix(),source_registry_sha256=digest(full_registry),
        source_apply_manifest_path=label_manifest.relative_to(root).as_posix(),source_apply_manifest_sha256=digest(label_manifest),
        source_observations_sha256=applied['observations_sha256'],source_observations_path=applied.get('observations_source_path') or
            (applied['label_snapshot_path']+'/label_observations.csv.gz'),full_observations_copied=False,
        selected_registry_rows=len(sliced),required_unique_addresses=len(required),missing_registry_addresses=len(missing),
        full_source_registry_rows=len(registry),reference_identity_rows=len(reference_sliced),
        reference_identity_path='reference_replay/reference_identity_slice.csv.gz',
        reference_identity_sha256=digest(reference_output/'reference_identity_slice.csv.gz'),
        selection='Union of all finite reference/canonical-cache endpoints and registered reference target rows, every saved actual/cache collector state/context endpoint, and the frozen initial actual frontier cohort; select existing registry rows only',
        reference_data_are_not_provider_neighbors=True,unknown_missing_rows_not_fabricated=True,
        publication_allowed=False,network_calls=0,source_inputs=sources,byte_identical_file_mappings=mappings)
    dump(output/'portable_manifest.json',manifest)
    verification=verify_portable(work,root,label_manifest,output)
    dump(output/'portable_validation.json',verification)
    return dict(output=output.relative_to(root).as_posix(),portable_manifest_sha256=digest(output/'portable_manifest.json'),
        registry_sha256=manifest['registry_sha256'],source_registry_sha256=manifest['source_registry_sha256'],
        selected_registry_rows=len(sliced),required_unique_addresses=len(required),full_observations_copied=False,validation=verification)

def verify_portable(work, root, label_manifest, output):
    """Independent full-vs-slice collector checks over saved facts; no network."""
    from dune_batch_r1 import BatchSavedDuneProvider, collect_with_labels, read_registry, registry_from_manifest
    from collector_inputs import load_exact_seed
    from collector import Scope
    from cache_probe import InheritedSparseCache, normalized_event
    applied=read(label_manifest);manifest=read(output/'portable_manifest.json')
    slice_path=output/manifest['registry_path'];full_path,_=registry_from_manifest(label_manifest,root)
    full=read_registry(full_path);subset=read_registry(slice_path)
    references,_,_=recompute(output/'reference_replay',subset)
    reference_metrics=metrics(references);expected=applied['reference']['after']
    if reference_metrics!=expected:raise ValueError('Portable finite reference metrics differ from final source version')
    final_reference=rows(label_manifest.parent/'reference_relations_updated.csv.gz')
    byid={r['old_relation_id']:r for r in final_reference}
    if set(byid)!={r['old_relation_id'] for r in references}:raise ValueError('Portable reference relation identities differ')
    fields=('actor','target_identity_class','task_reference_status','change_reason','collection_90d_status','verified_numeric_depth','unknown_intermediate_addresses','witness_id')
    def comparable(v):return stable(v) if isinstance(v,(list,dict)) else '' if v is None else str(v)
    mismatch=[r['old_relation_id'] for r in references if any(comparable(r.get(k))!=comparable(byid[r['old_relation_id']].get(k)) for k in fields)]
    if mismatch:raise ValueError('Portable reference row-by-row differences: '+str(len(mismatch)))
    policy=read(work/'bootstrap/config/STAGE1B_R1_POLICY.json')
    cache_path=work/'private/cache_replay/value_events_normalized.csv.gz';cache_events=[normalized_event(r) for r in rows(cache_path)]
    members=output/'reference_replay/reference_query_members.csv';events=output/'reference_replay/reference_event_slice.csv.gz'
    checks=[]
    for pilot in policy['query_pilots']:
        seed,_=load_exact_seed(pilot,members,events);scope=Scope.from_policy(pilot)
        for mode in ('SAVED_LIVE_PROVIDER','INHERITED_SPARSE_CACHE'):
            def provider():
                return BatchSavedDuneProvider(work/'private/dune_live_jobs',work,work/'private/dune_r1_jobs') if mode=='SAVED_LIVE_PROVIDER' else InheritedSparseCache(cache_events,digest(cache_path))
            source=collect_with_labels(provider(),full,scope,seed)
            portable_result=collect_with_labels(provider(),subset,scope,seed)
            original=source.metrics['candidate_stop_coverage_sha256'];new=portable_result.metrics['candidate_stop_coverage_sha256']
            if original!=new:raise ValueError('Portable collector differs: '+pilot['name']+'/'+mode)
            checks.append(dict(pilot=pilot['name'],mode=mode,full_registry_candidate_stop_coverage_sha256=original,
                portable_candidate_stop_coverage_sha256=new,identical=True,
                candidate_event_count=portable_result.metrics['candidate_event_count'],network_calls=0))
    return dict(passed=True,reference_relation_rows=len(references),reference_row_mismatches=len(mismatch),
        reference_metrics=reference_metrics,collector_checks=checks,network_calls=0,
        actual_provider_calls_by_validation=0,full_observations_not_needed_or_copied=True,
        source_registry_still_matches=digest(full_path)==applied['registry_sha256'])

def reference_delta(work, batchdir, identities, changes, verify_unchanged=False):
    """Audit only finite registered reference relations; never provider neighbors."""
    data=work/'inherited_min/private/reference_replay'
    baseline=rows(work/'baseline_derived/reference_relations_stage1b.csv.gz')
    changed={r['address'] for r in changes if r['identity_or_actor_changed']}
    affected_events={e['event_id'] for e in rows(data/'reference_event_slice.csv.gz')
                     if e['from_address'] in changed or e['to_address'] in changed}
    incident_ids={r['incident_id'] for r in rows(data/'reference_incident_event_membership.csv.gz') if r['event_id'] in affected_events}
    affected_queries={q['query_id'] for q in rows(data/'reference_query_members.csv') if q['incident_id'] in incident_ids}
    affected_queries.update(r['query_id'] for r in baseline if r['target_address'] in changed)
    recalculated_scope=set()
    if verify_unchanged:
        recalculated,witnesses,_=recompute(data,identities)
        changedby={r['old_relation_id']:r for r in recalculated}
        if set(changedby)!={r['old_relation_id'] for r in baseline}:raise ValueError('Reference relation keys changed')
        after=[changedby[r['old_relation_id']] for r in baseline]
        recalculated_scope={r['query_id'] for r in recalculated}
        write_csv(batchdir/'reference_affected_witnesses.csv.gz',witnesses)
    elif affected_queries:
        # The finite replay helper has no query-filter parameter. A private
        # subset contains only affected queries and their unchanged facts.
        subset=batchdir/'private_reference_affected_inputs';subset.mkdir()
        qs=[q for q in rows(data/'reference_query_members.csv') if q['query_id'] in affected_queries]
        inc={q['incident_id'] for q in qs};members=[r for r in rows(data/'reference_incident_event_membership.csv.gz') if r['incident_id'] in inc]
        eventids={r['event_id'] for r in members}|{q['seed_event_id'] for q in qs}
        oldrelations=[r for r in rows(data/'B_reference_relation_recheck.csv.gz') if r['query_id'] in affected_queries]
        oldsource={r['old_source_row'] for r in oldrelations}
        for name, values in [('reference_query_members.csv',qs),('reference_incident_event_membership.csv.gz',members),
            ('reference_event_slice.csv.gz',[r for r in rows(data/'reference_event_slice.csv.gz') if r['event_id'] in eventids]),
            ('B_reference_relation_recheck.csv.gz',oldrelations),
            ('A_original_reference_entries.csv.gz',[r for r in rows(data/'A_original_reference_entries.csv.gz') if r['old_source_row'] in oldsource])]:
            write_csv(subset/name,values)
        recalculated,witnesses,_=recompute(subset,identities)
        changedby={r['old_relation_id']:r for r in recalculated}
        after=[changedby.get(r['old_relation_id'],r) for r in baseline]
        write_csv(batchdir/'reference_affected_witnesses.csv.gz',witnesses)
        recalculated_scope=affected_queries
    else:after=baseline
    beforeby={r['old_relation_id']:r for r in baseline};deltas=[]
    fields=['actor','target_identity_class','task_reference_status','change_reason','collection_90d_status',
        'verified_numeric_depth','unknown_intermediate_addresses','witness_id']
    def norm(v):
        if isinstance(v,(list,dict)):return stable(v)
        return '' if v is None else str(v)
    for r in after:
        old=beforeby[r['old_relation_id']]
        diff={k:dict(before=norm(old.get(k)),after=norm(r.get(k))) for k in fields if norm(old.get(k))!=norm(r.get(k))}
        deltas.append(dict(old_relation_id=r['old_relation_id'],query_id=r['query_id'],target_address=r['target_address'],
            affected_query=r['query_id'] in affected_queries,changed=bool(diff),field_changes=diff,
            old_B=old['task_reference_status']=='VERIFIED_TASK_REFERENCE',new_B=r['task_reference_status']=='VERIFIED_TASK_REFERENCE',
            old_C=old['task_reference_status']=='VERIFIED_TASK_REFERENCE' and old['collection_90d_status']=='WITHIN_PER_ARRIVAL_90D',
            new_C=r['task_reference_status']=='VERIFIED_TASK_REFERENCE' and r['collection_90d_status']=='WITHIN_PER_ARRIVAL_90D'))
    write_csv(batchdir/'reference_relation_delta.csv.gz',deltas)
    write_csv(batchdir/'reference_relations_updated.csv.gz',after,list(baseline[0]))
    report=dict(before=metrics(baseline),after=metrics(after),affected_queries=sorted(affected_queries),
        changed_relations=sum(r['changed'] for r in deltas),row_by_row_relation_count=len(deltas),
        early_entry_rejections_before=sum(r['change_reason']=='ENTRY_BEFORE_EARLIEST_SEED' for r in baseline),
        early_entry_rejections_after=sum(r['change_reason']=='ENTRY_BEFORE_EARLIEST_SEED' for r in after),
        no_target_algorithm_performance_claim=True,denominator_finalized=False,
        unaffected_queries_copied_without_revalidation=not verify_unchanged,
        same_input_full_finite_reference_replay_performed=verify_unchanged,recomputed_query_count=len(recalculated_scope))
    dump(batchdir/'reference_delta_summary.json',report)
    return report

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('freeze','apply','optimize-static','record-failure','portable'))
    parser.add_argument('--work',type=Path,required=True);parser.add_argument('--project-root',type=Path,required=True)
    parser.add_argument('--batch',default='batch_01');parser.add_argument('--collection-root',type=Path)
    parser.add_argument('--base-snapshot',type=Path);parser.add_argument('--job-dir',type=Path)
    parser.add_argument('--job-dirs',type=Path,nargs='+')
    parser.add_argument('--label-manifest',type=Path);parser.add_argument('--output',type=Path)
    a=parser.parse_args()
    if a.action=='freeze':result=freeze(a.work,a.project_root,a.batch,a.collection_root,a.base_snapshot)
    elif a.action=='portable':
        if not a.label_manifest or not a.output:parser.error('portable requires --label-manifest and a new --output directory')
        result=portable(a.work,a.project_root,a.label_manifest,a.output)
    elif a.action=='record-failure':
        if not a.job_dirs:parser.error('record-failure requires --job-dirs for the two or three saved failed jobs')
        result=record_failure(a.work,a.project_root,a.job_dirs)
    elif a.action=='optimize-static':
        if not a.job_dir:parser.error('optimize-static requires failed --job-dir')
        result=optimize_static(a.work,a.project_root,a.job_dir)
    else:
        if not a.job_dir:parser.error('apply requires --job-dir')
        result=apply(a.work,a.project_root,a.batch,a.job_dir)
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
