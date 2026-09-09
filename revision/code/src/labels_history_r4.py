"""Finite offline F06/F07 history remediation; immutable originals stay in place.

This scans materialized label tables and saved Meta responses in PROJECT_ROOT,
not third-party repositories, code, test fixtures or redundant extracted ZIPs.
Only row patches and source identities are emitted; never network requests.
"""
import argparse
from collections import Counter, defaultdict
import csv
import gzip
import hashlib
import json
import os
from pathlib import Path

from labels_import import parse, batch_failure
from labels_policy import KNOWN_ROLES, explicit_claim
from reference_core import actor_key, arr, dump, sid, stable

VERSION = 'stage1b-r4-label-history-remediation-v1'
RETAINED = {'EXISTING_OTHER_SOURCE_RESOLUTION_RETAINED', 'EXISTING_ROLE_RETAINED_NO_NEW_EXPLICIT_OWNERSHIP'}
FIELDS = ('address', 'actor', 'identity_class', 'resolution_rule', 'adopted_observation_ids',
          'adopted_sources', 'adopted_source_versions', 'observation_ids')
SKIP = {'.git', 'node_modules', '__pycache__', '.testtmp', 'tests', 'fixtures', 'src',
        'configs', 'docs', 'bootstrap', 'baseline', 'checks', 'source_snapshots',
        'evidence_snapshots', 'repositories', 'stage0A_recovered'}


def digest(path):
    with Path(path).open('rb') as stream: return hashlib.file_digest(stream, 'sha256').hexdigest()


def candidates(root, excluded):
    for rel in ('02_sources/public_labels/consolidated', '02_sources/public_labels/metasleuth_daily',
                '03_workspaces', '04_deliverables'):
        base = root / rel
        if not base.exists(): continue
        for directory, dirs, files in os.walk(base, followlinks=False):
            dirs[:] = [name for name in dirs if name not in SKIP
                       and not name.startswith(('package_staging', 'final_extracted'))
                       and not (Path(directory)/name).is_symlink()
                       and not (Path(directory)/name).resolve().is_relative_to(excluded)]
            for name in sorted(files):
                path = Path(directory)/name
                if path.is_symlink() or not path.resolve().is_relative_to(root): continue
                low = path.relative_to(root).as_posix().lower()
                if not name.lower().endswith(('.csv', '.csv.gz', '.json', '.json.gz')): continue
                if any(word in name.lower() for word in ('label', 'registry', 'observation', 'resolution', 'metasleuth')) or any(
                    word in low for word in ('/label_snapshots/', '/metasleuth/', '/metasleuth_daily/')):
                    yield path


def objects(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8-sig', newline='') as stream:
        if '.csv' in path.name.lower():
            reader = csv.DictReader(stream)
            for index, row in enumerate(reader, 2): yield str(index), row
        else:
            doc = json.load(stream)
            stack = [('$', doc)]
            while stack:
                locator, value = stack.pop()
                if isinstance(value, dict):
                    yield locator, value
                    stack.extend((locator+'/'+str(key), item) for key, item in value.items() if isinstance(item, (dict, list)))
                elif isinstance(value, list):
                    stack.extend((locator+'/'+str(i), item) for i, item in enumerate(value) if isinstance(item, (dict, list)))


def header(path):
    if '.csv' not in path.name.lower(): return None
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8-sig', newline='') as stream: return next(csv.reader(stream), [])


def normalized_record(row):
    return {key: arr(row.get(key)) if key in ('adopted_observation_ids', 'adopted_sources',
             'adopted_source_versions', 'observation_ids') else row.get(key) for key in FIELDS}


def claim_key(actor):
    value = actor_key(actor)
    return 'bitpay' if value == 'bitpaycom' else value


def repair_record(record, observation_index):
    """Use only actual observation contents already listed by this materialization."""
    adopted = list(record['adopted_observation_ids'])
    actual_ids = set(record['observation_ids']) | set(adopted)
    attribution = record.get('actor') and record.get('identity_class') in KNOWN_ROLES
    support = []
    for oid in sorted(actual_ids):
        obs = observation_index.get(oid)
        if obs and not obs.get('id_conflict') and obs['explicit_claim'] and attribution and (
            obs['address'].lower(), claim_key(obs['actor']), obs['role']) == (
            record['address'].lower(), claim_key(record['actor']), record['identity_class']):
            support.append(obs)
    # Valid adopted support is retained; only a broken/empty pointer requires a
    # replacement from other already-listed historical supporting observations.
    support_by_id = {o['observation_id']: o for o in support}
    chosen = [support_by_id[oid] for oid in adopted if oid in support_by_id] or support
    if not attribution: chosen = []
    ids = [o['observation_id'] for o in chosen]
    after = dict(record, adopted_observation_ids=ids,
                 adopted_sources=sorted({o['platform'] for o in chosen}),
                 adopted_source_versions=sorted({o['source_version'] for o in chosen}),
                 provenance_status='SUPPORTED_BY_EXISTING_OBSERVATIONS' if chosen else
                 'PROVENANCE_UNRESOLVED' if attribution else 'NO_ADOPTED_ATTRIBUTION',
                 provenance_issues=['HISTORICAL_EVIDENCE_GAP'] if attribution and not chosen else [],
                 unadopted_observation_ids=sorted(actual_ids-set(ids)))
    changed = any(record[key] != after[key] for key in ('adopted_observation_ids', 'adopted_sources', 'adopted_source_versions'))
    invalid = [oid for oid in adopted if oid not in {o['observation_id'] for o in support}]
    return after, changed, invalid, chosen


def run(root, output, excluded=None):
    root, output = Path(root).resolve(), Path(output).resolve()
    if not output.is_relative_to(root): raise ValueError('Output outside PROJECT_ROOT')
    if output.exists(): raise FileExistsError('Use a fresh historical revision output')
    excluded = Path(excluded).resolve() if excluded else output
    if not excluded.is_relative_to(root): raise ValueError('Excluded work directory outside project')
    output.mkdir(parents=True)
    inventory = []
    by_hash = defaultdict(list)
    for path in sorted(candidates(root, excluded)):
        item = dict(path=path.relative_to(root).as_posix(), sha256=digest(path), bytes=path.stat().st_size)
        inventory.append(item); by_hash[item['sha256']].append(item)
    dump(output/'SOURCE_SCAN_INVENTORY.json', inventory)
    print(json.dumps({'phase':'inventory','files':len(inventory),'unique_bytes_versions':len(by_hash)}), flush=True)
    records = {}; counts = Counter(); scan_results = []; relevant_addresses = set()
    for index, (filehash, locations) in enumerate(by_hash.items()):
        path = root/locations[0]['path']; fields = header(path)
        stat = dict(source_sha256=filehash, equivalent_paths=[r['path'] for r in locations],
                    rows_examined=0, retained_rows=0, retained_attribution_rows=0)
        if fields is not None and 'resolution_rule' not in fields:
            stat['status']='HEADER_HAS_NO_MATERIALIZED_RESOLUTION'; scan_results.append(stat); continue
        for locator, row in objects(path):
            stat['rows_examined'] += 1
            if row.get('resolution_rule') not in RETAINED or not row.get('address'): continue
            stat['retained_rows'] += 1
            rec=normalized_record(row); key=sid(rec)
            if key not in records: records[key]={'before':rec,'occurrences':[]}
            records[key]['occurrences'].append({'source_sha256':filehash,'locator':locator,'paths':stat['equivalent_paths']})
            relevant_addresses.add(rec['address'].lower())
            stat['retained_attribution_rows'] += bool(rec.get('actor') and rec.get('identity_class') in KNOWN_ROLES)
        stat['status']='SCANNED'; scan_results.append(stat)
        counts['physical_materialized_rows'] += stat['rows_examined']*len(locations)
        counts['physical_retained_rows'] += stat['retained_rows']*len(locations)
        if index%25==0: print(json.dumps({'phase':'materializations','files_done':index+1,'unique_retained_rows':len(records)}), flush=True)
    print(json.dumps({'phase':'observations','retained_variants':len(records),'addresses':len(relevant_addresses)}), flush=True)
    observations = {}; observation_sources = []
    for filehash, locations in by_hash.items():
        path = root/locations[0]['path']; fields = header(path)
        if fields is not None and not {'observation_id','address','platform','role','actor'}<=set(fields): continue
        source_count=0
        for locator, row in objects(path):
            if not {'observation_id','address','platform','role','actor'}<=set(row):continue
            if str(row['address']).lower() not in relevant_addresses:continue
            source_count+=1
            obs={key:row.get(key) or '' for key in ('observation_id','address','actor','role','platform','source_version')}
            obs['explicit_claim']=explicit_claim(row)
            oid=obs['observation_id'];binding={'source_sha256':filehash,'path':locations[0]['path'],'locator':locator,'record_sha256':sid(row)}
            if oid in observations:
                old=observations[oid]
                if any(old.get(key)!=value for key,value in obs.items()):old['id_conflict']=True
            else:observations[oid]=dict(obs,bindings=[])
            observations[oid]['bindings'].append(binding)
        if source_count:observation_sources.append({'source_sha256':filehash,'matching_address_observations':source_count})
    corrected=[];unresolved=[];affected_support={}; affected_addresses=set();valid=0
    for key, item in records.items():
        after, changed, invalid, chosen=repair_record(item['before'], observations)
        if not changed and after['provenance_status']!='PROVENANCE_UNRESOLVED':valid+=1;continue
        result=dict(materialized_record_id=key,before=item['before'],after=after,
                    original_occurrences=item['occurrences'],pointer_fields_changed=changed,
                    incompatible_or_unavailable_adopted_ids=invalid,actor_changed=False,role_changed=False,
                    service_stop_changed=False,existing_observation_support_ids=[o['observation_id'] for o in chosen])
        corrected.append(result);affected_addresses.add(after['address'])
        if after['provenance_status']=='PROVENANCE_UNRESOLVED':unresolved.append(key)
        for oid in set(item['before']['adopted_observation_ids'])|set(after['adopted_observation_ids']):
            if oid in observations:affected_support[oid]=observations[oid]
    with gzip.open(output/'F06_CORRECTED_RETAINED_ROWS.jsonl.gz','wt',encoding='utf-8',newline='\n') as stream:
        for row in corrected:stream.write(stable(row)+'\n')
    dump(output/'F06_SUPPORTING_OBSERVATION_INDEX.json',list(affected_support.values()))
    dump(output/'F06_UNRESOLVED_RECORD_IDS.json',unresolved)
    dump(output/'MATERIALIZATION_SCAN_RESULTS.json',scan_results)
    dump(output/'OBSERVATION_SCAN_RESULTS.json',observation_sources)
    summary=dict(schema_version=VERSION,inventory_files=len(inventory),unique_file_versions=len(by_hash),
                 scanned_compressed_bytes=sum(i['bytes'] for i in inventory),**counts,
                 unique_retained_variants=len(records),unique_retained_addresses=len(relevant_addresses),
                 unique_observation_contents=len(observations),valid_existing_variants=valid,
                 corrected_or_unresolved_variants=len(corrected),affected_addresses=len(affected_addresses),
                 pointer_changed_variants=sum(x['pointer_fields_changed'] for x in corrected),
                 historical_evidence_gap_variants=len(unresolved),actor_changes=0,role_changes=0,
                 service_stop_changes=0,new_provider_calls=0,original_files_unchanged=all(digest(root/i['path'])==i['sha256'] for i in inventory),
                 scan_scope='Existing primary label materializations/observations in declared project roots; duplicate file bytes inspected once with all paths retained',
                 exclusions=sorted(SKIP)+['package_staging*','final_extracted*',str(excluded.relative_to(root))],
                 external_acceptance_status='PENDING_REVIEW')
    dump(output/'F06_HISTORY_SUMMARY.json',summary)
    print(json.dumps(summary),flush=True)
    return summary


def run_meta(root, inventory_path, output):
    """Reclassify every saved Meta batch and inspect hash-bound derived outcomes."""
    root, output = Path(root).resolve(), Path(output).resolve()
    inventory_path = Path(inventory_path).resolve()
    if not all(p.is_relative_to(root) for p in (output, inventory_path)): raise ValueError('Path outside project')
    if output.exists(): raise FileExistsError('Use a fresh Meta historical output')
    inventory = json.loads(inventory_path.read_text(encoding='utf-8'))
    documents = {}; outcomes = []; input_records = {}; records_by_hash = defaultdict(list)
    for item in inventory:
        path=root/item['path']
        if '.json' not in path.name.lower() or 'meta' not in item['path'].lower():continue
        if digest(path)!=item['sha256']:raise ValueError('Frozen history input changed')
        documents[item['path']]=json.loads(path.read_text(encoding='utf-8-sig'))
        input_records[item['path']]=item
    contexts=defaultdict(list); non_label_response_hashes=set()
    for rel,doc in documents.items():
        if not isinstance(doc,dict):continue
        chain=doc.get('chain_list')
        if isinstance(chain,dict) and str(chain.get('endpoint','')).endswith('/chain-list'):
            if chain.get('raw_response_sha256'):non_label_response_hashes.add(chain['raw_response_sha256'])
        batch=doc.get('batch')
        if isinstance(batch,dict) and batch.get('raw_response_sha256') and doc.get('frozen_sample_path'):
            roster=root/str(doc['frozen_sample_path']).replace('\\','/')
            if not roster.resolve().is_relative_to(root) or digest(roster)!=doc.get('frozen_sample_sha256'):
                raise ValueError('Historical frozen sample binding differs')
            with roster.open(encoding='utf-8-sig',newline='') as stream:expected=[row['address'] for row in csv.DictReader(stream)]
            contexts[batch['raw_response_sha256']].append({'metadata':{k:batch.get(k) for k in ('http_status','transport_error','error_class','request_id') if k in batch},
                'source_path':rel,'source_sha256':input_records[rel]['sha256'],'requested_addresses':expected,
                'frozen_roster':{'path':roster.relative_to(root).as_posix(),'sha256':digest(roster),'locator':'CSV/address'}})
        raw_hash=doc.get('raw_response_sha256') or (doc.get('sha256') if doc.get('provider')=='MetaSleuth' or 'request_metasleuth' in rel.lower() else None)
        if not raw_hash:continue
        entry={'metadata':{k:doc.get(k) for k in ('http_status','transport_error','error_class','queried_at','utc','request_id') if k in doc},'source_path':rel,'source_sha256':input_records[rel]['sha256'],'requested_addresses':None,'frozen_roster':None}
        request=doc.get('payload')
        if isinstance(request,dict) and isinstance(request.get('addresses'),list):
            entry['requested_addresses']=request['addresses'];entry['frozen_roster']={'path':rel,'sha256':input_records[rel]['sha256'],'locator':'$/payload/addresses'}
        roster=(root/rel).parent/'request_addresses.json'
        if roster.exists():
            roster_doc=json.loads(roster.read_text(encoding='utf-8-sig'))
            expected=roster_doc.get('addresses') if isinstance(roster_doc,dict) else roster_doc
            if isinstance(expected,list):entry['requested_addresses']=expected;entry['frozen_roster']={'path':roster.relative_to(root).as_posix(),'sha256':digest(roster),'locator':'$/addresses' if isinstance(roster_doc,dict) else '$'}
        contexts[raw_hash].append(entry)
    # Every actual provider-shaped raw response is identified by its bytes;
    # repeated materialized copies do not pretend to be additional calls.
    for rel,doc in documents.items():
        if not isinstance(doc,dict) or 'code' not in doc:continue
        filehash=input_records[rel]['sha256']
        if filehash in non_label_response_hashes:continue
        records_by_hash[filehash].append((rel,doc))
    repairs=[];summaries=[];failed_hashes=set();all_response_ids=set()
    for raw_hash, copies in records_by_hash.items():
        rel,payload=copies[0];all_response_ids.add(raw_hash)
        bound=contexts.get(raw_hash,[])
        rosters=[entry for entry in bound if entry['requested_addresses'] is not None]
        if rosters and any(entry['requested_addresses']!=rosters[0]['requested_addresses'] for entry in rosters):
            raise ValueError('Conflicting frozen rosters for one saved response')
        chosen=rosters[0] if rosters else bound[0] if bound else {'metadata':{},'requested_addresses':None,'frozen_roster':None}
        obs,result=parse(payload,chosen['metadata'],rel,raw_hash,requested_addresses=chosen['requested_addresses'])
        failed=any(item['request_status']=='FAILED' for item in result)
        if failed:failed_hashes.add(raw_hash)
        summaries.append(dict(response_sha256=raw_hash,immutable_copies=[p for p,_ in copies],
            provider_code=payload.get('code'),request_id=payload.get('request_id'),
            outcome_status='FAILED' if failed else 'SUCCESS',frozen_roster=chosen['frozen_roster'],
            original_metadata_bindings=[{k:v for k,v in entry.items() if k!='metadata' and k!='requested_addresses'} for entry in bound],
            addresses_requested=len(chosen['requested_addresses']) if chosen['requested_addresses'] is not None else None,
            normalized_observations=len(obs),address_outcomes=sum(x['scope']=='ADDRESS' for x in result),
            successful_empty_addresses=sum(x['scope']=='ADDRESS' and x['outcome_status']=='SUCCESS_EMPTY' for x in result),
            provenance_status='REQUEST_ROSTER_BOUND' if chosen['frozen_roster'] else 'HISTORICAL_REQUEST_ROSTER_UNAVAILABLE'))
        for item in result:outcomes.append(dict(item,raw_response_path=rel,derived_in_r4=True))
    scanned=0;matched=0
    for item in inventory:
        path=root/item['path'];fields=header(path)
        if fields is None or not {'raw_response_sha256','raw_sha256','raw_evidence_sha256'}&set(fields):continue
        if not {'request_status','api_response_status','response_status'}&set(fields):continue
        if digest(path)!=item['sha256']:raise ValueError('Frozen outcome file changed')
        for locator,row in objects(path):
            scanned+=1;raw_hash=row.get('raw_response_sha256') or row.get('raw_sha256') or row.get('raw_evidence_sha256')
            if raw_hash not in all_response_ids:continue
            matched+=1
            status=row.get('request_status') or row.get('api_response_status') or row.get('response_status')
            if raw_hash in failed_hashes and str(status).upper().startswith('SUCCESS'):
                repairs.append({'source_path':item['path'],'source_sha256':item['sha256'],'locator':locator,'before':row,
                    'after':dict(row,request_status='FAILED',outcome_status='FAILED_HISTORICAL_PROVIDER_RESPONSE'),
                    'original_response_sha256':raw_hash,'original_rows_unchanged':True})
    output.mkdir(parents=True)
    dump(output/'F07_RECLASSIFIED_BATCHES.json',summaries);dump(output/'F07_RECLASSIFIED_OUTCOMES.json',outcomes)
    dump(output/'F07_MISLEADING_OUTCOME_REPAIRS.json',repairs)
    summary=dict(schema_version=VERSION,inventory_files=len(inventory),meta_json_files=len(documents),
                 unique_saved_response_versions=len(records_by_hash),failed_saved_batches=len(failed_hashes),
                 excluded_verified_non_label_capability_responses=len(non_label_response_hashes),
                 batch_failure_with_missing_roster=sum(x['outcome_status']=='FAILED' and not x['frozen_roster'] for x in summaries),
                 known_success_batches=sum(x['outcome_status']=='SUCCESS' for x in summaries),
                 existing_outcome_rows_scanned=scanned,existing_hash_bound_outcome_rows=matched,
                 false_success_materializations=len(repairs),corrected_derived_outcomes=len(outcomes),
                 original_success_records_overwritten=0,new_meta_requests=0,
                 empty_output_omission_limit='A failed response is always reified as a batch failure even if no old row survived; no address is guessed without an actual frozen request roster.',
                 original_input_hashes_preserved=all(digest(root/x['path'])==x['sha256'] for x in input_records.values()),
                 external_acceptance_status='PENDING_REVIEW')
    dump(output/'F07_HISTORY_SUMMARY.json',summary)
    print(json.dumps(summary),flush=True);return summary


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-root',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--exclude-work',type=Path)
    parser.add_argument('--meta-inventory',type=Path)
    args=parser.parse_args()
    if args.meta_inventory:run_meta(args.project_root,args.meta_inventory,args.output)
    else:run(args.project_root,args.output,args.exclude_work)


if __name__=='__main__':main()
