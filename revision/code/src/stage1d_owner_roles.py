"""Exact owner metadata enrichment of already observed Stage1D addresses.

Raw address observations and the frozen role classifier remain authoritative.
This adapter does not infer a role from a name or spend another address slot.
"""
from collections import defaultdict
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha
import re

from context_access_r3 import read, sha, now
from page_attempts import atomic_json


def effective_rules(work):
    work = Path(work).resolve()
    original = work / 'private/stage1d_inputs/source_rules.json'
    pointer = work / 'private/stage1d_inputs/OWNER_RULES_CURRENT.json'
    if not pointer.exists():
        return read(original)
    record = read(pointer)
    path = (work / record['path']).resolve()
    if (not path.is_relative_to(work) or path.is_symlink()
            or sha(path) != record['sha256']
            or sha(original) != record['original_rules_sha256']):
        raise ValueError('Owner metadata rules provenance changed')
    return read(path)


def saved_observations(work):
    from stage1d_acquisition import rows
    work = Path(work)
    baseline = work / 'private/stage1d_inputs/label_observations.csv.gz'
    observations = rows(baseline) if baseline.exists() else []
    for path in sorted((work / 'derived/stage1d/labels').glob('*.json')):
        observations.extend(read(path).get('observations', []))
    unique = {}
    for observation in observations:
        item = deepcopy(observation)
        if isinstance(item.get('source_metadata'), str):
            item['source_metadata'] = json.loads(item['source_metadata'])
        oid = item['observation_id']
        if oid in unique and any(unique[oid].get(k) != item.get(k)
                                 for k in ('address', 'role', 'actor', 'source_version')):
            raise ValueError('Conflicting historical label observation identity')
        unique[oid] = item
    return list(unique.values())


def missing_owners(observations, rules):
    missing = defaultdict(set)
    for observation in observations:
        meta = observation.get('source_metadata') or {}
        if isinstance(meta, str):
            meta = json.loads(meta)
        raw = meta.get('raw_source_record') or {}
        key = (raw.get('owner_key') or '').lower()
        if meta.get('table') == 'labels.owner_addresses' and key and not rules['owner_details_cache'].get(key):
            if not re.fullmatch(r'[a-z0-9_\-]{1,100}', key):
                raise ValueError('Unexpected owner key syntax')
            missing[key].add(observation['address'])
    return {key: sorted(addresses) for key, addresses in sorted(missing.items())}


def owner_sql(keys, schema):
    keys = sorted(set(keys))
    if not keys or len(keys) > 100 or any(not re.fullmatch(r'[a-z0-9_\-]{1,100}', k) for k in keys):
        raise ValueError('Exact finite observed owner keys required')
    columns = {r['column_name']: r['data_type'] for r in schema
               if r['table_schema'] == 'labels' and r['table_name'] == 'owner_details'}
    wanted = ('owner_key', 'name', 'primary_category', 'category_tags', 'website',
              'project_documentation', 'project_github_url', 'description')
    if not set(wanted).issubset(columns) or columns['category_tags'] != 'array(varchar)':
        raise ValueError('Verified owner_details schema required')
    values = ', '.join("('" + key + "')" for key in keys)
    projection = ', '.join('d.' + column for column in wanted if column not in ('owner_key', 'category_tags'))
    return ("-- Stage1D recovery: missing owner metadata for existing observations only\n"
            "WITH needed(owner_key) AS (VALUES " + values + ")\n"
            "SELECT n.owner_key, (d.owner_key IS NOT NULL) AS matched, " + projection
            + ", json_format(CAST(d.category_tags AS JSON)) AS category_tags\n"
            "FROM needed n LEFT JOIN labels.owner_details d ON lower(d.owner_key)=n.owner_key\n"
            "ORDER BY n.owner_key")


def validate_details(rows, keys):
    keys = set(keys)
    seen = set()
    details = {}
    for row in rows:
        key = row.get('owner_key')
        if key not in keys or key in seen or type(row.get('matched')) is not bool:
            raise ValueError('Owner result identity, multiplicity or matched flag invalid')
        seen.add(key)
        if row['matched']:
            if not isinstance(row.get('primary_category'), (str, type(None))):
                raise ValueError('Invalid category')
            item = deepcopy(row)
            tags = item.get('category_tags')
            if tags is not None:
                tags = json.loads(tags) if isinstance(tags, str) else tags
                if not isinstance(tags, list) or any(not isinstance(v, str) for v in tags):
                    raise ValueError('Invalid category tags')
                item['category_tags'] = tags
            details[key] = item
    if seen != keys:
        raise ValueError('Missing owner rows cannot be treated as complete empty metadata')
    return details


def reclassify(observations, rules, details, evidence):
    from frontier_labels_r1 import classify
    updated_rules = deepcopy(rules)
    for key, detail in details.items():
        if key in updated_rules['owner_details_cache'] and updated_rules['owner_details_cache'][key] != detail:
            raise ValueError('This enrichment only fills missing owner categories')
        updated_rules['owner_details_cache'][key] = deepcopy(detail)
    added = []
    for original in observations:
        meta = original.get('source_metadata') or {}
        if isinstance(meta, str):
            meta = json.loads(meta)
        raw = meta.get('raw_source_record') or {}
        key = (raw.get('owner_key') or '').lower()
        if meta.get('table') != 'labels.owner_addresses' or key not in details:
            continue
        fields = classify('labels.owner_addresses', raw, updated_rules)
        observation = deepcopy(original)
        observation['source_metadata'] = deepcopy(meta)
        observation['source_metadata'].update(
            classification_rule=fields.pop('classification_rule'),
            local_owner_details=fields.pop('owner_details'),
            owner_metadata_missing=fields.pop('owner_metadata_missing'),
            owner_metadata_evidence=deepcopy(evidence),
            prior_observation_id=original['observation_id'])
        observation.update(fields)
        observation['observation_id'] = 'obs:' + hashlib.sha256(json.dumps(
            [original['observation_id'], evidence, observation['role'], observation['actor']],
            sort_keys=True).encode()).hexdigest()
        observation['collection_scope'] = 'STAGE1D_OBSERVED_OWNER_METADATA_ENRICHMENT'
        observation['semantic_label_key'] = hashlib.sha256(json.dumps(
            [observation['address'], observation['actor_key'], observation['role'], observation['raw_label']],
            sort_keys=True).encode()).hexdigest()
        added.append(observation)
    return updated_rules, added


def enrich(work):
    from stage1d_acquisition import Labels, save_sql
    from stage1d_runtime import execute_sql, result_rows
    from labels_policy import resolve_address
    work = Path(work).resolve()
    rules = effective_rules(work)
    observations = saved_observations(work)
    missing = missing_owners(observations, rules)
    if not missing:
        return {'status': 'NO_MISSING_OWNER_METADATA', 'new_address_slots': 0}
    folder = work / 'private/stage1d_owner_enrichment'
    folder.mkdir(exist_ok=True)
    request = {'missing_owner_addresses': missing, 'rules_sha256': hashlib.sha256(
        json.dumps(rules, sort_keys=True).encode()).hexdigest(), 'new_address_slots': 0}
    digest = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
    request_path = folder / (digest + '.request.json')
    if request_path.exists():
        if read(request_path) != request:
            raise ValueError('Owner request changed')
    else:
        atomic_json(request_path, request)
    query = next(q for q in active_batch(work)['queries']
                 if q['name'] == 'txphish_src001')
    addresses = sorted({a for group in missing.values() for a in group})
    freeze = save_sql(work, owner_sql(missing, rules['table_schema']), 'frontier_labels', query, [],
                      [{'path': request_path.relative_to(work).as_posix(), 'sha256': sha(request_path)}], addresses)
    outcome = execute_sql(work, freeze, 'SHARED', 'recovery_missing_owner_details')
    if outcome.get('status') != 'COMPLETED_EXPORTED':
        return {**outcome, 'missing_owners': missing, 'new_address_slots': 0}
    rawrows = result_rows(work, outcome['job_folder'])
    details = validate_details(rawrows, missing)
    evidence = {'sql_freeze': freeze.relative_to(work).as_posix(), 'sql_freeze_sha256': sha(freeze),
                'job_folder': Path(outcome['job_folder']).relative_to(work).as_posix(),
                'execution_id': read(Path(outcome['job_folder']) / 'job.json')['execution_id']}
    updated_rules, added = reclassify(observations, rules, details, evidence)
    rules_path = folder / (digest + '.rules.json')
    atomic_json(rules_path, updated_rules)
    current = Labels(work)
    grouped = defaultdict(list)
    for observation in observations + added:
        grouped[observation['address']].append(observation)
    resolved = []
    for address in addresses:
        prior = [o for o in observations if o['address'] == address]
        row = resolve_address(grouped[address], current.registry.get(address, {'address': address, 'identity_class': 'UNKNOWN'}),
                              baseline_observations=prior)
        row.update(lookup_status='COMPLETED_OWNER_METADATA_ENRICHMENT',
                   acquisition_scope='STAGE1D_EXISTING_OBSERVATION_OWNER_METADATA',
                   label_snapshot_version='owner_roles_' + digest)
        resolved.append(row)
    result = {'status': 'OWNER_METADATA_APPLIED', 'rows': resolved, 'observations': added,
              'metadata_evidence': evidence, 'source_rules_sha256': sha(rules_path),
              'requested_owners': missing, 'matched_owners': sorted(details), 'new_address_slots': 0,
              'old_observations_preserved': True, 'applied_at_utc': now()}
    atomic_json(work / 'derived/stage1d/labels' / ('zz_recovery_owner_roles_' + digest + '.json'), result)
    atomic_json(work / 'private/stage1d_inputs/OWNER_RULES_CURRENT.json', {
        'path': rules_path.relative_to(work).as_posix(), 'sha256': sha(rules_path),
        'original_rules_sha256': sha(work / 'private/stage1d_inputs/source_rules.json')})
    return result
