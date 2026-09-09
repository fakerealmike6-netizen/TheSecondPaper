"""One continued distinct-chain-address label pool, with explicit600 authority.

Read-only accounting: cohorts reserve opportunities; actual submitted executions
or complete four-table results confirm use. No counters, fees or clocks reset.
"""
import hashlib
import json
from pathlib import Path
from context_access_r3 import read, sha
from stage1d_native_candidate import inside

AUTH = 'LABEL_OPPORTUNITY_AMENDMENT_600_V1'
SCOPE_AUTH = 'STAGE1D_BATCH01_REFERENCE_FULL_V1'
AUTH_PATH = 'private/stage1d_authority/LABEL_OPPORTUNITY_AMENDMENT_600_V1.json'


def label_budget(work):
    from stage1d_recovery_policy import effective, AUTH as RECOVERY_AUTH, POLICY_PATH, POLICY_SHA
    work = Path(work).resolve()
    recovery = effective(work)
    if recovery:
        return {'cap': recovery['labels']['batch_distinct_chain_address_cap'], 'authorization_id': RECOVERY_AUTH,
                'authority_ref': {'path': POLICY_PATH, 'sha256': POLICY_SHA}, 'legacy': False}
    policy = work / 'private/STAGE1D_EFFECTIVE_POLICY.json' 
    labels = read(policy).get('labels', {}) if policy.exists() else {}
    cap = labels.get('max_new_external_distinct_addresses_this_batch', 100)
    binding = labels.get('opportunity_amendment')
    if type(cap) is not int: raise ValueError('Exact integer label opportunity cap required')
    if cap == 100 and not binding:
        return {'cap': 100, 'authorization_id': SCOPE_AUTH, 'authority_ref': None, 'legacy': True}
    if cap != 600 or not isinstance(binding, dict) or binding.get('authorization_id') != AUTH or binding.get('path') != AUTH_PATH:
        raise ValueError('Label expansion requires the explicit bound600 amendment')
    path = inside(work, binding['path'])
    if sha(path) != binding.get('sha256'): raise ValueError('Label opportunity authority SHA differs')
    authority = read(path)
    required = {'schema_version': 'stage1d-label-opportunity-amendment-v1', 'authorization_id': AUTH,
        'status': 'USER_CONFIRMED', 'source_kind': 'USER_MESSAGE', 'previous_cap': 100,
        'additional_opportunities': 500, 'total_cap': 600, 'same_batch_distinct_chain_address_pool': True,
        'scope_authorization_id': SCOPE_AUTH, 'other_resource_caps_unchanged': True}
    if any(authority.get(k) != v or type(authority.get(k)) is not type(v) for k, v in required.items()):
        raise ValueError('Exact continued100+500 label authority required')
    if not isinstance(authority.get('source_text'), str) or not authority['source_text'].strip():
        raise ValueError('Original explicit user-message evidence required')
    frozen = work / 'private/BATCH_QUERY_FREEZE.json'; batch = read(frozen)
    if (authority.get('batch_freeze_sha256') != sha(frozen) or batch.get('authorization_id') != SCOPE_AUTH
            or len(batch.get('queries', [])) != 4 or len({q['query_id'] for q in batch['queries']}) != 4):
        raise ValueError('Label amendment must bind the same four-query batch')
    return {'cap': 600, 'authorization_id': AUTH,
            'authority_ref': {'path': AUTH_PATH, 'sha256': sha(path)}, 'legacy': False}


def address_key(value):
    text = str(value)
    return text.lower() if text.lower().startswith('0x') else text


def cohort_usage(work, cohorts=None):
    work = Path(work).resolve()
    cohorts = cohorts if cohorts is not None else [(p, read(p)) for p in (work / 'private/stage1d_label_cohorts').glob('*.json')]
    batch = read(work / 'private/BATCH_QUERY_FREEZE.json')
    owners = {q['query_id']: q for q in batch.get('queries', [])}
    allocated = set(); used = set(); by_cohort = {}; incomplete = {}
    for path, record in cohorts:
        expected = hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()
        if path.stem != expected: raise ValueError('Immutable label cohort identity changed')
        owner = owners.get(record['query_id'], {})
        chain = str(record.get('chain_id') or owner.get('seed_event', {}).get('chain_id') or 'eip155:1')
        keys = {(chain, address_key(a)) for a in record['addresses']}; allocated.update(keys)
        complete = work / 'derived/stage1d/labels' / path.name
        if complete.exists():
            rows = read(complete).get('rows', [])
            if (len(rows) != len(record['addresses']) or {address_key(r['address']) for r in rows} != {address_key(a) for a in record['addresses']}
                    or any(r.get('lookup_status') != 'COMPLETED_FOUR_TABLE_OPPORTUNITY' for r in rows)):
                raise ValueError('Completed label opportunity result is inconsistent')
            used.update(keys); by_cohort[path.stem] = {'status': 'COMPLETED', 'confirmed_used': True}
        else:
            incomplete[path.relative_to(work).as_posix()] = (path.stem, keys)
            by_cohort[path.stem] = {'status': 'ALLOCATED_NOT_CONFIRMED_SUBMITTED', 'confirmed_used': False}
    if incomplete:
        for frozen_path in (work / 'private/stage1d_sql').glob('*/freeze_manifest.json'):
            frozen = read(frozen_path)
            if frozen.get('kind') != 'frontier_labels': continue
            for dep in frozen.get('dependencies', []):
                match = incomplete.get(dep.get('path'))
                if match is None: continue
                if sha(inside(work, dep['path'])) != dep.get('sha256'): raise ValueError('Cohort SQL dependency changed')
                state_path = work / 'private/dune_r2_jobs' / frozen['sql_sha256'] / 'job.json'
                if not state_path.exists(): continue
                state = read(state_path)
                if state.get('execution_id'):
                    used.update(match[1]); by_cohort[match[0]] = {'status': 'SUBMITTED', 'confirmed_used': True}
                else:
                    by_cohort[match[0]] = {'status': 'SUBMISSION_UNKNOWN_RESERVED', 'confirmed_used': False}
    chains = sorted({chain for chain, _ in allocated})
    return {'allocated_distinct': len(allocated), 'confirmed_used_distinct': len(used),
            'reserved_not_confirmed_distinct': len(allocated - used),
            'allocated_addresses_by_chain': {c: sorted(a for chain, a in allocated if chain == c) for c in chains},
            'cohorts': by_cohort, 'accounting_basis': 'One batch-wide distinct chain-address union; allocation is reserved, not a claim of actual external use.'}


def snapshot(work, cohorts=None):
    quota = label_budget(work); usage = cohort_usage(work, cohorts)
    return dict(quota, **usage, remaining=max(0, quota['cap'] - usage['allocated_distinct']),
                exceeded=usage['allocated_distinct'] > quota['cap'], new_pool_created=False)
