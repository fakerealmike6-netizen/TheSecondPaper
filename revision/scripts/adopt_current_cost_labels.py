"""Admit saved four-table label successes to the actual finite identity reader.

Preparation writes only a new report folder. Apply is an explicit safe-point
append to the live catalogue; historical code/failed checks are not replaced.
"""
from pathlib import Path
import argparse, copy, json, socket, sys

R = Path(__file__).resolve().parents[1]
C = R / 'code'
sys.path[:0] = [str(C / 'src'), str(R / 'scripts')]
from context_access_r3 import read, sha, now
from page_attempts import atomic_json
from stage1d_unknown_cost_registry import Registry, CHECK_SCHEMA, CURRENT_PATH
from stage1d_current_code_preparation import validate
from prepare_current_weth import safe_point
from offline_operation import offline_operation


def blocked(*args, **kwargs):
    raise RuntimeError('Saved label adoption has no network access')


socket.socket.connect = blocked
socket.create_connection = blocked


def ref(p):
    p = p.resolve()
    if not p.is_relative_to(R):
        raise ValueError('Reference outside current revision')
    import os
    return dict(path=Path(os.path.relpath(p, C)).as_posix(), sha256=sha(p))


def narrow_reader():
    r = Registry.__new__(Registry)
    r.work = C.resolve(); r.root = R.resolve()
    r._bytes = {}; r._json = {}; r._dune = {}
    return r


def immutable(p, value):
    if p.exists():
        if read(p) != value:
            raise ValueError('Immutable saved-label evidence conflict')
    else:
        atomic_json(p, value)
    return ref(p)


parser = argparse.ArgumentParser()
parser.add_argument('--preparation')
parser.add_argument('--sha256', required=True)
parser.add_argument('--output')
parser.add_argument('--apply-manifest')
args = parser.parse_args()
reader = narrow_reader()
current_path = C / CURRENT_PATH

if args.apply_manifest:
    safe_point(C)
    manifest_path = (R / args.apply_manifest).resolve()
    if not manifest_path.is_relative_to(R / 'reports') or sha(manifest_path) != args.sha256:
        raise ValueError('Exact confined prepared manifest required')
    manifest = read(manifest_path)
    for path, expected in manifest['immutable_application_bindings'].items():
        if sha(R / path) != expected:
            raise ValueError('Label adoption binding changed: ' + path)
    with offline_operation(C, 'admit_current_saved_online_label_identity'):
        current = read(current_path); old_sha = sha(current_path)
        new = copy.deepcopy(current)
        appended = []
        for r in manifest['descriptors']:
            descriptor = reader.read(r)
            if reader._online(r, descriptor)['status'] != 'ONLINE_CHECKED_NO_ADOPTABLE_ROLE':
                raise ValueError('Saved full-page check did not pass')
            if r not in new['identity_checks']:
                existing = [x for x in new['identity_checks'] if
                            reader.read(x).get('channel') == 'online' and
                            reader.read(x).get('address') == descriptor['address']]
                if existing:
                    raise ValueError('Another online check appeared; preserve it for explicit reconciliation')
                new['identity_checks'].append(r); appended.append(r)
        if sha(current_path) != old_sha:
            raise ValueError('Concurrent catalogue writer')
        if new != current:
            backup = C / 'private/stage1d_roles/catalogues' / (old_sha + '.json')
            backup.parent.mkdir(parents=True, exist_ok=True)
            if backup.exists() and sha(backup) != old_sha:
                raise ValueError('Prior catalogue archive differs')
            if not backup.exists(): backup.write_bytes(current_path.read_bytes())
            atomic_json(current_path, new)
        receipt = dict(status='SAVED_LABEL_IDENTITIES_ADOPTED_REPLAY_REQUIRED', utc=now(),
            manifest=ref(manifest_path), previous_catalog_sha256=old_sha,
            current_catalog_sha256=sha(current_path), appended=len(appended),
            descriptors=appended, checked_unknown_or_stop_inferred=False,
            new_requests=0, budget_or_attempt_or_clock_reset=False)
        stamp = receipt['utc'].replace(':', '').replace('+', '_')
        out = manifest_path.parent / ('ADOPTION_' + sha(current_path) + '_' + stamp + '.json')
        immutable(out, receipt)
    print(json.dumps(dict(status=receipt['status'], appended=len(appended), receipt=ref(out))))
else:
    if not args.preparation or not args.output:
        raise ValueError('Preparation reference and new report output required')
    prep, _, _, groups = validate(C, dict(path=args.preparation, sha256=args.sha256))
    out = (R / args.output).resolve()
    if not out.is_relative_to(R / 'reports') or out.exists():
        raise ValueError('New confined report directory required')
    out.mkdir(parents=True)
    current = read(current_path)
    old_addresses = {reader.read(r)['address'] for r in current['identity_checks']
                     if reader.read(r).get('channel') == 'online'}
    selected = {g['group']['address']: g['group']['label_status'] for g in groups.values()}
    descriptors = []; mapping = []; gaps = []
    for address, label in selected.items():
        if address in old_addresses: continue
        candidates = []; errors = []
        for evidence in label.get('evidence', []):
            if evidence.get('basis') != 'EXPLICIT_FOUR_TABLE_SUCCESS_OPPORTUNITIES': continue
            source_ref = evidence['source_ref']; path = (R / source_ref['path']).resolve()
            if not path.is_relative_to(C) or sha(path) != source_ref['sha256']:
                raise ValueError('Prepared label source differs')
            labels = read(path)
            for observation in labels.get('observations', []):
                if observation.get('address') != address or observation.get('request_or_execution_id') != evidence['execution_id']:
                    continue
                fref = dict(path=observation['raw_evidence_ref'], sha256=observation['raw_evidence_sha256'])
                if fref not in [x[0] for x in candidates]: candidates.append((fref, evidence['execution_id'], path))
        for fref, execution, path in candidates:
            try:
                freeze = reader.read(fref)
                jp = C / 'private/dune_r2_jobs' / freeze['sql_sha256'] / 'job.json'
                job = read(jp)
                if job['execution_id'] != execution: raise ValueError('Saved execution differs')
                descriptor = dict(schema_version=CHECK_SCHEMA, chain_id='eip155:1', address=address,
                    channel='online', adapter='DUNE_FOUR_TABLE_R4', freeze_ref=fref, job_ref=ref(jp),
                    source_resolution_ref=ref(path), historical_role_or_checked_unknown_claimed=False)
                dp = out / 'descriptors' / (address + '_' + freeze['sql_sha256'] + '.json')
                dref = immutable(dp, descriptor)
                result = reader._online(dref, descriptor)
                if result['status'] != 'ONLINE_CHECKED_NO_ADOPTABLE_ROLE': raise ValueError('Online reader did not accept')
                descriptors.append(dref)
                mapping.append(dict(address=address, descriptor=dref, execution_id=execution,
                                    verified_source_refs=result['evidence_refs']))
                break
            except (ValueError, KeyError, FileNotFoundError) as exc:
                errors.append(type(exc).__name__ + ': ' + str(exc))
        else:
            gaps.append(dict(address=address, reason='SAVED_ONLINE_PROOF_NOT_ADMITTED', errors=errors))
    bindings = {p:h for p,h in prep['source_and_current_input_sha256'].items()
                if p != 'code/private/stage1d_roles/UNKNOWN_COST_CURRENT.json'}
    # Code descriptors may be appended at the preceding safe point. The query,
    # source, roles, labels, original initial screen and policy remain bound.
    manifest = dict(status='PREPARED_SAVED_LABEL_IDENTITY_ADMISSION', utc=now(),
        code_preparation=dict(path=args.preparation, sha256=args.sha256),
        immutable_application_bindings=bindings, previous_catalog_sha256=sha(current_path),
        existing_online_addresses_reused=len(set(selected) & old_addresses),
        descriptors=descriptors, gaps=gaps, mapping=mapping, verified_cohorts=len(reader._dune),
        production_catalogue_changed=False, new_external_requests=0,
        checked_unknown_or_stop_inferred=False, source_script=ref(Path(__file__)))
    mr = immutable(out / 'MANIFEST.json', manifest)
    print(json.dumps(dict(status=manifest['status'], prepared=len(descriptors), gaps=len(gaps),
                         existing_reused=manifest['existing_online_addresses_reused'], manifest=mr)))
