"""Root-executed, offline review/apply of the fixed curated role bundle."""
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import argparse
import json
import shutil
import sqlite3
import sys
import tempfile

from prepare_candidate import HERE, PREFIX, CERT, ADDR, sha, read, encode


def safe(root, relative):
    path = root / relative
    if Path(relative).is_absolute() or not path.resolve().is_relative_to(root) or path.is_symlink():
        raise ValueError('Role candidate path escapes its root')
    return path


def verify_bundle(bundle):
    manifest = read(bundle / 'BUNDLE_MANIFEST.json')
    if manifest.get('status') != 'REVIEW_CANDIDATE_NOT_ADOPTED':
        raise ValueError('Expected an unadopted candidate bundle')
    for ref in manifest['files']:
        path = safe(bundle, ref['path'])
        if sha(path) != ref['sha256'] or path.stat().st_size != ref['bytes']:
            raise ValueError('Candidate bytes changed: ' + ref['path'])
    return manifest


def mirror(work, bundle, destination, manifest):
    for ref in manifest['scope_and_inherited_dependencies']:
        source = safe(work, ref['path'])
        if sha(source) != ref['sha256']: raise ValueError('Current inherited/scope source changed')
        target = safe(destination, ref['path']); target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    for ref in manifest['files']:
        target = safe(destination, ref['path']);target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(bundle / ref['path'], target)
    from stage1d_role_adoption import TechnicalRoles, validate_certificate
    verified = validate_certificate(destination, read(destination / CERT))
    roles = TechnicalRoles(destination)
    expected = read(bundle / 'private/stage1d_roles/CURRENT.json')['certificates']
    if len(roles.records) != len(expected) or verified['verification']['status'] != 'PASS':
        raise ValueError('Current plus new role certificates do not all independently validate')
    return verified


def safe_point(work):
    if (work / 'private/network_worker.lock').exists():
        raise ValueError('Existing network writer must reach its normal safe point')
    for directory in ('private/stage1d_sessions', 'private/context_sessions_r4'):
        for path in (work / directory).glob('*.json'):
            if read(path).get('closed') is not True:
                raise ValueError('Unclosed measured session; no adoption write allowed')
    database = work / 'private/read_retry_r4.sqlite'
    if not database.is_file(): raise ValueError('Existing read ledger is required; never create it')
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        a = db.execute("SELECT count(*) FROM read_requests WHERE state IN ('IN_FLIGHT','INFLIGHT')").fetchone()[0]
        b = db.execute("SELECT count(*) FROM read_attempts WHERE outcome IN ('IN_FLIGHT','INFLIGHT')").fetchone()[0]
        if a or b: raise ValueError('In-flight request state must remain untouched')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--bundle', type=Path, default=HERE / 'candidate')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args(argv)
    work, bundle = args.work.resolve(), args.bundle.resolve()
    manifest = verify_bundle(bundle)
    if sha(work / 'src/stage1d_closure_scope.py') != manifest['unchanged_scope_source_sha256']:
        raise ValueError('Current scope validator source changed')
    # Source installation is root's separate source-gate operation. Review uses
    # only these staged candidates; apply insists those exact sources are live.
    source_root = work / 'src' if args.apply else HERE / 'src'
    for name, digest in manifest['validator_source_sha256'].items():
        if sha(source_root / name) != digest: raise ValueError('Reviewed role validator source changed')
    sys.path[:0] = [str(source_root), str(work / 'src')]
    expected = {ref['path']: ref for ref in manifest['files']}
    for ref in manifest['base_registry_refs']:
        current = sha(work / ref['path'])
        if current not in (ref['sha256'], expected[ref['path']]['sha256']):
            raise ValueError('Role registry has unrelated changes; never overwrite')
    with tempfile.TemporaryDirectory(prefix='role_review_', dir=HERE) as name:
        verified = mirror(work, bundle, Path(name), manifest)
    receipt = {'status': 'CURATED_SCOPE_STOP_VALIDATED_REVIEW_ONLY', 'address': ADDR,
        'decision_basis': verified['decision_basis'], 'query_scopes': verified['query_scopes'],
        'verification': verified['verification'], 'bundle_manifest_sha256': sha(bundle / 'BUNDLE_MANIFEST.json'),
        'validator_source_sha256': manifest['validator_source_sha256'], 'network_calls': 0,
        'graph_replay_performed': False, 'label_values_changed': False, 'user_task_boundaries_changed': False,
        'custodial_service_certification': False, 'source_upper_bound_certification': False,
        'http_status': None, 'all_methods_scope_sync': 'PENDING_REPLAY_AND_FINAL_FREEZE'}
    if args.apply:
        safe_point(work)
        # Preserve the exact former registry bytes before additive replacement.
        for ref in manifest['base_registry_refs']:
            source = work / ref['path'];backup = work / PREFIX / 'pre_adoption' / source.name
            if not backup.exists():
                if sha(source) != ref['sha256']: raise ValueError('Prior registry backup missing after partial adoption')
                backup.parent.mkdir(parents=True, exist_ok=True)
                with backup.open('xb') as stream: stream.write(source.read_bytes())
            if sha(backup) != ref['sha256']: raise ValueError('Prior registry backup identity differs')
        changes = {r['path'] for r in manifest['base_registry_refs']}
        for ref in manifest['files']:
            target = safe(work, ref['path'])
            if ref['path'] not in changes and target.exists() and sha(target) != ref['sha256']:
                raise ValueError('New immutable curated evidence conflicts')
        pointer = 'private/stage1d_roles/CURRENT.json'
        ordered = sorted(manifest['files'], key=lambda r: (r['path'] in changes, r['path'] == pointer, r['path']))
        for ref in ordered:
            target = safe(work, ref['path']);target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() and sha(target) == ref['sha256']: continue
            data = (bundle / ref['path']).read_bytes()
            if ref['path'] in changes:
                temporary = target.with_name(target.name + '.8ac4.tmp')
                with temporary.open('xb') as stream: stream.write(data)
                temporary.replace(target)
            else:
                with target.open('xb') as stream: stream.write(data)
            if sha(target) != ref['sha256']: raise ValueError('Adopted curated source copy differs')
        from stage1d_role_adoption import TechnicalRoles
        if not any(c['address'] == ADDR for c in TechnicalRoles(work).records):
            raise ValueError('Actual curated certificate readback failed')
        receipt.update(status='CURATED_SCOPE_STOP_ADOPTED_REPLAY_PENDING', adopted_at_utc=datetime.now(timezone.utc).isoformat())
        target = work / PREFIX / 'ADOPTION_RECEIPT.json'
        if target.exists():
            old = read(target)
            if old['bundle_manifest_sha256'] != receipt['bundle_manifest_sha256']: raise ValueError('Another adoption receipt exists')
            receipt = old
        else:
            with target.open('xb') as stream: stream.write(encode(receipt))
    print(json.dumps(receipt, ensure_ascii=False, indent=2))
    return receipt


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    main()
