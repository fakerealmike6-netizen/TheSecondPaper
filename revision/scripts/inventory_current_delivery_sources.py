"""Read-only source census for later delivery, without freezing active data.

This is an inventory of explicit source roots, not a transitive data-dependency
closure, a publication allowlist, or a claim of a new line-by-line code audit.
"""
from pathlib import Path
import hashlib
import json
import socket

R = Path(__file__).resolve().parents[1]
C = R / 'code'


def denied(*args, **kwargs):
    raise RuntimeError('The delivery source census is offline')


socket.socket.connect = denied
socket.create_connection = denied


def describe(path):
    path = path.resolve(strict=True)
    if not path.is_relative_to(R):
        raise ValueError('Source path escapes the current revision')
    data = path.read_bytes()
    return {'path': path.relative_to(R).as_posix(), 'bytes': len(data),
            'sha256': hashlib.sha256(data).hexdigest()}


roots = ['code/src', 'code/tests', 'scripts', 'code/execution_tools',
         'code/review_tools']
rows = []
for root in roots:
    directory = R / root
    if not directory.is_dir():
        raise ValueError('Required current source root is absent: ' + root)
    for path in sorted(directory.rglob('*.py')):
        if path.is_symlink():
            raise ValueError('Source symlink needs separate review')
        rows.append(dict(describe(path), inventory_root=root))

explicit_dependencies = [
    'staging/unknown_cost_future_code_preparation/prepare_future_historical_code.py',
    'staging/unknown_cost_code_preparation/prepare_historical_code.py',
    'code/requirements.txt',
]
extra = [describe(R / p) for p in explicit_dependencies]
gate = describe(C / 'STAGE1D_PREFLIGHT_GATE.json')
doc = {'status': 'CURRENT_SOURCE_CENSUS_ONLY', 'source_roots': roots,
       'files': rows, 'explicit_additional_dependencies': extra,
       'source_gate': gate, 'script': describe(Path(__file__)),
       'counts': {root: sum(r['inventory_root'] == root for r in rows) for root in roots},
       'new_external_requests': 0, 'production_files_changed': False,
       'active_data_frozen': False, 'all_files_line_by_line_reaudited': False,
       'publication_approved_by_this_report': False,
       'complete_transitive_runtime_or_data_dependency_closure': False,
       'limitations': ['Staging and private evidence dependencies must be selected from actual final consumers.',
                       'Publication still requires a separate content review and public/private mapping.',
                       'Current source identities must be rechecked against this census at final payload freeze.']}
payload = json.dumps(doc, ensure_ascii=False, sort_keys=True, indent=2).encode('utf-8') + b'\n'
identity = hashlib.sha256(payload).hexdigest()
out = R / 'reports/final_preparation/source_census' / identity
out.mkdir(parents=True, exist_ok=True)
target = out / 'CURRENT_SOURCE_CENSUS.json'
if target.exists() and target.read_bytes() != payload:
    raise ValueError('Immutable source census conflicts')
if not target.exists():
    target.write_bytes(payload)
for entry in rows + extra + [gate, doc['script']]:
    if describe(R / entry['path'])['sha256'] != entry['sha256']:
        raise ValueError('Source advanced while making the census')
print(json.dumps({'status': doc['status'], 'counts': doc['counts'],
                  'receipt': describe(target), 'new_external_requests': 0}), flush=True)
