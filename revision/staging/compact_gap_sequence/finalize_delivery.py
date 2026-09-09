"""Finalize only this staged candidate, without installing or running production."""
from pathlib import Path
import difflib
import hashlib
import json

S = Path(__file__).resolve().parent
R = S.parent.parent
C = R / 'code'
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

base = json.loads((S / 'COLLECTOR_BASE.json').read_text(encoding='utf-8'))
assert sha(C / 'src/collector.py') == base['base_sha256'], 'Active collector base changed: do not install blindly'
expected = {
    'collector.py': '50cf349d07c6ebc73d985d8a224a1952bc0fd8dca4c4dbc2a0112409e782aad1',
    'stage1d_gap_sequence.py': '0d178279c0cdf2b18325a4281c484877e1165831b7803ed723677092d00cd6ab',
}
for name, digest in expected.items():
    assert sha(S / 'src' / name) == digest
receipt = json.loads((S / 'TEST_RECEIPT.json').read_text(encoding='utf-8'))
assert receipt['status'] == 'PASS' and receipt['tests'] == 22
assert receipt['source_sha256'] == expected
patch = ''.join(difflib.unified_diff(
    (C / 'src/collector.py').read_text(encoding='utf-8').splitlines(True),
    (S / 'src/collector.py').read_text(encoding='utf-8').splitlines(True),
    fromfile='a/src/collector.py', tofile='b/src/collector.py'))
(S / 'COLLECTOR.patch').write_text(patch, encoding='utf-8')

files = []
for rel, target, old in [
    ('src/stage1d_gap_sequence.py', 'src/stage1d_gap_sequence.py', None),
    ('src/collector.py', 'src/collector.py', base['base_sha256']),
    ('tests/test_stage1d_gap_sequence.py', 'tests/test_stage1d_gap_sequence.py', None),
]:
    p = S / rel
    files.append({'staged_path': rel, 'install_target': target,
                  'base_sha256': old, 'new_sha256': sha(p), 'bytes': p.stat().st_size})
write(S / 'SOURCE_MANIFEST.json', {
    'schema_version': 'stage1d-staged-source-manifest-v1',
    'status': 'READY_NOT_INSTALLED',
    'candidate_root': str(S), 'production_root': str(C),
    'files': files,
    'test_receipt': {'path': 'TEST_RECEIPT.json', 'sha256': sha(S / 'TEST_RECEIPT.json')},
    'source_base_rechecked': True,
    'production_writes': 0, 'network_calls': 0, 'formal_method_runs': 0,
    'installation_requires_coordinated_read_write_boundaries': True,
})

artifacts = []
for rel in ['SOURCE_MANIFEST.json', 'COLLECTOR_BASE.json', 'COLLECTOR.patch',
            'TEST_RECEIPT.json', 'HANDOFF.md', 'run_targeted.py', 'finalize_delivery.py']:
    p = S / rel
    artifacts.append({'path': rel, 'sha256': sha(p), 'bytes': p.stat().st_size})
write(S / 'DELIVERABLE_MANIFEST.json', {'schema_version': 'stage1d-staged-deliverable-v1',
                                     'status': 'READY_NOT_INSTALLED', 'artifacts': artifacts,
                                     'source_files': files})
print(json.dumps({'status': 'READY_NOT_INSTALLED',
                  'source_manifest_sha256': sha(S / 'SOURCE_MANIFEST.json'),
                  'deliverable_manifest_sha256': sha(S / 'DELIVERABLE_MANIFEST.json')}, indent=2))
