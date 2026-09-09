"""Append a narrow post-delivery receipt; do not overwrite the earlier receipt."""
from pathlib import Path
import io
import json
import shutil
import time
import unittest
from build_delivery import sha
from test_compatibility import CompatibilityChecks

S = Path(__file__).resolve().parent
EXPECTED_BUILDER = 'd7e5b2959077c9d26ceaf76b37418880b0dfd3692f0c57046cb1b5e700031fcb'
before = sha(S / 'build_delivery.py')
if before != EXPECTED_BUILDER: raise ValueError('Root delivered builder changed; do not test a guessed version')
prior = []
for name in ('CHECKS.json', 'DELIVERABLE_MANIFEST.json', 'CHECKS_COMPATIBILITY.json'):
    src = S / name; digest = sha(src)
    dst = S / 'prior_receipts' / (digest + '_' + name)
    dst.parent.mkdir(exist_ok=True)
    if not dst.exists(): shutil.copyfile(src, dst)
    if sha(dst) != digest: raise ValueError('Prior receipt copy differs')
    prior.append({'original_path': name, 'sha256': digest,
                  'preserved_copy': dst.relative_to(S).as_posix()})
stream = io.StringIO(); wall = time.perf_counter(); cpu = time.process_time()
result = unittest.TextTestRunner(stream=stream, verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(CompatibilityChecks))
receipt = {'schema_version': 'stage1d-two-address-post-delivery-compatibility-v1',
    'status': 'PASS' if result.wasSuccessful() else 'FAIL', 'tests': result.testsRun,
    'failures': len(result.failures), 'errors': len(result.errors),
    'builder_source_sha256': before, 'source_stable': before == sha(S / 'build_delivery.py'),
    'prior_receipt_pointers': prior, 'wall_seconds': time.perf_counter() - wall,
    'cpu_seconds': time.process_time() - cpu, 'output': stream.getvalue(),
    'prior_15_checks_rerun': False, 'production_state_writes': 0, 'dynamic_graph_reads': 0,
    'fixed_authorization_fixture_read': True,
    'network_calls': 0, 'formal_method_runs': 0, 'real_zip_read_or_rebuilt': False,
    'sealed_delivery_reported_by_root': {
        'path': 'reports/two_address_identity_20260909T190655+0800/delivery/Stage1D_Two_Address_Identity_Check.zip',
        'bytes': 1544118, 'sha256': '9b7507312e780c3a5f21c5644a1beecd9df4e4f520b4d77fccf893a036acb3e1',
        'member_count': 32, 'root_reported_crc_and_member_sha': 'PASS',
        'independently_reverified_in_this_task': False}}
target = S / 'CHECKS_COMPATIBILITY_FINAL.json'
if target.exists(): raise ValueError('Do not overwrite post-delivery receipt')
target.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(stream.getvalue()); print(json.dumps({k:v for k,v in receipt.items() if k != 'output'}, ensure_ascii=False, indent=2))
raise SystemExit(0 if result.wasSuccessful() and receipt['source_stable'] else 1)
