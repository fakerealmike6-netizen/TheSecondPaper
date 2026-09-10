"""Summarize existing finite code-wave receipts; never dispatch or adopt."""
from pathlib import Path
from collections import Counter
import hashlib
import json
import socket

R = Path(__file__).resolve().parents[1]
C = R / 'code'


def denied(*args, **kwargs):
    raise RuntimeError('This receipt summary is offline')


socket.socket.connect = denied
socket.create_connection = denied


def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def ref(path):
    path = path.resolve(strict=True)
    if not path.is_relative_to(R):
        raise ValueError('Receipt reference escapes the current revision')
    data = path.read_bytes()
    return dict(path=path.relative_to(R).as_posix(), bytes=len(data),
                sha256=hashlib.sha256(data).hexdigest())


waves = [
    ('PRIVATE_LITERAL_6', 'PRIVATE_LITERAL_4'),
    ('PRIVATE_LITERAL_10', 'PRIVATE_LITERAL_8'),
    ('PRIVATE_LITERAL_1', 'PRIVATE_LITERAL_3'),
    ('PRIVATE_LITERAL_5', 'PRIVATE_LITERAL_12'),
    ('PRIVATE_LITERAL_9', 'PRIVATE_LITERAL_7'),
    ('PRIVATE_LITERAL_2', 'PRIVATE_LITERAL_11'),
]
rows = []
for prep, operation in waves:
    path = C / 'private/integration_current_cost_code' / prep / ('EXECUTION_' + operation + '.json')
    doc = read(path)
    if doc['status'] != 'FINITE_CURRENT_CODE_EVIDENCE_RETURNED':
        raise ValueError('An unfinished or failed wave must be reported explicitly')
    calls = doc['provider_calls']
    if any(c['result']['status'] != 'COMPLETE' for c in calls):
        raise ValueError('A partially returned wave must not be summarized as complete')
    methods = Counter(n['request']['method'] for c in calls for n in c['needs'])
    rows.append(dict(receipt=ref(path), submitted_members_by_method=dict(methods),
        submitted_members=sum(methods.values()),
        recorded_actual_operations=sum(c['result']['actual_operations_this_call'] for c in calls),
        newly_imported_legacy_points=sum(a['statuses'].get('IMPORTED_POINT_SUCCESS', 0)
                                        for a in doc['legacy_admissions']),
        returned_cache_code_groups=len(doc['results'])))

ap = C / 'private/integration_current_cost_code' / waves[-1][0] / ('ADOPTION_' + waves[-1][1] + '.json')
adoption = read(ap)
if adoption['status'] != 'ADOPTED_REPLAY_REQUIRED':
    raise ValueError('Expected the successful batched code adoption')
manifest_path = R / 'reports/current_cost_label_admission_second_paid_batch_v6/MANIFEST.json'
manifest = read(manifest_path)
report = dict(status='FINITE_CODE_WAVE_ADOPTED_GRAPH_REPLAY_REQUIRED',
    waves=rows, code_adoption=ref(ap), code_groups_admitted=len(adoption['admitted']),
    affected_states=len(adoption['affected_states']), adoption_gaps=len(adoption['gaps']),
    submitted_rpc_members=sum(r['submitted_members'] for r in rows),
    recorded_actual_operations=sum(r['recorded_actual_operations'] for r in rows),
    newly_imported_legacy_points=sum(r['newly_imported_legacy_points'] for r in rows),
    saved_label_manifest=ref(manifest_path), saved_label_descriptors=len(manifest['descriptors']),
    saved_label_descriptor_gaps=len(manifest['gaps']),
    preserved_exhausted_header_groups=6,
    exhausted_group_basis=ref(R / 'reports/unknown_cost_boundary_v1/preparations/after_second_paid_batch_v22.json'),
    scope_or_budget_changed=False, counters_or_attempts_reset=False,
    source=ref(Path(__file__)), report_new_external_requests=0,
    limitations=['Adoption gaps describe the 262 successful admitted groups, not overall discovery closure.',
                 'Six pre-existing exhausted header groups were not retried; replay must preserve their partial states.',
                 'Repeated exact-cache audits are not counted as unique source reuse or claimed CU savings.',
                 'This code-wave count excludes the separate preceding label and BigQuery operations.'])
payload = json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2).encode('utf-8') + b'\n'
target = R / 'reports/second_paid_cost_wave' / hashlib.sha256(payload).hexdigest() / 'WAVE_RECEIPT_SUMMARY.json'
target.parent.mkdir(parents=True, exist_ok=True)
if target.exists() and target.read_bytes() != payload:
    raise ValueError('Immutable report conflicts')
if not target.exists():
    target.write_bytes(payload)
print(json.dumps({k: report[k] for k in ['status', 'code_groups_admitted', 'affected_states',
    'adoption_gaps', 'submitted_rpc_members', 'recorded_actual_operations', 'newly_imported_legacy_points']} |
    {'receipt': ref(target), 'report_new_external_requests': 0}), flush=True)
