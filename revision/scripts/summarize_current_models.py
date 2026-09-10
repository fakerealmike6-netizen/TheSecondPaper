"""Compact local status extraction; no model, provider or production writer."""
from pathlib import Path
from collections import Counter
from decimal import Decimal, localcontext
from fractions import Fraction
import argparse, datetime, hashlib, json, socket

R = Path(__file__).resolve().parents[1]
C = R / 'code'
seen = {}


def blocked(*args, **kwargs):
    raise RuntimeError('Status extraction cannot access a network')


socket.socket.connect = blocked
socket.create_connection = blocked


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read(path):
    path = path.resolve()
    if not path.is_relative_to(R):
        raise ValueError('Status dependency outside this revision')
    data = path.read_bytes()
    seen[path] = sha(data)
    return json.loads(data)


def ref(path):
    path = path.resolve()
    data = path.read_bytes()
    seen[path] = sha(data)
    return dict(path=path.relative_to(R).as_posix(), sha256=sha(data), bytes=len(data))


def bound(reference, root=C):
    path = (root / reference['path']).resolve()
    value = read(path)
    if seen[path] != reference['sha256']:
        raise ValueError('Saved dependency identity changed: ' + str(path))
    return path, value


def amounts(value):
    result = {k: value.get(k) for k in ('status', 'lower_raw', 'upper_raw',
              'point_raw', 'nominal_raw', 'interpretation', 'proof') if k in value}
    for key in ('lower_raw', 'upper_raw', 'point_raw', 'nominal_raw'):
        if value.get(key) is not None:
            exact = Fraction(value[key])
            with localcontext() as ctx:
                ctx.prec = 36
                result[key.replace('_raw', '_asset_units_display')] = format(
                    Decimal(exact.numerator) / Decimal(exact.denominator) / Decimal(10**18), '.18f')
    return result


def summarize(name):
    base = C / 'derived/stage1d/queries' / name
    collection = read(base / 'collection.json')
    row = dict(query_name=name, collection=ref(base / 'collection.json'),
               labels=ref(base / 'label_snapshot.json'),
               role_adoption=ref(base / 'ROLE_ADOPTION_AND_REPLAY.json'),
               discovery_status=collection['status'],
               counts={key: len(collection.get(key, [])) for key in
                       ('candidate_events', 'context_events', 'states', 'coverage',
                        'fact_conflicts', 'semantic_units', 'semantic_membership',
                        'stops', 'unresolved_frontier')},
               frontier_reasons=dict(Counter(x.get('reason', 'UNSPECIFIED')
                    for x in collection.get('unresolved_frontier', []))),
               context=None, method_runs=[])
    pending = base / 'pending_intervals.json'
    if pending.exists():
        intervals = read(pending)
        row['pending_intervals'] = dict(count=len(intervals), evidence=ref(pending))
    pointer = base / 'CURRENT_CLOSURE_CONTEXT.json'
    if pointer.exists():
        p, receipt = bound(read(pointer))
        ap, assembly = bound(receipt['assembly'])
        context = assembly['context_result']
        model = context.get('model_input') or {}
        ledgers = context.get('ledger_reconciliation', [])
        current_source = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in (C / 'src').glob('*.py')}
        row['context'] = dict(pointer=ref(pointer), receipt=ref(p), assembly=ref(ap),
            status=receipt['status'], completion_status=receipt['completion_status'],
            source_binding_current=receipt['source_sha256'] == current_source,
            missing_points=len(receipt['missing_points']),
            gaps_by_type=dict(Counter(g.get('type', 'UNSPECIFIED') for g in receipt['gaps'])),
            accounts=len(model.get('accounts', [])), transactions=len(model.get('transactions', [])),
            reconciliation_by_status=dict(Counter(x['reconciliation_status'] for x in ledgers)),
            all_account_differences_zero=bool(ledgers) and all(Fraction(x['difference_raw']) == 0
                for x in ledgers if x.get('difference_raw') is not None)
                and all(x.get('difference_raw') is not None for x in ledgers),
            fact_conflicts=len(context.get('fact_conflicts', [])),
            objective_groups=len(model.get('objective_groups', {})))
    for p in sorted((C / 'private/integration_query_methods' / name).glob('*/METHODS_RECEIPT.json')):
        receipt = read(p)
        pp, plan = bound(receipt['plan'])
        record = dict(receipt=ref(p), plan=ref(pp), status=receipt['status'],
            passed=receipt['passed'], source_version=receipt.get('source_version'),
            current_context_pointer_matches=pointer.exists() and
                plan['context_pointer']['sha256'] == ref(pointer)['sha256'],
            repair_continuation=receipt.get('repair_continuation'),
            new_external_requests=receipt['new_external_requests'])
        output = C / receipt['output']
        index_path = output / 'RESULTS_INDEX.json'
        if index_path.exists():
            index = read(index_path)
            record['results_index'] = ref(index_path)
            record['models'] = []
            for sample in index['method_results_index']:
                folder = output / sample['path']
                methods = read(folder / 'METHOD_RESULTS.json')
                efficiency = read(folder / 'EFFICIENCY.json')
                record['models'].append(dict(identity=sample,
                    results=ref(folder / 'METHOD_RESULTS.json'),
                    acceptance=ref(folder / 'QUERY_ACCEPTANCE.json'),
                    joint_by_method_asset={m: {asset: amounts(x) for asset, x in
                         (value.get('joint_by_asset') or {}).items()} for m, value in methods.items()},
                    efficiency=ref(folder / 'EFFICIENCY.json'),
                    timing_summary={m: dict(median_seconds=x['median_seconds'],
                        all_repetition_seconds=sum(y['elapsed_seconds'] for y in x['repetitions']),
                        deterministic=x['deterministic'],
                        repetition_statuses=[y['status'] for y in x['repetitions']])
                        for m, x in efficiency['profiles'].items()}))
        row['method_runs'].append(record)
    return row


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    output = (R / args.output).resolve()
    if not output.is_relative_to(R / 'reports') or output.exists():
        raise ValueError('A new local report path is required')
    policy = R / 'repair_authorization/package/config/REPAIR_RESUME_POLICY.json'
    rules = read(policy)
    report = dict(schema_version='stage1d-current-model-status-v1',
        created_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        authorization=ref(policy), window_seconds_by_query=rules['window_seconds_by_query'],
        depth_by_query=rules['depth_by_query'],
        queries=[summarize(q) for q in rules['query_processing_priority']],
        new_external_requests=0, model_executions=0,
        external_acceptance='PENDING_REVIEW', checkpoint_claimed=False,
        monetary_units='Raw rational amounts preserved; ETH and WETH reported separately, never added',
        qualification='Method PASS is software/scientific contract acceptance; context conditions and incomplete discovery remain explicit.')
    for path, expected in seen.items():
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError('Input changed during report snapshot: ' + str(path))
    report['source_script'] = ref(Path(__file__))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(dict(status='CURRENT_SAVED_MODELS_EXTRACTED_NO_EXECUTION', report=ref(output),
                         queries=len(report['queries']), new_external_requests=0)))
