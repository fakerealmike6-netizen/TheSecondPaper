"""Rebuild four R1 collection snapshots with R2 code; no providers or network.

The supplied baseline root must contain the named R1 derived subset. Output is
separate, making the same command suitable for a portable private review tree.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform

from cache_probe import fixed_graph
from collector import CollectionResult, Event
from dune_observed_replay import live_fixed_graph
from lp_run import fixed_graph_run

CASES = (
    ('dune_live_replay', 'atomic_simple_transfer', 'derived/label_update_success_same_raw'),
    ('dune_live_replay', 'harmony_high_branch', 'derived/label_update_success_same_raw'),
    ('cache_probe', 'atomic_simple_transfer', 'derived/bugfix_only_same_input/cache_probe'),
    ('cache_probe', 'harmony_high_branch', 'derived/bugfix_only_same_input/cache_probe'),
)


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def semantic_intervals(results):
    return {group: {'joint': {k: result['joint'].get(k) for k in
                              ('status', 'asset', 'lower_raw', 'upper_raw', 'objective_events')},
                    'entry_intervals': {event: {k: interval.get(k) for k in
                                               ('status', 'asset', 'lower_raw', 'upper_raw', 'objective_events')}
                                        for event, interval in result['entry_intervals'].items()}}
            for group, result in results.items()}


def replay(baseline, output):
    baseline, output = Path(baseline).resolve(), Path(output).resolve()
    if output == baseline or output.is_relative_to(baseline):
        raise ValueError('Baseline is read-only; output must be separate')
    cases = []
    for scope, pilot, base in CASES:
        folder = baseline / base / pilot
        collection_path = folder / ('collection.json' if scope == 'dune_live_replay' else 'cached_collection.json')
        graph_path, lp_path = folder / 'fixed_graph.json', folder / 'lp/lp_fixed_graph_result.json'
        original_graph, original_lp = read(graph_path), read(lp_path)
        collection = CollectionResult(**read(collection_path))
        seed_id = next(e['id'] for e in original_graph['events'] if e['kind'] == 'seed')
        seed = Event(**next(e for e in original_graph['physical_fact_manifest'] if e['event_id'] == seed_id))
        builder = live_fixed_graph if scope == 'dune_live_replay' else fixed_graph
        rebuilt_graph, rebuilt_scope = builder(collection, seed)
        dest = output / 'graphs' / scope / pilot
        write(dest / 'fixed_graph.json', rebuilt_graph)
        write(dest / 'model_scope.json', rebuilt_scope)
        fixed_graph_run(dest / 'fixed_graph.json', dest / 'lp')
        rebuilt_lp = read(dest / 'lp/lp_fixed_graph_result.json')
        differences = sorted(k for k in set(original_graph) | set(rebuilt_graph)
                             if original_graph.get(k) != rebuilt_graph.get(k))
        old_intervals, new_intervals = semantic_intervals(original_lp['results']), semantic_intervals(rebuilt_lp['results'])
        unchanged = not differences and old_intervals == new_intervals
        cases.append({'case': scope + '/' + pilot, 'classification': 'SAME_INPUT_FIXED_CODE',
                      'source_inputs': [{'path': p.relative_to(baseline).as_posix(), 'sha256': sha(p), 'bytes': p.stat().st_size}
                                        for p in (collection_path, graph_path, lp_path)],
                      'original_r1_graph_unchanged': sha(graph_path) == original_lp['graph_file_sha256'],
                      'same_graph_semantics': not differences, 'graph_differing_keys': differences,
                      'same_event_and_joint_intervals': old_intervals == new_intervals,
                      'event_count': len(rebuilt_graph['events']),
                      'physical_fact_count': len(rebuilt_graph['physical_fact_manifest']),
                      'unresolved_order_pairs': rebuilt_scope['order_validation']['unresolved_order_pairs'],
                      'target_status': 'OBSERVED_TARGET' if new_intervals else 'NO_OBSERVED_TARGET',
                      'joint_results': {group: value['joint'] for group, value in new_intervals.items()},
                      'passed': unchanged})
    summary = {'schema': 'stage1b-r2-same-input-order-replay-v1',
               'created_at_utc': datetime.now(timezone.utc).isoformat(),
               'platform': platform.platform(), 'python': platform.python_version(),
               'baseline_run': '20260906T212828+0800_stage1b_r1',
               'network_requests': 0, 'new_chain_events': 0, 'new_labels': 0,
               'graph_count': len(cases), 'event_entries_in_four_graphs': sum(c['event_count'] for c in cases),
               'cases': cases, 'passed': all(c['passed'] and c['original_r1_graph_unchanged'] for c in cases)}
    write(output / 'same_input_replay.json', summary)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = replay(args.baseline, args.output)
    print(json.dumps({k: result[k] for k in ('passed', 'graph_count', 'event_entries_in_four_graphs', 'network_requests')}))
    raise SystemExit(0 if result['passed'] else 1)
