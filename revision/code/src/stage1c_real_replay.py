"""Offline replay of final accepted evidence, ledgers, endpoints and WETH component.

Only the current dependency closure is required. Earlier whole run trees and ZIPs
are not dependencies. The WETH component remains separate from both ETH queries.
"""
import argparse
import hashlib
import json
from pathlib import Path

from context_ledger_r3 import replay_manifest
from context_lp_r3 import run_context_document
from review_replay_r4 import semantic_ledger, semantic_amounts, audit_result_witnesses, check_context_input_paths, check_weth_input_paths

PILOTS = ('atomic_simple_transfer', 'harmony_high_branch')


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def same_input_comparison(expected_ledger, actual_ledger, expected_amounts, actual_amounts):
    document = actual_ledger['model_input']
    original_audit = audit_result_witnesses(document, expected_amounts)
    replay_audit = audit_result_witnesses(document, actual_amounts)
    ledger_equal = semantic_ledger(expected_ledger) == semantic_ledger(actual_ledger)
    amount_equal = semantic_amounts(expected_amounts) == semantic_amounts(actual_amounts)
    return {'evidence_to_ledger_equal': ledger_equal,
            'amount_endpoints_status_equal': amount_equal,
            'bundled_witness_audit': original_audit,
            'regenerated_witness_audit': replay_audit,
            'passed': ledger_equal and amount_equal and original_audit['passed'] and replay_audit['passed']}


def replay(tree, output):
    from weth_trace_adapter_r4 import replay_weth_r4
    from verify_weth_shared_wire_r4_r1 import verify_tree
    tree, output = Path(tree).resolve(), Path(output).resolve()
    if tree == output or tree in output.parents and output.parts[len(tree.parts)] in {'raw','private','derived','configs','src','tests','fixtures'}:
        raise ValueError('Output cannot replace a frozen input directory')
    output.mkdir(parents=True, exist_ok=True)
    manifest = tree / 'configs/CONTEXT_REPLAY_R3.json'
    check_context_input_paths(tree, manifest)
    rows = []
    for name in PILOTS:
        actual = replay_manifest(manifest, tree, name, output / name / 'ledger')
        expected = read(tree / 'derived/context_pipeline' / name / 'ASSEMBLED_CONTEXT.json')
        amounts = run_context_document(actual['model_input'])
        expected_amounts = read(tree / 'derived/context_amounts' / name / 'CONTEXT_AMOUNT_RESULTS.json')
        write(output / name / 'amounts.json', amounts)
        row = same_input_comparison(expected, actual, expected_amounts, amounts)
        row.update(name=name, source_file_count=len(actual['source_manifest']['files']),
                   anchors=len(actual['balance_anchors']), value_events=sum(len(tx['flows']) for tx in actual['model_input']['transactions']),
                   fees=sum(len(tx['fees']) for tx in actual['model_input']['transactions']))
        rows.append(row)
    weth_manifest = tree / 'derived/weth_context_r4/inputs/R4_INPUT_MANIFEST.json'
    check_weth_input_paths(tree, weth_manifest)
    weth = replay_weth_r4(weth_manifest.parent, output / 'weth_component')
    expected_weth = read(tree / 'derived/weth_context_r4/replay_final_v2/WETH_COMPONENT_RESULT.json')
    wire = verify_tree(tree)
    write(output / 'WETH_SHARED_WIRE_REPLAY.json', wire)
    result = {'schema_version': 'stage1c-final-real-replay-v1', 'queries': rows,
              'weth_component': {'equal_to_accepted': weth == expected_weth,
                  'status': weth.get('status'), 'checks_passed': weth.get('checks_passed'),
                  'checks_total': weth.get('checks_total'), 'not_connected_to_real_pilots': True},
              'weth_shared_wire': wire,
              'network_requests': 0, 'new_provider_data': 0,
              'external_acceptance': 'PENDING_REVIEW',
              'passed': all(row['passed'] for row in rows) and weth == expected_weth and wire.get('status') == 'PASS'}
    write(output / 'REAL_REPLAY_RECEIPT.json', result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tree', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        result = replay(args.tree, args.output)
    except Exception as exc:
        result = {'schema_version': 'stage1c-final-real-replay-v1', 'passed': False,
                  'status': 'ERROR', 'error_type': type(exc).__name__, 'error': str(exc)}
        write(args.output / 'REAL_REPLAY_RECEIPT.json', result)
    print(json.dumps({'passed': result['passed'], 'receipt': str(args.output / 'REAL_REPLAY_RECEIPT.json')}))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
