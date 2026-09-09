"""Synthetic flat/compact boundary equivalence; no LP solve or provider calls."""
import copy
from dataclasses import dataclass
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from stage1d_gap_sequence import (as_gap_sequence, concat_gaps, serialize_gaps,
    iter_gaps, gap_count, storage_counts, GapSequence)
import stage1d_context as native
import stage1d_multiasset_context as multi
import stage1d_closure_context as closure
import context_lp_r3 as context
from test_stage1d_context import base, anchors, complete, A, T, X
from test_stage1d_multiasset_pipeline import pipeline_material
from test_stage1d_closure_context import fixture as closure_fixture


def before(module):
    # Find the installed pre-patch copy through the existing tests location;
    # this remains a pure synthetic test and carries no real material paths.
    import test_stage1d_context
    path = Path(test_stage1d_context.__file__).resolve().parents[1] / 'src' / (module + '.py')
    spec = importlib.util.spec_from_file_location('before_' + module, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def logical(value):
    if isinstance(value, list):
        return [logical(v) for v in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key in ('gaps', 'evidence_gaps'):
            result[key] = [logical(v) for v in iter_gaps(item)]
        elif key == 'input_collection_hash':
            # Representation hash changes, never the scientific fact/model.
            result[key] = '<bound-collection-representation-hash>'
        else:
            result[key] = logical(item)
    return result


def matrices(model):
    return (model.variables, model.eq, model.rhs, model.eq_names,
            model.event_variables, model.balance_lifts)


def native_args(gaps):
    query, collection, seed, outgoing = base()
    collection['gaps'] = gaps
    balances, headers = anchors([(A, 9, 20), (A, 11, 10)])
    coverage = complete(native.required_context_windows(query, collection))
    return query, collection, [], balances, headers, {}, {T: {'kind': 'SERVICE'}}, coverage


def native_build(module, args):
    return module.build_document(*args[:-1], coverage=args[-1])


class CompactContextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old_native = before('stage1d_context')
        cls.old_multi = before('stage1d_multiasset_context')
        cls.old_context = before('context_lp_r3')

    def test_native_order_duplicates_account_wrapping_and_matrix_equal(self):
        gaps = [{'reason': 'R', 'address': A}, {'reason': 'S', 'address': None},
                {'reason': 'R', 'address': A}, {'reason': 'U', 'address': X}] * 50
        old = native_build(self.old_native, native_args(gaps))
        for encoded in (gaps, serialize_gaps(gaps, force_compact=True), as_gap_sequence(gaps)):
            new = native_build(native, native_args(encoded))
            self.assertEqual(logical(old['model_input']), logical(new['model_input']))
            self.assertEqual(old['completion_status'], new['completion_status'])
            self.assertEqual(matrices(self.old_context.build_context_model(old['model_input'])),
                             matrices(context.build_context_model(new['model_input'])))
            reloaded = json.loads(json.dumps(new))
            self.assertEqual(logical(new), logical(reloaded))

    def test_empty_wire_does_not_create_partial_status(self):
        encoded = serialize_gaps([], force_compact=True)
        result = native_build(native, native_args(encoded))
        self.assertEqual(gap_count(result['evidence_gaps']), 0)
        self.assertEqual(result['completion_status'], 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')
        doc = result['model_input']
        self.assertEqual(context.build_context_model(doc).metadata['balance_status'], 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')
        self.assertEqual(context.target_completeness(doc, doc['all_service_entries'])['status'], 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')

    def test_target_account_filter_keeps_exact_order_and_none(self):
        result = native_build(native, native_args([]))
        doc = result['model_input']
        gaps = [{'type': 'G', 'account_id': X+'|ETH'}, {'type': 'G', 'account_id': None},
                {'type': 'G', 'account_id': A+'|ETH'}] * 100
        doc['gaps'] = serialize_gaps(gaps, force_compact=True)
        selected = context.target_completeness(doc, doc['all_service_entries'])
        expected = [g for g in gaps if g['account_id'] in (None, A+'|ETH')]
        self.assertEqual(list(iter_gaps(selected['gaps'])), expected)
        self.assertEqual(selected['status'], 'PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS')
        doc['gaps'] = serialize_gaps([gaps[0]] * 200, force_compact=True)
        self.assertEqual(context.target_completeness(doc, doc['all_service_entries'])['status'], 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')

    def test_native_independent_witness_positive_and_negative_equal(self):
        old = native_build(self.old_native, native_args([{'reason': 'DATA'}] * 180))['model_input']
        new = native_build(native, native_args(serialize_gaps([{'reason': 'DATA'}] * 180, force_compact=True)))['model_input']
        for amount in (70, 91):
            values = {f['event_id']: ('80' if f['role'] == 'SEED' else str(amount))
                      for tx in old['transactions'] for f in tx['flows']}
            values.update({f['fee_id']: '0' for tx in old['transactions'] for f in tx['fees']})
            previous = self.old_context.audit_context_witness(old, values)
            current = context.audit_context_witness(new, values)
            self.assertEqual(previous, current)
            self.assertEqual(current['exact_feasible'], amount == 70)

    def test_multiasset_deposit_and_withdraw_full_ledger_unchanged(self):
        for withdraw in (False, True):
            args = list(pipeline_material(withdraw))
            gaps = [{'reason': 'LIMIT', 'address': A}, {'reason': 'OTHER'}] * 90
            args[1]['gaps'] = gaps
            old = self.old_multi.build_document(*args)
            args[1]['gaps'] = serialize_gaps(gaps, force_compact=True)
            new = multi.build_document(*args)
            self.assertIsNotNone(new['model_input'])
            self.assertEqual(logical(old['model_input']), logical(new['model_input']))
            self.assertEqual(matrices(self.old_context.build_context_model(old['model_input'])),
                             matrices(context.build_context_model(new['model_input'])))
            self.assertEqual(old['completion_status'], new['completion_status'])

    def test_multiasset_blob_fee_negative_stays_blocked(self):
        args = list(pipeline_material())
        args[1]['gaps'] = [{'reason': 'DATA'}] * 180
        args[5]['0x'+'c'*64]['blobGasUsed'] = '0x1'
        old = self.old_multi.build_document(*args)
        args[1]['gaps'] = serialize_gaps(args[1]['gaps'], force_compact=True)
        new = multi.build_document(*args)
        self.assertIsNone(new['model_input'])
        self.assertEqual(logical(old), logical(new))
        self.assertTrue(any(g['type'] == 'BLOB_FEE_FIELDS_INCOMPLETE' for g in iter_gaps(new['evidence_gaps'])))

    def test_closure_missing_point_append_no_duplicate_loss(self):
        query, collection, labels, material = closure_fixture()
        gaps = [{'reason': 'COUNTED_REPEAT'}] * 180
        collection['gaps'] = serialize_gaps(gaps, force_compact=True)
        material['balances'] = {k: v for k, v in material['balances'].items() if v['request']['method'] != 'eth_call'}
        result = closure.assemble(query, collection, labels, material)
        decoded = list(iter_gaps(result['context_evidence']['gaps']))
        self.assertEqual(sum(g['type'] == 'CANDIDATE_ACQUISITION_GAP' for g in decoded), len(gaps))
        self.assertEqual(sum(g['type'] == 'FINAL_CONTEXT_POINT_REQUEST_MISSING' for g in decoded), len(result['missing_points']))
        self.assertEqual(decoded, list(iter_gaps(result['registration_arguments']['document']['gaps'])))
        json.dumps(result)

    def test_large_reference_does_not_iterate_logical_gap_stream(self):
        template = as_gap_sequence([{'reason': 'MISSING', 'address': A, 'ordinal': i} for i in range(100)])
        gaps = concat_gaps(*([template] * 200))
        with patch.object(GapSequence, '__iter__', side_effect=AssertionError('Logical gap expansion forbidden')):
            result = native_build(native, native_args(gaps))
            self.assertEqual(gap_count(result['evidence_gaps']), 20000)
            doc = result['model_input']
            selected = context.target_completeness(doc, doc['all_service_entries'])
            self.assertEqual(gap_count(selected['gaps']), 20000)
        self.assertLess(len(json.dumps(result['evidence_gaps'])), 20000 * 100)

    def test_seed_service_gap_copy_retains_sequence_and_status(self):
        query, collection, seed, _ = base(False)
        collection['stops'] = [{'reason': 'FIRST_IDENTIFIED_SERVICE', 'entry_event_id': seed.event_id,
                               'state': {'address': A}, 'identity': {'kind': 'SERVICE'}}]
        gaps = [{'reason': 'SEED_PENDING', 'address': A}] * 180
        collection['gaps'] = gaps
        old = self.old_native.build_document(query, collection, [], label_snapshot={A: {'kind': 'SERVICE'}})
        collection['gaps'] = serialize_gaps(gaps, force_compact=True)
        new = native.build_document(query, collection, [], label_snapshot={A: {'kind': 'SERVICE'}})
        self.assertTrue(new['model_input']['zero_hop'])
        self.assertEqual(logical(old['model_input']), logical(new['model_input']))
        self.assertEqual(old['completion_status'], new['completion_status'])
        self.assertEqual(gap_count(new['evidence_gaps']), 180)

    def test_dataclass_collection_read_preserves_gap_reference(self):
        @dataclass
        class Collection:
            query_id: str
            gaps: object
        sequence = concat_gaps(*([as_gap_sequence([{'reason': 'D'}])] * 200))
        item = Collection('q', sequence)
        with patch.object(GapSequence, '__iter__', side_effect=AssertionError('No expansion')):
            result = native._collection(item)
        self.assertIs(result['gaps'], sequence)


if __name__ == '__main__':
    unittest.main()
