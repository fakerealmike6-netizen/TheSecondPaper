"""Pure synthetic import serialization tests, with no real raw or credentials."""
from collections import Counter
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import stage1d_batch_binding_route as b
import stage1d_bq_context_prepare as h


def fixture(work, count=3, complete=True):
    # This is a serialization seam; it never represents a real validated ledger.
    needs = [dict(address='0x' + format(i + 1, '040x'), start_block=10 + i,
        end_block=20 + i, start_time=100 + i, end_time=200 + i,
        query_id='synthetic-query-' + str(i), scope_id='synthetic-scope')
        for i in range(count)]
    h.save(work / 'PREPARATION.json', {'need_rectangles': needs})
    h.save(work / 'job.json', {'synthetic': 'terminal-fixture'})
    result = dict(events=[{'event_id': 'synthetic:physical-event'}],
        rows=[{'source': 'SYNTHETIC_ONLY'}], root_bindings=[],
        validated={'fee_gaps': [] if complete else [{'reason': 'SYNTHETIC_FEE_GAP'}]},
        gaps=[] if complete else [{'reason': 'SYNTHETIC_EXPLICIT_GAP'}],
        native_complete=complete, full_context_claimed=False)
    return result, {'synthetic-spec': 'job.json'}, needs


class BatchBindingImmutableRefsTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix='synthetic-immutable-refs-')
        self.addCleanup(self.folder.cleanup)
        self.work = Path(self.folder.name)

    def test_each_immutable_proof_and_events_ref_is_hashed_once_for_many_needs(self):
        result, states, needs = fixture(self.work, count=5)
        real_sha, real_dep = h.sha, h.dep
        with patch.object(b, '_result', return_value=result), \
             patch.object(h, 'sha', wraps=real_sha) as hashing, \
             patch.object(h, 'dep', wraps=real_dep) as dependencies:
            output = b.import_completed(self.work, 'PREPARATION.json', states)
        hashes = Counter(Path(call.args[0]).name for call in hashing.call_args_list)
        deps = Counter(Path(call.args[1]).name for call in dependencies.call_args_list)
        self.assertEqual((hashes['proof.json'], hashes['events.json']), (1, 1))
        self.assertEqual((deps['proof.json'], deps['events.json']), (1, 1))
        records = [h.read(self.work / ref['path']) for ref in output['coverage_records']]
        self.assertEqual([record['addresses'] for record in records], [[n['address']] for n in needs])
        for record in records:
            self.assertEqual(record['native_decision_sha256'], output['proof']['sha256'])
            self.assertEqual(record['native_decision_path'], output['proof']['path'])
            self.assertEqual(record['events_sha256'], real_sha(self.work / record['events_path']))
            self.assertFalse(record['context_complete'])
            self.assertFalse(record['all_asset_export_complete'])
        self.assertFalse(output['full_context_claimed'])

    def test_partial_gaps_fees_and_coverage_are_not_upgraded(self):
        result, states, needs = fixture(self.work, complete=False)
        with patch.object(b, '_result', return_value=result):
            output = b.import_completed(self.work, 'PREPARATION.json', states)
        self.assertEqual(output['status'], 'BATCH_BINDING_PARTIAL_WITH_EXPLICIT_GAPS')
        self.assertEqual(output['gaps'], result['gaps'])
        self.assertEqual(output['fee_gaps'], result['validated']['fee_gaps'])
        for ref in output['coverage_records']:
            record = h.read(self.work / ref['path'])
            self.assertFalse(record['complete'])
            self.assertFalse(record['native_scope_complete'])
            self.assertEqual(record['normalization_gaps'], result['gaps'])

    def test_empty_need_list_does_not_add_events_hash_requirement(self):
        result, states, _ = fixture(self.work, count=0)
        with patch.object(b, '_result', return_value=result), patch.object(h, 'sha', wraps=h.sha) as hashing:
            output = b.import_completed(self.work, 'PREPARATION.json', states)
        hashes = Counter(Path(call.args[0]).name for call in hashing.call_args_list)
        self.assertEqual(output['coverage_records'], [])
        self.assertEqual((hashes['proof.json'], hashes['events.json']), (1, 0))

    def test_save_failure_propagates_before_reference_or_coverage_generation(self):
        result, states, _ = fixture(self.work)
        real_save, real_dep = h.save, h.dep
        def failing_save(path, value):
            if Path(path).name == 'events.json':
                raise OSError('synthetic save failure')
            return real_save(path, value)
        with patch.object(b, '_result', return_value=result), \
             patch.object(h, 'save', side_effect=failing_save), \
             patch.object(h, 'dep', wraps=real_dep) as dependencies:
            with self.assertRaisesRegex(OSError, 'synthetic save failure'):
                b.import_completed(self.work, 'PREPARATION.json', states)
        self.assertNotIn('proof.json', [Path(call.args[1]).name for call in dependencies.call_args_list])
        self.assertFalse((self.work / 'derived').exists())

    def test_proof_or_events_hash_failure_propagates_without_coverage(self):
        for filename in ('proof.json', 'events.json'):
            with self.subTest(filename=filename):
                result, states, _ = fixture(self.work)
                real_sha = h.sha
                def failing_sha(path):
                    if Path(path).name == filename:
                        raise OSError('synthetic hash failure: ' + filename)
                    return real_sha(path)
                with patch.object(b, '_result', return_value=result), patch.object(h, 'sha', side_effect=failing_sha):
                    with self.assertRaisesRegex(OSError, 'synthetic hash failure'):
                        b.import_completed(self.work, 'PREPARATION.json', states)
                self.assertFalse((self.work / 'derived').exists())

    def test_changed_preparation_still_fails_before_saved_proof_ref(self):
        result, states, _ = fixture(self.work)
        real_save = h.save
        def mutate_preparation_after_save(path, value):
            real_save(path, value)
            if Path(path).name == 'ledger_rows.json':
                (self.work / 'PREPARATION.json').write_text('{"changed":true}', encoding='utf-8')
        with patch.object(b, '_result', return_value=result), \
             patch.object(h, 'save', side_effect=mutate_preparation_after_save), \
             patch.object(h, 'sha', wraps=h.sha) as hashing:
            with self.assertRaisesRegex(ValueError, 'SHA-bound Transfers dependency missing or changed'):
                b.import_completed(self.work, 'PREPARATION.json', states)
        self.assertNotIn('proof.json', [Path(call.args[0]).name for call in hashing.call_args_list])
        self.assertFalse((self.work / 'derived').exists())


class BatchBindingVerifierMemoTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix='synthetic-verifier-memo-')
        self.addCleanup(self.folder.cleanup)
        self.work = Path(self.folder.name)
        self.result, self.states, _ = fixture(self.work, count=4)
        self.records = self.import_result(self.result)

    def import_result(self, result):
        with patch.object(b, '_result', return_value=result):
            output = b.import_completed(self.work, 'PREPARATION.json', self.states)
        return [h.read(self.work / ref['path']) for ref in output['coverage_records']]

    def test_many_records_verify_proof_identity_result_and_event_bytes_once(self):
        memo = {}
        with patch.object(b, '_result', return_value=self.result) as replay, \
             patch.object(h, 'digest', wraps=h.digest) as digests, \
             patch.object(b.transfers, 'sha', wraps=b.transfers.sha) as hashes, \
             patch.object(h, 'read', wraps=h.read) as reads:
            for record in self.records:
                self.assertEqual(b.verify_interval_record(self.work, record, memo=memo), self.result)
        filenames = Counter(Path(call.args[0]).name for call in hashes.call_args_list)
        readnames = Counter(Path(call.args[0]).name for call in reads.call_args_list)
        self.assertEqual((replay.call_count, digests.call_count), (1, 1))
        self.assertEqual((filenames['proof.json'], filenames['events.json']), (1, 1))
        self.assertEqual((readnames['proof.json'], readnames['events.json']), (1, 1))

    def test_each_record_still_checks_identity_range_claims_and_gaps(self):
        memo = {}
        with patch.object(b, '_result', return_value=self.result):
            b.verify_interval_record(self.work, self.records[0], memo=memo)
            for change in ({'evidence_id': 'wrong'}, {'end_time': 999999},
                           {'complete': False}, {'native_scope_complete': False},
                           {'context_complete': True}, {'all_asset_export_complete': True},
                           {'normalization_gaps': [{'reason': 'changed'}]},
                           {'addresses': ['0x' + 'f' * 40]}):
                with self.subTest(change=change), self.assertRaises(ValueError):
                    b.verify_interval_record(self.work, dict(self.records[1], **change), memo=memo)

    def test_path_or_sha_change_is_never_a_memo_hit(self):
        memo, record = {}, self.records[0]
        with patch.object(b, '_result', return_value=self.result) as replay:
            b.verify_interval_record(self.work, record, memo=memo)
            for field in ('native_decision_sha256', 'events_sha256'):
                with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'SHA-bound Transfers dependency'):
                    b.verify_interval_record(self.work, dict(record, **{field: '0' * 64}), memo=memo)
            for field in ('native_decision_path', 'events_path'):
                with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'SHA-bound Transfers dependency'):
                    b.verify_interval_record(self.work, dict(record, **{field: 'missing.json'}), memo=memo)
            (self.work / 'proof_copy.json').write_bytes((self.work / record['native_decision_path']).read_bytes())
            b.verify_interval_record(self.work, dict(record, native_decision_path='proof_copy.json'), memo=memo)
            self.assertEqual(replay.call_count, 2)

    def test_successful_event_memo_cannot_pollute_another_proof(self):
        memo = {}
        with patch.object(b, '_result', return_value=self.result):
            b.verify_interval_record(self.work, self.records[0], memo=memo)
        other = copy.deepcopy(self.result)
        other['events'] = [{'event_id': 'synthetic:different-physical-event'}]
        other_records = self.import_result(other)
        polluted = dict(other_records[0], events_path=self.records[0]['events_path'],
            events_sha256=self.records[0]['events_sha256'])
        with patch.object(b, '_result', return_value=other):
            with self.assertRaisesRegex(ValueError, 'Candidate cache differs'):
                b.verify_interval_record(self.work, polluted, memo=memo)

    def test_same_memo_does_not_bypass_new_workspace_files(self):
        memo = {}
        with patch.object(b, '_result', return_value=self.result):
            b.verify_interval_record(self.work, self.records[0], memo=memo)
            other_work = self.work / 'empty_workspace'
            other_work.mkdir()
            with self.assertRaisesRegex(ValueError, 'SHA-bound Transfers dependency'):
                b.verify_interval_record(other_work, self.records[0], memo=memo)

    def test_no_memo_means_each_call_revalidates_independently(self):
        with patch.object(b, '_result', return_value=self.result) as replay, \
             patch.object(h, 'digest', wraps=h.digest) as digests:
            for record in self.records[:2]:
                b.verify_interval_record(self.work, record)
        self.assertEqual((replay.call_count, digests.call_count), (2, 2))

    def test_new_event_bytes_require_sha_then_exact_expected_content(self):
        memo, record = {}, self.records[0]
        with patch.object(b, '_result', return_value=self.result):
            b.verify_interval_record(self.work, record, memo=memo)
            altered = self.work / 'altered_events.json'
            h.save(altered, [{'event_id': 'synthetic:unrelated'}])
            with self.assertRaisesRegex(ValueError, 'Candidate cache differs'):
                b.verify_interval_record(self.work, dict(record, events_path='altered_events.json',
                    events_sha256=h.sha(altered)), memo=memo)


if __name__ == '__main__':
    unittest.main()
