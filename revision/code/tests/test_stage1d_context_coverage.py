"""Coverage replay stays finite and preserves exact proof/source semantics."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from stage1d_context_coverage import merge_coverage
import stage1d_context_online as online
import stage1d_context_recovery as recovery
from stage1d_context import REQUIRED
from context_ledger_r3 import coverage_complete
from page_attempts import atomic_json


def proof(lo=10, hi=20, evidence='sha256:'+'a'*64, **overrides):
    row = dict(address='0x'+'1'*40, data_type=REQUIRED[0], start_block=lo,
               end_block=hi, status='COMPLETE', pagination_complete=True,
               provider_frozen_scope=True, date_domain_verified=True,
               block_domain_verified=True, evidence_ids=[evidence])
    row.update(overrides)
    return row


class CoverageSetTests(unittest.TestCase):
    def test_duplicates_idempotent_without_mutating_inputs(self):
        row = proof(); original = copy.deepcopy(row)
        result = merge_coverage([row]*50, [row]*100)
        self.assertEqual([row], result)
        self.assertEqual(result, merge_coverage(result, result))
        result[0]['evidence_ids'].append('other')
        self.assertEqual(original, row)

    def test_same_rectangle_keeps_all_source_ids_in_sorted_set(self):
        a, b = proof(evidence='b'), proof(evidence='a')
        actual = merge_coverage([a, b, a])
        self.assertEqual(1, len(actual))
        self.assertEqual(['a', 'b'], actual[0]['evidence_ids'])
        self.assertEqual(actual, merge_coverage([b, a]))

    def test_distinct_rectangles_never_fill_gap_or_merge_adjacency(self):
        rows = [proof(10, 11), proof(12, 14), proof(17, 20)]
        self.assertEqual(3, len(merge_coverage(rows)))

    def test_different_proof_flags_provider_metadata_and_status_remain(self):
        rows = [proof(), proof(date_domain_verified=False), proof(status='FAILED'),
                proof(pagination_complete=False), proof(provider_alias='other')]
        self.assertEqual(5, len(merge_coverage(rows)))

    def test_absent_null_empty_and_present_evidence_not_promoted(self):
        absent = proof(); absent.pop('evidence_ids')
        rows = [absent, proof(evidence_ids=None), proof(evidence_ids=[]), proof()]
        self.assertEqual(4, len(merge_coverage(rows)))

    def test_malformed_evidence_fails_instead_of_discarding_source(self):
        for ids in ['abc', [1], {'a': 'b'}]:
            with self.assertRaises(ValueError): merge_coverage([proof(evidence_ids=ids)])

    def test_existing_completion_and_missing_interval_contract_unchanged(self):
        rows = [proof(lo, hi, evidence=e, data_type=k) for k in REQUIRED
                for lo, hi in [(10, 14), (17, 20)] for e in ['a', 'b']]*10
        rows += [proof(15, 16, data_type=k, date_domain_verified=False) for k in REQUIRED]
        merged = merge_coverage(rows)
        account = dict(address=proof()['address'], ledger_start_block=10, ledger_end_block=20)
        self.assertEqual([[15, 16]], recovery.missing_intervals(account, merged))
        self.assertEqual(recovery.missing_intervals(account, rows), recovery.missing_intervals(account, merged))
        self.assertEqual(coverage_complete(rows, account['address'], 10, 20, REQUIRED)[0],
                         coverage_complete(merged, account['address'], 10, 20, REQUIRED)[0])

    def test_repeated_loader_rounds_stay_bounded_and_archive_original_hashes(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td); root = work/'context'; root.mkdir()
            original = root/'coverage.json'; atomic_json(original, [proof()]*20)
            before = original.read_bytes()
            first = root/'round_1'; first.mkdir()
            loaded = online._load_context(work, root, first)
            self.assertEqual(1, len(loaded[4])); self.assertEqual(1, len(loaded[6]))
            for source in loaded[6]:
                self.assertEqual(source['sha256'], hashlib.sha256((work/source['snapshot_path']).read_bytes()).hexdigest())
            atomic_json(first/'coverage.json', loaded[4])
            second = root/'round_2'; second.mkdir()
            repeated = online._load_context(work, root, second)
            self.assertEqual(1, len(repeated[4])); self.assertEqual(2, len(repeated[6]))
            self.assertEqual(before, original.read_bytes())

    def test_recovery_persist_and_model_receive_same_deduplicated_facts(self):
        coverage = [proof(), proof(evidence='b')]*5
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            with patch.object(recovery, 'build_document', return_value={
                    'completion_status':'PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS'}) as build:
                recovery._persist_round(folder, {}, {}, {'rows':[]}, {}, {}, {}, [],
                                        coverage, [], {}, [], 50000, {})
            stored = json.loads((folder/'coverage.json').read_text())
            self.assertEqual(merge_coverage(coverage), stored)
            self.assertEqual(stored, build.call_args.kwargs['coverage'])
            self.assertEqual(10, len(coverage))


if __name__ == '__main__': unittest.main()
