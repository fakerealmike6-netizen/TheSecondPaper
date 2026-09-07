"""Offline F06/F07 contract regressions, including the actual CLI."""
import copy
import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from labels_import import parse, merge_observations
from labels_policy import resolve_address
from labels_history_r4 import normalized_record, repair_record, run_meta, digest

A = '0x' + 'a' * 40
B = '0x' + 'b' * 40


def observation(actor='Existing Service', oid='old-existing', source='OldSource', **kw):
    return dict(address=A, actor=actor, platform=source, role='SERVICE',
                semantic_kind='ATTRIBUTION', raw_label=actor, source_metadata='',
                observation_id=oid, source_version='v1', collection_scope='GLOBAL',
                record_conflict=False, **kw)


def baseline():
    return dict(address=A, identity_class='SERVICE', actor='Existing Service',
                adopted_observation_ids=['old-existing'], adopted_sources=['OldSource'],
                adopted_source_versions=['v1'])


def payload(address=A, entity=None):
    return {'code': 200000, 'request_id': 'synthetic-request', 'data': [
        {'chain_id': '1', 'address': address, 'main_entity': entity,
         'main_entity_info': {'categories': [{'name': 'Exchange'}]} if entity else {},
         'attributes': []}]}


class RetainedProvenanceTests(unittest.TestCase):
    def test_original_different_actor_counterexample_is_unresolved_without_old_contents(self):
        new = observation('Different Service', 'new-different', 'Professional')
        result = resolve_address([new], baseline())
        self.assertEqual(result['actor'], 'Existing Service')
        self.assertEqual(result['identity_class'], 'SERVICE')
        self.assertEqual(result['adopted_observation_ids'], [])
        self.assertEqual(result['adopted_sources'], [])
        self.assertEqual(result['provenance_status'], 'PROVENANCE_UNRESOLVED')
        self.assertEqual(result['provenance_issues'], ['HISTORICAL_EVIDENCE_GAP'])
        self.assertEqual(result['unadopted_observation_ids'], ['new-different'])

    def test_old_actual_observation_is_carried_with_old_actor_role(self):
        old = observation(); new = observation('Different Service', 'new-different', 'Professional')
        result = resolve_address([new], baseline(), baseline_observations=[old])
        self.assertEqual(result['adopted_observation_ids'], ['old-existing'])
        self.assertEqual(result['adopted_sources'], ['OldSource'])
        self.assertEqual(result['adopted_source_versions'], ['v1'])
        self.assertEqual(result['provenance_status'], 'SUPPORTED_BY_EXISTING_OBSERVATIONS')
        self.assertTrue(result['preserved_conflict'])
        self.assertIn('new-different', result['unadopted_observation_ids'])

    def test_legacy_call_recovers_content_only_from_real_matching_prior_id(self):
        old = observation(); new = observation('Different Service', 'new-different', 'Professional')
        result = resolve_address([old, new], baseline())
        self.assertEqual(result['adopted_observation_ids'], ['old-existing'])

    def test_csv_serialized_ids_are_supported(self):
        base = baseline(); base['adopted_observation_ids'] = '["old-existing"]'
        self.assertEqual(resolve_address([observation()], base)['adopted_observation_ids'], ['old-existing'])

    def test_old_id_with_wrong_actor_does_not_support_old_identity(self):
        result = resolve_address([observation('Different Service')], baseline())
        self.assertEqual(result['provenance_status'], 'PROVENANCE_UNRESOLVED')
        self.assertEqual(result['adopted_observation_ids'], [])

    def test_old_actor_wrong_role_is_not_support(self):
        old = observation(); old['role'] = 'DEX_OR_PROTOCOL'
        result = resolve_address([], baseline(), baseline_observations=[old])
        self.assertEqual(result['identity_class'], 'SERVICE')
        self.assertEqual(result['adopted_observation_ids'], [])

    def test_new_matching_actor_is_not_invented_old_evidence(self):
        result = resolve_address([observation(oid='new-matching')], baseline())
        self.assertEqual(result['provenance_status'], 'PROVENANCE_UNRESOLVED')

    def test_dune_priority_still_adopts_new_supported_identity(self):
        new = observation('Different Service', 'dune-new', 'Dune')
        result = resolve_address([new], baseline(), baseline_observations=[observation()])
        self.assertEqual(result['actor'], 'Different Service')
        self.assertEqual(result['adopted_observation_ids'], ['dune-new'])

    def test_dune_internal_conflict_still_has_no_adopted_attribution(self):
        a = observation('One', 'one', 'Dune'); b = observation('Two', 'two', 'Dune')
        result = resolve_address([a, b], baseline())
        self.assertEqual(result['identity_class'], 'CONFLICTED_IDENTITY')
        self.assertEqual(result['adopted_observation_ids'], [])

    def test_bitpay_alias_old_support_is_accepted(self):
        base = baseline(); base['actor'] = 'BitPay'
        self.assertEqual(resolve_address([observation('BitPay.com')], base)['adopted_observation_ids'], ['old-existing'])

    def test_reference_targeted_fallback_still_supported(self):
        new = observation('Different Service', 'targeted', 'Professional'); new['collection_scope'] = 'REFERENCE_TARGETED'
        result = resolve_address([new], baseline())
        self.assertEqual(result['actor'], 'Different Service')
        self.assertEqual(result['adopted_observation_ids'], ['targeted'])

    def test_same_observation_id_with_conflicting_contents_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Conflicting contents'):
            resolve_address([observation('Different')], baseline(), baseline_observations=[observation()])


class MetaImportTests(unittest.TestCase):
    def parse(self, value, addresses=None, metadata=None):
        return parse(value, metadata or {}, 'synthetic-response', '0' * 64, requested_addresses=addresses)

    def test_provider_error_without_frozen_list_retains_batch_failure(self):
        obs, outcomes = self.parse({'code': 403, 'message': 'permission denied'})
        self.assertEqual(obs, [])
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0]['scope'], 'BATCH')
        self.assertIsNone(outcomes[0]['address'])
        self.assertEqual(outcomes[0]['outcome_status'], 'FAILED_PROVIDER_FAILED')

    def test_provider_error_covers_entire_frozen_request_list(self):
        obs, outcomes = self.parse({'code': 403}, [A, B])
        self.assertEqual(obs, [])
        self.assertEqual({o['address'] for o in outcomes if o['scope'] == 'ADDRESS'}, {A, B})
        self.assertTrue(all(o['request_status'] == 'FAILED' for o in outcomes))

    def test_malformed_structure_and_transport_are_not_success(self):
        for value, meta in [(None, {}), ({'code': 200000, 'data': {}}, {}),
                            (payload(), {'error_class': 'IncompleteRead'}),
                            (payload(), {'status': 'TRANSPORT_FAILED'}),
                            (payload(), {'http_status': 503}),
                            ({'code': 200000.0, 'data': []}, {})]:
            with self.subTest(value=value, metadata=meta):
                obs, outcomes = self.parse(value, [A], meta)
                self.assertEqual(obs, [])
                self.assertTrue(all(o['request_status'] == 'FAILED' for o in outcomes))

    def test_successful_empty_address_is_success_empty(self):
        obs, outcomes = self.parse(payload(), [A])
        self.assertEqual(len(obs), 1)
        self.assertEqual(outcomes[0]['outcome_status'], 'SUCCESS_EMPTY')
        self.assertEqual(outcomes[0]['label_result_status'], 'EMPTY_LABEL_RESULT')
        self.assertEqual(outcomes[0]['request_status'], 'SUCCESS')

    def test_successful_named_address_still_normalized(self):
        obs, outcomes = self.parse(payload(entity='Example Exchange'), [A])
        self.assertEqual(obs[0]['role'], 'SERVICE')
        self.assertEqual(outcomes[0]['outcome_status'], 'SUCCESS_WITH_LABEL')

    def test_empty_batch_without_list_does_not_guess_addresses(self):
        obs, outcomes = self.parse({'code': 200000, 'data': []})
        self.assertEqual(obs, [])
        self.assertEqual(outcomes[0]['outcome_status'], 'SUCCESS_EMPTY')
        self.assertIsNone(outcomes[0]['address'])

    def test_missing_member_is_failure_while_success_member_is_retained(self):
        obs, outcomes = self.parse(payload(), [A, B])
        self.assertEqual([o['address'] for o in obs], [A])
        self.assertTrue(any(o['address'] == B and o['request_status'] == 'FAILED' for o in outcomes))
        self.assertTrue(any(o['address'] == A and o['outcome_status'] == 'SUCCESS_EMPTY' for o in outcomes))
        self.assertTrue(any(o['scope'] == 'BATCH' and o['request_status'] == 'FAILED' for o in outcomes))

    def test_duplicate_wrong_chain_and_unexpected_members_not_adopted(self):
        p = payload(); p['data'] *= 2
        self.assertEqual(self.parse(p, [A])[0], [])
        p = payload(); p['data'][0]['chain_id'] = 56
        self.assertEqual(self.parse(p, [A])[0], [])
        self.assertEqual(self.parse(payload(B), [A])[0], [])

    def test_malformed_label_structure_fails_address(self):
        p = payload(); p['data'][0]['attributes'] = ['broken']
        obs, outcomes = self.parse(p, [A])
        self.assertEqual(obs, [])
        self.assertTrue(any(o['address'] == A and o['request_status'] == 'FAILED' for o in outcomes))

    def test_existing_success_not_overwritten_by_conflicting_id(self):
        old = observation(); changed = old | {'actor': 'Different'}
        with self.assertRaises(ValueError): merge_observations([old], [changed])
        self.assertEqual(merge_observations([old], []), [old])

    def test_cli_success_then_failure_preserves_success_bytes_and_returns_nonzero(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            root = Path(tmp); response = root / 'response.json'; metadata = root / 'metadata.json'
            dest = root / 'observations.json'; outcomes = root / 'outcomes.csv'
            response.write_text(json.dumps(payload(entity='Example Exchange')), encoding='utf-8')
            metadata.write_text(json.dumps({'requested_addresses': [A, B]}), encoding='utf-8')
            script = Path(__file__).resolve().parents[1] / 'src/labels_import.py'
            args = [sys.executable, '-B', str(script), '--payload', str(response), '--metadata', str(metadata),
                    '--raw-ref', 'synthetic-response', '--output', str(dest), '--outcomes', str(outcomes)]
            first = subprocess.run(args, capture_output=True, text=True)
            self.assertEqual(first.returncode, 2, first.stderr)
            original = dest.read_bytes(); self.assertEqual(len(json.loads(original)), 1)
            response.write_text(json.dumps({'code': 403, 'message': 'permission denied'}), encoding='utf-8')
            second = subprocess.run(args, capture_output=True, text=True)
            self.assertEqual(second.returncode, 2, second.stderr)
            self.assertEqual(dest.read_bytes(), original)
            with outcomes.open(encoding='utf-8', newline='') as stream: records = list(csv.DictReader(stream))
            self.assertTrue(any(r['request_status'] == 'SUCCESS' for r in records))
            self.assertTrue(any(r['address'] == B and r['request_status'] == 'FAILED' for r in records))

    def test_cli_valid_empty_response_exits_zero(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            root = Path(tmp); response = root / 'response.json'; metadata = root / 'metadata.json'
            response.write_text(json.dumps(payload()), encoding='utf-8')
            metadata.write_text(json.dumps({'requested_addresses': [A]}), encoding='utf-8')
            args = [sys.executable, '-B', str(Path(__file__).resolve().parents[1] / 'src/labels_import.py'),
                    '--payload', str(response), '--metadata', str(metadata), '--raw-ref', 'synthetic-response',
                    '--output', str(root/'obs.json'), '--outcomes', str(root/'outcomes.csv')]
            result = subprocess.run(args, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)['batch_status'], 'SUCCESS')


class HistoricalRemediationTests(unittest.TestCase):
    def test_materialized_missing_support_remains_gap_without_invented_id(self):
        record=normalized_record(baseline() | {'observation_ids':['old-existing']})
        after,changed,invalid,support=repair_record(record,{})
        self.assertEqual(after['actor'],record['actor'])
        self.assertEqual(after['adopted_observation_ids'],[])
        self.assertEqual(after['provenance_status'],'PROVENANCE_UNRESOLVED')
        self.assertEqual(invalid,['old-existing']);self.assertEqual(support,[])

    def test_materialized_pointer_repair_requires_matching_actor_role_and_listed_real_id(self):
        record=normalized_record(baseline() | {'adopted_observation_ids':['new-different'],
            'adopted_sources':['Professional'],'observation_ids':['old-existing','new-different']})
        old=observation() | {'explicit_claim':True}
        new=observation('Different Service','new-different','Professional') | {'explicit_claim':True}
        after,changed,invalid,support=repair_record(record,{'old-existing':old,'new-different':new})
        self.assertTrue(changed);self.assertEqual(after['adopted_observation_ids'],['old-existing'])
        self.assertEqual(after['adopted_sources'],['OldSource'])
        self.assertEqual(invalid,['new-different']);self.assertEqual(after['actor'],'Existing Service')

    def test_unlisted_observation_does_not_fill_historical_gap(self):
        record=normalized_record(baseline() | {'adopted_observation_ids':[],'observation_ids':[]})
        after,_,_,_=repair_record(record,{'old-existing':observation() | {'explicit_claim':True}})
        self.assertEqual(after['provenance_status'],'PROVENANCE_UNRESOLVED')

    def test_conflicted_identity_does_not_adopt_one_actor_as_resolution(self):
        record=normalized_record(baseline() | {'actor':'','identity_class':'CONFLICTED_IDENTITY'})
        after,changed,_,_=repair_record(record,{'old-existing':observation() | {'explicit_claim':True}})
        self.assertTrue(changed);self.assertEqual(after['adopted_observation_ids'],[])
        self.assertEqual(after['identity_class'],'CONFLICTED_IDENTITY')

    def test_history_scanner_reifies_failed_batch_and_repairs_only_failed_response_success_rows(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            root=Path(tmp);raw=root/'metasleuth_response.json';meta=root/'metasleuth_metadata.json'
            raw.write_text(json.dumps({'code':403,'message':'permission denied'}),encoding='utf-8')
            meta.write_text(json.dumps({'provider':'MetaSleuth','sha256':digest(raw),
                'payload':{'addresses':[A]},'http_status':200}),encoding='utf-8')
            old=root/'metasleuth_outcomes.csv'
            old.write_text('address,request_status,raw_response_sha256\n'+A+',SUCCESS,'+digest(raw)+'\n',encoding='utf-8')
            inventory=[{'path':p.name,'sha256':digest(p),'bytes':p.stat().st_size} for p in [raw,meta,old]]
            manifest=root/'inventory.json';manifest.write_text(json.dumps(inventory),encoding='utf-8')
            before=old.read_bytes();summary=run_meta(root,manifest,root/'corrected')
            self.assertEqual(summary['failed_saved_batches'],1)
            self.assertEqual(summary['false_success_materializations'],1)
            self.assertEqual(summary['new_meta_requests'],0)
            outcomes=json.loads((root/'corrected/F07_RECLASSIFIED_OUTCOMES.json').read_text(encoding='utf-8'))
            self.assertEqual({o['scope'] for o in outcomes},{'BATCH','ADDRESS'})
            self.assertEqual(old.read_bytes(),before)


if __name__ == '__main__': unittest.main()
