"""Synthetic-only offline frontier label and saved-page import regressions."""
import copy
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from frontier_labels_r1 import TABLES, addresses_in, build_sql, classify, observations_from_parsed, parse_result_rows, record_failure, saved_job_rows, select_registry_slice
from labels_policy import resolve_address
from reference_core import digest, dump, write_csv
from reference_recompute import rows

A='0x'+'1'*40
B='0x'+'2'*40

def rules():
    return dict(labels_addresses_mapping={'exact_record_rules':[
        {'category':'institution','label_type':'identifier','source':'static','classification':'CURATED_NAMED_CUSTODIAL_IF_NON_GENERIC_ACTOR'},
        {'category':'bridge','label_type':'identifier','source':'static','classification':'NAMED_BRIDGE'}]},
        owner_details_cache={'synthetic_exchange':{'name':'Synthetic Exchange','primary_category':'centralized exchange'}})

def record(alias,**kw):
    return dict.fromkeys(TABLES[alias][2]) | {'blockchain':'ethereum'} | kw

def result_row(address=A,**matches):
    row={'address':address}
    for alias in TABLES:
        values=matches.get(alias,[])
        row[alias+'_json']=json.dumps(values)
        row[alias+'_count']=len(values)
    return row

class FrontierLabelsR1Tests(unittest.TestCase):
    def test_aggregate_preserves_all_records_duplicates_and_nulls(self):
        match=record('cex_addresses',cex_name='Synthetic Exchange')
        parsed=parse_result_rows([result_row(cex_addresses=[match,match])],[A])
        self.assertEqual(parsed[A]['cex.addresses'],[match,match])
        self.assertIsNone(parsed[A]['cex.addresses'][0]['added_by'])

    def test_count_truncation_fails_closed(self):
        row=result_row();row['cex_addresses_count']=1
        with self.assertRaisesRegex(ValueError,'Truncated'):parse_result_rows([row],[A])

    def test_missing_fourth_source_fails_closed(self):
        row=result_row();del row['deposit_addresses_json']
        with self.assertRaises(ValueError):parse_result_rows([row],[A])

    def test_missing_and_duplicate_addresses_fail_closed(self):
        for value,expected in [([result_row()],[A,B]),([result_row(),result_row()],[A]),([result_row(B)],[A])]:
            with self.assertRaises(ValueError):parse_result_rows(value,expected)

    def test_source_fields_and_chain_verified(self):
        for patch in ({'blockchain':'base'},{'cex_name':14},{'unexpected':'x'}):
            with self.assertRaises(ValueError):parse_result_rows([result_row(cex_addresses=[record('cex_addresses',**patch)])],[A])

    def test_empty_success_is_durable_four_table_opportunity(self):
        parsed=parse_result_rows([result_row()],[A])
        obs,opps=observations_from_parsed(parsed,rules(),'SYNTHETIC','2026-01-01T00:00:00Z','synthetic.json','0'*64)
        self.assertEqual(len(opps),4);self.assertEqual(len(obs),4)
        self.assertTrue(all('NO_MATCH' in r['status'] for r in opps))
        self.assertEqual(resolve_address(obs)['identity_class'],'UNKNOWN')
        self.assertEqual(len({o['observation_id'] for o in obs}),4)

    def test_static_named_cex_keeps_method_undisclosed(self):
        out=classify('cex.addresses',record('cex_addresses',cex_name='Synthetic Exchange'),rules())
        self.assertEqual((out['actor'],out['role']),('Synthetic Exchange','SERVICE'))
        self.assertIsNone(out['generation_mechanism'])

    def test_persona_and_usage_cannot_become_control(self):
        for typ in ('persona','usage'):
            out=classify('labels.addresses',record('labels_addresses',name='Synthetic Exchange',category='institution',source='query',label_type=typ),rules())
            self.assertEqual(out['semantic_kind'],'BEHAVIOR');self.assertEqual(out['role'],'UNKNOWN')

    def test_deployer_cannot_become_owner(self):
        out=classify('cex.addresses',record('cex_addresses',cex_name='Synthetic Exchange',distinct_name='Contract deployer'),rules())
        self.assertEqual(out['role'],'UNKNOWN');self.assertEqual(out['semantic_kind'],'BEHAVIOR')

    def test_generic_institution_identifier_cannot_become_service(self):
        out=classify('labels.addresses',record('labels_addresses',name='Institution',category='institution',source='static',label_type='identifier'),rules())
        self.assertEqual(out['role'],'UNKNOWN')

    def test_programmatic_deposit_keeps_disclosed_method(self):
        out=classify('cex.deposit_addresses',record('deposit_addresses',cex_name='Synthetic Exchange'),rules())
        self.assertEqual(out['generation_mechanism'],'ALGORITHM_IDENTIFIED')
        self.assertIn('NOT_MANUAL',out['classification_rule'])

    def test_unknown_owner_details_do_not_infer_service(self):
        out=classify('labels.owner_addresses',record('owner_addresses',owner_key='unseen_exchange',custody_owner='Unseen Exchange'),rules())
        self.assertEqual(out['role'],'UNKNOWN');self.assertTrue(out['owner_metadata_missing'])

    def test_owner_internal_conflict_retained(self):
        out=classify('labels.owner_addresses',record('owner_addresses',owner_key='synthetic_exchange',custody_owner='Synthetic Exchange',account_owner='Other Exchange'),rules())
        self.assertTrue(out['record_conflict'])
        parsed=parse_result_rows([result_row(owner_addresses=[record('owner_addresses',owner_key='synthetic_exchange',custody_owner='Synthetic Exchange',account_owner='Other Exchange')])],[A])
        obs,_=observations_from_parsed(parsed,rules(),'SYNTHETIC','2026-01-01T00:00:00Z','synthetic.json','0'*64)
        self.assertEqual(resolve_address(obs)['identity_class'],'CONFLICTED_IDENTITY')

    def test_cross_source_conflict_retained_with_dune_priority(self):
        parsed=parse_result_rows([result_row(cex_addresses=[record('cex_addresses',cex_name='Synthetic Exchange')])],[A])
        obs,_=observations_from_parsed(parsed,rules(),'SYNTHETIC','2026-01-01T00:00:00Z','synthetic.json','0'*64)
        other=dict(obs[0],platform='Other Professional',actor='Other Exchange',observation_id='synthetic-other',semantic_kind='ATTRIBUTION',role='SERVICE')
        adopted=resolve_address(obs+[other])
        self.assertEqual(adopted['actor'],'Synthetic Exchange');self.assertTrue(adopted['preserved_conflict'])

    def test_sql_schema_verified_and_all_matches_aggregated(self):
        schema=[]
        for table,_,fields in TABLES.values():
            schema.extend(dict(table_schema=table.split('.')[0],table_name=table.split('.')[1],column_name=f,
                               data_type='varbinary' if f=='first_deposit_token_address' else 'varchar') for f in fields)
        sql=build_sql([A,B],schema);code='\n'.join(l for l in sql.splitlines() if not l.startswith('--'))
        self.assertNotIn('LIMIT',code);self.assertNotIn('DISTINCT',code)
        self.assertEqual(code.count('array_agg('),4);self.assertEqual(code.count('INNER JOIN frontier'),4)
        self.assertIn('to_hex(s.first_deposit_token_address)',code)
        schema[0]['data_type']='array(varchar)'
        with self.assertRaises(ValueError):build_sql([A],schema)

    def make_job(self,base):
        work=Path(base);batch=work/'batch';jobdir=work/'job';batch.mkdir();jobdir.mkdir()
        sql='-- synthetic\nSELECT 1\n';(jobdir/'query.sql').write_bytes(sql.encode('utf-8'))
        (batch/'combined_frontier_labels.sql').write_bytes(sql.encode('utf-8'))
        row=result_row();page=dict(execution_id='SYNTHETIC',state='QUERY_STATE_COMPLETED',result=dict(rows=[row],metadata=dict(row_count=1,total_row_count=1)))
        raw=work/'raw.json';dump(raw,page);dump(jobdir/'page_0.json',page)
        receipt=dict(http_status=200,error_class=None,execution_id='SYNTHETIC',parameters=dict(limit=50,offset=0),raw_path='raw.json',sha256=digest(raw))
        dump(jobdir/'page_0_receipt.json',receipt)
        job=dict(state='QUERY_STATE_COMPLETED',execution_id='SYNTHETIC',sql_sha256=digest(jobdir/'query.sql'),export_requests=1,export_offsets=[0],
            status_response=dict(state='QUERY_STATE_COMPLETED',execution_id='SYNTHETIC',result_metadata=dict(total_row_count=1)))
        dump(jobdir/'job.json',job)
        dump(batch/'manifest.json',dict(frozen_files=[dict(name='combined_frontier_labels.sql',sha256=digest(batch/'combined_frontier_labels.sql'))],sql_sha256=digest(jobdir/'query.sql'),external_addresses=[A]))
        return work,batch,jobdir

    def test_saved_raw_receipts_complete_pagination_import(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as td:
            work,batch,job=self.make_job(td)
            parsed,_,evidence=saved_job_rows(work,batch,job)
            self.assertEqual(set(parsed),{A});self.assertEqual(len(evidence),1)

    def test_same_sql_new_authorization_job_id_and_directory_imports(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as td:
            work,batch,job=self.make_job(td)
            state=json.loads((job/'job.json').read_text(encoding='utf-8'))
            state.update(logical_job_id='EXPLICIT_NEW_AUTHORIZATION',authorization_id='SYNTHETIC_SINGLE_JOB_CAP5')
            dump(job/'job.json',state)
            # Directory is "job", not SQL SHA, and logical job id is a grant.
            parsed,loaded,_=saved_job_rows(work,batch,job)
            self.assertEqual(set(parsed),{A});self.assertEqual(loaded['logical_job_id'],'EXPLICIT_NEW_AUTHORIZATION')

    def test_saved_page_tamper_fails_closed(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as td:
            work,batch,job=self.make_job(td)
            page=json.loads((job/'page_0.json').read_text(encoding='utf-8'));page['result']['rows'][0]['address']=B
            dump(job/'page_0.json',page)
            with self.assertRaisesRegex(ValueError,'does not match'):saved_job_rows(work,batch,job)

    def test_partial_or_changed_sql_cannot_import(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as td:
            work,batch,job=self.make_job(td)
            state=json.loads((job/'job.json').read_text(encoding='utf-8'));state['export_requests']=0;state['export_offsets']=[]
            dump(job/'job.json',state)
            with self.assertRaisesRegex(ValueError,'pagination not complete'):saved_job_rows(work,batch,job)
            (job/'query.sql').write_text('-- replacement\nSELECT 2\n',encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'frozen queue SQL'):saved_job_rows(work,batch,job)

    def make_failures(self,base,count=2):
        root=Path(base);work=root/'revision';work.mkdir();snapshot=root/'original_snapshot';snapshot.mkdir()
        oldobs,_=observations_from_parsed(parse_result_rows([result_row(B)],[B]),rules(),'OLD_SYNTHETIC','2026-01-01T00:00:00Z','old_synthetic.json','0'*64)
        write_csv(snapshot/'label_observations.csv.gz',oldobs)
        write_csv(snapshot/'address_registry.csv.gz',[dict(address=B,chain_id='1',identity_class='UNKNOWN',actor='',observation_count=4)])
        dump(snapshot/'label_policy.json',{})
        first=work/'derived/frontier_labels/batch_01';first.mkdir(parents=True)
        dump(first/'queue.json',dict(external_addresses=[A]))
        (first/'combined_frontier_labels.sql').write_bytes(b'-- frozen synthetic\nSELECT 1\n')
        manifest=dict(queue_sha256=digest(first/'queue.json'),external_addresses=[A],base_snapshot='original_snapshot',
            inherited_registry_sha256=digest(snapshot/'address_registry.csv.gz'),inherited_observation_sha256=digest(snapshot/'label_observations.csv.gz'),
            frozen_files=[dict(name=p.name,sha256=digest(p)) for p in first.iterdir()])
        dump(first/'manifest.json',manifest)
        jobs=[]
        for number in range(count):
            folder=work/f'job{number}';folder.mkdir();jobs.append(folder)
            (folder/'query.sql').write_bytes((first/'combined_frontier_labels.sql').read_bytes())
            status=dict(state='QUERY_STATE_FAILED',execution_id=f'SYNTHETIC{number}',execution_cost_credits=5 if number==2 else 1,
                error=dict(type='FAILED_TYPE_RESOURCES_CAP_REACHED',message='synthetic execution cap'))
            raw=work/f'raw{number}.json';dump(raw,status)
            receipt=dict(http_status=200,error_class=None,raw_path=raw.name,sha256=digest(raw))
            dump(folder/'job.json',dict(state='QUERY_STATE_FAILED',execution_id=status['execution_id'],status_response=status,status_receipt=receipt,
                export_requests=0,export_offsets=[],scope_freeze_path=str(first/'manifest.json'),scope_freeze_sha256=digest(first/'manifest.json'),
                sql_sha256=digest(folder/'query.sql'),authorization_id='SYNTHETIC_EXPLICIT_CAP5' if number==2 else 'SYNTHETIC_ORIGINAL',
                logical_job_id=f'SYNTHETIC_JOB_{number}',reserved_execution='5' if number==2 else '1'))
        return root,work,snapshot,jobs

    def test_two_failed_queries_add_only_unknown_context_not_no_match(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as td:
            root,work,snapshot,jobs=self.make_failures(td);oldsha=digest(snapshot/'label_observations.csv.gz')
            with patch('frontier_labels_r1.reference_delta',return_value={'synthetic_only':True}) as replay:
                result=record_failure(work,root,jobs)
                self.assertTrue(replay.call_args.kwargs['verify_unchanged'])
            self.assertEqual(result['new_observation_rows'],0);self.assertEqual(result['successful_source_opportunities'],0)
            self.assertEqual(result['observations_sha256'],oldsha);self.assertEqual(digest(snapshot/'label_observations.csv.gz'),oldsha)
            newdir=root/result['label_snapshot_path'];self.assertFalse((newdir/'label_observations.csv.gz').exists())
            newrow=next(r for r in rows(newdir/'address_registry.csv.gz') if r['address']==A)
            self.assertEqual(newrow['identity_class'],'UNKNOWN');self.assertEqual(newrow['lookup_status'],'LOOKUP_FAILED_RESOURCE_CAP')
            self.assertEqual(newrow['observation_count'],'0')
            opportunities=rows(work/'derived/frontier_labels/failed_lookup_revision_01/source_opportunities.csv')
            self.assertEqual(len(opportunities),4)
            self.assertTrue(all(r['status']=='FAILED_RESOURCE_CAP' and r['match_count']=='' for r in opportunities))

    def test_failure_raw_tamper_and_duplicate_execution_rejected(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as td:
            root,work,_,jobs=self.make_failures(td)
            with self.assertRaisesRegex(ValueError,'Duplicate'):record_failure(work,root,[jobs[0],jobs[0]])
            (work/'raw1.json').write_bytes(b'{}')
            with self.assertRaisesRegex(ValueError,'raw status evidence'):record_failure(work,root,jobs)
            self.assertFalse((work/'derived/label_snapshots').exists())

    def test_failed_result_cannot_certify_empty_source(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as td:
            work,batch,job=self.make_job(td)
            state=json.loads((job/'job.json').read_text(encoding='utf-8'));state['state']='QUERY_STATE_FAILED';dump(job/'job.json',state)
            with self.assertRaisesRegex(ValueError,'not completed'):saved_job_rows(work,batch,job)

    def test_third_failed_execution_creates_new_revision_preserves_first(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as td:
            root,work,_,jobs=self.make_failures(td,count=3)
            with patch('frontier_labels_r1.reference_delta',return_value={'synthetic_only':True}):
                original=record_failure(work,root,jobs[:2])
                first=work/'derived/frontier_labels/failed_lookup_revision_01/apply_manifest.json';originalsha=digest(first)
                # New transport stores scope paths relative to WORK.
                state=json.loads((jobs[2]/'job.json').read_text(encoding='utf-8'))
                state['scope_freeze_path']=str(Path(state['scope_freeze_path']).relative_to(work))
                dump(jobs[2]/'job.json',state)
                new=record_failure(work,root,jobs)
            self.assertEqual(new['actual_sql_attempts'],3);self.assertEqual(new['new_observation_rows'],0)
            self.assertEqual(digest(first),originalsha);self.assertNotEqual(original['label_snapshot_version'],new['label_snapshot_version'])
            self.assertTrue((work/'derived/frontier_labels/failed_lookup_revision_02/apply_manifest.json').exists())
            self.assertEqual(new['source_evidence'][2]['execution_cost_credits_observed'],'5')
            self.assertEqual(new['source_evidence'][2]['authorization_id'],'SYNTHETIC_EXPLICIT_CAP5')

    def test_registry_slice_preserves_failed_status_and_missing_rows(self):
        registry=[dict(address=A,identity_class='UNKNOWN',lookup_status='LOOKUP_FAILED_RESOURCE_CAP',actor=''),
                  dict(address=B,identity_class='SERVICE',actor='Synthetic Exchange')]
        unseen='0x'+'3'*40
        selected,missing=select_registry_slice(registry,{A,unseen})
        self.assertEqual(selected,[registry[0]]);self.assertEqual(missing,[unseen])
        self.assertEqual(registry[0]['lookup_status'],'LOOKUP_FAILED_RESOURCE_CAP')

    def test_portable_address_selection_is_structural(self):
        value=dict(states=[dict(state=dict(address=A,arrival=dict(sender=B,recipient=A)))],
                   target_address=B,raw_label='0x'+'3'*40,tx_hash='0x'+'4'*64)
        self.assertEqual(addresses_in(value),{A,B})

if __name__=='__main__':unittest.main()
