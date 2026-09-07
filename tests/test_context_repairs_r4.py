"""F01-F03 desired behavior, preserving the review's original synthetic facts."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone

from exact_fields_r4 import ExactFieldError, exact_uint, field_uint, field_status
from context_ledger_r3 import normalize_rows, assemble_model, coverage_complete
from context_queries_r3 import build, date_contract, verify_frozen_scope
from test_context_ledger_r3 import A, B, S, T, tx, trace, fixture, anchor, REQUIRED


def stamp(text):
    return int(datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp())


def header(number, date):
    return {'block_number': number, 'block_hash': '0x'+format(number, '064x'),
            'timestamp': stamp(date), 'evidence_ids': ['SYNTHETIC_BOUND_HEADER']}


def query(start=99, end=102):
    return {'rows': [{'address': A, 'role': 'NON_TERMINAL_MODEL_ACCOUNT',
                     'ledger_start_block': start, 'ledger_end_block': end,
                     'before_anchor_block': start-1, 'after_anchor_block': end,
                     'first_candidate_timestamp': stamp('2023-09-02T00:00:12Z'),
                     'last_candidate_timestamp': stamp('2023-09-02T00:00:24Z')}]}


class ContextExactValueR4Tests(unittest.TestCase):
    def test_legal_uint_minimal_units_and_hex(self):
        for value, expected in [(0,0),(1,1),('0',0),('1',1),('0x0',0),('0x1',1),('0Xff',255)]:
            with self.subTest(value=value): self.assertEqual(exact_uint(value), expected)

    def test_missing_null_invalid_states_distinct(self):
        for row, reason in [({},'MISSING'),({'value':None},'NULL'),({'value':True},'INVALID'),({'value':1.0},'INVALID'),({'value':7.9},'INVALID'),({'value':'-1'},'INVALID')]:
            with self.subTest(row=row), self.assertRaises(ExactFieldError) as caught:
                field_uint(row, ('value',))
            self.assertEqual(caught.exception.reason_code, reason)

    def test_uint_and_status_alias_conflicts(self):
        self.assertEqual(field_uint({'a':'1','b':'0x1'},('a','b')),1)
        with self.assertRaises(ExactFieldError): field_uint({'a':'1','b':'2'},('a','b'))
        with self.assertRaises(ExactFieldError): field_status({'success':True,'status':'0'},('success','status'))
        with self.assertRaises(ExactFieldError): field_status({'status':1.0},('status',))

    def test_value_records_do_not_default_amount(self):
        for kind in ('transaction','trace'):
            for bad in (None,True,False,1.0,7.9,'1.0','-1'):
                row = tx('a') if kind=='transaction' else trace('a',[],S,A,80)
                row['value_raw']=bad
                with self.subTest(kind=kind,bad=bad), self.assertRaises(ValueError): normalize_rows([row])
            row=tx('a') if kind=='transaction' else trace('a',[],S,A,80)
            del row['value_raw']
            with self.assertRaisesRegex(ValueError,'MISSING'): normalize_rows([row])

    def test_zero_value_is_known_zero_not_missing(self):
        result=normalize_rows([tx('a',value='0')])
        self.assertEqual(result['transactions'][0]['amount_raw'],'0')
        self.assertFalse(result['conflicts'])
        self.assertFalse(result['flows'])

    def test_receipt_without_value_merges_fee_status_and_no_double_flow(self):
        top=tx('a',fee=2)
        top.pop('fee_raw');top.pop('gas_used');top['success']=None
        receipt={'record_type':'receipt','transactionHash':top['tx_hash'],'blockNumber':'0xa','transactionIndex':'0x0','from':S,'to':A,'status':'0x1','gasUsed':'0x1','effectiveGasPrice':'0x2'}
        result=normalize_rows([receipt,top])
        self.assertFalse(result['conflicts'])
        self.assertEqual(len(result['flows']),1)
        self.assertEqual(result['flows'][0]['amount_raw'],'80')
        self.assertEqual(result['transactions'][0]['fee_raw'],'2')

    def test_receipt_only_is_non_value_evidence(self):
        result=normalize_rows([{'record_type':'receipt','transactionHash':'0x'+'a'*64,'status':'0x1','gasUsed':'0x5208','effectiveGasPrice':'0x1'}])
        self.assertFalse(result['flows'])
        self.assertEqual(result['excluded'][0]['reason'],'RECEIPT_ONLY_NON_VALUE_EVIDENCE')

    def test_receipt_conflicting_status_or_position_not_overwritten(self):
        top=tx('a')
        for patch in ({'status':'0x0'},{'blockNumber':'0xb'},{'transactionIndex':'0x1'}):
            receipt={'record_type':'receipt','transactionHash':top['tx_hash'],**patch}
            with self.subTest(patch=patch):self.assertTrue(normalize_rows([top,receipt])['conflicts'])

    def test_unknown_top_success_is_not_success(self):
        row=tx('a');row.pop('success')
        result=normalize_rows([row])
        self.assertFalse(result['flows'])
        self.assertIsNone(result['transactions'][0]['success'])

    def test_original_cancelling_background_flow_counterexample(self):
        # Exactly the review's 80 seed +100 background in -100 out ->90 target.
        rows=[tx('a',value=80),tx('c',index=1,sender=B,recipient=A,value=100),tx('d',index=2,sender=A,recipient=B,value=100),tx('b',block=11,sender=A,recipient=T,value=90)]
        from context_lp_r3 import run_context_document
        result=run_context_document(fixture(rows=rows,before=20,after=10)['model_input'])
        joint=result['variants']['BEST_AVAILABLE_CONTEXT']['all_service_joint']
        self.assertEqual((joint['lower_raw'],joint['upper_raw']),('0','80'))
        for missing in (True,False):
            bad=copy.deepcopy(rows)
            for row in bad[1:3]:
                if missing:row.pop('value_raw')
                else:row['value_raw']=None
            with self.assertRaises(ValueError):fixture(rows=bad,before=20,after=10)


class ContextRootConsistencyR4Tests(unittest.TestCase):
    def test_top_success_root_failed_conflicts_before_filter(self):
        top=tx('a');root=trace('a',[],S,A,80,success=False)
        result=normalize_rows([top,root])
        self.assertTrue(result['conflicts']);self.assertFalse(result['flows'])
        assembled=fixture(rows=[top,root,tx('b',block=11,sender=A,recipient=T,value=90)])
        self.assertEqual(assembled['completion_status'],'EVIDENCE_CONFLICT_MODEL_BLOCKED')
        from context_lp_r3 import build_context_model
        with self.assertRaisesRegex(ValueError,'EVIDENCE_CONFLICT_MODEL_BLOCKED'):build_context_model(assembled['model_input'])

    def test_success_top_failed_nested_is_valid(self):
        root=trace('a',[],S,A,80);root['subtraces']=1;root['tx_success']=True
        child=trace('a',[0],A,B,20,success=False);child['tx_success']=True;child['subtraces']=0
        result=normalize_rows([tx('a'),root,child])
        self.assertFalse(result['conflicts']);self.assertEqual(len(result['flows']),1)

    def test_failed_top_failed_root_keeps_fee_not_value(self):
        result=normalize_rows([tx('a',success=False,fee=2),trace('a',[],S,A,80,success=False)])
        self.assertFalse(result['flows']);self.assertFalse(result['conflicts'])
        self.assertEqual(result['transactions'][0]['fee_raw'],'2')

    def test_root_identity_and_amount_mismatches_detected(self):
        for patch in ({'value_raw':'81'},{'block_number':11},{'tx_index':1},{'to_address':B},{'block_hash':'0x'+'f'*64}):
            root=trace('a',[],S,A,80);root.update(patch)
            with self.subTest(patch=patch):self.assertTrue(normalize_rows([tx('a'),root])['conflicts'])

    def test_unknown_root_and_nested_success_leave_evidence_gap(self):
        rows=[tx('a'),trace('a',[],S,A,80)]
        rows[1].pop('success')
        result=normalize_rows(rows)
        self.assertFalse(result['conflicts'])
        self.assertTrue(any(r['reason']=='ROOT_SUCCESS_EVIDENCE_MISSING' for r in result['excluded']))


class ContextDatesR4Tests(unittest.TestCase):
    def test_candidate_timestamps_alone_cannot_prove_ledger_date(self):
        with self.assertRaisesRegex(ValueError,'BOUNDARY_TIME_EVIDENCE_MISSING'):build(query())

    def test_forward_backward_multiday_and_midnight_dates(self):
        for first,last,dates in [
            ('2023-09-02T00:00:00Z','2023-09-02T00:00:24Z',('2023-09-02','2023-09-02')),
            ('2023-09-01T23:59:48Z','2023-09-02T00:00:24Z',('2023-09-01','2023-09-02')),
            ('2023-09-02T23:59:48Z','2023-09-03T00:00:00Z',('2023-09-02','2023-09-03')),
            ('2023-08-31T23:59:48Z','2023-09-04T00:00:00Z',('2023-08-31','2023-09-04'))]:
            headers={99:header(99,first),102:header(102,last)}
            with self.subTest(first=first,last=last):
                sql=build(query(),block_headers=headers)
                self.assertIn(f"block_date BETWEEN DATE '{dates[0]}' AND DATE '{dates[1]}'",sql)
                self.assertIn('block_number BETWEEN 99 AND 102',sql)

    def test_different_account_windows_use_full_envelope(self):
        q=query();q['rows'].append({**q['rows'][0],'address':B,'ledger_start_block':110,'ledger_end_block':120})
        headers={99:header(99,'2023-09-01T23:59:48Z'),102:header(102,'2023-09-02T00:00:24Z'),110:header(110,'2023-09-03T00:00:00Z'),120:header(120,'2023-09-04T00:00:00Z')}
        self.assertIn("DATE '2023-09-01' AND DATE '2023-09-04'",build(q,block_headers=headers))

    def test_bad_timestamp_order_rejected(self):
        with self.assertRaises(ValueError):build(query(),block_headers={99:header(99,'2023-09-03T00:00:00Z'),102:header(102,'2023-09-02T00:00:00Z')})

    def test_saved_sql_date_tamper_even_with_new_hash_is_rejected(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            root=Path(temporary);q=query();headers={99:header(99,'2023-09-01T23:59:48Z'),102:header(102,'2023-09-02T00:00:24Z')}
            sql=build(q,block_headers=headers)
            frozen={'schema_version':'stage1b-r4-context-query-v1','account_windows':q['rows'],'coverage_contract':date_contract(q,headers),'sql_sha256':hashlib.sha256(sql.encode()).hexdigest()}
            (root/'query.sql').write_text(sql,encoding='utf-8',newline='\n');(root/'freeze_manifest.json').write_text(json.dumps(frozen),encoding='utf-8')
            self.assertEqual(verify_frozen_scope(root/'freeze_manifest.json',root,block_headers=headers)['status'],'PASS')
            bad=sql.replace("DATE '2023-09-01'","DATE '2023-09-02'")
            frozen['sql_sha256']=hashlib.sha256(bad.encode()).hexdigest()
            (root/'query.sql').write_text(bad,encoding='utf-8',newline='\n');(root/'freeze_manifest.json').write_text(json.dumps(frozen),encoding='utf-8')
            with self.assertRaises(ValueError):verify_frozen_scope(root/'freeze_manifest.json',root,block_headers=headers)

    def test_legacy_sql_requires_independent_bracket_proof(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            root=Path(temporary);q=query();headers={99:header(99,'2023-09-01T23:59:48Z'),102:header(102,'2023-09-02T00:00:24Z')}
            sql=build(q,block_headers=headers)
            frozen={'schema_version':'stage1b-r3-context-query-v1','account_windows':q['rows'],'sql_sha256':hashlib.sha256(sql.encode()).hexdigest()}
            (root/'query.sql').write_text(sql,encoding='utf-8',newline='\n');(root/'freeze_manifest.json').write_text(json.dumps(frozen),encoding='utf-8')
            self.assertEqual(verify_frozen_scope(root/'freeze_manifest.json',root,block_headers=headers)['compatibility_mode'],'LEGACY_R3_VERIFIED_WITH_SAVED_BLOCKS')
            bad=sql.replace("DATE '2023-09-01'","DATE '2023-09-02'");frozen['sql_sha256']=hashlib.sha256(bad.encode()).hexdigest()
            (root/'query.sql').write_text(bad,encoding='utf-8',newline='\n');(root/'freeze_manifest.json').write_text(json.dumps(frozen),encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'SQL_DATE_DOMAIN'):verify_frozen_scope(root/'freeze_manifest.json',root,block_headers=headers)

    def test_provider_coverage_needs_both_domains(self):
        coverage=[{'address':A,'data_type':'top','start_block':99,'end_block':102,'status':'COMPLETE','pagination_complete':True,'evidence_ids':['page'],'provider_frozen_scope':True,'date_domain_verified':False,'block_domain_verified':True}]
        self.assertFalse(coverage_complete(coverage,A,99,102,['top'])[0])
        coverage[0]['date_domain_verified']=True
        self.assertTrue(coverage_complete(coverage,A,99,102,['top'])[0])

    def test_freeze_cli_uses_actual_saved_header_evidence(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            root=Path(temporary)
            def save(path, value):
                target=root/path;target.parent.mkdir(parents=True,exist_ok=True)
                target.write_text(json.dumps(value),encoding='utf-8',newline='\n')
                return hashlib.sha256(target.read_bytes()).hexdigest()
            q=query();q.update(name='synthetic_context',query_id='synthetic',graph_identity={'sha256':'0'*64})
            save('derived/CONTEXT_TARGETS.json',{'queries':[q]})
            cli=Path(__file__).resolve().parents[1]/'src/context_queries_r3.py'
            command=[sys.executable,str(cli),'--work',str(root)]
            failed=subprocess.run(command,capture_output=True,text=True)
            self.assertNotEqual(failed.returncode,0)
            self.assertIn('BOUNDARY_TIME_EVIDENCE_MISSING',failed.stderr)
            requests=[];responses=[]
            for number,date in [(99,'2023-09-01T23:59:48Z'),(102,'2023-09-02T00:00:24Z')]:
                requests.append({'id':number,'method':'eth_getBlockByNumber','params':[hex(number),False]})
                responses.append({'id':number,'result':{'number':hex(number),'hash':'0x'+format(number,'064x'),'timestamp':hex(stamp(date))}})
            batch='raw/rpc_r3/synthetic'
            wire_sha=save(batch+'/response_body.bin',responses)
            save(batch+'/dispatch_intent.json',{'requests':requests})
            members=[]
            for request,response in zip(requests,responses):
                path=batch+'/envelope_'+str(request['id'])+'.json'
                digest=save(path,{'request':request,'response':response,'status':'SUCCESS_VALIDATED'})
                members.append({'artifact_path':path,'artifact_sha256':digest})
            save(batch+'/receipt.json',{'http_status':200,'raw_path':batch+'/response_body.bin','raw_sha256':wire_sha,'raw_bytes':(root/batch/'response_body.bin').stat().st_size,'members':members})
            passed=subprocess.run(command,capture_output=True,text=True)
            self.assertEqual(passed.returncode,0,passed.stderr)
            result=verify_frozen_scope(root/'private/context_queries/synthetic_context/freeze_manifest.json',root)
            self.assertEqual(result['sql_dates'],['2023-09-01','2023-09-02'])


if __name__=='__main__':unittest.main()
