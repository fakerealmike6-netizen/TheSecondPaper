"""Synthetic only; existing adapter, durable retry and original HTTP receipts."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
CODE = BASE if (BASE/'src/provider_etherscan.py').is_file() else BASE.parents[2]/'code'
sys.path.insert(0,str(CODE/'src')); sys.path.insert(0,str(BASE/'src'))
from stage1d_etherscan_probe import AUTH, prepare_plan, probe_in_session, parameters, RAW_READ_LIMIT
from page_attempts import atomic_json


class Clock:
    def __init__(self): self.value=2000000000.; self.waits=[]
    def __call__(self): return self.value
    def sleep(self,n): self.waits.append(n); self.value+=n


class Ledger:
    def __init__(self): self.jobs={}; self.operations=5
    def reserve(self,job,provider,purpose,amounts):
        assert job not in self.jobs and amounts=={'rpc_operations':1}
        self.jobs[job]={'provider':provider,'reserved':dict(amounts)}
    def settle(self,job,amounts):
        assert amounts=={'rpc_operations':1}
        self.jobs[job]['actual']=dict(amounts); self.operations+=1
    def snapshot(self): return {'rpc_operations':{'cap':'10000','actual':str(self.operations),'overrun':False,'actual_exceeded_reservation':False}}


class Runtime:
    def __init__(self): self.ledger=Ledger()
    def ledger_factory(self,path): return self.ledger
    def require_gate(self,work): pass
    def raw_risk(self,work):
        return sum(p.stat().st_size for p in (work/'raw').rglob('*') if p.is_file())+sum(json.loads(p.read_bytes())['additional_raw_risk_bytes'] for p in (work/'private/context_uncertainty').glob('*.json'))


class AccountProbeTests(unittest.TestCase):
    def setUp(self):
        temporary=BASE/'.test_tmp'; temporary.mkdir(exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=temporary); self.addCleanup(self.temp.cleanup)
        self.w=Path(self.temp.name); self.runtime=Runtime(); self.clock=Clock(); self.calls=[]; self.replies={}
        self.addr='0x'+'1'*40
        interval={'address':self.addr,'start_block':100,'end_block':120,'query_id':'q0','kind':'context'}
        scope='scope:synthetic'; scope_hash='s'*64
        atomic_json(self.w/'private/BATCH_QUERY_FREEZE.json',{'queries':[{'query_id':'q'+str(i),'name':'synthetic'+str(i),'scope_id':scope,'scope_hash':scope_hash} for i in range(4)]})
        freeze=self.w/'private/synthetic_scope/freeze_manifest.json'; freeze.parent.mkdir(parents=True)
        sql=b'SELECT synthetic finite same-domain account context'; (freeze.parent/'query.sql').write_bytes(sql)
        atomic_json(freeze,{'schema_version':'stage1d-sql-freeze-v1','authorization_id':AUTH,'kind':'context',
            'sql_sha256':hashlib.sha256(sql).hexdigest(),'dependencies':[],'query_ids':['q0'],'scope_id':scope,'scope_hash':scope_hash,'intervals':[interval]})
        self.plan=prepare_plan(self.w,freeze,'q0',self.addr,100,120)
    def row(self,action,i=1):
        r={'blockNumber':'110','timeStamp':'1660000000','hash':'0x'+format(i,'064x'),'from':self.addr,'to':'0x'+'2'*40,
            'value':str(2**256-1),'isError':'0','transactionIndex':'1','gasUsed':'21000','gasPrice':'10','blockHash':'0x'+'3'*64,'txreceipt_status':'1'}
        if action=='txlistinternal': r['traceId']='0_'+str(i)
        return r
    def transport(self,params,bound):
        self.calls.append(dict(params)); self.assertEqual(bound,RAW_READ_LIMIT+1)
        self.assertTrue(any('actual' not in j for j in self.runtime.ledger.jobs.values()))
        if self.replies.get(params['action']):
            value=self.replies[params['action']].pop(0)
            if isinstance(value,Exception): raise value
            return value
        return 200,json.dumps({'status':'1','message':'OK','result':[self.row(params['action'])]}).encode(),{}
    def run_probe(self): return probe_in_session(self.w,self.plan,self.runtime,self.clock()+1000,transport=self.transport,clock=self.clock,sleep=self.clock.sleep)
    def test_two_endpoints_budgeted_before_dispatch_and_cached_without_new_op(self):
        result=self.run_probe(); self.assertEqual(result['status'],'TWO_ENDPOINT_PAGES_VALIDATED')
        self.assertEqual(self.runtime.ledger.operations,7); self.assertEqual(len(self.calls),2)
        self.assertEqual([r['action'] for r in result['results']],['txlist','txlistinternal'])
        self.assertFalse(result['results'][1]['canonical_internal_events_admitted'])
        self.assertIn('verified_canonical_trace_path',result['results'][1]['detail_gaps'])
        self.assertEqual(self.run_probe()['status'],'TWO_ENDPOINT_PAGES_VALIDATED'); self.assertEqual(len(self.calls),2)
        self.assertEqual(self.runtime.ledger.operations,7)
    def test_full_first_page_remains_incomplete(self):
        self.replies['txlist']=[(200,json.dumps({'status':'1','message':'OK','result':[self.row('txlist',i) for i in range(1000)]}).encode(),{})]
        row=self.run_probe()['results'][0]
        self.assertFalse(row['provider_enumeration_exhausted']); self.assertEqual(row['next_page'],2)
        self.assertFalse(row['full_chain_ledger_complete']); self.assertEqual(len(self.calls),2)
    def test_failure_is_not_empty_and_other_endpoint_continues(self):
        self.replies['txlist']=[(200,b'{"status":"0","message":"NOTOK","result":"invalid api key"}',{})]
        result=self.run_probe(); self.assertEqual(result['status'],'PARTIAL_CAPABILITY_EVIDENCE')
        self.assertEqual(result['results'][0]['endpoint_status'],'FAILED_OR_DEFERRED')
        self.assertEqual(result['results'][1]['endpoint_status'],'CURRENT_ENDPOINT_RETURNED_VALID_PAGE')
        self.assertEqual(len(self.calls),2)
        self.run_probe(); self.assertEqual(len(self.calls),2)
    def test_three_attempt_failure_chain_is_persistent(self):
        self.replies['txlist']=[TimeoutError(),TimeoutError(),TimeoutError()]
        self.assertEqual(self.run_probe()['status'],'PARTIAL_CAPABILITY_EVIDENCE')
        self.assertEqual(len(self.calls),4); self.assertEqual(self.runtime.ledger.operations,9)
        self.run_probe(); self.assertEqual(len(self.calls),4)
        self.assertEqual(len(list((self.w/'private/context_uncertainty').glob('*.json'))),3)
    def test_retry_after_and_successful_empty_have_separate_meanings(self):
        self.replies['txlist']=[(429,b'{}',{'Retry-After':'6'}),(200,b'{"status":"0","message":"No transactions found","result":[]}',{})]
        result=self.run_probe(); self.assertTrue(result['results'][0]['empty_success'])
        self.assertTrue(any(n>=6 for n in self.clock.waits)); self.assertEqual(len(self.calls),3)
    def test_bad_physical_selector_never_enters_success_cache(self):
        bad=self.row('txlist'); bad['blockNumber']='121'
        self.replies['txlist']=[(200,json.dumps({'status':'1','message':'OK','result':[bad]}).encode(),{})]
        self.assertEqual(self.run_probe()['results'][0]['endpoint_status'],'FAILED_OR_DEFERRED')
        self.run_probe(); self.assertEqual(len(self.calls),2)
    def test_original_raw_is_rechecked_on_cache(self):
        result=self.run_probe(); raw=self.w/result['results'][0]['original_http_receipt']['raw_path']; raw.write_bytes(raw.read_bytes()+b' ')
        self.assertEqual(self.run_probe()['results'][0]['endpoint_status'],'FAILED_OR_DEFERRED'); self.assertEqual(len(self.calls),2)
    def test_source_and_endpoint_changes_rejected_without_requests(self):
        self.plan['interval']['end_block']=121
        with self.assertRaises(ValueError): self.run_probe()
        with self.assertRaises(ValueError): parameters(self.plan,'getapilimit')
        self.assertFalse(self.calls)
    def test_credential_echo_withheld_with_conservative_raw_risk(self):
        secret='SYNTHETIC_SECRET_FOR_ECHO_TEST_ONLY'
        self.replies['txlist']=[(200,secret.encode(),{})]
        with patch.dict('os.environ',{'ETHERSCAN_API_KEY':secret}): result=self.run_probe()
        self.assertEqual(result['results'][0]['endpoint_status'],'FAILED_OR_DEFERRED')
        self.assertGreaterEqual(self.runtime.raw_risk(self.w),RAW_READ_LIMIT)
        for p in self.w.rglob('*'):
            if p.is_file() and p.suffix in ('.json','.bin'): self.assertNotIn(secret.encode(),p.read_bytes())


if __name__=='__main__': unittest.main()
