"""Synthetic tests only; no Dune API, tokens, accounts or real result caches."""
import sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from provider_dune import DuneProvider,build_interval_sql,normalize_rows
from collector import NATIVE

A='0x'+'1'*40;B='0x'+'2'*40;TOKEN='0x'+'3'*40;TX='0x'+'4'*64

def row(kind='top',**kwargs):
    return dict(event_kind=kind,tx_hash=TX,sender=A,recipient=B,amount_raw='100000000000000000001',block_number=100,tx_index=1,block_time='2023-01-01 00:00:00.000 UTC',log_index=None,trace_address=None,success=True,contract_address=None,gas_used='21000' if kind=='top' else None,gas_price='7' if kind=='top' else None,trace_type=None,call_type=None)|kwargs

def page(rows,total,next_offset=None):
    value={'execution_id':'SYNTHETIC_EXECUTION','state':'QUERY_STATE_COMPLETED','result':{'rows':rows,'metadata':{'row_count':len(rows),'total_row_count':total}}}
    if next_offset is not None:value['next_offset']=next_offset
    return value

class DuneAdapterTests(unittest.TestCase):
    def invoke(self,provider):
        return provider.fetch_interval(A,NATIVE,100,200,start_time=1672531200,end_time=1672534800,global_end_time=1672534800)
    def test_sql_uses_actual_address_and_all_three_sources(self):
        sql=build_interval_sql(A,NATIVE,100,200,start_time=1672531200,end_time=1672534800)
        for table in ('ethereum.transactions','ethereum.traces','erc20_ethereum.evt_Transfer'):self.assertIn(table,sql)
        self.assertNotIn('LIMIT',sql.upper());self.assertNotIn('target',sql.lower())
        self.assertIn('BETWEEN 100 AND 200',sql);self.assertIn('NOT EXISTS',sql);self.assertIn('cardinality(t.trace_address)>0',sql)
        with self.assertRaises(ValueError):build_interval_sql(A+"';DROP",NATIVE,1,2,start_time=1,end_time=2)
    def test_token_filter_retains_native_context(self):
        sql=build_interval_sql(A,'erc20:eip155:1:'+TOKEN,100,200,start_time=1,end_time=2)
        self.assertIn('e.contract_address = '+TOKEN,sql);self.assertIn('gas_used',sql)
    def test_raw_integer_trace_index_and_failed_gas(self):
        es,gaps=normalize_rows([row(success=False),row('internal',trace_address=[1,0],call_type='call'),row('erc20',contract_address=TOKEN,log_index=14)])
        self.assertFalse(gaps);self.assertEqual(es[0].amount_raw,100000000000000000001)
        self.assertFalse(es[0].success);self.assertEqual(es[0].gas_raw,147000)
        self.assertEqual(es[1].trace_address,'1_0');self.assertEqual(es[2].event_id,'eip155:1:tx:'+TX+':log:14')
    def test_missing_success_or_exact_event_is_gap(self):
        es,gaps=normalize_rows([row(success=None),row('erc20',contract_address=TOKEN,log_index=None),row(amount_raw=1.25)])
        self.assertFalse(es);self.assertEqual(len(gaps),3)
    def test_two_pages_and_restart_reuse_execution(self):
        calls=[]
        def execute(sql,job):calls.append(('exec',job));return {'execution_id':'SYNTHETIC_EXECUTION','state':'QUERY_STATE_COMPLETED'}
        def export(eid,params,job):
            calls.append(('page',params['offset']));self.assertEqual(params['limit'],1)
            return page([row()],2,1) if params['offset']==0 else page([row('erc20',contract_address=TOKEN,log_index=1)],2)
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            first=self.invoke(DuneProvider(execute,export,tmp,page_size=1));self.assertTrue(first.complete);self.assertEqual(len(first.events),2)
            second=self.invoke(DuneProvider(None,None,tmp,page_size=1,replay_only=True));self.assertTrue(second.complete);self.assertEqual(second.real_requests,0);self.assertEqual(len(calls),3)
    def test_terminal_count_mismatch_is_not_empty_complete(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            p=DuneProvider(lambda *_:{'execution_id':'SYNTHETIC_EXECUTION','state':'QUERY_STATE_COMPLETED'},lambda *_:page([],20),tmp)
            result=self.invoke(p);self.assertFalse(result.complete);self.assertTrue(result.gaps)
    def test_uncertain_execution_cannot_resubmit_on_restart(self):
        calls=[]
        def bad(*_):calls.append(1);raise TimeoutError()
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            self.assertFalse(self.invoke(DuneProvider(bad,None,tmp)).complete)
            self.assertFalse(self.invoke(DuneProvider(bad,None,tmp)).complete)
            self.assertEqual(len(calls),1)
    def test_page_budget_failure_retains_collected_rows_and_cursor(self):
        def export(eid,params,job):
            if params['offset']:raise RuntimeError('synthetic budget refusal')
            return page([row()],2,1)
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            p=DuneProvider(lambda *_:{'execution_id':'SYNTHETIC_EXECUTION','state':'QUERY_STATE_COMPLETED'},export,tmp,page_size=1)
            r=self.invoke(p);self.assertFalse(r.complete);self.assertEqual(len(r.events),1);self.assertEqual(r.coverage[-1]['next_offset'],1)

if __name__=='__main__':unittest.main()
