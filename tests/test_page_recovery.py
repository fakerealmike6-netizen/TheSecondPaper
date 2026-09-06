"""R2-01/R2-03 regressions: pure fixtures, new ledgers, no network/providers."""
from pathlib import Path
import contextlib,copy,io,json,os,socket,subprocess,sys,tempfile,unittest
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/'src'))
from page_attempts import AttemptStore,RequestBlocked,atomic_json
from page_contract import validate_page,initial_progress,PageContractError
from provider_dune import DuneProvider
from collector import NATIVE
from dune_live import Live,read,dump

E='A'*26;A='0x'+'1'*40;B='0x'+'2'*40;TX='0x'+'3'*64
def row(n=0):
    return dict(event_kind='top',tx_hash='0x'+format(n+1,'064x'),sender=A,recipient=B,amount_raw='7',block_number=2+n,tx_index=0,block_time=2+n,success=True)
def page(offset=0,total=3,limit=2):
    rows=[{'v':i} for i in range(offset,min(total,offset+limit))]
    result=dict(execution_id=E,state='QUERY_STATE_COMPLETED',result=dict(rows=rows,metadata=dict(row_count=len(rows),total_row_count=total)))
    if offset+len(rows)<total:result['next_offset']=offset+len(rows)
    return result
def identity(offset=0,limit=2,execution=E,job='job'):
    return AttemptStore.identity('SYNTHETIC',job,execution,'results',{'limit':limit,'offset':offset})

class ContractTests(unittest.TestCase):
    def validate(self,p,offset=0,limit=2,progress=None,**kw):
        return validate_page(p,execution_id=E,offset=offset,limit=limit,progress=progress,**kw)
    def test_normal_multiple_pages_and_zero_result(self):
        first=self.validate(page());self.assertFalse(first['complete']);self.assertEqual(first['next_offset'],2)
        final=self.validate(page(2),offset=2,progress=first);self.assertTrue(final['complete']);self.assertEqual(final['observed_rows'],3)
        zero=self.validate(page(total=0));self.assertTrue(zero['complete']);self.assertEqual(zero['observed_rows'],0)
    def test_documented_optional_cursor_and_column_fields(self):
        # Identity/state/row metadata are never optional in this profile. Schema
        # descriptors and terminal cursor keys may be omitted without invention.
        p=page(total=1);p['next_offset']=None;p['next_uri']=None
        self.assertTrue(self.validate(p)['complete'])
    def test_external_three_abnormal_200_counterexamples(self):
        cases={}
        p=page();p.pop('execution_id');p.pop('state');cases['missing_identity_state']=p
        p=page();p['result']['metadata']['total_row_count']=999;cases['status_total3_page999']=p
        p=page(total=2);p['next_offset']=2;cases['terminal_cursor']=p
        for name,p in cases.items():
            with self.subTest(name=name),self.assertRaises(PageContractError):self.validate(p,status_metadata={'total_row_count':2 if name=='terminal_cursor' else 3})
    def test_identity_state_rows_and_exact_counts_rejected(self):
        mutations=[lambda p:p.update(execution_id='B'*26),lambda p:p.update(state='QUERY_STATE_EXECUTING'),
          lambda p:p['result'].update(rows=None),lambda p:p['result'].update(rows=[1,2]),
          lambda p:p['result']['metadata'].update(row_count=1),lambda p:p['result']['metadata'].update(total_row_count=True),
          lambda p:p['result']['metadata'].update(total_row_count='3'),lambda p:p['result'].pop('metadata')]
        for i,mutate in enumerate(mutations):
            p=page();mutate(p)
            with self.subTest(case=i),self.assertRaises(PageContractError):self.validate(p)
    def test_row_count_is_page_not_status_total(self):
        self.assertFalse(self.validate(page(),status_metadata={'row_count':3,'total_row_count':3})['complete'])
    def test_cursor_gaps_repeats_and_empty_loops(self):
        for nxt in (0,1,3,True,'2',-1):
            p=page();p['next_offset']=nxt
            with self.subTest(cursor=nxt),self.assertRaises(PageContractError):self.validate(p)
        p=page();p['result']['rows']=[];p['result']['metadata']['row_count']=0
        with self.assertRaises(PageContractError):self.validate(p)
        p=page();p.pop('next_offset')
        with self.assertRaises(PageContractError):self.validate(p)
    def test_total_row_schema_and_column_schema_stay_stable(self):
        p=page();p['result']['metadata'].update(column_names=['v'],column_types=['integer'])
        progress=self.validate(p)
        for mode in ('total','row_keys','column_type','schema_disappears'):
            q=page(2);q['result']['metadata'].update(column_names=['v'],column_types=['integer'])
            if mode=='total':q['result']['metadata']['total_row_count']=4
            if mode=='row_keys':q['result']['rows']=[{'x':2}]
            if mode=='column_type':q['result']['metadata']['column_types']=['varchar']
            if mode=='schema_disappears':q['result']['metadata'].pop('column_types')
            with self.subTest(mode=mode),self.assertRaises(PageContractError):self.validate(q,offset=2,progress=progress)
    def test_official_same_execution_next_uri_only(self):
        p=page();p['next_uri']=f'https://api.dune.com/api/v1/execution/{E}/results?offset=2&limit=2'
        self.assertEqual(self.validate(p)['next_offset'],2)
        p['next_uri']=f'https://api.dune.com/api/v1/execution/{E}/results?offset=2'
        self.assertEqual(self.validate(p)['next_offset'],2)
        urls=[f'http://api.dune.com/api/v1/execution/{E}/results?offset=2',f'https://evil.invalid/api/v1/execution/{E}/results?offset=2',
          f'https://api.dune.com@evil.invalid/api/v1/execution/{E}/results?offset=2',f'https://api.dune.com:444/api/v1/execution/{E}/results?offset=2',
          f'https://api.dune.com/api/v1/execution/{"B"*26}/results?offset=2',f'https://api.dune.com/api/v1/execution/{E}/results?offset=2&offset=2',
          f'https://api.dune.com/api/v1/execution/{E}/results?offset=2&filters=v>1',f'https://api.dune.com/api/v1/execution/{E}/results?offset=2&limit=50',
          f'https://api.dune.com/api/v1/execution/{E}/results?offset=2#x',f'https://api.dune.com/api/v1/execution/{E}/results?offset=1']
        for uri in urls:
            p['next_uri']=uri
            with self.subTest(uri=uri),self.assertRaises(PageContractError):self.validate(p)

class JournalTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.w=Path(self.temp.name);self.store=AttemptStore(self.w/'attempts.sqlite')
    def tearDown(self):self.temp.cleanup()
    def test_dispatch_states_count_and_recreated_instance_block(self):
        aid=self.store.dispatch(identity());self.assertEqual(self.store.rows()[0]['state'],'DISPATCH_INTENT')
        rebuilt=AttemptStore(self.store.path)
        for ident in (identity(),identity(2),identity(limit=3),identity(job='changed_sql')):
            with self.assertRaises(RequestBlocked):rebuilt.dispatch(ident)
        self.assertEqual(len(rebuilt.rows()),1)
        rebuilt.mark(aid,'UNKNOWN_TRANSPORT','TimeoutError');self.assertEqual(rebuilt.rows()[0]['state'],'UNKNOWN_TRANSPORT')
    def test_atomic_response_validation_and_warm_reuse(self):
        ident=identity();aid=self.store.dispatch(ident);p=page();progress=validate_page(p,execution_id=E,offset=0,limit=2)
        self.store.save_response(aid,p,{'synthetic':True},self.w/'page.json',self.w/'receipt.json');self.store.validated(aid,progress)
        rebuilt=AttemptStore(self.store.path);self.assertEqual(rebuilt.cached(ident)[0],p)
        with self.assertRaises(RequestBlocked):rebuilt.dispatch(ident)
        rebuilt.dispatch(identity(2));self.assertEqual(len(rebuilt.rows()),2)
    def test_response_file_tampering_fails_closed(self):
        ident=identity();aid=self.store.dispatch(ident);p=page();progress=validate_page(p,execution_id=E,offset=0,limit=2)
        self.store.save_response(aid,p,{},self.w/'page.json',self.w/'receipt.json');self.store.validated(aid,progress)
        (self.w/'page.json').write_text('{}')
        with self.assertRaises(RequestBlocked):self.store.cached(ident)
    def worker(self,*extra):
        return [sys.executable,str(BASE/'fixtures/fault/page_attempt_worker.py'),'--source-root',str(BASE/'src'),'--db',str(self.w/'worker.sqlite'),'--marker',str(self.w/'marker.txt'),*extra]
    def test_actual_child_process_death_never_redispatches(self):
        died=subprocess.run(self.worker('--die'),capture_output=True,timeout=20);self.assertEqual(died.returncode,17,died.stderr)
        restarted=subprocess.run(self.worker(),capture_output=True,timeout=20);self.assertEqual(restarted.returncode,20,restarted.stderr)
        self.assertEqual((self.w/'marker.txt').read_text().splitlines(),['SYNTHETIC_CALLBACK'])
        self.assertEqual(AttemptStore(self.w/'worker.sqlite').rows()[0]['state'],'DISPATCH_INTENT')
    def test_two_processes_compete_for_exact_same_page(self):
        workers=[subprocess.Popen(self.worker(),stdout=subprocess.PIPE,stderr=subprocess.PIPE) for _ in range(2)]
        outputs=[w.communicate(timeout=20) for w in workers]
        self.assertEqual(sorted(w.returncode for w in workers),[0,20],outputs)
        self.assertEqual(len((self.w/'marker.txt').read_text().splitlines()),1)
    def test_full_provider_process_dies_after_export_dispatch(self):
        first=subprocess.run(self.worker('--provider','--die'),capture_output=True,timeout=20);self.assertEqual(first.returncode,17,first.stderr)
        restarted=subprocess.run(self.worker('--provider'),capture_output=True,timeout=20);self.assertEqual(restarted.returncode,20,restarted.stderr)
        self.assertEqual((self.w/'marker.txt').read_text().splitlines(),['SYNTHETIC_CALLBACK'])
        attempts=AttemptStore(self.w/'provider_cache/request_attempts.sqlite').rows()
        self.assertEqual(len(attempts),2);self.assertEqual(sum(r['state']=='DISPATCH_INTENT' for r in attempts),1)

class ProviderRecoveryTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.w=Path(self.temp.name);self.calls=[]
    def tearDown(self):self.temp.cleanup()
    def execute(self,sql,job):self.calls.append('execute');return {'execution_id':E,'state':'QUERY_STATE_COMPLETED'}
    def invoke(self,provider,end=10):return provider.fetch_interval(A,NATIVE,1,end,start_time=1,end_time=end,global_end_time=end)
    def test_original_generic_timeout_recreated_adapter_one_page_attempt(self):
        def export(*args):self.calls.append('export');raise TimeoutError('unknown')
        a=self.invoke(DuneProvider(self.execute,export,self.w));b=self.invoke(DuneProvider(self.execute,export,self.w))
        self.assertEqual(self.calls,['execute','export']);self.assertEqual(a.real_requests,2);self.assertEqual(b.real_requests,0)
        self.assertFalse(a.complete);self.assertFalse(b.complete)
        self.assertEqual([r['state'] for r in AttemptStore(self.w/'request_attempts.sqlite').rows()].count('UNKNOWN_TRANSPORT'),1)
        # Replacing SQL cannot turn the old uncharged/unknown assumption into a new dispatch.
        self.invoke(DuneProvider(self.execute,export,self.w),end=11);self.assertEqual(len(self.calls),2)
    def test_pre_request_persistence_failure_makes_no_callback(self):
        p=DuneProvider(self.execute,lambda *a:self.calls.append('export'),self.w)
        with patch.object(p.attempts,'dispatch',side_effect=OSError('disk full')):r=self.invoke(p)
        self.assertEqual(self.calls,[]);self.assertFalse(r.complete);self.assertEqual(r.real_requests,0)
    def test_response_persist_failure_does_not_retry_after_restart(self):
        def export(*args):self.calls.append('export');return {'execution_id':E,'state':'QUERY_STATE_COMPLETED','result':{'rows':[row()],'metadata':{'row_count':1,'total_row_count':1}}}
        p=DuneProvider(self.execute,export,self.w);original=p.attempts.save_response
        def fail(aid,body,receipt,*paths):
            if receipt['operation']=='results':raise OSError('response disk full')
            return original(aid,body,receipt,*paths)
        with patch.object(p.attempts,'save_response',side_effect=fail):first=self.invoke(p)
        second=self.invoke(DuneProvider(self.execute,export,self.w))
        self.assertFalse(first.complete);self.assertFalse(second.complete);self.assertEqual(self.calls,['execute','export'])
    def test_successful_page_cache_only_replayed_and_page_limit_retains_cursor(self):
        def export(eid,params,job):
            self.calls.append(params['offset']);offset=params['offset'];p={'execution_id':E,'state':'QUERY_STATE_COMPLETED','result':{'rows':[row(offset)],'metadata':{'row_count':1,'total_row_count':2}}}
            if offset==0:p['next_offset']=1
            return p
        first=self.invoke(DuneProvider(self.execute,export,self.w,page_size=1,max_pages=1));self.assertFalse(first.complete);self.assertEqual(len(first.events),1)
        second=self.invoke(DuneProvider(self.execute,export,self.w,page_size=1));self.assertTrue(second.complete);self.assertEqual(self.calls,['execute',0,1])
        warm=self.invoke(DuneProvider(None,None,self.w,page_size=1,replay_only=True));self.assertTrue(warm.complete);self.assertEqual(warm.real_requests,0)
    def test_generic_abnormal_page_blocks_new_instance(self):
        def export(*args):self.calls.append('export');return page()|{'execution_id':None}
        first=self.invoke(DuneProvider(self.execute,export,self.w));second=self.invoke(DuneProvider(self.execute,export,self.w))
        self.assertFalse(first.complete);self.assertFalse(second.complete);self.assertEqual(self.calls,['execute','export'])
    def test_changed_limit_cannot_relabel_a_saved_page_identity(self):
        def export(*args):self.calls.append('export');return {'execution_id':E,'state':'QUERY_STATE_COMPLETED','result':{'rows':[row()],'metadata':{'row_count':1,'total_row_count':1}}}
        first=self.invoke(DuneProvider(self.execute,export,self.w,page_size=2));self.assertTrue(first.complete)
        changed=self.invoke(DuneProvider(self.execute,export,self.w,page_size=3));self.assertFalse(changed.complete)
        self.assertEqual(self.calls,['execute','export'])
    def test_concurrent_adapters_only_one_export_callback(self):
        entered=threading.Event();release=threading.Event()
        def export(*args):
            self.calls.append('export');entered.set()
            if not release.wait(5):raise TimeoutError('test barrier')
            return {'execution_id':E,'state':'QUERY_STATE_COMPLETED','result':{'rows':[],'metadata':{'row_count':0,'total_row_count':0}}}
        first=DuneProvider(self.execute,export,self.w);second=DuneProvider(self.execute,export,self.w)
        with ThreadPoolExecutor(max_workers=2) as pool:
            f=pool.submit(self.invoke,first);self.assertTrue(entered.wait(5))
            g=pool.submit(self.invoke,second);duplicate=g.result(timeout=5);release.set();original=f.result(timeout=5)
        self.assertTrue(original.complete);self.assertFalse(duplicate.complete);self.assertEqual(self.calls,['execute','export'])

class LiveRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.w=Path(self.temp.name);self.calls=[]
        dump(self.w/'private/dune_user_confirmation.json',dict(status='USER_CONFIRMED',execution_cap_credits='1',payment_method_added=False,extra_credits_enabled=False))
        self.live=Live(self.w);self.live.db.confirm('dune_credits','10','SYNTHETIC');self.live.db.reserve_dune_job('job','SYNTHETIC','1','1')
        self.folder=self.w/'private/dune_live_jobs/job';self.state=dict(logical_job_id='job',execution_id=E,state='QUERY_STATE_COMPLETED',execution_cost_credits='0.1',export_offsets=[],export_requests=0,
          status_response=dict(state='QUERY_STATE_COMPLETED',execution_cost_credits='0.1',result_metadata=dict(total_row_count=3,total_result_set_bytes=300,column_names=['v'])))
        dump(self.folder/'job.json',self.state)
    def tearDown(self):self.temp.cleanup()
    def call(self,op,execution=None,payload=None,params=None):
        self.calls.append(params.copy());return page(params['offset']),dict(http_status=200,error_class=None,execution_id=E,parameters=params,request_id='SYNTHETIC',raw_bytes=1)
    def quiet(self,fn,*args):
        with contextlib.redirect_stdout(io.StringIO()):return fn(*args)
    def test_original_live_abnormal200_stops_before_next_callback(self):
        for mode in ('missing_identity','missing_state','wrong_total','wrong_row_count','terminal_cursor'):
            with self.subTest(mode=mode):
                # Every counterexample needs a genuinely separate logical work/ledger.
                sub=Path(self.temp.name)/mode;dump(sub/'private/dune_user_confirmation.json',read(self.w/'private/dune_user_confirmation.json'))
                live=Live(sub);live.db.confirm('dune_credits','10','SYNTHETIC');live.db.reserve_dune_job('job','SYNTHETIC','1','1')
                folder=sub/'private/dune_live_jobs/job';state=copy.deepcopy(self.state)
                if mode=='terminal_cursor':state['status_response']['result_metadata']['total_row_count']=2
                dump(folder/'job.json',state);calls=[]
                def fake(op,execution=None,payload=None,params=None):
                    calls.append(params.copy());p=page(total=2 if mode=='terminal_cursor' else 3)
                    if mode=='missing_identity':p.pop('execution_id')
                    if mode=='missing_state':p.pop('state')
                    if mode=='wrong_total':p['result']['metadata']['total_row_count']=999
                    if mode=='wrong_row_count':p['result']['metadata']['row_count']=1
                    if mode=='terminal_cursor':p['next_offset']=2
                    return p,dict(http_status=200,error_class=None,execution_id=E,parameters=params,raw_bytes=1)
                live.call=fake;self.quiet(live.export,folder,2,0)
                rebuilt=Live(sub);rebuilt.call=fake
                for offset in (0,2):
                    with self.assertRaises(RuntimeError):self.quiet(rebuilt.export,folder,2,offset)
                self.assertEqual(len(calls),1);self.assertEqual(read(folder/'job.json')['export_status'],'EXPORT_FAILED_OR_UNCERTAIN')
                self.assertEqual(live.db.snapshot()['dune_credits']['reserved'],'2')
                self.assertEqual(live.attempts.rows()[0]['state'],'INVALID_RESPONSE')
    def test_live_normal_two_pages_complete_with_no_duplicate(self):
        self.live.call=self.call;self.quiet(self.live.export,self.folder,2,0)
        rebuilt=Live(self.w);rebuilt.call=self.call;self.quiet(rebuilt.export,self.folder,2,2)
        with self.assertRaises(RuntimeError):self.quiet(rebuilt.export,self.folder,2,2)
        self.assertEqual(len(self.calls),2);self.assertEqual(read(self.folder/'job.json')['export_status'],'COMPLETED_DECLARED_RESULT_ROWS')
    def test_live_pre_dispatch_journal_failure_no_network(self):
        self.live.call=self.call
        with patch.object(self.live.attempts,'dispatch',side_effect=OSError('disk full')),self.assertRaises(OSError):self.quiet(self.live.export,self.folder,2,0)
        self.assertEqual(self.calls,[])
    def test_live_state_write_failure_before_callback_remains_blocked(self):
        self.live.call=self.call
        with patch('dune_live.dump',side_effect=OSError('state disk full')),self.assertRaises(OSError):self.quiet(self.live.export,self.folder,2,0)
        rebuilt=Live(self.w);rebuilt.call=self.call
        with self.assertRaises(RuntimeError):self.quiet(rebuilt.export,self.folder,2,0)
        self.assertEqual(self.calls,[]);self.assertEqual(len(rebuilt.attempts.rows()),1)
    def test_live_response_persist_failure_blocks_retry_and_continuation(self):
        self.live.call=self.call
        with patch.object(self.live.attempts,'save_response',side_effect=OSError('response disk full')),self.assertRaises(OSError):self.quiet(self.live.export,self.folder,2,0)
        rebuilt=Live(self.w);rebuilt.call=self.call
        for offset in (0,2):
            with self.assertRaises(RuntimeError):self.quiet(rebuilt.export,self.folder,2,offset)
        self.assertEqual(len(self.calls),1);self.assertEqual(rebuilt.attempts.rows()[0]['state'],'UNKNOWN_TRANSPORT')

if __name__=='__main__':unittest.main()
