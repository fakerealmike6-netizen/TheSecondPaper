"""Synthetic server metadata discrepancies; never read real account data."""
import contextlib,hashlib,io,unittest
from decimal import Decimal
from pathlib import Path
import test_dune_cap_exception
from dune_live import read,dump
from dune_cap_exception import CAP5_AUTH,ordinary_sql_allowed

class ExportMetadataTests(unittest.TestCase):
    def setUp(self):
        self.h=test_dune_cap_exception.Cap5Tests();self.h.setUp();self.live=self.h.live;self.calls=[]
    def tearDown(self):self.h.tearDown()
    def fixture(self,*,ordinary=False,total=9,pagebytes=864,points=81,rawbytes=999999999,execution='4.5'):
        if ordinary:
            folder=self.h.w/'ordinary_job';job='ordinary_metadata';cost='0'
            self.h.db.reserve_dune_job(job,'synthetic metadata','1','1');self.h.db.observe_execution(job,cost,{'synthetic':True})
            state={'logical_job_id':job,'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED','execution_cost_credits':cost,'export_requests':0,'export_offsets':[]}
        else:
            self.h.reserve_exception();self.h.db.observe_execution(CAP5_AUTH,execution,{'synthetic':True})
            folder=self.h.w/'exception_job';state={'logical_job_id':CAP5_AUTH,'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED',
                'execution_cost_credits':execution,'export_requests':0,'export_offsets':[]}
        cols=['c'+str(i) for i in range(9)]
        state.update(status_receipt={'sha256':'a'*64,'request_id':'synthetic_status'},status_response={'state':'QUERY_STATE_COMPLETED',
            'execution_cost_credits':state['execution_cost_credits'],'result_metadata':{'total_row_count':total,'row_count':total,
                'total_result_set_bytes':738,'result_set_bytes':738,'column_names':cols,'datapoint_count':total*9}})
        dump(folder/'job.json',state)
        def callback(op,execution=None,payload=None,params=None):
            self.calls.append((op,params));n=min(params['limit'],total-params['offset']);next_offset=params['offset']+n
            body={'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED','result':{'rows':[{k:i for k in cols} for i in range(n)],
                'metadata':{'row_count':n,'total_row_count':total,'total_result_set_bytes':738,'result_set_bytes':pagebytes,
                    'datapoint_count':points,'column_names':cols}},'next_offset':next_offset if next_offset<total else None}
            return body,{'http_status':200,'error_class':None,'request_id':'synthetic_result','execution_id':'A'*26,
                'parameters':params,'raw_bytes':rawbytes,'sha256':'b'*64}
        self.live.call=callback;return folder
    def quiet(self,fn,*args):
        with contextlib.redirect_stdout(io.StringIO()):return fn(*args)
    def test_real_shape_738_864_is_one_credit_upper_not_http_actual(self):
        folder=self.fixture();self.quiet(self.live.export,folder,50,0)
        state=read(folder/'job.json');upper,b=self.live.export_envelope(state,50,folder)
        self.assertEqual(upper,Decimal(1));self.assertEqual(b['maximum_server_bytes'],864);self.assertEqual(b['maximum_datapoints'],'81')
        self.assertEqual(b['byte_scheme_credits'],'0.01728');self.assertEqual(b['datapoint_scheme_credits'],'0.081');self.assertTrue(b['server_byte_fields_disagree'])
        self.assertEqual([x['source_receipt_sha256'] for x in b['server_metadata_observations']],['a'*64,'b'*64])
        self.assertEqual(b['server_metadata_observations'][1]['saved_page_sha256'],hashlib.sha256((folder/'page_0.json').read_bytes()).hexdigest())
        result=self.live.settle(folder);self.assertEqual(result['upper'],'5.5');self.assertIsNone(result['actual']);self.assertIsNone(result['export_actual'])
        evidence=read(folder/'job.json')['settlement_evidence']['metered_units_evidence'];self.assertEqual(evidence['maximum_server_bytes'],864)
        self.assertEqual(len(self.calls),1)
    def test_large_page_bytes_retained_without_truncation_and_halts(self):
        folder=self.fixture(pagebytes=60000)
        with self.assertRaises(RuntimeError):self.quiet(self.live.export,folder,50,0)
        state=read(folder/'job.json');self.assertEqual(state['total_upper_credits'],'6.5');self.assertIsNone(state['known_export_cost_credits'])
        job=next(x for x in self.h.db.detailed_jobs() if x['job']==CAP5_AUTH)
        self.assertEqual(job['total_upper'],'6.5');self.assertEqual(job['accounting_status'],'OVERRUN_RECORDED_HALTED')
        with self.assertRaises(RuntimeError):self.quiet(self.live.export,folder,50,0)
        self.assertEqual(len(self.calls),1)
    def test_server_datapoints_can_increase_bound_without_larger_bytes(self):
        folder=self.fixture(pagebytes=738,points=1001)
        with self.assertRaises(RuntimeError):self.quiet(self.live.export,folder,50,0)
        state=read(folder/'job.json');self.assertEqual(state['r1_export_envelope']['maximum_datapoints'],'1001')
        self.assertEqual(state['total_upper_credits'],'6.5');self.assertEqual(len(self.calls),1)
    def test_export_cap_alone_stops_and_preserves_original_six(self):
        folder=self.fixture(pagebytes=60000,execution='3')
        with self.assertRaises(RuntimeError):self.quiet(self.live.export,folder,50,0)
        state=read(folder/'job.json');self.assertEqual(state['export_bound_cap_violation']['export_upper_credits'],'2')
        self.assertEqual(state['total_upper_credits'],'6');self.assertIsNone(state['known_export_cost_credits'])
        self.assertTrue((self.h.w/'private/dune_live_halt.json').exists())
        self.assertEqual(self.h.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'8')
        with self.assertRaises(RuntimeError):self.quiet(self.live.export,folder,50,0)
        self.assertEqual(len(self.calls),1)
    def test_tampered_saved_page_cannot_change_metering_evidence(self):
        folder=self.fixture();self.quiet(self.live.export,folder,50,0)
        page=read(folder/'page_0.json');page['result']['metadata']['result_set_bytes']=1;dump(folder/'page_0.json',page)
        with self.assertRaises(RuntimeError):self.live.export_envelope(read(folder/'job.json'),50,folder)
        self.assertEqual(self.h.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'8')
    def test_partial_oversized_page_stops_before_next_dispatch(self):
        folder=self.fixture(ordinary=True,total=51,pagebytes=60000,points=450)
        with self.assertRaises(RuntimeError):self.quiet(self.live.export,folder,50,0)
        state=read(folder/'job.json');self.assertFalse(state['verified_export_progress']['complete']);self.assertTrue(state['request_set_closed'])
        self.assertEqual(state['total_upper_credits'],'4');self.assertEqual(state['export_offsets'],[0])
        with self.assertRaises(RuntimeError):self.quiet(self.live.export,folder,50,50)
        self.assertEqual(self.calls,[('results',{'limit':50,'offset':0})])
    def test_uncertain_page_never_reconciles_or_retries(self):
        folder=self.fixture()
        def timeout(*args,**kwargs):self.calls.append('timeout');raise TimeoutError('synthetic')
        self.live.call=timeout
        with self.assertRaises(TimeoutError):self.quiet(self.live.export,folder,50,0)
        with self.assertRaises(RuntimeError):self.quiet(self.live.export,folder,50,0)
        job=next(x for x in self.h.db.detailed_jobs() if x['job']==CAP5_AUTH)
        self.assertIsNone(job['total_upper']);self.assertEqual(self.h.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'8')
        self.assertEqual(self.calls,['timeout'])
    def test_used_pages_cannot_silently_fall_back_to_status_only(self):
        folder=self.fixture();self.quiet(self.live.export,folder,50,0)
        with self.assertRaises(RuntimeError):self.live.export_envelope(read(folder/'job.json'),50)
    def test_status_preflight_ceiling_not_weakened(self):
        folder=self.fixture(total=51);self.live.call=lambda *a,**k:self.fail('oversized preflight called network')
        with self.assertRaises(RuntimeError):self.live.export(folder,50,0)
        self.assertEqual(read(folder/'job.json')['export_requests'],0)
    def test_keep5_user_decision_survives_settlement_and_still_blocks_sql(self):
        folder=self.fixture();control=read(self.h.w/'private/dune_cap5_control.json')
        control.update(status='USER_CONFIRMED_KEEP5_ORDINARY_SQL_PAUSED',sql_submissions_paused=True,keep5_confirmation_source='USER_CONFIRMED')
        dump(self.h.w/'private/dune_cap5_control.json',control)
        before=(self.h.w/'private/dune_cap5_control.json').read_bytes()
        self.quiet(self.live.export,folder,50,0);self.live.settle(folder)
        self.assertEqual((self.h.w/'private/dune_cap5_control.json').read_bytes(),before)
        with self.assertRaises(RuntimeError):ordinary_sql_allowed(read(self.h.w/'private/dune_cap5_control.json'))

if __name__=='__main__':unittest.main()
