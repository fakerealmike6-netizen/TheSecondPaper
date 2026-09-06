"""Dune lifecycle budget regressions using isolated synthetic ledgers only."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/'src'))
from dune_live import Live, dump, read


class DuneLiveTests(unittest.TestCase):
    def setUp(self):
        (BASE/'private').mkdir(exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(prefix='dune_regression_',dir=BASE/'private')
        self.work=Path(self.temp.name)
        dump(self.work/'private/dune_user_confirmation.json',{'status':'USER_CONFIRMED','execution_cap_credits':'1',
            'payment_method_added':False,'extra_credits_enabled':False})
        self.live=Live(self.work);self.live.db.confirm('dune_credits','10','SYNTHETIC_TEST_ONLY')
        self.live.db.reserve_dune_job('fixture_job','offline regression','1','1')
        self.folder=self.work/'private/dune_live_jobs/fixture'
        self.state={'logical_job_id':'fixture_job','execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED',
            'execution_cost_credits':'0.764852942','export_requests':0,'export_offsets':[],
            'usage_before':{'start_date':'2026-09-01','credits_used':'30.998'},
            'status_receipt':{'request_id':'fixture_status','utc':'2026-09-06T00:00:00Z'},
            'status_response':{'state':'QUERY_STATE_COMPLETED','execution_cost_credits':'0.764852942',
                'result_metadata':{'total_row_count':3,'total_result_set_bytes':300,'column_names':['value']}}}
        dump(self.folder/'job.json',self.state)
        self.live.call=Mock(side_effect=self.fake_call)

    def tearDown(self):
        self.assertTrue(self.work.resolve().is_relative_to(BASE.resolve()))
        self.temp.cleanup()

    def fake_call(self,op,execution=None,payload=None,params=None):
        receipt={'request_id':'fixture_'+op,'utc':'2026-09-06T00:00:00Z','http_status':200,'error_class':None,
                 'raw_bytes':20,'parameters':params}
        if op=='results':
            offset=params['offset'];limit=params['limit'];count=min(limit,3-offset)
            return {'state':'QUERY_STATE_COMPLETED','execution_id':'A'*26,'result':{'rows':[{'value':i} for i in range(offset,offset+count)]},
                    'next_offset':None if offset+count==3 else offset+count},receipt
        if op=='usage':
            return {'billing_periods':[{'start_date':'2026-09-01','credits_used':'31.762'}]},receipt
        raise AssertionError('Unexpected real transport operation in offline test: '+op)

    def quiet(self,fn,*args):
        with contextlib.redirect_stdout(io.StringIO()):return fn(*args)

    def test_export_fee_unknown_keeps_full_two_and_known_execution(self):
        self.quiet(self.live.export,self.folder,50,0)
        for observed in ('31.762','32.998'):
            # Both a rounded delta below execution and a delta above execution
            # are insufficient evidence that every export charge is final.
            self.live.call=Mock(return_value=({'billing_periods':[{'start_date':'2026-09-01','credits_used':observed}]},
                {'request_id':'usage_fixture','utc':None,'http_status':200,'error_class':None}))
            self.quiet(self.live.settle,self.folder)
            state=read(self.folder/'job.json');snapshot=self.live.db.snapshot()['dune_credits']
            self.assertEqual(state['settlement_status'],'UNKNOWN_EXPORT_CHARGE_RESERVED')
            self.assertEqual(state['known_execution_cost_credits'],'0.764852942')
            self.assertIsNone(state['known_export_cost_credits'])
            self.assertNotIn('settled_credits',state)
            self.assertEqual(snapshot['reserved'],'2');self.assertEqual(snapshot['remaining'],'8')
        self.live.db.reserve_dune_job('next_job','another bounded task','1','1')
        self.assertEqual(self.live.db.snapshot()['dune_credits']['reserved'],'4')

    def test_settled_execution_only_job_cannot_later_export(self):
        self.quiet(self.live.settle,self.folder)
        self.assertEqual(self.live.db.snapshot()['dune_credits']['actual'],'0.764852942')
        with self.assertRaisesRegex(RuntimeError,'no pending'):
            self.live.export(self.folder,50,0)
        self.live.call.assert_not_called()

    def test_only_contiguous_fixed_size_pages_and_no_post_terminal_page(self):
        self.quiet(self.live.export,self.folder,2,0)
        with self.assertRaisesRegex(RuntimeError,'exact next'):
            self.live.export(self.folder,2,1)
        with self.assertRaisesRegex(RuntimeError,'page size is frozen'):
            self.live.export(self.folder,1,2)
        self.quiet(self.live.export,self.folder,2,2)
        with self.assertRaisesRegex(RuntimeError,'already complete'):
            self.live.export(self.folder,2,3)
        self.assertEqual(self.live.call.call_count,2)
        self.assertEqual(read(self.folder/'job.json')['export_status'],'COMPLETED_DECLARED_RESULT_ROWS')

    def test_uncertain_page_does_not_become_successful_end_or_retry(self):
        self.live.call=Mock(return_value=(None,{'request_id':'timeout_fixture','http_status':None,'error_class':'TimeoutError',
            'raw_bytes':0,'parameters':{'limit':2,'offset':0}}))
        self.quiet(self.live.export,self.folder,2,0)
        self.assertEqual(read(self.folder/'job.json')['export_status'],'EXPORT_FAILED_OR_UNCERTAIN')
        for offset in (0,2):
            with self.assertRaisesRegex(RuntimeError,'failed or remains uncertain'):
                self.live.export(self.folder,2,offset)
        self.assertEqual(self.live.call.call_count,1)
        self.assertEqual(self.live.db.snapshot()['dune_credits']['reserved'],'2')

    def test_failed_poll_preserves_known_charge_and_terminal_state(self):
        self.live.call=Mock(return_value=({'error':'rate limited'},{'request_id':'failed_status','http_status':429,'error_class':None}))
        self.quiet(self.live.poll,self.folder)
        state=read(self.folder/'job.json')
        self.assertEqual(state['execution_cost_credits'],'0.764852942')
        self.assertEqual(state['state'],'QUERY_STATE_COMPLETED')
        self.assertEqual(state['latest_status_response'],{'error':'rate limited'})

    def test_execution_cap_violation_keeps_full_value_and_halts_next_calls(self):
        body={'state':'QUERY_STATE_COMPLETED','execution_cost_credits':'1.23456789123456789'}
        self.live.call=Mock(return_value=(body,{'request_id':'over_cap_status','http_status':200,'utc':None,'error_class':None}))
        self.quiet(self.live.poll,self.folder)
        self.assertEqual(read(self.folder/'job.json')['known_execution_cost_credits'],'1.23456789123456789')
        self.assertTrue((self.work/'private/dune_live_halt.json').exists())
        with self.assertRaisesRegex(RuntimeError,'halted'):
            self.live.export(self.folder,50,0)
        with self.assertRaisesRegex(RuntimeError,'halted'):
            self.live.submit('must_not_read.sql','blocked')
        self.assertEqual(self.live.call.call_count,1)
        self.quiet(self.live.settle,self.folder)
        self.assertEqual(self.live.db.snapshot()['dune_credits']['actual'],'1.23456789123456789')

    def test_existing_single_page_receipt_is_recognized_as_complete(self):
        state=read(self.folder/'job.json');state['export_requests']=1;state['export_offsets']=[0]
        dump(self.folder/'job.json',state)
        page,receipt=self.fake_call('results',params={'limit':50,'offset':0})
        dump(self.folder/'page_0.json',page);dump(self.folder/'page_0_receipt.json',receipt)
        with self.assertRaisesRegex(RuntimeError,'already complete'):
            self.live.export(self.folder,50,0)
        self.live.call.assert_not_called()


if __name__=='__main__':unittest.main()
