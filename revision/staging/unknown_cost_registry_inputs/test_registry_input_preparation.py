from dataclasses import asdict,replace
from pathlib import Path
import json,time,unittest
import prepare_registry_inputs as p
from collector import Event,Scope,State
from stage1d_unknown_cost_boundary import activity,NATIVE,WETH

A='0x'+'a'*40;B='0x'+'b'*40;S='0x'+'c'*40

class IndexTests(unittest.TestCase):
    def fixture(self,asset=NATIVE):
        q=Scope('q','q',1,10000,0,3600,4,3600)
        a=Event('arrival','0x'+'0'*64,S,A,asset,1000,1,0,0,
                kind='erc20' if asset==WETH else 'top',log_index=1 if asset==WETH else None)
        s=State('q',A,asset,a,0,q.local_end(a))
        row=asdict(Event('out','0x'+'1'*64,A,B,asset,1,2,0,1,
                kind='erc20' if asset==WETH else 'top',log_index=2 if asset==WETH else None))
        return s,q,row
    def compare(self,s,q,rows):
        index,arrivals,report=p.build_index(rows)
        subset=index.get(('eip155:1',A,s.asset),[])+arrivals.get(s.arrival.event_id,[])
        full=activity(s,q,rows);fast=activity(s,q,subset)
        for key in ('N_obs','D_seconds','strict_gt20_observed','rate_truth','physical_event_count','transaction_keys'):
            self.assertEqual(full[key],fast[key],key)
        self.assertFalse(fast['full_coverage'])
        return fast,report
    def test_duplicate_sources_and_unrelated_sender(self):
        s,q,row=self.fixture();v,_=self.compare(s,q,[row,dict(row,provenance='other'),dict(row,event_id='irrelevant',tx_hash='0x'+'2'*64,sender=S)])
        self.assertEqual(v['N_obs'],1)
    def test_conflict_variant_cross_sender_not_lost_by_index(self):
        s,q,row=self.fixture();v,r=self.compare(s,q,[row,dict(row,sender=S)])
        self.assertEqual(v['N_obs'],0);self.assertEqual(len(r['global_conflicts']),1)
    def test_conflict_variant_cross_asset_not_lost_by_index(self):
        s,q,row=self.fixture();v,_=self.compare(s,q,[row,dict(row,asset=WETH)])
        self.assertEqual(v['N_obs'],0)
    def test_arrival_conflict_not_hidden_by_sender_index(self):
        s,q,row=self.fixture();v,_=self.compare(s,q,[row,dict(asdict(s.arrival),amount_raw=2)])
        self.assertIsNone(v['N_obs'])
    def test_rollback_missing_and_zero_do_not_count(self):
        s,q,row=self.fixture();v,_=self.compare(s,q,[row,dict(row,ancestor_success=False),
            dict(row,event_id='bad',tx_hash='0x'+'2'*64,success='true'),
            dict(row,event_id='zero',tx_hash='0x'+'3'*64,amount_raw=0)])
        self.assertEqual(v['N_obs'],0)
    def test_same_second_reliable_log_order_survives(self):
        s,q,row=self.fixture(WETH);row.update(tx_hash=s.arrival.tx_hash,block=1,timestamp=0)
        v,_=self.compare(s,q,[row]);self.assertEqual(v['N_obs'],1);self.assertTrue(v['strict_gt20_observed'])

if __name__=='__main__':
    started=time.perf_counter();result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(IndexTests))
    receipt={'status':'PASS' if result.wasSuccessful() else 'FAIL','tests':result.testsRun,'failures':len(result.failures),
             'errors':len(result.errors),'skipped':len(result.skipped),'seconds':time.perf_counter()-started,
             'network_attempts':0,'production_execution':False,'source_sha256':p.sha(Path(p.__file__))}
    (Path(__file__).parent/'SYNTHETIC_TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n')
    raise SystemExit(0 if result.wasSuccessful() else 1)
