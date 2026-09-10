"""Offline guards for the root current-demand adapter, including real envelope shape."""
from pathlib import Path
import sys,unittest,json,socket
R=Path(__file__).resolve().parents[1];C=R/'code'
sys.path[:0]=[str(R/'scripts'),str(C/'src'),str(R/'staging/current_weth_legacy_admission')]
from prepare_current_weth import derive_query
from test_prepare_current_weth import Tests,fixture
from context_access_r3 import sha,now
from page_attempts import atomic_json
def blocked(*a,**k):raise RuntimeError('Driver checks have no network')
socket.socket.connect=blocked;socket.create_connection=blocked
class EnvelopeTests(unittest.TestCase):
    def test_current_metric_membership(self):
        q,c,r=fixture();c['metrics']['candidate_membership']=c.pop('membership')
        c['candidate_events'][1]['context_only']=False
        d,n=derive_query(q,c,r,r);self.assertEqual(len(n),4)
    def test_conflicting_membership_cannot_authorize(self):
        q,c,r=fixture();c['metrics']['candidate_membership']=[]
        with self.assertRaisesRegex(ValueError,'Conflicting'):derive_query(q,c,r,r)
    def test_real_candidate_flag_retained(self):
        q,c,r=fixture();c['candidate_events'][1]['context_only']=False
        d,n=derive_query(q,c,r,r)
        self.assertIs(d['events'][0]['context_only'],False);self.assertEqual(len(n),4)
    def test_context_only_cannot_authorize(self):
        q,c,r=fixture();c['candidate_events'][1]['context_only']=True
        with self.assertRaisesRegex(ValueError,'Context-only'):derive_query(q,c,r,r)
    def test_unknown_field_cannot_be_silently_stripped(self):
        q,c,r=fixture();c['candidate_events'][1]['unexpected']=True
        with self.assertRaises(TypeError):derive_query(q,c,r,r)
folder=R/'checks/semantic_units/current_weth_driver';folder.mkdir(parents=True,exist_ok=True)
atomic_json(folder/'PRIOR_LOCAL_FAILURE.json',{'status':'FAILED_BEFORE_ADMISSION','error_class':'TypeError',
    'reason':"Event.__init__() got an unexpected keyword argument 'context_only'",'current_driver_fix':'Explicit False candidate envelope field retained in demand; only constructor unwraps it',
    'new_external_requests':0,'legacy_imports':0,'staged_driver_sha256':'PRIVATE_LITERAL_1'})
suite=unittest.TestSuite([unittest.defaultTestLoader.loadTestsFromTestCase(Tests),unittest.defaultTestLoader.loadTestsFromTestCase(EnvelopeTests)])
with (folder/'TEST_OUTPUT.txt').open('w',encoding='utf8') as f:result=unittest.TextTestRunner(stream=f,verbosity=2).run(suite)
receipt={'status':'PASS' if result.wasSuccessful() else 'FAIL','tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
    'new_external_requests':0,'utc':now(),'driver_sha256':sha(R/'scripts/prepare_current_weth.py'),'log_sha256':sha(folder/'TEST_OUTPUT.txt')}
atomic_json(folder/'TEST_EVIDENCE.json',receipt);print(json.dumps(receipt))
if not result.wasSuccessful():raise SystemExit(1)
