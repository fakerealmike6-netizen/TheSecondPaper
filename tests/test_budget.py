import tempfile, unittest, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from budget import Ledger, valid

class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory(); self.p=Path(self.t.name)/'ledger.sqlite'; self.l=Ledger(self.p)
    def tearDown(self): self.t.cleanup()
    def test_restart_shared_reservations(self):
        self.l.confirm('rpc_operations',10,'fixture'); self.l.reserve('a','fixture','test',{'rpc_operations':7})
        l=Ledger(self.p)
        with self.assertRaises(RuntimeError): l.reserve('b','fixture','test',{'rpc_operations':4})
        self.assertEqual(l.snapshot()['rpc_operations']['reserved'],'7')
    def test_overrun_full_record_and_halt(self):
        self.l.confirm('rpc_operations',6,'fixture'); self.l.reserve('a','fixture','test',{'rpc_operations':5})
        self.l.settle('a',{'rpc_operations':120}); self.assertEqual(self.l.snapshot()['rpc_operations']['actual'],'120')
        with self.assertRaises(RuntimeError): self.l.reserve('b','fixture','test',{'rpc_operations':1})
    def test_unknown_settlement_preserves(self):
        self.l.confirm('rpc_operations',10,'fixture'); self.l.reserve('a','fixture','test',{'rpc_operations':3}); self.l.settle('a',{'rpc_operations':None})
        self.assertEqual(Ledger(self.p).snapshot()['rpc_operations']['reserved'],'3')
    def test_duplicates_invalid_atomic(self):
        self.l.confirm('rpc_operations',10,'fixture'); self.l.reserve('a','fixture','test',{'rpc_operations':3})
        with self.assertRaises(ValueError): self.l.settle('a',{'rpc_operations':-1})
        self.assertEqual(self.l.snapshot()['rpc_operations']['reserved'],'3')
        self.l.settle('a',{'rpc_operations':2})
        with self.assertRaises(ValueError): self.l.settle('a',{'rpc_operations':2})
    def test_numeric_gates(self):
        for n in ['NaN','Infinity',-1,True,1.1]:
            with self.assertRaises(ValueError): valid(n,'bigquery_bytes')
        self.assertEqual(int(valid(9007199254740993,'bigquery_bytes')),9007199254740993)
    def test_dune_combined_and_bigquery_job_cap(self):
        for unit,cap,attempt in [('dune_credits',10,'2.01'),('bigquery_bytes',5368709120,1073741825)]:
            self.l.confirm(unit,cap,'fixture')
            with self.assertRaises(ValueError): self.l.reserve(unit,'fixture','test',{unit:attempt})
    def test_actual_over_reserved_halts_below_stage_cap(self):
        self.l.confirm('dune_credits',10,'fixture'); self.l.reserve_dune_job('a','test',1,0)
        self.l.settle('a',{'dune_credits':'1.5'})
        self.assertEqual(self.l.snapshot()['dune_credits']['actual'],'1.5')
        with self.assertRaises(RuntimeError): Ledger(self.p).reserve_dune_job('b','test',1,0)
    def test_partial_known_cost_never_hidden(self):
        for u in ('rpc_operations','alchemy_cu'): self.l.confirm(u,100,'fixture')
        self.l.reserve('a','fixture','test',{'rpc_operations':1,'alchemy_cu':10})
        self.l.settle('a',{'rpc_operations':2,'alchemy_cu':None})
        self.assertEqual(self.l.snapshot()['rpc_operations']['actual'],'2')
        self.assertEqual(self.l.snapshot()['alchemy_cu']['reserved'],'10')
        with self.assertRaises(RuntimeError): self.l.reserve('b','fixture','test',{'rpc_operations':1})
        self.l.settle('a',{'alchemy_cu':8})
        self.assertEqual(self.l.snapshot()['alchemy_cu']['actual'],'8')
    def test_negative_export_not_netted(self):
        self.l.confirm('dune_credits',10,'fixture')
        with self.assertRaises(ValueError): self.l.reserve_dune_job('a','test',2,-1)
        self.assertEqual(self.l.rows(),[])

if __name__=='__main__': unittest.main()
