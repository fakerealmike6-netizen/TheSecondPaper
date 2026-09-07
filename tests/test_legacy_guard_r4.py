"""Bounded legacy API/CLI refusal before budget or network side effects."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from legacy_guard_r4 import LegacyEntryBlocked, reject_legacy_workspace
import test_dune_r4 as dune_support
import test_context_access_r4 as rpc_support


class LegacyGuardR4Tests(unittest.TestCase):
    def setUp(self):
        parent=Path(__file__).resolve().parents[1]/'checks/legacy_guard_test_tmp'
        parent.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=parent);self.addCleanup(self.temp.cleanup)
        self.w=Path(self.temp.name);(self.w/'private').mkdir();(self.w/'configs').mkdir()
        self.marker=self.w/'configs/STAGE1B_R4_POLICY.json';self.marker.write_text('{}')
        self.src=Path(__file__).resolve().parents[1]/'src'

    def files(self):
        return {p.relative_to(self.w).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in self.w.rglob('*') if p.is_file()}

    def test_all_legacy_live_constructors_reject_before_any_database_or_transport(self):
        from network import Network
        from dune_live import Live
        from dune_r1 import RevisionLive as R1
        from dune_r2 import RevisionLive as R2
        from context_access_r3 import ContextDune, RpcAccess
        from rpc_context_r1 import RpcContextClient
        before=self.files()
        with patch('urllib.request.build_opener',side_effect=AssertionError('Transport must not run')) as transport:
            for constructor in (Network,Live,R1,R2,ContextDune,RpcAccess,RpcContextClient):
                with self.subTest(constructor=constructor.__module__),self.assertRaises(LegacyEntryBlocked):constructor(self.w)
            self.assertFalse(transport.called)
        self.assertEqual(self.files(),before)

    def test_legacy_functions_and_migrations_reject_before_input_reads_or_writes(self):
        from network import preflight
        from context_access_r3 import migrate, session, dune_usage, execute_dune
        from budget_r2 import migrate_revision
        from pilot_actions_r2 import execute
        def enter():
            with session(self.w,'SHARED','legacy'):pass
        calls=[lambda:preflight(self.w),lambda:migrate(self.w,self.w/'missing'),
               lambda:migrate_revision(self.w,self.w/'missing',{}),enter,
               lambda:dune_usage(self.w),lambda:execute_dune(self.w,'SHARED',self.w/'missing','legacy'),
               lambda:execute(self.w,'SHARED',self.w/'missing','legacy')]
        before=self.files()
        for call in calls:
            with self.assertRaises(LegacyEntryBlocked):call()
        self.assertEqual(self.files(),before)

    def test_standard_old_ledger_paths_cannot_create_independent_budget(self):
        from budget import Ledger
        from budget_r1 import RevisionLedger as R1
        from budget_r2 import RevisionLedger as R2
        before=self.files()
        for constructor in (Ledger,R1,R2):
            for name in ('shared_budget.sqlite','shared_budget_r1.sqlite','shared_budget_r2.sqlite','shared_budget_r3.sqlite'):
                with self.subTest(constructor=constructor.__module__,name=name),self.assertRaises(LegacyEntryBlocked):
                    constructor(self.w/'private'/name)
        self.assertEqual(self.files(),before)

    def test_new_r4_ledger_path_stays_available(self):
        from budget_r2 import RevisionLedger
        ledger=RevisionLedger(self.w/'private/shared_budget_r4.sqlite')
        self.assertTrue((self.w/'private/shared_budget_r4.sqlite').is_file())
        self.assertIn('dune_credits',ledger.snapshot())
        self.assertFalse((self.w/'private/shared_budget.sqlite').exists())

    def test_explicit_unmarked_historical_workspace_is_not_inferred_from_parent(self):
        from network import Network
        old=self.w/'historical_fixture';(old/'private').mkdir(parents=True)
        network=Network(old)
        self.assertTrue((old/'private/shared_budget.sqlite').exists())
        self.assertEqual(network.work,old)

    def test_public_r4_marker_blocks_but_public_r3_marker_preserves_history(self):
        from network import Network
        from budget import Ledger
        self.marker.unlink()
        public=self.w/'configs/STAGE1B_R4_PUBLIC_POLICY.json';public.write_text('{}')
        before=self.files()
        with self.assertRaises(LegacyEntryBlocked):Network(self.w)
        with self.assertRaises(LegacyEntryBlocked):Ledger(self.w/'private/shared_budget.sqlite')
        self.assertEqual(self.files(),before)
        public.unlink();(self.w/'configs/STAGE1B_R3_PUBLIC_POLICY.json').write_text('{}')
        self.assertEqual(Network(self.w).work,self.w)
        self.assertTrue((self.w/'private/shared_budget.sqlite').is_file())

    def test_network_object_created_before_marker_cannot_call_meta_after_marker(self):
        from network import Network
        self.marker.unlink();network=Network(self.w);self.marker.write_text('{}')
        before=self.files()
        with patch('urllib.request.build_opener',side_effect=AssertionError('No Meta or other network')) as transport:
            with self.assertRaises(LegacyEntryBlocked):network.call('metasleuth','batch_labels',payload={'addresses':[]})
            self.assertFalse(transport.called)
        self.assertEqual(self.files(),before)

    def test_unscoped_or_explicit_r4_publicnode_transport_is_blocked(self):
        from rpc_context_r1 import http_transport
        with patch('urllib.request.build_opener',side_effect=AssertionError('No PublicNode network')) as transport:
            with self.assertRaises(LegacyEntryBlocked):http_transport({},16,work=self.w)
            with patch('rpc_context_r1.__file__',str(self.w/'src/rpc_context_r1.py')):
                with self.assertRaises(LegacyEntryBlocked):http_transport({},16)
            self.assertFalse(transport.called)

    def test_all_normal_old_cli_paths_fail_before_side_effects(self):
        plan=self.w/'plans.json';plan.write_text('{}')
        local_src=self.w/'src';local_src.mkdir()
        (local_src/'dune_live.py').write_bytes((self.src/'dune_live.py').read_bytes())
        cases=[(self.src/'network.py',[str(self.w)]),
               (self.src/'dune_r1.py',['usage','--work',str(self.w)]),
               (self.src/'dune_r2.py',['snapshot','--work',str(self.w)]),
               (local_src/'dune_live.py',['poll','--folder',str(self.w/'missing')]),
               (self.src/'rpc_context_r1.py',['--work',str(self.w),'--method','eth_getCode','--params-json-file',str(plan),'--resource-check-file',str(plan)]),
               (self.src/'pilot_actions_r2.py',['--work',str(self.w),'--probe','SHARED','--freeze',str(plan),'--label','legacy'])]
        code="""import sys,runpy,urllib.request
source,search=sys.argv[1:3]
sys.path.insert(0,search)
def forbidden(*a,**k):raise AssertionError('SYNTHETIC_TRANSPORT_REACHED')
urllib.request.build_opener=forbidden
sys.argv=[source]+sys.argv[3:]
runpy.run_path(source,run_name='__main__')
"""
        before=self.files()
        for source,args in cases:
            result=subprocess.run([sys.executable,'-B','-c',code,str(source),str(self.src),*args],capture_output=True,text=True)
            with self.subTest(source=source.name):
                self.assertNotEqual(result.returncode,0)
                self.assertIn('LegacyEntryBlocked',result.stderr)
                self.assertNotIn('SYNTHETIC_TRANSPORT_REACHED',result.stderr)
                self.assertEqual(self.files(),before)

    def test_new_r4_dune_constructor_and_real_export_helpers_remain_enabled(self):
        case=dune_support.DuneR4Tests();case.setUp();self.addCleanup(case.tearDown)
        (case.w/'configs').mkdir(exist_ok=True);(case.w/'configs/STAGE1B_R4_POLICY.json').write_text('{}')
        from dune_r4 import ContextDuneR4
        live=ContextDuneR4(case.w,case.transport,clock=case.clock,sleeper=case.clock.sleep)
        self.assertTrue(live.export(case.folder)['progress']['complete'])
        self.assertEqual(len(case.calls),1)
        self.assertFalse((case.w/'private/shared_budget.sqlite').exists())

    def test_new_r4_rpc_constructor_and_real_costed_read_remain_enabled(self):
        case=rpc_support.ContextAccessR4Tests();case.setUp();self.addCleanup(case.tearDown)
        result=case.access().call_batch(case.plans(),'SHARED','guard_positive')
        self.assertEqual(result['status'],'COMPLETE')
        self.assertGreater(result['actual_operations_this_call'],0)
        self.assertFalse((case.w/'private/shared_budget.sqlite').exists())


if __name__=='__main__':unittest.main()
