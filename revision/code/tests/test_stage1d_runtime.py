import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from dataclasses import asdict
from collector import Event,Scope, NATIVE
from stage1d_runtime import Runtime,AUTH
from stage1d_acquisition import interval_sql,CachedIntervals,Labels
from context_access_r3 import sha

class Stage1DRuntimeTests(unittest.TestCase):
    def test_inherited_role_mapping_stops_known_protocols(self):
        labels=Labels.__new__(Labels);labels.success=set()
        for role,kind in [('DEX_OR_PROTOCOL','UNSUPPORTED_PROTOCOL'),('BRIDGE_BOUNDARY','BRIDGE'),('MIXER_BOUNDARY','MIXER'),('SERVICE','SERVICE')]:
            labels.registry={'synthetic':{'identity_class':role,'provenance_status':'SUPPORTED'}}
            self.assertEqual(labels('synthetic')['kind'],kind)
        labels.registry={'synthetic':{'identity_class':'SERVICE','provenance_status':'PROVENANCE_UNRESOLVED'}}
        self.assertEqual(labels('synthetic')['kind'],'UNKNOWN')
    def test_separate_query_clocks_preserve_total_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            w=Path(tmp);p=w/'private/stage1d_sessions';p.mkdir(parents=True)
            for n,q in enumerate(('q1','q2')):(p/f'{n}.json').write_text(json.dumps({'query':q,'closed':True,'elapsed_seconds':5400}))
            self.assertEqual(Runtime().clock_used(w),10800)
            self.assertEqual(Runtime().clock_used(w,'q1'),5400)
            self.assertEqual(Runtime().clock_used(w,'q2'),5400)
    def test_unclosed_session_cannot_reset_clock(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'private/stage1d_sessions';p.mkdir(parents=True)
            (p/'x.json').write_text(json.dumps({'closed':False,'elapsed_seconds':0}))
            with self.assertRaises(RuntimeError):Runtime().clock_used(Path(tmp))
    def test_raw_budget_excludes_only_inherited_file_identities(self):
        with tempfile.TemporaryDirectory() as tmp:
            w=Path(tmp);(w/'private').mkdir();(w/'raw').mkdir()
            (w/'raw/old').write_bytes(b'old');(w/'raw/new').write_bytes(b'new facts')
            (w/'private/STAGE1D_LEDGER_MIGRATION.json').write_text(json.dumps({'inherited_raw_files':{'raw/old':{}}}))
            self.assertEqual(Runtime().raw_risk(w),9)
    def test_candidate_sql_partitioned_and_no_reference_filter(self):
        a='0x'+'1'*40;b='0x'+'2'*40
        sql=interval_sql([a,b],10,100,0,108*86400)
        self.assertIn('IN ('+a+','+b+')',sql)
        self.assertIn("DATE '1970-01-01' AND DATE '1970-04-19'",sql)
        self.assertEqual(sql.count('FROM ethereum.traces'),1)
        self.assertNotIn(' LIMIT ',sql)
    def test_gate_rejects_changed_source_before_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            w=Path(tmp);(w/'src').mkdir();(w/'src/access.py').write_text('changed')
            (w/'STAGE1D_PREFLIGHT_GATE.json').write_text(json.dumps({'status':'PASS','authorization_id':AUTH,'old_and_new_tests_passed':True,'source_sha256':{'access.py':'0'*64}}))
            with self.assertRaisesRegex(RuntimeError,'source changed'):Runtime().require_gate(w)
    def test_old_partial_interval_not_complete_full_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            w=Path(tmp);p=w/'derived/stage1d/intervals';p.mkdir(parents=True)
            ep=p/'a.events.json';ep.write_text('[]')
            (p/'a.coverage.json').write_text(json.dumps({'addresses':['a'],'events_path':ep.relative_to(w).as_posix(),'events_sha256':sha(ep),
                'start_block':1,'end_block':999,'start_time':0,'end_time':90*86400,'complete':True,'evidence_id':'old'}))
            provider=CachedIntervals(w)
            self.assertFalse(provider.fetch_interval('a',NATIVE,1,999,start_time=0,end_time=108*86400,global_end_time=108*86400).complete)
            self.assertTrue(provider.fetch_interval('a',NATIVE,1,999,start_time=0,end_time=90*86400,global_end_time=108*86400).complete)

if __name__=='__main__':unittest.main()
