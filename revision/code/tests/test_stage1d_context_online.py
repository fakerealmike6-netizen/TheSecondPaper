"""Offline context scheduling, physical cache preservation and local gaps."""
from dataclasses import asdict
from pathlib import Path
import copy
import hashlib
import json
import tempfile
import unittest
from unittest.mock import patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import stage1d_context_online as online
from context_ledger_r3 import EvidenceConflict
from page_attempts import atomic_json
from read_retry_r4 import ReadRetryStore
from stage1d_runtime import Runtime
from stage1d_context import REQUIRED
import test_stage1d_context as facts


class Labels:
    def __init__(self,work):pass
    def __call__(self,address):return {'kind':'SERVICE' if address==facts.T else 'UNKNOWN'}


class ContextOnlineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.w=Path(self.tmp.name)
        self.q,self.c,seed,out=facts.base();self.c['candidate_events']=[asdict(seed),asdict(out)]
        self.c['states']=[{'state':{'address':facts.A,'asset':seed.asset}},{'state':{'address':facts.T,'asset':out.asset}}]
        self.q.update(seed_to=facts.A,start_block=10,end_block=12,start_time=1693612790,end_time=1693612820,scope_id='synthetic:scope',scope_hash='f'*64)
        self.qdir=self.w/'derived/stage1d/queries'/self.q['name'];self.qdir.mkdir(parents=True)
        self.root=self.qdir/'context';self.root.mkdir()
        atomic_json(self.qdir/'collection.json',self.c)
        atomic_json(self.w/'private/BATCH_QUERY_FREEZE.json',{'queries':[self.q]})
        atomic_json(self.w/'private/STAGE1D_POLICY.json',{'new_batch_resource_limits':{'context_events_per_query':50000}})
        self.calls=[];self.sqls=[];self.capacity=100;self.fail_headers=False;self.fail_balances=False
        self.seed,self.out=seed,out
        patches=[patch.object(online,'rpc',side_effect=self.rpc),patch.object(online,'_cached_rpc',return_value=[]),
            patch.object(online,'_rpc_capacity',side_effect=self.budget),patch.object(online,'Labels',Labels),
            patch.object(online,'execute_sql',side_effect=self.execute),patch.object(online,'result_rows',return_value=[])]
        for p in patches:p.start();self.addCleanup(p.stop)

    def budget(self,work):
        return {'rpc_operations':{'remaining':str(self.capacity)},'alchemy_cu':{'remaining':str(self.capacity*20)}},{m:20 for m in ['eth_getBalance','eth_getBlockByNumber','eth_getTransactionReceipt']}

    def rpc(self,work,plans,query,label):
        self.calls.append(copy.deepcopy(plans));self.capacity-=len(plans);members=[]
        for p in plans:
            method=p['method'];arg=p['params'][0]
            if method=='eth_getBlockByNumber':value={'number':arg,'hash':'0x'+format(int(arg,16),'064x'),'timestamp':hex(1693612790+(int(arg,16)-10)*12)}
            elif method=='eth_getBalance':value='0x14' if int(p['params'][1],16)==9 else '0xa'
            else:
                e=self.out if arg==self.out.tx_hash else self.seed
                value={'transactionHash':arg,'blockNumber':hex(e.block),'transactionIndex':hex(e.tx_index),'status':'0x1','gasUsed':'0x0','effectiveGasPrice':'0x0'}
            failed=method=='eth_getBlockByNumber' and self.fail_headers or method=='eth_getBalance' and self.fail_balances
            members.append({'status':'PERMANENT_FAILURE' if failed else 'SUCCESS_VALIDATED','result':value,'artifact_sha256':'a'*64})
        return {'members':members}

    def execute(self,work,freeze,query,label):
        self.sqls.append((freeze.parent/'query.sql').read_text())
        return {'status':'COMPLETED_EXPORTED','job_folder':'synthetic:job'}

    def run(self,*args,**kwargs):
        return super().run(*args,**kwargs)

    def invoke(self,**kwargs):return online.run_context(self.w,self.q,**kwargs)

    def save_prior(self,folder=None):
        folder=folder or self.root
        b,h=facts.anchors([(facts.A,9,20),(facts.A,11,10)])
        for number,value in h.items():value['timestamp']=hex(1693612790+(number-10)*12)
        atomic_json(folder/'headers.json',h);atomic_json(folder/'balances.json',b)
        receipts={self.out.tx_hash:{'request':{'method':'eth_getTransactionReceipt','params':[self.out.tx_hash]},'response':{'result':{'transactionHash':self.out.tx_hash,'blockNumber':'0xb','transactionIndex':'0x0','status':'0x1','gasUsed':'0x0','effectiveGasPrice':'0x0'}}}}
        atomic_json(folder/'receipts.json',receipts)
        return b,h

    def test_missing_headers_do_not_prevent_exact_block_whole_date_sql(self):
        self.fail_headers=True
        result=self.invoke(output_variant='round_1')
        self.assertEqual(1,len(self.sqls));self.assertIn("DATE '2023-09-01' AND DATE '2023-09-02'",self.sqls[0])
        self.assertIn(f'({facts.A},10,11)',self.sqls[0])
        self.assertNotIn("block_time BETWEEN",self.sqls[0])
        saved=json.loads((self.root/'round_1/context_result.json').read_text())
        self.assertTrue(any(g['type']=='CONTEXT_ANCHOR_BLOCK_HASH_MISSING' for g in saved['evidence_gaps']))
        self.assertEqual('PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS',result['context_status'])

    def test_new_round_retains_successes_and_old_root_bytes(self):
        self.save_prior();original={p.name:p.read_bytes() for p in self.root.iterdir() if p.is_file()}
        result=self.invoke(fetch_ledger=False,output_variant='round_2')
        self.assertEqual([],self.calls);self.assertEqual(2,result['known_balance_anchors'])
        self.assertEqual(original,{p.name:p.read_bytes() for p in self.root.iterdir() if p.is_file()})
        manifest=json.loads((self.root/'round_2/input_evidence_manifest.json').read_text())
        self.assertTrue(manifest['sources'])
        for row in manifest['sources']:self.assertEqual(row['sha256'],hashlib.sha256((self.w/row['snapshot_path']).read_bytes()).hexdigest())

    def test_failed_new_balance_does_not_erase_existing_anchor(self):
        b,h=self.save_prior();b.pop(facts.A+':11');atomic_json(self.root/'balances.json',b)
        self.fail_balances=True
        result=self.invoke(fetch_ledger=False,output_variant='round_partial')
        kept=json.loads((self.root/'round_partial/balances.json').read_text())
        self.assertEqual(1,len(kept));self.assertEqual('0x14',kept[facts.A+':9']['response']['result'])
        self.assertEqual(1,result['known_balance_anchors'])

    def test_complete_round_500_full_cache_prevents_sql_and_rpc(self):
        folder=self.root/'round_500_full';self.save_prior(folder)
        plan=online.required_context_windows(self.q,self.c)
        atomic_json(folder/'coverage.json',facts.complete(plan));atomic_json(folder/'ledger_rows.json',[])
        result=self.invoke(output_variant='round_reuse',account_selection='all')
        self.assertEqual([],self.calls);self.assertEqual([],self.sqls)
        self.assertEqual('COMPLETE_CONTEXT_CACHE_REUSED',result['acquisition']['status'])

    def test_physical_rows_deduplicate_and_contradiction_blocks(self):
        row=facts.raw_transaction(self.out)
        a=dict(row,evidence_ids=['one']);b=dict(row,evidence_ids=['two'])
        merged=online._merge_rows([a],[b]);self.assertEqual(1,len(merged));self.assertEqual(['one','two'],merged[0]['evidence_ids'])
        with self.assertRaises(EvidenceConflict):online._merge_rows([row],[row|{'value_raw':'91'}])

    def test_conflicting_headers_or_balances_cannot_win_by_new_round_order(self):
        b,h=self.save_prior();other=self.root/'v5';atomic_json(other/'balances.json',b|{facts.A+':9':{'result':'0x15'}})
        with self.assertRaises(EvidenceConflict):self.invoke(output_variant='round_conflict')
        self.assertEqual([],self.calls);self.assertEqual([],self.sqls)

    def test_all_selection_includes_leaf_after_debit_and_resource_gap_is_local(self):
        leaf=asdict(facts.event(3,facts.A,facts.B,1,11,1));self.c['candidate_events'].append(leaf)
        self.c['states'].append({'state':{'address':facts.B,'asset':leaf['asset']}});atomic_json(self.qdir/'collection.json',self.c)
        self.capacity=2
        result=self.invoke(output_variant='round_all',account_selection='all',fetch_anchor_headers=False)
        self.assertEqual(2,result['queried_accounts']);self.assertEqual(2,sum(len(x) for x in self.calls))
        self.assertTrue(all(p['params'][0]==facts.A for p in self.calls[0]))
        self.assertTrue(result['resource_gaps']);self.assertEqual(1,len(self.sqls));self.assertIn(f'({facts.B},11,11)',self.sqls[0])

    def test_zero_rpc_budget_still_runs_safe_context_sql(self):
        self.capacity=0
        result=self.invoke(output_variant='round_zero')
        self.assertEqual([],self.calls);self.assertEqual(1,len(self.sqls));self.assertTrue(result['resource_gaps'])

    def test_context_cap_preserves_all_raw_rows_and_blocks_model(self):
        atomic_json(self.w/'private/STAGE1D_POLICY.json',{'new_batch_resource_limits':{'context_events_per_query':1}})
        rows=[facts.raw_transaction(self.seed),facts.raw_transaction(self.out)]
        with patch.object(online,'result_rows',return_value=rows):result=self.invoke(output_variant='round_cap')
        self.assertEqual('CONTEXT_RESOURCE_LIMIT_MODEL_BLOCKED',result['context_status'])
        self.assertEqual(2,len(json.loads((self.root/'round_cap/ledger_rows.json').read_text())))
        self.assertFalse((self.root/'round_cap/model_input.json').exists())

    def test_date_domain_missing_account_does_not_hide_ready_account(self):
        rows=[{'address':facts.A,'ledger_start_block':10,'ledger_end_block':11},
              {'address':facts.B,'ledger_start_block':8,'ledger_end_block':9}]
        ready,domain=online._dates(self.w,self.q,rows,{})
        self.assertEqual([facts.A],[r['address'] for r in ready]);self.assertEqual(facts.B,domain['unready'][0]['address'])

    def test_production_nested_scope_times_supply_full_query_dates(self):
        self.q['scope']={k:self.q[k] for k in ('start_block','end_block','start_time','end_time')}
        del self.q['start_time'];del self.q['end_time']
        atomic_json(self.w/'private/BATCH_QUERY_FREEZE.json',{'queries':[self.q]})
        self.fail_headers=True
        result=self.invoke(output_variant='round_nested')
        self.assertEqual(1,len(self.sqls));self.assertIn("DATE '2023-09-01' AND DATE '2023-09-02'",self.sqls[0])
        self.assertFalse(any(x['type']=='CONTEXT_DATE_DOMAIN_UNPROVED' for x in result['resource_gaps']))

    def test_unsafe_output_path_rejected_before_network(self):
        with self.assertRaises(ValueError):self.invoke(output_variant='../old')
        self.assertEqual([],self.calls)


class PersistentCacheTests(unittest.TestCase):
    def test_success_payload_recovered_read_only_and_checked_against_artifact(self):
        with tempfile.TemporaryDirectory() as temp:
            work=Path(temp);plan={'method':'eth_getBalance','params':[facts.A,'0x9']}
            artifact=work/'raw/synthetic.json';request={'jsonrpc':'2.0','id':'synthetic',**plan}
            atomic_json(artifact,{'request':request,'response':{'jsonrpc':'2.0','id':'synthetic','result':'0x14'}})
            digest=hashlib.sha256(artifact.read_bytes()).hexdigest()
            store=ReadRetryStore(online.retry_path(work));identity=Runtime().rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',plan)
            store.import_attempt(identity,'synthetic',outcome='SUCCESS',payload='0x14',receipt={'artifact_path':'raw/synthetic.json','artifact_sha256':digest})
            self.assertEqual([(plan,'0x14',['sha256:'+digest])],online._cached_rpc(work,[plan]))
            artifact.write_text('{}')
            with self.assertRaises(EvidenceConflict):online._cached_rpc(work,[plan])


if __name__=='__main__':unittest.main()
