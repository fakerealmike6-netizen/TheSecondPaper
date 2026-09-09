import copy,json,sqlite3,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from stage1d_finite_state_route import cached,validate_plan_list,execute,key,sha
from stage1d_finite_state_rpc import balance_plan,FiniteStateRuntime
from read_retry_r4 import logical_key

class FiniteRouteTests(unittest.TestCase):
 def test_cached_exact_balance_reuses_verified_bytes(self):
  with tempfile.TemporaryDirectory() as d:
   w=Path(d);(w/'private').mkdir();p=balance_plan('0x'+'1'*40,123)
   request={'jsonrpc':'2.0','id':'saved',**p};response={'jsonrpc':'2.0','id':'saved','result':'0x'+'0'*64}
   artifact=w/'saved.json';artifact.write_text(json.dumps({'request':request,'response':response}))
   receipt={'artifact_path':'saved.json','artifact_sha256':sha(artifact)}
   runtime=FiniteStateRuntime([p]);ident=runtime.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',p)
   db=sqlite3.connect(w/'private/read_retry_r4.sqlite')
   db.execute('CREATE TABLE read_requests(logical_key TEXT,state TEXT,success_payload TEXT,success_receipt TEXT)')
   db.execute('INSERT INTO read_requests VALUES(?,?,?,?)',(logical_key(ident),'SUCCESS',json.dumps(response['result']),json.dumps(receipt)));db.commit();db.close()
   got,missing=cached(w,[p]);self.assertEqual(len(got),1);self.assertEqual(missing,[])
   got,missing=cached(w,[balance_plan('0x'+'1'*40,124)]);self.assertEqual(got,[]);self.assertEqual(len(missing),1)
   artifact.write_text('{}')
   with self.assertRaisesRegex(ValueError,'artifact changed'):cached(w,[p])
 def test_large_missing_routes_before_any_access(self):
  plans=[balance_plan('0x'+format(i,'040x'),123) for i in range(1,202)]
  requirement={'plans':plans,'query_name':'q','purpose':'NECESSARY_ASSET_LEDGER'}
  with tempfile.TemporaryDirectory() as d:
   path=Path(d)/'requirement.json';path.write_text('{}')
   with patch('stage1d_finite_state_route.verify',return_value=requirement),patch('stage1d_finite_state_route.cached',return_value=([],plans)),patch('stage1d_finite_state_route.RpcAccess') as access:
    result=execute(d,path);self.assertEqual(result['status'],'BATCH_BINDING_REQUIRED')
    self.assertEqual(result['missing_selectors'],plans);self.assertEqual(result['actual_operations_this_call'],0);access.assert_not_called()
 def test_no_selector_alias_or_unrestricted_call(self):
  p=balance_plan('0x'+'1'*40,123)
  with self.assertRaises(ValueError):validate_plan_list([p,p])
  bad=copy.deepcopy(p);bad['params'][1]='latest'
  with self.assertRaises(ValueError):validate_plan_list([bad])
  with self.assertRaises(ValueError):validate_plan_list([{'method':'debug_traceTransaction','params':['0x'+'1'*64,{}]}])

if __name__=='__main__':unittest.main()
