"""Only isolated CLI/lock controls and AST comparison; never run replay/import C."""
import argparse,ast,contextlib,hashlib,io,json,os,pathlib,sys,time,uuid

S=pathlib.Path(__file__).resolve().parent
source=(S/'replay_current_scope.py').read_text(encoding='utf8')
tree=ast.parse(source)
checks=[]
def passed(name):checks.append({'name':name,'status':'PASS'})

start=next(i for i,n in enumerate(tree.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='parser' for t in n.targets))
end=next(i for i,n in enumerate(tree.body) if isinstance(n,ast.With))
cli=compile(ast.Module(body=tree.body[start:end],type_ignores=[]),'isolated_cli','exec')
original_argv=sys.argv[:]
try:
 for args,expected in [([],['txphish_src001','txphish_src002','xscam_src001','lifi_src001']),
      (['--query','txphish_src002'],['txphish_src002']),
      (['--query','txphish_src001','--query','txphish_src002'],['txphish_src001','txphish_src002']),
      (['--query','txphish_src002','--query','txphish_src001'],['txphish_src002','txphish_src001'])]:
  sys.argv=['candidate',*args];ns={'argparse':argparse};exec(cli,ns);assert ns['names']==expected
 passed('CLI default/single/pair/reversed supplied order')
 for args in (['--query','txphish_src001','--query','txphish_src001'],['--query','unknown']):
  sys.argv=['candidate',*args]
  with contextlib.redirect_stderr(io.StringIO()):
   try:exec(cli,{'argparse':argparse})
   except SystemExit as e:assert e.code==2
   else:raise AssertionError('Invalid selection accepted')
 passed('Duplicate and invalid query rejected before initialization')
finally:sys.argv=original_argv

gate_calls=[]
class FakeRuntime:
 failure=False
 def require_gate(self,work):
  assert (work/'private/network_worker.lock').is_file()
  gate_calls.append(str(work))
  if self.failure:raise RuntimeError('synthetic gate closed')
ns={'contextmanager':contextlib.contextmanager,'Path':pathlib.Path,'Runtime':FakeRuntime,
    'json':json,'os':os,'uuid':uuid,'time':time}
locknode=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='offline_replay_lock')
exec(compile(ast.Module(body=[locknode],type_ignores=[]),'isolated_lock','exec'),ns)
work=S/('scratch_'+uuid.uuid4().hex);(work/'private').mkdir(parents=True)
lock=work/'private/network_worker.lock';hold=ns['offline_replay_lock']
with hold(work,['txphish_src001','txphish_src002']):
 saved=lock.read_bytes();assert json.loads(saved)['online_clock_increment']==0
 try:
  with hold(work,['txphish_src002']):raise AssertionError('Second writer entered')
 except RuntimeError:pass
 assert lock.read_bytes()==saved
assert not lock.exists() and len(gate_calls)==1
passed('Exclusive pair lock held through body; nested writer rejected without deletion')
FakeRuntime.failure=True
try:
 with hold(work,['txphish_src001']):raise AssertionError('Closed gate body entered')
except RuntimeError as e:assert str(e)=='synthetic gate closed'
assert not lock.exists();FakeRuntime.failure=False
passed('Gate runs after lock and before body; failed gate releases owned lock')
try:
 with hold(work,['txphish_src001']):raise ValueError('synthetic replay failure')
except ValueError:pass
assert not lock.exists()
passed('Replay exception releases owned lock')
lock.write_bytes(b'synthetic pre-existing writer')
try:
 with hold(work,['txphish_src001']):raise AssertionError('Existing writer entered')
except RuntimeError:pass
assert lock.read_bytes()==b'synthetic pre-existing writer';lock.unlink()
passed('Pre-existing lock preserved')
try:
 with hold(work,['txphish_src001']):lock.write_bytes(b'synthetic replacement writer')
except RuntimeError as e:assert 'changed' in str(e)
assert lock.read_bytes()==b'synthetic replacement writer';lock.unlink()
passed('Changed lock preserved rather than deleting another owner')

old=ast.parse((S/'BASE_SOURCE.py').read_text(encoding='utf8'))
oldloop=next(n for n in old.body if isinstance(n,ast.For))
main=tree.body[end];newloop=next(n for n in main.body if isinstance(n,ast.For))
assert ast.dump(oldloop)==ast.dump(newloop)
passed('Entire per-query replay/archive/output loop AST unchanged')
for name in ('CachedIntervals','Labels','load_current_resolver'):
 calls=[n for n in ast.walk(main) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id==name]
 assert len(calls)==1
 assert not any(isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id==name for n in ast.walk(newloop))
assert not any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr in ('session','clock_used') for n in ast.walk(tree))
passed('Single shared initialization; no online session or clock mutation call')
receipt={'status':'PASS','checks':checks,'count':len(checks),'production_imports':0,'production_replay_runs':0,
 'network_requests':0,'scope':'ISOLATED_STAGED_SYNTHETIC_CLI_AND_LOCK_WITH_AST_CHECKS',
 'candidate_sha256':hashlib.sha256((S/'replay_current_scope.py').read_bytes()).hexdigest()}
(S/'CHECKS.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf8')
print(json.dumps(receipt))
