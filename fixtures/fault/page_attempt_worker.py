"""Synthetic process-death/concurrency worker. Its 'dispatch' writes a marker only."""
import argparse,os,sys
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--source-root');p.add_argument('--db');p.add_argument('--marker');p.add_argument('--die',action='store_true');p.add_argument('--provider',action='store_true');a=p.parse_args()
sys.path.insert(0,a.source_root)
from page_attempts import AttemptStore,RequestBlocked
def callback_marker():
    with Path(a.marker).open('a',encoding='utf-8') as f:f.write('SYNTHETIC_CALLBACK\n');f.flush();os.fsync(f.fileno())
    if a.die:os._exit(17)
if a.provider:
    from provider_dune import DuneProvider
    from collector import NATIVE
    def execute(*args):return {'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED'}
    def export(*args):
        callback_marker()
        return {'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED','result':{'rows':[],'metadata':{'row_count':0,'total_row_count':0}}}
    provider=DuneProvider(execute,export,Path(a.db).parent/'provider_cache')
    result=provider.fetch_interval('0x'+'1'*40,NATIVE,1,10,start_time=1,end_time=10,global_end_time=10)
    sys.exit(0 if result.complete else 20)
store=AttemptStore(a.db)
identity=AttemptStore.identity('SYNTHETIC','job','A'*26,'results',{'limit':2,'offset':0})
try:
    store.dispatch(identity)
except RequestBlocked:
    sys.exit(20)
callback_marker()
sys.exit(0)
