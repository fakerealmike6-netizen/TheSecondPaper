"""Current revision wrapper for the preserved offline future-arrival grouper."""
from pathlib import Path
import argparse,importlib.util,json,sys,socket
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
p=argparse.ArgumentParser();p.add_argument('--name',required=True);p.add_argument('--batch-size',type=int,choices=(5,100),default=100);a=p.parse_args()
if not a.name.replace('_','').isalnum():raise ValueError('Simple new preparation name required')
def denied(*args,**kwargs):raise RuntimeError('Current cost preparation is offline')
socket.socket.connect=denied;socket.create_connection=denied
path=R/'staging/unknown_cost_future_code_preparation/prepare_future_historical_code.py'
spec=importlib.util.spec_from_file_location('_current_future_code_preparation',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
result=module.prepare(R,a.batch_size,'reports/unknown_cost_boundary_v1/preparations/'+a.name+'.json')
from stage1d_current_code_preparation import validate
validate(C,{'path':result['path'],'sha256':result['sha256']})
print(json.dumps(result),flush=True)
