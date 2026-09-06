"""Portable offline unittest suite. No API credentials or outbound sockets."""
import argparse,io,json,os,socket,sys,tempfile,unittest
from pathlib import Path

def main():
    base=Path(__file__).resolve().parents[1]
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,default=base/'09_TEST_RESULTS.json');a=p.parse_args()
    tmp=base/'.test_tmp';tmp.mkdir(exist_ok=True);tempfile.tempdir=str(tmp)
    for key in list(os.environ):
        if any(x in key.upper() for x in ('API_KEY','TOKEN','CREDENTIALS')): os.environ.pop(key,None)
    attempts=[]
    def blocked(*args,**kwargs):
        attempts.append('outbound socket blocked');raise RuntimeError('OFFLINE_TEST_NETWORK_FORBIDDEN')
    socket.create_connection=blocked;socket.socket.connect=blocked
    sys.path.insert(0,str(base/'src'))
    stream=io.StringIO();suite=unittest.defaultTestLoader.discover(str(base/'tests'))
    result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)
    a.output.parent.mkdir(parents=True,exist_ok=True)
    log=a.output.with_suffix('.txt');log.write_text(stream.getvalue(),encoding='utf-8')
    data={'tests_run':result.testsRun,'passed':result.testsRun-len(result.failures)-len(result.errors)-len(result.skipped),'failed':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),'success':result.wasSuccessful(),'network_attempts':attempts,'fixtures':'synthetic only; private original project tests excluded','test_log':log.name,'failures':[{'test':str(t),'traceback':d} for t,d in result.failures+result.errors]}
    a.output.write_text(json.dumps(data,indent=2),encoding='utf-8');print(json.dumps({k:v for k,v in data.items() if k!='failures'},indent=2))
    return 0 if data['success'] and not attempts else 1

if __name__=='__main__': raise SystemExit(main())
