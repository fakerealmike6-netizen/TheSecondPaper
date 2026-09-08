"""Extracted-package execution using the inherited tested offline guard."""
import argparse, json, shutil, platform
from pathlib import Path
from datetime import datetime,timezone
from validate_review_bundle_r1 import Validator, input_path, read, write, tree_hashes
from archive_safety import verify_manifest
from run_stage1c import verify_freeze

def result_projection(data):
    result={}
    for method,v in data.items():
        result[method]={k:v.get(k) for k in ('status','applicability','output_kind','input_fact_hash','scope_hash','label_version','positive_addresses')}
        for category in ('addresses','events','joint_by_asset'):
            result[method][category]=None if v.get(category) is None else {key:{field:row.get(field) for field in ('lower_raw','upper_raw','point_raw','nominal_raw','marked','reachable')} for key,row in v[category].items()}
        result[method]['allocation_raw']=v.get('allocation_raw')
    return result

class Stage1CValidator(Validator):
    def __init__(self,tree,output,kind):
        super().__init__(tree,output,kind)
        if (self.tree/'controlled_v1').exists():shutil.copytree(self.tree/'controlled_v1',self.mirror/'controlled_v1')

    def batch(self,selection):
        folder=self.out/('stage1c_'+selection)
        if self.command('stage1c_'+selection,'src/run_stage1c.py',
            ['--tree',self.tree,'--kind',self.kind,'--selection',selection,'--output',folder]):
            result=read(folder/'RESULTS_INDEX.json')
            expected_count=60 if selection=='controlled' else 2
            self.check(selection+'_result_receipt',result.get('passed') is True and result.get('sample_count')==expected_count and result.get('input_unchanged') is True,
                samples=result.get('sample_count'),success=result.get('passed'))
            saved=self.tree/'results'/selection
            checks=[]
            for row in result['method_results_index']:
                name=row['path'];prior=input_path(saved,name+'/METHOD_RESULTS.json')
                current=folder/name/'METHOD_RESULTS.json'
                checks.append(result_projection(read(prior))==result_projection(read(current)))
            self.check(selection+'_fixed_result_reproduction',all(checks) and len(checks)==expected_count,samples_compared=len(checks),
                comparison='Exact typed intervals/point/nominal/sets/input identities; nonunique LP witnesses and host timings excluded, independently re-audited in runner')

    def run_stage1c(self):
        self.bounded('payload_manifest',lambda:self.check('payload_manifest_verified',verify_manifest(self.tree/'12_FILE_HASHES.txt')>0))
        self.bounded('frozen_inputs',lambda:self.check('frozen_source_and_inputs_verified',verify_freeze(self.tree,self.kind)[0]['stage']=='Stage1C'))
        self.public_checks()
        self.bounded('controlled_experiments',lambda:self.batch('controlled'))
        if self.kind=='min':
            self.bounded('real_experiments',lambda:self.batch('real'))
            dest=self.out/'real_evidence_replay'
            if self.command('final_real_evidence_replay','src/stage1c_real_replay.py',['--tree',self.tree,'--output',dest]):
                receipt=read(dest/'REAL_REPLAY_RECEIPT.json')
                self.check('real_evidence_replay_receipt',receipt.get('passed') is True,receipt=receipt)
        else:self.skipped('private_real_models','Public package intentionally excludes private original evidence and two-real replay',required=False)
        self.check('frozen_tree_unchanged',tree_hashes(self.tree)==self.before)
        failures=self.validation_failures(['payload_manifest_verified','frozen_source_and_inputs_verified','unit_test_receipt','controlled_oracle_receipt','controlled_result_receipt','controlled_fixed_result_reproduction','frozen_tree_unchanged']+(['real_result_receipt','real_fixed_result_reproduction','real_evidence_replay_receipt'] if self.kind=='min' else []))
        result={'schema_version':'stage1c-extracted-validation-1.0','passed':not failures,'status':'PASS' if not failures else 'FAIL',
            'kind':self.kind,'runtime':platform.platform(),'python':platform.python_version(),
            'utc':datetime.now(timezone.utc).isoformat(),'checks':self.commands,'failures':failures,
            'guard':'Unmodified Stage1B-R1/R4 inherited OFFLINE_SITE/GUARDED_LAUNCH; sanitized child environment; sockets/DNS blocked; only guarded Python subprocesses; writes confined to new output.',
            'network_access_permitted':False,'credentials_required':False,'windows_executed':platform.system()=='Windows','linux_executed':platform.system()=='Linux'}
        write(self.out/'VALIDATION_RECEIPT.json',result)
        return result

def main():
    p=argparse.ArgumentParser();p.add_argument('--tree',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--kind',choices=('min','public'),required=True);a=p.parse_args()
    try:result=Stage1CValidator(a.tree,a.output,a.kind).run_stage1c()
    except Exception as exc:print(json.dumps({'status':'ERROR','reason':type(exc).__name__+': '+str(exc)}));return 1
    print(json.dumps({'status':result['status'],'checks':len(result['checks']),'failures':result['failures']}))
    return 0 if result['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
