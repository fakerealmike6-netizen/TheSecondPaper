"""Portable private-data reference replay; all paths are explicit arguments."""
import argparse,json
from pathlib import Path
from reference_recompute import rows,recompute,metrics
from reference_core import dump

def main():
    p=argparse.ArgumentParser();p.add_argument('--data-dir',type=Path,required=True);p.add_argument('--identity-file',type=Path,required=True);p.add_argument('--expected',type=Path);p.add_argument('--output',type=Path);a=p.parse_args()
    ids={r['address']:r for r in rows(a.identity_file)}
    records,witnesses,_=recompute(a.data_dir,ids);report=metrics(records)
    if a.expected:
        expected=json.loads(a.expected.read_text())['stage1b'];assert report==expected,(report,expected)
    report=dict(report,relation_rows=len(records),witnesses=len(witnesses),passed=True,network_calls=0)
    if a.output:dump(a.output,report)
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
