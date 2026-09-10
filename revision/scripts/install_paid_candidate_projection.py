"""Install the isolated checked caller connection only after the writer closes."""
from pathlib import Path
import sys,json,hashlib
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from prepare_current_weth import safe_point
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
from stage1d_runtime import Runtime
S=R/'staging/paid_candidate_projection';out=R/'reports/paid_candidate_projection/source_before'
Runtime().require_gate(C);safe_point(C)
if list((R/'operations').glob('*DRIVER.lock')):raise ValueError('An outer acquisition driver still exists')
latest=sorted(S.glob('TEST_RECEIPT_*.json'))[-1];tests=read(latest)
if tests['tests']!=10 or tests['failures'] or tests['errors'] or tests['skipped']:raise ValueError('Staged regression is not PASS')
origin=read(S/'STAGING_SOURCE.json')
if sha(C/'src/stage1d_acquisition.py')!=origin['original_sha256']:raise ValueError('Live caller changed during staging')
out.mkdir(parents=True,exist_ok=False)
backups=[]
for source in (C/'STAGE1D_PREFLIGHT_GATE.json',C/'src/stage1d_acquisition.py',R/'REPAIR_CONTROL.json'):
 target=out/source.name;target.write_bytes(source.read_bytes());assert sha(source)==sha(target)
 backups.append(dict(original=source.relative_to(R).as_posix(),backup=target.relative_to(R).as_posix(),sha256=sha(target)))
control=read(R/'REPAIR_CONTROL.json')
if control['research_network_allowed'] is not True:raise ValueError('Do not override a separate pause or gate')
control.update(research_network_allowed=False,phase='OFFLINE_REPAIR',
 route_gate_reason='Install tested exact paid-native coverage projection into actual cache consumer at a completed writer safe point',
 route_gate_started_at_utc=now())
atomic_json(R/'REPAIR_CONTROL.json',control)
installed=[]
for file,folder in [('stage1d_acquisition.py','src'),('stage1d_paid_candidate_projection.py','src'),
                    ('test_stage1d_paid_candidate_projection.py','tests')]:
 src=S/file;dst=C/folder/file
 if dst.exists() and file!='stage1d_acquisition.py':raise ValueError('New module already exists')
 dst.write_bytes(src.read_bytes());assert sha(dst)==sha(src)
 installed.append(dict(path=dst.relative_to(R).as_posix(),sha256=sha(dst)))
receipt=dict(status='INSTALLED_AWAITING_ACTUAL_SOURCE_GATE',utc=now(),source_backups=backups,installed=installed,
 staged_tests=dict(path=latest.relative_to(R).as_posix(),sha256=sha(latest)),new_external_requests=0,
 budget_reset=False,source_zero_claimed=False)
atomic_json(out/'INSTALL_RECEIPT.json',receipt);print(json.dumps(receipt))
