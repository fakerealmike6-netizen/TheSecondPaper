"""Run the standalone LP tests and persist a machine-readable receipt."""
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import sys
import time
import unittest


def run(root):
    started=datetime.now(timezone.utc).isoformat();clock=time.perf_counter()
    suite=unittest.defaultTestLoader.discover(str(root/"tests"),pattern="test_lp*.py")
    stream=io.StringIO()
    result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)
    logs=root/"logs";logs.mkdir(exist_ok=True)
    derived=root/"derived";derived.mkdir(exist_ok=True)
    (logs/"lp_unittest.log").write_text(stream.getvalue(),encoding="utf-8")
    receipt={"scope":"OFFLINE_SYNTHETIC_LP","started_at_utc":started,"elapsed_seconds":round(time.perf_counter()-clock,6),
        "tests_run":result.testsRun,"passed":result.testsRun-len(result.failures)-len(result.errors)-len(result.skipped),
        "failures":len(result.failures),"errors":len(result.errors),"skipped":len(result.skipped),"all_passed":result.wasSuccessful(),
        "network_calls":0,"provider_integration_validated":False,"command":"python src/lp_test_receipt.py",
        "failures_detail":[{"test":str(t),"traceback":trace} for t,trace in result.failures+result.errors]}
    (derived/"lp_test_results.json").write_text(json.dumps(receipt,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(receipt,indent=2))
    return result.wasSuccessful()


if __name__=="__main__":
    raise SystemExit(0 if run(Path(__file__).resolve().parents[1]) else 1)
