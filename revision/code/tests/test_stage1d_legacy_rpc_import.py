"""Synthetic filesystem/SQLite evidence only; all temporary paths stay staged."""
from copy import deepcopy
from contextlib import closing
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

STAGED=Path(__file__).resolve().parent
SRC=STAGED.parent/"src"
sys.path.insert(0,str(SRC))
sys.path.insert(0,str(STAGED))
from collector import Scope
from stage1d_context_online import _cached_rpc
from stage1d_legacy_rpc_import import LegacyPointImporter,canonical,exact_request_sha,serializable_preparation,PROVIDER
from stage1d_runtime import Runtime
from read_retry_r4 import ReadRetryStore

TX="0x"+"a"*64
BH="0x"+"b"*64
OTHER_BH="0x"+"c"*64
A="0x"+"1"*40
B="0x"+"2"*40
T=1700000000
VALUE=2**127+1

def sha(data):return hashlib.sha256(data).hexdigest()
def utc(stamp):return datetime.fromtimestamp(stamp,timezone.utc).isoformat().replace("+00:00","Z")
def response(value):return {"jsonrpc":"2.0","id":7,"result":value}
def tx():return {"hash":TX,"blockHash":BH,"blockNumber":"0x64","transactionIndex":"0x0",
    "from":A,"to":B,"value":hex(VALUE),"gas":"0x5208","gasPrice":"0x2","input":"0x"}
def receipt():return {"transactionHash":TX,"blockHash":BH,"blockNumber":"0x64","transactionIndex":"0x0",
    "status":"0x1","gasUsed":"0x5208","effectiveGasPrice":"0x2","logs":[],"from":A,"to":B}
def header(block=100):return {"number":hex(block),"hash":BH if block==100 else OTHER_BH,
    "parentHash":"0x"+"d"*64,"timestamp":hex(T+(block-100)*12),"transactions":[TX] if block==100 else []}


class ImporterTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix="synthetic_",dir=STAGED)
        self.work=Path(self.tmp.name).resolve()
        self.raw=self.work/"legacy_raw";self.raw.mkdir()
        (self.work/"private").mkdir()
        self.query={"query_id":"qry:synthetic","name":"synthetic","start_block":100,"end_block":102,
            "start_block_hash":BH,"end_block_hash":OTHER_BH,"start_time_utc":utc(T),"end_time_utc":utc(T+24),
            "max_acquisition_depth":2,"window_mode":"REFERENCE_FULL","local_window_seconds":None,
            "seed_tx_hash":TX,"seed_from":A,"seed_to":B,"seed_amount_raw":str(VALUE),
            "seed_event":{"block":100,"block_hash":BH,"tx_index":0,"success":True,"gas_used":21000,"gas_price":2}}
        scope=Scope.from_policy(self.query)
        self.query.update(scope_id=scope.scope_id,scope_hash=scope.scope_hash,scope=scope.freeze_dict())
        self.freeze=self.work/"private/BATCH_QUERY_FREEZE.json"
        self.freeze.write_bytes(canonical({"queries":[self.query]}))
        evidence=self.work/"private/current_needs.json"
        evidence.write_bytes(canonical({"synthetic":True,"query_id":self.query["query_id"]}))
        self.ref={"path":evidence.relative_to(self.work).as_posix(),"sha256":sha(evidence.read_bytes()),"bytes":evidence.stat().st_size}
        self.index=self.work/"metadata.sqlite"
        with closing(sqlite3.connect(self.index)) as db, db:
            db.executescript("""
                CREATE TABLE metadata_sources(path TEXT PRIMARY KEY,kind TEXT,sha256 TEXT,bytes INTEGER,mtime_utc TEXT);
                CREATE TABLE rpc_entries(manifest_path TEXT,ordinal INTEGER,method TEXT,request_sha256 TEXT,
                    response_sha256 TEXT,payload_path TEXT,declared_bytes INTEGER,provenance_roots_json TEXT,sealed INTEGER);
                CREATE INDEX rpc_request_lookup ON rpc_entries(method,request_sha256);
            """)
        self.network=patch.object(socket.socket,"connect",side_effect=AssertionError("NETWORK FORBIDDEN IN OFFLINE TEST"))
        self.network.start()

    def tearDown(self):
        self.network.stop()
        self.tmp.cleanup()

    def importer(self):
        return LegacyPointImporter(self.work,self.index,synthetic=True,synthetic_raw_root=self.raw)

    def need(self,method="eth_getTransactionByHash",params=None,block=100):
        return {**{k:self.query[k] for k in ("query_id","scope_id","scope_hash")},"method":method,
            "params":params if params is not None else [TX],"expected_block":block,
            "reason":"Synthetic current physical evidence gap","evidence_refs":[self.ref]}

    def add_raw(self,plan,value,group="arbitrary_old_case_name"):
        folder=self.raw/group;folder.mkdir()
        data=canonical(value);response_sha=sha(data);payload=folder/(response_sha+".json")
        payload.write_bytes(data)
        entry={"method":plan["method"],"request_sha256":exact_request_sha(plan).upper(),
            "response_sha256":response_sha.upper(),"bytes":len(data),"file":payload.name,"provenance_roots":[]}
        manifest=folder/"cache_manifest.json";manifest.write_bytes(canonical({"entries":[entry]}))
        with closing(sqlite3.connect(self.index)) as db, db:
            db.execute("INSERT INTO metadata_sources VALUES(?,?,?,?,?)",(str(manifest),"RPC_CACHE_MANIFEST",sha(manifest.read_bytes()),manifest.stat().st_size,"2026-08-25T00:00:00Z"))
            db.execute("INSERT INTO rpc_entries VALUES(?,?,?,?,?,?,?,?,?)",(str(manifest),0,plan["method"],exact_request_sha(plan),response_sha,str(payload),len(data),"[]",1))
        return payload,manifest

    def default_raw(self,value=None):
        need=self.need();self.add_raw({k:need[k] for k in ("method","params")},response(tx() if value is None else value))
        return need

    def rejected(self,need=None):
        result=self.importer().prepare_one(self.query,need or self.need())
        self.assertEqual(result["status"],"POINT_ADMISSION_QUARANTINED",result)
        self.assertFalse((self.work/"private/read_retry_r4.sqlite").exists())
        self.assertFalse((self.work/"raw").exists())
        return result

    def test_exact_codec_matches_fixed_synthetic_vector(self):
        result=exact_request_sha({"method":"eth_getTransactionByHash","params":[TX]})
        self.assertEqual(result,"da079b6e93084ff91815f80a72b5220a9484e2fd1c35cf9745ff0a616e1ec8fe")

    def test_prepare_readonly_point_exact_uint_and_no_case_inference(self):
        need=self.default_raw();importer=self.importer()
        before={str(p.relative_to(self.work)):sha(p.read_bytes()) for p in self.work.rglob("*") if p.is_file()}
        result=importer.prepare_one(self.query,need)
        self.assertEqual(result["status"],"ADMISSIBLE_POINT_PENDING_ROOT_APPLY",result)
        self.assertEqual(result["normalization"]["normalized"]["transactions"][0]["amount_raw"],str(VALUE))
        self.assertFalse(result["coverage_complete"])
        self.assertEqual(result["new_alchemy_cu"],0)
        self.assertIn("arbitrary_old_case_name",result["legacy_source"]["raw_path"])
        self.assertNotIn("_raw",serializable_preparation(result))
        after={str(p.relative_to(self.work)):sha(p.read_bytes()) for p in self.work.rglob("*") if p.is_file()}
        self.assertEqual(before,after)

    def test_no_exact_params_match(self):
        self.default_raw();need=self.need(params=["0x"+"e"*64])
        result=self.importer().prepare_one(self.query,need)
        self.assertEqual(result["status"],"NO_EXACT_LEGACY_REQUEST_MATCH")

    def test_variants_quarantined_before_payload_read(self):
        need=self.default_raw();other=response(tx());other["id"]=8
        self.add_raw({k:need[k] for k in ("method","params")},other,"another_case")
        for p in self.raw.glob("*/*.json"):
            if p.name!="cache_manifest.json":p.unlink()
        result=self.importer().prepare_one(self.query,need)
        self.assertEqual(result["status"],"LEGACY_RESPONSE_VARIANTS_QUARANTINED")
        self.assertEqual(len(result["response_sha256_variants"]),2)

    def test_payload_sha_tamper(self):
        self.default_raw();payload=next(p for p in self.raw.glob("*/*.json") if p.name!="cache_manifest.json")
        data=payload.read_bytes();payload.write_bytes(data.replace(b'"id":7',b'"id":8'))
        self.assertIn("SHA mismatch",self.rejected()["reason"])

    def test_payload_size_tamper(self):
        self.default_raw();payload=next(p for p in self.raw.glob("*/*.json") if p.name!="cache_manifest.json")
        payload.write_bytes(payload.read_bytes()+b" ")
        self.assertIn("size differs",self.rejected()["reason"])

    def test_manifest_sha_tamper(self):
        self.default_raw();manifest=next(self.raw.glob("*/cache_manifest.json"))
        manifest.write_bytes(manifest.read_bytes().replace(b'"entries"',b'"enTRIES"'))
        self.assertIn("SHA mismatch",self.rejected()["reason"])

    def test_index_payload_escape_rejected_before_read(self):
        self.default_raw()
        with closing(sqlite3.connect(self.index)) as db, db:db.execute("UPDATE rpc_entries SET payload_path=?",(str(self.work/"forbidden.json"),))
        self.assertIn("escapes",self.rejected()["reason"])

    def test_index_changed_after_binding_rejected(self):
        self.default_raw();importer=self.importer()
        with closing(sqlite3.connect(self.index)) as db, db:db.execute("UPDATE rpc_entries SET declared_bytes=2")
        result=importer.prepare_one(self.query,self.need())
        self.assertEqual(result["status"],"POINT_ADMISSION_QUARANTINED")
        self.assertIn("index changed",result["reason"])

    def test_query_scope_and_freeze_cannot_be_changed(self):
        self.default_raw();importer=self.importer();query=deepcopy(self.query);query["max_acquisition_depth"]+=1
        self.assertEqual(importer.prepare_one(query,self.need())["status"],"POINT_ADMISSION_QUARANTINED")
        self.freeze.write_bytes(self.freeze.read_bytes()+b" ")
        self.assertIn("freeze changed",importer.prepare_one(self.query,self.need())["reason"])

    def test_need_wrong_scope_and_outside_range(self):
        self.default_raw();need=self.need();need["scope_hash"]="e"*64
        self.assertIn("scope identity",self.rejected(need)["reason"])
        self.assertIn("inside frozen range",self.rejected(self.need(block=98))["reason"])

    def test_current_evidence_ref_required_and_sha_bound(self):
        self.default_raw();need=self.need();need["evidence_refs"]=[]
        self.assertIn("source refs required",self.rejected(need)["reason"])
        need=self.need();need["evidence_refs"][0]["sha256"]="f"*64
        self.assertIn("SHA mismatch",self.rejected(need)["reason"])

    def test_hydrated_trace_and_moving_state_prohibited(self):
        for need in [self.need("eth_getBlockByNumber",["0x64",True]),
                self.need("debug_traceTransaction",[TX,{"tracer":"callTracer","tracerConfig":{"withLog":True}}]),
                self.need("eth_getBalance",[A,"latest"])]:
            self.rejected(need)

    def test_float_wei_missing_amount_and_seed_conflict_quarantined(self):
        for field in (float(VALUE),None,hex(VALUE+1)):
            with self.subTest(field=field):
                value=tx();value["value"]=field
                importer=self.importer()
                with self.assertRaises(ValueError):importer._normalize(self.query,self.need(),{"method":"eth_getTransactionByHash","params":[TX]},response(value))

    def test_current_normalizer_rejects_alias_conflict(self):
        value=tx();value["amount_raw"]=str(VALUE+1)
        self.default_raw(value)
        self.rejected()

    def test_known_current_field_conflict(self):
        self.default_raw();need=self.need();need["known_current_value"]={"value":hex(VALUE+1)}
        self.assertIn("disagrees",self.rejected(need)["reason"])

    def test_seed_position_and_receipt_fee_binding(self):
        importer=self.importer();plan={"method":"eth_getTransactionByHash","params":[TX]};value=tx();value["transactionIndex"]="0x1"
        with self.assertRaisesRegex(ValueError,"position conflict"):importer._normalize(self.query,self.need(),plan,response(value))
        for field,bad in (("status","0x0"),("gasUsed","0x1"),("effectiveGasPrice","0x3")):
            value=receipt();value[field]=bad;plan={"method":"eth_getTransactionReceipt","params":[TX]}
            with self.assertRaisesRegex(ValueError,"execution or fee conflict"):importer._normalize(self.query,self.need(plan["method"]),plan,response(value))

    def test_receipt_only_never_supplies_zero_value(self):
        need=self.need("eth_getTransactionReceipt");self.add_raw({k:need[k] for k in ("method","params")},response(receipt()))
        result=self.importer().prepare_one(self.query,need)
        self.assertEqual(result["status"],"ADMISSIBLE_POINT_PENDING_ROOT_APPLY",result)
        self.assertEqual(result["normalization"]["normalized"]["transactions"],[])
        self.assertEqual(result["normalization"]["point_semantics"],"RECEIPT_ONLY_NON_VALUE_EVIDENCE")

    def test_receipt_log_physical_binding(self):
        value=receipt();value["logs"]=[{"transactionHash":TX,"blockHash":OTHER_BH,"blockNumber":"0x64","transactionIndex":"0x0"}]
        need=self.need("eth_getTransactionReceipt");self.add_raw({k:need[k] for k in ("method","params")},response(value))
        self.assertIn("log physical binding",self.rejected(need)["reason"])

    def test_header_boundary_timestamp_cannot_be_forged(self):
        need=self.need("eth_getBlockByNumber",["0x64",False]);value=header();value["timestamp"]=hex(T+1)
        self.add_raw({k:need[k] for k in ("method","params")},response(value))
        self.assertIn("timestamp conflict",self.rejected(need)["reason"])

    def test_balance_without_header_stays_unbound_and_exact(self):
        need=self.need("eth_getBalance",[A,"0x64"]);self.add_raw({k:need[k] for k in ("method","params")},response(hex(VALUE)))
        result=self.importer().prepare_one(self.query,need)
        self.assertEqual(result["status"],"ADMISSIBLE_POINT_PENDING_ROOT_APPLY",result)
        norm=result["normalization"]["normalized"]
        self.assertEqual(norm["actual_balance_raw"],str(VALUE));self.assertEqual(norm["position"],"BLOCK_END")
        self.assertEqual(norm["verification_status"],"BLOCK_NUMBER_BOUND_HASH_MISSING")

    def test_hash_state_selector_needs_current_block_binding(self):
        need=self.need("eth_getCode",[A,{"blockHash":BH,"requireCanonical":True}])
        self.assertIn("numbered header binding",self.rejected(need)["reason"])

    def test_header_then_balance_and_empty_code_remain_point_facts(self):
        header_need=self.need("eth_getBlockByNumber",["0x64",False])
        balance_need=self.need("eth_getBalance",[A,"0x64"])
        code_need=self.need("eth_getCode",[A,{"blockHash":BH,"requireCanonical":True}])
        for need,value,group in ((header_need,header(),"headers"),(balance_need,hex(VALUE),"balance"),(code_need,"0x","code")):
            self.add_raw({k:need[k] for k in ("method","params")},response(value),group)
        importer=self.importer()
        result=importer.apply_many(self.query,[header_need,balance_need,code_need])
        self.assertEqual(result["reused_point_facts"],3,result)
        self.assertEqual(result["results"][1]["normalization"]["normalized"]["verification_status"],"BLOCK_IDENTITY_BOUND")
        self.assertEqual(result["results"][2]["normalization"]["point_semantics"],"HISTORICAL_CODE_ONLY_NOT_SERVICE_IDENTITY")
        self.assertEqual(result["covered_ranges"],0);self.assertFalse(result["context_complete"])

    def test_apply_cache_compatible_and_current_first_skips_legacy(self):
        need=self.default_raw();importer=self.importer();result=importer.apply_one(self.query,need)
        self.assertEqual(result["status"],"IMPORTED_POINT_SUCCESS",result)
        cached=_cached_rpc(self.work,[result["plan"]]);self.assertEqual(cached[0][1],tx())
        copied=self.work/"raw/legacy_rpc"/(result["legacy_source"]["raw_sha256"]+".json")
        self.assertEqual(copied.read_bytes(),canonical(response(tx())))
        for path in self.raw.glob("*/*.json"):path.unlink()
        prior=result["imported_storage_added_bytes"]
        again=importer.apply_one(self.query,need)
        self.assertEqual(again["status"],"CURRENT_SUCCESS_REUSED_LEGACY_NOT_READ",again)
        inventory=json.loads((self.work/"private/stage1d_recovery_imported_storage.json").read_bytes())
        self.assertEqual(sum(x["bytes"] for x in inventory["files"].values()),prior)

    def test_old_attempts_and_financial_file_unchanged_zero_new_cost(self):
        need=self.default_raw();identity=Runtime().rpc_identity(PROVIDER,{k:need[k] for k in ("method","params")})
        store=ReadRetryStore(self.work/"private/read_retry_r4.sqlite")
        store.import_attempt(identity,"synthetic-old-timeout",outcome="RETRYABLE_FAILURE",accounting={"cu_risk":20},error_class="READ_TIMEOUT")
        old=store.attempts(identity)[0]
        financial=self.work/"private/shared_budget_r4.sqlite";financial.write_bytes(b"SYNTHETIC OLD FINANCIAL BYTES")
        before=sha(financial.read_bytes());result=self.importer().apply_one(self.query,need)
        self.assertEqual(sha(financial.read_bytes()),before)
        attempts=store.attempts(identity);self.assertEqual(attempts[0],old);self.assertEqual(len(attempts),2)
        self.assertIsNone(attempts[-1]["dispatched_at"])
        self.assertEqual(json.loads(attempts[-1]["accounting_json"])["new_alchemy_cu"],0)
        self.assertEqual(result["new_rpc_operations"],0)

    def test_single_writer_lock_prevents_application(self):
        need=self.default_raw();lock=self.work/"private/network_worker.lock";lock.write_bytes(b"SYNTHETIC LIVE OWNER")
        with self.assertRaises(FileExistsError):self.importer().apply_one(self.query,need)
        self.assertEqual(lock.read_bytes(),b"SYNTHETIC LIVE OWNER")
        self.assertFalse((self.work/"raw").exists())

    def test_inflight_rpc_refuses_legacy(self):
        self.default_raw();identity=Runtime().rpc_identity(PROVIDER,{"method":"eth_getTransactionByHash","params":[TX]})
        store=ReadRetryStore(self.work/"private/read_retry_r4.sqlite");store.claim(identity)
        result=self.importer().prepare_one(self.query,self.need())
        self.assertEqual(result["status"],"POINT_ADMISSION_QUARANTINED",result)
        self.assertIn("in-flight",result["reason"])

    def test_crash_before_cache_write_keeps_admission_and_retries_idempotently(self):
        need=self.default_raw();importer=self.importer()
        with patch.object(ReadRetryStore,"import_attempt",side_effect=OSError("synthetic disk failure")):
            with self.assertRaises(OSError):importer.apply_one(self.query,need)
        self.assertFalse((self.work/"private/network_worker.lock").exists())
        self.assertEqual(len(list((self.work/"private/stage1d_legacy_import/admissions").glob("*.json"))),1)
        calls=[json.loads(p.read_bytes()) for p in (self.work/"private/stage1d_legacy_import/calls").glob("*.json")]
        self.assertEqual(calls[0]["status"],"LOCAL_IMPORT_INTERRUPTED")
        result=importer.apply_one(self.query,need)
        self.assertEqual(result["status"],"IMPORTED_POINT_SUCCESS",result)
        self.assertEqual(result["imported_storage_added_bytes"],0)
        self.assertEqual(len(_cached_rpc(self.work,[result["plan"]])),1)


if __name__=="__main__":unittest.main(verbosity=2)
