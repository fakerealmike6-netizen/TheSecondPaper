"""Current-demand-only import of legacy RPC point evidence, never graph/results.

prepare_one is read-only. Only explicit apply_one/apply_many writes, under the
existing one-writer lock. No HTTP client, provider dispatch or financial ledger
mutation exists here. Imported success preserves all prior failed attempts.
"""
from __future__ import annotations
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

from collector import Scope
from context_ledger_r3 import integer, normalize_anchor, normalize_rows
from stage1d_context import _headers
from stage1d_context_online import _merge
from stage1d_runtime import Runtime
from stage1d_closure_scope import active_batch_path
from stage1d_recovery_policy import _policy as recovery_policy, POLICY_PATH, POLICY_SHA
from read_retry_r4 import ReadRetryStore, logical_key

AUTH = "STAGE1D_RECOVERY_ROUTING_REUSE_V1"
PROVIDER = "ALCHEMY_ETH_MAINNET_EXISTING"
ALLOWED_METHODS = {"eth_getTransactionByHash", "eth_getTransactionReceipt",
    "eth_getBlockByNumber", "eth_getBlockByHash", "eth_getBalance", "eth_getCode"}
SCHEMA = "stage1d-legacy-rpc-point-admission-v1"
MAX_RAW_BYTES = 16 * 1024 * 1024


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def exact_request_sha(plan):
    # Independently verified against current seed requests and original manifests.
    return digest({"chain_id":1,**plan})


def _inside(path, root, *, links=False):
    path,root=Path(path).absolute(),Path(root).resolve()
    resolved=path.resolve()
    if resolved!=root and not resolved.is_relative_to(root):
        raise ValueError("Path escapes declared root")
    if not links:
        current=root
        for part in path.relative_to(root).parts:
            current=current/part
            if current.is_symlink() or getattr(current,"is_junction",lambda:False)():
                raise ValueError("Linked evidence path is not admitted")
    return resolved


def _verified_bytes(path, *, expected_sha, expected_bytes=None, limit=64*1024*1024):
    if not isinstance(expected_sha,str) or not re.fullmatch(r"[0-9a-fA-F]{64}",expected_sha):
        raise ValueError("Required source SHA missing")
    before=path.stat()
    if before.st_size>limit or expected_bytes is not None and before.st_size!=expected_bytes:
        raise ValueError("Evidence size differs or exceeds bounded admission")
    data=path.read_bytes();after=path.stat()
    if (before.st_mtime_ns,before.st_size)!=(after.st_mtime_ns,after.st_size):
        raise ValueError("Evidence changed during read")
    if hashlib.sha256(data).hexdigest()!=expected_sha.lower():
        raise ValueError("Evidence SHA mismatch")
    return data


def _read(path):
    return json.loads(path.read_bytes().decode("utf-8-sig"))


def _hex(value,digits):
    if not isinstance(value,str) or not re.fullmatch(r"0x[0-9a-fA-F]{"+str(digits)+"}",value):
        raise ValueError("Exact hexadecimal identity required")
    return value.lower()


def _write_immutable(path, data):
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():
        if path.read_bytes()!=data:
            raise ValueError("Immutable import artifact differs")
        return False
    with path.open("xb") as handle:
        handle.write(data)
    return True


def _stamp(path):
    info=path.stat()
    return info.st_size,info.st_mtime_ns,info.st_ino


@contextmanager
def _readonly(path):
    db=sqlite3.connect(path.resolve().as_uri()+"?mode=ro",uri=True)
    db.row_factory=sqlite3.Row
    try:
        db.execute("BEGIN")
        yield db
    finally:
        db.close()


class LegacyPointImporter:
    def __init__(self,work,index_path,*,runtime=None,synthetic=False,synthetic_raw_root=None):
        self.work=Path(work).resolve()
        self.synthetic=synthetic
        self.runtime=runtime or Runtime()
        self.index=_inside(index_path,self.work if synthetic else self.work.parent)
        self.index_sha=hashlib.sha256(self.index.read_bytes()).hexdigest()
        self.index_stamp=_stamp(self.index)
        self.freeze_path=_inside(active_batch_path(self.work),self.work)
        self.freeze_bytes=self.freeze_path.read_bytes()
        self.freeze_sha=hashlib.sha256(self.freeze_bytes).hexdigest()
        self.queries={q["query_id"]:q for q in json.loads(self.freeze_bytes)["queries"]}
        self.policy_path=None
        self.policy_bytes=None
        if synthetic:
            self.raw_root=_inside(synthetic_raw_root,self.work)
            self.allowed_groups=None
        else:
            if synthetic_raw_root is not None:
                raise ValueError("Production whitelist cannot be overridden")
            self.policy_path=_inside(self.work/POLICY_PATH,self.work)
            self.policy_bytes=_verified_bytes(self.policy_path,expected_sha=POLICY_SHA)
            policy=recovery_policy(self.work)
            if policy.get("authorization_id")!=AUTH or policy.get("legacy_reuse",{}).get("authorized") is not True:
                raise ValueError("Explicit recovery legacy reuse authority required")
            raw_root=Path(policy["legacy_reuse"]["roots"]["raw"])
            if raw_root.is_symlink() or getattr(raw_root,"is_junction",lambda:False)():
                raise ValueError("Declared legacy raw root cannot be a link")
            self.raw_root=raw_root.resolve()
            self.allowed_groups=set(policy["legacy_reuse"]["priority_groups"])
        self._manifest_cache={}
        self._evidence_cache={}

    def _bind_need(self,query,need):
        if _inside(active_batch_path(self.work),self.work)!=self.freeze_path:
            raise ValueError("Active batch freeze changed; re-open importer")
        if self.freeze_path.read_bytes()!=self.freeze_bytes:
            raise ValueError("Current batch freeze changed; re-open importer")
        if self.policy_path is not None and self.policy_path.read_bytes()!=self.policy_bytes:
            raise ValueError("Frozen legacy reuse policy changed; re-open importer")
        frozen=self.queries.get(query.get("query_id"))
        if frozen is None or frozen!=query:
            raise ValueError("Caller query is not the exact current frozen query")
        scope=Scope.from_policy(query)
        if query.get("scope_id")!=scope.scope_id or query.get("scope_hash")!=scope.scope_hash:
            raise ValueError("Current frozen scope identity mismatch")
        if any(need.get(k)!=query[k] for k in ("query_id","scope_id","scope_hash")):
            raise ValueError("Current demand has different query/scope identity")
        if not isinstance(need.get("reason"),str) or not need["reason"] or not need.get("evidence_refs"):
            raise ValueError("Current independent demand reason and source refs required")
        for ref in need["evidence_refs"]:
            p=_inside(self.work/ref["path"],self.work)
            if not self.synthetic and any(part in {"reference","catalog","experts","legacy_rpc","stage1d_legacy_import"}
                                          for part in p.relative_to(self.work).parts):
                raise ValueError("Legacy/expert evidence cannot originate a new demand")
            reference_fingerprint=(ref["sha256"],ref.get("bytes"),_stamp(p))
            if self._evidence_cache.get(str(p))!=reference_fingerprint:
                _verified_bytes(p,expected_sha=ref["sha256"],expected_bytes=ref.get("bytes"))
                self._evidence_cache[str(p)]=reference_fingerprint
        plan=self.runtime.validate_rpc({"method":need["method"],"params":need["params"]})
        if plan["method"] not in ALLOWED_METHODS:
            raise ValueError("Only explicitly needed RPC point evidence is admitted")
        if plan["method"]=="eth_getBlockByNumber" and plan["params"][1] is not False:
            raise ValueError("Hydrated old blocks are outside this bounded importer")
        block=need.get("expected_block")
        if type(block) is not int or not scope.start_block-1<=block<=scope.end_block:
            raise ValueError("Point must be inside frozen range or its immediate prior anchor block")
        if plan["method"]=="eth_getBlockByNumber" and integer(plan["params"][0])!=block:
            raise ValueError("Header request and needed block differ")
        if plan["method"] in {"eth_getBalance","eth_getCode"}:
            selector=plan["params"][1]
            if not isinstance(selector,dict) and integer(selector)!=block:
                raise ValueError("Historical state request and needed block differ")
            if isinstance(selector,dict):
                current=self._current_success({"method":"eth_getBlockByNumber","params":[hex(block),False]})
                if not current or current["payload"]["hash"].lower()!=selector["blockHash"].lower():
                    raise ValueError("Hash state selector lacks current exact numbered header binding")
        return scope,plan

    def _current_success(self,plan):
        path=self.work/"private/read_retry_r4.sqlite"
        if not path.is_file():
            return None
        identity=self.runtime.rpc_identity(PROVIDER,plan)
        with _readonly(path) as db:
            row=db.execute("SELECT * FROM read_requests WHERE logical_key=?",(logical_key(identity),)).fetchone()
            if row is None:return None
            if json.loads(row["identity_json"])!=identity:
                raise ValueError("Current RPC logical identity conflict")
            if row["state"]=="IN_FLIGHT":
                raise ValueError("Current RPC writer has an in-flight request")
            if row["state"]!="SUCCESS":return None
            receipt=json.loads(row["success_receipt"])
            artifact=_inside(self.work/receipt["artifact_path"],self.work)
            env=json.loads(_verified_bytes(artifact,expected_sha=receipt["artifact_sha256"]))
            request,response=env["request"],env["response"]
            value=json.loads(row["success_payload"])
            if ({k:request.get(k) for k in ("method","params")}!=plan
                    or self.runtime.rpc_result_status(request,response)!="SUCCESS_VALIDATED"
                    or response.get("result")!=value):
                raise ValueError("Current successful RPC artifact failed validation")
            return {"payload":value,"receipt":receipt,"logical_key":logical_key(identity)}

    def _manifest(self,row,source):
        path=_inside(row["manifest_path"],self.raw_root)
        if self.allowed_groups is not None and path.relative_to(self.raw_root).parts[0] not in self.allowed_groups:
            raise ValueError("Legacy group is outside the adopted metadata index whitelist")
        if path.name!="cache_manifest.json":
            raise ValueError("Only original RPC cache_manifest metadata admitted")
        fingerprint=(source["sha256"],source["bytes"],path.stat().st_mtime_ns,path.stat().st_size)
        cached=self._manifest_cache.get(str(path))
        if cached is None or cached[0]!=fingerprint:
            data=_verified_bytes(path,expected_sha=source["sha256"],expected_bytes=source["bytes"])
            manifest=json.loads(data)
            self._manifest_cache[str(path)]=(fingerprint,data,manifest)
        else:
            _,data,manifest=cached
        entry=manifest["entries"][row["ordinal"]]
        if Path(entry["file"]).name!=entry["file"]:
            raise ValueError("Manifest payload must be a direct local hash filename")
        if (entry["method"]!=row["method"] or entry["request_sha256"].lower()!=row["request_sha256"]
                or entry["response_sha256"].lower()!=row["response_sha256"]
                or entry["bytes"]!=row["declared_bytes"]
                or _inside(path.parent/entry["file"],self.raw_root)!=_inside(row["payload_path"],self.raw_root)):
            raise ValueError("Manifest row and current index disagree")
        return path,data

    def _normalize(self,query,need,plan,response,evidence_ids=None):
        if (not isinstance(response,dict) or response.get("jsonrpc")!="2.0"
                or type(response.get("id")) not in (int,str)):
            raise ValueError("Original raw must be a complete JSON-RPC response")
        request={"jsonrpc":"2.0","id":response["id"],**plan}
        if self.runtime.rpc_result_status(request,response)!="SUCCESS_VALIDATED":
            raise ValueError("Current strict Runtime rejects original response")
        method,value,block=plan["method"],response["result"],need["expected_block"]
        facts={"normalizer":"CURRENT_RUNTIME_POINT_VALIDATION","context_complete":False,"covered_ranges":0,
               "evidence_ids":list(evidence_ids or [])}
        if method in {"eth_getBlockByNumber","eth_getBlockByHash"}:
            if integer(value["number"])!=block:
                raise ValueError("Historical header block differs")
            _hex(value["hash"],64);_hex(value["parentHash"],64)
            integer(value["timestamp"])
            if not isinstance(value.get("transactions"),list):
                raise ValueError("Header full transaction hash list required")
            hashes=[_hex(h,64) for h in value["transactions"]]
            if len(set(hashes))!=len(hashes):raise ValueError("Duplicate header transaction identities")
            if block==query["start_block"] and query.get("start_block_hash") and value["hash"].lower()!=query["start_block_hash"].lower():
                raise ValueError("Frozen start header conflict")
            if block==query["end_block"] and query.get("end_block_hash") and value["hash"].lower()!=query["end_block_hash"].lower():
                raise ValueError("Frozen end header conflict")
            for edge in ("start","end"):
                if block==query[edge+"_block"]:
                    timestamp=int(datetime.fromisoformat(query[edge+"_time_utc"].replace("Z","+00:00")).timestamp())
                    if integer(value["timestamp"])!=timestamp:
                        raise ValueError("Frozen boundary timestamp conflict")
            facts.update(normalizer="stage1d_context._headers",normalized=_headers({block:value}))
        elif method in {"eth_getTransactionByHash","eth_getTransactionReceipt"}:
            if integer(value["blockNumber"])!=block:raise ValueError("Current needed transaction block differs")
            _hex(value["blockHash"],64);integer(value["transactionIndex"])
            if plan["params"][0].lower()==query["seed_tx_hash"].lower():
                seed=query["seed_event"]
                if (block!=seed["block"] or value["blockHash"].lower()!=seed["block_hash"].lower()
                        or integer(value["transactionIndex"])!=seed["tx_index"]):
                    raise ValueError("Frozen seed physical position conflict")
            if method=="eth_getTransactionByHash":
                _hex(value["from"],40)
                if value.get("to") is not None:_hex(value["to"],40)
                if "to" not in value:raise ValueError("Transaction recipient missing")
                integer(value["value"]);integer(value["gas"])
                if not isinstance(value.get("input"),str) or not re.fullmatch(r"0x(?:[a-fA-F0-9]{2})*",value["input"]):
                    raise ValueError("Transaction calldata missing")
                if plan["params"][0].lower()==query["seed_tx_hash"].lower():
                    if (_hex(value["from"],40)!=query["seed_from"].lower()
                        or _hex(value["to"],40)!=query["seed_to"].lower()
                        or integer(value["value"])!=integer(query["seed_amount_raw"])):
                        raise ValueError("Frozen seed integer or endpoints conflict")
                kind="transaction"
            else:
                if integer(value["status"]) not in (0,1):raise ValueError("Receipt status missing/invalid")
                integer(value["gasUsed"])
                integer(value["effectiveGasPrice"])
                if plan["params"][0].lower()==query["seed_tx_hash"].lower():
                    seed=query["seed_event"]
                    if (bool(integer(value["status"])) is not seed["success"]
                        or integer(value["gasUsed"])!=seed["gas_used"]
                        or integer(value["effectiveGasPrice"])!=seed["gas_price"]):
                        raise ValueError("Frozen seed execution or fee conflict")
                if not isinstance(value.get("logs"),list):raise ValueError("Receipt logs missing")
                for log in value["logs"]:
                    if (_hex(log["transactionHash"],64)!=plan["params"][0].lower()
                        or _hex(log["blockHash"],64)!=value["blockHash"].lower()
                        or integer(log["blockNumber"])!=block
                        or integer(log["transactionIndex"])!=integer(value["transactionIndex"])):
                        raise ValueError("Receipt log physical binding conflict")
                    integer(log["logIndex"]);_hex(log["address"],40)
                    if not isinstance(log.get("topics"),list):raise ValueError("Receipt log topics missing")
                    for topic in log["topics"]:_hex(topic,64)
                    if not isinstance(log.get("data"),str) or not re.fullmatch(r"0x(?:[a-fA-F0-9]{2})*",log["data"]):
                        raise ValueError("Receipt log bytes missing")
                    if log.get("removed") is not False:raise ValueError("Receipt log removed status not established")
                kind="receipt"
            normalized=normalize_rows([dict(value,record_type=kind,evidence_ids=list(evidence_ids or []))])
            if normalized.get("conflicts"):raise ValueError("Current strict ledger normalizer found conflict")
            facts.update(normalizer="context_ledger_r3.normalize_rows",normalized=normalized,
                         point_semantics="RECEIPT_ONLY_NON_VALUE_EVIDENCE" if kind=="receipt" else "TRANSACTION_POINT_STATUS_REQUIRES_RECEIPT")
        elif method=="eth_getBalance":
            header_plan={"method":"eth_getBlockByNumber","params":[hex(block),False]}
            current=self._current_success(header_plan)
            header=current["payload"] if current else None
            normalized=normalize_anchor(request,response,header,evidence_ids)
            facts.update(normalizer="context_ledger_r3.normalize_anchor",normalized=normalized,
                         point_semantics="BLOCK_END_BALANCE",block_hash_bound=normalized["block_hash"] is not None)
        elif method=="eth_getCode":
            facts.update(point_semantics="HISTORICAL_CODE_ONLY_NOT_SERVICE_IDENTITY",normalized=value)
        if need.get("known_current_value") is not None:
            known=need["known_current_value"]
            if isinstance(known,dict) and isinstance(value,dict):_merge(known,value)
            elif known!=value:raise ValueError("Current known point conflicts with legacy result")
        return request,facts

    def prepare_one(self,query,need):
        """Read-only admission audit; never persist or claim any method outcome."""
        try:
            _,plan=self._bind_need(query,need)
            identity=self.runtime.rpc_identity(PROVIDER,plan)
            current=self._current_success(plan)
            base={"schema":SCHEMA,"query_id":query["query_id"],"scope_id":query["scope_id"],
                "scope_hash":query["scope_hash"],"need":deepcopy(need),"plan":plan,
                "logical_key":logical_key(identity),"request_sha256":exact_request_sha(plan),
                "freeze_sha256":self.freeze_sha,"index_sha256":self.index_sha,
                "freeze_path":self.freeze_path.relative_to(self.work).as_posix(),
                "legacy_reuse_policy_path":self.policy_path.relative_to(self.work).as_posix() if self.policy_path else None,
                "legacy_reuse_policy_sha256":hashlib.sha256(self.policy_bytes).hexdigest() if self.policy_bytes else None,
                "new_network_requests":0,"new_rpc_operations":0,"new_alchemy_cu":0,
                "new_bigquery_scanned_bytes":0,"coverage_complete":False}
            if current:
                return dict(base,status="CURRENT_SUCCESS_REUSED_LEGACY_NOT_READ",current_receipt=current["receipt"])
            if _stamp(self.index)!=self.index_stamp:
                raise ValueError("Read-only metadata index changed; re-open and re-audit importer")
            with _readonly(self.index) as db:
                rows=[dict(r) for r in db.execute("SELECT * FROM rpc_entries WHERE method=? AND request_sha256=? ORDER BY manifest_path,ordinal",
                                                  (plan["method"],base["request_sha256"]))]
                if not rows:return dict(base,status="NO_EXACT_LEGACY_REQUEST_MATCH")
                variants=sorted({r["response_sha256"] for r in rows})
                if len(variants)!=1:return dict(base,status="LEGACY_RESPONSE_VARIANTS_QUARANTINED",response_sha256_variants=variants)
                sources={r["manifest_path"]:dict(db.execute("SELECT * FROM metadata_sources WHERE path=?",(r["manifest_path"],)).fetchone()) for r in rows}
            if _stamp(self.index)!=self.index_stamp:
                raise ValueError("Read-only metadata index changed during lookup")
            row=min(rows,key=lambda r:(sources[r["manifest_path"]]["bytes"],r["manifest_path"],r["ordinal"]))
            manifest_path,manifest_bytes=self._manifest(row,sources[row["manifest_path"]])
            payload_path=_inside(row["payload_path"],self.raw_root)
            if not re.fullmatch(r"[a-fA-F0-9]{64}\.json",payload_path.name):
                raise ValueError("Only content-hash-named RPC raw response allowed")
            if payload_path.stem.lower()!=row["response_sha256"]:
                raise ValueError("Original raw filename and SHA disagree")
            raw=_verified_bytes(payload_path,expected_sha=row["response_sha256"],expected_bytes=row["declared_bytes"],limit=MAX_RAW_BYTES)
            response=json.loads(raw.decode("utf-8-sig"))
            request,normalized=self._normalize(query,need,plan,response,["legacy_raw_sha256:"+row["response_sha256"],
                "legacy_manifest_sha256:"+sources[row["manifest_path"]]["sha256"]])
            return dict(base,status="ADMISSIBLE_POINT_PENDING_ROOT_APPLY",legacy_source={
                "raw_path":str(payload_path),"raw_sha256":row["response_sha256"],"raw_bytes":len(raw),
                "manifest_path":str(manifest_path),"manifest_sha256":sources[row["manifest_path"]]["sha256"],
                "manifest_bytes":len(manifest_bytes),"manifest_ordinal":row["ordinal"],
                "same_response_manifest_locations":len(rows),"request_parameters_reconstructed_by_exact_hash":True},
                normalization=normalized,_raw=raw,_manifest=manifest_bytes,_request=request,_response=response)
        except (ValueError,KeyError,TypeError,IndexError,FileNotFoundError) as exc:
            return {"schema":SCHEMA,"status":"POINT_ADMISSION_QUARANTINED","query_id":query.get("query_id"),
                    "need":deepcopy(need),"error_class":type(exc).__name__,"reason":str(exc),
                    "new_network_requests":0,"coverage_complete":False}

    def _commit(self,prepared):
        if prepared["status"]!="ADMISSIBLE_POINT_PENDING_ROOT_APPLY":return prepared
        source=prepared["legacy_source"];key=prepared["logical_key"]
        private=self.work/"private/stage1d_legacy_import"
        admission_key=digest({"logical_key":key,"need":prepared["need"],"raw_sha256":source["raw_sha256"]})
        admission_path=private/"admissions"/(admission_key+".json")
        admission={k:v for k,v in prepared.items() if not k.startswith("_")}
        # Durable admission precedes copied artifacts and cache SUCCESS. If the
        # process stops mid-commit, root can inspect this exact local boundary.
        _write_immutable(admission_path,canonical(admission))
        raw_path=self.work/"raw/legacy_rpc"/(source["raw_sha256"]+".json")
        new_raw=_write_immutable(raw_path,prepared["_raw"])
        manifest_path=private/"manifests"/(source["manifest_sha256"]+".json")
        _write_immutable(manifest_path,prepared["_manifest"])
        envelope={"schema":SCHEMA,"provider_alias":PROVIDER,
            "evidence_kind":"SYNTHETIC_LEGACY_IMPORT" if self.synthetic else "REAL_CHAIN_LEGACY_RAW_REUSE",
            "request":prepared["_request"],"response":prepared["_response"],"status":"SUCCESS_VALIDATED",
            "response_complete":True,"raw_body_sha256":source["raw_sha256"],
            "raw_path":raw_path.relative_to(self.work).as_posix(),
            "manifest_path":manifest_path.relative_to(self.work).as_posix(),"legacy_source":source,
            "import_is_new_provider_dispatch":False,"original_http_status_not_preserved":True,
            "strict_normalizer":prepared["normalization"]["normalizer"]}
        envelope_path=private/"envelopes"/(key+"_"+source["raw_sha256"]+".json")
        envelope_bytes=canonical(envelope);_write_immutable(envelope_path,envelope_bytes)
        receipt={"artifact_path":envelope_path.relative_to(self.work).as_posix(),
                 "artifact_sha256":hashlib.sha256(envelope_bytes).hexdigest(),
                 "legacy_raw_sha256":source["raw_sha256"],"local_import":True,
                 "historical_attempt_total":"UNKNOWN_PRESERVED_NO_NEW_DISPATCH",
                 "admission_path":admission_path.relative_to(self.work).as_posix(),
                 "admission_sha256":hashlib.sha256(canonical(admission)).hexdigest(),
                 "new_network_requests":0,"new_alchemy_cu":0}
        inventory_path=self.work/"private/stage1d_recovery_imported_storage.json"
        inventory=_read(inventory_path) if inventory_path.exists() else {"schema":SCHEMA,"files":{}}
        relative=raw_path.relative_to(self.work).as_posix()
        record={"bytes":source["raw_bytes"],"sha256":source["raw_sha256"]}
        if relative in inventory["files"] and inventory["files"][relative]!=record:
            raise ValueError("Imported storage classification conflict")
        inventory["files"][relative]=record
        temp=inventory_path.with_suffix(".tmp")
        temp.write_bytes(canonical(inventory));temp.replace(inventory_path)
        accounting={"new_network_requests":0,"new_rpc_operations":0,"new_alchemy_cu":0,
            "legacy_cost_and_risk_preserved":True,"is_local_evidence_import":True,
            "historical_attempt_total":"UNKNOWN_NOT_ASSERTED_ONE"}
        # This API appends an evidence-import row, preserves every historical
        # attempt and does not mutate the financial ledger or any old cost.
        imported=ReadRetryStore(self.work/"private/read_retry_r4.sqlite").import_attempt(
            self.runtime.rpc_identity(PROVIDER,prepared["plan"]),
            "stage1d-legacy-point:"+key+":"+source["raw_sha256"],
            dispatched=False,outcome="SUCCESS",payload=prepared["_response"]["result"],
            accounting=accounting,receipt=receipt)
        result={k:v for k,v in prepared.items() if not k.startswith("_")}
        result.update(status="IMPORTED_POINT_SUCCESS",cache_import=imported,receipt=receipt,
                      imported_storage_added_bytes=source["raw_bytes"] if new_raw else 0,
                      original_raw_copied_without_change=True,covered_ranges=0)
        completion={"schema":SCHEMA,"status":"IMPORTED_POINT_SUCCESS","logical_key":key,"receipt":receipt,
                    "covered_ranges":0,"new_network_requests":0,"new_alchemy_cu":0}
        _write_immutable(private/"completions"/(admission_key+".json"),canonical(completion))
        return result

    def apply_many(self,query,needs):
        """Explicit root-only write entry; caller may first inspect prepare_one."""
        if not isinstance(needs,list) or len(needs)>10000:
            raise ValueError("Finite explicit current needed request list required")
        lock=self.work/"private/network_worker.lock"
        with lock.open("x",encoding="utf-8") as handle:
            json.dump({"pid":os.getpid(),"kind":"OFFLINE_LEGACY_POINT_IMPORT","authorization_id":AUTH},handle)
        results=[]
        call_path=self.work/"private/stage1d_legacy_import/calls"/(uuid.uuid4().hex+".json")
        try:
            # Prepare again while holding the writer lock, including current
            # cache success checks; no stale approval may replace newer facts.
            for need in needs:
                results.append(self._commit(self.prepare_one(query,need)))
            report={"schema":SCHEMA,"authorization_id":AUTH,"query_id":query["query_id"],
                "results":results,"new_network_requests":0,"financial_ledger_mutated":False,
                "reused_point_facts":sum(r["status"]=="IMPORTED_POINT_SUCCESS" for r in results),
                "imported_storage_added_bytes":sum(r.get("imported_storage_added_bytes",0) for r in results),
                "covered_ranges":0,"context_complete":False,"created_at_unix":time.time()}
            _write_immutable(call_path,canonical(report))
            return report
        except Exception as exc:
            _write_immutable(call_path,canonical({"schema":SCHEMA,"status":"LOCAL_IMPORT_INTERRUPTED",
                "query_id":query.get("query_id"),"completed_results":results,"error_class":type(exc).__name__,
                "new_network_requests":0,"covered_ranges":0,"context_complete":False}))
            raise
        finally:
            lock.unlink()

    def apply_one(self,query,need):
        return self.apply_many(query,[need])["results"][0]


def serializable_preparation(prepared):
    """Serializable private audit view (contains true-chain locators; not public release)."""
    return {k:v for k,v in prepared.items() if not k.startswith("_")}
