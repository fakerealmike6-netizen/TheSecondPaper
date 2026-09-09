"""Stage1D-only completed-export upper bounds from verified actual responses.

No requests occur. Read-only analysis is separate from explicit reconciliation.
The same conservative rates remain in force; this never claims final actuals.
"""
from decimal import Decimal, ROUND_CEILING
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
from datetime import datetime, timezone

from page_contract import validate_page, exact_count
from stage1d_costs import AUTH, Stage1DDune, canonical, inside
from context_access_r3 import RAW_CAP

POLICY = "STAGE1D_COMPLETED_RESPONSE_EXPORT_UPPER_V1"
PLAN_POLICY = "STAGE1D_UNIQUE_COMPLETE_REQUEST_SET_V1"
RATE = {"credits_per_decimal_MB": "20", "credits_per_1000_datapoints": "1",
        "minimum_whole_credits_per_request": "1", "round_each_request_up": True,
        "actual_export_charge_known": False}
KNOWN_COLUMNS = {
    "candidate": ["event_kind", "tx_hash", "sender", "recipient", "amount_raw", "block_number", "tx_index", "block_time",
                  "log_index", "trace_address", "success", "contract_address", "gas_used", "gas_price", "trace_type", "call_type"],
    "context": ["record_type", "block_number", "block_hash", "block_time", "tx_hash", "tx_index", "from_address", "to_address", "value_raw",
                "success", "gas_used", "effective_gas_price", "gas_limit", "input_data", "trace_address", "trace_type", "call_type", "subtraces",
                "error", "tx_success", "created_address", "refund_address", "withdrawal_index", "fee_recipient"],
    "frontier_labels": ["address", "cex_addresses_json", "cex_addresses_count", "labels_addresses_json", "labels_addresses_count",
                        "owner_addresses_json", "owner_addresses_count", "deposit_addresses_json", "deposit_addresses_count"]}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read(path):
    return json.loads(Path(path).read_bytes())


def rows(path, table):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("SELECT * FROM " + table)]


def page_upper(metadata, row_count, raw_bytes):
    names = metadata.get("column_names")
    if not isinstance(names, list) or not names or len(names) != len(set(names)):
        raise ValueError("Complete unique column metadata required")
    if exact_count(metadata.get("row_count"), "row_count") != row_count:
        raise ValueError("Page row count differs from physical rows")
    size = max(exact_count(metadata.get("result_set_bytes"), "result_set_bytes"), exact_count(raw_bytes, "raw_bytes"))
    points = max(row_count * len(names), exact_count(metadata.get("datapoint_count"), "datapoint_count"),
                 (size + 99) // 100)
    upper = max(Decimal(1), max(Decimal(size) * 20 / 1000000, Decimal(points) / 1000).to_integral_value(rounding=ROUND_CEILING))
    return {"row_count": row_count, "column_count": len(names), "metadata_datapoint_count": metadata["datapoint_count"],
        "metadata_result_set_bytes": metadata["result_set_bytes"], "original_http_json_bytes": raw_bytes,
        "conservative_billable_bytes": size, "conservative_datapoints": points, "upper_credits": str(upper),
        "is_actual": False}


def analyze(work, folder):
    """Revalidate one fully closed current-batch job without constructing a ledger."""
    work = Path(work).resolve()
    folder = inside(work, folder)
    sources = {}
    def bound(path, expected=None):
        path = inside(work, path)
        data = path.read_bytes()
        if expected is not None and digest(data) != expected:
            raise ValueError("File SHA256 mismatch: " + path.relative_to(work).as_posix())
        record = {"path": path.relative_to(work).as_posix(), "sha256": digest(data), "bytes": len(data)}
        if record["path"] in sources and sources[record["path"]] != record:
            raise ValueError("Evidence changed while being read")
        sources[record["path"]] = record
        return data
    state = json.loads(bound(folder / "job.json"))
    freeze = json.loads(bound(state["scope_freeze_path"], state["scope_freeze_sha256"]))
    if freeze.get("schema_version") != "stage1d-sql-freeze-v1" or freeze.get("authorization_id") != AUTH:
        raise ValueError("Only the current Stage1D SQL authority may reconcile export risk")
    batch = json.loads(bound("private/BATCH_QUERY_FREEZE.json"))
    ids = {q["query_id"] for q in batch.get("queries", [])}
    if batch.get("schema_version") != "stage1d-batch-query-freeze-v1" or len(ids) != 4:
        raise ValueError("The predeclared four-query batch must be bound")
    if not freeze.get("query_ids") or not set(freeze["query_ids"]) <= ids:
        raise ValueError("Export is outside the four-query authority")
    sql_sha = digest(bound(folder / "query.sql"))
    if sql_sha != state["sql_sha256"] or sql_sha != freeze["sql_sha256"]:
        raise ValueError("Immutable SQL identity mismatch")
    for dependency in freeze.get("dependencies", []):
        bound(dependency["path"], dependency["sha256"])
    if state.get("state") != "QUERY_STATE_COMPLETED" or state.get("request_set_closed") is not True:
        raise ValueError("Only a fully closed completed job may release an export upper bound")
    if state.get("export_status") != "COMPLETED_DECLARED_RESULT_ROWS" or not state.get("r4_verified_pages"):
        raise ValueError("A complete verified export is required; unrequested exports release nothing")
    status_receipt = state["status_receipt"]
    def verify_receipt(body, receipt, operation):
        if receipt.get("http_status") != 200 or receipt.get("error_class") or receipt.get("operation") != operation:
            raise ValueError("Verified successful original receipt is required")
        if receipt.get("execution_id") != state["execution_id"]:
            raise ValueError("Receipt execution identity mismatch")
        raw = bound(receipt["raw_path"], receipt["sha256"])
        if len(raw) != receipt.get("raw_bytes") or canonical(json.loads(raw, parse_float=Decimal)) != canonical(body):
            raise ValueError("Original raw response does not match the verified body/size")
        persisted = json.loads(bound("logs/" + receipt["request_id"] + ".json"))
        if canonical(persisted) != canonical(receipt):
            raise ValueError("Persisted original receipt differs")
        return len(raw)
    verify_receipt(state["status_response"], status_receipt, "status")
    if state["status_response"].get("execution_id") != state["execution_id"] or state["status_response"].get("state") != "QUERY_STATE_COMPLETED" or state["status_response"].get("error"):
        raise ValueError("Status response does not certify the same completed execution")
    rate = state.get("r4_export_envelope", {}).get("rate_evidence", {})
    if str(rate.get("export_credits_per_decimal_MB_for_budget")) != "20" or str(rate.get("datapoint_scheme_credits_per_1000")) != "1":
        raise ValueError("Existing conservative rates are not bound")
    if (folder / "stage1d_export_plan.json").exists():
        plan = verify_plan(work, folder)
        plan_raw = bound(folder / "stage1d_export_plan.json")
        if state["r4_export_envelope"].get("plan_sha256") != digest(plan_raw):
            raise ValueError("Original reserved unique-set plan identity changed")
        bound(plan["byte_bound_evidence"]["path"], plan["byte_bound_evidence"]["sha256"])
    ledger = work / "private/shared_budget_r4.sqlite"
    components = rows(ledger, "r2_components")
    component = next((r for r in components if r["job"] == state["logical_job_id"]), None)
    amounts = [r for r in rows(ledger, "amounts") if r["job"] == state["logical_job_id"] and r["unit"] == "dune_credits"]
    exports = [r for r in rows(ledger, "r4_dune_exports") if r["job"] == state["logical_job_id"]]
    baselines = [r for r in rows(ledger, "r4_dune_export_baselines") if r["job"] == state["logical_job_id"]]
    if not component or component["origin"] != "R2_NEW" or component["export_actual"] is not None or len(amounts) != 1 or amounts[0]["actual"] is not None:
        raise ValueError("Historical or actual-settled accounting cannot be changed")
    if component["status"] != "BOUNDED_ACCOUNTING_NOT_FINAL" or len(baselines) != 1 or Decimal(baselines[0]["inherited_export_risk"]) != 0:
        raise ValueError("Inherited, failed or unknown export risk must be preserved")
    if Decimal(amounts[0]["reserved"]) != Decimal(component["execution_risk"]) + Decimal(component["export_risk"]):
        raise ValueError("Current execution/export amount components disagree")
    requests = rows(work / "private/read_retry_r4.sqlite", "read_requests")
    attempts = rows(work / "private/read_retry_r4.sqlite", "read_attempts")
    requests = [r for r in requests if json.loads(r["identity_json"]).get("execution_id") == state["execution_id"] and json.loads(r["identity_json"]).get("method") == "GET_RESULTS"]
    requested_keys = {r["logical_key"] for r in requests}
    verified_keys = {r["logical_read_key"] for r in state["r4_verified_pages"].values()}
    if requested_keys != verified_keys or any(r["state"] != "SUCCESS" for r in requests):
        raise ValueError("An additional, failed or unknown result request must retain all risk")
    progress = None
    page_proofs = []
    success_ids = set()
    receipt_ids = set()
    abandoned = []
    bound_requests = []
    bound_attempts = []
    for offset_text, artifact in sorted(state["r4_verified_pages"].items(), key=lambda item: int(item[0])):
        page = json.loads(bound(artifact["page_path"], artifact["page_sha256"]))
        receipt = json.loads(bound(artifact["receipt_path"], artifact["receipt_sha256"]))
        raw_bytes = verify_receipt(page, receipt, "results")
        params = receipt["parameters"]
        if set(params) != {"limit", "offset"} or params["offset"] != int(offset_text):
            raise ValueError("Only unmodified full-column/unfiltered ordinary result pages qualify")
        progress = validate_page(page, execution_id=state["execution_id"], offset=int(offset_text), limit=params["limit"],
            progress=progress, status_metadata=state["status_response"]["result_metadata"], receipt=receipt, parameters=params)
        request = next(r for r in requests if r["logical_key"] == artifact["logical_read_key"])
        identity = json.loads(request["identity_json"])
        if digest(canonical(identity)) != request["logical_key"] or identity.get("provider") != "DUNE_EXISTING_ACCOUNT" or identity.get("account_context_ref") != state.get("account_context_ref"):
            raise ValueError("Logical read/account identity mismatch")
        if identity.get("parameters") != params or identity.get("columns") != page["result"]["metadata"]["column_names"]:
            raise ValueError("Stored request identity differs from full page selection")
        if canonical(json.loads(request["success_receipt"])) != canonical(receipt):
            raise ValueError("SQLite successful receipt differs")
        payload = json.loads(request["success_payload"])
        if canonical(payload) != canonical({"body": page, "receipt": receipt}):
            raise ValueError("SQLite successful response differs")
        related = [r for r in attempts if r["logical_key"] == request["logical_key"]]
        success = [r for r in related if r["outcome"] == "SUCCESS"]
        if len(success) != 1:
            raise ValueError("Exactly one successful dispatched attempt per page is required")
        one = success[0]
        if one["dispatched_at"] is None or one["finished_at"] is None or one["error_class"] or canonical(json.loads(one["receipt_json"])) != canonical(receipt):
            raise ValueError("Successful attempt transport facts differ")
        if one["payload_sha256"] != digest(request["success_payload"].encode()):
            raise ValueError("Successful attempt payload hash differs")
        for other in related:
            if other["attempt_id"] == one["attempt_id"]:
                continue
            if other["outcome"] != "ABANDONED_BEFORE_DISPATCH" or other["dispatched_at"] is not None or other["accounting_json"] is not None or other["payload_sha256"] is not None or other["receipt_json"] is not None:
                raise ValueError("Failed/unknown/additional dispatched attempts must retain all risk")
            abandoned.append(other)
        export = next((r for r in exports if r["attempt_id"] == one["attempt_id"]), None)
        if not export or export["dispatched"] != 1 or Decimal(export["increment"]) != 0:
            raise ValueError("Original export reservation does not identify the successful first dispatch")
        success_ids.add(one["attempt_id"])
        receipt_ids.add(receipt["request_id"])
        bound_requests.append({k: request[k] for k in ("logical_key", "identity_json", "state")})
        bound_attempts.extend(related)
        page_proofs.append({"offset": int(offset_text), "limit": params["limit"], "attempt_id": one["attempt_id"],
            "request_id": receipt["request_id"], "raw_sha256": receipt["sha256"], "logical_read_key": request["logical_key"],
            **page_upper(page["result"]["metadata"], len(page["result"]["rows"]), raw_bytes)})
    if not progress or not progress["complete"] or success_ids != {r["attempt_id"] for r in exports}:
        raise ValueError("Complete pages and dispatched export accounting do not close exactly")
    logged = set()
    for path in (work / "logs").glob("*.json"):
        value = read(path)
        if value.get("operation") == "results" and value.get("execution_id") == state["execution_id"]:
            logged.add(value["request_id"])
    if logged != receipt_ids:
        raise ValueError("An additional result HTTP receipt lies outside the verified page set")
    old = Decimal(component["export_risk"])
    new = sum((Decimal(p["upper_credits"]) for p in page_proofs), Decimal(0))
    for source in list(sources.values()):
        bound(source["path"], source["sha256"])
    return {"schema_version": "stage1d-completed-export-proof-v1", "policy": POLICY,
        "logical_job_id": state["logical_job_id"], "execution_id": state["execution_id"], "query_ids": freeze["query_ids"],
        "rate": RATE, "page_proofs": page_proofs, "complete_verified_progress": progress,
        "current_export_upper": str(old), "verified_response_export_upper": str(new), "candidate_release": str(old - new),
        "export_actual": None, "sources": sorted(sources.values(), key=lambda r: r["path"]),
        "component_before": component, "amount_before": amounts[0], "r4_export_rows_preserved": exports,
        "r4_baselines_preserved": baselines, "read_request_identities": bound_requests,
        "all_read_attempts_preserved": bound_attempts, "abandoned_before_dispatch_preserved": abandoned}


def response_evidence_id(proof):
    """Exclude mutable accounting amounts, keeping every response/attempt fact."""
    value = {k: v for k, v in proof.items() if k not in {
        "current_export_upper", "candidate_release", "component_before", "amount_before"}}
    return digest(canonical(value))


def _stored(db, job):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='stage1d_export_reconciliations'").fetchone():
        return None
    row = db.execute("SELECT record_json FROM stage1d_export_reconciliations WHERE job=?", (job,)).fetchone()
    return json.loads(row[0]) if row else None


def unique_set_upper(metadata, limit, full_response_byte_upper, *, successful_pages=0, observed_rows=0):
    """One complete set, with conservative cross-page byte/point heterogeneity.

    Sum(max(byte_cost_i, point_cost_i)) is bounded by the sum of both
    aggregate costs. A per-page rounding margin is then added. This avoids
    pretending a whole-result maximum is each page's actual response size.
    """
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("Stage1D keeps the existing validated page limit1..1000")
    names = metadata.get("column_names")
    if not isinstance(names, list) or not names or len(names) != len(set(names)):
        raise ValueError("Complete unique column metadata required")
    total = exact_count(metadata.get("total_row_count"), "total_row_count")
    size = exact_count(full_response_byte_upper, "full_response_byte_upper")
    if size < exact_count(metadata.get("total_result_set_bytes"), "total_result_set_bytes"):
        raise ValueError("The full response byte envelope cannot undercut server metadata")
    if not 0 <= observed_rows <= total or successful_pages < 0:
        raise ValueError("Invalid observed request-set progress")
    pages = max(1, successful_pages + (total - observed_rows + limit - 1) // limit)
    byte_cost = Decimal(size) * 20 / 1000000
    point_cost = Decimal(total * len(names)) / 1000
    upper = max(Decimal(1), (byte_cost + point_cost).to_integral_value(rounding=ROUND_CEILING) + pages - 1)
    # In the absence of a per-row width proof, all bounded bytes may occur in
    # one request. Only a real additional dispatch reserves this retry bound.
    per = max(Decimal(1), max(byte_cost, Decimal(min(limit, total) * len(names)) / 1000).to_integral_value(rounding=ROUND_CEILING))
    return upper, per, pages


def _plan_identity(work, folder):
    state = read(folder / "job.json")
    frozen_path = inside(work, state["scope_freeze_path"])
    if digest(frozen_path.read_bytes()) != state["scope_freeze_sha256"]:
        raise ValueError("SQL authority freeze hash mismatch")
    frozen = read(frozen_path)
    batch = read(work / "private/BATCH_QUERY_FREEZE.json")
    ids = {q["query_id"] for q in batch.get("queries", [])}
    if (frozen.get("schema_version") != "stage1d-sql-freeze-v1" or frozen.get("authorization_id") != AUTH
            or batch.get("schema_version") != "stage1d-batch-query-freeze-v1" or len(ids) != 4
            or not frozen.get("query_ids") or not set(frozen["query_ids"]) <= ids):
        raise ValueError("Only the frozen current Stage1D four-query domain can plan exports")
    if digest((folder / "query.sql").read_bytes()) != state["sql_sha256"] or frozen["sql_sha256"] != state["sql_sha256"]:
        raise ValueError("Immutable SQL identity differs")
    for dependency in frozen.get("dependencies", []):
        if digest(inside(work, dependency["path"]).read_bytes()) != dependency["sha256"]:
            raise ValueError("Frozen SQL dependency changed")
    receipt, body = state["status_receipt"], state["status_response"]
    raw = inside(work, receipt["raw_path"]).read_bytes()
    if (state.get("state") != "QUERY_STATE_COMPLETED" or body.get("state") != "QUERY_STATE_COMPLETED"
            or body.get("execution_id") != state["execution_id"] or body.get("error")
            or receipt.get("operation") != "status" or receipt.get("http_status") != 200
            or receipt.get("execution_id") != state["execution_id"] or receipt.get("error_class")
            or digest(raw) != receipt["sha256"] or len(raw) != receipt["raw_bytes"]
            or canonical(json.loads(raw, parse_float=Decimal)) != canonical(body)
            or canonical(read(work / "logs" / (receipt["request_id"] + ".json"))) != canonical(receipt)):
        raise ValueError("Reliable completed same-execution metadata/raw receipt required")
    return state, {"logical_job_id": state["logical_job_id"], "execution_id": state["execution_id"],
        "sql_sha256": state["sql_sha256"], "scope_freeze_sha256": state["scope_freeze_sha256"],
        "batch_freeze_sha256": digest((work / "private/BATCH_QUERY_FREEZE.json").read_bytes()),
        "status_raw_sha256": receipt["sha256"], "query_ids": frozen["query_ids"],
        "result_metadata": body["result_metadata"]}


def _recovery_caps(work, plan=None):
    from stage1d_recovery_policy import effective, AUTH as RECOVERY_AUTH, POLICY_SHA
    recovery=effective(work)
    binding=plan.get('recovery_resource_authorization') if plan is not None else None
    if plan is not None and binding is None:return RAW_CAP,50000,None
    if plan is not None and (not recovery or binding!={'authorization_id':RECOVERY_AUTH,'policy_sha256':POLICY_SHA}):
        raise ValueError('Frozen recovery export resource binding changed')
    if recovery:return recovery['resources']['new_raw_logical_unique_bytes_hard'],None,{'authorization_id':RECOVERY_AUTH,'policy_sha256':POLICY_SHA}
    return RAW_CAP,50000,None


def prepare_plan(work, folder, *, full_response_byte_upper, evidence, maximum_total_rows, limit=1000):
    """Freeze an explicit shape/byte margin before any result dispatch.

    `evidence` is a SHA-bound local document declaring the same SQL and the
    complete-request-set byte upper, with a concrete `basis`. No network or
    budget write occurs here. The caller must substantiate that byte bound;
    status total_result_set_bytes alone is explicitly insufficient.
    """
    work = Path(work).resolve(); folder = inside(work, folder)
    state, identity = _plan_identity(work, folder)
    metadata = identity["result_metadata"]
    total = exact_count(metadata.get("total_row_count"), "total_row_count")
    maximum_total_rows = exact_count(maximum_total_rows, "maximum_total_rows")
    size = exact_count(full_response_byte_upper, "full_response_byte_upper")
    raw_cap,row_cap,recovery_binding=_recovery_caps(work)
    if maximum_total_rows<=0 or row_cap is not None and maximum_total_rows>row_cap or total>maximum_total_rows:
        raise ValueError("Declared query/context row resource cap blocks this result")
    if size > raw_cap:
        raise ValueError("Unchanged cumulative raw cap blocks this result")
    path = inside(work, evidence["path"]); raw = path.read_bytes()
    if digest(raw) != evidence["sha256"]:
        raise ValueError("Declared byte-bound evidence hash mismatch")
    proof = json.loads(raw)
    if (proof.get("sql_sha256") != state["sql_sha256"] or proof.get("full_response_byte_upper") != size
            or not isinstance(proof.get("basis"), str) or not proof["basis"].strip()):
        raise ValueError("Same-SQL full response byte envelope and concrete basis required")
    upper, per, pages = unique_set_upper(metadata, limit, size)
    plan = {"schema_version": "stage1d-unique-export-plan-v1", "policy": PLAN_POLICY, **identity,
        "rate": RATE, "page_limit": limit, "maximum_total_rows": maximum_total_rows,
        "full_response_byte_upper": size, "full_export_upper_credits": str(upper),
        "actual_retry_request_upper_credits": str(per), "initial_maximum_pages": pages,
        "byte_bound_evidence": {"path": path.relative_to(work).as_posix(), "sha256": digest(raw), "bytes": len(raw)},
        "hypothetical_retries_reserved": 0, "filters_or_column_changes": False, "export_actual": None}
    if recovery_binding:plan["recovery_resource_authorization"]=recovery_binding
    target = folder / "stage1d_export_plan.json"
    data = canonical(plan)
    if target.exists():
        if target.read_bytes() != data: raise ValueError("Immutable export plan conflict")
        return plan
    component = next(r for r in rows(work / "private/shared_budget_r4.sqlite", "r2_components") if r["job"] == state["logical_job_id"])
    attempts = rows(work / "private/read_retry_r4.sqlite", "read_attempts")
    requests = rows(work / "private/read_retry_r4.sqlite", "read_requests")
    keys = {r["logical_key"] for r in requests if json.loads(r["identity_json"]).get("execution_id") == state["execution_id"]
            and json.loads(r["identity_json"]).get("method") == "GET_RESULTS"}
    exports = [r for r in rows(work / "private/shared_budget_r4.sqlite", "r4_dune_exports") if r["job"] == state["logical_job_id"]]
    if (component["origin"] != "R2_NEW" or Decimal(component["export_risk"]) != 0 or component["export_actual"] is not None
            or exports or state.get("r4_verified_pages") or state.get("request_set_closed")
            or any(a["logical_key"] in keys and a["dispatched_at"] is not None for a in attempts)):
        raise ValueError("Do not replace any inherited, dispatched, unknown or reserved export risk")
    with target.open("xb") as output: output.write(data)
    return plan


def verify_plan(work, folder):
    """Read-only immutable plan validation; does not reserve or dispatch."""
    work = Path(work).resolve(); folder = inside(work, folder)
    path = folder / "stage1d_export_plan.json"
    if not path.exists(): return None
    plan = read(path); _, identity = _plan_identity(work, folder)
    if plan.get("schema_version") != "stage1d-unique-export-plan-v1" or plan.get("policy") != PLAN_POLICY or plan.get("rate") != RATE:
        raise ValueError("Unknown or changed unique request-set plan")
    if any(plan.get(key) != value for key, value in identity.items()):
        raise ValueError("Frozen export identity/metadata changed")
    evidence = plan["byte_bound_evidence"]; raw = inside(work, evidence["path"]).read_bytes()
    if digest(raw) != evidence["sha256"] or len(raw) != evidence["bytes"]:
        raise ValueError("Byte-bound source changed")
    proof = json.loads(raw)
    if proof.get("sql_sha256") != identity["sql_sha256"] or proof.get("full_response_byte_upper") != plan["full_response_byte_upper"] or not proof.get("basis"):
        raise ValueError("Byte-bound identity changed")
    raw_cap,row_cap,_=_recovery_caps(work,plan)
    upper, per, pages = unique_set_upper(identity["result_metadata"], plan["page_limit"], plan["full_response_byte_upper"])
    if (str(upper) != plan["full_export_upper_credits"] or str(per) != plan["actual_retry_request_upper_credits"]
            or pages != plan["initial_maximum_pages"] or plan.get("hypothetical_retries_reserved") != 0
            or plan.get("filters_or_column_changes") is not False or plan.get("export_actual") is not None
            or plan["maximum_total_rows"]<=0 or row_cap is not None and plan["maximum_total_rows"]>row_cap
            or identity["result_metadata"]["total_row_count"] > plan["maximum_total_rows"] or plan["full_response_byte_upper"] > raw_cap):
        raise ValueError("Frozen request-set/resource envelope was changed")
    return plan


def prepare_known_plan(work, folder, limit=1000):
    """Automatic Stage1D opt-in for ordinary frozen schemas, before first GET.

    The declared byte envelope is a conservative planning margin, not a claim
    of actual export size. The provider's metadata alone is not an HTTP JSON
    bound. Every response is retained and checked against the declared margin;
    any observed excess raises the risk and prevents another dispatch.
    """
    work = Path(work).resolve(); folder = inside(work, folder)
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("Stage1D keeps the existing validated page limit1..1000")
    if (folder / "stage1d_export_plan.json").exists(): return verify_plan(work, folder)
    state, identity = _plan_identity(work, folder)
    component = next(r for r in rows(work / "private/shared_budget_r4.sqlite", "r2_components") if r["job"] == state["logical_job_id"])
    # Existing/unknown risk is never replaced by a smaller new planning mode.
    if Decimal(component["export_risk"]) != 0 or state.get("r4_verified_pages") or state.get("request_set_closed"):
        return None
    metadata = identity["result_metadata"]; kind = state.get("kind")
    if metadata.get("column_names") != KNOWN_COLUMNS.get(kind) or kind not in KNOWN_COLUMNS:
        raise ValueError("A new schema requires an explicit SHA-bound byte-envelope plan")
    total = exact_count(metadata.get("total_row_count"), "total_row_count")
    metadata_bytes = exact_count(metadata.get("total_result_set_bytes"), "total_result_set_bytes")
    pages = max(1, (total + limit - 1) // limit)
    maximum = {"candidate": min(50000, 25000 * len(identity["query_ids"])), "context": 50000, "frontier_labels": 100}[kind]
    raw_cap,row_cap,recovery_binding=_recovery_caps(work)
    if recovery_binding:maximum=max(1,total)  # SQL rows are not physical candidate/model events.
    factor = 6 if kind == "frontier_labels" else 4
    byte_upper = metadata_bytes * factor + total * 2048 + pages * 65536
    # Size alone is sufficient to reject giant results; no inference that an
    # oversized SQL row count equals the distinct modeled money-event count.
    if metadata_bytes > raw_cap or byte_upper > raw_cap:
        raise ValueError("Declared result byte envelope exceeds the unchanged raw cap; no GET")
    proof = {"schema_version": "stage1d-known-schema-byte-envelope-v1", "sql_sha256": identity["sql_sha256"],
        "full_response_byte_upper": byte_upper, "kind": kind, "column_names": metadata["column_names"],
        "status_raw_sha256": identity["status_raw_sha256"], "metadata_total_result_set_bytes": metadata_bytes,
        "metadata_total_row_count": total, "planned_pages": pages, "metadata_byte_multiplier": factor,
        "row_syntax_and_keys_margin_bytes": 2048, "per_response_metadata_margin_bytes": 65536,
        "basis": "Declared ordinary frozen schema: metadata bytes times escaping/serialization margin, plus repeated row keys/syntax and response metadata. Raw page evidence revises this estimate; it is not actual billing or proof of unrelatedness.",
        "is_actual": False, "remaining_resource_checks_required_before_dispatch": True}
    path = folder / "stage1d_export_byte_evidence.json"; data = canonical(proof)
    if path.exists():
        if path.read_bytes() != data: raise ValueError("Immutable byte planning evidence conflict")
    else:
        with path.open("xb") as output: output.write(data)
    return prepare_plan(work, folder, full_response_byte_upper=byte_upper,
        evidence={"path": path.relative_to(work).as_posix(), "sha256": digest(data)}, maximum_total_rows=maximum, limit=limit)


def reconcile(work, folder, ledger):
    """Explicit single-writer action; preserve history and change only open upper.

    The caller supplies the current Runtime-created ledger, so account authority
    and cumulative resource limits are neither duplicated nor hard-coded here.
    """
    work = Path(work).resolve()
    if Path(ledger.path).resolve() != work / "private/shared_budget_r4.sqlite":
        raise ValueError("Use the same active shared ledger")
    proof = analyze(work, folder)
    evidence_id = response_evidence_id(proof)
    job = proof["logical_job_id"]
    with ledger.connection() as db:
        db.execute("BEGIN IMMEDIATE")
        ledger._authorized(db)
        component_tuple = db.execute("SELECT * FROM r2_components WHERE job=?", (job,)).fetchone()
        component = dict(zip(("job", "origin", "execution_risk", "execution_known", "export_risk", "export_actual", "status", "evidence"), component_tuple))
        amount_tuple = db.execute("SELECT job,unit,reserved,actual FROM amounts WHERE job=? AND unit='dune_credits'", (job,)).fetchone()
        amount = dict(zip(("job", "unit", "reserved", "actual"), amount_tuple))
        if component != proof["component_before"] or amount != proof["amount_before"]:
            raise ValueError("Accounting changed after read-only proof; reanalyze")
        old_record = _stored(db, job)
        if old_record is not None:
            if old_record["response_evidence_id"] != evidence_id or Decimal(component["export_risk"]) != Decimal(old_record["after_export_upper"]):
                raise ValueError("Existing reconciliation or current upper conflicts with response proof")
            return {"status": "ALREADY_RECONCILED", "logical_job_id": job, "released_this_call": "0", "record": old_record}
        old, new = Decimal(component["export_risk"]), Decimal(proof["verified_response_export_upper"])
        if new > old:
            raise ValueError("Observed response upper exceeds current reservation; no speculative release")
        if old == new:
            return {"status": "NO_CHANGE", "logical_job_id": job, "released_this_call": "0"}
        for source in proof["sources"]:
            if digest(inside(work, source["path"]).read_bytes()) != source["sha256"]:
                raise ValueError("Response proof file changed before reconciliation")
        after_component = dict(component, export_risk=str(new))
        after_amount = dict(amount, reserved=str(Decimal(component["execution_risk"]) + new))
        record = {"schema_version": "stage1d-export-upper-reconciliation-v1", "policy": POLICY,
            "logical_job_id": job, "response_evidence_id": evidence_id,
            "before_export_upper": str(old), "after_export_upper": str(new), "released_upper": str(old - new),
            "before_component": component, "after_component": after_component,
            "before_amount": amount, "after_amount": after_amount, "proof": proof,
            "export_actual": None, "historical_or_failed_risk_released": False,
            "original_attempts_and_observations_unchanged": True}
        data = canonical(record)
        path = inside(work, "private/stage1d_export_reconciliations/" + evidence_id + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != data:
                raise ValueError("Append-only reconciliation artifact conflict")
        else:
            with path.open("xb") as output:
                output.write(data)
                output.flush()
        db.execute("CREATE TABLE IF NOT EXISTS stage1d_export_reconciliations(job TEXT PRIMARY KEY,evidence_id TEXT UNIQUE NOT NULL,record_json TEXT NOT NULL,utc TEXT NOT NULL)")
        db.execute("UPDATE r2_components SET export_risk=? WHERE job=?", (str(new), job))
        db.execute("UPDATE amounts SET reserved=? WHERE job=? AND unit='dune_credits'", (after_amount["reserved"], job))
        db.execute("INSERT INTO stage1d_export_reconciliations VALUES(?,?,?,?)", (job, evidence_id, data.decode(), datetime.now(timezone.utc).isoformat()))
        ledger._record(db, job, "STAGE1D_EXPORT_UPPER_RECONCILED", {"record_path": path.relative_to(work).as_posix(),
            "record_sha256": digest(data), "response_evidence_id": evidence_id, "before_export_upper": str(old),
            "after_export_upper": str(new), "released_upper": str(old - new), "export_actual": None})
    return {"status": "RECONCILED_UPPER_NOT_ACTUAL", "logical_job_id": job,
            "released_this_call": str(old - new), "record": record}


class Stage1DPageDune(Stage1DDune):
    """One planned complete set; actual retries are reserved before dispatch."""
    def _planned_envelope(self, state, limit, folder, plan):
        if limit != plan["page_limit"]:
            raise ValueError("Unique request-set page parameters remain frozen")
        progress, pages = self._verified_pages(folder, state)
        page_proofs = []
        for page, receipt, _ in pages:
            raw = inside(self.w, receipt["raw_path"]).read_bytes()
            if (digest(raw) != receipt["sha256"] or len(raw) != receipt["raw_bytes"]
                    or canonical(json.loads(raw, parse_float=Decimal)) != canonical(page)):
                raise ValueError("Verified page raw/SHA differs from unique-set evidence")
            page_proofs.append(page_upper(page["result"]["metadata"], len(page["result"]["rows"]), len(raw)))
        observed_bytes = sum(p["conservative_billable_bytes"] for p in page_proofs)
        size = max(plan["full_response_byte_upper"], observed_bytes)
        upper, per, maximum_pages = unique_set_upper(plan["result_metadata"], limit, size,
            successful_pages=len(page_proofs), observed_rows=progress["observed_rows"])
        # A physical page larger than expected never silently releases risk.
        upper = max(upper, sum((Decimal(p["upper_credits"]) for p in page_proofs), Decimal(0)))
        rate = self.rate_evidence()
        if str(rate.get("datapoint_scheme_credits_per_1000")) != "1":
            raise ValueError("Existing conservative datapoint rate evidence is required")
        return upper, {"rate_evidence": rate, "result_metadata": plan["result_metadata"],
            "is_actual": False, "basis": PLAN_POLICY, "page_limit": limit, "maximum_pages": maximum_pages,
            "per_request_upper_credits": str(per), "full_export_upper_credits": str(upper),
            "plan_sha256": digest((folder / "stage1d_export_plan.json").read_bytes()),
            "full_response_byte_upper": plan["full_response_byte_upper"], "observed_page_bytes": observed_bytes,
            "observed_page_upper_credits": [p["upper_credits"] for p in page_proofs],
            "byte_margin_exceeded": observed_bytes > plan["full_response_byte_upper"],
            "whole_result_per_page_repeated": False, "hypothetical_retries_reserved": 0}

    def export_envelope(self, state, limit, folder=None):
        with self.db.connection() as db:
            record = _stored(db, state["logical_job_id"])
        if record is None:
            plan = verify_plan(self.w, folder) if folder is not None else None
            if plan is None: return super().export_envelope(state, limit, folder)
            return self._planned_envelope(state, limit, Path(folder), plan)
        if folder is None:
            raise ValueError("Reconciled export envelope requires its verified folder")
        proof = analyze(self.w, folder)
        if response_evidence_id(proof) != record["response_evidence_id"] or Decimal(proof["current_export_upper"]) != Decimal(record["after_export_upper"]):
            raise ValueError("Reconciled request set changed; retain evidence and stop")
        return Decimal(record["after_export_upper"]), {
            "rate_evidence": state["r4_export_envelope"]["rate_evidence"], "is_actual": False,
            "basis": POLICY, "response_evidence_id": record["response_evidence_id"],
            "per_request_upper_credits": str(max(Decimal(p["upper_credits"]) for p in proof["page_proofs"])),
            "full_export_upper_credits": record["after_export_upper"],
            "maximum_pages": len(proof["page_proofs"]), "page_limit": limit,
            "complete_verified_page_upper_credits": [p["upper_credits"] for p in proof["page_proofs"]],
            "old_whole_result_per_page_envelope_reused": False}

    def _reserve_page(self, state, claim, base, basis):
        if basis.get("basis") != PLAN_POLICY:
            return super()._reserve_page(state, claim, base, basis)
        if basis.get("byte_margin_exceeded"):
            raise RuntimeError("Observed response exceeds declared byte margin; retain risk and re-evaluate before any next GET")
        # A claim number may include abandoned-before-dispatch tombstones.
        # Only a prior actual/unknown dispatched request incurs retry risk.
        prior = [a for a in rows(Path(self.w) / "private/read_retry_r4.sqlite", "read_attempts")
                 if a["logical_key"] == claim["logical_key"] and a["attempt_id"] != claim["attempt_id"] and a["dispatched_at"] is not None]
        adjusted = dict(claim, attempt_no=2 if prior else 1)
        return super()._reserve_page(state, adjusted, base, basis)

    def export(self, folder, limit=1000, offset=0):
        self.require_gate(); self.ensure_not_halted()
        folder = self._inside(folder); state = read(folder / "job.json")
        plan = verify_plan(self.w, folder)
        if plan is None and state.get("state") == "QUERY_STATE_COMPLETED" and not state.get("request_set_closed"):
            # The SQL freeze is checked by prepare_known_plan. Legacy schemas
            # without Stage1D authority retain their original R4 behavior.
            frozen = read(inside(self.w, state["scope_freeze_path"]))
            if frozen.get("schema_version") == "stage1d-sql-freeze-v1" and frozen.get("authorization_id") == AUTH:
                plan = prepare_known_plan(self.w, folder, limit)
        if plan is not None:
            _, basis = self._planned_envelope(state, limit, folder, plan)
            if basis["byte_margin_exceeded"]:
                raise RuntimeError("Previous response exceeded declared byte margin; no next GET")
            remaining = max(0, plan["full_response_byte_upper"] - basis["observed_page_bytes"])
            raw_cap,_,_=_recovery_caps(self.w,plan)
            if self.active_raw_risk() + remaining > raw_cap:
                raise RuntimeError("Remaining complete request set does not fit unchanged cumulative raw cap")
        result = super().export(folder, limit, offset)
        if plan is not None:
            current = read(folder / "job.json")
            _, after = self._planned_envelope(current, limit, folder, plan)
            if after["byte_margin_exceeded"]:
                # Parent has already observed/reserved the larger page risk.
                raise RuntimeError("Observed response exceeded declared byte margin; evidence/risk retained and further GET blocked")
        return result

    def settle(self, folder):
        result = super().settle(folder)
        folder = self._inside(folder); state = read(folder / "job.json")
        if verify_plan(self.w, folder) is not None and state.get("request_set_closed"):
            try:
                adjustment = reconcile(self.w, folder, self.db)
            except ValueError as exc:
                # Failed/unknown/retried requests retain all original risk.
                # This is a reported reconciliation refusal, not a success or
                # an export failure being reclassified as an empty result.
                result["export_upper_reconciliation"] = {"status": "RISK_RETAINED", "reason": str(exc)}
            else:
                result["export_upper_reconciliation"] = adjustment
                result["cumulative"] = self._budget_snapshot()["dune_credits"]
        return result
