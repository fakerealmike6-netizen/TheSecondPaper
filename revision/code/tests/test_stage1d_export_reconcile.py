"""Synthetic complete HTTP/page/accounting proofs; never uses a real ledger."""
from contextlib import contextmanager, closing
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / "src"))
from stage1d_export_reconcile import AUTH, Stage1DPageDune, analyze, canonical, digest, page_upper, reconcile, rows


class TestLedger:
    def __init__(self, path): self.path = str(path)
    @contextmanager
    def connection(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            yield db
    def _authorized(self, db):
        assert db.execute("SELECT cap FROM limits WHERE unit='dune_credits'").fetchone()[0] == "2000"
    def _record(self, db, job, kind, payload):
        db.execute("INSERT INTO r2_observations(job,kind,payload,utc) VALUES(?,?,?,?)", (job, kind, json.dumps(payload), "synthetic"))
    def final_execution_cost(self, job): return Decimal(1)


def write(work, relative, value):
    path = work / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical(value))
    return digest(path.read_bytes())


def fixture(work):
    work.mkdir()
    sql = b"SELECT 'synthetic complete export'"
    sql_sha = digest(sql)
    folder = work / "private/dune_r2_jobs" / sql_sha
    folder.mkdir(parents=True)
    (folder / "query.sql").write_bytes(sql)
    write(work, "private/BATCH_QUERY_FREEZE.json", {"schema_version": "stage1d-batch-query-freeze-v1", "queries": [{"query_id": "synthetic:q" + str(i)} for i in range(4)]})
    freeze_path = "private/stage1d_sql/" + sql_sha + "/freeze_manifest.json"
    freeze_sha = write(work, freeze_path, {"schema_version": "stage1d-sql-freeze-v1", "authorization_id": AUTH,
        "query_ids": ["synthetic:q0"], "sql_sha256": sql_sha, "dependencies": []})
    job = "dune_r4:" + sql_sha
    execution = "SYNTHETIC_EXECUTION"
    names = ["column" + str(i) for i in range(24)]
    metadata = {"column_names": names, "column_types": ["varchar"] * 24, "total_row_count": 2584,
        "row_count": 2584, "total_result_set_bytes": 800000, "result_set_bytes": 800000, "datapoint_count": 62016}
    ledger = TestLedger(work / "private/shared_budget_r4.sqlite")
    with ledger.connection() as db:
        db.executescript("""CREATE TABLE limits(unit TEXT PRIMARY KEY,cap TEXT);
        CREATE TABLE r2_components(job TEXT PRIMARY KEY,origin TEXT,execution_risk TEXT,execution_known TEXT,export_risk TEXT,export_actual TEXT,status TEXT,evidence TEXT);
        CREATE TABLE amounts(job TEXT,unit TEXT,reserved TEXT,actual TEXT,PRIMARY KEY(job,unit));
        CREATE TABLE r4_dune_exports(attempt_id TEXT PRIMARY KEY,job TEXT,increment TEXT,upper_after TEXT,dispatched INTEGER);
        CREATE TABLE r4_dune_export_baselines(job TEXT PRIMARY KEY,inherited_export_risk TEXT);
        CREATE TABLE r2_observations(n INTEGER PRIMARY KEY,job TEXT,kind TEXT,payload TEXT,utc TEXT);""")
        db.execute("INSERT INTO limits VALUES('dune_credits','2000')")
        db.execute("INSERT INTO r2_components VALUES(?,?,?,?,?,?,?,?)", (job, "R2_NEW", "1", "1", "189", None, "BOUNDED_ACCOUNTING_NOT_FINAL", '{"original":"unchanged"}'))
        db.execute("INSERT INTO amounts VALUES(?,?,?,NULL)", (job, "dune_credits", "190"))
        db.execute("INSERT INTO r4_dune_export_baselines VALUES(?,?)", (job, "0"))
        db.execute("INSERT INTO r2_components VALUES('synthetic:legacy','INHERITED','7.5',NULL,'0',NULL,'INHERITED_UNCHANGED','{}')")
        db.execute("INSERT INTO amounts VALUES('synthetic:legacy','dune_credits','7.5',NULL)")
        db.execute("INSERT INTO amounts VALUES('synthetic:rpc','rpc_requests','2',NULL)")
    retry = work / "private/read_retry_r4.sqlite"
    with closing(sqlite3.connect(retry)) as db, db:
        db.executescript("""CREATE TABLE read_requests(logical_key TEXT PRIMARY KEY,identity_json TEXT,state TEXT,next_eligible_at REAL,success_payload TEXT,success_receipt TEXT);
        CREATE TABLE read_attempts(attempt_id TEXT PRIMARY KEY,logical_key TEXT,attempt_no INTEGER,retry_of TEXT,owner TEXT,owner_pid INTEGER,created_at REAL,dispatched_at REAL,finished_at REAL,outcome TEXT,error_class TEXT,receipt_json TEXT,accounting_json TEXT,payload_sha256 TEXT);""")
    def receipt(body, request_id, operation, parameters):
        raw_path = "raw/dune/" + request_id + ".json"
        raw_sha = write(work, raw_path, body)
        value = {"request_id": request_id, "execution_id": execution, "http_status": 200, "error_class": None,
            "operation": operation, "parameters": parameters, "raw_path": raw_path, "raw_bytes": (work / raw_path).stat().st_size,
            "sha256": raw_sha, "evidence_kind": "SYNTHETIC_TEST_ONLY"}
        write(work, "logs/" + request_id + ".json", value)
        return value
    state = {"logical_job_id": job, "execution_id": execution, "account_context_ref": "synthetic:account",
        "state": "QUERY_STATE_COMPLETED", "sql_sha256": sql_sha, "scope_freeze_path": freeze_path,
        "scope_freeze_sha256": freeze_sha, "request_set_closed": True, "settlement_status": "BOUNDED_ACCOUNTING_NOT_FINAL",
        "export_status": "COMPLETED_DECLARED_RESULT_ROWS", "r4_verified_pages": {},
        "r4_export_envelope": {"rate_evidence": {"export_credits_per_decimal_MB_for_budget": "20", "datapoint_scheme_credits_per_1000": "1"}}}
    state["status_response"] = {"execution_id": execution, "state": "QUERY_STATE_COMPLETED", "result_metadata": metadata}
    state["status_receipt"] = receipt(state["status_response"], "synthetic_status", "status", None)
    for number, offset in enumerate((0, 1000, 2000)):
        count = min(1000, 2584 - offset)
        page = {"execution_id": execution, "state": "QUERY_STATE_COMPLETED", "result": {
            "metadata": dict(metadata, row_count=count, datapoint_count=count * 24, result_set_bytes=count * 20),
            "rows": [dict.fromkeys(names, "x") for _ in range(count)]}}
        if offset + count < 2584:
            page["next_offset"] = offset + count
        params = {"limit": 1000, "offset": offset}
        r = receipt(page, "synthetic_results_" + str(number), "results", params)
        identity = {"execution_id": execution, "provider": "DUNE_EXISTING_ACCOUNT", "account_context_ref": "synthetic:account",
                    "method": "GET_RESULTS", "columns": names, "parameters": params}
        logical_key = digest(canonical(identity))
        pp = (folder / "r4_verified" / logical_key / "page.json").relative_to(work).as_posix()
        rp = (folder / "r4_verified" / logical_key / "receipt.json").relative_to(work).as_posix()
        state["r4_verified_pages"][str(offset)] = {"page_path": pp, "page_sha256": write(work, pp, page),
            "receipt_path": rp, "receipt_sha256": write(work, rp, r), "logical_read_key": logical_key}
        payload = canonical({"body": page, "receipt": r}).decode()
        attempt_id = "synthetic-attempt-" + str(number)
        with closing(sqlite3.connect(retry)) as db, db:
            db.execute("INSERT INTO read_requests VALUES(?,?,?,?,?,?)", (logical_key, canonical(identity).decode(), "SUCCESS", 0, payload, canonical(r).decode()))
            db.execute("INSERT INTO read_attempts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (attempt_id, logical_key, 1, None, "synthetic", 0, 0, 1, 2,
                "SUCCESS", None, canonical(r).decode(), canonical({"logical_job_id": job, "export_upper_after": "189"}).decode(), digest(payload.encode())))
        with ledger.connection() as db:
            db.execute("INSERT INTO r4_dune_exports VALUES(?,?,?,?,?)", (attempt_id, job, "0", "189", 1))
    write(work, (folder / "job.json").relative_to(work), state)
    return folder, ledger, state


class ExportReconciliationTests(unittest.TestCase):
    def setUp(self):
        temporary_root = BASE / ".test_tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=temporary_root)
        self.work = Path(self.temp.name) / "work"
        self.folder, self.ledger, self.state = fixture(self.work)
    def tearDown(self): self.temp.cleanup()
    def analyze(self): return analyze(self.work, self.folder)
    def reconcile(self): return reconcile(self.work, self.folder, self.ledger)
    def save_state(self): write(self.work, (self.folder / "job.json").relative_to(self.work), self.state)
    def test_actual_response_sizes_bound_each_page_once(self):
        proof = self.analyze()
        self.assertEqual([p["upper_credits"] for p in proof["page_proofs"]], ["24", "24", "15"])
        self.assertEqual(proof["candidate_release"], "126")
    def test_raw_byte_scheme_remains_conservative(self):
        value = page_upper({"column_names": ["x"], "row_count": 1, "result_set_bytes": 5, "datapoint_count": 1}, 1, 200001)
        self.assertEqual(value["upper_credits"], "5")
    def test_reconciliation_only_changes_open_upper_and_preserves_history(self):
        before = {table: rows(self.ledger.path, table) for table in ("r4_dune_exports", "r4_dune_export_baselines", "limits")}
        legacy = next(r for r in rows(self.ledger.path, "r2_components") if r["origin"] == "INHERITED")
        result = self.reconcile()
        self.assertEqual(result["released_this_call"], "126")
        after = {table: rows(self.ledger.path, table) for table in before}
        self.assertEqual(before, after)
        self.assertEqual(legacy, next(r for r in rows(self.ledger.path, "r2_components") if r["origin"] == "INHERITED"))
        component = next(r for r in rows(self.ledger.path, "r2_components") if r["job"] == self.state["logical_job_id"])
        self.assertEqual(component["export_risk"], "63")
        self.assertEqual(component["execution_risk"], "1")
        self.assertIsNone(component["export_actual"])
        self.assertEqual(len(rows(self.ledger.path, "r2_observations")), 1)
    def test_same_evidence_is_idempotent(self):
        self.reconcile()
        self.assertEqual(self.reconcile()["released_this_call"], "0")
        self.assertEqual(len(rows(self.ledger.path, "stage1d_export_reconciliations")), 1)
        self.assertEqual(len(rows(self.ledger.path, "r2_observations")), 1)
    def test_raw_tamper_rejected(self):
        path = self.work / "raw/dune/synthetic_results_0.json"
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "SHA256"):
            self.reconcile()
    def test_additional_unknown_dispatched_attempt_rejected(self):
        with closing(sqlite3.connect(self.work / "private/read_retry_r4.sqlite")) as db, db:
            key = next(iter(self.state["r4_verified_pages"].values()))["logical_read_key"]
            db.execute("INSERT INTO read_attempts(attempt_id,logical_key,attempt_no,dispatched_at,outcome) VALUES('unknown',?,2,1,'UNKNOWN_AFTER_DISPATCH')", (key,))
        with self.assertRaisesRegex(ValueError, "unknown"):
            self.reconcile()
    def test_abandoned_before_dispatch_tombstone_is_retained(self):
        with closing(sqlite3.connect(self.work / "private/read_retry_r4.sqlite")) as db, db:
            key = next(iter(self.state["r4_verified_pages"].values()))["logical_read_key"]
            db.execute("INSERT INTO read_attempts(attempt_id,logical_key,attempt_no,outcome) VALUES('abandoned',?,1,'ABANDONED_BEFORE_DISPATCH')", (key,))
        proof = self.reconcile()["record"]["proof"]
        self.assertEqual(len(proof["abandoned_before_dispatch_preserved"]), 1)
    def test_unrequested_or_unclosed_result_releases_nothing(self):
        self.state["request_set_closed"] = False
        self.save_state()
        with self.assertRaisesRegex(ValueError, "fully closed"):
            self.reconcile()
    def test_legacy_component_rejected(self):
        with self.ledger.connection() as db:
            db.execute("UPDATE r2_components SET origin='INHERITED' WHERE job=?", (self.state["logical_job_id"],))
        with self.assertRaisesRegex(ValueError, "Historical"):
            self.reconcile()
    def test_partial_export_cannot_release_whole_job(self):
        del self.state["r4_verified_pages"]["2000"]
        self.save_state()
        with self.assertRaisesRegex(ValueError, "additional"):
            self.reconcile()
    def test_extra_http_receipt_without_accounted_attempt_rejected(self):
        write(self.work, "logs/hidden.json", {"operation": "results", "execution_id": self.state["execution_id"], "request_id": "hidden"})
        with self.assertRaisesRegex(ValueError, "additional result HTTP"):
            self.reconcile()
    def test_success_cache_and_settle_keep_reconciled_envelope(self):
        self.reconcile()
        live = object.__new__(Stage1DPageDune)
        live.w = self.work.resolve()
        live.db = self.ledger
        live.require_gate = lambda: None
        live.ensure_not_halted = lambda: None
        def reject_network(*args, **kwargs): raise AssertionError("No request expected")
        live.call = reject_network
        first = live.export(self.folder)
        self.assertTrue(first["cache_reused"])
        self.assertTrue(live.settle(self.folder)["already_closed"])
        self.assertEqual(live.export_envelope(self.state, 1000, self.folder)[0], Decimal(63))
        self.assertEqual(self.reconcile()["released_this_call"], "0")


if __name__ == "__main__": unittest.main()
