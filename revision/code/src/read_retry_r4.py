"""Small durable retry/claim mechanism for explicitly idempotent reads only.

It never executes a network request, releases a budget reservation or retries a
Dune SQL creation. Callers dispatch and account each attempt separately.
"""
from __future__ import annotations
from contextlib import contextmanager
from datetime import timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
from pathlib import Path
import random
import socket
import sqlite3
import time
import uuid


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def logical_key(provider, method=None, params=None):
    identity = provider if isinstance(provider, dict) and method is None else {"provider": provider, "method": method, "params": params}
    if not isinstance(identity, dict) or not identity.get("provider") or not identity.get("method"):
        raise ValueError("Stable read identity needs provider, method and nonsecret selectors")
    def inspect(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key.lower().replace("-", "_") in {"run", "run_id", "api_key", "apikey", "token", "authorization", "cookie", "password", "credential"}:
                    raise ValueError("Run identity and authentication must not enter logical request keys")
                inspect(child)
        elif isinstance(value, list):
            for child in value: inspect(child)
    inspect(identity)
    return hashlib.sha256(_json(identity).encode()).hexdigest()


def retry_after_seconds(value, now):
    if value is None: return 0.0
    try:
        seconds = float(value)
        if seconds < 0: return 0.0
        if seconds == float("inf") or seconds != seconds: raise ValueError()
        return seconds
    except (ValueError, TypeError):
        try:
            stamp = parsedate_to_datetime(str(value))
            if stamp.tzinfo is None: stamp = stamp.replace(tzinfo=timezone.utc)
            return max(0.0, stamp.timestamp() - now)
        except (ValueError, TypeError, OverflowError):
            return 0.0


def classify_failure(exception=None, *, http_status=None, provider_error=None, category=None, headers=None):
    """Provider messages are classified in memory, never echoed in receipts."""
    error_class = category
    retryable = False
    transient_categories = {"TimeoutError", "IncompleteRead", "ConnectionReset", "ConnectionResetError", "TemporaryDNSFailure", "RATE_LIMIT", "TEMPORARY_PROVIDER_ERROR", "UNRECEIVED_BATCH_MEMBER"} | {"HTTP_"+str(code) for code in (408,429,500,502,503,504)}
    if category in transient_categories:
        retryable = True
    elif category in {"AUTH_FAILURE", "ENTITLEMENT_FAILURE", "INVALID_PARAMS", "SCHEMA_ERROR", "FACT_CONFLICT", "PERMANENT_FAILURE"}:
        retryable = False
    elif http_status in {408, 429, 500, 502, 503, 504}:
        error_class, retryable = "HTTP_" + str(http_status), True
    elif http_status is not None and http_status >= 400:
        error_class = "HTTP_" + str(http_status)
    elif provider_error is not None:
        if isinstance(provider_error,dict) and isinstance(provider_error.get("error"),dict):
            provider_error = provider_error["error"]
        code = provider_error.get("code") if isinstance(provider_error, dict) else None
        text = str(provider_error.get("message", "")) if isinstance(provider_error, dict) else str(provider_error)
        lowered = text.lower()
        if any(word in lowered for word in ("upgrade", "not available on free", "not supported", "not authorized", "invalid api key", "missing api key", "unauthorized", "forbidden", "invalid param")) or code in {-32600, -32601, -32602}:
            error_class = "AUTH_ENTITLEMENT_OR_INVALID_REQUEST"
        elif code in {429, -32005} or any(word in lowered for word in ("rate limit", "too many requests", "max rate", "temporarily unavailable", "server busy", "try again", "timeout")):
            error_class, retryable = "RATE_LIMIT" if "rate" in lowered or code in {429, -32005} else "TEMPORARY_PROVIDER_ERROR", True
        else:
            error_class = "PROVIDER_REJECTED_OR_SCHEMA_ERROR"
    elif exception is not None:
        from http.client import IncompleteRead
        reason = getattr(exception, "reason", exception)
        if isinstance(reason, (TimeoutError, socket.timeout)):
            error_class, retryable = "TimeoutError", True
        elif isinstance(reason, IncompleteRead):
            error_class, retryable = "IncompleteRead", True
        elif isinstance(reason, ConnectionResetError):
            error_class, retryable = "ConnectionReset", True
        elif isinstance(reason, socket.gaierror) and reason.errno == socket.EAI_AGAIN:
            error_class, retryable = "TemporaryDNSFailure", True
        else:
            error_class = type(reason).__name__
    return {"retryable": retryable, "error_class": error_class or "UNKNOWN_ERROR",
            "outcome": "RETRYABLE_FAILURE" if retryable else "PERMANENT_FAILURE",
            "retry_after": (headers or {}).get("Retry-After", (headers or {}).get("retry-after"))}


def _process_alive(pid):
    if pid == os.getpid(): return True
    if os.name == "nt":
        import ctypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = ctypes.c_void_p
        handle = kernel.OpenProcess(0x1000, False, int(pid))
        if not handle: return ctypes.get_last_error() == 5  # access denied is not death
        code = ctypes.c_ulong()
        try:
            ok = kernel.GetExitCodeProcess(ctypes.c_void_p(handle), ctypes.byref(code))
            return not ok or code.value == 259
        finally:
            kernel.CloseHandle(ctypes.c_void_p(handle))
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError: return False
    except PermissionError: return True


class ReadRetryStore:
    def __init__(self, sqlite_path, *, clock=time.time, rng=random.random, total_attempts=3, lease_seconds=60, base_backoff=1, backoff_cap=30, owner_alive=None):
        if total_attempts != 3: raise ValueError("R4 read authorization is exactly three total attempts")
        self.path = Path(sqlite_path); self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock, self.rng, self.total_attempts = clock, rng, total_attempts
        self.lease_seconds, self.base_backoff, self.backoff_cap = lease_seconds, base_backoff, backoff_cap
        self.owner, self.owner_alive = uuid.uuid4().hex, owner_alive or _process_alive
        with self._db() as db:
            db.executescript("""CREATE TABLE IF NOT EXISTS read_requests(
              logical_key TEXT PRIMARY KEY, identity_json TEXT NOT NULL, state TEXT NOT NULL,
              next_eligible_at REAL NOT NULL DEFAULT 0, success_payload TEXT, success_receipt TEXT);
            CREATE TABLE IF NOT EXISTS read_attempts(
              attempt_id TEXT PRIMARY KEY, logical_key TEXT NOT NULL, attempt_no INTEGER NOT NULL,
              retry_of TEXT, owner TEXT, owner_pid INTEGER, created_at REAL NOT NULL,
              dispatched_at REAL, finished_at REAL, outcome TEXT NOT NULL,
              error_class TEXT, receipt_json TEXT, accounting_json TEXT, payload_sha256 TEXT);
            CREATE INDEX IF NOT EXISTS read_attempt_key ON read_attempts(logical_key,created_at,attempt_id);
            CREATE TABLE IF NOT EXISTS legacy_cache_migrations(
              artifact_sha256 TEXT, logical_key TEXT, legacy_path TEXT, classification TEXT NOT NULL,
              imported_attempt_id TEXT, created_at REAL NOT NULL, PRIMARY KEY(artifact_sha256,logical_key));
            """)

    @contextmanager
    def _db(self, write=False):
        db = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            if write: db.execute("BEGIN IMMEDIATE")
            yield db
            if write: db.commit()
        except BaseException:
            if write: db.rollback()
            raise
        finally: db.close()

    def _request(self, db, identity):
        key = logical_key(identity)
        db.execute("INSERT OR IGNORE INTO read_requests(logical_key,identity_json,state) VALUES(?,?,'READY')", (key, _json(identity)))
        return key

    def claim(self, identity, *, deadline=None, allow_recovery=True):
        now = self.clock()
        with self._db(True) as db:
            key = self._request(db, identity)
            request = db.execute("SELECT * FROM read_requests WHERE logical_key=?", (key,)).fetchone()
            if request["state"] == "SUCCESS":
                return {"state": "CACHE_HIT", "logical_key": key, "payload": json.loads(request["success_payload"]), "receipt": json.loads(request["success_receipt"] or "{}")}
            if request["state"] == "HISTORICAL_ATTEMPT_COUNT_UNRESOLVED":
                return {"state":"HISTORICAL_ATTEMPT_COUNT_UNRESOLVED", "logical_key":key, "old_attempt_risk_preserved":True}
            attempts = db.execute("SELECT * FROM read_attempts WHERE logical_key=? AND outcome!='ABANDONED_BEFORE_DISPATCH' ORDER BY attempt_no,created_at", (key,)).fetchall()
            active = next((a for a in reversed(attempts) if a["outcome"] == "IN_FLIGHT"), None)
            if active:
                # Lease expiry by itself is insufficient to authorize concurrent
                # transport. Recovery additionally requires a dead prior owner.
                dead = not self.owner_alive(active["owner_pid"])
                if not allow_recovery or not dead or now < active["created_at"] + self.lease_seconds:
                    return {"state": "IN_FLIGHT", "logical_key": key, "attempt_id": active["attempt_id"]}
                db.execute("UPDATE read_attempts SET outcome='UNRESOLVED',finished_at=?,error_class='PROCESS_INTERRUPTED_READ' WHERE attempt_id=?", (now, active["attempt_id"]))
            last = attempts[-1] if attempts else None
            if request["state"] == "PERMANENT_FAILURE":
                return {"state": "PERMANENT_FAILURE", "logical_key": key, "attempt_id": last["attempt_id"] if last else None}
            if len(attempts) >= self.total_attempts:
                return {"state": "RETRIES_EXHAUSTED", "logical_key": key, "attempts": len(attempts)}
            eligible = request["next_eligible_at"]
            if now < eligible or deadline is not None and now >= deadline:
                return {"state": "DEFERRED", "logical_key": key, "next_eligible_at": eligible, "deadline": deadline}
            attempt_id = "read-r4-" + uuid.uuid4().hex
            number = len(attempts) + 1
            retry_of = last["attempt_id"] if last else None
            db.execute("INSERT INTO read_attempts(attempt_id,logical_key,attempt_no,retry_of,owner,owner_pid,created_at,outcome) VALUES(?,?,?,?,?,?,?,'IN_FLIGHT')", (attempt_id,key,number,retry_of,self.owner,os.getpid(),now))
            db.execute("UPDATE read_requests SET state='IN_FLIGHT' WHERE logical_key=?", (key,))
            return {"state": "CLAIMED", "logical_key": key, "attempt_id": attempt_id, "attempt_no": number, "retry_of": retry_of}

    def mark_dispatched(self, attempt_id, *, accounting=None):
        with self._db(True) as db:
            row = db.execute("SELECT * FROM read_attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if row is None or row["outcome"] != "IN_FLIGHT": raise ValueError("Only a live claimed read can dispatch")
            if row["dispatched_at"] is not None:
                raise ValueError("Attempt already dispatched; transport cannot run twice")
            db.execute("UPDATE read_attempts SET dispatched_at=?,accounting_json=? WHERE attempt_id=?", (self.clock(), _json(accounting or {}), attempt_id))

    def abandon_before_dispatch(self, attempt_id, reason="BUDGET_DEFERRED"):
        with self._db(True) as db:
            row = db.execute("SELECT * FROM read_attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if row is None or row["dispatched_at"] is not None or row["outcome"] != "IN_FLIGHT": raise ValueError("Dispatched read intent must retain risk and attempt")
            db.execute("UPDATE read_attempts SET outcome='ABANDONED_BEFORE_DISPATCH',finished_at=?,error_class=? WHERE attempt_id=?", (self.clock(), reason, attempt_id))
            db.execute("UPDATE read_requests SET state='READY' WHERE logical_key=?", (row["logical_key"],))

    def finish(self, attempt_id, outcome, *, payload=None, error_class=None, retry_after=None, receipt=None, accounting=None):
        if outcome not in {"SUCCESS", "RETRYABLE_FAILURE", "PERMANENT_FAILURE", "UNRESOLVED"}: raise ValueError("Explicit read outcome required")
        now = self.clock()
        with self._db(True) as db:
            row = db.execute("SELECT * FROM read_attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if row is None: raise ValueError("Unknown read intent")
            if row["outcome"] != "IN_FLIGHT":
                if row["outcome"] == outcome:
                    new_hash = hashlib.sha256(_json(payload).encode()).hexdigest() if outcome == "SUCCESS" else None
                    if new_hash != row["payload_sha256"] or accounting is not None and accounting != json.loads(row["accounting_json"] or "{}"):
                        raise ValueError("Final attempt payload and accounting are immutable")
                    return dict(row)
                raise ValueError("Final attempt outcome is immutable")
            old_accounting = json.loads(row["accounting_json"] or "{}")
            if accounting is not None and old_accounting and accounting != old_accounting:
                raise ValueError("Do not overwrite old attempt accounting or release its uncertainty")
            persisted_accounting = old_accounting or accounting or {}
            payload_text = _json(payload) if outcome == "SUCCESS" else None
            db.execute("UPDATE read_attempts SET finished_at=?,outcome=?,error_class=?,receipt_json=?,accounting_json=?,payload_sha256=? WHERE attempt_id=?", (now,outcome,error_class,_json(receipt or {}),_json(persisted_accounting),hashlib.sha256(payload_text.encode()).hexdigest() if payload_text is not None else None,attempt_id))
            delay = max(retry_after_seconds(retry_after, now), max(0.0,min(1.0,float(self.rng()))) * min(self.backoff_cap,self.base_backoff * (2 ** (row["attempt_no"] - 1)))) if outcome in {"RETRYABLE_FAILURE", "UNRESOLVED"} else 0
            state = "SUCCESS" if outcome == "SUCCESS" else "PERMANENT_FAILURE" if outcome == "PERMANENT_FAILURE" else "RETRIES_EXHAUSTED" if row["attempt_no"] >= self.total_attempts else "READY"
            db.execute("UPDATE read_requests SET state=?,next_eligible_at=?,success_payload=?,success_receipt=? WHERE logical_key=?", (state,now+delay,payload_text,_json(receipt or {}) if outcome=="SUCCESS" else None,row["logical_key"]))
            return {"state":state,"logical_key":row["logical_key"],"attempt_id":attempt_id,"attempt_no":row["attempt_no"],"retry_of":row["retry_of"],"next_eligible_at":now+delay,"old_attempt_risk_preserved":True}

    def import_attempt(self, identity, legacy_id, *, dispatched=True, outcome="UNRESOLVED", payload=None, accounting=None, receipt=None, created_at=None, error_class=None, retry_after=None):
        """Idempotent historical import; old bytes/budget rows remain untouched."""
        if outcome not in {"SUCCESS", "RETRYABLE_FAILURE", "PERMANENT_FAILURE", "UNRESOLVED"}: raise ValueError("Invalid legacy outcome")
        attempt_id = "legacy-read-" + hashlib.sha256(str(legacy_id).encode()).hexdigest()
        with self._db(True) as db:
            key = self._request(db, identity)
            old = db.execute("SELECT * FROM read_attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if old:
                if old["logical_key"] != key: raise ValueError("Legacy attempt identity reassigned")
                expected_hash = hashlib.sha256(_json(payload).encode()).hexdigest() if outcome == "SUCCESS" else None
                if old["outcome"] != outcome or old["payload_sha256"] != expected_hash or accounting is not None and accounting != json.loads(old["accounting_json"] or "{}"):
                    raise ValueError("Historical attempt outcome, payload and accounting are immutable")
                return {"state":"ALREADY_IMPORTED","attempt_id":attempt_id,"logical_key":key}
            previous = db.execute("SELECT * FROM read_attempts WHERE logical_key=? AND outcome!='ABANDONED_BEFORE_DISPATCH' ORDER BY attempt_no DESC,created_at DESC LIMIT 1", (key,)).fetchone()
            request = db.execute("SELECT * FROM read_requests WHERE logical_key=?", (key,)).fetchone()
            encoded = _json(payload) if outcome == "SUCCESS" else None
            if request["state"] == "SUCCESS" and outcome == "SUCCESS" and request["success_payload"] != encoded:
                raise ValueError("Historical successful read payload conflict")
            number = previous["attempt_no"] + 1 if previous else 1
            now = self.clock(); stamp = now if created_at is None else created_at
            db.execute("INSERT INTO read_attempts(attempt_id,logical_key,attempt_no,retry_of,owner,owner_pid,created_at,dispatched_at,finished_at,outcome,error_class,receipt_json,accounting_json,payload_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (attempt_id,key,number,previous["attempt_id"] if previous else None,"HISTORICAL_IMPORT",0,stamp,stamp if dispatched else None,stamp,outcome,error_class,_json(receipt or {}),_json(accounting or {}),hashlib.sha256(encoded.encode()).hexdigest() if encoded is not None else None))
            if request["state"] != "SUCCESS":
                state = "SUCCESS" if outcome=="SUCCESS" else "PERMANENT_FAILURE" if outcome=="PERMANENT_FAILURE" else "RETRIES_EXHAUSTED" if number>=self.total_attempts else "READY"
                db.execute("UPDATE read_requests SET state=?,next_eligible_at=?,success_payload=?,success_receipt=? WHERE logical_key=?", (state,now+retry_after_seconds(retry_after,now),encoded,_json(receipt or {}) if encoded is not None else None,key))
            return {"state":"IMPORTED","attempt_id":attempt_id,"logical_key":key,"attempt_no":number}

    def attempts(self, identity):
        with self._db() as db:
            rows = db.execute("SELECT * FROM read_attempts WHERE logical_key=? ORDER BY attempt_no,created_at", (logical_key(identity),)).fetchall()
            return [dict(row) for row in rows]

    def migrate_cache(self, identity, path, classification, *, payload=None, history=None):
        """Migrate immutable bytes without inventing an old attempt allowance.

        Failed bytes alone do not establish how many reads occurred. ``history``
        must be the caller's complete evidence-backed prior attempt list, each
        entry containing legacy_id plus import_attempt kwargs. No history means
        an explicit unresolved boundary. Successful bytes can be reused without
        making a new request or freeing any original risk.
        """
        path = Path(path); sha = hashlib.sha256(path.read_bytes()).hexdigest()
        outcome = "SUCCESS" if classification == "SUCCESS" else "RETRYABLE_FAILURE" if classification == "RETRYABLE_FAILURE" else "PERMANENT_FAILURE"
        if history is not None:
            if not isinstance(history,list) or not history: raise ValueError("Complete historical attempt evidence must be nonempty")
            for attempt in history:
                item = dict(attempt); legacy_id = item.pop("legacy_id")
                result = self.import_attempt(identity,legacy_id,**item)
        elif outcome == "SUCCESS":
            result = self.import_attempt(identity, "legacy-success-cache:"+logical_key(identity)+":"+sha, outcome=outcome, payload=payload, receipt={"legacy_path":str(path),"artifact_sha256":sha,"bytes_retained":True,"historical_attempt_total":"UNKNOWN_NO_NEW_DISPATCH_AUTHORIZED"}, accounting={"legacy_cost_and_risk":"PRESERVED_IN_ORIGINAL_LEDGER_NOT_REDUCED_BY_CACHE_MIGRATION"})
        else:
            with self._db(True) as db:
                key = self._request(db,identity)
                previous = db.execute("SELECT imported_attempt_id FROM legacy_cache_migrations WHERE artifact_sha256=? AND logical_key=?",(sha,key)).fetchone()
                if previous and previous["imported_attempt_id"]:
                    result = {"state":"MIGRATION_ALREADY_INDEXED","attempt_id":previous["imported_attempt_id"],"logical_key":key}
                else:
                    db.execute("UPDATE read_requests SET state='HISTORICAL_ATTEMPT_COUNT_UNRESOLVED' WHERE logical_key=? AND state!='SUCCESS'",(key,))
                    result = {"state":"HISTORICAL_ATTEMPT_COUNT_UNRESOLVED", "attempt_id":None,"logical_key":key}
        with self._db(True) as db:
            db.execute("INSERT OR IGNORE INTO legacy_cache_migrations VALUES(?,?,?,?,?,?)", (sha,logical_key(identity),str(path),classification,result["attempt_id"],self.clock()))
            if result["attempt_id"]:
                db.execute("UPDATE legacy_cache_migrations SET imported_attempt_id=COALESCE(imported_attempt_id,?) WHERE artifact_sha256=? AND logical_key=?",(result["attempt_id"],sha,logical_key(identity)))
        return result
