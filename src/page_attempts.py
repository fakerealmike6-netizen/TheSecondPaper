"""Local crash-safe, process-safe request journal. No networking or billing.

A DISPATCH_INTENT may have reached the server. It is never reset because a
response file is absent. An unresolved request blocks further requests for its
account context, including changing SQL/limit. Explicit recovery is deliberately
not implemented: it requires a separate authorized risk reservation.
"""
from __future__ import annotations
import hashlib,json,os,sqlite3,uuid
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime,timezone

class RequestBlocked(RuntimeError):
    reason='DUNE_PRIOR_ATTEMPT_UNRESOLVED_NO_AUTOMATIC_RESUBMISSION'

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,default=str)
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()
def atomic_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with tmp.open('x',encoding='utf-8',newline='\n') as f:
            f.write(canonical(value));f.flush();os.fsync(f.fileno())
        os.replace(tmp,path)
    finally:
        if tmp.exists():tmp.unlink()

class AttemptStore:
    def __init__(self,path):
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        with self.connection() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS streams(
              stream_id TEXT PRIMARY KEY, account TEXT NOT NULL, identity TEXT NOT NULL,
              fixed_parameters TEXT NOT NULL, next_offset INTEGER, complete INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS attempts(
              attempt_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, offset INTEGER NOT NULL,
              identity TEXT NOT NULL, state TEXT NOT NULL, error TEXT, response_path TEXT,
              receipt_path TEXT, response_sha TEXT, receipt_sha TEXT, progress TEXT,
              dispatched_utc TEXT NOT NULL, UNIQUE(stream_id,offset));
            CREATE TABLE IF NOT EXISTS transitions(
              n INTEGER PRIMARY KEY AUTOINCREMENT, attempt_id TEXT NOT NULL,
              state TEXT NOT NULL, utc TEXT NOT NULL);''')
    @contextmanager
    def connection(self):
        db=sqlite3.connect(self.path,timeout=30,isolation_level=None)
        db.row_factory=sqlite3.Row
        try:
            db.execute('PRAGMA synchronous=FULL');yield db
        finally:db.close()
    @staticmethod
    def identity(account,job,execution,operation,parameters):
        if not all(isinstance(v,str) and v for v in (account,job,execution,operation)):
            raise ValueError('Non-secret account/job/execution/operation references required')
        return dict(account_context_ref=account,logical_job_id=job,execution_id=execution,operation=operation,parameters=dict(parameters))
    @staticmethod
    def stream_identity(identity):return {k:v for k,v in identity.items() if k!='parameters'}
    def unresolved(self,account):
        with self.connection() as db:return [dict(r) for r in db.execute("SELECT a.* FROM attempts a JOIN streams s USING(stream_id) WHERE s.account=? AND a.state!='SUCCESS_VALIDATED'",(account,))]
    def rows(self):
        with self.connection() as db:return [dict(r) for r in db.execute('SELECT * FROM attempts ORDER BY dispatched_utc,attempt_id')]
    def get(self,identity):
        with self.connection() as db:
            row=db.execute('SELECT * FROM attempts WHERE attempt_id=?',(digest(identity),)).fetchone()
            if row is None:
                prior=db.execute('SELECT * FROM attempts WHERE stream_id=? AND offset=?',(digest(self.stream_identity(identity)),identity['parameters'].get('offset',0))).fetchone()
                if prior is not None:raise RequestBlocked('Saved offset is bound to different frozen parameters')
            return dict(row) if row else None
    def dispatch(self,identity):
        """Commit the only dispatch right before a caller may invoke transport."""
        aid=digest(identity);sid=digest(self.stream_identity(identity));params=identity['parameters'];offset=params.get('offset',0)
        if isinstance(offset,bool) or not isinstance(offset,int) or offset<0:raise ValueError('Exact nonnegative offset required')
        fixed={k:v for k,v in params.items() if k!='offset'};stamp=datetime.now(timezone.utc).isoformat()
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                if db.execute("SELECT 1 FROM attempts a JOIN streams s USING(stream_id) WHERE s.account=? AND a.state!='SUCCESS_VALIDATED'",(identity['account_context_ref'],)).fetchone():
                    raise RequestBlocked('A submitted request remains failed or uncertain; no retry, replacement SQL or continuation')
                stream=db.execute('SELECT * FROM streams WHERE stream_id=?',(sid,)).fetchone()
                if stream is None:
                    if offset!=0:raise RequestBlocked('A stream must begin at offset0')
                    db.execute('INSERT INTO streams VALUES(?,?,?,?,?,0)',(sid,identity['account_context_ref'],canonical(self.stream_identity(identity)),canonical(fixed),0))
                elif stream['fixed_parameters']!=canonical(fixed):raise RequestBlocked('Frozen request parameters changed')
                elif stream['complete'] or stream['next_offset']!=offset:raise RequestBlocked('Only an unrequested exact next offset may be dispatched')
                if db.execute('SELECT 1 FROM attempts WHERE stream_id=? AND offset=?',(sid,offset)).fetchone():raise RequestBlocked('Page already dispatched; use validated cached response')
                db.execute('INSERT INTO attempts(attempt_id,stream_id,offset,identity,state,dispatched_utc) VALUES(?,?,?,?,?,?)',(aid,sid,offset,canonical(identity),'PLANNED',stamp))
                db.execute('INSERT INTO transitions(attempt_id,state,utc) VALUES(?,?,?)',(aid,'PLANNED',stamp))
                db.execute("UPDATE attempts SET state='DISPATCH_INTENT' WHERE attempt_id=?",(aid,))
                db.execute('INSERT INTO transitions(attempt_id,state,utc) VALUES(?,?,?)',(aid,'DISPATCH_INTENT',stamp))
                db.commit()
            except BaseException:db.rollback();raise
        return aid
    def mark(self,aid,state,error):
        if state not in ('UNKNOWN_TRANSPORT','INVALID_RESPONSE'):raise ValueError('Invalid non-success state')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                row=db.execute('SELECT state FROM attempts WHERE attempt_id=?',(aid,)).fetchone()
                if not row or row['state']=='SUCCESS_VALIDATED':raise RequestBlocked('Cannot mutate a validated attempt')
                db.execute('UPDATE attempts SET state=?,error=? WHERE attempt_id=?',(state,str(error),aid))
                db.execute('INSERT INTO transitions(attempt_id,state,utc) VALUES(?,?,?)',(aid,state,datetime.now(timezone.utc).isoformat()));db.commit()
            except BaseException:db.rollback();raise
    def save_response(self,aid,response,receipt,response_path,receipt_path):
        atomic_json(response_path,response);atomic_json(receipt_path,receipt)
        rp,cp=Path(response_path).resolve(),Path(receipt_path).resolve()
        with self.connection() as db:
            db.execute('UPDATE attempts SET response_path=?,receipt_path=?,response_sha=?,receipt_sha=? WHERE attempt_id=?',
                (str(rp),str(cp),hashlib.sha256(rp.read_bytes()).hexdigest(),hashlib.sha256(cp.read_bytes()).hexdigest(),aid))
    def validated(self,aid,progress):
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                row=db.execute('SELECT * FROM attempts WHERE attempt_id=?',(aid,)).fetchone()
                if not row or row['state']!='DISPATCH_INTENT' or not row['response_sha'] or not row['receipt_sha']:
                    raise RequestBlocked('Response must be atomically persisted before validation')
                self.verify_files(dict(row))
                db.execute("UPDATE attempts SET state='SUCCESS_VALIDATED',progress=? WHERE attempt_id=?",(canonical(progress),aid))
                db.execute('UPDATE streams SET next_offset=?,complete=? WHERE stream_id=?',(progress.get('next_offset'),int(progress.get('complete',False)),row['stream_id']))
                db.execute('INSERT INTO transitions(attempt_id,state,utc) VALUES(?,?,?)',(aid,'SUCCESS_VALIDATED',datetime.now(timezone.utc).isoformat()));db.commit()
            except BaseException:db.rollback();raise
    @staticmethod
    def verify_files(row):
        for p,h in [('response_path','response_sha'),('receipt_path','receipt_sha')]:
            if not row.get(p) or hashlib.sha256(Path(row[p]).read_bytes()).hexdigest()!=row.get(h):raise RequestBlocked('Saved response/receipt hash mismatch')
    def cached(self,identity):
        row=self.get(identity)
        if row is None:return None
        if row['state']!='SUCCESS_VALIDATED':raise RequestBlocked('Prior submitted page failed or remains uncertain')
        self.verify_files(row)
        return json.loads(Path(row['response_path']).read_text(encoding='utf-8')),json.loads(Path(row['receipt_path']).read_text(encoding='utf-8')),json.loads(row['progress'])
