"""Small persistent shared stage ledger. Decimal credits and integer byte/operation units."""
import sqlite3, json, datetime
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from legacy_guard_r4 import reject_legacy_ledger_path

CAPS = {'dune_credits': '10', 'bigquery_bytes': '5368709120', 'rpc_operations': '500',
        'alchemy_cu': '50000', 'meta_addresses': '10', 'meta_requests': '10'}
INTEGER = set(CAPS) - {'dune_credits'}

def valid(value, unit):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError('Invalid amount type')
    if unit in INTEGER and isinstance(value,float): raise ValueError('Integer units must not originate from float')
    try: d = Decimal(str(value))
    except InvalidOperation: raise ValueError('Invalid numeric amount')
    if not d.is_finite() or d < 0 or (unit in INTEGER and d != d.to_integral_value()):
        raise ValueError('Invalid finite/nonnegative/integer amount')
    return d

class Ledger:
    def __init__(self, path):
        reject_legacy_ledger_path(path)
        self.path = str(path)
        with self.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS limits(unit TEXT PRIMARY KEY, cap TEXT, available TEXT, evidence TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, provider TEXT, purpose TEXT, utc TEXT, status TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS amounts(job TEXT, unit TEXT, reserved TEXT, actual TEXT, PRIMARY KEY(job,unit))')
            for u,c in CAPS.items(): db.execute('INSERT OR IGNORE INTO limits VALUES(?,?,NULL,?)',(u,c,'Allowance not confirmed'))

    @contextmanager
    def connection(self):
        db=sqlite3.connect(self.path, timeout=20); db.execute('PRAGMA busy_timeout=20000')
        try:
            with db: yield db
        finally: db.close()

    def confirm(self, unit, available, evidence):
        d=valid(available,unit)
        if not evidence: raise ValueError('Allowance evidence required')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('UPDATE limits SET available=?, evidence=? WHERE unit=?',(str(d),evidence,unit))

    def snapshot(self, db=None):
        if db is None:
            with self.connection() as cx: return self.snapshot(cx)
        out={}
        for u,c,a,e in db.execute('SELECT * FROM limits'):
            spent=Decimal(0); reserved=Decimal(0); exceeded=False
            for r,v in db.execute('SELECT reserved,actual FROM amounts WHERE unit=?',(u,)):
                if v is None: reserved+=Decimal(r)
                else:
                    spent+=Decimal(v)
                    exceeded |= Decimal(v)>Decimal(r)
            effective=None if a is None else min(Decimal(c),Decimal(a))
            out[u]={'cap':c,'confirmed_allowance':a,'actual':str(spent),'reserved':str(reserved),
                    'remaining':None if effective is None else str(max(Decimal(0),effective-spent-reserved)),
                    'overrun':effective is not None and spent+reserved>effective,
                    'actual_exceeded_reservation':exceeded,'evidence':e}
        return out

    def reserve(self, job, provider, purpose, amounts):
        parsed={u:valid(v,u) for u,v in amounts.items()}
        if not parsed or any(u not in CAPS for u in parsed): raise ValueError('Unknown unit')
        if provider=='alchemy' and not {'alchemy_cu','rpc_operations'}.issubset(parsed): raise ValueError('Alchemy requires both operation and CU reservations')
        if parsed.get('dune_credits',0)>2: raise ValueError('Dune logical job exceeds combined cap')
        if parsed.get('bigquery_bytes',0)>1073741824: raise ValueError('BigQuery job exceeds cap')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE'); snap=self.snapshot(db)
            if any(x['overrun'] or x['actual_exceeded_reservation'] for x in snap.values()): raise RuntimeError('Stage halted after overrun')
            for u,n in parsed.items():
                if snap[u]['remaining'] is None or n>Decimal(snap[u]['remaining']): raise RuntimeError('Allowance unknown or insufficient: '+u)
            db.execute('INSERT INTO jobs VALUES(?,?,?,?,?)',(job,provider,purpose,datetime.datetime.now(datetime.timezone.utc).isoformat(),'RESERVED'))
            for u,n in parsed.items(): db.execute('INSERT INTO amounts VALUES(?,?,?,NULL)',(job,u,str(n)))

    def settle(self, job, amounts):
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            rows=db.execute('SELECT unit,actual FROM amounts WHERE job=?',(job,)).fetchall()
            pending={u for u,v in rows if v is None}
            if not rows or not pending: raise ValueError('Unknown or settled job')
            if set(amounts)!=pending: raise ValueError('Settle only all pending units; settled units cannot be repeated')
            vals={u:None if v is None else valid(v,u) for u,v in amounts.items()}
            for u,v in vals.items():
                if v is not None: db.execute('UPDATE amounts SET actual=? WHERE job=? AND unit=?',(str(v),job,u))
            if any(v is None for v in vals.values()):
                db.execute('UPDATE jobs SET status=? WHERE id=?',('UNKNOWN_RESERVED',job)); return
            db.execute('UPDATE jobs SET status=? WHERE id=?',('SETTLED',job))

    def reserve_dune_job(self, job, purpose, execution_estimate, export_estimate):
        execution=valid(execution_estimate,'dune_credits')
        export=valid(export_estimate,'dune_credits')
        return self.reserve(job,'dune',purpose,{'dune_credits':execution+export})

    def rows(self):
        with self.connection() as db:
            names=['request_id','provider','purpose','utc','status','unit','reserved','actual']
            return [dict(zip(names,r)) for r in db.execute('SELECT id,provider,purpose,utc,status,unit,reserved,actual FROM jobs JOIN amounts ON id=job ORDER BY utc,id,unit')]
