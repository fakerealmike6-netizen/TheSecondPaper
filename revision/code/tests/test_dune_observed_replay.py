"""Synthetic saved results only; no live caches or network access."""
import hashlib, json, sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from collector import NATIVE
from provider_dune import build_interval_sql
from dune_observed_replay import SavedDuneProvider, parse_interval_sql

A='0x'+'1'*40; B='0x'+'2'*40; TX='0x'+'4'*64


def row():
    return dict(event_kind='top', tx_hash=TX, sender=A, recipient=B,
                amount_raw='101', block_number=100, tx_index=2,
                block_time='2023-01-01 00:00:00.000 UTC', log_index=None,
                trace_address=None, success=True, contract_address=None,
                gas_used='21000', gas_price='7', trace_type=None, call_type=None)


def write_job(work, *, count=1, next_offset=None, rows=None):
    sql=build_interval_sql(A,NATIVE,100,200,start_time=1672531200,end_time=1672534800)
    digest=hashlib.sha256(sql.encode()).hexdigest()
    folder=work/'jobs'/digest;folder.mkdir(parents=True)
    (folder/'query.sql').write_text(sql,encoding='utf-8')
    (folder/'job.json').write_text(json.dumps(dict(state='QUERY_STATE_COMPLETED',
        execution_id='SYNTHETIC_EXECUTION',logical_job_id='synthetic:'+digest,sql_sha256=digest)))
    page=dict(execution_id='SYNTHETIC_EXECUTION',state='QUERY_STATE_COMPLETED',
              result=dict(rows=rows if rows is not None else [row()],metadata=dict(row_count=len(rows) if rows is not None else 1,total_row_count=count)))
    if next_offset is not None:page['next_offset']=next_offset
    raw=json.dumps(page).encode();rawpath=work/'raw.json';rawpath.write_bytes(raw)
    (folder/'page_0.json').write_text(json.dumps(page,indent=2))
    receipt=dict(execution_id='SYNTHETIC_EXECUTION',http_status=200,parameters={'offset':0},
                 raw_path='raw.json',raw_bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
    (folder/'page_0_receipt.json').write_text(json.dumps(receipt))
    return folder


class SavedDuneReplayTests(unittest.TestCase):
    def invoke(self,provider,address=A,**overrides):
        args=dict(start_block=100,end_block=200,start_time=1672531200,end_time=1672534800,global_end_time=1672534800)
        args.update(overrides)
        return provider.fetch_interval(address,NATIVE,**args)

    def test_complete_page_raw_receipt_and_missing_actual_frontier(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            work=Path(tmp);write_job(work);provider=SavedDuneProvider(work/'jobs',work)
            observed=self.invoke(provider);self.assertTrue(observed.complete)
            self.assertEqual(len(observed.events),1);self.assertEqual(observed.real_requests,0)
            missing=self.invoke(provider,B);self.assertFalse(missing.complete)
            self.assertFalse(missing.events);self.assertEqual(missing.gaps[0]['reason'],'DUNE_SAVED_INTERVAL_MISSING')

    def test_partial_page_never_substitutes_complete_interval(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            work=Path(tmp);write_job(work,count=2,next_offset=1)
            provider=SavedDuneProvider(work/'jobs',work)
            self.assertEqual(len(provider.rejected_jobs),1);self.assertFalse(self.invoke(provider).events)

    def test_raw_payload_mismatch_rejects_saved_page(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            work=Path(tmp);folder=write_job(work)
            page=json.loads((folder/'page_0.json').read_text());page['result']['rows'][0]['amount_raw']='999'
            (folder/'page_0.json').write_text(json.dumps(page))
            provider=SavedDuneProvider(work/'jobs',work)
            self.assertFalse(self.invoke(provider).complete);self.assertIn('differs',provider.rejected_jobs[0]['reason'])

    def test_template_accepts_equivalent_partition_but_rejects_limit(self):
        sql=build_interval_sql(A,NATIVE,100,200,start_time=1672531200,end_time=1672534800)
        partitioned=sql.replace('WHERE block_number','WHERE block_date BETWEEN DATE \'2023-01-01\' AND DATE \'2023-01-01\' AND block_number')
        self.assertEqual(parse_interval_sql(partitioned)['address'],A)
        with self.assertRaises(ValueError):parse_interval_sql(sql+' LIMIT 1')
        with self.assertRaises(ValueError):parse_interval_sql(partitioned.replace("DATE '2023-01-01' AND", "DATE '2023-01-02' AND"))

    def test_outside_saved_time_not_claimed_complete(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            work=Path(tmp);write_job(work);provider=SavedDuneProvider(work/'jobs',work)
            self.assertFalse(self.invoke(provider,end_block=201).complete)
            self.assertFalse(self.invoke(provider,start_time=1672531199).complete)

    def test_physical_duplicate_not_extra_capacity_and_malformed_is_gap(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            work=Path(tmp);write_job(work,count=2,rows=[row(),row()]);provider=SavedDuneProvider(work/'jobs',work)
            result=self.invoke(provider);self.assertTrue(result.complete);self.assertEqual(len(result.events),1)
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            work=Path(tmp);write_job(work,rows=[row()|{'success':None}]);provider=SavedDuneProvider(work/'jobs',work)
            result=self.invoke(provider);self.assertFalse(result.complete);self.assertFalse(result.events)


if __name__=='__main__':unittest.main()
