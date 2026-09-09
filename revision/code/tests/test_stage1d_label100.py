import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from frontier_labels_r1 import TABLES, build_sql
from frontier_labels_r2 import optimized_sql
from page_attempts import atomic_json
from stage1d_runtime import Runtime
import stage1d_acquisition as acquisition


def addresses(count):return ['0x'+format(i+1,'040x') for i in range(count)]
def schema():
    return [{'table_schema':table.split('.')[0],'table_name':table.split('.')[1],
             'column_name':field,'data_type':'varchar'} for table,_,fields in TABLES.values() for field in fields]


class Label100Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.work=Path(self.tmp.name)
        self.query={'query_id':'query-1','name':'query1','scope_id':'scope-1','scope_hash':'a'*64}
        atomic_json(self.work/'private/BATCH_QUERY_FREEZE.json',{'queries':[self.query]})
        atomic_json(self.work/'private/stage1d_inputs/source_rules.json',{'table_schema':schema()})

    def tearDown(self):self.tmp.cleanup()

    def test_old_r1_r2_default40_remains(self):
        for constructor in (build_sql,optimized_sql):
            self.assertTrue(constructor(addresses(40),schema()))
            with self.assertRaises(ValueError):constructor(addresses(41),schema())

    def test_explicit100_one_sql_four_tables_and_nine_columns(self):
        cohort=addresses(100)
        sql=optimized_sql(list(reversed(cohort)),schema(),maximum_addresses=100)
        self.assertEqual(sql.count('AND s.address IN ('),4)
        self.assertEqual(sql.count('array_agg('),4)
        self.assertEqual(sql.count('GROUP BY s.address'),4)
        values=sql.split('frontier(address) AS (\n  VALUES\n    ',1)[1].split('\n)',1)[0]
        self.assertEqual(re.findall(r'0x[0-9a-f]{40}',values),cohort)
        aliases=re.findall(r' AS ([a-z_]+)(?:,|\n|$)',sql.rsplit('\nSELECT\n',1)[1].split('\nFROM frontier',1)[0])
        self.assertEqual(aliases,['address']+[name+suffix for name in TABLES for suffix in ('_json','_count')])
        body='\n'.join(line for line in sql.splitlines() if not line.startswith('--'))
        self.assertNotRegex(body,r'\b(?:LIMIT|DISTINCT)\b')
        self.assertTrue(sql.endswith('ORDER BY address\n'))

    def test_zero_101_and_duplicates_refused(self):
        for constructor in (build_sql,optimized_sql):
            for cohort in ([],addresses(101),addresses(2)+addresses(1),['0x'+'a'*40,'0x'+'A'*40]):
                with self.subTest(constructor=constructor.__name__,count=len(cohort)):
                    with self.assertRaises(ValueError):constructor(cohort,schema(),maximum_addresses=100)

    def test_illegal_addresses_and_unapproved_ceiling_refused(self):
        for constructor in (build_sql,optimized_sql):
            for bad in ('0x123','0x'+'g'*40,True,None,'0x'+'1'*40+'); DROP TABLE x'):
                with self.assertRaises(ValueError):constructor([bad],schema(),maximum_addresses=100)
            for cap in (True,0,41,101,'100'):
                with self.assertRaises(ValueError):constructor(addresses(1),schema(),maximum_addresses=cap)

    def test_stage1d_explicit100_passes_entire_freeze_chain_and_reuses_cohort(self):
        cohort=addresses(100);collection={'states':[{'state':{'address':a}} for a in cohort]}
        quota={'cap':2000,'remaining':2000,'legacy':True,'allocated_addresses_by_chain':{},
               'allocated_distinct':0,'confirmed_used_distinct':0}
        captures=[]
        def execute(work,freeze,*args):
            frozen=Runtime().verify_sql_freeze(freeze,work,sql_path=Path(freeze).parent/'query.sql')
            captures.append(frozen)
            return {'status':'SYNTHETIC_PAUSED_NO_NETWORK'}
        with patch('stage1d_label_opportunities.snapshot',return_value=quota),patch.object(acquisition,'execute_sql',side_effect=execute):
            result=acquisition.acquire_labels(self.work,self.query,collection)
            paths=list((self.work/'private/stage1d_label_cohorts').glob('*.json'));before=paths[0].read_bytes()
            again=acquisition.acquire_labels(self.work,self.query,collection)
        self.assertEqual(len(captures),2);self.assertEqual(captures[0],captures[1])
        self.assertEqual(captures[0]['addresses'],cohort)
        self.assertEqual(paths[0].read_bytes(),before)
        self.assertEqual(result['opportunities_reserved_now'],100)
        self.assertEqual(again['opportunities_reserved_now'],0);self.assertTrue(again['cohort_reused'])
        self.assertEqual(len(list((self.work/'private/stage1d_sql').glob('*/freeze_manifest.json'))),1)

    def test_runtime_freeze_rejects_zero_101_duplicates_and_invalid(self):
        sql=optimized_sql(addresses(1),schema(),maximum_addresses=100)
        freeze=acquisition.save_sql(self.work,sql,'frontier_labels',self.query,[],[],addresses(1))
        original=acquisition.read(freeze)
        for cohort in ([],addresses(101),addresses(1)*2,['invalid']):
            atomic_json(freeze,dict(original,addresses=cohort))
            with self.assertRaises(ValueError):Runtime().verify_sql_freeze(freeze,self.work,sql_path=freeze.parent/'query.sql')


if __name__=='__main__':unittest.main()
