import json,tempfile,unittest
from pathlib import Path
from frontier_labels_r2 import select_addresses,optimized_sql,verify_frozen,apply,unqueried_arrivals,VERSION,TABLES
from reference_core import dump,digest,write_csv
from reference_recompute import rows

A='0x'+'1'*40;B='0x'+'2'*40;C='0x'+'3'*40
class FrontierLabelsR2Tests(unittest.TestCase):
    def test_reuse_old_and_local_without_querying(self):
        selected,review=select_addresses([A,B,C],{B:{}},{A})
        self.assertEqual(selected,[C]);self.assertEqual([r['reason'] for r in review],['PRIOR_FROZEN_OR_SUCCESS_REUSED','LOCAL_REGISTRY_REUSED','NEW_ACTUAL_FRONTIER'])
    def test_cumulative40_has_no_three_job_limit_and_no_reset(self):
        prior={'0x'+format(i,'040x') for i in range(39)}
        selected,review=select_addresses([A,B],{},prior)
        self.assertEqual(selected,[A]);self.assertEqual(review[-1]['reason'],'CUMULATIVE40_LABEL_GAP_UNKNOWN')
        self.assertEqual(select_addresses([B],{},prior|{A})[0],[])
    def test_depth_boundary_and_unknown_unqueried_registry_remain_eligible(self):
        c={'states':[{'state':{'address':B,'depth':5,'arrival':{'event_id':'end'}},'identity':{'status':'UNQUERIED'}}]}
        actual=unqueried_arrivals(c,5)
        self.assertEqual(actual[0][1]['address'],B)
        selected,_=select_addresses([v['address'] for _,v in actual],{B:{'identity_class':'UNKNOWN','lookup_status':'UNQUERIED'}},set())
        self.assertEqual(selected,[B]);self.assertEqual(select_addresses([B],{B:{'identity_class':'UNKNOWN','lookup_status':'COMPLETED_FOUR_TABLE_OPPORTUNITY'}},set())[0],[])
    def test_static_shape_keeps_four_complete_sources(self):
        schema=[{'table_schema':table.split('.')[0],'table_name':table.split('.')[1],'column_name':field,'data_type':'varchar'} for table,_,fields in TABLES.values() for field in fields]
        sql=optimized_sql([B,A],schema)
        self.assertEqual(sql.count('AND s.address IN ('),4);self.assertEqual(sql.count('array_agg('),4)
        self.assertNotIn('INNER JOIN frontier f ON',sql);self.assertEqual(sql.count('GROUP BY s.address'),4)
    def test_complete_empty_application_preserves_base_and_adds_unknown(self):
        base=Path(__file__).resolve().parents[1]/'checks/r2_test_tmp';base.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=base) as tmp:
            root=Path(tmp);work=root/'work';batch=work/'private/frozen_batches/labels_001';jobdir=work/'private/dune_r2_jobs/synthetic';batch.mkdir(parents=True);jobdir.mkdir(parents=True)
            registry=root/'base/address_registry.csv.gz';write_csv(registry,[{'chain_id':'1','address':A,'identity_class':'UNKNOWN','actor':''}]);original=digest(registry)
            rules={'labels_addresses_mapping':{'exact_record_rules':[]},'owner_details_cache':{},'acquisition_scope':'ACTUAL_PILOT_FRONTIER_UNIFORM_FOUR_TABLE_OPPORTUNITY'}
            dump(batch/'source_rules.json',rules);(batch/'query.sql').write_text('-- synthetic\nSELECT 1')
            freeze={'schema_version':VERSION,'batch':'labels_001','external_addresses':[B],'sql_sha256':digest(batch/'query.sql'),'base_registry':{'path':registry.relative_to(root).as_posix(),'sha256':original},'base_label_manifest':{'sha256':'b'*64},'observation_sources':[],'cumulative_unique_addresses':10,'frozen_files':[{'name':p.name,'sha256':digest(p)} for p in batch.iterdir()]}
            dump(batch/'freeze_manifest.json',freeze);(jobdir/'query.sql').write_bytes((batch/'query.sql').read_bytes())
            result={'address':B}
            for k in TABLES:result[k+'_json']='[]';result[k+'_count']=0
            execution='A'*26;md={'total_row_count':1,'row_count':1,'column_names':list(result),'total_result_set_bytes':100}
            status={'execution_id':execution,'state':'QUERY_STATE_COMPLETED','execution_cost_credits':'1','result_metadata':md}
            page={'execution_id':execution,'state':'QUERY_STATE_COMPLETED','result':{'rows':[result],'metadata':md}}
            def receipt(name,body,parameters=None):
                raw=work/'raw'/name;dump(raw,body)
                return {'http_status':200,'error_class':None,'execution_id':execution,'parameters':parameters,'utc':'2026-09-07T00:00:00Z','raw_path':raw.relative_to(work).as_posix(),'raw_bytes':raw.stat().st_size,'sha256':digest(raw)}
            sr=receipt('status.json',status);pr=receipt('page.json',page,{'limit':1000,'offset':0});dump(jobdir/'page_0.json',page);dump(jobdir/'page_0_receipt.json',pr)
            job={'kind':'frontier_labels','sql_sha256':freeze['sql_sha256'],'scope_freeze_sha256':digest(batch/'freeze_manifest.json'),'state':'QUERY_STATE_COMPLETED','execution_id':execution,'status_response':status,'status_receipt':sr,'export_offsets':[0],'export_requests':1};dump(jobdir/'job.json',job)
            report=apply(work,root,batch/'freeze_manifest.json',jobdir)
            self.assertEqual(report['source_opportunities'],4);self.assertEqual(report['new_observation_rows'],4);self.assertEqual(report['identity_changes'],0);self.assertEqual(digest(registry),original)
            saved={r['address']:r for r in rows(root/report['label_snapshot_path']/'address_registry.csv.gz')}
            self.assertEqual(saved[B]['identity_class'],'UNKNOWN');self.assertEqual(saved[B]['lookup_status'],'COMPLETED_FOUR_TABLE_OPPORTUNITY');self.assertEqual(apply(work,root,batch/'freeze_manifest.json',jobdir),report)
    def test_changed_frozen_file_refused(self):
        base=Path(__file__).resolve().parents[1]/'checks/r2_test_tmp';base.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=base) as tmp:
            p=Path(tmp);(p/'query.sql').write_text('-- original');h=digest(p/'query.sql');dump(p/'freeze_manifest.json',{'schema_version':VERSION,'external_addresses':[A],'sql_sha256':h,'frozen_files':[{'name':'query.sql','sha256':h}]});(p/'query.sql').write_text('-- changed')
            with self.assertRaises(ValueError):verify_frozen(p/'freeze_manifest.json')
if __name__=='__main__':unittest.main()
