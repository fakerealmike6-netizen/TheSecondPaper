from pathlib import Path
from copy import deepcopy
import random
import sys
import unittest

STAGE=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(STAGE/'src')]
import stage1d_transfers_acquisition as a


class NeedOverlapTests(unittest.TestCase):
    def row(self,bs,be,ts,te,**kwargs):
        return {'query_id':'qry:synthetic','scope_id':'scope:synthetic','scope_hash':'f'*64,
            'address':'0x'+'1'*40,'asset':a.NATIVE,'direction':'OUTGOING','reason':'SYNTHETIC',
            'start_block':bs,'end_block':be,'start_time':ts,'end_time':te,**kwargs}

    def merge(self,*rows):return [row for _,row in a._merge_overlapping_needs([((i,),row) for i,row in enumerate(rows)])]

    def points(self,rows):
        return {(b,t) for row in rows for b in range(row['start_block'],row['end_block']+1)
                for t in range(row['start_time'],row['end_time']+1)}

    def test_contained_rectangle_merged(self):
        outer=self.row(1,8,10,20);inner=self.row(3,6,12,15)
        self.assertEqual(self.merge(inner,outer),[outer])

    def test_same_block_axis_overlapping_time_union(self):
        self.assertEqual(self.merge(self.row(1,5,3,6),self.row(1,5,5,9)),[self.row(1,5,3,9)])

    def test_same_time_axis_overlapping_block_union(self):
        self.assertEqual(self.merge(self.row(1,5,3,6),self.row(4,9,3,6)),[self.row(1,9,3,6)])

    def test_crossing_rectangles_never_expand_to_bounding_box(self):
        rows=[self.row(1,4,3,7),self.row(3,7,1,4)]
        self.assertEqual(self.merge(*rows),rows)

    def test_only_touching_by_discrete_adjacency_is_not_overlap(self):
        rows=[self.row(1,4,3,7),self.row(1,4,8,10)]
        self.assertEqual(self.merge(*rows),rows)

    def test_one_shared_boundary_is_real_overlap(self):
        self.assertEqual(self.merge(self.row(1,4,3,7),self.row(1,4,7,10)),[self.row(1,4,3,10)])

    def test_other_scope_asset_address_or_metadata_never_merge(self):
        first=self.row(1,8,1,8)
        for key,value in [('scope_id','other'),('asset','token:synthetic'),('address','0x'+'2'*40),
                          ('direction','INCOMING'),('reason','DIFFERENT_AUTHORITY')]:
            second=self.row(2,4,2,4,**{key:value})
            self.assertEqual(self.merge(first,second),[first,second])

    def test_transitive_union_rechecks_earlier_rectangle(self):
        rows=[self.row(1,2,1,5),self.row(4,6,1,5),self.row(2,4,1,5)]
        self.assertEqual(self.merge(*rows),[self.row(1,6,1,5)])

    def test_inputs_are_unchanged(self):
        rows=[self.row(1,8,1,8),self.row(2,4,2,4)];before=deepcopy(rows)
        self.merge(*rows);self.assertEqual(rows,before)

    def test_earliest_original_frontier_rank_retained(self):
        ranked=[((7,),self.row(1,8,1,8)),((2,),self.row(2,4,2,4)),((3,),self.row(1,2,1,2,address='0x'+'2'*40))]
        result=a._merge_overlapping_needs(ranked)
        self.assertEqual([rank for rank,_ in result],[(2,),(3,)])

    def test_exhaustive_small_grid_union_preserved_for_random_inputs(self):
        rng=random.Random(8017)
        for _ in range(250):
            rows=[]
            for _ in range(7):
                bs,be=sorted([rng.randrange(7),rng.randrange(7)])
                ts,te=sorted([rng.randrange(7),rng.randrange(7)])
                rows.append(self.row(bs,be,ts,te))
            self.assertEqual(self.points(rows),self.points(self.merge(*rows)))

    def test_merge_precedes_twenty_request_limit(self):
        query={'query_id':'qry:synthetic:merge','name':'txphish_src001','start_block':1,'end_block':100,
            'start_time_utc':'1970-01-01T00:16:40Z','end_time_utc':'1970-01-01T00:33:20Z',
            'max_acquisition_depth':13,'window_mode':'REFERENCE_FULL'}
        scope=a.Scope.from_policy(query)
        def need(address,start):
            return {'query_id':query['query_id'],'query_name':query['name'],'scope_id':scope.scope_id,'scope_hash':scope.scope_hash,
                'address':address,'asset':a.NATIVE,'direction':'OUTGOING','start_block':start,'end_block':100,
                'start_time':1000+start,'end_time':2000,'fact_type':'POSITIVE_NATIVE_CANDIDATE_INDEX','gap_reason':'SYNTHETIC'}
        first,second='0x'+'1'*40,'0x'+'2'*40
        needs=[need(first,2),need(first,3),need(second,4)]
        frontier=[{'reason':'INTERVAL_INCOMPLETE','state':{'address':address,'asset':a.NATIVE,'protocol_context':'ordinary',
            'depth':1,'arrival':{'block':1,'timestamp':1000,'tx_index':0,'event_id':'synthetic:'+address}}} for address in (first,second)]
        result=a.select_needs(query,{'needed_ranges':needs,'frontier':frontier},maximum=2)
        self.assertEqual(result,[needs[0],needs[2]])


if __name__=='__main__':unittest.main(verbosity=2)
