"""Synthetic-only recovery adapter checks; no network and no active DB writes."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest

STAGED = Path(__file__).resolve().parent
SRC = STAGED.parent / "src"
sys.path.insert(0,str(SRC))
sys.path.insert(0,str(STAGED))

from collector import Event,NATIVE,Scope
from stage1d_runtime import Runtime
from stage1d_alchemy_transfers import (METHOD,TransferPageChain,TransfersRuntime,
    bind_need_to_scope,compare_canary,fetch_next_page,normalize_chain,permission_with_transfer_rate,
    request_plan,route_gate,transfers_result_status,validate_transfers_rpc)

TX="0x"+"a"*64
A="0x"+"1"*40
B="0x"+"2"*40
BH="0x"+"b"*64
T=1700000000
VALUE=2**127+1


def need():
    return dict(query_id="qry:synthetic",query_name="synthetic",scope_id="scope:synthetic",
        scope_hash="c"*64,address=A,asset=NATIVE,direction="OUTGOING",start_block=100,end_block=102,
        start_time=T,end_time=T+24,fact_type="POSITIVE_NATIVE_CANDIDATE_INDEX",gap_reason="SYNTHETIC_TEST")


def row(category="external"):
    return dict(blockNum="0x64",uniqueId=TX+":"+category,hash=TX,**{"from":A,"to":B},
        value=0.000000000000000001,asset="ETH",category=category,
        rawContract=dict(value=hex(VALUE),address=None,decimal="0x12"),
        metadata={"blockTimestamp":datetime.fromtimestamp(T,timezone.utc).isoformat()})


def member(rows, key=None, cache=False):
    result={"transfers":deepcopy(rows)}
    if key is not None:result["pageKey"]=key
    return dict(status="SUCCESS_VALIDATED",result=result,artifact_path="synthetic/envelope.json",
                artifact_sha256="d"*64,cache_hit=cache)


def chain(rows=None):
    result=TransferPageChain(need(),page_size=2)
    result.append(result.next_plan(0),member([row()] if rows is None else rows),received_at_seconds=0)
    return result


def facts():
    tx=dict(hash=TX,blockNumber="0x64",blockHash=BH,transactionIndex="0x0",value=hex(VALUE),
            **{"from":A,"to":B})
    receipt=dict(transactionHash=TX,blockNumber="0x64",blockHash=BH,transactionIndex="0x0",
                 status="0x1",gasUsed="0x5208",effectiveGasPrice="0x3b9aca00")
    header=dict(number="0x64",hash=BH,timestamp=hex(T),transactions=[TX])
    return dict(transactions={TX:tx},receipts={TX:receipt},headers={100:header})


def proof():
    return dict(complete=True,source="CURRENT_VERIFIED_DUNE_RPC",source_hashes_verified=True,
        needed_range=need(),evidence_refs=[{"path":"synthetic/current_complete.json","sha256":"d"*64}])


def internal(position="0"):
    return Event("eip155:1:tx:"+TX+":trace:"+position,TX,A,B,NATIVE,VALUE,100,0,T,"internal",
                 trace_address=position,provenance="SYNTHETIC_STRICT_TRACE")


class TransfersTests(unittest.TestCase):
    def test_zero_padded_raw_hex_is_exact_data_but_quantity_stays_strict(self):
        raw = row()
        raw['rawContract']['value'] = '0x00' + format(VALUE, 'x')
        normalized = normalize_chain(chain([raw]), **facts())
        self.assertFalse(normalized['gaps'])
        self.assertEqual(normalized['events'][0]['amount_raw'], VALUE)
        raw['rawContract']['value'] = 1.0
        with self.assertRaises(ValueError):
            chain([raw])
        raw = row()
        raw['blockNum'] = '0x064'
        with self.assertRaises(ValueError):
            chain([raw])

    def test_explicit_need_scope_binding(self):
        scope=Scope("qry:synthetic","synthetic",100,102,T,T+24,1,local_window_seconds=None,window_mode="REFERENCE_FULL")
        n=need();n.update(scope_id=scope.scope_id,scope_hash=scope.scope_hash)
        self.assertEqual(bind_need_to_scope(n,scope),n)
        n["end_block"]=103
        with self.assertRaises(ValueError):bind_need_to_scope(n,scope)

    def test_fixed_request_and_incoming_rejected(self):
        p=request_plan(need())
        self.assertEqual(p["params"][0]["maxCount"],"0x3e8")
        self.assertEqual(validate_transfers_rpc(p),p)
        wrong=need();wrong["direction"]="INCOMING"
        with self.assertRaises(ValueError):request_plan(wrong)

    def test_latest_and_unapproved_categories_rejected(self):
        p=request_plan(need());p["params"][0]["toBlock"]="latest"
        with self.assertRaises(ValueError):validate_transfers_rpc(p)
        p=request_plan(need());p["params"][0]["category"]=["external"]
        with self.assertRaises(ValueError):validate_transfers_rpc(p)

    def test_exact_wei_ignores_float_and_keeps_gas(self):
        out=normalize_chain(chain(),**facts())
        self.assertEqual(out["gaps"],[])
        self.assertEqual(out["events"][0]["amount_raw"],VALUE)
        self.assertEqual(out["events"][0]["gas_raw"],21000*10**9)
        self.assertFalse(out["context_complete"])

    def test_external_missing_raw_uses_exact_tx_value(self):
        r=row();r["rawContract"]["value"]=None
        out=normalize_chain(chain([r]),**facts())
        self.assertEqual(out["events"][0]["amount_raw"],VALUE)

    def test_raw_integer_contradiction_is_gap(self):
        f=facts();f["transactions"][TX]["value"]=hex(VALUE+1)
        out=normalize_chain(chain(),**f)
        self.assertFalse(out["positive_native_index_complete"])
        self.assertEqual(out["events"],[])

    def test_failed_or_missing_receipt_not_zero(self):
        f=facts();f["receipts"][TX]["status"]="0x0"
        out=normalize_chain(chain(),**f)
        self.assertEqual(out["events"],[])
        f=facts();f["receipts"]={}
        self.assertEqual(len(normalize_chain(chain(),**f)["gaps"]),1)

    def test_header_order_binding_required(self):
        f=facts();f["headers"][100]["transactions"]=["0x"+"e"*64]
        self.assertEqual(normalize_chain(chain(),**f)["events"],[])

    def test_new_internal_does_not_guess_unique_id_position(self):
        r=row("internal");r["uniqueId"]=TX+":internal:0_1"
        out=normalize_chain(chain([r]),**facts())
        self.assertEqual(out["events"],[])
        self.assertEqual(out["gaps"][0]["reason"],"TRACE_POSITION_OR_ANCESTRY_UNRESOLVED")

    def test_internal_matches_unique_already_complete_trace(self):
        r=row("internal");r["rawContract"]["value"]=None
        out=normalize_chain(chain([r]),**facts(),strict_internal_events=[internal()],complete_trace_transactions=[TX])
        self.assertEqual(out["gaps"],[])
        self.assertEqual(out["events"][0]["event_id"],internal().event_id)
        self.assertEqual(out["events"][0]["amount_raw"],VALUE)

    def test_multiple_same_amount_traces_never_pick_one(self):
        out=normalize_chain(chain([row("internal")]),**facts(),
            strict_internal_events=[internal("0"),internal("1")],complete_trace_transactions=[TX])
        self.assertEqual(out["events"],[])
        self.assertEqual(len(out["gaps"]),1)

    def test_contradictory_existing_trace_is_not_last_wins(self):
        from dataclasses import replace
        with self.assertRaisesRegex(ValueError,"Conflicting supplied"):
            normalize_chain(chain([row("internal")]),**facts(),
                strict_internal_events=[internal(),replace(internal(),amount_raw=VALUE+1)],
                complete_trace_transactions=[TX])

    def test_pending_page_never_complete(self):
        c=TransferPageChain(need());c.append(c.next_plan(0),member([row()],"next"),received_at_seconds=0)
        out=normalize_chain(c,**facts())
        self.assertFalse(out["positive_native_index_complete"])
        self.assertEqual(out["gaps"][-1]["reason"],"TRANSFERS_PAGE_CHAIN_NOT_EXHAUSTED")

    def test_expired_cursor_restarts_last_touched_block(self):
        c=TransferPageChain(need());c.append(c.next_plan(0),member([row()],"next"),received_at_seconds=0)
        with self.assertRaisesRegex(ValueError,"EXPIRED"):c.next_plan(600)
        recovery=c.recovery_range()
        self.assertEqual(recovery["needed_range"]["start_block"],100)
        self.assertEqual(recovery["needed_range"]["end_block"],102)
        self.assertEqual(recovery["needed_range"]["end_time"],T+24)

    def test_cached_cursor_age_is_not_refreshed_by_read(self):
        c=TransferPageChain(need());c.append(c.next_plan(900),member([row()],"next",True),received_at_seconds=900)
        with self.assertRaisesRegex(ValueError,"AGE_UNKNOWN"):c.next_plan(901)

    def test_duplicate_boundary_retains_one_physical_event(self):
        c=TransferPageChain(need(),page_size=1)
        c.append(c.next_plan(0),member([row()],"next"),received_at_seconds=0)
        c.append(c.next_plan(1),member([row()]),received_at_seconds=1,requested_at_seconds=1)
        out=normalize_chain(c,**facts())
        self.assertEqual(c.duplicates,1)
        self.assertEqual(len(out["events"]),1)
        self.assertTrue(out["positive_native_index_complete"])
        check=compare_canary("pagination_or_boundary_duplicate",need(),out,out["events"],proof())
        self.assertEqual(check["status"],"PASS")

    def test_conflicting_duplicate_does_not_advance_page(self):
        c=TransferPageChain(need(),page_size=1)
        c.append(c.next_plan(0),member([row()],"next"),received_at_seconds=0)
        bad=row();bad["rawContract"]["value"]=hex(VALUE+1)
        with self.assertRaisesRegex(ValueError,"Contradictory"):
            c.append(c.next_plan(1),member([bad]),received_at_seconds=1,requested_at_seconds=1)
        self.assertEqual(len(c.pages),1)
        self.assertFalse(c.closed)

    def test_wrong_rpc_id_rejected(self):
        request={"id":"a","jsonrpc":"2.0",**request_plan(need())}
        response={"id":"b","jsonrpc":"2.0","result":{"transfers":[]}}
        self.assertEqual(transfers_result_status(request,response),"INVALID_RPC_BINDING")

    def test_canary_requires_current_complete_evidence(self):
        out=normalize_chain(chain(),**facts());p=proof();p["complete"]=False
        with self.assertRaises(ValueError):compare_canary("ordinary_top",need(),out,out["events"],p)
        self.assertEqual(compare_canary("ordinary_top",need(),out,out["events"],proof())["status"],"PASS")

    def test_empty_and_internal_canary_semantics(self):
        out=normalize_chain(chain([]),**facts())
        self.assertEqual(compare_canary("empty",need(),out,[],proof())["status"],"PASS")
        with self.assertRaises(ValueError):compare_canary("internal",need(),out,[],proof())

    def test_route_gate_requires_four_and_keeps_internal_fallback(self):
        with self.assertRaises(ValueError):route_gate([])
        rows=[{"canary_kind":k,"status":"PASS"} for k in
              ["ordinary_top","internal","empty","pagination_or_boundary_duplicate"]]
        rows[1]["status"]="MISMATCH_FALLBACK"
        gate=route_gate(rows)
        self.assertEqual(gate["external_route"],"ALCHEMY_TRANSFERS")
        self.assertEqual(gate["internal_route"],"CLASSIC_BIGQUERY_OR_VALIDATED_DUNE_NATIVE")
        self.assertFalse(gate["context_complete"])

    def test_old_rpc_identity_and_validation_unchanged(self):
        p={"method":"eth_getBalance","params":[A,"0x64"]}
        old,new=Runtime(),TransfersRuntime()
        self.assertEqual(old.validate_rpc(p),new.validate_rpc(p))
        self.assertEqual(old.rpc_identity("provider",p),new.rpc_identity("provider",p))

    def test_rate_is_added_to_copy_without_reset(self):
        p={"method_cu_upper_bounds":{"eth_getBalance":20},"included_compute_units_remaining":1000000}
        before=deepcopy(p);new=permission_with_transfer_rate(p)
        self.assertEqual(p,before)
        self.assertEqual(new["method_cu_upper_bounds"][METHOD],120)
        self.assertEqual(new["included_compute_units_remaining"],1000000)

    def test_root_hook_one_member_no_old_capability_pool(self):
        c=TransferPageChain(need())
        class FakeAccess:
            def call_batch(self,plans,query,label,*,capability,deadline):
                self.plans=plans;self.capability=capability
                return {"members":[member([])]}
        access=FakeAccess();fetch_next_page(access,c,clock=lambda:0,label="synthetic")
        self.assertEqual(len(access.plans),1)
        self.assertFalse(access.capability)
        self.assertTrue(c.closed)


if __name__=="__main__":
    unittest.main()
