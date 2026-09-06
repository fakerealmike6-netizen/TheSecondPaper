"""Portable synthetic regressions. No API key, provider entitlement or network."""
import dataclasses
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from collector import Collector, Event, FetchResult, Limits, NATIVE, Scope, strictly_after
from provider_etherscan import EtherscanProvider, normalize_rows
from provider_receipts import ReceiptEnricher, TRANSFER_TOPIC

A, B, C, S, X = ["0x" + str(i) * 40 for i in range(1, 6)]
def event(i, sender, recipient, time, amount=1, asset=NATIVE, **kw):
    tx = "0x" + format(i, "064x")
    return Event("eip155:1:tx:" + tx + ":top", tx, sender, recipient, asset, amount, time, 0, time, **kw)

class SyntheticProvider:
    def __init__(self, events, complete=True):
        self.events, self.complete, self.requests = events, complete, []
    def fetch_interval(self, address, asset, start_block, end_block, **kw):
        self.requests.append((address, asset, start_block, end_block, kw["start_time"], kw["end_time"]))
        return FetchResult([e for e in self.events if address in (e.sender, e.recipient) and start_block <= e.block <= end_block], [{"address": address, "start_block": start_block, "end_block": end_block, "complete": self.complete}], self.complete)

def labels(address):
    return {"kind": "SERVICE", "actor": "SyntheticService", "status": "RESOLVED"} if address == S else {"kind": "UNKNOWN", "status": "NO_LABEL"}

class CollectorTests(unittest.TestCase):
    def scope(self, depth=4, end=500, window=90):
        return Scope("synthetic-query", "synthetic", 1, end, 1, end, depth, window)

    def test_service_stop_unknown_and_one_raw_unit(self):
        seed = event(1, X, A, 1)
        p = SyntheticProvider([seed, event(2, A, B, 2), event(3, B, S, 3), event(4, S, C, 4)])
        r = Collector(p, labels).run(self.scope(), seed)
        self.assertEqual(3, len(r.candidate_events))
        self.assertNotIn(S, [q[0] for q in p.requests])
        self.assertEqual(1, r.metrics["service_address_count"])

    def test_zero_external_and_self_do_not_renew(self):
        seed = event(1, X, A, 1)
        p = SyntheticProvider([seed, event(2, X, A, 80, 100), event(3, A, A, 80), event(4, A, B, 80, 0), event(5, A, S, 100)])
        r = Collector(p, labels).run(self.scope(), seed)
        self.assertEqual(1, len(r.candidate_events))
        self.assertEqual(1, len(p.requests))
        self.assertIn("SELF_TRANSFER_CONTEXT_NO_RENEWAL", {e["context_reason"] for e in r.context_events})

    def test_downstream_own_window_and_later_return(self):
        seed = event(1, X, A, 1)
        p = SyntheticProvider([seed, event(2, A, B, 80), event(3, B, A, 150), event(4, A, S, 230)])
        r = Collector(p, labels).run(self.scope(depth=4), seed)
        self.assertEqual(4, len(r.candidate_events))
        a = [q for q in p.requests if q[0] == A]
        self.assertEqual([91, 240], [q[-1] for q in a])

    def test_event_state_dedup_not_address_dedup(self):
        seed = event(1, X, A, 1)
        arrival1, arrival2, out = event(2, A, B, 2), event(3, A, B, 3), event(4, B, S, 4)
        p = SyntheticProvider([seed, arrival1, arrival1, arrival2, out, out])
        r = Collector(p, labels).run(self.scope(), seed)
        self.assertEqual(4, len(r.candidate_events))
        self.assertEqual(2, sum(q[0] == B for q in p.requests))
        self.assertEqual(1, r.metrics["service_entry_state_count"])

    def test_global_cutoff_and_depth_zero_receipt(self):
        seed = event(1, X, A, 1)
        p = SyntheticProvider([seed, event(2, A, B, 2), event(3, B, C, 3)])
        r = Collector(p, labels).run(self.scope(depth=1, end=10), seed)
        self.assertEqual([A], [q[0] for q in p.requests])
        self.assertEqual(0, r.states[0]["state"]["depth"])
        self.assertEqual(10, r.states[0]["state"]["local_end"])

    def test_reference_hidden_request_plan_identical(self):
        # Deliberately construct inconsistent answer files. Collector receives
        # neither file nor answer-derived node list; its API is seed/scope only.
        seed = event(1, X, A, 1)
        events = [seed, event(2, A, B, 2), event(3, B, C, 3)]
        with tempfile.TemporaryDirectory() as tmp:
            ref = Path(tmp) / "reference_terminal.json"
            ref.write_text(json.dumps([S]))
            p1 = SyntheticProvider(events)
            r1 = Collector(p1, labels).run(self.scope(), seed)
            ref.write_text(json.dumps([X, A, B]))
            p2 = SyntheticProvider(events)
            r2 = Collector(p2, labels).run(self.scope(), seed)
            ref.unlink()
            p3 = SyntheticProvider(events)
            r3 = Collector(p3, labels).run(self.scope(), seed)
        self.assertEqual(p1.requests, p2.requests)
        self.assertEqual(p1.requests, p3.requests)
        self.assertEqual(r1.metrics["candidate_stop_coverage_sha256"], r3.metrics["candidate_stop_coverage_sha256"])

    def test_context_only_other_asset_is_not_frontier(self):
        seed = event(1, X, A, 1)
        token = "erc20:eip155:1:" + X
        p = SyntheticProvider([seed, event(2, A, B, 2, asset=token)])
        r = Collector(p, labels).run(self.scope(), seed)
        self.assertEqual(1, len(r.candidate_events))
        self.assertEqual([A], [q[0] for q in p.requests])

    def test_order_ambiguity_and_failed_gas_preserved(self):
        seed = event(1, X, A, 1)
        arrival = dataclasses.replace(seed, kind="internal", event_id=seed.event_id + ":trace:1", trace_address="1")
        out = dataclasses.replace(seed, sender=A, recipient=S, kind="erc20", event_id=seed.event_id + ":log:2", log_index=2)
        self.assertIsNone(strictly_after(out, arrival))
        failed = dataclasses.replace(event(2, A, S, 2, 10), success=False, gas_raw=3)
        r = Collector(SyntheticProvider([failed]), labels).run(self.scope(), seed)
        self.assertEqual(1, len(r.candidate_events))
        self.assertEqual(3, r.context_events[0]["gas_raw"])

    def test_partial_interval_is_not_empty_complete(self):
        seed = event(1, X, A, 1)
        partial = Collector(SyntheticProvider([], False), labels).run(self.scope(), seed)
        complete = Collector(SyntheticProvider([], True), labels).run(self.scope(), seed)
        self.assertEqual("INCOMPLETE_PROVIDER_OR_DATA_GAP", partial.status)
        self.assertEqual("COMPLETED_WITHIN_DECLARED_SCOPE", complete.status)
        self.assertTrue(partial.unresolved_frontier)

    def test_resource_limit_retains_whole_response_and_frontier(self):
        seed = event(1, X, A, 1)
        result = Collector(SyntheticProvider([event(2, A, B, 2), event(3, A, C, 3)]), labels, Limits(max_events=2)).run(self.scope(), seed)
        self.assertEqual(3, len(result.candidate_events))
        self.assertEqual("INCOMPLETE_RESOURCE_LIMIT", result.status)
        self.assertEqual(2, len(result.unresolved_frontier))

    def test_restart_does_not_reset_online_time_allowance(self):
        seed = event(1, X, A, 1)
        scope = self.scope()
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = Path(tmp) / "checkpoint.json"
            checkpoint.write_text(json.dumps({"query_id": scope.query_id, "scope": dataclasses.asdict(scope), "cumulative_online_seconds": 5400}))
            provider = SyntheticProvider([])
            result = Collector(provider, labels, checkpoint_path=checkpoint).run(scope, seed)
            self.assertEqual([], provider.requests)
            self.assertEqual("INCOMPLETE_RESOURCE_LIMIT", result.status)
            self.assertEqual(5400, result.metrics["cumulative_online_collection_seconds"])
            provider.replay_only = True
            replay = Collector(provider, labels, checkpoint_path=checkpoint).run(scope, seed)
            self.assertEqual("COMPLETED_WITHIN_DECLARED_SCOPE", replay.status)

class EtherscanTests(unittest.TestCase):
    def row(self, i=1):
        return {"hash": "0x" + format(i, "064x"), "from": A, "to": B, "value": "1", "blockNumber": "2", "timeStamp": "2", "transactionIndex": "0", "isError": "0", "gasPrice": "2", "gasUsed": "3"}

    def fetch(self, provider):
        return provider.fetch_interval(A, NATIVE, 1, 10, start_time=1, end_time=10, global_end_time=10)

    def test_page_cache_replay_and_physical_dedup(self):
        def transport(params):
            rows = [self.row()] if params["action"] == "txlist" and params["page"] == 1 else []
            return {"status": "1", "message": "OK", "result": rows}
        with tempfile.TemporaryDirectory() as tmp:
            cold = self.fetch(EtherscanProvider(transport, tmp, page_size=1))
            def forbidden(_):
                self.fail("cache replay attempted network")
            warm = self.fetch(EtherscanProvider(forbidden, tmp, page_size=1, replay_only=True))
        self.assertEqual(4, cold.real_requests)
        self.assertEqual(0, warm.real_requests)
        self.assertEqual(cold.events, warm.events)
        self.assertTrue(warm.complete)

    def test_provider_rejection_not_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            failed = self.fetch(EtherscanProvider(lambda _: {"status": "0", "result": "NOTOK"}, tmp))
        self.assertFalse(failed.complete)
        self.assertTrue(failed.gaps)

    def test_empty_page_success_and_token_locator_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            empty = self.fetch(EtherscanProvider(lambda _: {"status": "0", "message": "No transactions found", "result": []}, tmp))
        self.assertTrue(empty.complete)
        row = self.row() | {"contractAddress": X}
        events, gaps = normalize_rows("tokentx", [row])
        self.assertEqual([], events)
        self.assertEqual("EVENT_IDENTITY_UNRESOLVED", gaps[0]["reason"])
        events, gaps = normalize_rows("tokentx", [row | {"logIndex": "7"}])
        self.assertEqual(7, events[0].log_index)
        self.assertTrue(events[0].event_id.endswith(":log:7"))

    def test_local_window_needs_safe_block_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            provider = EtherscanProvider(lambda _: self.fail("outside-window request"), tmp)
            result = provider.fetch_interval(A, NATIVE, 1, 500, start_time=1, end_time=91, global_end_time=500)
        self.assertFalse(result.complete)
        self.assertEqual("LOCAL_WINDOW_BLOCK_BOUND_UNRESOLVED", result.gaps[0]["reason"])

    def test_pagination_repeat_is_explicit_gap(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = self.fetch(EtherscanProvider(lambda _: {"status": "1", "result": [self.row()]}, tmp, page_size=1))
        self.assertFalse(result.complete)
        self.assertIn("PAGINATION_REPEATED_PAGE", {g["reason"] for g in result.gaps})

    def test_receipt_enrichment_keeps_identical_distinct_transfers(self):
        row = self.row() | {"contractAddress": X}
        topics = [TRANSFER_TOPIC, "0x" + "0" * 24 + A[2:], "0x" + "0" * 24 + B[2:]]
        logs = [{"address": X, "topics": topics, "data": "0x" + format(1, "064x"), "logIndex": hex(i)} for i in (7, 8)]
        receipt = {"transactionHash": row["hash"], "transactionIndex": "0x4", "blockNumber": "0x2", "status": "0x1", "logs": logs}
        enricher = ReceiptEnricher(lambda tx: (receipt, {"real_requests": 1}))
        enriched, gaps, counts = enricher("tokentx", [row, row])
        events, norm_gaps = normalize_rows("tokentx", enriched)
        self.assertEqual([], gaps + norm_gaps)
        self.assertEqual([7, 8], [e.log_index for e in events])
        self.assertEqual(1, counts["real_requests"])
        self.assertEqual(2, len({e.event_id for e in events}))

    def test_receipt_mismatch_does_not_create_index_event(self):
        row = self.row() | {"contractAddress": X}
        receipt = {"transactionHash": row["hash"], "transactionIndex": "0x0", "blockNumber": "0x2", "status": "0x1", "logs": []}
        enriched, gaps, _ = ReceiptEnricher(lambda tx: (receipt, {}))("tokentx", [row])
        self.assertEqual([], enriched)
        self.assertEqual("INDEX_RECEIPT_TRANSFER_MULTIPLICITY_MISMATCH", gaps[0]["reason"])

if __name__ == "__main__":
    unittest.main()
