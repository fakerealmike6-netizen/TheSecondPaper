"""Offline synthetic replay: python src/collector_replay.py [fixture] [output]."""
import json
import sys
from pathlib import Path
from collector import Collector, Event, FetchResult, Scope

class FixtureProvider:
    def __init__(self, events):
        self.events = events
        self.requests = []

    def fetch_interval(self, address, asset, start_block, end_block, **times):
        request = {"address": address, "asset": asset, "start_block": start_block, "end_block": end_block, **times}
        self.requests.append(request)
        selected = [e for e in self.events if address in (e.sender, e.recipient) and start_block <= e.block <= end_block]
        return FetchResult(selected, [request | {"complete": True, "basis": "synthetic full fixture"}], True)

def main():
    root = Path(__file__).resolve().parents[1]
    fixture = Path(sys.argv[1]) if len(sys.argv) > 1 else root / "fixtures" / "collector" / "ordinary_graph.json"
    output = Path(sys.argv[2]) if len(sys.argv) > 2 else root / "derived" / "collector_synthetic_replay.json"
    data = json.loads(fixture.read_text(encoding="utf-8"))
    events = [Event(**e) for e in data["events"]]
    seed = next(e for e in events if e.event_id == data["seed_event_id"])
    provider = FixtureProvider(events)
    result = Collector(provider, lambda a: data["labels"].get(a, {"kind": "UNKNOWN", "status": "NO_LABEL"})).run(Scope(**data["scope"]), seed)
    digest = result.write(output)
    print(json.dumps({"status": result.status, "candidate_events": len(result.candidate_events), "synthetic_requests": len(provider.requests), "sha256": digest, "network_requests": 0}))

if __name__ == "__main__":
    main()
