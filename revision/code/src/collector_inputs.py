"""Read only the exact authorized seed rows; never expose reference neighbors."""
import csv
import gzip
import hashlib
from pathlib import Path
from collector import Event, utc_seconds

def load_exact_seed(pilot, query_members_path, event_slice_path):
    query_members_path, event_slice_path = Path(query_members_path), Path(event_slice_path)
    with query_members_path.open(encoding="utf-8-sig", newline="") as handle:
        members = [r for r in csv.DictReader(handle) if r["query_id"] == pilot["query_id"]]
    if len(members) != 1 or members[0]["seed_rule"] != "S2" or members[0]["seed_event_id"] != pilot["seed_event_id"] or members[0]["seed_match_status"] != "EXACT_EVENT_MATCH":
        raise ValueError("authorized S2 exact seed membership mismatch")
    opener = gzip.open if event_slice_path.suffix == ".gz" else open
    with opener(event_slice_path, "rt", encoding="utf-8-sig", newline="") as handle:
        rows = [r for r in csv.DictReader(handle) if r["event_id"] == pilot["seed_event_id"]]
    if len(rows) != 1:
        raise ValueError("exact seed event missing or duplicated")
    row, member = rows[0], members[0]
    for left, right in (("amount_raw", "seed_amount_raw"), ("asset_key", "seed_asset"), ("from_address", "seed_from"), ("to_address", "seed_to"), ("block_timestamp", "seed_time"), ("tx_hash", "seed_tx_hash")):
        if row[left].lower() != member[right].lower():
            raise ValueError("seed member / canonical event inconsistency: " + left)
    if row["transaction_status"] != "SUCCESS" or row["chain_id"] != "1":
        raise ValueError("successful Ethereum mainnet seed required")
    kind = {"ETH_TOP_LEVEL": "top", "ETH_INTERNAL": "internal", "ERC20_TRANSFER": "erc20"}.get(row["event_type"])
    if kind is None:
        raise ValueError("unsupported seed kind")
    seed = Event(row["event_id"], row["tx_hash"], row["from_address"], row["to_address"], row["asset_key"], int(row["amount_raw"]), int(row["block_number"]), int(row["transaction_index"]) if row["transaction_index"] else None, utc_seconds(row["block_timestamp"]), kind, int(row["log_index"]) if row["log_index"] else None, row["trace_address"] or None, provenance="STAGE1A_R1_EXACT_SEED")
    manifest = {"query_id": pilot["query_id"], "seed_event_id": seed.event_id, "query_members_sha256": hashlib.sha256(query_members_path.read_bytes()).hexdigest(), "event_slice_sha256": hashlib.sha256(event_slice_path.read_bytes()).hexdigest(), "seed_raw_evidence_sha256": row["raw_evidence_sha256"], "input_filter": "exact query + seed event only; reference neighbor/target data not passed to collector"}
    return seed, manifest
