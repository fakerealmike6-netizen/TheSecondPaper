"""Optional unexecuted SQL-only optimization for the two authorized probes.

This file does not replace the active provider, call a service, or change any
result predicate. Dune documents block_date on both raw Ethereum tables.
Explicit date partition predicates are added alongside exact timestamp/block
predicates. Decode-table columns and original ancestor checks are unchanged.
"""
from provider_dune import build_interval_sql, timestamp, sql_time, integer

SCOPES = (
    (17395962, 17427582, "2023-06-02T22:20:11Z", "2023-06-07T09:23:11Z"),
    (16402635, 16403140, "2023-01-14T04:24:35Z", "2023-01-14T06:06:11Z"),
)

def build_partitioned_interval_sql(address, asset, start_block, end_block, *, start_time, end_time):
    start_block, end_block = integer(start_block), integer(end_block)
    if not any(a <= start_block <= end_block <= b and timestamp(c) <= timestamp(start_time) <= timestamp(end_time) <= timestamp(d) for a, b, c, d in SCOPES):
        raise ValueError("Outside either currently authorized fixed probe")
    sql = build_interval_sql(address, asset, start_block, end_block, start_time=start_time, end_time=end_time)
    dates = f"block_date BETWEEN DATE '{sql_time(start_time)[:10]}' AND DATE '{sql_time(end_time)[:10]}' AND "
    for table in ("ethereum.transactions", "ethereum.traces"):
        original = "FROM " + table + " WHERE "
        if sql.count(original) != 1:
            raise ValueError("Upstream SQL shape changed; review candidate again")
        sql = sql.replace(original, original + dates, 1)
    return sql

REVIEW = {
    "status": "STATIC_REVIEW_ONLY_NOT_DUNE_COMPILE_OR_COST_ESTIMATE",
    "schema_sources": [
        "https://docs.dune.com/data-catalog/evm/ethereum/raw/transactions",
        "https://docs.dune.com/data-catalog/evm/ethereum/raw/traces",
        "https://docs.dune.com/query-engine/writing-efficient-queries",
    ],
    "reason": "Raw tables document block_date; explicit dates may improve partition pruning without relaxing exact time or block constraints.",
    "active_provider_modified": False,
    "sql_executed": False,
    "reference_whitelist_added": False,
    "limit_or_sampling_added": False,
    "cost_claim": "No static credit estimate or completion guarantee. Actual cap and combined export budget remain required.",
}
