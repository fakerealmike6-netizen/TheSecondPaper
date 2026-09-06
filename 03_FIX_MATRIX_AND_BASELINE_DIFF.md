# Repairs and comparison

| Defect | Reproduction | Repaired behavior | Status |
|---|---|---|---|
| R2-02 | Same physical event changes destination/value across jobs | Shared fact registry quarantines all versions; invalidates candidates/stops/coverage/LP; compatible duplicates merge provenance | COMPLETED |
| R2-01 | Timeout/restart invokes same export page twice | SQLite dispatch intent precedes callback, crash/race durable; unknown page blocks reuse and advancement | COMPLETED |
| R2-03 | Abnormal HTTP 200 page accepted until subsequent call | Execution/schema/count/cursor/URI contract validated before any next paid request | COMPLETED |
| R2-04 | Contradictory tx/block/log/source accepted as WETH | Cross-source request/response binding; trusted real acquisition catalog; synthetic remains synthetic | COMPLETED |

Old same-input four graphs retained candidate, first-service stop, coverage, target and amount semantics. Paths and provenance can change; physical fact contradictions cannot be relaxed away. The independent fixed-graph checker verified 23 intervals and 46 allocation witnesses, without importing the LP or its Oracle.

Public fault fixtures and tests reproduce repair boundaries. Private original defect outputs are preserved in the local review evidence; reproducing a defect is not a passing repair test.
