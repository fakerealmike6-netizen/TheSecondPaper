# Amount replay

The continuous time-expanded LP passed 12 controlled scenarios and 32 independent Oracle comparisons, including joint capacity, missing-balance feasible-set inclusion, strict ordering, native gas separation and one raw unit. Synthetic graph labels are isolated from real evidence.

Final observed graph intervals (raw integers):

```json
[
  {
    "probe": "atomic_simple_transfer",
    "target": "OBSERVED_SERVICE_1",
    "lower_raw": "0",
    "upper_raw": "287395000000000000000",
    "asset": "ETH",
    "scope": "ASSUMPTION_CONDITIONAL",
    "graph_sha256": "ac6977d3b755cf5bb49d46f929c588df98093569bce77d09ec2b6d9e764b15c6"
  },
  {
    "probe": "harmony_high_branch",
    "target": null,
    "status": "NO_OBSERVED_TARGET",
    "scope": "ASSUMPTION_CONDITIONAL",
    "graph_sha256": "12e4cb9cbbd07d45f892c7c5d42b25f1ce91547dccc73bfb63105f367f312762"
  }
]
```

ASSUMPTION_CONDITIONAL means missing initial balances/gas/flows and incomplete acquisition remain assumptions. It is not a guaranteed full-chain bound. NO_OBSERVED_TARGET is not a zero amount. Source allocation witnesses are included locally; they are not claimed as general independent KKT certificates.
