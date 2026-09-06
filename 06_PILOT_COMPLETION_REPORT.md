# Pilot completion

| Probe | Status | Exported rows reused/new total | Unique observed events | Candidate events | Remaining frontier |
|---|---|---:|---:|---:|---:|
| atomic_simple_transfer | PARTIAL | 18 | 15 | 11 | 1 |
| harmony_high_branch | PARTIAL | 5 | 5 | 4 | 3 |

Atomic: original seed, blocks 17395962–17427582, depth 2. Harmony: original seed, blocks 16402635–16403140, depth 5. Each arrival has its own 90-day acquisition window bounded by the frozen query end. Strict time, same-asset semantics, service stop, context separation and physical event deduplication remain. Reference neighbors never enter acquisition.

Counts describe actual saved provider observations. Reused original pages are not new network acquisition. Fully paginated top-level transactions do not prove complete internal/ERC20 Ethereum histories. Partial scope, label failures, balance and gas omissions are reported separately. Offline replay time is not the cumulative online resource clock.
