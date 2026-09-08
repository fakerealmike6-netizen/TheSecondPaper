# Efficiency and scope

One warmup and five timed repetitions were saved for each sample/method, with a deterministic rotated method order. Common JSON/target preprocessing, method model construction, and solve/certification are distinguished where applicable. Baseline timings include their own compilation/propagation.

| Population | Method | Completed / all | Median query method time (seconds) |
| --- | --- | --- | --- |
| controlled | FULL_INTERVAL | 60 / 60 | 0.01124704999892856 |
| controlled | BOUNDED_REACHABILITY | 60 / 60 | 0.00011945000005653128 |
| controlled | POISON | 60 / 60 | 0.00013155000124243088 |
| controlled | HAIRCUT | 54 / 60 | 0.00021855000159121118 |
| controlled | NO_CROSS_TARGET_COUPLING | 60 / 60 | 0.008190050004486693 |
| controlled | NO_PROTOCOL_CONTINUATION | 60 / 60 | 0.01133395000215387 |
| controlled | BALANCE_INFORMATION_REMOVED | 60 / 60 | 0.011644449998129858 |
| real | FULL_INTERVAL | 2 / 2 | 0.37817535000067437 |
| real | BOUNDED_REACHABILITY | 2 / 2 | 0.0020870999978797045 |
| real | POISON | 2 / 2 | 0.00190775000010035 |
| real | HAIRCUT | 2 / 2 | 0.0032586000015726313 |
| real | NO_CROSS_TARGET_COUPLING | 2 / 2 | 0.34278824999637436 |
| real | NO_PROTOCOL_CONTINUATION | 2 / 2 | 0.35871189999670605 |
| real | BALANCE_INFORMATION_REMOVED | 2 / 2 | 0.38261805000001914 |

Per-query build/solve times, endpoint optimization counts, output workloads and raw repetitions are retained. Computing all certified intervals and finding a reachable set are different tasks; these times do not establish equivalent-task speed superiority. Memory is the Python allocation high-water during warmup and excludes native solver allocations. The experiment reads a local cache and does not measure online acquisition or claim network collection savings. Small timing differences are not asserted statistically significant. Actual runtime: {"numpy": "2.4.2", "platform": "Windows-11-10.0.26200-SP0", "python": "3.14.3", "scipy": "1.17.1", "system": "Windows"}.
