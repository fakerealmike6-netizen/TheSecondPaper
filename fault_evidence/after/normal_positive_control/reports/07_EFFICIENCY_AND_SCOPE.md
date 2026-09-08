# Efficiency and scope

One warmup and five timed repetitions were saved for each sample/method, with a deterministic rotated method order. Common JSON/target preprocessing, method model construction, and solve/certification are distinguished where applicable. Baseline timings include their own compilation/propagation.

| Population | Method | Completed / all | Median query method time (seconds) |
| --- | --- | --- | --- |
| controlled | FULL_INTERVAL | 1 / 1 | 0.0035069999867118895 |
| controlled | BOUNDED_REACHABILITY | 1 / 1 | 9.380000119563192e-05 |
| controlled | POISON | 1 / 1 | 9.459999273531139e-05 |
| controlled | HAIRCUT | 1 / 1 | 0.000145500001963228 |
| controlled | NO_CROSS_TARGET_COUPLING | 1 / 1 | 0.00331729999743402 |
| controlled | NO_PROTOCOL_CONTINUATION | 1 / 1 | 0.0034945999941555783 |
| controlled | BALANCE_INFORMATION_REMOVED | 1 / 1 | 0.003824600004008971 |
| real | FULL_INTERVAL | 0 / 0 | None |
| real | BOUNDED_REACHABILITY | 0 / 0 | None |
| real | POISON | 0 / 0 | None |
| real | HAIRCUT | 0 / 0 | None |
| real | NO_CROSS_TARGET_COUPLING | 0 / 0 | None |
| real | NO_PROTOCOL_CONTINUATION | 0 / 0 | None |
| real | BALANCE_INFORMATION_REMOVED | 0 / 0 | None |

Per-query build/solve times, endpoint optimization counts, output workloads and raw repetitions are retained. Computing all certified intervals and finding a reachable set are different tasks; these times do not establish equivalent-task speed superiority. Memory is the Python allocation high-water during warmup and excludes native solver allocations. The experiment reads a local cache and does not measure online acquisition or claim network collection savings. Small timing differences are not asserted statistically significant. Actual runtime: {"numpy": "2.4.2", "platform": "Windows-11-10.0.26200-SP0", "python": "3.14.3", "scipy": "1.17.1", "system": "Windows"}.
