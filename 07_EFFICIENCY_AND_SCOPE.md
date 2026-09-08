# Efficiency and scope

One warmup and five timed repetitions were saved for each sample/method, with a deterministic rotated method order. Common JSON/target preprocessing, method model construction, and solve/certification are distinguished where applicable. Baseline timings include their own compilation/propagation.

| Population | Method | Completed / all | Median query method time (seconds) |
| --- | --- | --- | --- |
| controlled | FULL_INTERVAL | 60 / 60 | 0.026234850003675092 |
| controlled | BOUNDED_REACHABILITY | 60 / 60 | 0.0003522000042721629 |
| controlled | POISON | 60 / 60 | 0.00032639999699313194 |
| controlled | HAIRCUT | 54 / 60 | 0.0005601500088232569 |
| controlled | NO_CROSS_TARGET_COUPLING | 60 / 60 | 0.019149399995512795 |
| controlled | NO_PROTOCOL_CONTINUATION | 60 / 60 | 0.026668850005080458 |
| controlled | BALANCE_INFORMATION_REMOVED | 60 / 60 | 0.025772649998543784 |
| real | FULL_INTERVAL | 2 / 2 | 0.7708113999979105 |
| real | BOUNDED_REACHABILITY | 2 / 2 | 0.004930150003929157 |
| real | POISON | 2 / 2 | 0.004969649999111425 |
| real | HAIRCUT | 2 / 2 | 0.00766624999960186 |
| real | NO_CROSS_TARGET_COUPLING | 2 / 2 | 0.7343747999912011 |
| real | NO_PROTOCOL_CONTINUATION | 2 / 2 | 0.7592576499882853 |
| real | BALANCE_INFORMATION_REMOVED | 2 / 2 | 0.7851679000086733 |

Per-query build/solve times, endpoint optimization counts, output workloads and raw repetitions are retained. Computing all certified intervals and finding a reachable set are different tasks; these times do not establish equivalent-task speed superiority. Memory is the Python allocation high-water during warmup and excludes native solver allocations. The experiment reads a local cache and does not measure online acquisition or claim network collection savings. Small timing differences are not asserted statistically significant. Actual runtime: {"numpy": "2.4.2", "platform": "Windows-11-10.0.26200-SP0", "python": "3.14.3", "scipy": "1.17.1", "system": "Windows"}.
