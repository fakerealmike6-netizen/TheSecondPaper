# Stage1C-R1 review

Internal repair complete; final ZIP execution/publication recorded externally. External acceptance: **PENDING_REVIEW**. Stop: **CHECKPOINT_1C_R1_REACHED**.

Start at 00_REVIEW_INDEX.md. This revision repairs C1-V01 acceptance, status propagation, reports and extracted-package validation. Scientific methods, the original60 controlled observations/hidden assignments, two developed real ETH contexts, labels and reference denominators are unchanged.

Windows Python3.14.3 / NumPy2.4.2 / SciPy1.17.1 actually tested. Use an existing environment; no real credentials or dependency downloads are required on that host. Choose the command matching the package and a fresh output directory:

```text
python -B src/validate_stage1c.py --tree . --kind public --output ../r1_public_check
python -B src/validate_stage1c.py --tree . --kind min --output ../r1_min_check
```

The validator uses the inherited strict offline guard, removes credential environment variables, blocks sockets/DNS and unguarded children, then runs950 tests, original12 scenarios/32 comparisons,60 controlled experiments and MIN-only two real experiments/evidence replay. Saved results and regenerated results each pass the common contract and scientific checks before equality comparison. The root RESULTS_INDEX is navigation; executable batch indices are in results/controlled and results/real.

```text
python -B src/run_stage1c.py --tree . --kind public --selection controlled --output ../r1_controlled
python -B src/stage1c_reports.py --tree . --results results/controlled --output ../r1_controlled_report
python -B src/stage1c_result_gate.py --tree . --results results/controlled --output ../r1_saved_gate.json
python -B tests/test_stage1c_r1_faults.py --fault-child --tree . --kind public --case full_missing_output --output ../r1_expected_failure
```

The last command intentionally exits1 and saves the missing-output failure. Use normal_positive_control for exit0. Full38-case diagnostics and3 independently implemented external checks are in MIN. Direct commands require the same offline conditions; final recorded validation used the guarded package entry. No --freeze or --freeze-revision is needed for review. The original EXPERIMENT_FREEZE is immutable; REVISION_FREEZE separately binds current execution code and the public execution policy. The complete authorizing policy and account records remain private.

Neither package authorizes new data collection, queue execution, Stage1D, or full-cohort evaluation. Linux GitHub archive construction is not Linux experiment testing.
