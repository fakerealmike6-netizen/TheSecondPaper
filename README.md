# TheSecondPaper: Stage1B research prototype

Public source, synthetic controls, and a limited research-review snapshot.
External acceptance is **PENDING_REVIEW**. Real probes remain **PARTIAL** until
their full declared intervals are evidenced; mock tests and sparse cache replay
are not provider validation. See [review status](00_REVIEW_INDEX.md).

Use Python 3.11 or newer. The reviewed environment used Python 3.14.3,
NumPy 2.4.2, SciPy 1.17.1 and its bundled HiGHS solver.

```text
python -m venv .venv
python -m pip install -r requirements.txt
python src/run_tests.py
python src/lp_run.py --fixtures fixtures/controlled --output derived/replayed_lp
python src/collector_replay.py
```

Activate the project environment using the normal command for your shell before
installing dependencies. Tests remove credential variables and block outbound
sockets. No key, account, historical project tree, real blockchain cache or
Windows drive is required. All bundled fixtures are synthetic.

`lp_model.py` consumes event graphs, not hidden allocations or Oracle files.
`lp_oracle.py` independently enumerates rational vertices. The collector accepts
an exact seed, outer scope, identity resolver and provider callbacks; it does
not load reference targets as neighbors. Network adapters require separately
authorized current entitlements and the same persistent shared budget.

Private-data reconstruction tools accept explicit input paths. To reproduce
real evidence, obtain the original data under its own terms, verify the stated
source versions/hashes in the local review, and supply the necessary canonical
events, seed members and label snapshot. Full third-party labels, reference
tables and RPC/account caches are deliberately absent from this repository.
No claim dependent on those missing data can be independently reproduced from
the public package alone.

Re-exporting is an explicit local action, with no publication side effect:

```text
python src/publication_export.py --source-work SOURCE_STAGE_DIRECTORY --destination NEW_EXPORT_DIRECTORY --stage-task Stage1B
```

Read [NOTICE](NOTICE.md) before reuse. Original code licensing awaits the
researcher's selection; public visibility does not grant an unstated license.
