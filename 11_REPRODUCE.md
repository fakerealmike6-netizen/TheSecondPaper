# Stage1C review

Start at [00_REVIEW_INDEX.md](00_REVIEW_INDEX.md). This is a development/first validation batch: 60 new controlled queries, the two previously developed Atomic/Harmony ETH contexts, and separate historical 12/32 regression evidence.

Python 3.14.3, NumPy 2.4.2 and SciPy 1.17.1 were tested on Windows. Use an existing environment with these dependencies. No dependency download is needed for replay on the tested host. Public runs controlled inputs; MIN additionally runs the two private evidence-bound models. No credentials are needed.

```text
python -B src/validate_stage1c.py --tree . --kind public --output ../stage1c_public_validation
python -B src/validate_stage1c.py --tree . --kind min --output ../stage1c_min_validation
```

Choose the command matching the package. The validator copies executable inputs to a fresh output mirror, blocks network and credentials using the inherited strict guard, runs the retained/new tests, old regression, and the actual frozen methods. Inputs are hash-checked before and after. Output must not already exist.

The direct experiment entry is `python -B src/run_stage1c.py --tree . --kind public --selection controlled --output ../controlled_results`; private users can use `--kind min --selection real`. `--freeze` is an authoring operation for a fully supplied private policy/input tree, not needed for review. Hidden allocations are only opened by evaluation after all methods finish.

External acceptance: PENDING_REVIEW. Stop: CHECKPOINT_1C_REACHED. No next-query acquisition, full-cohort study, or Stage1D is authorized by these results.
