# TheSecondPaper: Stage1B-R1

Bounded Ethereum event collection, four verified implementation repairs, versioned ownership policy and a time-expanded LP research prototype. External acceptance **PENDING_REVIEW**; see [review index](00_REVIEW_INDEX.md) and explicit real-probe gaps.

Tested with Python 3.14.3, NumPy 2.4.2 and SciPy 1.17.1. Install requirements in an isolated environment, then run without credentials or network:

```text
python -B src/run_tests.py --output validation/tests.json
python -B src/lp_run.py --fixtures fixtures/controlled --output validation/controlled_lp
python -B src/validate_review_bundle_r1.py --tree . --kind public --output ../fresh_validation
```

The public archive intentionally includes synthetic fixtures only. The local MIN additionally replays the required real input subsets, reference deltas and WETH evidence; neither establishes complete-chain coverage. Source/test/fixture equivalence is in manifests/PUBLIC_SOURCE_MAPPING.json.
