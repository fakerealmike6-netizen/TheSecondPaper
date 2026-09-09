# Stage1D interim source snapshot

INTERIM_SNAPSHOT / PAUSED / PENDING_REVIEW

This is an intermediate source export for independent review. Research collection remains paused. It is not a completed Stage1D result or external acceptance.

Generic production modules and auxiliary source are copied without algorithm changes. Source files that embed private evidence identifiers are explicitly withheld in full; their exact originals are in the private review handoff. See PUBLIC_SOURCE_INDEX.json for the included public files and PUBLIC_OMISSIONS.md for the limitations.

Code: revision/code/src/. Tests: revision/code/tests/. Synthetic fixtures are under revision/code/fixtures/ and revision/code/controlled_v1/. Some integration tests require private modules or historical private inputs and are not advertised as independently runnable from this public subset.

Workflow originals, if included, are stored outside .github/workflows under workflow_source_snapshot/ and cannot activate Actions on this branch. No provider data, query configuration, private handoff, account usage, or original authorization is published.

Next steps remain subject to external guidance: finish the existing-data-to-context adapter, reconcile pending identity evidence, close current ledgers, then freeze and validate the shared model inputs. No new collection or implementation is authorized by this README.
