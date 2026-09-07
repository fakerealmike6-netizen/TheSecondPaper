# Fixed WETH shared-response material check

The R4-R1 offline verifier checks the original five-member RPC response and its saved split envelopes. It does not import, modify, or replace any of the twelve existing WETH certification predicates. No new chain request or fee is required.

Run from an extracted MIN bundle:

```text
python -B src/verify_weth_shared_wire_r4_r1.py --tree . --output fresh/WETH_SHARED_WIRE_VALIDATION.json
```

The input manifest is `derived/weth_shared_wire_r4_r1/manifest.json`. All artifact paths are relative to the extracted bundle. `source_manifest.original_lookup_path` records the historical workspace lookup; `source_manifest.portable_path` identifies the accepted R4 manifest retained in the final MIN. The historical lookup path is not a bundle input path. The output must not already exist; this prevents accidental overwrite of evidence.

Successful material verification returns exit code 0 and JSON fields `status: PASS`, `material_status: VERIFIED_ORIGINAL_SHARED_BODY`, `original_body_available: true`, `original_body_sha256`, `members_verified: 5`, and `network_requests: 0`. A contradictory identity, changed bytes, duplicate or missing member, bad status, invalid selector, or unsafe path returns `status: FAIL` and a nonzero exit code. An explicit manifest documenting an unavailable original body can return `status: PASS` only with `material_status: MISSING_ORIGINAL_BODY`, `original_body_available: false`, and `members_verified: 0`; this is a documented material gap, never successful evidence verification. A missing manifest, including the intentionally private-input-free public tree, fails direct invocation. The public validator must explicitly skip this private material check, not report it as verified.

The recovered original body has 25,319 bytes and SHA-256 `c22430d52fd472458dc477c28a862986b0e2b54d2d4971770f78287033d006e7`. Its five members comprise four successful historical reads and one preserved trace RPC error. IDs bind each request, shared-response member, split envelope and batch receipt; array order is not used as the identity. The check also binds transaction, receipt and block identities, the historical code selector, three receipt log identities, and the 3,124-byte runtime code digest. The trace error is not promoted to a successful call tree.

The original request and receipt times remain `2026-09-07T07:50:42.712167+00:00` and `2026-09-07T07:50:44.488741+00:00`. The body and five envelopes are exact byte copies. The request-identity and batch-receipt views omit authentication-related commitments and account cost fields; the omitted field names are recorded. Each view has its own SHA-256, separate from its original source-file SHA-256. Original source hashes were checked locally during the finite copy; a redacted view cannot independently reproduce the original source-file hash. The original body is never reconstructed from the split envelopes.

The original response, split envelopes, views, manifest and lookup receipt belong only in MIN. The public bundle contains the verifier, its synthetic tests and this interface description. Synthetic tests inject explicitly synthetic fixed identities into `verify_document`; the real CLI always uses the accepted fixed identity and has no override flag.

Twenty-four dedicated tests passed on Windows. They cover the valid mixed success/error batch, member reordering, ID and identity conflicts, exact-byte tampering, historical selectors, status preservation, authentication-field rejection, safe paths, missing-material semantics and CLI exit/overwrite behavior. This subtask did not run a Linux environment. External acceptance remains `PENDING_REVIEW`.
