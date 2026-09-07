# FIX04 WETH evidence interface

The repaired verifier is offline. Only the authorized network worker acquires
real evidence. Original evidence and billing ledgers remain unchanged.

`verify_component(policy, tx, receipt, internal_response, *, call_trace=None,
historical_code=None, source_attestation=None, evidence_context=None)` preserves
the original API. Extra dictionaries alone never authorize a real conversion.
`SYNTHETIC` context may produce `synthetic_component_verified=true`; it always
keeps `semantic_unit.certified`, `real_component_certified`, next real collection
and real LP eligibility false. An omitted context is unverified provenance.

## Smallest useful next real input

A successful `debug_traceTransaction` with callTracer and withLog for the exact
policy transaction, saved with its request, response, RPC ID, response hash and
chain context, can establish call/log binding even when the returned trace has
no transaction hash. A trace-only catalogue is accepted as partial evidence;
it does not require invented source/code records and cannot certify the component.
A failed/403 request is not a valid record. Historical `eth_getCode` must use the
exact policy contract and fixed block number (or the matching EIP-1898 block hash).
No opcode trace is needed. A request without logs cannot close the Deposit binding.

Create an immutable response envelope **from an actual successful acquisition**:

```json
{
  "http_status": 200,
  "request": {"jsonrpc":"2.0","id":"weth-trace-1","method":"debug_traceTransaction","params":["<exact policy transaction hash>",{"tracer":"callTracer","tracerConfig":{"withLog":true}}]},
  "response": {"jsonrpc":"2.0","id":"weth-trace-1","result":"<actual trace object>"}
}
```

This is a schema illustration, not executable or real evidence. Store no key,
authorization header, cookie or credential-bearing URL. Preserve original raw
bytes separately when the envelope is normalized, and link them in the acquisition
record. Never manufacture successful envelopes for failed requests.

The **separate trusted acquisition catalogue** is maintained by the authorized
collector/reviewer, not supplied by an arbitrary evidence-bundle caller:

```json
{
  "schema_version":"weth-acquisition-catalogue-1",
  "evidence_kind":"REAL_CHAIN",
  "acquisition_run_id":"<actual acquisition run>",
  "reviewer_record_id":"<review record>",
  "approved_provider_hosts":["<approved actual provider hostname>"],
  "records": {
    "trace-record": {
      "role":"trace", "evidence_kind":"REAL_CHAIN", "status":"SUCCESS_VALIDATED",
      "chain_id":1, "origin_url":"https://<approved actual provider hostname>",
      "artifact_path":"responses/trace.json", "artifact_sha256":"<SHA256 of complete envelope bytes>",
      "payload_sha256":"<weth_evidence.payload_hash(actual result object)>",
      "request":"<exact request object from the envelope>"
    }
  }
}
```

`bundle.json` only selects approved records: `{"records":{"trace":"trace-record"}}`.
All artifact paths resolve beneath the catalogue directory. Catalogue origin,
kind, success, request and hashes must already be justified by acquisition records.
The loader verifies local integrity and bindings; hashes do not independently
authenticate a provider or make a caller-written catalogue trustworthy. Treat the
catalogue as an explicit trusted input. Its SHA and bundle SHA are retained.

```python
from weth_evidence import load_evidence_context
from weth_component import verify_component
context = load_evidence_context("evidence/bundle.json", "evidence/catalogue.json")
result = verify_component(policy, tx, receipt, internal,
    call_trace=context.records.get("trace", {}).get("payload"),
    historical_code=context.records.get("historical_code", {}).get("payload"),
    evidence_context=context)
```

CLI accepts `--evidence-bundle` and `--acquisition-catalogue` together. It uses
catalogue payloads for supplied roles and the archived original inputs for roles
not yet acquired. Partial records improve only the proof obligations they support.

## Full real certification requirements

Records bind transaction (`eth_getTransactionByHash`), receipt
(`eth_getTransactionReceipt`), internal (`txlistinternal`, exact txhash/chainid),
trace, historical_code (`eth_getCode`, exact contract/historical block), source,
and deployment_runtime. Each record has the envelope/hash fields above.

The currently supported source adapter is deliberately narrow:
`source_type=ETHERSCAN_VERIFIED_SOURCE`, a successful `getsourcecode` response
bound to chain 1 and the exact contract, nonempty SourceCode/CompilerVersion,
verified ABI and `Proxy="0"`. The trusted catalogue also carries `source_review`:

- `review_id`, `source_record_id`, chain_id=1 and exact contract;
- `source_text_sha256` of the actual SourceCode string;
- `semantics=CANONICAL_WETH9_DEPOSIT_NO_FEE_NO_REFUND` established from that source;
- `runtime_code_sha256` of bytecode bytes;
- `deployment_runtime_match_evidence_id` naming the selected deployment_runtime record;
- `verification_block_number`, the fixed block used in that record's eth_getCode.

Deployment runtime must exactly equal the independently acquired historical code.
The source review is a recorded reviewer assertion anchored to actual verified
source and deployed bytes, not a general compiler proof. This does not support
unreviewed proxies or arbitrary protocol conversions. Other source formats stay
unverified until an explicit narrow adapter and evidence review are added.

Explicit chain/transaction/block/hash contradictions anywhere in supplied inputs
are rejected. Matching Deposit content with a contradictory global log index is
rejected. If a trace omits global log indices, an identical repeated receipt-log
pattern is ambiguous and stays unbound. `trace:1` in the old indexed cache is a
row ordinal; the actual call-tree path is reported separately only when evidenced.

## Offline replay

```text
python -B -m unittest discover -s tests -p "test_weth*.py" -v
python -B src/weth_repair_replay.py --module src/weth_component.py --fixtures fixtures/fault/weth --output checks/weth_fixed_faults.json --expect-fixed
```

The nine retained external/second-inspection controls are entirely synthetic.
The original six test intentions remain; the prior expectation that a complete
synthetic fixture constitutes real mainnet certification is explicitly corrected.
