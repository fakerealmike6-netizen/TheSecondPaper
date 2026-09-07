# Finite historical RPC evidence

`src/rpc_context_r1.py` makes exactly one explicitly selected HTTP request to the
already authorized keyless PublicNode endpoint. It never sequences code/trace
calls, retries, enumerates addresses, reads credentials, or discovers neighbors.
The root operator decides whether a second, different call is appropriate after
examining the first receipt, access result and cumulative resource review.

The separate root-managed revision ledger must already contain the original
Stage1B operations and authorization. The client never initializes a fresh grant.
Every batch member, successful or failed, reserves and settles one operation;
HTTP timeout is an attempt. Dune/Alchemy rights and monetary cost are not inferred.
Methods are limited to historical `eth_getCode`, historical `eth_getBalance`, exact
`eth_getTransactionReceipt` / `eth_getTransactionByHash`, and an exact transaction
`debug_traceTransaction` with `callTracer` plus `withLog: true`. No opcode trace.

Create a params JSON file **inside the revision workspace**, containing the exact
authorized parameters. Create a separate root-reviewed resource JSON:

```json
{
  "schema_version": "rpc-resource-review-1",
  "review_id": "actual-root-review-id",
  "approved": true,
  "offline_repair_gate_passed": true,
  "cumulative_raw_bytes": 12345,
  "cumulative_raw_cap_bytes": 536870912,
  "method": "eth_getCode",
  "params": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "0x100"]
}
```

The example identity, historical block and byte count above are **synthetic**.
Replace them with the root's exact approved target and actual cumulative reviewed
bytes, including original Stage1B, all R1 providers, and unresolved raw-byte risk.
The client does not independently audit other providers' files or acquisition
time. The root must do that review and apply the query online-time boundary.
Any prior timeout retains its maximum read bound as resource risk rather than
claiming zero bytes. This bound is explicitly not a measured actual. The supplied
cumulative value must cover the previous RPC review plus its recorded read/risk.

From the revision workspace, the root alone may run:

```text
python -B src/rpc_context_r1.py --work . --method eth_getCode --params-json-file private/weth_code_params.json --resource-check-file private/weth_code_resource_review.json
```

There is no implicit trace call. A trace params file has this form:

```json
["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", {"tracer": "callTracer", "tracerConfig": {"withLog": true}}]
```

The Python interface is `RpcContextClient(work).call(method, params, review)`.
An optional `call_batch(plans, review)` accepts one to four explicit members; its
review uses `requests: [{method, params}, ...]`. One batch HTTP response is bounded
at 8 MiB total. Each member counts separately. Request identity is normalized, so
changing case, ordering a batch, or moving an old member to a single request does
not enable an automatic retry. Every intent is persistent before reservation;
the reserved dispatch intent is durable before invoking the transport. A crash
with unresolved dispatch blocks later work until explicit offline reconciliation.

Each attempt creates `raw/rpc_context_r1/rpc_r1_<logical-plan-hash>/` containing:

- `intent.json` and `dispatch_intent.json` with exact method, parameters and IDs;
- `response_body.bin`, preserving the actual received body or bounded prefix;
- one `envelope_N.json` per member with exact request and response;
- `receipt.json` and its SHA-256 sidecar, with hashes of body, envelopes and intents.

HTTP redirects are rejected; urllib socket timeout is 30 seconds; no automatic
retry is configured. Reading stops at 8 MiB plus a one-byte overflow sentinel.
Oversize data remain `RESPONSE_TOO_LARGE`, truncated and incomplete. They cannot
be accepted as a successful WETH evidence record. Raw bytes retained and known
received lower bounds are separate. A read exception preserves an upper bound
reservation for possible received bytes. The root must account for this risk.

For WETH, a successful receipt's `members` entry is an acquisition-catalogue
candidate. Its `artifact_path` is relative to the revision workspace. A catalogue
stored at workspace root can use it unchanged. If the catalogue is stored in a
subdirectory, copy the exact envelope under that directory, verify its hash, and
adjust only the relative path; `weth_evidence` forbids `../` traversal. A successful
RPC response is only transport/request evidence. A trusted root catalogue review
and the separate WETH identity/source/semantic gates remain required. Empty code
`0x` is an observed successful RPC result, never proof of deployed WETH code.

Injected transports are explicitly `SYNTHETIC_TRANSPORT` and cannot enter the
real WETH catalogue. Offline tests never invoke an actual provider:

```text
python -B -m unittest discover -s tests -p "test_rpc_context_r1.py" -v
```

This module's development and verification issued **zero real API requests**.
