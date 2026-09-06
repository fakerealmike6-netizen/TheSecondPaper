# Dune candidate adapter schema and invocation

`src/provider_dune.py` is implemented and tested with synthetic responses. No Dune SQL was executed by this module during preparation; provider integration remains conditional on root-session authorization and a persisted combined execution/export budget.

Schema verified on 2026-09-06 using primary sources:

- `ethereum.transactions`: `hash`, `from`, `to`, `value`, `block_number`, `index`, `block_time`, `success`, `gas_used`, `gas_price`. [Official transaction schema](https://docs.dune.com/data-catalog/evm/ethereum/raw/transactions).
- `ethereum.traces`: `tx_hash`, `from`, `to`, `address`, `refund_address`, `value`, `block_number`, `tx_index`, `block_time`, `trace_address`, `success`, `tx_success`, `type`, `call_type`. [Official trace schema](https://docs.dune.com/data-catalog/evm/ethereum/raw/traces).
- `erc20_ethereum.evt_Transfer`: `evt_tx_hash`, `evt_block_number`, `evt_block_time`, `evt_index`, `contract_address`, `from`, `to`, `value`. The official Ethereum base model passes this source to `transfers_base`; that macro references every listed field and joins transaction `index`. [Official source binding](https://github.com/duneanalytics/spellbook/blob/main/dbt_subprojects/tokens/models/transfers_and_balances/ethereum/tokens_ethereum_base_transfers.sql), [official macro field usage](https://github.com/duneanalytics/spellbook/blob/main/dbt_subprojects/tokens/macros/transfers/transfers_base.sql).
- Results use `result.rows`, `result.metadata.total_row_count`, `next_offset` and execution ID. [Official pagination](https://docs.dune.com/api-reference/executions/pagination).

The adapter queries only the current actual frontier address, its local time interval and query outer block bounds. It does not accept a reference target set. Top-level rows retain failed-transaction gas; internal traces exclude the root capacity duplicated by the top transaction, non-value delegate/static calls and any explicitly failed ancestor. ERC20 success and transaction index come from the raw transaction join. Missing fields produce a gap; a contract creation with unresolved recipient is not converted into a fabricated address. Trace paths do not establish trace-versus-log chronology.

SQL has no `LIMIT`, sampling or amount ranking. API pages use 1–1000 rows, default50; result count and cursor continuity establish only provider enumeration completeness. Full raw pages are cached and retained on interruption. The same SQL hash reuses the saved execution ID; an uncertain submission is not automatically repeated. Unfinished export retains its cursor and rows. Shared stage resources and all callback charges remain caller responsibilities.

Example integration:

```python
provider = DuneProvider(execute_and_wait, export_page, cache_dir, page_size=50)
```

`execute_and_wait(sql, logical_job_id)` must enforce the actual account cap/overage settings, reserve the combined logical job, run only an allowed engine, preserve SQL/execution ID/status and return the official completed status. `export_page(execution_id, {limit, offset}, logical_job_id)` must reserve/settle each result read under the same combined job, return the unfiltered official JSON, and refuse when remaining allowance cannot cover the page. Neither callback may send `ignore_max_credits_per_request=true`, enable partial results, or trigger a replacement query for pagination. The adapter itself contains no credentials or networking code.

The result contains16 columns; 1000 rows is not a promise of affordability. Dune export credits depend on datapoints/bytes and actual plan, so callers must price the prospective exports against the combined 2-credit job limit before fetching. A resource or credit stop remains partial coverage, never proof that an unexplored frontier is irrelevant.
