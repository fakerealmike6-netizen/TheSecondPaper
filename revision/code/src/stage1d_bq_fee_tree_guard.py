"""Local completeness guard for BQ point facts; no network, writes or formal run.

The current ledger only models execution gas. A positive blob fee remains an
explicit fee gap until the authorized context layer supports that component.
"""
from context_ledger_r3 import integer,trace_path


def inspect_rows(rows,normalized):
    result={"conflicts":[],"top_gaps":[],"internal_gaps":[],"fee_gaps":[],"fee_components":[]}
    tops={r["tx_hash"]:r for r in rows if r["record_type"]=="transaction"}
    roots={r["tx_hash"]:r for r in rows if r["record_type"]=="trace" and trace_path(r["trace_address"])==()}
    normalized_tops={r["tx_hash"]:r for r in normalized["transactions"]}

    def item(row,reason,**extra):
        return {"type":reason,"tx_hash":row["tx_hash"],"block_number":integer(row["block_number"]),
                "address":row.get("from_address"),"evidence_ids":row.get("evidence_ids",[]),**extra}

    for tx_hash,row in tops.items():
        tx=normalized_tops.get(tx_hash,{})
        if row["success"] is True and integer(row["value_raw"])>0 and tx.get("recipient") is None:
            result["top_gaps"].append(item(row,"POSITIVE_CREATE_RECIPIENT_NOT_ESTABLISHED"))
        created=row.get("created_address")
        root=roots.get(tx_hash)
        if created and root and root.get("trace_type","").lower() in {"create","create2"}:
            root_recipient=root.get("created_address") or root.get("to_address")
            if root_recipient and created.lower()!=root_recipient.lower():
                result["conflicts"].append(item(row,"CREATED_CONTRACT_RECEIPT_ROOT_CONFLICT"))
        transaction_type=row.get("transaction_type")
        blob_used=row.get("blob_gas_used")
        blob_price=row.get("blob_gas_price")
        try:
            kind=integer(transaction_type) if transaction_type is not None else None
            used=integer(blob_used) if blob_used is not None else None
            price=integer(blob_price) if blob_price is not None else None
        except ValueError as exc:
            result["conflicts"].append(item(row,"INVALID_EXACT_BLOB_FEE_FIELD",error_class=type(exc).__name__))
            continue
        execution=tx.get("fee_raw")
        if row.get("gas_used") is None or row.get("effective_gas_price") is None:
            result["fee_gaps"].append(item(row,"EXECUTION_FEE_OPERAND_MISSING"))
        blob_fee=None
        if kind is None:
            result["fee_gaps"].append(item(row,"TRANSACTION_TYPE_MISSING_BLOB_FEE_NOT_EXCLUDED"))
        elif kind in {0,1,2}:
            if used not in (None,0) or price not in (None,0):
                result["conflicts"].append(item(row,"NON_BLOB_TYPE_HAS_BLOB_FEE_FIELDS"))
            else:
                blob_fee=0
        elif kind==3:
            if used is None or price is None:
                result["fee_gaps"].append(item(row,"BLOB_FEE_OPERAND_MISSING"))
            elif used==0 or price==0:
                # A blob transaction cannot prove fee-free blobs with zeros.
                result["conflicts"].append(item(row,"BLOB_TRANSACTION_HAS_ZERO_BLOB_GAS_OR_PRICE"))
            else:
                blob_fee=used*price
                result["fee_gaps"].append(item(row,"POSITIVE_BLOB_FEE_REQUIRES_CONTEXT_COMPONENT",
                                              blob_fee_raw=str(blob_fee)))
        else:
            result["fee_gaps"].append(item(row,"TRANSACTION_TYPE_FEE_SEMANTICS_UNSUPPORTED",transaction_type=kind))
        result["fee_components"].append(item(row,"EXACT_FEE_COMPONENT_EVIDENCE",
            transaction_type=kind,execution_fee_raw=execution,
            blob_fee_raw=str(blob_fee) if blob_fee is not None else None,
            total_fee_raw=str(integer(execution)+blob_fee) if execution is not None and blob_fee is not None else None,
            total_fee_installed_in_current_ledger=False))

    for row in rows:
        if row["record_type"]!="trace":continue
        kind=row.get("trace_type")
        call=row.get("call_type")
        if row.get("success") is True and row.get("error"):
            result["conflicts"].append(item(row,"TRACE_SUCCESS_ERROR_CONFLICT",trace_address=row["trace_address"]))
        if not isinstance(kind,str) or kind.lower() not in {"call","create","create2","suicide","selfdestruct"}:
            result["internal_gaps"].append(item(row,"TRACE_TYPE_PHYSICAL_SEMANTICS_UNSUPPORTED",trace_address=row["trace_address"]))
        elif kind.lower()=="call" and (not isinstance(call,str) or call.lower() not in {"call","callcode","delegatecall","staticcall"}):
            result["internal_gaps"].append(item(row,"CALL_TYPE_PHYSICAL_SEMANTICS_UNSUPPORTED",trace_address=row["trace_address"]))
    return result


def fee_gaps_for_range(review,needed):
    return [gap for gap in review["fee_gaps"] if gap["address"]==needed["address"]
            and needed["start_block"]<=gap["block_number"]<=needed["end_block"]]
