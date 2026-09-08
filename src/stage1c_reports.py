"""Post-result summaries with an allowlisted public aggregation boundary.

No methods are run here. Private references are opened only after the completed
results index and method outputs. Public outputs contain named case aggregates,
never real account/transaction identities, reference rows, or account ledgers.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
from decimal import Decimal, localcontext
from fractions import Fraction as F
import hashlib
import json
from pathlib import Path
import re
import statistics

METHODS = ("FULL_INTERVAL", "BOUNDED_REACHABILITY", "POISON", "HAIRCUT", "NO_CROSS_TARGET_COUPLING", "NO_PROTOCOL_CONTINUATION", "BALANCE_INFORMATION_REMOVED")
REPORT_VERSION = "STAGE1C_POST_RESULT_REPORTS_V1"


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def mean(values):
    values = [F(v) for v in values if v is not None]
    return str(sum(values, F(0)) / len(values)) if values else None


def dec(value, places=6, scale=1):
    if value is None:
        return "NA"
    value = F(value) / scale
    with localcontext() as context:
        context.prec = 90
        return format(Decimal(value.numerator) / Decimal(value.denominator), f".{places}f")


def table(headers, rows):
    return "\n".join(["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"] + ["| " + " | ".join(str(v).replace("|", "/") for v in row) + " |" for row in rows])


def positive(value):
    if value.get("supported") is not None:
        return value["supported"]
    for key in ("upper_raw", "point_raw", "nominal_raw"):
        if value.get(key) is not None:
            return F(value[key]) > 0
    return False


def output_sets(result):
    addresses = set(result.get("positive_addresses") or result.get("output_address_ids") or [])
    # Baselines visit context/fee ports too: restrict reported output events to
    # the identical service-entry domain, rather than counting visited ports.
    service_entries = {e for row in (result.get("addresses") or {}).values() for e in row.get("events", row.get("objective_events", []))}
    events = {eid for eid, row in (result.get("events") or {}).items() if eid in service_entries and positive(row)}
    return addresses, events


def controlled_summary(records):
    status = {m: Counter() for m in METHODS}
    query_rows, comparison_counts = [], Counter()
    endpoint_errors, haircut_raw_errors = defaultdict(list), defaultdict(list)
    recalls = {m: [] for m in METHODS}
    false_positives = Counter()
    empty_positive_controls = Counter()
    scoped_outputs = {m: {"addresses": set(), "address_assets": set(), "events": set()} for m in METHODS}
    tiny = []
    for item in records:
        row, results, evaluation = item["index"], item["methods"], item["evaluation"]
        q = {"sample_id": row["sample_id"], "family": row["family"], "evaluation_passed": evaluation.get("passed", False),
             "errors": evaluation.get("errors", []), "method_statuses": {m: results[m]["status"] for m in METHODS},
             "joint_intervals": {}, "haircut_exact_feasible": (evaluation.get("haircut_full_assignment_audit") or {}).get("exact_feasible"),
             "haircut_query_mean_normalized_error": mean(p["normalized_absolute_error"] for p in evaluation.get("haircut_point_errors", [])),
             "all_full_objectives_hidden_covered": all(c["hidden_covered"] for c in evaluation.get("comparisons", [])) if evaluation.get("comparisons") else None,
             "ablation_by_asset": item["ablations"]}
        event_assets = {event: group.rsplit("|", 1)[1] for group, v in (results["FULL_INTERVAL"].get("addresses") or {}).items() for event in v.get("objective_events", [])}
        for method in METHODS:
            status[method][results[method]["status"]] += 1
            output_addresses, output_events = output_sets(results[method])
            scoped_outputs[method]["addresses"].update((row["sample_id"], a.rsplit("|", 1)[0]) for a in output_addresses)
            scoped_outputs[method]["address_assets"].update((row["sample_id"], a) for a in output_addresses)
            scoped_outputs[method]["events"].update((row["sample_id"], e) for e in output_events)
            metric = evaluation.get("address_metrics", {}).get(method, {})
            if metric.get("recall") is not None:
                recalls[method].append(metric["recall"])
            false_positives[method] += metric.get("false_positive_count") or 0
            empty_positive_controls[method] += bool(metric.get("no_positive_control"))
        for comparison in evaluation.get("comparisons", []):
            category, objective = comparison["category"], comparison["objective"]
            comparison_counts[category + "_total"] += 1
            comparison_counts[category + "_covered"] += bool(comparison["hidden_covered"])
            asset = objective.rsplit("|", 1)[-1] if category == "addresses" else objective if category == "joint_by_asset" else event_assets.get(objective)
            if comparison.get("endpoint_error_raw") is not None and asset:
                endpoint_errors[asset].append(F(comparison["endpoint_error_raw"]))
            if category == "joint_by_asset":
                full = results["FULL_INTERVAL"]["joint_by_asset"][objective]
                poison = (results["POISON"].get("joint_by_asset") or {}).get(objective, {}).get("nominal_raw")
                q["joint_intervals"][objective] = {**{k: full.get(k) for k in ("lower_raw", "upper_raw")},
                    "width_raw": comparison["width_raw"], "width_over_seed": comparison["width_over_seed"],
                    "hidden_covered": comparison["hidden_covered"], "positive_lower": comparison["positive_lower"],
                    "poison_nominal_raw": poison,
                    "poison_minus_full_upper_raw": str(F(poison) - F(full["upper_raw"])) if poison is not None and full.get("upper_raw") is not None else None,
                    "poison_over_full_upper": str(F(poison) / F(full["upper_raw"])) if poison is not None and full.get("upper_raw") is not None and F(full["upper_raw"]) > 0 else None}
        for point in evaluation.get("haircut_point_errors", []):
            haircut_raw_errors[point["address_asset"].rsplit("|", 1)[1]].append(F(point["absolute_point_error_to_one_hidden_assignment_raw"]))
        if evaluation.get("tiny_crosscheck", {}).get("status") != "NOT_REQUIRED":
            tiny.append(evaluation.get("tiny_crosscheck"))
        query_rows.append(q)
    def aggregate(rows):
        joints = [v for q in rows for v in q["joint_intervals"].values()]
        query_widths = [mean(v["width_over_seed"] for v in q["joint_intervals"].values()) for q in rows]
        return {"query_count": len(rows), "evaluation_passed": sum(q["evaluation_passed"] for q in rows),
                "query_mean_normalized_union_width": mean(query_widths), "resolved_query_asset_union_count": len(joints),
                "positive_joint_lower_count": sum(v["positive_lower"] is True for v in joints),
                "zero_joint_lower_count": sum(v["positive_lower"] is False for v in joints),
                "joint_hidden_covered_count": sum(v["hidden_covered"] for v in joints),
                "all_objectives_covered_query_count": sum(q["all_full_objectives_hidden_covered"] is True for q in rows),
                "haircut_exact_feasible_query_count": sum(q["haircut_exact_feasible"] is True for q in rows),
                "haircut_query_mean_normalized_error": mean(q["haircut_query_mean_normalized_error"] for q in rows),
                "poison_exceeds_feasible_union_upper_count": sum(v["poison_minus_full_upper_raw"] is not None and F(v["poison_minus_full_upper_raw"]) > 0 for v in joints),
                "coupling_upper_changed_query_asset_count": sum(F(a["independent_upper_excess_raw"]) > 0 for q in rows for a in q["ablation_by_asset"]),
                "protocol_changed_query_asset_count": sum(a["protocol_result_changed"] for q in rows for a in q["ablation_by_asset"]),
                "balance_nested_query_asset_count": sum(a["balance_nested_endpoints"] for q in rows for a in q["ablation_by_asset"])}
    return {"population": "ALL_60_PRESPECIFIED_CONTROLS_RETAINED", **aggregate(query_rows),
            "method_status_counts": {m: dict(v) for m, v in status.items()}, "comparison_counts": dict(comparison_counts),
            "maximum_endpoint_error_raw_by_asset": {a: str(max(v)) for a, v in endpoint_errors.items()},
            "haircut_maximum_error_to_one_hidden_assignment_raw_by_asset": {a: str(max(v)) for a, v in haircut_raw_errors.items()},
            "address_metrics": {m: {"query_macro_recall": mean(recalls[m]), "eligible_positive_query_count": len(recalls[m]),
                "full_population_count": len(records), "empty_positive_control_count": empty_positive_controls[m],
                "false_positive_query_address_asset_pairs": false_positives[m]} for m in METHODS},
            "deduplicated_outputs_with_synthetic_query_namespace": {m: {unit: len(values) for unit, values in units.items()} for m, units in scoped_outputs.items()},
            "by_family": {family: aggregate([q for q in query_rows if q["family"] == family]) for family in sorted({q["family"] for q in query_rows})},
            "tiny_checks": tiny, "queries": query_rows}


def reference_metrics(reference, result):
    address, events = output_sets(result)
    known_address = set(reference.get("known_positive_address_asset_keys", []))
    known_events = set(reference.get("known_positive_entry_event_ids", []))
    allowed = reference.get("recall_allowed", False) and bool(known_address)
    actors = {r["actor"] for r in reference.get("positive_reference_rows", []) if r.get("actor") and r["target_address"] + "|ETH" in address}
    known_actors = set(reference.get("known_positive_actors", []))
    return {"denominator_status": reference.get("denominator_status", "REFERENCE_UNAVAILABLE"),
            "known_positive_address_asset_count": len(known_address), "known_positive_entry_event_count": len(known_events),
            "known_positive_actor_count": len(known_actors), "reference_globally_complete": False,
            "matched_known_address_asset_count": len(address & known_address) if allowed else None,
            "matched_known_entry_event_count": len(events & known_events) if allowed else None,
            "matched_known_actor_count": len(actors & known_actors) if allowed else None,
            "address_recall": str(F(len(address & known_address), len(known_address))) if allowed else None,
            "event_recall": str(F(len(events & known_events), len(known_events))) if allowed and known_events else None,
            "actor_recall": str(F(len(actors & known_actors), len(known_actors))) if allowed and known_actors else None,
            "precision": None, "false_positive_count": None,
            "unknown_or_reference_external_outputs_are_negatives": False}


def real_summary(records, references, metadata):
    queries = []
    global_sets = {m: {"addresses": set(), "address_assets": set(), "events": set(), "known_reference_actors": set()} for m in METHODS}
    query_address_counts, query_event_counts = Counter(), Counter()
    for item in records:
        sid, results = item["index"]["sample_id"], item["methods"]
        ref, meta = references.get(sid, {}), metadata.get(sid, {})
        entry = {"case": "Atomic" if sid == "atomic_simple_transfer" else "Harmony" if sid == "harmony_high_branch" else "UNNAMED_REAL_CASE",
                 "sample_id": sid, "incident_id": meta.get("incident_id"), "source_layer": meta.get("source_layer", "S2"),
                 "zero_hop": meta.get("zero_hop", False), "counts": meta.get("accepted_counts"),
                 "same_input_full_regression": item["evaluation"].get("same_input_full_regression"),
                 "real_source_amount_truth": None, "real_amount_accuracy": None, "methods": {}, "ablations": item["ablations"]}
        for method in METHODS:
            value = results[method]
            aa, ee = output_sets(value)
            global_sets[method]["address_assets"].update(aa)
            global_sets[method]["addresses"].update(a.rsplit("|", 1)[0] for a in aa)
            global_sets[method]["events"].update(ee)
            query_address_counts[method] += len(aa)
            query_event_counts[method] += len(ee)
            actor_hits = {r["actor"] for r in ref.get("positive_reference_rows", []) if r.get("actor") and r["target_address"] + "|ETH" in aa}
            global_sets[method]["known_reference_actors"].update(actor_hits)
            joint = (value.get("joint_by_asset") or {}).get("ETH", {})
            amounts = {k: joint.get(k) for k in ("lower_raw", "upper_raw", "point_raw", "nominal_raw")}
            boundary = value.get("boundary_completion") or {}
            entry["methods"][method] = {"status": value["status"], "applicability": value.get("applicability"),
                "output_kind": value.get("output_kind"), "asset": "ETH", "joint_raw": amounts,
                "joint_eth_display_rounded_18dp": {k.removesuffix("_raw"): dec(v, 18, 10**18) if v is not None else None for k, v in amounts.items()},
                "positive_address_asset_count": len(aa), "positive_entry_event_count": len(ee),
                "task_counts": value.get("task_counts"), "reference": reference_metrics(ref, value),
                "haircut_exact_assignment_feasible": (item["evaluation"].get("haircut_full_assignment_audit") or {}).get("exact_feasible") if method == "HAIRCUT" else None,
                "boundary_convention": boundary.get("convention"), "boundary_adopted": boundary.get("adopted"),
                "boundary_B0_raw": boundary.get("B0_out_raw"), "boundary_is_observed_fact": boundary.get("is_observed_fact"),
                "connected_protocol_feature": (value.get("modifications") or {}).get("feature_status")}
        queries.append(entry)
    macro = {}
    for method in METHODS:
        cases = defaultdict(list)
        for q in queries:
            recall = q["methods"][method]["reference"]["address_recall"]
            if recall is not None:
                cases[q["incident_id"]].append(recall)
        macro[method] = {"query_macro_known_positive_address_recall": mean(q["methods"][method]["reference"]["address_recall"] for q in queries),
                         "incident_macro_known_positive_address_recall": mean(mean(v) for v in cases.values()),
                         "eligible_incidents": len(cases), "all_incidents": len({q["incident_id"] for q in queries}),
                         "eligible_queries": sum(q["methods"][method]["reference"]["address_recall"] is not None for q in queries)}
    return {"query_count": len(queries), "queries": queries, "query_and_incident_macro": macro,
            "deduplication": {m: {"global_unique_addresses": len(s["addresses"]), "global_unique_address_assets": len(s["address_assets"]),
                "global_unique_service_entry_events": len(s["events"]), "query_address_asset_pairs": query_address_counts[m],
                "query_entry_event_pairs": query_event_counts[m], "global_matched_known_reference_actors": len(s["known_reference_actors"]),
                "global_all_output_actors": None, "all_output_actor_status": "NOT_ALL_OUTPUT_ADDRESSES_HAVE_A_FROZEN_ACTOR_MAPPING"} for m, s in global_sets.items()},
            "strata": {"S1": {"evaluated": 0}, "S2": {"evaluated": len(queries)}, "S3": {"evaluated": 0, "new_injections": 0}},
            "reference_limitation": "Atomic has one independently retained known-positive address and six entries. Harmony has no currently verified reference positives after first-service correction; its runtime targets are not a reference denominator. Recall is null there."}


def efficiency_summary(records):
    output = {}
    for kind in ("controlled", "real"):
        selected = [r for r in records if r["index"]["kind"] == kind]
        output[kind] = {}
        for method in METHODS:
            rows = []
            for item in selected:
                efficiency = item["efficiency"]
                profile = efficiency["profiles"][method]
                preprocessing = efficiency["shared_preprocessing_raw_seconds"][1:]
                rows.append({"sample_id": item["index"]["sample_id"], "method_status": item["methods"][method]["status"],
                    "method_median_seconds": profile["median_seconds"], "method_min_seconds": profile["min_seconds"], "method_max_seconds": profile["max_seconds"],
                    "common_preprocess_median_seconds": statistics.median(preprocessing),
                    "fixed_graph_end_to_end_median_seconds": profile["median_seconds"] + statistics.median(preprocessing),
                    "python_warmup_peak_bytes": profile["warmup_python_peak_bytes"],
                    "task_counts": item["methods"][method].get("task_counts"),
                    "median_model_build_seconds": statistics.median(r["parts"]["model_build_seconds"] for r in profile["repetitions"][1:]) if all(r.get("parts") and "model_build_seconds" in r["parts"] for r in profile["repetitions"][1:]) else None,
                    "median_solve_and_certification_seconds": statistics.median(r["parts"]["solve_and_certification_seconds"] for r in profile["repetitions"][1:]) if all(r.get("parts") and "solve_and_certification_seconds" in r["parts"] for r in profile["repetitions"][1:]) else None})
            supported = [r for r in rows if r["method_status"] == "COMPLETED"]
            output[kind][method] = {"population_count": len(rows), "completed_count": len(supported),
                "median_of_completed_query_method_medians_seconds": statistics.median(r["method_median_seconds"] for r in supported) if supported else None,
                "queries": rows}
    return {"warmup_repetitions": 1, "timed_repetitions": 5, "groups": output,
            "measurement_scope": "Local fixed observed graph; shared JSON/target preprocessing and method-only computations separated; no online collection timed.",
            "task_equivalence": False, "memory_scope": "Python allocation high-water in warmup only; native HiGHS/SciPy allocations excluded.",
            "statistical_significance_claim": False, "online_acquisition_saving_claim": False}


DICTIONARY = {
    "primary_statistical_unit": "One frozen query; synthetic query IDs scoped to their individual graph.",
    "secondary_unit": "Incident macro mean of eligible query means; report eligible/all counts; no extrapolation from two development cases.",
    "address_unit": "Unique query,address,asset for query metrics; real global address and address-asset identities deduplicated separately.",
    "event_unit": "Unique exact service-entry physical event; baseline visited context/fee ports excluded from output counts.",
    "actor_unit": "Frozen known-reference actor mapping is separate from address identity. Unmapped total output actor count is null.",
    "controlled_positive": "Independent Oracle feasible address/asset upper >0; hidden-zero alone is not a negative.",
    "normalized_union_width": "(U-L)/Q within query; controlled converted WETH uses the exact 1:1 raw-equivalent source Q. Do not add different assets.",
    "raw_endpoint_error": "max(abs(FULL.L-Oracle.L),abs(FULL.U-Oracle.U)); report max separately by asset.",
    "hidden_coverage": "FULL interval contains the separately constructed feasible hidden assignment for the same objective.",
    "haircut_error": "Absolute distance to one hidden feasible assignment, not error to a uniquely identified source truth; query mean normalized by Q.",
    "poison_amount": "POISON_NOMINAL_RAW is nominal marked physical volume. Exceeding feasible joint upper describes looseness, not a conservative allocation guarantee.",
    "real_accuracy": "null: neither reference labels, incomplete reference paths, nor Value_in_USD establish real source-amount ground truth.",
    "real_recall": "Known-positive recall only where the versioned independent reference denominator is nonempty and verified; no false-positive classification outside reference.",
    "failure_policy": "All prespecified samples retained; ERROR and NOT_APPLICABLE distinct; no missing amount imputed as zero.",
    "zero_positive_recall": "Recall null with no positive Oracle addresses; these controls remain in total sample and false-positive counts.",
    "timing": "One warmup plus five method timings; JSON/target preprocessing separate; workloads differ and timings do not establish same-task speed superiority.",
    "information_ablation": "Same graph and physical events; selected supported source-balance upper bounds relaxed. This is distinct from target-copy coupling ablation.",
    "coupling_ablation": "Independent target-specific complete feasible copies; sum of their maxima need not be realizable in one shared physical allocation.",
    "protocol_ablation": "Net source terminates at supported protocol input boundary; gross/refund conservation retained and actual output source fixed zero.",
}


def markdown_reports(stats):
    c, real, efficiency = stats["controlled"], stats["real"], stats["efficiency"]
    status_rows = [(m, c["method_status_counts"][m].get("COMPLETED", 0), c["method_status_counts"][m].get("NOT_APPLICABLE", 0), c["method_status_counts"][m].get("ERROR", 0)) for m in METHODS]
    overall = "# Stage1C first paired experiment results\n\nExternal acceptance: `PENDING_REVIEW`. Stop: `CHECKPOINT_1C_REACHED`.\n\n"
    overall += f"Actual batch status: {stats['batch_status']}; {c['query_count']} controlled queries and {real['query_count']} real development queries. Controlled evaluations passed: {c['evaluation_passed']}/{c['query_count']}. These are first development validations, not unseen holdout or full-population evidence.\n\n"
    overall += table(["Method", "Completed controls", "Not applicable", "Errors"], status_rows) + "\n\n"
    overall += f"Maximum FULL/independent-Oracle endpoint error by asset: {json.dumps(c['maximum_endpoint_error_raw_by_asset'])}. Joint hidden coverage: {c['joint_hidden_covered_count']}/{c['resolved_query_asset_union_count']}. Haircut exact feasible assignments: {c['haircut_exact_feasible_query_count']}; all unsupported cases remain listed.\n\n"
    overall += "No new research-platform request or cost was recorded by the offline runner. Existing provider risk/unknown-cost ledgers remain separate and are not reset by this report. Publication and final bundle identities are recorded in the external publication receipt. No new-query collection, full-sample experiment or Stage1D is authorized here.\n"
    methods = "# Methods and fair input boundary\n\nAll seven methods use identical frozen observed physical facts, target domain, event order, asset scope, actual fees and available balances. Baselines cannot read hidden allocations, Oracle answers, references or FULL endpoints. Reference evaluation occurs after method results are saved.\n\n"
    methods += table(["Method", "Output and interpretation"], [
        ("FULL_INTERVAL", "Exact-certified event/address/joint feasible source intervals"),
        ("BOUNDED_REACHABILITY", "Temporal/asset address-event set; no amount"),
        ("POISON", "Persistent boolean marking and nominal raw physical volume"),
        ("HAIRCUT", "Independent exact-rational proportional replay; explicit B_min boundary convention when needed"),
        ("NO_CROSS_TARGET_COUPLING", "Product of independent target-specific complete source-allocation copies"),
        ("NO_PROTOCOL_CONTINUATION", "Supported protocol net source terminates at input boundary"),
        ("BALANCE_INFORMATION_REMOVED", "Same physical graph with selected balance information relaxed")]) + "\n\n"
    methods += "Haircut is not chosen by optimizing the primary LP. The primary constraints only verify its already-constructed allocation. H_BMIN_BOUNDARY_V1 supplies the minimum non-source outside balance needed by the observed chronology; it is a baseline assumption, not an observed balance. Unknown modeled balances outside this convention are reported unsupported. WETH refund examples use a synthetic gross/refund envelope around the canonical net deposit; WETH9.deposit itself is not claimed to refund.\n\nSee METHOD_SPEC_EFFECTIVE.md, EXPERIMENT_FREEZE.json and STATISTICAL_DICTIONARY.json for frozen semantics, code identities, and metric units.\n"
    controlled = "# CONTROLLED_V1 results\n\nAll six prespecified families contain ten newly constructed samples. The historical twelve scenarios and thirty-two comparisons are separate regression evidence. Hidden feasible allocations and independent Oracle extrema serve different roles; neither constitutes external blind truth.\n\n"
    controlled += table(["Family", "Queries", "Passed", "Mean normalized union width", "Positive / zero lower", "Haircut feasible", "Poison > full upper"],
        [(f, s["query_count"], s["evaluation_passed"], dec(s["query_mean_normalized_union_width"]), f"{s['positive_joint_lower_count']} / {s['zero_joint_lower_count']}", s["haircut_exact_feasible_query_count"], s["poison_exceeds_feasible_union_upper_count"]) for f, s in c["by_family"].items()]) + "\n\n"
    controlled += f"Query mean normalized union width: {dec(c['query_mean_normalized_union_width'])}. Exact endpoint errors by asset: {json.dumps(c['maximum_endpoint_error_raw_by_asset'])}. Hidden coverage by statistical unit: {json.dumps(c['comparison_counts'])}.\n\n"
    controlled += table(["Method", "Query macro address recall", "Eligible positive queries / all", "False-positive query/address/asset pairs"],
        [(m, dec(v["query_macro_recall"]), f"{v['eligible_positive_query_count']} / {v['full_population_count']}", v["false_positive_query_address_asset_pairs"]) for m, v in c["address_metrics"].items()]) + "\n\n"
    controlled += f"Haircut query mean normalized distance to one hidden assignment: {dec(c['haircut_query_mean_normalized_error'])}. This distance does not imply that a different feasible proportional allocation is wrong. Exact whole-assignment feasibility is assessed separately. {len(c['tiny_checks'])} prespecified tiny crosschecks are retained; the dedicated evidence file reports objective counts and full enumeration status.\n\nPer-query and per-objective results are preserved in the machine-readable batch output. All failures and unsupported cases remain in STATISTICS.json and METHOD_SUPPORT_MATRIX.csv.\n"
    pilots = "# Real pilot comparisons\n\nThe two ETH graphs are the accepted full contexts: 55 anchors, 69 value events and 61 fees in total. They are previously developed cases. No real source-amount truth is available; no real amount accuracy or precision is reported.\n\n"
    real_rows = []
    for q in real["queries"]:
        for m in METHODS:
            v = q["methods"][m]; a = v["joint_eth_display_rounded_18dp"]
            amount = "[" + str(a["lower"]) + ", " + str(a["upper"]) + "]" if a["lower"] is not None else a["point"] or a["nominal"] or "No amount"
            real_rows.append((q["case"], m, amount, v["positive_address_asset_count"], v["positive_entry_event_count"], dec(v["reference"]["address_recall"])))
    pilots += table(["Case", "Method", "Joint ETH / point / nominal", "Positive addresses", "Positive entries", "Known-positive address recall"], real_rows) + "\n\n"
    pilots += real["reference_limitation"] + " Unknown labels and reference-external outputs are not negative examples.\n\n"
    for q in real["queries"]:
        h = q["methods"]["HAIRCUT"]
        pilots += f"{q['case']}: Haircut complete assignment feasible={h['haircut_exact_assignment_feasible']}; B_min convention adopted={h['boundary_adopted']}, B0={dec(h['boundary_B0_raw'],18,10**18)} ETH. B0 is an assumed completion, not an observed fact. The two real ETH graphs contain no connected supported WETH conversion, so the protocol-ablation feature is absent.\n\n"
    pilots += "Amounts displayed to 18 decimals; exact rational raw quantities remain in STATISTICS.json. Public reporting omits real account/transaction identities and third-party reference rows; MIN retains detailed per-address and per-event results. Query and incident macro summaries report their eligible denominators, with global address, address-asset, entry-event and known-actor counts deduplicated separately.\n"
    ablation = "# Ablations and semantic equivalence\n\n"
    ablation += f"Controlled query/asset unions: removing cross-target coupling increases {c['coupling_upper_changed_query_asset_count']} upper bounds; protocol termination changes {c['protocol_changed_query_asset_count']} unions; balance-relaxed endpoint nesting holds in {c['balance_nested_query_asset_count']} unions. Exact per-query intervals and removed-bound/copy identities are in the raw method results.\n\n"
    ablation += "The coupling experiment constructs separate target-specific variable/constraint copies; the explicit small block-diagonal product is independently checked. Its independent maxima sum is not a jointly realizable FULL amount when it exceeds FULL union upper. The witness remains a statement about shared source allocation, not an error in optimizing a single target.\n\n"
    ablation += "Protocol termination preserves gross/refund/net conservation and actual output balances while making cross-protocol output source zero. It does not delete input funds. Ten synthetic WETH components include output/joint/unequal/signed-weight comparisons. The two tiny components enumerate full boundary relations for source alpha ranging from zero to gross. Raw g=r+d, m=d, sum(y)=m folds to sum(y)=g-r, with inverse d=m=sum(y). The recorded algebraic bijection applies to these closed 1:1 capacities only. See SEMANTIC_COMPONENT_EQUIVALENCE.json; real ETH pilot feature absence is reported without adding an unrelated WETH transaction.\n"
    timing = "# Efficiency and scope\n\nOne warmup and five timed repetitions were saved for each sample/method, with a deterministic rotated method order. Common JSON/target preprocessing, method model construction, and solve/certification are distinguished where applicable. Baseline timings include their own compilation/propagation.\n\n"
    timing += table(["Population", "Method", "Completed / all", "Median query method time (seconds)"],
        [(kind, method, f"{v['completed_count']} / {v['population_count']}", v["median_of_completed_query_method_medians_seconds"]) for kind, group in efficiency["groups"].items() for method, v in group.items()]) + "\n\n"
    timing += "Per-query build/solve times, endpoint optimization counts, output workloads and raw repetitions are retained. Computing all certified intervals and finding a reachable set are different tasks; these times do not establish equivalent-task speed superiority. Memory is the Python allocation high-water during warmup and excludes native solver allocations. The experiment reads a local cache and does not measure online acquisition or claim network collection savings. Small timing differences are not asserted statistically significant. Actual runtime: " + json.dumps(stats["runtime"]) + ".\n"
    return {"01_CHECKPOINT_1C_REPORT.md": overall, "03_METHOD_SPEC_AND_FAIR_INPUTS.md": methods,
            "04_CONTROLLED_EXPERIMENT_RESULTS.md": controlled, "05_REAL_PILOT_COMPARISONS.md": pilots,
            "06_ABLATIONS_AND_SEMANTIC_EQUIVALENCE.md": ablation, "07_EFFICIENCY_AND_SCOPE.md": timing}


def generate_reports(tree, results, output, public_output=None):
    tree, results, output = Path(tree), Path(results), Path(output)
    index = read(results / "RESULTS_INDEX.json")
    records = []
    for row in index["method_results_index"]:
        folder = results / row["path"]
        records.append({"index": row, "methods": read(folder / "METHOD_RESULTS.json"), "evaluation": read(folder / "EVALUATION.json"),
                        "ablations": read(folder / "ABLATIONS.json"), "efficiency": read(folder / "EFFICIENCY.json")})
    reference_path = tree / "private/REAL_REFERENCE_EVALUATION.json"
    references = {q["name"]: q for q in read(reference_path).get("queries", [])} if reference_path.exists() else {}
    meta_path = tree / "private/REAL_EXPERIMENT_INPUTS.json"
    metadata = {q["sample_id"]: q for q in read(meta_path)} if meta_path.exists() else {}
    stats = {"schema_version": REPORT_VERSION, "batch_status": index["status"], "runtime": index["runtime"],
             "results_index_sha256": hashlib.sha256((results / "RESULTS_INDEX.json").read_bytes()).hexdigest(),
             "controlled": controlled_summary([r for r in records if r["index"]["kind"] == "controlled"]),
             "real": real_summary([r for r in records if r["index"]["kind"] == "real"], references, metadata),
             "efficiency": efficiency_summary(records), "external_acceptance": "PENDING_REVIEW", "checkpoint": "CHECKPOINT_1C_REACHED",
             "new_research_platform_requests": index.get("research_platform_requests"), "new_research_cost": index.get("new_research_cost"),
             "public_safety": "ALLOWLISTED_AGGREGATES_NO_REAL_ADDRESSES_EVENTS_REFERENCE_ROWS_OR_ACCOUNT_LEDGER"}
    documents = markdown_reports(stats)
    # No privacy regex substitution: unexpected identifier leakage blocks the
    # report instead of silently redacting parts of an unsafe object.
    serialized = json.dumps(stats, ensure_ascii=False) + "\n".join(documents.values())
    if re.search(r"0x[0-9a-fA-F]{40,64}", serialized):
        raise ValueError("Unexpected real address/transaction identity in aggregate report")
    destinations = [output] + ([Path(public_output)] if public_output is not None else [])
    for destination in destinations:
        destination.mkdir(parents=True, exist_ok=True)
        write(destination / "STATISTICS.json", stats)
        write(destination / "STATISTICAL_DICTIONARY.json", DICTIONARY)
        for name, text in documents.items():
            (destination / name).write_text(text, encoding="utf-8")
    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tree", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--public-output", type=Path)
    args = parser.parse_args()
    stats = generate_reports(args.tree, args.results, args.output, args.public_output)
    print(json.dumps({"status": "GENERATED", "controlled": stats["controlled"]["query_count"], "real": stats["real"]["query_count"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
