"""Frozen, outcome-independent CONTROLLED_V1 construction and hidden replay.

The generator constructs legal observed totals and a separately randomized
source/normal decomposition by exact integer chronological accounting. Neither
the primary method nor the Oracle participates in construction or selection.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

GENERATOR_VERSION = "CONTROLLED_V1_GENERATOR_1.0.0"
FAMILIES = ("normal_mixing", "split_merge_shared_targets", "repeated_entries_timed_returns",
            "gas_background_and_anchors", "canonical_weth_1to1", "missing_information_and_boundary_controls")


def _json(value):
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _hash(data):
    return hashlib.sha256(data).hexdigest()


class Bytes:
    """Each draw hashes seed_digest || unsigned 4-byte big-endian counter.

    First eight digest bytes are unsigned big-endian, mapped modulo range.
    Counter begins at zero. Modulo bias is intentional and frozen; no PRNG,
    rejection sampling, external entropy, or output-based retries are used.
    """
    def __init__(self, seed):
        self.digest = hashlib.sha256(seed.encode("utf-8")).digest()
        self.counter = 0

    def draw(self, low, high):
        value = hashlib.sha256(self.digest + self.counter.to_bytes(4, "big")).digest()
        self.counter += 1
        return low + int.from_bytes(value[:8], "big") % (high - low + 1)


class Builder:
    def __init__(self, family, index, initial):
        self.seed_text = f"TheSecondPaper/Stage1C/controlled-v1/{family}/{index}"
        self.random = Bytes(self.seed_text)
        self.family, self.index = family, index
        self.sample_id = f"controlled-v1/{family}/{index:02d}"
        self.initial = {k: int(v) for k, v in initial.items()}
        self.actual = dict(self.initial)
        self.source = {k: 0 for k in initial}
        self.events, self.hidden = [], {}

    def key(self, account, asset):
        key = account + "|" + asset
        if key not in self.actual:
            self.actual[key], self.source[key], self.initial[key] = 0, 0, 0
        return key

    def amount(self, account, asset="ETH"):
        return self.actual[self.key(account, asset)]

    def take(self, account, asset, amount):
        key = self.key(account, asset)
        b, s = self.actual[key], self.source[key]
        if not 0 <= amount <= b:
            raise ValueError("Generator attempted physical overspend")
        share = self.random.draw(max(0, s - (b - amount)), min(s, amount))
        self.actual[key] -= amount
        self.source[key] -= share
        return share

    def give(self, account, asset, amount, share):
        key = self.key(account, asset)
        self.actual[key] += amount
        self.source[key] += share

    def event(self, kind, eid, amount, sender=None, to=None, asset="ETH", **extra):
        e = {"id": eid, "kind": kind, "order": len(self.events) + 1, "asset": asset, "amount_raw": str(amount), **extra}
        if sender is not None:
            e["from"] = sender
        if to is not None:
            e["to"] = to
        if kind in ("transfer", "gas", "boundary_outflow"):
            share = self.take(sender, asset, amount)
        else:
            share = amount if kind == "seed" else 0
        if kind in ("seed", "normal_incoming", "transfer"):
            self.give(to, asset, amount, share)
        self.hidden[eid] = str(share)
        self.events.append(e)
        return e

    def transfer(self, eid, amount, sender, to, asset="ETH"):
        return self.event("transfer", eid, amount, sender, to, asset)

    def conversion(self, gross, refund):
        share = self.take("A", "ETH", gross)
        net = gross - refund
        refund_share = self.random.draw(max(0, share - net), min(share, refund))
        self.give("A", "ETH", refund, refund_share)
        self.give("A", "WETH", net, share - refund_share)
        eid = "wrap"
        self.events.append({"id": eid, "kind": "conversion", "order": len(self.events) + 1,
                            "from": "A", "to": "A", "asset": "ETH", "output_asset": "WETH",
                            "gross_raw": str(gross), "refund_raw": str(refund), "output_raw": str(net),
                            "certified_semantics": "SYNTHETIC_CONTROLLED",
                            "protocol": "Canonical_WETH9_deposit" if not refund else "SYNTHETIC_GROSS_REFUND_ENVELOPE_WITH_CANONICAL_NET_WETH_DEPOSIT",
                            "refund_semantics_note": "Refund belongs to synthetic pre-deposit envelope, not to WETH9.deposit itself."})
        self.hidden.update({eid + ":input": str(share), eid + ":refund": str(refund_share),
                            eid + ":net": str(share - refund_share), eid + ":output": str(share - refund_share)})

    def split(self, eid, amounts, sender="A", asset="WETH"):
        gross = sum(amounts)
        source = self.take(sender, asset, gross)
        self.hidden[eid + ":input"] = str(source)
        remaining, normal = source, gross - source
        outputs = []
        for i, amount in enumerate(amounts):
            target = ("T", "U", "V")[i]
            share = self.random.draw(max(0, amount - normal), min(remaining, amount))
            normal -= amount - share
            remaining -= share
            self.give(target, asset, amount, share)
            self.hidden[eid + f":output{i}"] = str(share)
            outputs.append({"to": target, "amount_raw": str(amount)})
        self.events.append({"id": eid, "kind": "multioutput", "order": len(self.events) + 1,
                            "from": sender, "asset": asset, "gross_raw": str(gross), "outputs": outputs,
                            "certified_semantics": "SYNTHETIC_CONTROLLED"})

    def finish(self, targets, missing=(), notes=()):
        groups = {}
        for e in self.events:
            if e.get("to") in targets and e["kind"] in ("seed", "transfer"):
                groups.setdefault(e["to"] + "|" + e["asset"], []).append(e["id"])
            if e["kind"] == "multioutput":
                for i, out in enumerate(e["outputs"]):
                    if out["to"] in targets:
                        groups.setdefault(out["to"] + "|" + e["asset"], []).append(e["id"] + f":output{i}")
        initial = {k: None if k in missing else str(v) for k, v in sorted(self.initial.items())}
        graph = {"schema_version": "stage1c-controlled-observed-1.0", "scenario_id": self.sample_id,
                 "query_id": self.sample_id, "incident_id": "SYNTHETIC_CONTROLLED_V1", "source_layer": "SYNTHETIC",
                 "family": self.family, "index": self.index, "generator_version": GENERATOR_VERSION,
                 "provenance": "ORIGINAL_SYNTHETIC_STAGE1C_NOT_EXTERNAL_BLIND_GT",
                 "scope": "FROZEN_ORDERED_OBSERVED_GRAPH", "label_version": "CONTROLLED_V1_SERVICES_1",
                 "target_accounts": sorted(targets), "initial_balances": initial,
                 "events": self.events, "objective_groups": groups,
                 "sampling": {"chain_id": 1, "strict_time_order": True, "first_identified_service_stops_branch": True,
                              "lp_path_hop_limit": None, "lp_fund_age_limit": None},
                 "construction_notes": list(notes), "zero_hop": any(e["kind"] == "seed" and e["to"] in targets for e in self.events)}
        hidden = {"sample_id": self.sample_id, "scope": "EVALUATION_ONLY_NOT_METHOD_INPUT",
                  "event_source_amounts_raw": self.hidden,
                  "complete_initial_actual_balances_raw": {k: str(v) for k, v in sorted(self.initial.items())},
                  "terminal_source_balances_raw": {k: str(v) for k, v in sorted(self.source.items())},
                  "construction": "Exact chronological feasible source/normal decomposition; deterministic bounded random integer shares; no LP or Oracle called."}
        if len(self.events) > 40 or len({k.split("|", 1)[0] for k in self.initial}) > 16 or len(targets) > 3:
            raise ValueError("Construction exceeds frozen policy bounds")
        return graph, hidden


def build_sample(family, index):
    if family not in FAMILIES or not 0 <= index < 10:
        raise ValueError("Unknown frozen sample identity")
    seed_text = f"TheSecondPaper/Stage1C/controlled-v1/{family}/{index}"
    rng = Bytes(seed_text)
    q = 2 if index < 2 else rng.draw(3, 8)
    normal = 2 if index < 2 else rng.draw(2, 7)
    if family == "normal_mixing":
        b = Builder(family, index, {"A|ETH": normal})
        b.event("seed", "seed", q, to="A")
        # Even indices force positive lower; odd indices retain lower zero.
        amount = q + normal - 1 if index % 2 == 0 else min(normal, q)
        b.transfer("enter", amount, "A", "T")
        return b.finish({"T"}, notes=["Parity-fixed payout construction gives both forced-source and zero-lower mixing controls."])
    if family == "split_merge_shared_targets":
        b = Builder(family, index, {"A|ETH": q})
        b.event("seed", "seed", q, to="A")
        for eid, amount, sender, to in (("a_b", q, "A", "B"), ("a_c", q, "A", "C"), ("b_d", q, "B", "D"), ("c_d", q, "C", "D")):
            b.transfer(eid, amount, sender, to)
        if index % 2:
            b.split("shared_split", [q, q], "D", "ETH")
        else:
            b.transfer("d_t", q, "D", "T")
            b.transfer("d_u", q, "D", "U")
        return b.finish({"T", "U"}, notes=["Both target marginal maxima can use the same source; one joint physical allocation cannot."])
    if family == "repeated_entries_timed_returns":
        b = Builder(family, index, {"A|ETH": normal, "B|ETH": 1})
        b.transfer("before_seed", 1, "A", "T")
        b.event("seed", "seed", q, to="A")
        b.transfer("entry_first", 1, "A", "T")
        b.transfer("to_return_account", b.amount("A") - 1, "A", "B")
        b.transfer("timed_return", b.amount("B") - 1, "B", "A")
        b.transfer("entry_second", b.amount("A"), "A", "T")
        b.transfer("other_target", b.amount("B"), "B", "U")
        return b.finish({"T", "U"}, notes=["One earlier entry precedes source injection; returns are actual forward-time operations, not a timeless cycle."])
    if family == "gas_background_and_anchors":
        asset = "ETH" if index < 5 else "TOK"
        initial = {"A|" + asset: normal}
        if asset != "ETH":
            initial["A|ETH"] = 3
        b = Builder(family, index, initial)
        b.event("seed", "seed", q, to="A", asset=asset)
        b.event("gas", "gas_success", 1, sender="A", transaction_status="SUCCESS")
        b.event("normal_incoming", "background_in", 1, to="A", asset=asset)
        e = b.event("gas", "gas_failed", 1, sender="A", transaction_status="FAILED")
        e["balance_anchors_after"] = {k: str(v) for k, v in b.actual.items() if k.startswith("A|")}
        b.transfer("a_b", max(1, b.amount("A", asset) // 2), "A", "B", asset)
        b.transfer("b_t", b.amount("B", asset), "B", "T", asset)
        b.transfer("a_u", b.amount("A", asset), "A", "U", asset)
        return b.finish({"T", "U"}, notes=["Actual successful and failed native fees are distinct; latter five instances seed TOK and cannot spend TOK source as ETH gas."])
    if family == "canonical_weth_1to1":
        b = Builder(family, index, {"A|ETH": normal})
        b.event("seed", "seed", q, to="A")
        refund = 1 if index in (1, 5, 7, 9) else 0
        b.conversion(q + normal, refund)
        amount = b.amount("A", "WETH")
        if index % 2:
            b.split("weth_split", [amount // 2, amount - amount // 2])
            targets = {"T", "U"}
        else:
            b.transfer("weth_enter", amount, "A", "T", "WETH")
            targets = {"T"}
        return b.finish(targets, notes=["Canonical net deposit is 1:1; any refund is an explicitly synthetic gross/refund envelope, never claimed to be a WETH9 native refund."])
    if index < 2:
        b = Builder(family, index, {})
        b.event("seed", "seed", q, to="T")
        b.event("normal_incoming", "normal_only", normal, to="A")
        b.transfer("unrelated_service", normal, "A", "U")
        return b.finish({"T", "U"}, notes=["Legal zero-hop seed receipt is a service objective."])
    if index < 4:
        b = Builder(family, index, {"A|ETH": normal})
        b.transfer("pre_seed_service", normal, "A", "T")
        b.event("seed", "seed", q, to="A")
        b.transfer("no_service_path", q, "A", "B")
        b.event("boundary_outflow", "terminal_boundary", q, sender="B")
        return b.finish({"T"}, notes=["The sole service receipt precedes the seed; all source leaves a terminal boundary, so no reachable service."])
    b = Builder(family, index, {"A|ETH": normal, "outside|ETH": normal})
    b.event("seed", "seed", q, to="A")
    if index >= 8:
        b.transfer("to_outside", q, "A", "outside")
        b.transfer("unknown_return", q, "outside", "A")
    e = b.transfer("enter_missing", q + normal - 1, "A", "T")
    if index % 2:
        e["balance_anchors_after"] = {"A|ETH": "1"}
    return b.finish({"T"}, missing={"A|ETH", "outside|ETH"}, notes=["Missing observed initial balances retain zero initial source; hidden complete actual balances are evaluation-only."])


def generate_suite(root):
    root = Path(root)
    base = root / "controlled_v1"
    base.mkdir(parents=True, exist_ok=True)

    def save(path, value):
        data = _json(value)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.read_bytes() != data:
            raise ValueError("Frozen synthetic input differs; choose a new version/output directory: " + str(path))
        if not path.exists():
            path.write_bytes(data)
        return _hash(data)

    design = {"generator_version": GENERATOR_VERSION, "families": list(FAMILIES), "indices": list(range(10)),
              "sample_count": 60, "seed_template": "TheSecondPaper/Stage1C/controlled-v1/<family>/<index>",
              "byte_mapping": Bytes.__doc__.strip(), "parameter_rule": "q=normal=2 for indices0,1; otherwise q=3+first_draw%6, normal=2+second_draw%6; structural alternatives fixed by family/index.",
              "tiny_checks": "indices 0 and 1 of every family; direct chronological complete integer enumeration with network integrality linking continuous extrema",
              "selection": "ALL_60_FIXED_BEFORE_METHOD_BATCH_NO_RESULT_BASED_FILTERING", "max_accounts": 16, "max_operations": 40, "max_targets": 3,
              "zero_hop_indices": ["missing_information_and_boundary_controls/0", "missing_information_and_boundary_controls/1"],
              "no_reachable_service_indices": ["missing_information_and_boundary_controls/2", "missing_information_and_boundary_controls/3"],
              "oracle_separation": "Observed, hidden, and Oracle result files are separate; generator does not call Oracle or primary method."}
    design_hash = save(base / "PARAMETERS_AND_DESIGN.json", design)
    samples = []
    for family in FAMILIES:
        for index in range(10):
            graph, hidden = build_sample(family, index)
            folder = base / family / f"{index:02d}"
            op, hp = folder / "observed.json", folder / "hidden.json"
            oh, hh = save(op, graph), save(hp, hidden)
            samples.append({"sample_id": graph["scenario_id"], "family": family, "index": index,
                            "observed_path": op.relative_to(root).as_posix(), "hidden_path": hp.relative_to(root).as_posix(),
                            "observed_sha256": oh, "hidden_sha256": hh, "seed_text": f"TheSecondPaper/Stage1C/controlled-v1/{family}/{index}",
                            "seed_sha256": _hash(f"TheSecondPaper/Stage1C/controlled-v1/{family}/{index}".encode()),
                            "tiny_enumeration_required": index < 2, "operations": len(graph["events"]),
                            "accounts": len({k.split("|", 1)[0] for k in graph["initial_balances"]}),
                            "target_count": len(graph["target_accounts"]), "zero_hop": graph["zero_hop"]})
    manifest = {"schema_version": "stage1c-controlled-manifest-1.0", "suite": "CONTROLLED_V1", "generator_version": GENERATOR_VERSION,
                "generator_sha256": _hash(Path(__file__).read_bytes()), "parameters_sha256": design_hash,
                "sample_count": len(samples), "samples": samples, "status": "FROZEN_BEFORE_METHOD_RESULTS",
                "research_scope": "DEVELOPMENT_FIRST_VALIDATION_NOT_EXTERNAL_BLIND_GROUND_TRUTH"}
    save(base / "MANIFEST.json", manifest)
    return manifest


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    print(json.dumps(generate_suite(parser.parse_args().root), indent=2))
