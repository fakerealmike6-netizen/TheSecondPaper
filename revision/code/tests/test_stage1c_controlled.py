"""Stage1C controls: independent Oracle and construction checks."""
from copy import deepcopy
from fractions import Fraction
import hashlib
import importlib.util
import inspect
import json
from pathlib import Path
import sys

import unittest
import tempfile
from contextlib import contextmanager

@contextmanager
def raises(error, match=None):
    with unittest.TestCase().assertRaisesRegex(error, match or ".*"):
        yield

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from stage1c_controlled import FAMILIES, Bytes, build_sample, generate_suite
from stage1c_oracle import oracle_intervals, oracle_weighted_interval, tiny_enumeration_check, enumerate_integer_allocations, semantic_equivalence_report


def test_60_constructions_preserve_hidden_source_and_oracle_coverage(family, index):
    # The primary checker is used only AFTER independently generating hidden.
    from lp_model import build_model
    graph, hidden = build_sample(family, index)
    model = build_model(graph)
    vector = model.lift_hidden_allocation_for_audit(hidden["event_source_amounts_raw"])
    assert model.audit_vector(vector)["exact_feasible"]
    oracle = oracle_intervals(graph)
    assert oracle["status"] == "OPTIMAL_EXACT_INTEGER_NETWORK"
    for eid, interval in oracle["events"].items():
        value = Fraction(hidden["event_source_amounts_raw"][eid])
        assert Fraction(interval["lower_raw"]) <= value <= Fraction(interval["upper_raw"])
    assert len(graph["events"]) <= 40
    assert len(graph["target_accounts"]) <= 3


def test_twelve_independent_tiny_enumerations(family, index):
    graph, _ = build_sample(family, index)
    checked = tiny_enumeration_check(graph)
    assert checked["status"] == "PASS"
    assert checked["allocation_count"] >= 1
    assert checked["objectives_checked"] >= 2


def _manifest_check(tmp_path):
    manifest = generate_suite(tmp_path)
    assert manifest == generate_suite(tmp_path)
    assert manifest["sample_count"] == 60
    assert sum(s["tiny_enumeration_required"] for s in manifest["samples"]) == 12
    assert sum(s["zero_hop"] for s in manifest["samples"]) == 2
    for sample in manifest["samples"]:
        for kind in ("observed", "hidden"):
            p = tmp_path / sample[kind + "_path"]
            assert hashlib.sha256(p.read_bytes()).hexdigest() == sample[kind + "_sha256"]
    (tmp_path / manifest["samples"][0]["observed_path"]).write_text("changed", encoding="utf-8")
    with raises(ValueError, match="Frozen synthetic input differs"):
        generate_suite(tmp_path)


def test_seed_mapping_exact_documented_bytes():
    seed = "TheSecondPaper/Stage1C/controlled-v1/normal_mixing/0"
    raw = hashlib.sha256(hashlib.sha256(seed.encode()).digest() + (0).to_bytes(4, "big")).digest()
    assert Bytes(seed).draw(2, 8) == 2 + int.from_bytes(raw[:8], "big") % 7


def test_no_hidden_or_primary_dependency_in_oracle_ast():
    import ast
    import stage1c_oracle
    tree = ast.parse(inspect.getsource(stage1c_oracle))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(n.name for n in node.names)
        if isinstance(node, ast.ImportFrom):
            imports.append(node.module)
    assert set(imports) <= {"__future__", "collections", "copy", "itertools"}
    forbidden = {"open", "read_text", "read_bytes", "build_model", "solve_interval", "linprog"}
    assert not [n for n in ast.walk(tree) if isinstance(n, ast.Name) and n.id in forbidden]


def test_hidden_change_cannot_change_observed_or_oracle():
    graph, hidden = build_sample("normal_mixing", 0)
    before = oracle_intervals(graph)
    hidden["event_source_amounts_raw"] = {"fiction": "999999"}
    assert oracle_intervals(graph) == before


def test_zero_hop_and_no_reachable_service_are_distinct():
    for index in (0, 1):
        graph, _ = build_sample(FAMILIES[-1], index)
        assert graph["zero_hop"]
        result = oracle_intervals(graph)
        assert int(result["addresses"]["T|ETH"]["lower_raw"]) > 0
        assert result["addresses"]["U|ETH"]["upper_raw"] == "0"
    for index in (2, 3):
        graph, _ = build_sample(FAMILIES[-1], index)
        assert oracle_intervals(graph)["joint_by_asset"]["ETH"]["upper_raw"] == "0"


def test_native_gas_never_consumes_token_source():
    graph, _ = build_sample("gas_background_and_anchors", 8)
    result = oracle_intervals(graph)
    assert result["events"]["gas_success"]["upper_raw"] == "0"
    assert result["events"]["gas_failed"]["upper_raw"] == "0"


def test_gross_refund_cannot_circularly_fund_itself():
    graph, _ = build_sample("canonical_weth_1to1", 1)
    wrap = next(e for e in graph["events"] if e["kind"] == "conversion")
    wrap["gross_raw"] = "99"
    wrap["refund_raw"] = "96"
    wrap["output_raw"] = "3"
    with raises(ValueError, match="pre-refund"):
        oracle_intervals(graph)


def test_non_one_to_one_protocol_is_explicitly_unsupported():
    graph, _ = build_sample("canonical_weth_1to1", 0)
    graph["events"][1]["output_raw"] = "1"
    with raises(ValueError, match="1:1"):
        oracle_intervals(graph)


def test_shared_capacity_independent_upper_sum_is_not_joint():
    graph, _ = build_sample("split_merge_shared_targets", 0)
    result = oracle_intervals(graph)
    summed = sum(int(v["upper_raw"]) for v in result["addresses"].values())
    joint = int(result["joint_by_asset"]["ETH"]["upper_raw"])
    assert summed == 2 * joint and joint == 2


def test_weighted_protocol_directions_agree_with_enumeration():
    graph, _ = build_sample("canonical_weth_1to1", 1)
    allocations = enumerate_integer_allocations(graph)
    for weights in ({"wrap:output": 1}, {"weth_split:output0": 1, "weth_split:output1": 1},
                    {"weth_split:output0": 2, "weth_split:output1": 3},
                    {"wrap:refund": 3, "weth_split:output0": -2, "weth_split:output1": 1}):
        values = [sum(w * a[e] for e, w in weights.items()) for a in allocations]
        found = oracle_weighted_interval(graph, weights)
        assert int(found["lower_raw"]) == min(values)
        assert int(found["upper_raw"]) == max(values)


def test_semantic_full_tiny_boundary_bijection_and_weights():
    for index in (0, 1):
        graph, _ = build_sample("canonical_weth_1to1", index)
        report = semantic_equivalence_report(graph)
        assert report["status"] == "PASS"
        boundary = report["full_tiny_boundary_relation"]
        assert boundary["raw_boundary_tuples"] == boundary["folded_boundary_tuples"]
        assert boundary["raw_boundary_count"] >= 5
        assert len(report["whole_query_weighted_directions"]) >= 2


class ControlledConstructionTests(unittest.TestCase):
    def test_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            _manifest_check(Path(temporary))


def _case(function, *arguments):
    def method(self):
        function(*arguments)
    return method


for _family in FAMILIES:
    for _index in range(10):
        setattr(ControlledConstructionTests, f"test_construct_{_family}_{_index:02d}",
                _case(test_60_constructions_preserve_hidden_source_and_oracle_coverage, _family, _index))
    for _index in (0, 1):
        setattr(ControlledConstructionTests, f"test_enumerate_{_family}_{_index:02d}",
                _case(test_twelve_independent_tiny_enumerations, _family, _index))
for _name, _function in list(globals().items()):
    if _name.startswith("test_") and callable(_function) and _function.__code__.co_argcount == 0:
        setattr(ControlledConstructionTests, _name, _case(_function))

if __name__ == "__main__":
    unittest.main()
