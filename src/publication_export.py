"""Explicit allowlist export only. This script never invokes git or networking.

An authorized stage task is required each time. No scheduler or next-stage
behavior exists. Suspected sensitive content is reported by rule and path only;
it is never printed, hashed separately, or automatically redacted.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil

SOURCE_NAMES = (
    "archive_safety.py", "budget.py", "cache_probe.py", "collector.py",
    "collector_inputs.py", "collector_replay.py", "label_queue.py",
    "labels_policy.py", "lp_fixture_factory.py", "lp_model.py", "lp_oracle.py",
    "lp_run.py", "lp_test_receipt.py", "network.py", "provider_dune.py",
    "provider_etherscan.py", "provider_receipts.py", "reference_core.py",
    "reference_recompute.py", "reference_replay.py", "run_tests.py",
    "weth_component.py", "publication_export.py",
)
TEST_NAMES = (
    "test_archive.py", "test_budget.py", "test_collector.py", "test_labels.py",
    "test_lp.py", "test_provider_dune.py", "test_weth.py", "test_publication_export.py",
)
EXCLUSIONS = {
    "src/labels_build.py": "Local orchestration embeds historical snapshot/run paths; reusable queue factored into label_queue.py.",
    "src/labels_import.py": "Local ingestion of full provider/source observations is not needed for public tests.",
    "src/lp_record_correction.py": "One-off preservation utility; synthetic correction history is described in public report.",
    "private/": "Account scopes, budgets, credentials-adjacent metadata and full reference review subsets.",
    "raw/": "Provider payloads, RPC caches, third-party labels, browser/account evidence.",
    "derived/": "Default deny: actual address/reference/event tables and full model outputs; public aggregate report generated separately.",
    "logs/": "Local request metadata, internal paths and provider/account records.",
    "manifests/": "Default deny: local input/label/source manifests can expose excluded paths and large-source metadata.",
    "configs/STAGE1B_POLICY.json": "Full policy is replaced by an explicit public subset without local publication paths or historical package metadata.",
    "bootstrap and prior stages": "No historical packages, attachments, full datasets, one-off recovery archives or prior Git history.",
}

PUBLIC_REPORT = """# Stage1B public review snapshot

This is a research prototype with external acceptance `PENDING_REVIEW`.
Overall real-data work is `PARTIAL`: real probe completion has not been claimed.
The two authorized S2 scopes are Atomic Wallet and Harmony. Access failures,
budget gates, sparse inherited cache replay and observed-data conditional
models must be distinguished from completed provider interval collection.
The local review package contains the non-public row-level evidence.

On the inherited incomplete fixed graph, four target groups and thirteen entry
objectives have exact conditional certificates. These are conditional results
for observed sparse cached events, with missing balance/gas/flow coverage; they
are not complete-chain source amount guarantees or completed real probes.

Implemented here are versioned Dune-preferred explicit ownership resolution,
a deterministic one-off targeted queue, an event-state collector, injected
Etherscan/Dune provider adapters, a persistent shared budget ledger, and a sparse
time-expanded continuous LP with exact rational certificates and an independent
rational-polytope Oracle. Unknown intermediaries remain expandable, one raw
unit is admissible, and identified service entries stop before platform ledger
expansion. Reference neighbors are not accepted by the collector API.

Twelve fully synthetic LP scenarios cover original normal funds, splitting,
merging, repeated/multiple targets, timing, gas, WETH, fixed-ratio conversion,
refunds, missing-balance relaxation and a deliberately broken shared-capacity
negative control. Their hidden allocations and Oracle definitions are separate
from solver inputs. A discovered fractional-refund Oracle omission was corrected
using the pre-refund balance constraint; no failed scenario was removed.

The real WETH evidence subset matches one 150 ETH input and its Deposit log,
but historical runtime and execution-context retrieval were blocked. Component
certification is incomplete; the real conversion remains disabled. Synthetic
WETH tests do not certify mainnet bytecode or the surrounding bridge operation.

Offline test results apply to synthetic fixtures. They do not establish real
provider access, complete address histories, unbiased label coverage, actual
source attribution truth, or readiness for full-sample experiments. Label-based
changes in the reference pool are not algorithmic gains. No paper denominator
or final Go/Revise/Stop decision is frozen here.

The current stage ends at `CHECKPOINT_1B_REACHED`; a later explicit task is
required before further collection or experiments. The local final review
record supplies final counts, remaining blockers and publication receipts.
"""

README = """# TheSecondPaper: Stage1B research prototype

Public source, synthetic controls, and a limited research-review snapshot.
External acceptance is **PENDING_REVIEW**. Real probes remain **PARTIAL** until
their full declared intervals are evidenced; mock tests and sparse cache replay
are not provider validation. See [review status](00_REVIEW_INDEX.md).

Use Python 3.11 or newer. The reviewed environment used Python 3.14.3,
NumPy 2.4.2, SciPy 1.17.1 and its bundled HiGHS solver.

```text
python -m venv .venv
python -m pip install -r requirements.txt
python src/run_tests.py
python src/lp_run.py --fixtures fixtures/controlled --output derived/replayed_lp
python src/collector_replay.py
```

Activate the project environment using the normal command for your shell before
installing dependencies. Tests remove credential variables and block outbound
sockets. No key, account, historical project tree, real blockchain cache or
Windows drive is required. All bundled fixtures are synthetic.

`lp_model.py` consumes event graphs, not hidden allocations or Oracle files.
`lp_oracle.py` independently enumerates rational vertices. The collector accepts
an exact seed, outer scope, identity resolver and provider callbacks; it does
not load reference targets as neighbors. Network adapters require separately
authorized current entitlements and the same persistent shared budget.

Private-data reconstruction tools accept explicit input paths. To reproduce
real evidence, obtain the original data under its own terms, verify the stated
source versions/hashes in the local review, and supply the necessary canonical
events, seed members and label snapshot. Full third-party labels, reference
tables and RPC/account caches are deliberately absent from this repository.
No claim dependent on those missing data can be independently reproduced from
the public package alone.

Re-exporting is an explicit local action, with no publication side effect:

```text
python src/publication_export.py --source-work SOURCE_STAGE_DIRECTORY --destination NEW_EXPORT_DIRECTORY --stage-task Stage1B
```

Read [NOTICE](NOTICE.md) before reuse. Original code licensing awaits the
researcher's selection; public visibility does not grant an unstated license.
"""

NOTICE = """# Sources, dependencies and licensing

The source in this export was authored within the current clean-room project.
General finite-reference utilities were reused from that project's accepted R1
implementation. No MFTracer or AMLGuard algorithm source is imported or bundled.
Their outputs are not amount truth. DSU-style component ideas are acknowledged
as prior work, not claimed as a new general concept.

Original-code license: awaiting the researcher's choice. No blanket MIT or other
license is asserted over original or third-party content by this publication.
Synthetic fixtures were generated for this stage. Full third-party datasets,
label stores, provider responses and account information are not redistributed.

Dependencies retain their own licenses: Python (PSF), NumPy (BSD 3-Clause),
SciPy (BSD 3-Clause), and HiGHS (MIT). Consult the installed distribution's license
and third-party notices for exact version-specific terms and bundled libraries.
No dependency binaries are included.

Provider specifications remain the providers' documentation: Etherscan API V2,
Dune Ethereum transaction/trace and ERC20 schemas, and Ethereum JSON-RPC receipt
semantics. Schema-source links are in `configs/DUNE_ADAPTER_SCHEMA.md`.
Canonical WETH9 behavior is used as a limited modeling rule; source/runtime
equivalence at a historical block requires separate evidence.
"""

def sha256(data):
    return hashlib.sha256(data).hexdigest()

def scan_text(path, data):
    findings = []
    try:
        value = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return [{"path": path, "rule": "NON_UTF8_OR_BINARY_NOT_ALLOWLISTED"}]
    rules = {
        "PRIVATE_KEY_MATERIAL": r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
        "KNOWN_TOKEN_SHAPE": r"(?:gh[pousr]_[A-Za-z0-9]{24,}|github_pat_[A-Za-z0-9_]{30,}|AKIA[A-Z0-9]{16}|sk-[A-Za-z0-9_-]{24,})",
        "URL_USERINFO": r"https?://[^/\s:@]+:[^/\s@]+@",
        "CREDENTIAL_QUERY_VALUE": r"[?&](?:apikey|api_key|access_token|token|key)=[A-Za-z0-9_-]{12,}",
        "ABSOLUTE_USER_DIRECTORY": r"(?i)(?:[A-Z]:[\\/](?:Users|Documents and Settings)[\\/]|/(?:Users|home)/[A-Za-z0-9_.-]+/)",
        "LOCAL_DRIVE_PATH": r"(?i)(?<![A-Za-z0-9_])[A-Z]:[\\/][^\s'\"]+",
        "LITERAL_SECRET_ASSIGNMENT": r"(?i)(?:api[_-]?key|access[_-]?token|secret|password)['\"]?\s*[:=]\s*['\"][A-Za-z0-9_+/=-]{20,}['\"]",
    }
    for rule, pattern in rules.items():
        if re.search(pattern, value):
            # This exact invalid archive path is synthetic test data, not a user
            # directory. Keep the meaningful rejection test unchanged.
            if path == "tests/test_archive.py" and rule == "LOCAL_DRIVE_PATH" and re.findall(pattern, value) == ["C" + ":/x"]:
                continue
            findings.append({"path": path, "rule": rule})
    return findings

def public_policy(work):
    full = json.loads((work / "configs" / "STAGE1B_POLICY.json").read_text(encoding="utf-8-sig"))
    keys = ("schema_version", "checkpoint", "daily_metasleuth_schedule_enabled", "label_policy", "budgets", "resource_limits", "sampling_rules", "query_pilots", "weth_component", "prototype")
    subset = {key: full[key] for key in keys}
    subset["publication_scope"] = "PUBLIC_POLICY_SUBSET; ceilings are task limits, not current account balances or executable entitlement"
    subset["external_acceptance_status"] = "PENDING_REVIEW"
    return (json.dumps(subset, indent=2, sort_keys=True) + "\n").encode()

def export(work, destination, stage_task, aggregate_report=None):
    if stage_task != "Stage1B":
        raise ValueError("Only the explicitly requested Stage1B task is supported")
    work, destination = Path(work).resolve(), Path(destination).resolve()
    if destination == work or destination.is_relative_to(work):
        raise ValueError("Export must be an independent reviewed destination")
    selected = {}
    for folder, names in (("src", SOURCE_NAMES), ("tests", TEST_NAMES)):
        for name in names:
            file = work / folder / name
            if file.is_symlink():
                raise ValueError("Source symlink is not allowed")
            selected[f"{folder}/{name}"] = file.read_bytes()
    for directory in ("fixtures/controlled", "fixtures/collector"):
        for file in sorted((work / directory).rglob("*.json")):
            if file.is_symlink() or not file.resolve().is_relative_to(work):
                raise ValueError("Fixture path escape")
            selected[file.relative_to(work).as_posix()] = file.read_bytes()
    selected["configs/DUNE_ADAPTER_SCHEMA.md"] = (work / "configs/DUNE_ADAPTER_SCHEMA.md").read_bytes()
    selected["configs/STAGE1B_POLICY.json"] = public_policy(work)
    selected.update({
        "README.md": README.encode(), "NOTICE.md": NOTICE.encode(),
        "00_REVIEW_INDEX.md": PUBLIC_REPORT.encode(),
        "requirements.txt": b"numpy==2.4.2\nscipy==1.17.1\n",
        ".gitignore": b".venv/\n__pycache__/\n.test_tmp/\n*.pyc\nraw/\nprivate/\nlogs/\nderived/\n*.sqlite\n.env*\n09_TEST_RESULTS*\n",
    })
    if aggregate_report:
        report = Path(aggregate_report).resolve()
        if not report.is_relative_to(work / "publication") or report.suffix != ".md" or report.is_symlink():
            raise ValueError("Final public aggregate report must be an explicit Markdown file in source-work/publication")
        selected["00_REVIEW_INDEX.md"] = report.read_bytes()
    if sum(map(len, selected.values())) > 20 * 1024 * 1024 or any(len(v) > 2 * 1024 * 1024 for v in selected.values()):
        raise ValueError("Unexpected public export size")
    findings = [hit for path, data in selected.items() for hit in scan_text(path, data)]
    if findings:
        raise ValueError(json.dumps({"export_blocked_sensitive_pattern_findings": findings}))
    marker = destination / ".publication-managed.json"
    previous = None
    if destination.exists():
        if marker.is_symlink() or not marker.resolve().is_relative_to(destination):
            raise ValueError("Managed marker path escape")
        if not marker.exists():
            raise ValueError("Existing destination has no managed-publication marker; no overwrite")
        previous = json.loads(marker.read_text(encoding="utf-8"))
        if previous.get("stage_task") != stage_task:
            raise ValueError("Do not overwrite another stage's export")
        if set(previous["managed_paths"]) - set(selected):
            raise ValueError("Previously managed path left allowlist; review required, no deletion")
    for path in selected:
        target = destination / path
        if target.exists() and (not previous or path not in previous["managed_paths"]):
            raise ValueError("Refuse to overwrite an unmanaged destination file")
        if target.is_symlink() or not target.resolve().is_relative_to(destination):
            raise ValueError("Destination path escape")
        if target.exists() and previous:
            old = next(row for row in previous["files"] if row["path"] == path)
            if sha256(target.read_bytes()) != old["sha256"]:
                raise ValueError("Managed file changed since export; preserve and review before regenerating")
    destination.mkdir(parents=True, exist_ok=True)
    for path, data in selected.items():
        target = destination / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists() or target.read_bytes() != data:
            tmp = target.with_name(target.name + ".export-tmp")
            tmp.write_bytes(data)
            tmp.replace(target)
    manifest = {"stage_task": stage_task, "publish_network_action_taken": False, "git_action_taken": False, "source_files_and_synthetic_fixtures_only": True, "external_acceptance_status": "PENDING_REVIEW", "managed_paths": sorted(selected), "files": [{"path": p, "bytes": len(d), "sha256": sha256(d)} for p, d in sorted(selected.items())], "scan": {"files_scanned": len(selected), "findings": [], "method": "explicit path allowlist plus bounded literal/token/path pattern scan; no automatic redaction", "reviewed_exception": "tests/test_archive.py intentionally rejects the synthetic C-drive root path"}}
    marker.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    local_manifest = work / "manifests"
    local_manifest.mkdir(exist_ok=True)
    (local_manifest / "publication_allowlist.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (local_manifest / "excluded_public_paths.json").write_text(json.dumps({"default_policy": "DENY_UNLESS_EXPLICITLY_SELECTED", "excluded": EXCLUSIONS}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"public_files": len(selected), "total_bytes": sum(map(len, selected.values())), "scan_findings": 0, "stage_task": stage_task, "publication_status": "PREPARED_NOT_PUBLISHED"}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-work", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--stage-task", required=True)
    parser.add_argument("--aggregate-report", type=Path)
    args = parser.parse_args()
    print(json.dumps(export(args.source_work, args.destination, args.stage_task, args.aggregate_report), indent=2))
