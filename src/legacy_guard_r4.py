"""Reject superseded live writers in a marked R4 workspace before side effects.

Pure historical adapters and common R4-inherited arithmetic remain available.
The marker check is local to the explicit workspace, so isolated historical
synthetic fixtures do not acquire R4 authority from a containing directory.
"""
from pathlib import Path


class LegacyEntryBlocked(RuntimeError):
    reason_code = 'R4_LEGACY_LIVE_ENTRY_FORBIDDEN'


def reject_legacy_workspace(work, entry):
    root = Path(work).resolve()
    if any((root / 'configs' / name).exists() for name in
           ('STAGE1B_R4_POLICY.json', 'STAGE1B_R4_PUBLIC_POLICY.json')):
        raise LegacyEntryBlocked(entry + ': R4 workspace requires context_access_r4 or dune_r4; legacy budget/network entry is disabled')
    return root


def reject_legacy_ledger_path(path):
    target = Path(path).resolve()
    if target.parent.name.lower() == 'private' and target.name.lower() in {
            'shared_budget.sqlite', 'shared_budget_r1.sqlite', 'shared_budget_r2.sqlite', 'shared_budget_r3.sqlite'}:
        reject_legacy_workspace(target.parent.parent, 'legacy ledger constructor')


def reject_unscoped_legacy_transport(entry, *, work=None, module_file=None):
    if work is not None:
        return reject_legacy_workspace(work, entry)
    # A transport without a workspace must not infer an older authorization
    # when invoked from the active R4 module or working directory.
    reject_legacy_workspace(Path.cwd(), entry)
    if module_file is not None:
        reject_legacy_workspace(Path(module_file).resolve().parents[1], entry)
