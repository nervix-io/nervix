from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import unittest

from scripts import bolero, deloxide_lane, loom_models, rate_limit_checks, shuttle_checks


ROOT = Path(__file__).resolve().parents[2]


class FocusedEvidenceTests(unittest.TestCase):
    def test_property_scope_preserves_budgets_and_drops_unrelated_preparations(self) -> None:
        full = bolero.load_inventory()
        selected = rate_limit_checks.property_inventory(full)
        self.assertEqual({item.id for item in selected.targets}, {"rate-limit-quota-bounds", "rate-limit-utc-generations"})
        self.assertEqual({item.package for item in selected.targets}, {rate_limit_checks.PACKAGE})
        self.assertEqual({item.random_iterations for item in selected.targets}, {256})
        self.assertEqual(selected.prepared_targets, ())
        with self.assertRaises(bolero.BoleroError):
            rate_limit_checks.property_inventory(replace(full, targets=()))

    def test_models_keep_only_the_engine_and_its_registered_weakening(self) -> None:
        full = loom_models.parse_inventory((ROOT / loom_models.INVENTORY).read_text())
        selected = rate_limit_checks.loom_inventory(full)
        self.assertEqual(selected.packages(), [rate_limit_checks.PACKAGE])
        identities = {item.id for item in selected.invariants}
        self.assertEqual(identities, {"rate-limit.direct.exact-refund"})
        self.assertEqual({item.invariant for item in selected.qualifications}, identities)
        self.assertGreater(len(full.invariants), len(selected.invariants))
        with self.assertRaises(loom_models.RunnerError):
            rate_limit_checks.loom_inventory(loom_models.Inventory((), ()))

    def test_shuttle_selection_cannot_silently_select_another_package(self) -> None:
        full = shuttle_checks.parse_inventory((ROOT / shuttle_checks.INVENTORY).read_text())
        selected = rate_limit_checks.shuttle_inventory(full)
        self.assertEqual(set(selected.checks), {rate_limit_checks.PACKAGE})
        self.assertEqual(selected.checks[rate_limit_checks.PACKAGE], full.checks[rate_limit_checks.PACKAGE])
        with self.assertRaises(shuttle_checks.RunnerError):
            rate_limit_checks.shuttle_inventory(shuttle_checks.Inventory({}))

    def test_every_diagnostic_selection_reaches_the_new_lock_owner(self) -> None:
        full = deloxide_lane.load_inventory(ROOT)
        selected = rate_limit_checks.diagnostic_inventory(full)
        self.assertEqual(set(selected.invocations), {"rate-limit-store"})
        self.assertEqual(set(selected.owners), {"crates/rate-limit/src/keyed.rs"})
        for selection in full.selections:
            workloads = selected.workloads_of("rate-limit-store", selection)
            self.assertEqual(len(workloads), 1, selection)
            self.assertEqual(workloads[0].test, "diagnostic_tests::deloxide_keyed_transactions_expire_evict_refund_and_restore")
        self.assertEqual(full.bounds, selected.bounds)
        with self.assertRaises(deloxide_lane.LaneError):
            rate_limit_checks.diagnostic_inventory(replace(full, invocations={}))

    def test_shared_owner_record_does_not_claim_unselected_workloads(self) -> None:
        full = deloxide_lane.load_inventory(ROOT)
        owner = full.owners["crates/rate-limit/src/keyed.rs"]
        foreign = next(name for name in full.workloads if name not in owner.workloads)
        augmented = replace(full, owners={owner.path: replace(owner, workloads=(*owner.workloads, foreign))})
        selected = rate_limit_checks.diagnostic_inventory(augmented)
        self.assertEqual(selected.owners[owner.path].workloads, owner.workloads)
        self.assertIn(foreign, augmented.workloads)


if __name__ == "__main__":
    unittest.main()
