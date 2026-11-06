"""Client scopes retain complete registered evidence and cannot claim global coverage."""

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import client_recovery_checks, deloxide_lane, shuttle_checks

ROOT = Path(__file__).resolve().parents[2]


class ClientRecoveryChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.full = deloxide_lane.load_inventory(ROOT)

    def test_every_registered_owner_workload_survives_without_mutating_the_full_inventory(self) -> None:
        before = dict(self.full.invocations)
        scoped = client_recovery_checks.diagnostic_inventory(self.full)
        self.assertEqual(self.full.invocations, before)
        self.assertNotEqual(scoped.invocations, before)
        for path in client_recovery_checks.OWNERS:
            self.assertLessEqual(set(self.full.owners[path].workloads), scoped.workloads.keys())
        for name, item in scoped.workloads.items():
            self.assertIs(item, self.full.workloads[name])
        for name, item in scoped.invocations.items():
            self.assertEqual(item.tags, self.full.invocations[name].tags)
            self.assertEqual(item.features, self.full.invocations[name].features)
            self.assertEqual(item.bound_seconds, self.full.invocations[name].bound_seconds)

    def test_every_diagnostic_selection_matches_its_complete_feature_registration(self) -> None:
        scoped = client_recovery_checks.diagnostic_inventory(self.full)
        for selection in scoped.selections:
            selected = 0
            for invocation in scoped.invocations.values():
                registered = scoped.workloads_of(invocation.id, selection)
                if not registered:
                    continue
                selected += sum(item.examples or 1 for item in registered)
                if invocation.kind is deloxide_lane.Kind.CUCUMBER:
                    discovered = deloxide_lane.discover_scenarios(ROOT, invocation.inputs, invocation.tags)
                    self.assertEqual(deloxide_lane.scenario_problems(invocation, registered, discovered), [])
            self.assertGreater(selected, 0)

    def test_a_missing_owner_cannot_silently_shrink_the_scope(self) -> None:
        owners = dict(self.full.owners)
        del owners[client_recovery_checks.OWNERS[0]]
        with self.assertRaises(deloxide_lane.LaneError):
            client_recovery_checks.diagnostic_inventory(replace(self.full, owners=owners))

    def test_local_evidence_cannot_enter_a_whole_workspace_coverage_collection(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(deloxide_lane, "Processes") as processes:
            with self.assertRaises(deloxide_lane.LaneError):
                client_recovery_checks.run_diagnostic(ROOT, Path(directory), "deloxide",
                                                     {deloxide_lane.COVERAGE_ATTEMPT_VARIABLE: "collection"})
            processes.assert_not_called()

    def test_the_shuttle_scope_keeps_every_registered_client_check(self) -> None:
        full = shuttle_checks.parse_inventory((ROOT / shuttle_checks.INVENTORY).read_text())
        scoped = client_recovery_checks.shuttle_inventory(full)
        self.assertEqual(scoped.checks, {client_recovery_checks.PACKAGE: full.checks[client_recovery_checks.PACKAGE]})


if __name__ == "__main__":
    unittest.main()
