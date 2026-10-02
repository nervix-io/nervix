from __future__ import annotations

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.libtest_accounting import (
    Counts,
    InventoryError,
    counts,
    main,
    outcomes,
    parse_inventory,
)

LIBRARY = """\
     Running unittests src/lib.rs (target/debug/deps/nervix_interconnect-1234)

running 12 tests
test wire::simulation_checks::a ... ok
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 431 filtered out; finished in 3.21s
"""

SIMULATION = """\
     Running tests/simulation.rs (target/debug/deps/simulation-5678)

running 36 tests
test result: ok. 34 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 61.0s
"""

SCENARIOS = """\
running 3 tests
test transport::partition_heals ... ok
test process_dependent_trace ... ignored, started in a fresh process by diverged_and_panicked_runs
test relay::replies_arrive ... ok

test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 9.0s
"""

INVENTORY = """
[[invocation]]
name = "simulation"
tests = ["relay::replies_arrive", "transport::partition_heals"]
ignored = ["process_dependent_trace"]
"""

NOTHING = """\
running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 443 filtered out; finished in 0.00s
"""


FRESH_PROCESS = """\
running 3 tests
test authentication::replay_trace_in_fresh_process ... ignored, started in a fresh process by semantic_trace_replays
test authentication::semantic_trace_replays ... 
running 1 test
test authentication::replay_trace_in_fresh_process ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 70 filtered out; finished in 2.52s

ok
test wire::round_trips ... ok

test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 58 filtered out; finished in 22.30s
"""


class CountTests(unittest.TestCase):
    def test_a_fresh_process_a_test_starts_is_credited_to_that_test(self) -> None:
        self.assertEqual(
            counts(FRESH_PROCESS), Counts(discovered=61, selected=3, executed=2, completed=2)
        )
        self.assertEqual(
            outcomes(FRESH_PROCESS),
            {
                "authentication::replay_trace_in_fresh_process": "ignored",
                "authentication::semantic_trace_replays": "ok",
                "wire::round_trips": "ok",
            },
        )

    def test_a_result_line_counts_its_binary(self) -> None:
        self.assertEqual(
            counts(LIBRARY), Counts(discovered=443, selected=12, executed=12, completed=12)
        )

    def test_ignored_tests_are_selected_and_not_executed(self) -> None:
        self.assertEqual(
            counts(SIMULATION), Counts(discovered=36, selected=36, executed=34, completed=34)
        )

    def test_every_binary_of_one_invocation_adds_up(self) -> None:
        self.assertEqual(
            counts(LIBRARY + SIMULATION),
            Counts(discovered=479, selected=48, executed=46, completed=46),
        )

    def test_a_failed_test_is_executed_and_not_completed(self) -> None:
        failed = (
            "running 4 tests\n"
            "test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out\n"
        )
        self.assertEqual(counts(failed), Counts(discovered=4, selected=4, executed=4, completed=3))

    def test_output_without_a_result_line_has_no_counts(self) -> None:
        self.assertIsNone(counts("running 3 tests\ntest a ... "))


class SuiteTests(unittest.TestCase):
    def run_suite(self, *outputs: str) -> tuple[int, str]:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        logs = []
        for index, output in enumerate(outputs):
            log = Path(directory.name) / f"invocation-{index}.log"
            log.write_text(output, encoding="utf-8")
            logs.append(str(log))
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(out):
            status = main(["turmoil", *logs])
        return status, out.getvalue()

    def test_a_suite_whose_invocations_all_ran_tests_passes(self) -> None:
        status, report = self.run_suite(LIBRARY, SIMULATION)
        self.assertEqual(status, 0, report)
        self.assertIn("turmoil: discovered 479, selected 48, executed 46, completed 46", report)
        self.assertIn("turmoil: invocation-0: discovered 443, selected 12", report)

    def test_an_invocation_that_matched_nothing_fails_the_suite(self) -> None:
        status, report = self.run_suite(LIBRARY, NOTHING)
        self.assertEqual(status, 1)
        self.assertIn("invocation-1 executed no test; its selection matched nothing", report)

    def test_an_invocation_that_did_not_finish_fails_the_suite(self) -> None:
        status, report = self.run_suite(LIBRARY, "running 36 tests\n")
        self.assertEqual(status, 1)
        self.assertIn("invocation-1.log holds no test result", report)

    def test_a_suite_without_logs_fails(self) -> None:
        status, report = self.run_suite()
        self.assertEqual(status, 1)
        self.assertIn("no invocation of the suite left a log", report)

    def test_a_failed_test_fails_the_suite(self) -> None:
        status, report = self.run_suite(
            "running 4 tests\n"
            "test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out\n"
        )
        self.assertEqual(status, 1)
        self.assertIn("a test failed", report)


class InventoryTests(unittest.TestCase):
    def run_suite(self, output: str, inventory: str = INVENTORY) -> tuple[int, str]:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        log = Path(directory.name) / "simulation.log"
        log.write_text(output, encoding="utf-8")
        registered = Path(directory.name) / "inventory.toml"
        registered.write_text(inventory, encoding="utf-8")
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(out):
            status = main(["turmoil", "--inventory", str(registered), str(log)])
        return status, out.getvalue()

    def test_every_reported_test_and_its_outcome_is_read(self) -> None:
        self.assertEqual(
            outcomes(SCENARIOS),
            {
                "transport::partition_heals": "ok",
                "process_dependent_trace": "ignored",
                "relay::replies_arrive": "ok",
            },
        )

    def test_a_suite_that_ran_its_inventory_passes(self) -> None:
        status, report = self.run_suite(SCENARIOS)
        self.assertEqual(status, 0, report)

    def test_a_registered_test_that_disappeared_fails(self) -> None:
        status, report = self.run_suite(SCENARIOS.replace("test relay::replies_arrive ... ok\n", ""))
        self.assertEqual(status, 1)
        self.assertIn("simulation: the registered test relay::replies_arrive did not run", report)

    def test_a_registered_test_that_is_ignored_fails(self) -> None:
        status, report = self.run_suite(
            SCENARIOS.replace("relay::replies_arrive ... ok", "relay::replies_arrive ... ignored")
        )
        self.assertEqual(status, 1)
        self.assertIn("the registered test relay::replies_arrive is ignored", report)

    def test_an_unregistered_test_fails(self) -> None:
        status, report = self.run_suite(
            SCENARIOS.replace(
                "test relay::replies_arrive ... ok\n",
                "test relay::replies_arrive ... ok\ntest relay::new_case ... ok\n",
            )
        )
        self.assertEqual(status, 1)
        self.assertIn("simulation: relay::new_case ran but is not registered", report)

    def test_a_probe_that_is_no_longer_ignored_fails(self) -> None:
        status, report = self.run_suite(
            SCENARIOS.replace("process_dependent_trace ... ignored", "process_dependent_trace ... ok")
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "process_dependent_trace is registered as ignored but was not reported ignored", report
        )

    def test_an_invocation_without_a_log_or_an_unregistered_log_fails(self) -> None:
        status, report = self.run_suite(
            SCENARIOS,
            INVENTORY + '\n[[invocation]]\nname = "library"\ntests = ["wire::a"]\n',
        )
        self.assertEqual(status, 1)
        self.assertIn("the registered invocation library left no log", report)
        status, report = self.run_suite(SCENARIOS, INVENTORY.replace('"simulation"', '"other"'))
        self.assertEqual(status, 1)
        self.assertIn("simulation is not an invocation the inventory registers", report)

    def test_a_marker_requires_only_the_invariants_it_marks(self) -> None:
        library = (
            "running 2 tests\n"
            "test workers::simulation_checks::a_job_runs_on_the_host ... ok\n"
            "test workers::tests::ordinary ... ok\n"
            "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n"
        )
        registered = (
            '[[invocation]]\nname = "simulation"\nmarker = "simulation_checks"\n'
            'tests = ["workers::simulation_checks::a_job_runs_on_the_host"]\n'
        )
        status, report = self.run_suite(library, registered)
        self.assertEqual(status, 0, report)
        status, report = self.run_suite(
            library.replace("ok\ntest result", "ok\ntest workers::simulation_checks::new ... ok\ntest result"),
            registered,
        )
        self.assertEqual(status, 1)
        self.assertIn("workers::simulation_checks::new ran but is not registered", report)
        with self.assertRaises(InventoryError):
            parse_inventory(registered.replace("a_job_runs_on_the_host", "x").replace("workers::simulation_checks::x", "workers::tests::x"))

    def test_a_malformed_inventory_is_refused(self) -> None:
        with self.assertRaises(InventoryError):
            parse_inventory('[[invocation]]\nname = "simulation"\ntests = []\n')
        with self.assertRaises(InventoryError):
            parse_inventory(INVENTORY.replace('ignored = ["process_dependent_trace"]', 'ignored = ["relay::replies_arrive"]'))
        with self.assertRaises(InventoryError):
            parse_inventory("")
        status, report = self.run_suite(SCENARIOS, "[[invocation]]\nname = 1\n")
        self.assertEqual(status, 1)
        self.assertIn("needs a non-empty string `name`", report)


if __name__ == "__main__":
    unittest.main()
