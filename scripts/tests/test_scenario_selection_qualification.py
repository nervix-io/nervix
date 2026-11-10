"""Fault controls for the expected-empty scenario-runner qualification verdict."""

import unittest

from scripts.scenario_selection_qualification import verify


EMPTY = "0 scenarios\nAttempts: 0 completed, 0 active; retries: 0.\nno scenarios selected\n"


class SelectionQualificationTests(unittest.TestCase):
    def test_exact_empty_selection_failure_is_accepted(self):
        verify(101, EMPTY)

    def test_successful_empty_run_is_never_qualification(self):
        with self.assertRaisesRegex(ValueError, "exit 101"):
            verify(0, EMPTY)

    def test_unrelated_failure_is_not_the_selection_guard(self):
        with self.assertRaisesRegex(ValueError, "selection guard"):
            verify(101, EMPTY.replace("no scenarios selected", "unrelated panic"))

    def test_executed_or_active_work_is_not_an_empty_selection(self):
        for count in ("1 completed, 0 active; retries: 0", "0 completed, 1 active; retries: 0",
                      "0 completed, 0 active; retries: 1"):
            with self.subTest(count=count), self.assertRaisesRegex(ValueError, "attempted scenario work"):
                verify(101, EMPTY.replace("0 completed, 0 active; retries: 0", count))

    def test_nonempty_scenario_summary_is_refused(self):
        with self.assertRaisesRegex(ValueError, "zero scenarios"):
            verify(101, EMPTY.replace("0 scenarios", "1 scenario (1 failed)"))
