"""Deterministic endpoint-open checks for the paced simulation application."""

import unittest
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import paced_simulation as driver


class CoverageStartupTests(unittest.TestCase):
    def startup(self, program, environment):
        coverage = Mock()
        hook = Path(__file__).resolve().parents[3] / "scripts/paced_simulation_coverage/sitecustomize.py"
        with patch.dict(sys.modules, {"coverage": coverage}), \
                patch.object(sys, "argv", [program]), \
                patch.dict(driver.os.environ, environment, clear=True):
            spec = spec_from_file_location("sitecustomize", hook)
            spec.loader.exec_module(module_from_spec(spec))
        return coverage

    def test_published_driver_starts_its_requested_coverage_collection(self):
        coverage = self.startup("paced_simulation.py", {"COVERAGE_PROCESS_START": "coverage.ini"})
        coverage.process_startup.assert_called_once_with()

    def test_other_programs_and_uninstrumented_drivers_do_not_start_coverage(self):
        coverage = self.startup("unittest", {"COVERAGE_PROCESS_START": "coverage.ini"})
        coverage.process_startup.assert_not_called()
        coverage = self.startup("paced_simulation.py", {})
        coverage.process_startup.assert_not_called()


class EndpointOpenTests(unittest.TestCase):
    def open_sequence(self, refusals, intent, times):
        def attempt(out):
            refusal = next(refusals)
            if refusal is not None:
                raise driver.BindingError(driver.ERROR_REJECTED, "refused", refusal)
            out._obj.value = 42
            return None

        opened = Mock(side_effect=attempt)
        with patch.object(driver.time, "monotonic", side_effect=times), \
                patch.object(driver.time, "sleep") as backoff:
            try:
                handle = driver.open_endpoint(opened, "ingestor", "producer", intent)
            finally:
                self.attempts = opened.call_count
                self.backoffs = backoff.call_count
        return handle

    def test_following_start_waits_for_the_serving_nodes_start_application(self):
        handle = self.open_sequence(
            iter([driver.REFUSAL_DOMAIN_STOPPED, driver.REFUSAL_ENDPOINT_UNAVAILABLE, None]),
            driver.OpenIntent.FOLLOWING_START,
            [0, 1, 2],
        )
        self.assertEqual(handle.value, 42)
        self.assertEqual((self.attempts, self.backoffs), (3, 2))

    def test_a_stopped_domain_exhausts_the_same_open_budget(self):
        with self.assertRaisesRegex(driver.ConfigurationError, "domain stopped"):
            self.open_sequence(
                iter([driver.REFUSAL_DOMAIN_STOPPED, driver.REFUSAL_DOMAIN_STOPPED]),
                driver.OpenIntent.FOLLOWING_START,
                [0, 1, driver.OPEN_RETRY_BUDGET],
            )
        self.assertEqual((self.attempts, self.backoffs), (2, 1))

    def test_an_open_in_the_current_generation_refuses_a_stopped_domain(self):
        with self.assertRaisesRegex(driver.ConfigurationError, "domain stopped"):
            self.open_sequence(iter([driver.REFUSAL_DOMAIN_STOPPED]),
                               driver.OpenIntent.CURRENT_GENERATION, [0])
        self.assertEqual((self.attempts, self.backoffs), (1, 0))

    def test_following_start_keeps_schema_and_missing_endpoint_refusals_terminal(self):
        for refusal in (1, 3, 6):
            with self.subTest(refusal=refusal), \
                    self.assertRaisesRegex(driver.ConfigurationError, driver.REFUSALS[refusal]):
                self.open_sequence(iter([refusal]), driver.OpenIntent.FOLLOWING_START, [0])
            self.assertEqual((self.attempts, self.backoffs), (1, 0))

    def test_an_unavailable_endpoint_remains_retryable_in_the_current_generation(self):
        handle = self.open_sequence(iter([driver.REFUSAL_ENDPOINT_UNAVAILABLE, None]),
                                    driver.OpenIntent.CURRENT_GENERATION, [0, 1])
        self.assertEqual(handle.value, 42)
        self.assertEqual((self.attempts, self.backoffs), (2, 1))

    def test_following_start_opens_with_the_observed_generation_intent(self):
        run = Mock()
        run.settings.follow_generations = True
        run.open_producer.return_value = 2
        run.clock.paced.return_value.paced.return_value = {"origin": 0, "period": 100}
        with patch.object(driver, "line"):
            grid = driver.Run.follow(run, (driver.PACED, 2), SimpleNamespace(generation=1))
        self.assertEqual(grid.generation, 2)
        run.open_producer.assert_called_once_with(driver.OpenIntent.FOLLOWING_START)
        run.set_generation.assert_called_once_with(2)
