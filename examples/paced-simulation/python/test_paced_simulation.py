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


class ConnectionTests(unittest.TestCase):
    def test_connect_constructs_the_native_session_with_its_domain_and_credentials(self):
        settings = SimpleNamespace(server="http://127.0.0.1:47391", domain="simulation",
                                   username="sensor", password="credential")
        run = driver.Run(settings)
        session = object()
        with patch.object(driver.nx, "Session", autospec=True, return_value=session) as constructor:
            run.connect()
        constructor.assert_called_once_with(
            settings.server, domain=settings.domain,
            username=settings.username, password=settings.password)
        self.assertIs(run.session, session)


class EndpointOpenTests(unittest.TestCase):
    def open_sequence(self, refusals, intent, times):
        def attempt():
            refusal = next(refusals)
            if refusal is not None:
                failure = driver.nx.RejectedError("refused")
                failure.message = "refused"
                failure.open_refusal = refusal
                raise failure
            return 42

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
            iter([driver.nx.OpenRefusal.DOMAIN_STOPPED, driver.nx.OpenRefusal.ENDPOINT_UNAVAILABLE, None]),
            driver.OpenIntent.FOLLOWING_START,
            [0, 1, 2],
        )
        self.assertEqual(handle, 42)
        self.assertEqual((self.attempts, self.backoffs), (3, 2))

    def test_a_stopped_domain_exhausts_the_same_open_budget(self):
        with self.assertRaisesRegex(driver.ConfigurationError, "domain stopped"):
            self.open_sequence(
                iter([driver.nx.OpenRefusal.DOMAIN_STOPPED, driver.nx.OpenRefusal.DOMAIN_STOPPED]),
                driver.OpenIntent.FOLLOWING_START,
                [0, 1, driver.OPEN_RETRY_BUDGET],
            )
        self.assertEqual((self.attempts, self.backoffs), (2, 1))

    def test_an_open_in_the_current_generation_refuses_a_stopped_domain(self):
        with self.assertRaisesRegex(driver.ConfigurationError, "domain stopped"):
            self.open_sequence(iter([driver.nx.OpenRefusal.DOMAIN_STOPPED]),
                               driver.OpenIntent.CURRENT_GENERATION, [0])
        self.assertEqual((self.attempts, self.backoffs), (1, 0))

    def test_following_start_keeps_schema_and_missing_endpoint_refusals_terminal(self):
        for refusal in (driver.nx.OpenRefusal.DOMAIN_NOT_FOUND,
                        driver.nx.OpenRefusal.ENDPOINT_NOT_FOUND,
                        driver.nx.OpenRefusal.SCHEMA_MISMATCH):
            with self.subTest(refusal=refusal), \
                    self.assertRaisesRegex(driver.ConfigurationError, driver.REFUSALS[refusal]):
                self.open_sequence(iter([refusal]), driver.OpenIntent.FOLLOWING_START, [0])
            self.assertEqual((self.attempts, self.backoffs), (1, 0))

    def test_an_unavailable_endpoint_remains_retryable_in_the_current_generation(self):
        handle = self.open_sequence(iter([driver.nx.OpenRefusal.ENDPOINT_UNAVAILABLE, None]),
                                    driver.OpenIntent.CURRENT_GENERATION, [0, 1])
        self.assertEqual(handle, 42)
        self.assertEqual((self.attempts, self.backoffs), (2, 1))

    def test_following_start_opens_with_the_observed_generation_intent(self):
        run = Mock()
        run.settings.follow_generations = True
        run.open_producer.return_value = 2
        run.clock.paced.return_value.paced = SimpleNamespace(origin=0, period=100)
        with patch.object(driver, "line"):
            grid = driver.Run.follow(run, (driver.PACED, 2), SimpleNamespace(generation=1))
        self.assertEqual(grid.generation, 2)
        run.open_producer.assert_called_once_with(driver.OpenIntent.FOLLOWING_START)
        run.set_generation.assert_called_once_with(2)


class ClockPolicyTests(unittest.TestCase):
    def test_an_interruption_pauses_until_a_state_is_observed(self):
        clock = driver.Clock(Mock(), "simulation", driver.Counters(), (driver.PACED, 7))
        with patch.object(driver, "line"):
            self.assertTrue(clock.observe(SimpleNamespace(kind=driver.nx.ClockEventKind.INTERRUPTED)))
            self.assertEqual(clock.current()[0], (driver.PAUSED, 7))
            self.assertTrue(clock.observe(SimpleNamespace(kind=driver.nx.ClockEventKind.TICK)))
            self.assertTrue(clock.observe(SimpleNamespace(kind=driver.nx.ClockEventKind.RESTORATION_FAILED)))
            self.assertEqual(clock.current()[0], (driver.PAUSED, 7))
            mapping = SimpleNamespace(period=100, skew=20, origin=0, anchor=0, rate=1.0)
            self.assertTrue(clock.observe(SimpleNamespace(
                kind=driver.nx.ClockEventKind.STATE, state=driver.nx.ClockState.PACED,
                generation=7, paced=mapping)))
        self.assertEqual(clock.current()[0], (driver.PACED, 7))
        self.assertEqual(clock.counters.ticks_observed, 1)

    def test_a_domain_ending_stops_the_clock_follower(self):
        clock = driver.Clock(Mock(), "simulation", driver.Counters(), (driver.PACED, 7))
        with patch.object(driver, "line"):
            continuing = clock.observe(SimpleNamespace(
                kind=driver.nx.ClockEventKind.ENDED,
                end_reason=driver.nx.ClockEndReason.DOMAIN_REMOVED))
        self.assertFalse(continuing)
        self.assertEqual(clock.current()[0], (driver.ENDED, None))


class OutcomeTests(unittest.TestCase):
    def test_terminal_outcomes_preserve_the_ledger_cause(self):
        cases = [
            (SimpleNamespace(result=driver.nx.SubmissionResult.COMPLETED), ("completed", "")),
            (SimpleNamespace(result=driver.nx.SubmissionResult.NOT_ADMITTED,
                             refusal=driver.nx.SubmissionRefusal.SUSPENDED),
             ("not_admitted", "suspended")),
            (SimpleNamespace(result=driver.nx.SubmissionResult.PROCESSING_FAILED,
                             failure=driver.nx.ProcessingFailure.ACK_TIMED_OUT),
             ("processing_failed", "ack_timeout")),
            (SimpleNamespace(result=driver.nx.SubmissionResult.OUTCOME_UNKNOWN,
                             uncertainty=driver.nx.Uncertainty.OWNER_LOST),
             ("outcome_unknown", "owner_lost")),
        ]
        for outcome, expected in cases:
            with self.subTest(outcome=outcome):
                self.assertEqual(driver.outcome_of(outcome), expected)

    def test_an_outcome_releases_its_producers_credit_after_replacement(self):
        run = driver.Run(SimpleNamespace())
        endpoint = Mock()
        endpoint.rejoin.return_value = SimpleNamespace(result=driver.nx.SubmissionResult.COMPLETED)
        submitting = driver.Producer(endpoint)
        submitting.outstanding.update(batches=1, bytes=64)
        replacement = driver.Producer(Mock())
        replacement.outstanding.update(batches=2, bytes=128)
        run.producer = replacement
        run.ledger = Mock()
        with patch.object(driver, "line"):
            run.await_outcome(submitting, 3, ["reading"], ["rejection"], 64, 12)
        self.assertEqual(submitting.outstanding, {"batches": 0, "bytes": 0})
        self.assertEqual(replacement.outstanding, {"batches": 2, "bytes": 128})
        self.assertEqual(run.awaited_notices, ["rejection"])
        run.ledger.outcome.assert_called_once_with(["reading"], "completed", "")


class DeliveryTests(unittest.TestCase):
    def consumer(self):
        run = Mock()
        run.counters = driver.Counters()
        loop = driver.ConsumerLoop(run, "output-1", "observed_readings", "reading", 0, None)
        delivery = Mock()
        columns = [["g7-t2-s0-0"], ["sensor-0"], [2], [123], [124], ["at"], [62]]
        delivery.batch = SimpleNamespace(row_count=1, column=columns.__getitem__)
        delivery.ack.return_value = driver.nx.Settlement.CONFIRMED
        delivery.retry.return_value = driver.nx.Settlement.CONFIRMED
        return run, loop, delivery

    def test_effects_are_recorded_before_acknowledgement_with_every_field(self):
        run, loop, delivery = self.consumer()
        applied = []
        run.effects.apply.side_effect = lambda kind, record: applied.append((kind, record)) or True

        def acknowledge():
            self.assertEqual(applied, [("reading", {
                "kind": "reading", "reading_id": "g7-t2-s0-0", "sensor": "sensor-0", "tick": 2,
                "occurred_at": driver.rfc3339(123), "admitted_at": driver.rfc3339(124),
                "timestamp_source": "at", "value": 62, "generation": 7,
            })])
            return driver.nx.Settlement.CONFIRMED

        delivery.ack.side_effect = acknowledge
        with patch.object(driver, "line"):
            loop.handle(delivery, 7)
        delivery.ack.assert_called_once_with()
        self.assertEqual(run.counters.effects, 1)

    def test_a_failed_effect_requests_redelivery(self):
        run, loop, delivery = self.consumer()
        run.effects.apply.side_effect = OSError("cannot append")
        with patch.object(driver, "line"), patch.object(driver, "error_line"):
            loop.handle(delivery, 7)
        delivery.retry.assert_called_once_with()
        delivery.ack.assert_not_called()
        self.assertEqual(run.counters.effects, 0)

    def test_a_duplicate_effect_is_acknowledged_without_counting_it_again(self):
        run, loop, delivery = self.consumer()
        run.effects.apply.return_value = False
        with patch.object(driver, "line"):
            loop.handle(delivery, 7)
        delivery.ack.assert_called_once_with()
        self.assertEqual(run.counters.effects, 0)
        self.assertEqual(run.counters.duplicates, 1)
