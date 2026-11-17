"""Fault controls for the public connector-metric evidence verifier."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from scripts.chaos import connector_metrics


def emitter_scrape(node="node-2", total=0):
    labels = (
        'connector="KAFKA",domain="chaos_baseline",emitter="chaos_emitter",'
        f'entity="TOPIC",instance="-",physical_node_id="{node}"'
    )
    lines = []
    for operation in ("publish", "commit"):
        for outcome in ("succeeded", "transient", "misconfigured", "violation"):
            value = total if operation == "publish" and outcome == "succeeded" else 0
            lines.append(
                f'nervix_emitter_publish_attempts_total{{{labels},operation="{operation}",'
                f'outcome="{outcome}"}} {value}'
            )
        value = total if operation == "publish" else 0
        lines.append(
            f'nervix_emitter_publish_duration_seconds_count{{{labels},'
            f'operation="{operation}"}} {value}'
        )
    return "\n".join(lines) + "\n"


def ingestor_scrape(node="node-2", total=0):
    labels = (
        'connector="KAFKA",domain="chaos_baseline",entity="TOPIC",'
        f'ingestor="chaos_ingestor",instance="0",physical_node_id="{node}"'
    )
    return (
        f'nervix_ingestor_read_payloads_total{{{labels}}} {total}\n'
        f'nervix_ingestor_read_bytes_total{{{labels}}} {total * 100}\n'
        f'nervix_ingestor_ack_outcomes_total{{{labels},outcome="acknowledged"}} {total}\n'
    )


def description(kind, node="node-2"):
    section = "destination" if kind == "emitter" else "source"
    return f"owner: {node}\n{section}:\n"


class ConnectorMetricEvidenceTests(unittest.TestCase):
    def observation(self, kind, text, node="node-2"):
        return connector_metrics.Observation.read(text, kind, node)

    def test_fresh_owner_has_zero_registered_children_for_both_boundaries(self):
        for kind, fixture in (("emitter", emitter_scrape), ("ingestor", ingestor_scrape)):
            with self.subTest(kind=kind):
                observed = self.observation(kind, fixture())
                observed.require_zero()
                observed.require_description(description(kind))

    def test_progress_compares_the_same_new_owner_without_another_process_total(self):
        for kind, fixture in (("emitter", emitter_scrape), ("ingestor", ingestor_scrape)):
            with self.subTest(kind=kind):
                before = self.observation(kind, fixture())
                after = self.observation(kind, fixture(total=2))
                before.require_progress(after)

    def test_missing_child_is_not_zero(self):
        text = emitter_scrape().replace('outcome="violation"} 0\n', 'outcome="other"} 0\n', 1)
        with self.assertRaisesRegex(connector_metrics.EvidenceError, "missing"):
            self.observation("emitter", text)

    def test_duplicate_sample_is_ambiguous(self):
        text = ingestor_scrape()
        with self.assertRaisesRegex(connector_metrics.EvidenceError, "duplicate"):
            self.observation("ingestor", text + text.splitlines()[0] + "\n")

    def test_wrong_process_is_not_a_replacement_observation(self):
        with self.assertRaisesRegex(connector_metrics.EvidenceError, "node"):
            self.observation("emitter", emitter_scrape(node="node-1"))

    def test_another_instance_cannot_supply_the_default_ingestors_samples(self):
        text = ingestor_scrape().replace('instance="0"', 'instance="1"')
        with self.assertRaisesRegex(connector_metrics.EvidenceError, "instance"):
            self.observation("ingestor", text)

    def test_numbered_instance_cannot_supply_single_emitter_samples(self):
        for instance in ("0", "1"):
            with self.subTest(instance=instance):
                text = emitter_scrape().replace('instance="-"', f'instance="{instance}"')
                with self.assertRaisesRegex(connector_metrics.EvidenceError, "instance"):
                    self.observation("emitter", text)

    def test_wrong_entity_or_domain_cannot_supply_missing_samples(self):
        for name, value in (("emitter", "another"), ("domain", "another"), ("entity", "STREAM")):
            with self.subTest(label=name):
                text = emitter_scrape().replace(
                    f'{name}="' + {"emitter": "chaos_emitter", "domain": "chaos_baseline", "entity": "TOPIC"}[name] + '"',
                    f'{name}="{value}"',
                )
                with self.assertRaises(connector_metrics.EvidenceError):
                    self.observation("emitter", text)

    def test_payload_or_branch_labels_cannot_expand_the_family(self):
        text = emitter_scrape().replace('connector="KAFKA"', 'connector="KAFKA",branch="secret"')
        with self.assertRaisesRegex(connector_metrics.EvidenceError, "labels"):
            self.observation("emitter", text)

    def test_nonfinite_negative_and_fractional_counter_values_fail(self):
        for value in ("NaN", "+Inf", "-1", "0.5"):
            with self.subTest(value=value):
                text = ingestor_scrape().replace("} 0\n", "} " + value + "\n", 1)
                with self.assertRaises(connector_metrics.EvidenceError):
                    self.observation("ingestor", text)

    def test_malformed_text_is_a_failed_observation(self):
        with self.assertRaises(connector_metrics.EvidenceError):
            self.observation("ingestor", 'nervix_ingestor_read_bytes_total{broken} 1\n')

    def test_nonzero_replacement_is_not_fresh(self):
        with self.assertRaisesRegex(connector_metrics.EvidenceError, "zero"):
            self.observation("ingestor", ingestor_scrape(total=1)).require_zero()

    def test_a_stalled_family_does_not_qualify(self):
        before = self.observation("emitter", emitter_scrape())
        with self.assertRaisesRegex(connector_metrics.EvidenceError, "advance"):
            before.require_progress(self.observation("emitter", emitter_scrape()))

    def test_payload_progress_without_transport_confirmation_does_not_qualify(self):
        before = self.observation("ingestor", ingestor_scrape())
        text = ingestor_scrape(total=2).replace('outcome="acknowledged"} 2', 'outcome="acknowledged"} 0')
        with self.assertRaisesRegex(connector_metrics.EvidenceError, "advance"):
            before.require_progress(self.observation("ingestor", text))

    def test_process_change_or_counter_reset_does_not_qualify_as_progress(self):
        before = self.observation("ingestor", ingestor_scrape(total=2))
        with self.assertRaisesRegex(connector_metrics.EvidenceError, "node"):
            before.require_progress(self.observation("ingestor", ingestor_scrape("node-3", 3), "node-3"))
        with self.assertRaisesRegex(connector_metrics.EvidenceError, "decreased"):
            before.require_progress(self.observation("ingestor", ingestor_scrape(total=1)))

    def test_describe_must_route_to_the_observed_owner_and_section(self):
        observed = self.observation("ingestor", ingestor_scrape())
        for text in (description("ingestor", "node-1"), description("emitter")):
            with self.subTest(text=text):
                with self.assertRaises(connector_metrics.EvidenceError):
                    observed.require_description(text)

    def test_command_retains_success_and_failure_verdicts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            before = root / "before.prom"
            after = root / "after.prom"
            described = root / "describe.txt"
            output = root / "verdict.json"
            before.write_text(ingestor_scrape())
            described.write_text(description("ingestor"))
            for phase, total, expected, later in (("progress", 2, 0, True),
                                                  ("progress", 0, 1, True),
                                                  ("progress", 0, 1, False),
                                                  ("zero", 0, 0, False),
                                                  ("zero", 1, 1, False)):
                with self.subTest(phase=phase, total=total):
                    before.write_text(ingestor_scrape(total=total if phase == "zero" else 0))
                    after.write_text(ingestor_scrape(total=total))
                    command = [
                        sys.executable, connector_metrics.__file__, phase, "--kind", "ingestor",
                        "--node", "node-2", "--before", str(before),
                        "--description", str(described), "--output", str(output),
                    ]
                    if later:
                        command.extend(["--after", str(after)])
                    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, expected, result.stderr)
                    verdict = json.loads(output.read_text())
                    self.assertEqual(verdict["physical_node_id"], "node-2")
                    self.assertEqual(verdict["verdict"], "pass" if expected == 0 else "fail")
                    self.assertEqual(len(verdict["before"]), 3)
                    if expected:
                        reason = "zero" if phase == "zero" else "advance" if later else "later scrape"
                        self.assertIn(reason, verdict["error"])

    def test_replacement_input_gate_opens_only_after_a_passing_zero_observation(self):
        controller = Path(connector_metrics.__file__).with_name("connector-metrics.sh")
        program = r'''
set -euo pipefail
source "$CONTROLLER"
artifact_dir="$CASE_DIR"
mkdir -p "$artifact_dir/traffic"
touch "$artifact_dir/traffic/hold-load"
connector_metrics_kind=ingestor
connector_metrics_initial_node=node-1
record_count=3
owner_from_description() { printf '%s\n' "$REPLACEMENT_NODE"; }
crash_fail() { return 1; }
wait_for() {
    # Opening the gate before judging zero would let traffic invalidate the observation.
    test -f "$artifact_dir/traffic/hold-load" || exit 42
    printf 'observed\n' > "$artifact_dir/zero-attempted"
    return "$ZERO_STATUS"
}
status=0
connector_metric_replacement "$artifact_dir/crash" || status=$?
test "$status" -eq "$EXPECTED_STATUS"
if test "$EXPECTED_STATUS" -eq 0; then
    # Keep the final record for the existing post-restart delivery oracle.
    test "$(cat "$artifact_dir/traffic/hold-load")" -eq 2
    connector_metric_restart_input
    test ! -f "$artifact_dir/traffic/hold-load"
else
    test -f "$artifact_dir/traffic/hold-load"
fi
if test "$REPLACEMENT_NODE" = node-1; then
    test ! -e "$artifact_dir/zero-attempted"
else
    test -f "$artifact_dir/zero-attempted"
fi
'''
        for node, zero_status, expected in (("node-2", 0, 0), ("node-2", 1, 1), ("node-1", 0, 1)):
            with self.subTest(node=node, zero=zero_status), tempfile.TemporaryDirectory() as directory:
                environment = dict(os.environ, CONTROLLER=str(controller), CASE_DIR=directory,
                                   REPLACEMENT_NODE=node, ZERO_STATUS=str(zero_status),
                                   EXPECTED_STATUS=str(expected))
                result = subprocess.run(["bash", "-c", program], env=environment,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_failed_capture_cannot_reuse_a_previous_scrape(self):
        controller = Path(connector_metrics.__file__).with_name("connector-metrics.sh")
        with tempfile.TemporaryDirectory() as directory:
            program = r'''
set -euo pipefail
source "$CONTROLLER"
artifact_dir="$CASE_DIR"
mkdir -p "$artifact_dir/public" "$artifact_dir/phase"
printf 'stale evidence\n' > "$artifact_dir/phase/metrics.prom"
capture_metrics() { return 0; }
domain_cli_command() { printf 'description called\n' > "$artifact_dir/described"; }
status=0
connector_metric_capture ingestor node-2 "$artifact_dir/phase" || status=$?
test "$status" -ne 0
test ! -e "$artifact_dir/described"
'''
            result = subprocess.run(
                ["bash", "-c", program],
                env=dict(os.environ, CONTROLLER=str(controller), CASE_DIR=directory),
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
