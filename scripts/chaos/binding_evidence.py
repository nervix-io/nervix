"""Judge the Python binding's public submission, effect and settlement ledger.

This external test owner never imports product code. An uncertain submission is allowed to have
an effect, but never authorizes replay. Completed work requires its exact effect; an ACK's lost
answer stays unknown. Repeated delivery identities keep their content and each attempt has one ACK.
"""

import argparse
from collections import Counter
import json
from pathlib import Path
import sys


def read_ledger(path):
    """Read one bounded, complete NDJSON artifact, refusing missing or truncated evidence."""
    raw = Path(path).read_bytes()
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError(f"ledger exceeds 16 MiB: {path}")
    if raw and not raw.endswith(b"\n"):
        raise ValueError(f"ledger ends inside a line: {path}")
    lines = raw.splitlines()
    if len(lines) > 20000:
        raise ValueError(f"ledger exceeds 20000 events: {path}")
    return [json.loads(line) for line in lines]


class Evidence:
    def __init__(self, records):
        self.expected = {}
        self.failures = []
        for row in records:
            sequence = row["sequence"]
            if sequence in self.expected:
                self.failures.append(f"duplicate input sequence {sequence}")
            self.expected[sequence] = row
        if not self.expected:
            self.failures.append("no input records")
        self.submissions = {}
        self.outcomes = {}
        self.processing_failure_kinds = Counter()
        self.deliveries = {}
        self.references = {}
        self.effects = Counter()
        self.settlements = Counter()
        self.confirmed = set()
        self.finished = []

    def check_record(self, row, description):
        sequence = row["sequence"]
        if self.expected.get(sequence) != row:
            self.failures.append(f"{description} differs from input record {sequence}")
        return sequence

    def submitted(self, event):
        sequence = event["sequence"]
        if sequence not in self.expected:
            self.failures.append(f"unknown submitted record {sequence}")
        if sequence in self.submissions:
            self.failures.append(f"record {sequence} submitted more than once")
        self.submissions[sequence] = event["submission"]

    def outcome(self, event):
        sequence = event["sequence"]
        if sequence in self.outcomes:
            self.failures.append(f"record {sequence} has more than one outcome")
        if self.submissions.get(sequence) != event["submission"]:
            self.failures.append(f"outcome for record {sequence} has no matching submission")
        if event["result"] == "processing_failed":
            failure = event["failure"]
            if failure not in ("rejected", "ack_timed_out") or not isinstance(event["message"], str):
                raise ValueError("processing failure needs its declared typed cause and server message")
            self.processing_failure_kinds[failure] += 1
        if event["result"] not in ("completed", "processing_failed", "outcome_unknown"):
            self.failures.append(f"record {sequence} ended with {event['result']}")
        self.outcomes[sequence] = event["result"]

    def delivery(self, event):
        identity, reference = event["identity"], event["reference"]
        rows = event["records"]
        if not rows:
            self.failures.append(f"delivery {identity} is empty")
        if identity in self.deliveries and self.deliveries[identity] != rows:
            self.failures.append(f"delivery {identity} changed its delivered content")
        if reference in self.references:
            self.failures.append(f"delivery reference {reference} was read more than once")
        self.deliveries[identity] = rows
        self.references[reference] = identity
        for row in rows:
            self.check_record(row, "delivered content")

    def effect(self, event):
        identity, row = event["identity"], event["record"]
        sequence = self.check_record(row, "effect")
        if row not in self.deliveries.get(identity, []):
            self.failures.append(f"effect for record {sequence} has no matching delivery")
        self.effects[sequence] += 1
        if self.effects[sequence] > 1:
            self.failures.append(f"record {sequence} has more than one effect")

    def settlement(self, event):
        identity, reference = event["identity"], event["reference"]
        if self.references.get(reference) != identity:
            self.failures.append(f"settlement {reference} has no matching delivery")
        self.settlements[reference] += 1
        if self.settlements[reference] > 1:
            self.failures.append(f"reference {reference} settled more than once")
        if event["result"] == "confirmed":
            self.confirmed.add(reference)
        elif event["result"] not in ("stale_reference", "consumer_ended", "unknown"):
            self.failures.append(f"settlement {reference} ended with {event['result']}")

    def read_event(self, event):
        kind = event["kind"]
        if kind == "submitted":
            self.submitted(event)
        elif kind == "outcome":
            self.outcome(event)
        elif kind == "delivery":
            self.delivery(event)
        elif kind == "effect":
            self.effect(event)
        elif kind == "settlement":
            self.settlement(event)
        elif kind == "finished":
            self.finished.append(event["records"])
        elif kind == "error":
            self.failures.append(f"client error: {event['message']}")
        elif kind not in ("started", "interrupted", "publisher_finished"):
            self.failures.append(f"unknown event {kind}")

    def finish(self):
        for sequence in self.expected:
            if sequence not in self.submissions:
                self.failures.append(f"record {sequence} was never submitted")
            if sequence not in self.outcomes:
                self.failures.append(f"record {sequence} has no outcome")
            elif self.outcomes[sequence] == "completed":
                if self.effects[sequence] != 1:
                    self.failures.append(f"completed record {sequence} has {self.effects[sequence]} effects")
        for identity, rows in self.deliveries.items():
            for row in rows:
                if self.effects[row["sequence"]] != 1:
                    self.failures.append(f"delivered record {row['sequence']} has no exact effect")
                if row["sequence"] not in self.submissions:
                    self.failures.append(f"delivery {identity} contains unsubmitted work")
        for reference in self.references:
            if self.settlements[reference] == 0:
                self.failures.append(f"delivery reference {reference} has no settlement")
        if self.finished != [len(self.expected)]:
            self.failures.append("client has no single finished event for the complete input")
        if self.expected:
            if self.outcomes.get(min(self.expected)) != "completed":
                self.failures.append("first record did not complete before the fault")
            if self.outcomes.get(max(self.expected)) != "completed":
                self.failures.append("final record did not complete after recovery")
        return {
            "verdict": "fail" if self.failures else "pass",
            "failures": self.failures,
            "records": len(self.expected),
            "completed": sum(result == "completed" for result in self.outcomes.values()),
            "processing_failed": sum(result == "processing_failed" for result in self.outcomes.values()),
            "processing_failure_kinds": dict(sorted(self.processing_failure_kinds.items())),
            "unknown": sum(result == "outcome_unknown" for result in self.outcomes.values()),
            "effects": self.effects.total(),
            "deliveries": len(self.references),
            "confirmed_settlements": len(self.confirmed),
        }


def verify(records, events):
    evidence = Evidence(records)
    for event in events:
        evidence.read_event(event)
    return evidence.finish()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--result", required=True)
    args = parser.parse_args()
    try:
        verdict = verify(read_ledger(args.input), read_ledger(args.ledger))
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"binding evidence is incomplete or invalid: {error}", file=sys.stderr)
        return 2
    Path(args.result).write_text(json.dumps(verdict, indent=2) + "\n")
    return int(verdict["verdict"] != "pass")


if __name__ == "__main__":
    raise SystemExit(main())
