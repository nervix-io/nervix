"""Judge connector observations; Prometheus owns parsing its exposition format.

Layer: test harness.
Owns: zero registration and progress evidence for one Kafka boundary's physical owner.
Depends on: retained public scrapes, DESCRIBE output and the official Prometheus parser.
Must not know: node storage, runtime implementation or another process's counter totals.
"""

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path

from prometheus_client.parser import text_string_to_metric_families


class EvidenceError(ValueError):
    """The public observations do not establish the requested invariant."""


@dataclass
class Observation:
    kind: str
    node: str
    values: dict

    @staticmethod
    def requirements(kind):
        if kind == "ingestor":
            return {
                ("nervix_ingestor_read_payloads_total", ()),
                ("nervix_ingestor_read_bytes_total", ()),
                ("nervix_ingestor_ack_outcomes_total", (("outcome", "acknowledged"),)),
            }
        required = set()
        for operation in ("publish", "commit"):
            for outcome in ("succeeded", "transient", "misconfigured", "violation"):
                required.add((
                    "nervix_emitter_publish_attempts_total",
                    (("operation", operation), ("outcome", outcome)),
                ))
            required.add((
                "nervix_emitter_publish_duration_seconds_count",
                (("operation", operation),),
            ))
        return required

    @classmethod
    def read(cls, text, kind, node):
        required = cls.requirements(kind)
        names = {name for name, _ in required}
        target = f"chaos_{kind}"
        identity = {"domain", kind, "physical_node_id", "connector", "entity"}
        if kind == "ingestor":
            identity.add("instance")
        values = {}
        try:
            for family in text_string_to_metric_families(text):
                for sample in family.samples:
                    if sample.name not in names or sample.labels.get(kind) != target:
                        continue
                    if sample.labels.get("domain") != "chaos_baseline":
                        continue
                    if sample.labels.get("physical_node_id") != node:
                        raise EvidenceError("sample names another physical node")
                    if sample.labels.get("connector") != "KAFKA" or sample.labels.get("entity") != "TOPIC":
                        raise EvidenceError("sample names another connector entity")
                    if kind == "ingestor" and sample.labels.get("instance") != "0":
                        raise EvidenceError("sample names another ingestor instance")
                    extras = tuple(sorted((key, value) for key, value in sample.labels.items() if key not in identity))
                    key = (sample.name, extras)
                    if key not in required:
                        if any(name not in {"operation", "outcome"} for name, _ in extras):
                            raise EvidenceError("sample has unexpected labels")
                        continue
                    if key in values:
                        raise EvidenceError("duplicate connector sample")
                    value = sample.value
                    if not math.isfinite(value) or value < 0 or not value.is_integer():
                        raise EvidenceError("connector counter is not a finite nonnegative integer")
                    values[key] = int(value)
        except EvidenceError:
            raise
        except (ValueError, TypeError) as error:
            raise EvidenceError("malformed Prometheus observation") from error
        missing = required - values.keys()
        if missing:
            raise EvidenceError(f"missing connector children: {sorted(missing)}")
        return cls(kind, node, values)

    def require_zero(self):
        for key, value in self.values.items():
            if value != 0:
                raise EvidenceError(f"fresh owner must register zero for {key}")

    def require_description(self, text):
        if f"owner: {self.node}" not in text.splitlines():
            raise EvidenceError("DESCRIBE names another owner node")
        section = "source:" if self.kind == "ingestor" else "destination:"
        if section not in text.splitlines():
            raise EvidenceError("DESCRIBE lacks the connector interaction section")

    def require_progress(self, after):
        if self.kind != after.kind or self.node != after.node:
            raise EvidenceError("progress names another boundary or physical node")
        for key, before in self.values.items():
            if after.values[key] < before:
                raise EvidenceError(f"counter decreased for {key}")
        if self.kind == "ingestor":
            advancing = self.requirements(self.kind)
        else:
            advancing = {
                ("nervix_emitter_publish_attempts_total", (("operation", "publish"), ("outcome", "succeeded"))),
                ("nervix_emitter_publish_duration_seconds_count", (("operation", "publish"),)),
            }
        for key in advancing:
            if after.values[key] <= self.values[key]:
                raise EvidenceError(f"counter did not advance for {key}")

    def report(self):
        return [
            {"family": name, "labels": dict(labels), "value": value}
            for (name, labels), value in sorted(self.values.items())
        ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("zero", "progress"))
    parser.add_argument("--kind", choices=("ingestor", "emitter"), required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path)
    parser.add_argument("--description", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = {"phase": args.phase, "kind": args.kind, "physical_node_id": args.node}
    try:
        text = args.before.read_text()
        before = Observation.read(text, args.kind, args.node)
        report["before"] = before.report()
        # This identifies retained evidence; it makes no authenticity claim.
        report["before_sha256"] = hashlib.sha256(text.encode()).hexdigest()
        before.require_description(args.description.read_text())
        if args.phase == "zero":
            before.require_zero()
        else:
            if args.after is None:
                raise EvidenceError("progress requires a later scrape")
            after = Observation.read(args.after.read_text(), args.kind, args.node)
            before.require_progress(after)
            report["after"] = after.report()
        report["verdict"] = "pass"
        status = 0
    except (EvidenceError, OSError) as error:
        report.update(verdict="fail", error=str(error))
        status = 1
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
