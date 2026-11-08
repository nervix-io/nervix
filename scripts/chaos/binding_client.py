"""Drive client endpoints through the maintained Nervix Python package, outside the nodes.

The producer calls submit once per input record and keeps reading that submission's outcome. The
consumer applies an idempotent effect by event identity and settles each delivery attempt once.
Every wait has a physical controller bound; losing a result never authorizes a second submission.
"""

import argparse
import json
from pathlib import Path
import struct
import sys
import threading
import time


def enum_name(value):
    return repr(value).rsplit(".", 1)[1].lower()


def publish_records(producer, records, *, build, cancel, emit, before_record, interval):
    for row in records:
        before_record(row["sequence"])
        submission = producer.submit(build(row), cancel=cancel())
        emit({"kind": "submitted", "sequence": row["sequence"], "submission": submission})
        outcome = producer.rejoin(submission, cancel=cancel())
        result = enum_name(outcome.result)
        event = {"kind": "outcome", "sequence": row["sequence"], "submission": submission,
                 "result": result}
        if result == "processing_failed":
            event.update(failure=enum_name(outcome.failure), message=outcome.message)
        emit(event)
        if interval:
            time.sleep(interval)


class Ledger:
    def __init__(self):
        self.lock = threading.Lock()

    def emit(self, event):
        with self.lock:
            print(json.dumps(event, separators=(",", ":")), flush=True)


class BindingClient:
    def __init__(self, nx, args, records):
        self.nx, self.args, self.records = nx, args, records
        self.ledger = Ledger()
        self.ends_at = time.monotonic() + args.timeout
        self.publisher_cancel = nx.Cancel(deadline=args.timeout)
        self.published = threading.Event()
        self.failed = threading.Event()
        self.failures = []
        self.effects = {}

    def cancel(self, bound=240):
        remaining = self.ends_at - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("the binding client's whole-run bound expired")
        return self.nx.Cancel(deadline=min(bound, remaining))

    def before_record(self, sequence):
        hold = Path(self.args.control) / "hold-binding"
        while True:
            try:
                limit = int(hold.read_text())
            except FileNotFoundError:
                break
            if sequence <= limit:
                break
            self.cancel()
            if self.failed.wait(0.05):
                raise RuntimeError("the consumer failed while the publisher held")

    def build(self, schema, row):
        builder = self.nx.BatchBuilder(schema, 1)
        for column, name in ((0, "event_id"), (2, "content")):
            value = row[name].encode()
            builder.varlen(column, 0, [0, len(value)], value)
        builder.fixed(1, 0, struct.pack("<Q", row["sequence"]))
        return builder.finish()

    def publish(self, producer):
        try:
            publish_records(producer, self.records, build=lambda row: self.build(producer.schema, row),
                            cancel=lambda: self.publisher_cancel, emit=self.ledger.emit, before_record=self.before_record,
                            interval=self.args.interval_ms / 1000)
            self.ledger.emit({"kind": "publisher_finished", "records": len(self.records)})
        except Exception as error:
            self.failures.append(error)
            self.failed.set()
            self.ledger.emit({"kind": "error", "message": f"publisher: {error!r}"})
        finally:
            self.published.set()

    def consume(self, consumer):
        while not self.published.is_set():
            if self.failed.is_set():
                raise RuntimeError("the publisher failed")
            try:
                delivery = consumer.next(cancel=self.cancel(1))
            except self.nx.DeadlineError:
                continue
            except self.nx.ConsumerInterruptedError:
                self.ledger.emit({"kind": "interrupted"})
                continue
            identity, reference = delivery.identity.hex(), delivery.reference.hex()
            batch = delivery.batch
            if batch.row_count != delivery.members:
                raise RuntimeError("a delivery's row count differs from its member count")
            columns = [batch.column(column) for column in range(3)]
            rows = [dict(zip(("event_id", "sequence", "content"), values, strict=True))
                    for values in zip(*columns, strict=True)]
            self.ledger.emit({"kind": "delivery", "identity": identity, "reference": reference,
                              "records": rows})
            for row in rows:
                prior = self.effects.get(row["event_id"])
                if prior is None:
                    self.effects[row["event_id"]] = row
                    self.ledger.emit({"kind": "effect", "identity": identity, "record": row})
                elif prior != row:
                    raise RuntimeError("a repeated event changed its content")
            try:
                result = enum_name(delivery.ack(cancel=self.cancel()))
                event = {"kind": "settlement", "identity": identity, "reference": reference,
                         "result": result}
            except self.nx.ClientError as error:
                event = {"kind": "settlement", "identity": identity, "reference": reference,
                         "result": "unknown", "failure_kind": enum_name(error.kind)}
            self.ledger.emit(event)

    def run(self):
        nx = self.nx
        fields = nx.Fields()
        fields.add("event_id", nx.FieldType.STRING)
        fields.add("sequence", nx.FieldType.U64)
        fields.add("content", nx.FieldType.STRING)
        with nx.Session(self.args.uri, domain=self.args.domain, username=self.args.username,
                        password=self.args.password, seeds=self.args.seeds.split(","),
                        connect_timeout=30, request_timeout=240, cancel=self.cancel()) as session:
            with session.subscribe_emitter(self.args.domain, "chaos_binding_out", fields,
                                          batches=4, bytes=1048576, cancel=self.cancel()) as consumer:
                with session.open_ingestor(self.args.domain, "chaos_binding_in", fields,
                                           batches=1, bytes=1048576, cancel=self.cancel()) as producer:
                    self.ledger.emit({"kind": "started", "records": len(self.records)})
                    publisher = threading.Thread(target=self.publish, args=(producer,))
                    publisher.start()
                    try:
                        self.consume(consumer)
                    except Exception:
                        self.failed.set()
                        self.publisher_cancel.trigger()
                        raise
                    finally:
                        publisher.join(max(0, self.ends_at - time.monotonic()))
                    if publisher.is_alive():
                        raise TimeoutError("publisher did not finish within its controller bound")
                    if self.failures:
                        raise RuntimeError("publisher failed") from self.failures[0]
        self.ledger.emit({"kind": "finished", "records": len(self.records)})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--seeds", required=True)
    parser.add_argument("--domain", default="chaos_binding")
    parser.add_argument("--username", default="default")
    parser.add_argument("--password", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--control", required=True)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--interval-ms", type=int, default=250)
    args = parser.parse_args()
    import nervix_client as nx

    records = [json.loads(line) for line in Path(args.input).read_text().splitlines()]
    BindingClient(nx, args, records).run()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"binding client failed: {error!r}", file=sys.stderr, flush=True)
        raise SystemExit(1) from error
