"""What the suite's tests share: the cluster the scenario started, bounded waits, a listener that
never answers, a TCP relay a test cuts, and the typed rows the endpoint tests publish.

Every wait in the suite is a `Cancel` deadline or a bounded poll of a condition. The bound is
generous because scenarios run many at a time on one machine; a wait ends as soon as its condition
holds, so the bound costs nothing when nothing is wrong.
"""

from __future__ import annotations

import os
import socket
import struct
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import nervix_client as nx

# How long a test waits for something the cluster does.
WAIT_SECONDS = 120.0


def deadline(seconds: float = WAIT_SECONDS) -> nx.Cancel:
    """A token that ends a wait after `seconds`."""

    return nx.Cancel(deadline=seconds)


def wait_until(condition: Callable[[], bool], what: str, within: float = WAIT_SECONDS) -> None:
    """Polls `condition` until it holds, and fails naming `what` once `within` seconds pass."""

    ends = time.monotonic() + within
    while not condition():
        if time.monotonic() > ends:
            raise AssertionError(f"{what} did not happen within {within} seconds")
        time.sleep(0.01)


def execute(session: nx.Session, query: str) -> nx.Outcome:
    """Prepares and runs `query`, bounded by the suite's wait."""

    return session.execute(session.prepare(query), cancel=deadline())


def completed(session: nx.Session, query: str) -> nx.Outcome:
    """Runs `query` and fails the test with the server's message unless it completes."""

    outcome = execute(session, query)
    assert outcome.disposition == nx.Disposition.COMPLETED, (query, outcome.message)
    return outcome


@dataclass(frozen=True)
class Cluster:
    """The scenario cluster the cluster tier runs against, as the harness describes it."""

    # The session listener of the node the suite starts on, which serves TLS.
    server: str
    # The listener of the node that led when the scenario started the suite.
    leader: str
    # The session listener of every node.
    seeds: tuple[str, ...]
    username: str
    password: str
    domain: str
    # The certificate authority the listeners' certificates chain to, as PEM.
    ca_certificate: bytes
    # A directory for backup archives, which the scenario removes.
    backups: Path

    @classmethod
    def from_environment(cls) -> Cluster:
        environment = os.environ
        missing = [name for name in CLUSTER_ENVIRONMENT if not environment.get(name)]
        if missing:
            raise RuntimeError(
                f"the cluster tier needs {', '.join(missing)}; "
                "`tests/features/runtime/client_suites.feature` sets them"
            )
        authority = Path(environment["NERVIX_SUITE_CA_CERTIFICATE"]).read_bytes()
        return cls(
            server=environment["NERVIX_PROBE_GRPC_URI"],
            leader=environment["NERVIX_SUITE_LEADER_GRPC_URI"],
            seeds=tuple(environment["NERVIX_SUITE_SEEDS"].split(",")),
            username=environment["NERVIX_PROBE_USERNAME"],
            password=environment["NERVIX_PROBE_PASSWORD"],
            domain=environment["NERVIX_PROBE_DOMAIN"],
            ca_certificate=authority,
            backups=Path(environment["NERVIX_SUITE_BACKUP_DIRECTORY"]),
        )

    def connect(self, server: str | None = None, domain: str | None = None) -> nx.Session:
        """A session on `server`, or on the node the suite starts on, in `domain`, or in the
        scenario's domain, authenticated as the scenario's user on its listener transport."""

        return nx.Session(
            server or self.server,
            domain=domain or self.domain,
            username=self.username,
            password=self.password,
            ca_certificate=self.ca_certificate,
            cancel=deadline(),
        )


CLUSTER_ENVIRONMENT = (
    "NERVIX_PROBE_GRPC_URI",
    "NERVIX_SUITE_LEADER_GRPC_URI",
    "NERVIX_SUITE_SEEDS",
    "NERVIX_PROBE_USERNAME",
    "NERVIX_PROBE_PASSWORD",
    "NERVIX_PROBE_DOMAIN",
    "NERVIX_SUITE_CA_CERTIFICATE",
    "NERVIX_SUITE_BACKUP_DIRECTORY",
)


class SilentListener:
    """A TCP listener that accepts connections and never answers on them, standing in for a
    server that never replies. It counts what it accepted."""

    def __init__(self) -> None:
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.listener.settimeout(0.05)
        self.port: int = self.listener.getsockname()[1]
        self.accepted: list[socket.socket] = []
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    @property
    def uri(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def accept(self) -> None:
        while not self.stopping.is_set():
            try:
                connection, _ = self.listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self.accepted.append(connection)

    def close(self) -> None:
        self.stopping.set()
        self.thread.join()
        self.listener.close()
        for connection in self.accepted:
            connection.close()


def unused_port() -> int:
    """A loopback port nothing listens on once this returns."""

    with socket.create_server(("127.0.0.1", 0)) as probe:
        port: int = probe.getsockname()[1]
    return port


class Relay:
    """A TCP relay in front of one node's session listener. Cutting it ends every session that
    reached the node through it, and restoring it lets them reconnect at the same address. It
    forwards the encrypted bytes as they are, so TLS still ends at the node."""

    # The thread that accepts at the relay's address while it listens.
    accepting: threading.Thread

    def __init__(self, target: str) -> None:
        parsed = urllib.parse.urlsplit(target)
        if parsed.hostname is None or parsed.port is None:
            raise ValueError(f"{target} names no host and port")
        self.scheme = parsed.scheme
        self.target = (parsed.hostname, parsed.port)
        self.port = unused_port()
        self.lock = threading.Lock()
        self.connections: list[socket.socket] = []
        self.stopping = threading.Event()
        self.restore()

    @property
    def uri(self) -> str:
        return f"{self.scheme}://127.0.0.1:{self.port}"

    def restore(self) -> None:
        """Listens again at the relay's address."""

        self.stopping = threading.Event()
        listener = socket.create_server(("127.0.0.1", self.port))
        listener.settimeout(0.05)
        self.accepting = threading.Thread(
            target=self.accept, args=(listener, self.stopping), daemon=True
        )
        self.accepting.start()

    def accept(self, listener: socket.socket, stopping: threading.Event) -> None:
        """Relays every connection `listener` accepts until `stopping` is set, then closes it."""

        with listener:
            while not stopping.is_set():
                try:
                    client, _ = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    return
                try:
                    upstream = socket.create_connection(self.target)
                except OSError:
                    client.close()
                    continue
                with self.lock:
                    self.connections.extend((client, upstream))
                threading.Thread(target=pump, args=(client, upstream), daemon=True).start()
                threading.Thread(target=pump, args=(upstream, client), daemon=True).start()

    def cut(self) -> None:
        """Stops listening and ends every connection through the relay."""

        self.stopping.set()
        # The accepting thread closes the listener once it sees the stop, so the address is free
        # for `restore`, and every connection it accepted before then is registered for the cut.
        if self.accepting.is_alive():
            self.accepting.join(timeout=WAIT_SECONDS)
            if self.accepting.is_alive():
                raise AssertionError(f"the relay still listened after {WAIT_SECONDS} seconds")
        with self.lock:
            connections, self.connections = self.connections, []
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()

    def close(self) -> None:
        self.cut()


def pump(source: socket.socket, target: socket.socket) -> None:
    """Copies what `source` sends to `target` until either end closes."""

    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            target.sendall(data)
    except OSError:
        pass
    try:
        target.shutdown(socket.SHUT_WR)
    except OSError:
        pass


# ---- The endpoint graph ------------------------------------------------------------------------

# The input schema of the scenario's client ingestors, `typed_in` and `watched_in`, in declared
# order: each field's name, the (type, length) of every level of its type, and whether it is
# nullable and sensitive. The emitter `typed_out` adds `echo`, a copy of `id`.
INPUT_FIELDS: tuple[tuple[str, tuple[tuple[str, int], ...], bool, bool], ...] = (
    ("id", (("U32", 0),), False, False),
    ("tenant", (("STRING", 0),), False, False),
    ("amount", (("I64", 0),), False, False),
    ("ratio", (("F64", 0),), False, False),
    ("flag", (("BOOL", 0),), False, False),
    ("note", (("STRING", 0),), True, False),
    ("raw", (("BYTES", 0),), True, False),
    ("at", (("DATETIME", 0),), False, False),
    ("tags", (("LIST", 0), ("STRING", 0)), False, False),
    ("grid", (("FIXED_LIST", 2), ("I16", 0)), False, False),
    ("secret", (("STRING", 0),), False, True),
)
ECHO_FIELD = ("echo", (("U32", 0),), False, False)


def fields_of(specs: Iterable[tuple[str, tuple[tuple[str, int], ...], bool, bool]]) -> nx.Fields:
    """The fields `specs` describe, one type level at a time."""

    fields = nx.Fields()
    for name, levels, nullable, sensitive in specs:
        kind, length = levels[0]
        fields.add(
            name, getattr(nx.FieldType, kind), length, nullable=nullable, sensitive=sensitive
        )
        for kind, length in levels[1:]:
            fields.element(getattr(nx.FieldType, kind), length)
    return fields


def input_fields() -> nx.Fields:
    return fields_of(INPUT_FIELDS)


def output_fields() -> nx.Fields:
    return fields_of(INPUT_FIELDS + (ECHO_FIELD,))


@dataclass(frozen=True)
class Row:
    """One row of the endpoint graph's input. `note` and `raw` are optional."""

    id: int
    tenant: str = "acme"
    amount: int = 0
    ratio: float = 0.0
    flag: bool = False
    note: str | None = None
    raw: bytes | None = None
    at: int = 0
    tags: tuple[str, ...] = ()
    grid: tuple[int, int] = (0, 0)
    secret: str = "s"


def varlen(values: Sequence[bytes]) -> tuple[list[int], bytes]:
    """The offsets, from zero with a final end, and the concatenation of `values`."""

    offsets = [0]
    data = bytearray()
    for value in values:
        data += value
        offsets.append(len(data))
    return offsets, bytes(data)


def build(schema: nx.Schema, rows: Sequence[Row]) -> nx.Batch:
    """A batch of `rows` for the endpoint graph's input schema, or for the emitter's output schema
    with `echo` copying `id`, built one level at a time."""

    builder = nx.BatchBuilder(schema, len(rows))
    builder.fixed(0, 0, struct.pack(f"={len(rows)}I", *(row.id for row in rows)))
    builder.varlen(1, 0, *varlen([row.tenant.encode() for row in rows]))
    builder.fixed(2, 0, struct.pack(f"={len(rows)}q", *(row.amount for row in rows)))
    builder.fixed(3, 0, struct.pack(f"={len(rows)}d", *(row.ratio for row in rows)))
    builder.fixed(4, 0, bytes(int(row.flag) for row in rows))
    builder.states(5, [row.note is not None for row in rows])
    builder.varlen(5, 0, *varlen([(row.note or "").encode() for row in rows]))
    builder.states(6, [row.raw is not None for row in rows])
    builder.varlen(6, 0, *varlen([row.raw or b"" for row in rows]))
    builder.fixed(7, 0, struct.pack(f"={len(rows)}q", *(row.at for row in rows)))
    lists = [0]
    for row in rows:
        lists.append(lists[-1] + len(row.tags))
    builder.offsets(8, 0, lists)
    builder.varlen(8, 1, *varlen([tag.encode() for row in rows for tag in row.tags]))
    cells = [cell for row in rows for cell in row.grid]
    builder.fixed(9, 1, struct.pack(f"={len(cells)}h", *cells))
    builder.varlen(10, 0, *varlen([row.secret.encode() for row in rows]))
    if len(schema.fields) > len(INPUT_FIELDS):
        builder.fixed(11, 0, struct.pack(f"={len(rows)}I", *(row.id for row in rows)))
    return builder.finish()


def ids_of(batch: nx.Batch) -> list[int]:
    """The `id` column of a batch of the endpoint graph."""

    ids = batch.column(0)
    assert all(isinstance(value, int) for value in ids), ids
    return [value for value in ids if isinstance(value, int)]


def next_of(consumer: nx.Consumer, ids: Sequence[int]) -> nx.Delivery:
    """The next delivery whose rows are `ids`. A delivery of rows another test left behind is
    acknowledged and skipped, so one test's leftovers never decide another's outcome."""

    while True:
        delivery = consumer.next(cancel=deadline())
        if ids_of(delivery.batch) == list(ids):
            return delivery
        delivery.ack(cancel=deadline())
