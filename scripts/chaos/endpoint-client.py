"""The external endpoint client of the endpoints scenario.

Runs inside the pinned Python tool image and uses only the standard library. Two modes share one
ledger format, one NDJSON object per line on standard output, flushed as it is written, so the
runner reads the ledger from the container's log and the client writes nothing into the run's
directories:

http       Posts every record of the input file to an HTTP endpoint route, one record due per
           interval on the monotonic clock, through the nodes in turn. An attempt that ends without
           HTTP 200 is attempted again through the next node: the sender owns retry. Every attempt
           is one ledger line, so the verifier knows which records the endpoint accepted and which
           it may have delivered without saying so.
websocket  Opens one WebSocket connection, sends its records as text frames, waits for the runner
           to replace the source, sends one more frame on the retained connection and records how
           the endpoint closes it, then opens a new connection and sends the remaining records.

Hold and stop files follow the continuous load's contract: while the hold file exists and names a
count of records the client has already sent, it waits, and the runner releases it by removing the
file. An empty hold file holds the client where it stands.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import json
import os
import pathlib
import socket
import struct
import sys
import time

WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
# The HTTP status of an acknowledged endpoint request whose graph acknowledgement completed.
ACCEPTED = 200


def monotonic_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def wall_ms() -> int:
    return time.time_ns() // 1_000_000


class Ledger:
    """Writes one JSON object per line to standard output and flushes it, so a killed client loses
    no written line."""

    def write(self, entry: dict) -> None:
        entry["at_ms"] = wall_ms()
        sys.stdout.write(json.dumps(entry, separators=(",", ":"), sort_keys=True) + "\n")
        sys.stdout.flush()


class Control:
    """The runner's hold and stop files."""

    def __init__(self, directory: pathlib.Path, hold: str, stop: str) -> None:
        self.hold = directory / hold
        self.stop = directory / stop

    def stopped(self) -> bool:
        return self.stop.exists()

    def held(self, produced: int) -> bool:
        if not self.hold.exists():
            return False
        try:
            boundary = self.hold.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return False
        if not boundary:
            return True
        return produced >= int(boundary)

    def wait_while_held(self, produced: int) -> None:
        while self.held(produced) and not self.stopped():
            time.sleep(0.2)


def read_records(path: pathlib.Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [line for line in lines if line]


def parse_target(target: str) -> tuple[str, int]:
    host, separator, port = target.rpartition(":")
    if not separator or not host:
        raise SystemExit(f"endpoint client: target {target!r} is not HOST:PORT")
    return host, int(port)


def post_once(target: str, host_header: str, path: str, body: bytes, timeout: float) -> dict:
    """One HTTP attempt. Returns the attempt's status, retry delay and outcome object, or its error."""
    address, port = parse_target(target)
    connection = http.client.HTTPConnection(address, port, timeout=timeout)
    try:
        connection.request("POST", path, body=body, headers={
            "Host": host_header,
            "Content-Type": "application/json",
            "Connection": "close",
        })
        response = connection.getresponse()
        payload = response.read(4096)
        retry_after = response.getheader("Retry-After")
        outcome = None
        try:
            decoded = json.loads(payload.decode("utf-8")) if payload else None
        except (UnicodeDecodeError, ValueError):
            decoded = None
        if isinstance(decoded, dict):
            outcome = decoded
        return {"status": response.status, "retry_after": retry_after, "outcome": outcome}
    except OSError as error:
        return {"status": None, "error": f"{type(error).__name__}: {error}"}
    except http.client.HTTPException as error:
        return {"status": None, "error": f"{type(error).__name__}: {error}"}
    finally:
        connection.close()


def retry_delay_seconds(attempt: dict) -> float:
    retry_after = attempt.get("retry_after")
    if retry_after is not None:
        try:
            return max(0.2, float(retry_after))
        except ValueError:
            return 1.0
    if attempt.get("status") is None:
        return 0.5
    return 1.0


def run_http(arguments: argparse.Namespace) -> int:
    targets = arguments.targets.split(",")
    records = read_records(pathlib.Path(arguments.input))
    ledger = Ledger()
    control = Control(pathlib.Path(arguments.control), arguments.hold_file, arguments.stop_file)
    interval_ms = arguments.interval_ms
    catch_up_ms = interval_ms // 2
    produced = 0
    due_ms = monotonic_ms()
    for record in records:
        now = monotonic_ms()
        if now < due_ms:
            time.sleep((due_ms - now) / 1000)
        control.wait_while_held(produced)
        if control.stopped():
            break
        sequence = json.loads(record)["sequence"]
        body = record.encode("utf-8")
        deadline_ms = monotonic_ms() + arguments.record_deadline_seconds * 1000
        attempt_number = 0
        accepted = False
        while not accepted:
            target = targets[(sequence + attempt_number) % len(targets)]
            started_ms = wall_ms()
            attempt = post_once(target, arguments.host, arguments.path, body, arguments.attempt_timeout_seconds)
            ledger.write({"kind": "http_attempt", "sequence": sequence, "attempt": attempt_number,
                          "target": target, "started_ms": started_ms, **attempt})
            attempt_number += 1
            if attempt.get("status") == ACCEPTED:
                accepted = True
                break
            if monotonic_ms() >= deadline_ms:
                ledger.write({"kind": "http_abandoned", "sequence": sequence, "attempts": attempt_number})
                break
            if control.stopped():
                ledger.write({"kind": "http_interrupted", "sequence": sequence, "attempts": attempt_number})
                return 0
            time.sleep(retry_delay_seconds(attempt))
        produced += 1
        now = monotonic_ms()
        if now - due_ms <= catch_up_ms:
            due_ms += interval_ms
        else:
            due_ms = now + interval_ms
    ledger.write({"kind": "http_finished", "produced": produced})
    return 0


class WebSocket:
    """A minimal RFC 6455 client: one connection, masked text frames out, frames read until close."""

    def __init__(self, target: str, host_header: str, path: str, timeout: float) -> None:
        address, port = parse_target(target)
        self.socket = socket.create_connection((address, port), timeout=timeout)
        self.buffer = b""
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (f"GET {path} HTTP/1.1\r\nHost: {host_header}\r\nUpgrade: websocket\r\n"
                   f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n")
        self.socket.sendall(request.encode("ascii"))
        head = self.read_until(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        self.status = int(lines[0].split(" ")[1])
        headers = {}
        for line in lines[1:]:
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
        expected = base64.b64encode(hashlib.sha1((key + WEBSOCKET_GUID).encode("ascii")).digest()).decode("ascii")
        self.accepted = self.status == 101 and headers.get("sec-websocket-accept") == expected

    def read_until(self, marker: bytes) -> bytes:
        while marker not in self.buffer:
            chunk = self.socket.recv(4096)
            if not chunk:
                raise ConnectionError("the connection ended during the opening handshake")
            self.buffer += chunk
        head, _, self.buffer = self.buffer.partition(marker)
        return head

    def read_exact(self, count: int) -> bytes:
        while len(self.buffer) < count:
            chunk = self.socket.recv(4096)
            if not chunk:
                raise ConnectionError("the connection ended inside a frame")
            self.buffer += chunk
        data, self.buffer = self.buffer[:count], self.buffer[count:]
        return data

    def send_text(self, text: str) -> None:
        payload = text.encode("utf-8")
        mask = os.urandom(4)
        length = len(payload)
        if length < 126:
            header = struct.pack("!BB", 0x81, 0x80 | length)
        elif length < 65536:
            header = struct.pack("!BBH", 0x81, 0x80 | 126, length)
        else:
            header = struct.pack("!BBQ", 0x81, 0x80 | 127, length)
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.socket.sendall(header + mask + masked)

    def read_close(self, timeout: float) -> dict:
        """Reads frames until a close frame or the connection's end; answers pings."""
        self.socket.settimeout(timeout)
        try:
            while True:
                first, second = self.read_exact(2)
                opcode = first & 0x0F
                length = second & 0x7F
                if length == 126:
                    (length,) = struct.unpack("!H", self.read_exact(2))
                elif length == 127:
                    (length,) = struct.unpack("!Q", self.read_exact(8))
                payload = self.read_exact(length)
                if opcode == 0x8:
                    code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else None
                    return {"closed": "frame", "code": code, "reason": payload[2:].decode("utf-8", "replace")}
                if opcode == 0x9:
                    mask = os.urandom(4)
                    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
                    self.socket.sendall(struct.pack("!BB", 0x8A, 0x80 | len(payload)) + mask + masked)
        except (ConnectionError, OSError) as error:
            return {"closed": "connection", "code": None, "reason": f"{type(error).__name__}: {error}"}

    def close(self) -> None:
        try:
            mask = os.urandom(4)
            payload = struct.pack("!H", 1000)
            masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
            self.socket.sendall(struct.pack("!BB", 0x88, 0x80 | len(payload)) + mask + masked)
        except OSError:
            pass
        self.socket.close()


def wait_for_file(path: pathlib.Path, control: Control, timeout_seconds: int) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while not path.exists():
        if control.stopped() or time.monotonic() >= deadline:
            return False
        time.sleep(0.2)
    return True


def send_frames(connection: WebSocket, label: str, records: list[str], ledger: Ledger, interval_ms: int) -> None:
    for record in records:
        sequence = json.loads(record)["sequence"]
        connection.send_text(record)
        ledger.write({"kind": "ws_sent", "connection": label, "sequence": sequence})
        time.sleep(interval_ms / 1000)


def run_websocket(arguments: argparse.Namespace) -> int:
    records = read_records(pathlib.Path(arguments.input))
    retained, probe, replacement = (records[:arguments.retained_records],
                                    records[arguments.retained_records],
                                    records[arguments.retained_records + 1:])
    ledger = Ledger()
    control = Control(pathlib.Path(arguments.control), arguments.hold_file, arguments.stop_file)
    first = WebSocket(arguments.target, arguments.host, arguments.path, arguments.attempt_timeout_seconds)
    ledger.write({"kind": "ws_open", "connection": "retained", "target": arguments.target,
                  "status": first.status, "accepted": first.accepted})
    if not first.accepted:
        return 1
    send_frames(first, "retained", retained, ledger, arguments.interval_ms)
    ledger.write({"kind": "ws_retained_sent", "count": len(retained)})
    replaced = pathlib.Path(arguments.control) / arguments.replaced_file
    if not wait_for_file(replaced, control, arguments.replacement_timeout_seconds):
        ledger.write({"kind": "ws_replacement_missing"})
        first.close()
        return 1
    probe_sequence = json.loads(probe)["sequence"]
    first.send_text(probe)
    ledger.write({"kind": "ws_probe_sent", "connection": "retained", "sequence": probe_sequence})
    ledger.write({"kind": "ws_retained_closed", "connection": "retained",
                  **first.read_close(arguments.close_timeout_seconds)})
    first.socket.close()
    second = WebSocket(arguments.target, arguments.host, arguments.path, arguments.attempt_timeout_seconds)
    ledger.write({"kind": "ws_open", "connection": "replacement", "target": arguments.target,
                  "status": second.status, "accepted": second.accepted})
    if not second.accepted:
        return 1
    send_frames(second, "replacement", replacement, ledger, arguments.interval_ms)
    ledger.write({"kind": "ws_finished", "count": len(replacement)})
    second.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    modes = parser.add_subparsers(dest="mode", required=True)
    for mode in ("http", "websocket"):
        subparser = modes.add_parser(mode)
        subparser.add_argument("--host", required=True, help="the Host header naming the endpoint's VHOST")
        subparser.add_argument("--path", required=True)
        subparser.add_argument("--input", required=True, help="NDJSON records, one JSON object with a sequence each")
        subparser.add_argument("--control", required=True, help="the directory holding the runner's control files")
        subparser.add_argument("--hold-file", default="hold-endpoint")
        subparser.add_argument("--stop-file", default="stop-endpoint")
        subparser.add_argument("--interval-ms", type=int, required=True)
        subparser.add_argument("--attempt-timeout-seconds", type=float, default=15.0)
    http_mode = modes.choices["http"]
    http_mode.add_argument("--targets", required=True, help="comma-separated HOST:PORT node listeners")
    http_mode.add_argument("--record-deadline-seconds", type=int, default=240)
    websocket_mode = modes.choices["websocket"]
    websocket_mode.add_argument("--target", required=True, help="the HOST:PORT node listener to connect to")
    websocket_mode.add_argument("--retained-records", type=int, required=True)
    websocket_mode.add_argument("--replaced-file", default="websocket-source-replaced")
    websocket_mode.add_argument("--replacement-timeout-seconds", type=int, default=600)
    websocket_mode.add_argument("--close-timeout-seconds", type=float, default=30.0)
    arguments = parser.parse_args()
    if arguments.mode == "http":
        return run_http(arguments)
    return run_websocket(arguments)


if __name__ == "__main__":
    sys.exit(main())
