#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "cbor2>=5.6",
#   "kafka-python>=2.0",
#   "nats-py>=2.7",
#   "paho-mqtt>=2.0",
# ]
# ///
"""Stream source-specific datalake activity into examples/datalake/datalake.nspl."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import math
import random
import sys
import time
from dataclasses import dataclass
from typing import Iterable


TENANTS = ("tenant-a", "tenant-b", "tenant-c")
DEVICES = ("device-001", "device-002", "device-003", "device-004")
AVAILABLE_SOURCES = ("device", "edge", "auth")
DEFAULT_SOURCES = AVAILABLE_SOURCES
DBIP_SAMPLE_IPS = (
    "8.8.8.187",
    "1.1.1.44",
    "9.9.9.42",
    "80.80.80.30",
    "185.199.108.133",
)
MQTT_INFLIGHT_MESSAGES = 1000
MQTT_PENDING_BACKPRESSURE = 10_000
MQTT_PENDING_DRAIN_BATCH = 1000


@dataclass(frozen=True)
class EdgeSite:
    edge_id: str
    edge_name: str
    lat: float
    lon: float
    protocol: str


@dataclass(frozen=True)
class SourceEvent:
    source: str
    payload: dict[str, object]


EDGE_SITES = (
    EdgeSite("edge-sfo-1", "edge-sfo-1", 37.7749, -122.4194, "mqtt"),
    EdgeSite("edge-ord-1", "edge-ord-1", 41.8781, -87.6298, "coap"),
    EdgeSite("edge-zrh-1", "edge-zrh-1", 47.3769, 8.5417, "mqtt"),
    EdgeSite("edge-syd-1", "edge-syd-1", -33.8688, 151.2093, "coap"),
)


EDGE_PROTO_FIELDS = {
    "source": 1,
    "event_id": 2,
    "tenant_id": 3,
    "device_id": 4,
    "session_id": 5,
    "edge_id": 6,
    "edge_name": 7,
    "event_type": 8,
    "protocol": 9,
    "ts": 10,
    "seq": 11,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish synthetic IoT, edge, and auth datalake activity."
    )
    parser.add_argument("--rate", type=float, default=100.0, help="events per second")
    parser.add_argument("--duration", type=float, default=0.0, help="seconds; 0 runs forever")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="print JSON instead of publishing")
    parser.add_argument(
        "--progress-interval",
        type=float,
        default=1.0,
        help="seconds between progress reports; 0 disables progress",
    )
    parser.add_argument("--quiet", action="store_true", help="suppress progress output")
    parser.add_argument(
        "--sources",
        default=",".join(DEFAULT_SOURCES),
        help="comma-separated sources to publish to; use 'all' for device,edge,auth",
    )
    parser.add_argument(
        "--location-burst",
        type=int,
        default=2,
        help=(
            "device location reports emitted for each scenario with IoT activity; "
            "these MQTT records drive GeoIP WASM enrichment"
        ),
    )
    parser.add_argument("--kafka-bootstrap", default="127.0.0.1:9092")
    parser.add_argument("--auth-topic", default="datalake_auth_activity")
    parser.add_argument("--mqtt-host", default="127.0.0.1")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--device-topic", default="datalake/device_activity")
    parser.add_argument("--nats-server", default="nats://127.0.0.1:4222")
    parser.add_argument("--edge-subject", default="datalake_edge_activity")
    return parser.parse_args()


def import_cbor2():
    try:
        import cbor2
    except ImportError:
        print(
            "Missing dependency: cbor2. Run this script with `uv run generate_datalake_load.py`.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return cbor2


def import_kafka():
    try:
        from kafka import KafkaProducer
    except ImportError:
        print(
            "Missing dependency: kafka-python. Run this script with `uv run generate_datalake_load.py`.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return KafkaProducer


def import_mqtt():
    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        print(
            "Missing dependency: paho-mqtt. Run this script with `uv run generate_datalake_load.py`.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return mqtt


def import_nats():
    try:
        import nats
    except ImportError:
        print(
            "Missing dependency: nats-py. Run this script with `uv run generate_datalake_load.py`.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return nats


def mqtt_client(mqtt):
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="nervix-datalake-loadgen")
    except AttributeError:
        return mqtt.Client(client_id="nervix-datalake-loadgen")


def validate_args(args: argparse.Namespace) -> None:
    if args.rate <= 0:
        raise SystemExit("--rate must be greater than 0")
    if args.duration < 0:
        raise SystemExit("--duration must be greater than or equal to 0")
    if args.progress_interval < 0:
        raise SystemExit("--progress-interval must be greater than or equal to 0")
    if args.location_burst < 0:
        raise SystemExit("--location-burst must be greater than or equal to 0")
    args.sources = parse_sources(args.sources)


def parse_sources(raw: str) -> tuple[str, ...]:
    selected = tuple(source.strip().lower() for source in raw.split(",") if source.strip())
    if selected == ("all",):
        return AVAILABLE_SOURCES
    unknown = sorted(set(selected).difference(AVAILABLE_SOURCES))
    if unknown:
        allowed = ", ".join((*AVAILABLE_SOURCES, "all"))
        raise SystemExit(f"--sources contains unknown source(s) {unknown}; allowed: {allowed}")
    if not selected:
        raise SystemExit("--sources must include at least one source")
    return selected


def rfc3339_now(offset_ms: int = 0) -> str:
    now = dt.datetime.now(dt.UTC) + dt.timedelta(milliseconds=offset_ms)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def near(site: EdgeSite) -> tuple[float, float]:
    return (
        site.lat + random.uniform(-0.025, 0.025),
        site.lon + random.uniform(-0.025, 0.025),
    )


def far_from(site: EdgeSite) -> tuple[float, float]:
    candidates = [candidate for candidate in EDGE_SITES if candidate.edge_id != site.edge_id]
    far_site = max(candidates, key=lambda candidate: rough_distance(site, candidate))
    return near(far_site)


def rough_distance(left: EdgeSite, right: EdgeSite) -> float:
    return math.hypot(left.lat - right.lat, left.lon - right.lon)


def source_ip() -> str:
    return random.choice(DBIP_SAMPLE_IPS)


def scenario_stream(location_burst: int = 2) -> Iterable[SourceEvent]:
    seq = 0
    scenario = 0

    def location_reports(
        tenant_id: str,
        device_id: str,
        session_id: str,
        edge: EdgeSite,
        far: bool,
    ) -> Iterable[SourceEvent]:
        nonlocal seq
        for _ in range(location_burst):
            lat, lon = far_from(edge) if far else near(edge)
            seq += 1
            yield device_event(seq, tenant_id, device_id, session_id, edge, "location", lat, lon)

    while True:
        tenant_id = random.choice(TENANTS)
        device_id = random.choice(DEVICES)
        edge = random.choice(EDGE_SITES)
        session_id = f"{tenant_id}-{device_id}-{scenario:012d}"
        principal_id = f"user-{device_id[-3:]}"
        scenario_kind = scenario % 8

        if scenario_kind == 0:
            lat, lon = near(edge)
            yield device_event(seq := seq + 1, tenant_id, device_id, session_id, edge, "connect", lat, lon)
            yield edge_event(seq := seq + 1, tenant_id, device_id, session_id, edge, "connect")
            yield auth_event(
                seq := seq + 1,
                tenant_id,
                device_id,
                session_id,
                edge,
                principal_id,
                "authorized",
                "allow",
                random.uniform(0.01, 0.20),
            )
            yield from location_reports(tenant_id, device_id, session_id, edge, far=False)
        elif scenario_kind == 1:
            yield from location_reports(tenant_id, device_id, session_id, edge, far=False)
        elif scenario_kind == 2:
            yield from location_reports(tenant_id, device_id, session_id, edge, far=True)
        elif scenario_kind == 3:
            lat, lon = near(edge)
            yield from location_reports(tenant_id, device_id, session_id, edge, far=False)
            yield device_event(
                seq := seq + 1,
                tenant_id,
                device_id,
                session_id,
                edge,
                "disconnect",
                lat,
                lon,
            )
            yield edge_event(seq := seq + 1, tenant_id, device_id, session_id, edge, "disconnect")
        elif scenario_kind == 4:
            yield edge_event(seq := seq + 1, tenant_id, device_id, session_id, edge, "disconnect")
        elif scenario_kind == 5:
            yield auth_event(
                seq := seq + 1,
                tenant_id,
                device_id,
                session_id,
                edge,
                principal_id,
                "authorized",
                "deny",
                random.uniform(0.75, 0.99),
                "mfa_failed",
            )
        elif scenario_kind == 6:
            yield auth_event(
                seq := seq + 1,
                tenant_id,
                device_id,
                session_id,
                edge,
                principal_id,
                "authorized",
                "allow",
                random.uniform(0.30, 0.65),
            )
        else:
            lat, lon = far_from(edge)
            yield device_event(seq := seq + 1, tenant_id, device_id, session_id, edge, "connect", lat, lon)
            yield edge_event(seq := seq + 1, tenant_id, device_id, session_id, edge, "connect")
            yield from location_reports(tenant_id, device_id, session_id, edge, far=True)

        scenario += 1


def device_event(
    seq: int,
    tenant_id: str,
    device_id: str,
    session_id: str,
    edge: EdgeSite,
    event_type: str,
    lat: float,
    lon: float,
) -> SourceEvent:
    return SourceEvent(
        "device",
        {
            "source": "iot_device",
            "event_id": f"dev-{seq:012d}",
            "tenant_id": tenant_id,
            "device_id": device_id,
            "session_id": session_id,
            "edge_id": edge.edge_id,
            "event_type": event_type,
            "source_ip": source_ip(),
            "device_lat": round(lat, 6),
            "device_lon": round(lon, 6),
            "battery_pct": round(random.uniform(18.0, 99.0), 2),
            "firmware": random.choice(("fw-2026.04", "fw-2026.05", "fw-2026.06")),
            "ts": rfc3339_now(),
            "seq": seq,
        },
    )


def edge_event(
    seq: int,
    tenant_id: str,
    device_id: str,
    session_id: str,
    edge: EdgeSite,
    event_type: str,
) -> SourceEvent:
    return SourceEvent(
        "edge",
        {
            "source": "edge_server",
            "event_id": f"edge-{seq:012d}",
            "tenant_id": tenant_id,
            "device_id": device_id,
            "session_id": session_id,
            "edge_id": edge.edge_id,
            "edge_name": edge.edge_name,
            "event_type": event_type,
            "protocol": edge.protocol,
            "ts": rfc3339_now(10),
            "seq": seq,
        },
    )


def auth_event(
    seq: int,
    tenant_id: str,
    device_id: str,
    session_id: str,
    edge: EdgeSite,
    principal_id: str,
    event_type: str,
    auth_result: str,
    risk_score: float,
    reason: str | None = None,
) -> SourceEvent:
    payload = {
        "source": "auth_server",
        "event_id": f"auth-{seq:012d}",
        "tenant_id": tenant_id,
        "device_id": device_id,
        "session_id": session_id,
        "edge_id": edge.edge_id,
        "principal_id": principal_id,
        "event_type": event_type,
        "auth_result": auth_result,
        "risk_score": round(risk_score, 4),
        "ts": rfc3339_now(20),
        "seq": seq,
    }
    if reason is not None:
        payload["reason"] = reason
    return SourceEvent("auth", payload)


def encode_edge_activity(payload: dict[str, object]) -> bytes:
    encoded = bytearray()
    for field_name, field_number in EDGE_PROTO_FIELDS.items():
        value = payload[field_name]
        if field_name == "seq":
            encoded.extend(protobuf_key(field_number, 0))
            encoded.extend(protobuf_varint(int(value)))
        else:
            raw = str(value).encode("utf-8")
            encoded.extend(protobuf_key(field_number, 2))
            encoded.extend(protobuf_varint(len(raw)))
            encoded.extend(raw)
    return bytes(encoded)


def protobuf_key(field_number: int, wire_type: int) -> bytes:
    return protobuf_varint((field_number << 3) | wire_type)


def protobuf_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("protobuf varint requires non-negative value")
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def debug_wire_size(event: SourceEvent, cbor2=None) -> int:
    if event.source == "device":
        if cbor2 is None:
            return len(json.dumps(event.payload).encode("utf-8"))
        return len(cbor2.dumps(event.payload))
    if event.source == "edge":
        return len(encode_edge_activity(event.payload))
    return len(json.dumps(event.payload).encode("utf-8"))


class Progress:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.start = time.monotonic()
        self.next_report = self.start + args.progress_interval
        self.counts = {source: 0 for source in args.sources}
        self.total = 0

    def log(self, message: str) -> None:
        if not self.args.quiet:
            print(message, file=sys.stderr, flush=True)

    def start_report(self) -> None:
        duration = "forever" if self.args.duration == 0 else f"{self.args.duration:g}s"
        mode = "dry-run" if self.args.dry_run else "publish"
        self.log(
            "starting datalake load: "
            f"mode={mode}, sources={','.join(self.args.sources)}, "
            f"target_rate={self.args.rate:g}/s, duration={duration}, "
            f"location_burst={self.args.location_burst}"
        )

    def begin_publishing(self) -> None:
        self.start = time.monotonic()
        self.next_report = self.start + self.args.progress_interval

    def record(self, source: str) -> None:
        self.total += 1
        self.counts[source] += 1

    def elapsed(self) -> float:
        return max(time.monotonic() - self.start, 0.001)

    def maybe_report(self) -> None:
        if self.args.quiet or self.args.progress_interval == 0:
            return
        now = time.monotonic()
        if now < self.next_report:
            return
        elapsed = max(now - self.start, 0.001)
        counts = ", ".join(f"{source}={self.counts[source]}" for source in self.args.sources)
        self.log(
            f"published {self.total} events in {elapsed:.1f}s "
            f"actual_rate={self.total / elapsed:.1f}/s target_rate={self.args.rate:g}/s "
            f"({counts})"
        )
        self.next_report = now + self.args.progress_interval

    def final_report(self) -> None:
        elapsed = self.elapsed()
        counts = ", ".join(f"{source}={self.counts[source]}" for source in self.args.sources)
        self.log(
            f"finished datalake load: {self.total} events in {elapsed:.1f}s "
            f"actual_rate={self.total / elapsed:.1f}/s target_rate={self.args.rate:g}/s "
            f"({counts})"
        )


class Publishers:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.cbor2 = None
        self.kafka = None
        self.mqtt = None
        self.nats = None
        self.mqtt_pending = []

    async def start(self) -> None:
        if "device" in self.args.sources:
            self.cbor2 = import_cbor2()
        if self.args.dry_run:
            return
        if "auth" in self.args.sources:
            KafkaProducer = import_kafka()
            self.kafka = KafkaProducer(
                bootstrap_servers=self.args.kafka_bootstrap,
                max_block_ms=5_000,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )

        if "device" in self.args.sources:
            mqtt = import_mqtt()
            self.mqtt = mqtt_client(mqtt)
            self.mqtt.max_inflight_messages_set(MQTT_INFLIGHT_MESSAGES)
            self.mqtt.max_queued_messages_set(MQTT_PENDING_BACKPRESSURE)
            self.mqtt.connect(self.args.mqtt_host, self.args.mqtt_port)
            self.mqtt.loop_start()

        if "edge" in self.args.sources:
            nats = import_nats()
            self.nats = await nats.connect(
                servers=[self.args.nats_server],
                name="nervix-datalake-loadgen",
            )

    async def close(self) -> None:
        if self.kafka is not None:
            self.kafka.flush()
            self.kafka.close()
        if self.mqtt is not None:
            self.drain_mqtt(block=True)
            self.mqtt.loop_stop()
            self.mqtt.disconnect()
        if self.nats is not None:
            await self.nats.drain()

    def drain_mqtt(self, block: bool = False) -> None:
        if block:
            for info in self.mqtt_pending:
                info.wait_for_publish()
            self.mqtt_pending.clear()
            return
        self.mqtt_pending = [info for info in self.mqtt_pending if not info.is_published()]
        if len(self.mqtt_pending) < MQTT_PENDING_BACKPRESSURE:
            return
        for info in self.mqtt_pending[:MQTT_PENDING_DRAIN_BATCH]:
            info.wait_for_publish()
        self.mqtt_pending = [info for info in self.mqtt_pending if not info.is_published()]

    async def publish(self, event: SourceEvent) -> None:
        if self.args.dry_run:
            wire_format = {
                "device": "cbor",
                "edge": "protobuf",
                "auth": "json",
            }[event.source]
            print(
                json.dumps(
                    {
                        "source": event.source,
                        "wire_format": wire_format,
                        "wire_bytes": debug_wire_size(event, self.cbor2),
                        "payload": event.payload,
                    },
                    sort_keys=True,
                )
            )
            return

        if event.source == "device":
            body = self.cbor2.dumps(event.payload)
            info = self.mqtt.publish(self.args.device_topic, body, qos=0)
            if info.rc:
                raise RuntimeError(f"MQTT publish failed with code {info.rc}")
            self.mqtt_pending.append(info)
            if len(self.mqtt_pending) >= MQTT_PENDING_DRAIN_BATCH:
                self.drain_mqtt()
        elif event.source == "edge":
            await self.nats.publish(self.args.edge_subject, encode_edge_activity(event.payload))
        elif event.source == "auth":
            self.kafka.send(self.args.auth_topic, event.payload)
        else:
            raise ValueError(f"unknown source {event.source}")


async def run(args: argparse.Namespace) -> None:
    validate_args(args)
    if args.seed is not None:
        random.seed(args.seed)

    publishers = Publishers(args)
    progress = Progress(args)
    progress.start_report()
    interval = 1.0 / args.rate
    try:
        await publishers.start()
        progress.begin_publishing()
        next_publish = time.monotonic()
        for event in scenario_stream(args.location_burst):
            if event.source not in args.sources:
                continue
            if args.duration and progress.elapsed() >= args.duration:
                break
            now = time.monotonic()
            wait = next_publish - now
            if wait > 0:
                await asyncio.sleep(wait)
                if args.duration and progress.elapsed() >= args.duration:
                    break
            await publishers.publish(event)
            progress.record(event.source)
            progress.maybe_report()
            next_publish += interval
            if next_publish < time.monotonic() - 1.0:
                next_publish = time.monotonic()
            if progress.total % 100 == 0:
                await asyncio.sleep(0)
    finally:
        await publishers.close()
        progress.final_report()


def main() -> None:
    try:
        asyncio.run(run(parse_args()))
    except (RuntimeError, OSError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from None
    except KeyboardInterrupt:
        print("stopped datalake load", file=sys.stderr)
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
