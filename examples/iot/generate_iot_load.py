#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "paho-mqtt>=2.0",
# ]
# ///
"""Stream fake smart-factory telemetry into the Nervix IoT MQTT example."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import random
import signal
import sys
import time
from dataclasses import dataclass
from typing import TypedDict

SENSORS = {
    "temperature": ("c", 48.0, 72.0, 85.0),
    "vibration": ("mm_s", 3.0, 8.0, 13.0),
    "pressure": ("bar", 4.5, 7.0, 9.0),
    "humidity": ("pct", 42.0, 75.0, 90.0),
    "current": ("amp", 12.0, 24.0, 34.0),
}

EVENT_NORMAL = "normal"
EVENT_BATTERY = "battery"
EVENT_THERMAL = "thermal"
EVENT_MAINTENANCE = "maintenance"
EVENT_CRITICAL = "critical"
EVENT_INVALID = "invalid"


class PendingPayload(TypedDict):
    release_at: float
    payload: dict[str, object]


@dataclass(frozen=True)
class DeviceRef:
    site_index: int
    line_index: int
    device_index: int

    @property
    def site(self) -> str:
        return f"site-{self.site_index:02d}"

    @property
    def line(self) -> str:
        return f"line-{self.line_index:02d}"

    @property
    def device_id(self) -> str:
        return f"{self.site}-{self.line}-dev-{self.device_index:04d}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish streaming MQTT load for examples/iot/iot.nspl."
    )
    parser.add_argument("--broker", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", default="factory_telemetry")
    parser.add_argument("--client-id", default="nervix-iot-loadgen")
    parser.add_argument("--sites", type=int, default=3)
    parser.add_argument("--lines", type=int, default=4)
    parser.add_argument("--devices-per-line", type=int, default=50)
    parser.add_argument("--rate", type=float, default=250.0, help="average messages per second")
    parser.add_argument(
        "--rate-variation",
        type=float,
        default=0.35,
        help="fractional slow drift around --rate; 0 keeps the baseline flat",
    )
    parser.add_argument(
        "--burst-rate",
        type=float,
        default=0.03,
        help="chance per second of starting a short telemetry burst",
    )
    parser.add_argument(
        "--burst-multiplier",
        type=float,
        default=3.0,
        help="temporary rate multiplier while a burst is active",
    )
    parser.add_argument(
        "--burst-duration",
        type=float,
        default=2.5,
        help="average burst duration in seconds",
    )
    parser.add_argument("--duration", type=float, default=0.0, help="seconds; 0 runs forever")
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.03,
        help="critical alert event rate",
    )
    parser.add_argument("--maintenance-alert-rate", type=float, default=0.06)
    parser.add_argument("--thermal-alert-rate", type=float, default=0.04)
    parser.add_argument("--battery-alert-rate", type=float, default=0.04)
    parser.add_argument("--invalid-rate", type=float, default=0.005)
    parser.add_argument("--offline-rate", type=float, default=0.003)
    parser.add_argument(
        "--late-event-rate",
        type=float,
        default=0.08,
        help="fraction of generated events held briefly before publish",
    )
    parser.add_argument(
        "--max-lateness",
        type=float,
        default=4.0,
        help="maximum seconds a late event is timestamped and delivered behind newer events",
    )
    parser.add_argument("--qos", type=int, choices=(0, 1), default=0)
    parser.add_argument("--seed", type=int, default=None)
    return parser.parse_args()


def import_mqtt():
    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        print(
            "Missing dependency: paho-mqtt. Install it with `python -m pip install paho-mqtt`.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return mqtt


def mqtt_client(mqtt, client_id: str):
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    except AttributeError:
        return mqtt.Client(client_id=client_id)


def choose_device(args: argparse.Namespace) -> DeviceRef:
    return DeviceRef(
        site_index=random.randint(1, args.sites),
        line_index=random.randint(1, args.lines),
        device_index=random.randint(1, args.devices_per_line),
    )


def stable_phase(device: DeviceRef, sensor: str) -> float:
    sensor_hash = sum((index + 1) * ord(char) for index, char in enumerate(sensor))
    raw = (
        device.site_index * 73856093
        ^ device.line_index * 19349663
        ^ device.device_index * 83492791
        ^ sensor_hash
    )
    return float(abs(raw) % 6283) / 1000.0


def telemetry_value(device: DeviceRef, sensor: str, seq: int, event: str) -> float:
    _unit, baseline, warn_high, critical_high = SENSORS[sensor]
    phase = stable_phase(device, sensor)
    wave = math.sin(seq / 37.0 + phase) * baseline * 0.08
    noise = random.uniform(-baseline * 0.03, baseline * 0.03)
    value = baseline + wave + noise
    if event == EVENT_CRITICAL:
        value = random.uniform(critical_high * 1.02, critical_high * 1.2)
    elif event == EVENT_THERMAL:
        value = random.uniform(80.0, critical_high - 0.1)
    elif event == EVENT_MAINTENANCE:
        value = random.uniform(warn_high * 1.03, critical_high * 1.18)
        value = min(value, critical_high - 0.1)
    return round(value, 3)


def choose_event(args: argparse.Namespace) -> str:
    threshold = random.random()
    cumulative = args.invalid_rate
    if threshold < cumulative:
        return EVENT_INVALID
    cumulative += args.battery_alert_rate
    if threshold < cumulative:
        return EVENT_BATTERY
    cumulative += args.anomaly_rate
    if threshold < cumulative:
        return EVENT_CRITICAL
    cumulative += args.thermal_alert_rate
    if threshold < cumulative:
        return EVENT_THERMAL
    cumulative += args.maintenance_alert_rate
    if threshold < cumulative:
        return EVENT_MAINTENANCE
    return EVENT_NORMAL


def choose_sensor(event: str) -> str:
    if event == EVENT_THERMAL:
        return "temperature"
    if event == EVENT_MAINTENANCE:
        return random.choice(("vibration", "pressure", "humidity", "current"))
    return random.choice(tuple(SENSORS))


def next_device_sequence(
    device: DeviceRef, device_sequences: dict[str, int] | None, fallback_seq: int
) -> int:
    if device_sequences is None:
        return fallback_seq
    key = device.device_id
    value = device_sequences.get(key, 0) + 1
    device_sequences[key] = value
    return value


def make_payload(
    args: argparse.Namespace,
    seq: int,
    *,
    now: dt.datetime | None = None,
    device_sequences: dict[str, int] | None = None,
) -> dict[str, object]:
    device = choose_device(args)
    event = choose_event(args)
    sensor = choose_sensor(event)
    unit, _baseline, warn_high, critical_high = SENSORS[sensor]
    device_seq = next_device_sequence(device, device_sequences, seq)
    event_time = now or dt.datetime.now(dt.UTC)

    status = "fault" if event == EVENT_CRITICAL and random.random() < 0.35 else "ok"
    if event == EVENT_NORMAL and random.random() < args.offline_rate:
        status = "offline"

    battery_pct = max(1.0, 100.0 - (seq % 20000) / 220.0 - random.random() * 4.0)
    if event == EVENT_BATTERY:
        battery_pct = random.uniform(3.0, 14.0)

    payload = {
        "site": device.site,
        "line": device.line,
        "device_id": device.device_id,
        "sensor": sensor,
        "value": telemetry_value(device, sensor, seq, event),
        "unit": unit,
        "warn_high": warn_high,
        "critical_high": critical_high,
        "battery_pct": round(battery_pct, 2),
        "status": status,
        "ts": event_time.isoformat(),
        "seq": device_seq,
    }

    if event == EVENT_INVALID:
        if random.random() < 0.5:
            payload["device_id"] = ""
        else:
            payload["sensor"] = ""

    return payload


def validate_args(args: argparse.Namespace) -> None:
    if args.sites < 1:
        raise SystemExit("--sites must be greater than 0")
    if args.lines < 1:
        raise SystemExit("--lines must be greater than 0")
    if args.devices_per_line < 1:
        raise SystemExit("--devices-per-line must be greater than 0")
    if args.rate <= 0:
        raise SystemExit("--rate must be greater than 0")
    if args.rate_variation < 0:
        raise SystemExit("--rate-variation must be greater than or equal to 0")
    if args.burst_rate < 0:
        raise SystemExit("--burst-rate must be greater than or equal to 0")
    if args.burst_multiplier < 1:
        raise SystemExit("--burst-multiplier must be greater than or equal to 1")
    if args.burst_duration <= 0:
        raise SystemExit("--burst-duration must be greater than 0")
    if not 0 <= args.anomaly_rate <= 1:
        raise SystemExit("--anomaly-rate must be between 0 and 1")
    if not 0 <= args.maintenance_alert_rate <= 1:
        raise SystemExit("--maintenance-alert-rate must be between 0 and 1")
    if not 0 <= args.thermal_alert_rate <= 1:
        raise SystemExit("--thermal-alert-rate must be between 0 and 1")
    if not 0 <= args.battery_alert_rate <= 1:
        raise SystemExit("--battery-alert-rate must be between 0 and 1")
    if not 0 <= args.invalid_rate <= 1:
        raise SystemExit("--invalid-rate must be between 0 and 1")
    if not 0 <= args.offline_rate <= 1:
        raise SystemExit("--offline-rate must be between 0 and 1")
    if not 0 <= args.late_event_rate <= 1:
        raise SystemExit("--late-event-rate must be between 0 and 1")
    if args.max_lateness < 0:
        raise SystemExit("--max-lateness must be greater than or equal to 0")
    total_event_rate = (
        args.anomaly_rate
        + args.maintenance_alert_rate
        + args.thermal_alert_rate
        + args.battery_alert_rate
        + args.invalid_rate
    )
    if total_event_rate > 1:
        raise SystemExit(
            "sum of --anomaly-rate, --maintenance-alert-rate, --thermal-alert-rate, "
            "--battery-alert-rate, and --invalid-rate must be at most 1"
        )


def publish_payload(client, mqtt, topic: str, payload: dict[str, object], qos: int) -> None:
    encoded = json.dumps(payload, separators=(",", ":"))
    result = client.publish(topic, encoded, qos=qos, retain=False)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(f"MQTT publish failed with code {result.rc}")
    if qos > 0:
        result.wait_for_publish()


def main() -> int:
    args = parse_args()
    validate_args(args)
    if args.seed is not None:
        random.seed(args.seed)
    mqtt = import_mqtt()

    stop = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    client = mqtt_client(mqtt, args.client_id)
    client.max_inflight_messages_set(1000)
    client.max_queued_messages_set(10000)
    client.connect(args.broker, args.port, keepalive=30)
    client.loop_start()

    started = time.monotonic()
    next_publish = started
    next_report = started + 1.0
    burst_until = 0.0
    pending_late: list[PendingPayload] = []
    device_sequences: dict[str, int] = {}
    seq = 0

    try:
        while not stop:
            now = time.monotonic()
            if args.duration and now - started >= args.duration:
                break
            if now < next_publish:
                time.sleep(min(next_publish - now, 0.01))
                continue

            seq += 1
            event_time = dt.datetime.now(dt.UTC)
            payload = make_payload(
                args,
                seq,
                now=event_time,
                device_sequences=device_sequences,
            )
            if args.max_lateness > 0 and random.random() < args.late_event_rate:
                delay = random.uniform(0.05, args.max_lateness)
                payload["ts"] = (event_time - dt.timedelta(seconds=delay)).isoformat()
                pending_late.append({"release_at": now + delay, "payload": payload})
            else:
                publish_payload(client, mqtt, args.topic, payload, args.qos)

            ready_late = [
                pending for pending in pending_late if pending["release_at"] <= now
            ]
            pending_late = [
                pending for pending in pending_late if pending["release_at"] > now
            ]
            random.shuffle(ready_late)
            for pending in ready_late:
                publish_payload(client, mqtt, args.topic, pending["payload"], args.qos)

            elapsed = now - started
            baseline_wave = math.sin(elapsed / 17.0) * args.rate_variation
            secondary_wave = math.sin(elapsed / 5.3 + 1.7) * args.rate_variation * 0.35
            current_rate = args.rate * max(0.05, 1.0 + baseline_wave + secondary_wave)
            if now >= burst_until and random.random() < args.burst_rate / current_rate:
                burst_until = now + random.expovariate(1.0 / args.burst_duration)
            if now < burst_until:
                current_rate *= args.burst_multiplier
            interval = random.expovariate(current_rate)
            next_publish += interval
            if now >= next_report:
                elapsed = max(now - started, 0.001)
                print(f"published={seq} rate={seq / elapsed:.1f}/s topic={args.topic}")
                next_report = now + 1.0
    finally:
        for pending in pending_late:
            publish_payload(client, mqtt, args.topic, pending["payload"], args.qos)
        client.loop_stop()
        client.disconnect()

    elapsed = max(time.monotonic() - started, 0.001)
    print(f"done published={seq} elapsed={elapsed:.1f}s rate={seq / elapsed:.1f}/s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
