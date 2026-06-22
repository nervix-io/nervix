#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "nats-py>=2.7",
# ]
# ///
"""Stream fake machine telemetry into the NATS factory window example."""

from __future__ import annotations

import argparse
import asyncio
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
    "temperature": ("c", 58.0, 35.0, 76.0, 88.0),
    "vibration": ("mm_s", 3.6, 0.0, 8.5, 14.0),
    "pressure": ("bar", 5.9, 3.5, 8.0, 10.5),
    "humidity": ("pct", 48.0, 25.0, 72.0, 88.0),
    "current": ("amp", 17.0, 6.0, 29.0, 38.0),
}

EVENT_NORMAL = "normal"
EVENT_BATTERY = "battery"
EVENT_CRITICAL = "critical"
EVENT_DRIFT = "drift"
EVENT_INVALID = "invalid"
EVENT_OFFLINE = "offline"


class PendingPayload(TypedDict):
    release_at: float
    payload: dict[str, object]


@dataclass(frozen=True)
class AssetRef:
    site_index: int
    line_index: int
    asset_index: int

    @property
    def site(self) -> str:
        return f"site-{self.site_index:02d}"

    @property
    def line(self) -> str:
        return f"line-{self.line_index:02d}"

    @property
    def asset_id(self) -> str:
        return f"{self.site}-{self.line}-asset-{self.asset_index:04d}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish streaming NATS load for examples/nats-factory-windows."
    )
    parser.add_argument("--server", default="nats://127.0.0.1:4222")
    parser.add_argument("--subject", default="factory_signals")
    parser.add_argument("--client-name", default="nervix-factory-window-loadgen")
    parser.add_argument("--sites", type=int, default=3)
    parser.add_argument("--lines", type=int, default=4)
    parser.add_argument("--assets-per-line", type=int, default=60)
    parser.add_argument("--rate", type=float, default=300.0, help="average messages per second")
    parser.add_argument(
        "--rate-variation",
        type=float,
        default=0.4,
        help="fractional slow drift around --rate; 0 keeps the baseline flat",
    )
    parser.add_argument(
        "--burst-rate",
        type=float,
        default=0.025,
        help="chance per second of starting a short telemetry burst",
    )
    parser.add_argument(
        "--burst-multiplier",
        type=float,
        default=3.5,
        help="temporary rate multiplier while a burst is active",
    )
    parser.add_argument(
        "--burst-duration",
        type=float,
        default=2.0,
        help="average burst duration in seconds",
    )
    parser.add_argument("--duration", type=float, default=0.0, help="seconds; 0 runs forever")
    parser.add_argument("--critical-rate", type=float, default=0.025)
    parser.add_argument("--drift-rate", type=float, default=0.07)
    parser.add_argument("--battery-alert-rate", type=float, default=0.035)
    parser.add_argument("--invalid-rate", type=float, default=0.004)
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
    parser.add_argument("--flush-every", type=int, default=200)
    parser.add_argument("--seed", type=int, default=None)
    return parser.parse_args()


def import_nats():
    try:
        import nats
    except ImportError:
        print(
            "Missing dependency: nats-py. Install it with `python -m pip install nats-py`.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return nats


def validate_args(args: argparse.Namespace) -> None:
    if args.sites < 1:
        raise SystemExit("--sites must be greater than 0")
    if args.lines < 1:
        raise SystemExit("--lines must be greater than 0")
    if args.assets_per_line < 1:
        raise SystemExit("--assets-per-line must be greater than 0")
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
    if args.flush_every < 1:
        raise SystemExit("--flush-every must be greater than 0")
    for name in (
        "critical_rate",
        "drift_rate",
        "battery_alert_rate",
        "invalid_rate",
        "offline_rate",
        "late_event_rate",
    ):
        value = getattr(args, name)
        if not 0 <= value <= 1:
            raise SystemExit(f"--{name.replace('_', '-')} must be between 0 and 1")
    if args.max_lateness < 0:
        raise SystemExit("--max-lateness must be greater than or equal to 0")
    total_event_rate = (
        args.critical_rate
        + args.drift_rate
        + args.battery_alert_rate
        + args.invalid_rate
        + args.offline_rate
    )
    if total_event_rate > 1:
        raise SystemExit(
            "sum of --critical-rate, --drift-rate, --battery-alert-rate, "
            "--invalid-rate, and --offline-rate must be at most 1"
        )


def choose_asset(args: argparse.Namespace) -> AssetRef:
    return AssetRef(
        site_index=random.randint(1, args.sites),
        line_index=random.randint(1, args.lines),
        asset_index=random.randint(1, args.assets_per_line),
    )


def choose_event(args: argparse.Namespace) -> str:
    threshold = random.random()
    cumulative = args.invalid_rate
    if threshold < cumulative:
        return EVENT_INVALID
    cumulative += args.offline_rate
    if threshold < cumulative:
        return EVENT_OFFLINE
    cumulative += args.battery_alert_rate
    if threshold < cumulative:
        return EVENT_BATTERY
    cumulative += args.critical_rate
    if threshold < cumulative:
        return EVENT_CRITICAL
    cumulative += args.drift_rate
    if threshold < cumulative:
        return EVENT_DRIFT
    return EVENT_NORMAL


def choose_sensor() -> str:
    return random.choice(tuple(SENSORS))


def stable_phase(asset: AssetRef, sensor: str) -> float:
    sensor_hash = sum((index + 1) * ord(char) for index, char in enumerate(sensor))
    raw = (
        asset.site_index * 73856093
        ^ asset.line_index * 19349663
        ^ asset.asset_index * 83492791
        ^ sensor_hash
    )
    return float(abs(raw) % 6283) / 1000.0


def signal_value(asset: AssetRef, sensor: str, seq: int, event: str) -> float:
    _unit, baseline, target_min, target_max, critical_high = SENSORS[sensor]
    phase = stable_phase(asset, sensor)
    wave = math.sin(seq / 41.0 + phase) * baseline * 0.08
    noise = random.uniform(-baseline * 0.025, baseline * 0.025)
    value = baseline + wave + noise
    if event == EVENT_CRITICAL:
        value = random.uniform(critical_high * 1.02, critical_high * 1.16)
    elif event == EVENT_DRIFT:
        if target_min > 0 and random.random() < 0.25:
            value = random.uniform(target_min * 0.55, target_min * 0.95)
        else:
            value = random.uniform(target_max * 1.03, min(critical_high * 0.97, target_max * 1.22))
    return round(value, 3)


def next_branch_sequence(
    asset: AssetRef, sensor: str, branch_sequences: dict[str, int], fallback_seq: int
) -> int:
    key = f"{asset.asset_id}:{sensor}"
    value = branch_sequences.get(key, 0) + 1
    branch_sequences[key] = value
    return value or fallback_seq


def make_payload(
    args: argparse.Namespace,
    seq: int,
    *,
    now: dt.datetime | None = None,
    branch_sequences: dict[str, int] | None = None,
) -> dict[str, object]:
    asset = choose_asset(args)
    event = choose_event(args)
    sensor = choose_sensor()
    unit, _baseline, target_min, target_max, critical_high = SENSORS[sensor]
    event_time = now or dt.datetime.now(dt.UTC)
    if branch_sequences is None:
        branch_seq = seq
    else:
        branch_seq = next_branch_sequence(asset, sensor, branch_sequences, seq)

    status = "ok"
    if event == EVENT_CRITICAL:
        status = "fault"
    elif event == EVENT_OFFLINE:
        status = "offline"

    battery_pct = max(18.0, 100.0 - (seq % 30000) / 310.0 - random.random() * 5.0)
    if event == EVENT_BATTERY:
        battery_pct = random.uniform(3.0, 14.0)

    payload = {
        "site": asset.site,
        "line": asset.line,
        "asset_id": asset.asset_id,
        "sensor": sensor,
        "value": signal_value(asset, sensor, seq, event),
        "unit": unit,
        "target_min": target_min,
        "target_max": target_max,
        "critical_high": critical_high,
        "battery_pct": round(battery_pct, 2),
        "status": status,
        "ts": event_time.isoformat(),
        "seq": branch_seq,
    }

    if event == EVENT_INVALID:
        if random.random() < 0.5:
            payload["asset_id"] = ""
        else:
            payload["sensor"] = ""

    return payload


async def publish_payload(nc, subject: str, payload: dict[str, object]) -> None:
    encoded = json.dumps(payload, separators=(",", ":")).encode()
    await nc.publish(subject, encoded)


def install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, request_stop)
        except NotImplementedError:
            signal.signal(signum, lambda _signum, _frame: request_stop())


async def run() -> int:
    args = parse_args()
    validate_args(args)
    if args.seed is not None:
        random.seed(args.seed)

    nats = import_nats()
    nc = await nats.connect(servers=[args.server], name=args.client_name)

    stop = asyncio.Event()
    install_signal_handlers(stop)

    started = time.monotonic()
    next_publish = started
    next_report = started + 1.0
    burst_until = 0.0
    pending_late: list[PendingPayload] = []
    branch_sequences: dict[str, int] = {}
    seq = 0

    try:
        while not stop.is_set():
            now = time.monotonic()
            if args.duration and now - started >= args.duration:
                break
            if now < next_publish:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=min(next_publish - now, 0.01))
                    break
                except TimeoutError:
                    continue

            seq += 1
            event_time = dt.datetime.now(dt.UTC)
            payload = make_payload(
                args,
                seq,
                now=event_time,
                branch_sequences=branch_sequences,
            )
            if args.max_lateness > 0 and random.random() < args.late_event_rate:
                delay = random.uniform(0.05, args.max_lateness)
                payload["ts"] = (event_time - dt.timedelta(seconds=delay)).isoformat()
                pending_late.append({"release_at": now + delay, "payload": payload})
            else:
                await publish_payload(nc, args.subject, payload)

            ready_late = [
                pending for pending in pending_late if pending["release_at"] <= now
            ]
            pending_late = [
                pending for pending in pending_late if pending["release_at"] > now
            ]
            random.shuffle(ready_late)
            for pending in ready_late:
                await publish_payload(nc, args.subject, pending["payload"])

            if seq % args.flush_every == 0:
                await nc.flush(timeout=2)

            elapsed = now - started
            baseline_wave = math.sin(elapsed / 19.0) * args.rate_variation
            secondary_wave = math.sin(elapsed / 6.7 + 1.3) * args.rate_variation * 0.3
            current_rate = args.rate * max(0.05, 1.0 + baseline_wave + secondary_wave)
            if now >= burst_until and random.random() < args.burst_rate / current_rate:
                burst_until = now + random.expovariate(1.0 / args.burst_duration)
            if now < burst_until:
                current_rate *= args.burst_multiplier
            next_publish += random.expovariate(current_rate)

            if now >= next_report:
                elapsed = max(now - started, 0.001)
                print(f"published={seq} rate={seq / elapsed:.1f}/s subject={args.subject}")
                next_report = now + 1.0
    finally:
        for pending in pending_late:
            await publish_payload(nc, args.subject, pending["payload"])
        await nc.flush(timeout=2)
        await nc.drain()

    elapsed = max(time.monotonic() - started, 0.001)
    print(f"done published={seq} elapsed={elapsed:.1f}s rate={seq / elapsed:.1f}/s")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
