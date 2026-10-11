"""The runner load recorder's memory accounting and its record of a real sampling run."""

from __future__ import annotations

import csv
import pathlib
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from scripts import record_runner_load

SCRIPT = pathlib.Path(record_runner_load.__file__)


class MemoryTests(unittest.TestCase):
    def test_used_memory_excludes_what_the_kernel_can_still_hand_out(self) -> None:
        memory = record_runner_load.memory_sample(
            "MemTotal:       16777216 kB\n"
            "MemFree:         1048576 kB\n"
            "MemAvailable:    4194304 kB\n"
            "SwapTotal:       2097152 kB\n"
            "SwapFree:        1048576 kB\n"
        )
        self.assertEqual(memory, record_runner_load.Memory(16384.0, 12288.0, 1024.0))

    def test_a_runner_without_swap_uses_none(self) -> None:
        memory = record_runner_load.memory_sample("MemTotal: 2048 kB\nMemAvailable: 1024 kB\n")
        self.assertEqual(memory.swap_used_mib, 0.0)


class RecordingTests(unittest.TestCase):
    def test_every_sample_reaches_the_record_and_the_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            records = pathlib.Path(directory) / "load.csv"
            summary = pathlib.Path(directory) / "load.md"
            stopped = threading.Event()
            recorder = threading.Thread(
                target=record_runner_load.record, args=(records, summary, 0.01, stopped)
            )
            recorder.start()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not (
                records.exists() and len(records.read_text().splitlines()) > 3
            ):
                time.sleep(0.01)
            stopped.set()
            recorder.join(timeout=30)
            self.assertFalse(recorder.is_alive())
            with records.open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertGreaterEqual(len(rows), 3)
            used = [float(row["memory_used_mib"]) for row in rows]
            text = summary.read_text()
            self.assertIn(f"samples: {len(rows)} (0.01 s cadence)", text)
            self.assertIn(f"| Memory used | {sum(used) / len(used):.1f} MiB | {max(used):.1f} MiB |", text)

    def test_a_recorder_stopped_before_its_first_sample_summarizes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            stopped = threading.Event()
            stopped.set()
            summary = pathlib.Path(directory) / "load.md"
            record_runner_load.record(pathlib.Path(directory) / "load.csv", summary, 5, stopped)
            self.assertIn("samples: 0 (5 s cadence)", summary.read_text())
            self.assertIn("| CPU utilization | 0.0% | 0.0% |", summary.read_text())

    def test_a_stopped_recorder_keeps_every_sample_and_its_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            records = pathlib.Path(directory) / "load.csv"
            summary = pathlib.Path(directory) / "load.md"
            recorder = subprocess.Popen(
                [sys.executable, str(SCRIPT), str(records), str(summary), "0.05"]
            )
            # Wait for samples by observing the record, not by sleeping a guessed interval.
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if records.exists() and len(records.read_text().splitlines()) > 2:
                    break
                time.sleep(0.01)
            recorder.send_signal(signal.SIGTERM)
            self.assertEqual(recorder.wait(timeout=30), 0)
            with records.open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertGreaterEqual(len(rows), 2)
            for row in rows:
                self.assertGreater(float(row["memory_used_mib"]), 0)
                self.assertGreaterEqual(float(row["swap_used_mib"]), 0)
                self.assertGreaterEqual(float(row["cpu_utilization_pct"]), 0)
            text = summary.read_text()
            self.assertIn(f"samples: {len(rows)} (0.05 s cadence)", text)
            for metric in ("CPU utilization", "CPU steal time", "Memory used", "Swap used"):
                self.assertIn(f"| {metric} |", text)


if __name__ == "__main__":
    unittest.main()
