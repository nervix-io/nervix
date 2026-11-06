"""Check that fixture generation is confined to the paths the caller requested."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


GENERATOR = Path(__file__).resolve().parents[1] / "train_simple_onnx.py"
OUTPUT_OPTIONS = (
    "output",
    "alternate-output",
    "batch-output",
    "f64-output",
    "matrix-output",
    "dynamic-batch-output",
    "scalar-output",
    "running-sum-output",
    "checked-running-sum-output",
)


class FixtureGenerationTests(unittest.TestCase):
    def test_every_fixture_uses_its_requested_destination_and_is_reproducible(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arguments = [sys.executable, str(GENERATOR)]
            outputs = []
            for option in OUTPUT_OPTIONS:
                output = root / "requested" / option / "model.onnx"
                outputs.append(output)
                arguments.extend(["--" + option, str(output)])
            subprocess.run(arguments, cwd=root, check=True, capture_output=True, timeout=15)
            self.assertEqual(sorted(root.rglob("*.onnx")), sorted(outputs))
            first = {output: output.read_bytes() for output in outputs}
            self.assertTrue(all(first.values()))
            subprocess.run(arguments, cwd=root, check=True, capture_output=True, timeout=15)
            self.assertEqual(first, {output: output.read_bytes() for output in outputs})


if __name__ == "__main__":
    unittest.main()
