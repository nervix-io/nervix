"""Check that fixture generation is confined to the paths the caller requested, and that the
chaos suite's checked-in running-sum model is exactly what the generator writes."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


GENERATOR = Path(__file__).resolve().parents[1] / "train_simple_onnx.py"
# A chaos run builds nothing, so the stateful scenario uploads this checked-in model.
CHAOS_RUNNING_SUM = (
    Path(__file__).resolve().parents[1] / "chaos" / "fixtures" / "onnx" / "models" / "running_sum.onnx"
)
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


class ChaosFixtureTests(unittest.TestCase):
    def test_the_chaos_running_sum_model_is_the_generated_model(self) -> None:
        specification = importlib.util.spec_from_file_location("train_simple_onnx", GENERATOR)
        self.assertIsNotNone(specification)
        generator = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(generator)
        expected = generator.model_proto(generator.running_sum_graph())
        self.assertEqual(
            CHAOS_RUNNING_SUM.read_bytes(),
            expected,
            "regenerate scripts/chaos/fixtures/onnx/models/running_sum.onnx with "
            "scripts/train_simple_onnx.py --running-sum-output",
        )


if __name__ == "__main__":
    unittest.main()
