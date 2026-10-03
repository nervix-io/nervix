from __future__ import annotations

import json
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch

from scripts.build_onnxruntime import BuildSpec, file_digest
from scripts.onnxruntime.artifacts import ManagedRuntimeBuild, build_spec
from scripts.onnxruntime.macos import qualification_path, qualify_native, require_qualification, validate_program
from scripts.onnxruntime.toolchain import BuildError
from scripts.tests.test_build_onnxruntime import construct_package, fixture_repository


class MacOSArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        self.enterContext(patch.dict(os.environ, {"CI": ""}))
        self.temporary = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(self.temporary)
        self.repo = fixture_repository(self.root / "repo")
        self.spec = build_spec("darwin/arm64", self.repo)
        self.build = ManagedRuntimeBuild(self.spec, self.root / "stage")

    def package(self) -> None:
        construct_package(self.build.package_dir, platform="darwin/arm64")
        verification = self.build.package_dir / "verification"
        verification.mkdir()
        (verification / "nervix-onnx-smoke").write_bytes(struct.pack("<4I", 0xFEEDFACF, 0x0100000C, 0, 2))
        (verification / "output.onnx").write_bytes(b"fixture model")
        self.build._seal(self.build.package_dir)

    def test_sdk_and_cross_compiler_inputs_select_the_mac_artifact(self) -> None:
        linux = [build_spec(target, self.repo, variant=variant)
                 for target in ("linux/amd64", "linux/arm64") for variant in ("portable", "docker")]
        catalog_path = self.repo / "scripts/onnxruntime/downloads.json"
        catalog = json.loads(catalog_path.read_text())
        catalog["macos_sdk"]["sdk_image"] = "fixture@sha256:" + "0" * 64
        catalog_path.write_text(json.dumps(catalog))
        self.assertNotEqual(self.spec.fingerprint, build_spec("darwin/arm64", self.repo).fingerprint)
        for spec in linux:
            self.assertEqual(spec.fingerprint, build_spec(spec.platform, self.repo, variant=spec.variant).fingerprint)
            self.assertEqual(spec.fingerprint, BuildSpec.create(spec.platform, self.repo, variant=spec.variant).fingerprint)

    def test_pinning_requires_a_successful_native_receipt_for_the_exact_archive(self) -> None:
        self.package()
        with self.assertRaisesRegex(BuildError, "native ARM64 inference qualification"):
            self.build.checksum()
        self.assertIsNone(self.spec.artifact_checksum)
        archive = self.build.archive()
        receipt = qualification_path(self.build)
        receipt.parent.mkdir(parents=True)
        receipt.write_text(json.dumps({"fingerprint": self.spec.fingerprint,
                                      "archive_sha256": "0" * 64,
                                      "manifest_sha256": file_digest(self.build.package_dir / "manifest.json"),
                                      "platform": "darwin/arm64", "inference": "passed"}))
        with self.assertRaisesRegex(BuildError, "does not match"):
            self.build.checksum()
        metadata = json.loads(receipt.read_text())
        metadata["archive_sha256"] = file_digest(archive)
        receipt.write_text(json.dumps(metadata))
        require_qualification(self.build)
        self.assertEqual(self.build.checksum(), file_digest(archive))

    def test_native_qualification_records_success_only_after_inference_completes(self) -> None:
        self.package()
        archive = self.build.archive()
        receipt = self.root / "qualification.json"
        with patch("scripts.onnxruntime.macos.native_platform", return_value="darwin/arm64"), \
                patch("scripts.onnxruntime.macos.platform.mac_ver", return_value=("15.0", (), "arm64")):
            with patch("scripts.onnxruntime.macos.run", side_effect=[None, BuildError("inference failed")]):
                with self.assertRaisesRegex(BuildError, "inference failed"):
                    qualify_native(self.build, archive, receipt)
                self.assertFalse(receipt.exists())
            with patch("scripts.onnxruntime.macos.run") as execution:
                qualify_native(self.build, archive, receipt)
                self.assertEqual(execution.call_args.args[0][-2:], [self.spec.configuration["version"], "cpu"])
        metadata = json.loads(receipt.read_text())
        self.assertEqual(metadata["archive_sha256"], file_digest(archive))
        self.assertEqual(metadata["platform"], "darwin/arm64")
        self.assertEqual(metadata["inference"], "passed")

    def test_linux_cannot_qualify_mac_inference_and_ci_cannot_pin(self) -> None:
        with patch("scripts.onnxruntime.macos.native_platform", return_value="linux/amd64"):
            with self.assertRaisesRegex(BuildError, "requires an ARM64 macOS host"):
                qualify_native(self.build, self.root / "archive.tar.gz", self.root / "receipt.json")
        with patch.dict(os.environ, {"CI": "true"}):
            with self.assertRaisesRegex(BuildError, "disabled in CI"):
                self.build.checksum()

    def test_the_verification_program_must_execute_on_mac_arm64(self) -> None:
        program = self.root / "smoke"
        program.write_bytes(struct.pack("<4I", 0xFEEDFACF, 0x01000007, 3, 2))
        with self.assertRaisesRegex(BuildError, "ARM64 Mach-O"):
            validate_program(program)


if __name__ == "__main__":
    unittest.main()
