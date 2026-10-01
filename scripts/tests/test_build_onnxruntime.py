from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
from urllib.request import Request

from scripts.build_onnxruntime import BuildError, BuildSpec, R2Cache, R2Publisher, RuntimeBuild, copy_shared_library, file_digest


ROOT = Path(__file__).resolve().parents[2]


def fixture_repository(destination: Path) -> Path:
    names = [*BuildSpec.create(repo=ROOT).identity["build_files"],
             "scripts/onnxruntime/manifest.toml", "scripts/onnxruntime/upload.py", "justfile", "rust-toolchain.toml",
             "pyproject.toml", "uv.lock"]
    for name in names:
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    (destination / "scripts/onnxruntime/checksums.toml").write_text("")
    (destination / "crates/web-console").mkdir(parents=True)
    if (ROOT / ".venv").is_dir():
        (destination / ".venv").symlink_to(ROOT / ".venv", target_is_directory=True)
    return destination


def pin_checksum(spec: BuildSpec, checksum: str) -> None:
    (spec.repo / "scripts/onnxruntime/checksums.toml").write_text(
        f'[{json.dumps(spec.platform)}]\nfingerprint = "{spec.fingerprint}"\nsha256 = "{checksum}"\n'
    )


def elf_fixture(platform: str) -> bytes:
    header = bytearray(20)
    header[:6] = b"\x7fELF\x02\x01"
    header[18:20] = (62 if platform == "linux/amd64" else 183).to_bytes(2, "little")
    return bytes(header) + b"vendor fixture"


def construct_cuda_runtime(destination: Path, platform: str = "linux/amd64") -> None:
    runtime = destination / "runtime/lib"
    runtime.mkdir(parents=True)
    for name in ("libonnxruntime_providers_shared.so", "libonnxruntime_providers_cuda.so",
                 "libcudnn_graph.so.9", "libnvrtc-builtins.so.13.2"):
        (runtime / name).write_bytes(elf_fixture(platform))


def construct_package(destination: Path, *, platform: str = "linux/amd64") -> None:
    (destination / "lib").mkdir(parents=True)
    (destination / "include").mkdir()
    (destination / "lib/libonnxruntime.a").write_bytes(b"!<arch>\nfixture")
    (destination / "include/onnxruntime_c_api.h").write_text("fixture header")
    (destination / "LICENSE").write_text("fixture license")
    (destination / "ThirdPartyNotices.txt").write_text("fixture notices")
    if platform.startswith("linux/"):
        construct_cuda_runtime(destination, platform)


class BuildCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        environment = patch.dict(os.environ, {"CI": ""})
        environment.start()
        self.addCleanup(environment.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = fixture_repository(self.root / "repository")
        self.spec = BuildSpec.create("linux/amd64", repo=self.repo)
        self.build = RuntimeBuild(self.spec, self.root)

    def build_package(self) -> Mock:
        with patch.object(self.build, "_build", side_effect=construct_package) as builder:
            self.build.build_source()
        self.build.checksum()
        return builder

    def mock_download(self, payload: bytes) -> Mock:
        return self.enterContext(patch("scripts.build_onnxruntime.urlopen", side_effect=lambda *args, **kwargs: io.BytesIO(payload)))

    def test_completed_local_build_skips_build_and_network(self) -> None:
        self.build_package().assert_called_once()
        remote = Mock(spec=R2Cache)
        for _ in range(2):
            with patch.object(self.build, "_build") as builder:
                self.assertEqual(self.build.prepare(remote), self.build.package_dir / "lib")
            builder.assert_not_called()
        remote.restore.assert_not_called()

    def test_remote_hit_skips_source_build(self) -> None:
        self.build_package()
        archive = self.root / "package.tar.gz"
        self.build.write_archive(str(archive))
        payload = archive.read_bytes()
        download = self.mock_download(payload)
        restored = RuntimeBuild(self.spec, self.root / "restored")
        with patch.object(restored, "_build") as builder:
            restored.prepare()
        builder.assert_not_called()
        download.assert_called_once()
        restored.validate_package()

    def test_ci_cache_miss_requires_a_published_package(self) -> None:
        remote = Mock(spec=R2Cache)
        pin_checksum(self.spec, "a" * 64)
        remote.restore.return_value = None
        with patch.dict(os.environ, {"CI": "true"}):
            with patch.object(self.build, "_build") as builder:
                with self.assertRaisesRegex(BuildError, "unavailable.*publish-onnxruntime"):
                    self.build.prepare(remote)
        builder.assert_not_called()
        remote.restore.assert_called_once()

    def test_development_cache_miss_requires_a_published_package(self) -> None:
        remote = Mock(spec=R2Cache)
        pin_checksum(self.spec, "a" * 64)
        remote.restore.return_value = None
        with patch.object(self.build, "_build") as builder:
            with patch("scripts.build_onnxruntime.HostToolchain.discover") as compilers:
                with self.assertRaisesRegex(BuildError, "unavailable.*maintainer.*publish-onnxruntime"):
                    self.build.prepare(remote)
        builder.assert_not_called()
        compilers.assert_not_called()
        remote.restore.assert_called_once()

    def test_local_artifact_preparation_does_not_access_r2(self) -> None:
        from scripts.build_onnxruntime import main

        self.build_package()
        arguments = ["onnxruntime", "fetch", "--platform", self.spec.platform, "--stage", str(self.root)]
        with patch("scripts.build_onnxruntime.ROOT", self.repo), patch("scripts.build_onnxruntime.BuildSpec.create", return_value=self.spec):
            with patch("scripts.build_onnxruntime.urlopen") as download, patch("sys.argv", arguments):
                with patch("sys.stdout", io.StringIO()):
                    self.assertEqual(main(), 0)
        download.assert_not_called()

    def test_development_and_ci_fetch_public_artifacts_without_credentials_or_compilers(self) -> None:
        from scripts.build_onnxruntime import main

        self.build_package()
        payload = self.build.archive().read_bytes()
        for ci in ("", "true"):
            with self.subTest(ci=ci):
                stage = self.root / ("ci-download" if ci else "development-download")
                arguments = ["onnxruntime", "fetch", "--platform", self.spec.platform, "--stage", str(stage)]
                body = io.BytesIO(payload)
                stdout, stderr = io.StringIO(), io.StringIO()
                with patch.dict(os.environ, {"CI": ci, "R2_ACCESS_KEY_ID": "", "R2_SECRET_ACCESS_KEY": ""}):
                    with patch("scripts.build_onnxruntime.BuildSpec.create", return_value=self.spec), patch("sys.argv", arguments):
                        with patch("scripts.build_onnxruntime.urlopen", return_value=body) as download:
                            with patch("boto3.client") as credentials, patch("scripts.build_onnxruntime.HostToolchain.discover") as compilers:
                                with patch("sys.stdout", stdout), patch("sys.stderr", stderr):
                                    self.assertEqual(main(), 0, stderr.getvalue())
                download.assert_called_once()
                request = download.call_args.args[0]
                self.assertIsInstance(request, Request)
                self.assertEqual(request.full_url,
                                 f"https://pub-4668ad14d0814ca58c09a124f6bd96f3.r2.dev/onnxruntime/{self.spec.object_key}")
                self.assertEqual(request.get_header("User-agent"), "nervix-onnxruntime-artifacts")
                self.assertEqual(download.call_args.kwargs, {"timeout": 120})
                credentials.assert_not_called()
                compilers.assert_not_called()
                self.assertTrue(body.closed)
                restored = RuntimeBuild(self.spec, stage)
                restored.validate_package()
                self.assertEqual(stdout.getvalue().strip(), str(restored.package_dir / "lib"))

    def test_pinned_local_package_is_ready_without_compilation(self) -> None:
        self.build_package()
        with patch.object(self.build, "_build") as builder:
            path = self.build.prepare()
        self.assertEqual(path, self.build.package_dir / "lib")
        builder.assert_not_called()

    def test_artifact_identity_does_not_require_build_tools_on_macos(self) -> None:
        with patch("scripts.build_onnxruntime.subprocess.check_output", side_effect=AssertionError("compiler lookup")):
            spec = BuildSpec.create("darwin/arm64", repo=ROOT)
        self.assertEqual(spec.platform, "darwin/arm64")

    def test_upload_display_changes_preserve_the_artifact_identity(self) -> None:
        with (self.repo / "scripts/onnxruntime/upload.py").open("a") as stream:
            stream.write("\n# Transfer display settings do not affect compiled packages.\n")
        self.assertEqual(BuildSpec.create(self.spec.platform, repo=self.repo).fingerprint, self.spec.fingerprint)

    def test_native_build_compiles_on_the_host(self) -> None:
        with patch.object(self.build, "_source"):
            with patch.object(self.build, "compile") as compiler:
                with patch("scripts.build_onnxruntime.HostToolchain.discover",
                           return_value=Mock(metadata={"compiler": "clang"})):
                    self.build._build(self.root / "destination")
        compiler.assert_called_once_with(self.root / "destination")

    def test_validation_changes_reuse_serialized_compiler_intermediates(self) -> None:
        checkout = self.root / "repo"
        for name in self.spec.identity["build_files"]:
            target = checkout / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)
        shutil.copyfile(ROOT / "scripts/onnxruntime/manifest.toml", checkout / "scripts/onnxruntime/manifest.toml")
        with (checkout / "scripts/onnxruntime/smoke.cc").open("a") as stream:
            stream.write("\n// Additional package verification.\n")
        changed = RuntimeBuild(BuildSpec.create("linux/amd64", repo=checkout), self.root)
        self.assertNotEqual(self.build.package_dir, changed.package_dir)
        first_entered = threading.Event()
        second_discovered = threading.Event()
        second_entered = threading.Event()
        release_first = threading.Event()
        tools = Mock(metadata={"compiler": "clang"})

        def discover(*arguments: object) -> Mock:
            if first_entered.is_set():
                second_discovered.set()
            return tools

        def first_compile(destination: Path) -> None:
            first_entered.set()
            self.assertTrue(release_first.wait(timeout=10))

        with patch("scripts.build_onnxruntime.HostToolchain.discover", side_effect=discover):
            with patch.object(RuntimeBuild, "_source"):
                with patch.object(self.build, "compile", side_effect=first_compile):
                    with patch.object(changed, "compile", side_effect=lambda destination: second_entered.set()):
                        with ThreadPoolExecutor(max_workers=2) as pool:
                            first = pool.submit(self.build._build, self.root / "first")
                            try:
                                self.assertTrue(first_entered.wait(timeout=10))
                                second = pool.submit(changed._build, self.root / "second")
                                self.assertTrue(second_discovered.wait(timeout=10))
                                entered_concurrently = second_entered.wait(timeout=0.25)
                            finally:
                                release_first.set()
                            first.result(timeout=10)
                            second.result(timeout=10)
        self.assertFalse(entered_concurrently)
        self.assertEqual(self.build.build_dir, changed.build_dir)

    def test_manual_source_build_can_be_pinned_and_published(self) -> None:
        client = Mock()
        remote = R2Publisher(client, "fixture-bucket")
        self.build_package().assert_called_once()
        self.build.publish(remote)
        client.upload_file.assert_called_once()
        self.build.validate_package()

    def test_multipart_upload_reports_bytes_and_completion_on_stderr(self) -> None:
        self.build_package()
        archive = self.build.archive()
        total = archive.stat().st_size
        client = Mock()

        def upload(filename: str, bucket: str, key: str, **arguments: object) -> None:
            self.assertEqual(Path(filename), archive)
            self.assertEqual(bucket, "fixture-bucket")
            self.assertEqual(key, f"onnxruntime/{self.spec.object_key}")
            self.assertEqual(arguments["ExtraArgs"], {"Metadata": {"sha256": self.spec.artifact_checksum}})
            chunks = [total // 16] * 15 + [total - 15 * (total // 16)]
            with ThreadPoolExecutor(max_workers=8) as workers:
                list(workers.map(arguments["Callback"], chunks))

        client.upload_file.side_effect = upload
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch("sys.stdout", stdout), patch("sys.stderr", stderr):
            self.build.publish(R2Publisher(client, "fixture-bucket"))
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("Uploading linux/amd64", stderr.getvalue())
        self.assertIn("100%", stderr.getvalue())
        self.assertRegex(stderr.getvalue(), r"B/s")
        self.assertTrue(stderr.getvalue().endswith("\n"))
        client.upload_file.assert_called_once()

    def test_failed_upload_closes_progress_and_preserves_the_archive(self) -> None:
        from boto3.exceptions import S3UploadFailedError

        self.build_package()
        archive = self.build.archive()
        checksum = file_digest(archive)
        client = Mock()

        def upload(*arguments: object, **keywords: object) -> None:
            keywords["Callback"](archive.stat().st_size // 4)
            raise S3UploadFailedError("upload interrupted")

        client.upload_file.side_effect = upload
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            with self.assertRaisesRegex(BuildError, "R2 publish failed: upload interrupted"):
                self.build.publish(R2Publisher(client, "fixture-bucket"))
        self.assertIn("Uploading linux/amd64", stderr.getvalue())
        self.assertNotIn("100%", stderr.getvalue())
        self.assertTrue(stderr.getvalue().endswith("\n"))
        self.assertEqual(file_digest(archive), checksum)
        self.build.validate_package()

    def test_remote_hit_rejects_a_package_for_another_target(self) -> None:
        arm = RuntimeBuild(BuildSpec.create("linux/arm64", repo=ROOT), self.root)
        with patch.object(arm, "_build", side_effect=construct_package):
            arm.build_source()
        archive = self.root / "package.tar.gz"
        arm.write_archive(str(archive))
        payload = archive.read_bytes()
        pin_checksum(self.spec, hashlib.sha256(payload).hexdigest())
        self.mock_download(payload)
        with patch.object(self.build, "_build") as builder:
            with self.assertRaisesRegex(BuildError, "build inputs"):
                self.build.prepare()
        builder.assert_not_called()
        self.assertFalse(self.build.package_dir.exists())

    def test_storage_failure_does_not_start_a_build(self) -> None:
        pin_checksum(self.spec, "a" * 64)
        for code in (403, 429, 500):
            with self.subTest(status=code):
                body = io.BytesIO(b"unavailable")
                error = HTTPError("https://fixture", code, "Unavailable", {}, body)
                with patch("scripts.build_onnxruntime.urlopen", side_effect=error):
                    with patch.object(self.build, "_build") as builder:
                        with self.assertRaisesRegex(BuildError, f"R2 artifact download failed: .*HTTP {code}"):
                            self.build.prepare()
                builder.assert_not_called()
                self.assertTrue(body.closed)
                self.assertFalse(self.build.package_dir.exists())

    def test_public_not_found_requires_maintainer_publication(self) -> None:
        pin_checksum(self.spec, "a" * 64)
        body = io.BytesIO(b"not found")
        error = HTTPError("https://fixture", 404, "Not Found", {}, body)
        with patch("scripts.build_onnxruntime.urlopen", side_effect=error) as download:
            with patch.object(self.build, "_build") as builder:
                with self.assertRaisesRegex(BuildError, "unavailable.*maintainer.*publish-onnxruntime"):
                    self.build.prepare()
        download.assert_called_once()
        builder.assert_not_called()
        self.assertTrue(body.closed)
        self.assertFalse(self.build.package_dir.exists())

    def test_public_network_failure_does_not_start_a_build(self) -> None:
        pin_checksum(self.spec, "a" * 64)
        with patch("scripts.build_onnxruntime.urlopen", side_effect=URLError("connection interrupted")):
            with patch.object(self.build, "_build") as builder:
                with self.assertRaisesRegex(BuildError, "R2 artifact download failed: .*connection interrupted"):
                    self.build.prepare()
        builder.assert_not_called()
        self.assertFalse(self.build.package_dir.exists())

    def test_interrupted_public_download_closes_the_response_without_installing(self) -> None:
        pin_checksum(self.spec, "a" * 64)
        body = io.BytesIO(b"incomplete archive")
        with patch.object(body, "read", side_effect=OSError("download interrupted")):
            with patch("scripts.build_onnxruntime.urlopen", return_value=body):
                with patch.object(self.build, "_build") as builder:
                    with self.assertRaisesRegex(BuildError, "download or extraction failed: download interrupted"):
                        self.build.prepare()
        builder.assert_not_called()
        self.assertTrue(body.closed)
        self.assertFalse(self.build.package_dir.exists())

    def test_corrupt_download_is_not_installed(self) -> None:
        pin_checksum(self.spec, "0" * 64)
        self.mock_download(b"incomplete archive")
        with patch.object(self.build, "_build") as builder:
            with self.assertRaisesRegex(BuildError, "checksum"):
                self.build.prepare()
        builder.assert_not_called()
        self.assertFalse(self.build.package_dir.exists())

    def test_download_must_match_the_pinned_checksum_before_extraction(self) -> None:
        with patch.object(self.build, "_build", side_effect=construct_package):
            self.build.build_source()
        archive = self.root / "original.tar.gz"
        self.build.write_archive(str(archive))
        expected = hashlib.sha256(archive.read_bytes()).hexdigest()
        pin_checksum(self.spec, expected)
        (self.build.package_dir / "lib/libonnxruntime.a").write_bytes(b"!<arch>\ntampered fixture")
        (self.build.package_dir / "manifest.json").unlink()
        self.build._seal(self.build.package_dir)
        archive = self.root / "tampered.tar.gz"
        self.build.write_archive(str(archive))
        payload = archive.read_bytes()
        body = io.BytesIO(payload)
        self.enterContext(patch("scripts.build_onnxruntime.urlopen", return_value=body))
        restored = RuntimeBuild(self.spec, self.root / "restored")
        with patch.object(restored, "_build") as builder, patch(
            "scripts.build_onnxruntime.tarfile.open", wraps=tarfile.open
        ) as extraction:
            with self.assertRaisesRegex(BuildError, "pinned.*checksum"):
                restored.prepare()
        builder.assert_not_called()
        extraction.assert_not_called()
        self.assertTrue(body.closed)
        self.assertFalse(restored.package_dir.exists())
        self.assertEqual(self.spec.artifact_checksum, expected)

    def test_remote_download_requires_a_pin_before_network_access(self) -> None:
        download = self.mock_download(b"untrusted archive")
        with self.assertRaisesRegex(BuildError, "pinned.*checksum"):
            R2Cache().restore(self.spec, self.root / "destination")
        download.assert_not_called()

    def test_ci_requires_a_checksum_pin_before_downloading_or_building(self) -> None:
        download = self.mock_download(b"untrusted archive")
        with patch.dict(os.environ, {"CI": "true"}), patch.object(self.build, "_build") as builder:
            with self.assertRaisesRegex(BuildError, "pinned.*checksum"):
                self.build.prepare()
        download.assert_not_called()
        builder.assert_not_called()

    def test_checksum_pinning_preserves_the_build_identity(self) -> None:
        before = self.spec.fingerprint
        self.build_package()
        checksum = self.build.checksum()
        after = BuildSpec.create("linux/amd64", repo=self.repo)
        self.assertEqual(before, after.fingerprint)
        self.assertEqual(after.artifact_checksum, checksum)

    def test_archives_ignore_file_metadata_time_and_output_filename(self) -> None:
        self.build_package()
        first = self.root / "first.tar.gz"
        self.build.write_archive(str(first))
        for path in self.build.package_dir.rglob("*"):
            os.utime(path, (123456789, 123456789))
            path.chmod(0o700 if path.is_dir() else 0o600)
        second = self.root / "second.tar.gz"
        self.build.write_archive(str(second))
        self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_checksum_pinning_reuses_the_completed_archive(self) -> None:
        self.build_package()
        with patch.object(self.build, "write_archive") as writer:
            checksum = self.build.checksum()
        writer.assert_not_called()
        self.assertEqual(checksum, self.spec.artifact_checksum)

    def test_corrupt_cached_archive_is_recreated_from_the_validated_package(self) -> None:
        self.build_package()
        expected = self.spec.artifact_checksum
        (self.root / "archives" / f"{self.spec.fingerprint}.tar.gz").write_bytes(b"incomplete archive")
        self.assertEqual(self.build.checksum(), expected)
        self.build.validate_package()

    def test_changed_pin_is_checked_for_a_completed_download(self) -> None:
        self.build_package()
        archive = self.root / "package.tar.gz"
        self.build.write_archive(str(archive))
        payload = archive.read_bytes()
        download = self.mock_download(payload)
        restored = RuntimeBuild(self.spec, self.root / "restored")
        restored.prepare()
        pin_checksum(self.spec, "b" * 64)
        with patch.object(restored, "_build") as builder:
            with self.assertRaisesRegex(BuildError, "pinned.*checksum"):
                restored.prepare()
        builder.assert_not_called()
        self.assertEqual(download.call_count, 2)

    def test_manual_rebuild_replaces_the_verification_receipt_when_repaired_and_pinned(self) -> None:
        self.build_package()
        pin_checksum(self.spec, "b" * 64)
        (self.build.package_dir / "lib/libonnxruntime.a").write_bytes(b"interrupted artifact")
        with patch.object(self.build, "_build", side_effect=construct_package) as builder:
            self.build.build_source()
            self.build.checksum()
            self.build.prepare()
        builder.assert_called_once()
        self.build.validate_package()

    def test_failed_build_is_retried_without_a_completion_marker(self) -> None:
        with patch.object(self.build, "_build", side_effect=BuildError("compile failed")):
            with self.assertRaisesRegex(BuildError, "compile failed"):
                self.build.build_source()
        self.assertFalse(self.build.package_dir.exists())
        self.build_package().assert_called_once()
        self.build.validate_package()

    def test_upload_retry_reuses_the_completed_build(self) -> None:
        self.build_package()
        remote = Mock(spec=R2Publisher)
        remote.publish.side_effect = BuildError("upload failed")
        with self.assertRaisesRegex(BuildError, "upload failed"):
            self.build.publish(remote)
        self.build.validate_package()
        remote.publish.side_effect = None
        self.build_package().assert_not_called()
        self.build.publish(remote)
        self.assertEqual(remote.publish.call_count, 2)

    def test_ci_uses_a_local_package_without_resolving_compilers(self) -> None:
        self.build_package()
        with patch.dict(os.environ, {"CI": "true"}):
            with patch("scripts.build_onnxruntime.HostToolchain.discover", side_effect=AssertionError("compiler lookup")):
                self.assertEqual(self.build.prepare(), self.build.package_dir / "lib")

    def test_ci_disables_explicit_source_builds_even_with_a_local_package(self) -> None:
        self.build_package()
        with patch.dict(os.environ, {"CI": "true"}), patch.object(self.build, "_build") as builder:
            with self.assertRaisesRegex(BuildError, "source compilation.*manual operation.*disabled in CI"):
                self.build.build_source()
        builder.assert_not_called()
        self.build.validate_package()

    def test_unpinned_manual_build_is_retained_for_checksum_pinning(self) -> None:
        with patch.object(self.build, "_build", side_effect=construct_package) as builder:
            self.build.build_source()
            with self.assertRaisesRegex(BuildError, "no pinned artifact checksum.*maintainer"):
                self.build.prepare()
            self.build.build_source()
        builder.assert_called_once()
        self.build.checksum()
        self.assertEqual(self.build.prepare(), self.build.package_dir / "lib")

    def test_ci_publication_is_disabled(self) -> None:
        self.build_package()
        remote = Mock(spec=R2Publisher)
        with patch.dict(os.environ, {"CI": "true"}):
            with self.assertRaisesRegex(BuildError, "manual operation.*disabled in CI"):
                self.build.publish(remote)
        remote.publish.assert_not_called()

    def test_deleted_archive_rebuilds_from_current_inputs(self) -> None:
        self.build_package()
        (self.build.package_dir / "lib/libonnxruntime.a").unlink()
        self.build_package().assert_called_once()
        self.build.validate_package()

    def test_each_platform_has_one_artifact_with_its_required_execution_support(self) -> None:
        arm = BuildSpec.create("linux/arm64", repo=ROOT)
        mac = BuildSpec.create("darwin/arm64", repo=ROOT)
        self.assertEqual(len({self.spec.fingerprint, arm.fingerprint, mac.fingerprint}), 3)
        self.assertTrue(self.spec.cuda_enabled)
        self.assertTrue(arm.cuda_enabled)
        self.assertFalse(mac.cuda_enabled)
        for spec in (self.spec, arm, mac):
            self.assertEqual(spec.object_key,
                             f"1.24.2/{spec.platform}/{spec.fingerprint}.tar.gz")

    def test_manifest_covers_all_packaged_files(self) -> None:
        self.build_package()
        manifest = json.loads((self.build.package_dir / "manifest.json").read_text())
        self.assertEqual(set(manifest["files"]), {
            "lib/libonnxruntime.a", "include/onnxruntime_c_api.h", "LICENSE", "ThirdPartyNotices.txt",
            "runtime/lib/libonnxruntime_providers_shared.so", "runtime/lib/libonnxruntime_providers_cuda.so",
            "runtime/lib/libcudnn_graph.so.9", "runtime/lib/libnvrtc-builtins.so.13.2",
        })
        (self.build.package_dir / "include/onnxruntime_c_api.h").write_text("modified")
        with self.assertRaisesRegex(BuildError, "checksum"):
            self.build.validate_package()

    def test_manifest_must_match_the_requested_inputs(self) -> None:
        self.build_package()
        manifest_path = self.build.package_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["identity"]["platform"] = "linux/arm64"
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(BuildError, "build inputs"):
            self.build.validate_package()

    def test_manifest_requires_a_checksum_map(self) -> None:
        self.build_package()
        manifest_path = self.build.package_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["files"] = []
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(BuildError, "file map"):
            self.build.validate_package()

    def test_vendor_library_keeps_its_runtime_loader_name(self) -> None:
        source = self.root / "libnvrtc-builtins.so.13.0.88"
        source.write_bytes(b"vendor fixture")
        destination = self.root / "runtime"
        destination.mkdir()
        with patch("scripts.build_onnxruntime.subprocess.check_output",
                   return_value="Dynamic Section:\n  SONAME libnvrtc-builtins.so.13.0\n"):
            target = copy_shared_library(source, destination)
            self.assertEqual(copy_shared_library(source, destination), target)
        self.assertEqual(target.name, "libnvrtc-builtins.so.13.0")
        self.assertEqual(target.read_bytes(), source.read_bytes())
        self.assertEqual(list(destination.iterdir()), [target])

    def test_cuda_package_resolves_dependencies_from_the_selected_host_sdks(self) -> None:
        for platform, cuda_relative, cudnn_relative in (
            ("linux/amd64", "lib64", "lib/x86_64-linux-gnu"),
            ("linux/arm64", "targets/sbsa-linux/lib", "lib/aarch64-linux-gnu"),
        ):
            with self.subTest(platform=platform):
                self.assert_cuda_package(platform, cuda_relative, cudnn_relative)

    def test_arm64_packaging_rejects_a_cudnn_sdk_for_another_architecture(self) -> None:
        with self.assertRaisesRegex(BuildError, "ELF.*linux/arm64"):
            self.assert_cuda_package("linux/arm64", "targets/sbsa-linux/lib", "lib/aarch64-linux-gnu",
                                     cudnn_platform="linux/amd64")

    def test_arm64_cross_package_selects_sbsa_libraries_from_a_mixed_sdk(self) -> None:
        self.assert_cuda_package("linux/arm64", "targets/sbsa-linux/lib", "lib/aarch64-linux-gnu", cross=True)

    def assert_cuda_package(self, platform: str, cuda_relative: str, cudnn_relative: str,
                           *, cudnn_platform: str | None = None, cross: bool = False) -> None:
        root = self.root / platform.split("/")[-1]
        build = RuntimeBuild(BuildSpec.create(platform, repo=ROOT), root)
        build.build_dir.mkdir(parents=True)
        for name in ("libonnxruntime_providers_shared.so", "libonnxruntime_providers_cuda.so"):
            (build.build_dir / name).write_bytes(elf_fixture(platform))
        cuda = root / "cuda"
        cudnn = root / "cudnn"
        for sdk, relative, names in (
            (cuda, cuda_relative, ("libnvrtc.so.13", "libnvrtc-builtins.so.13.2", "libcublas.so.13", "libnvJitLink.so.13")),
            (cudnn, cudnn_relative, ("libcudnn.so.9", "libcudnn_graph.so.9")),
        ):
            (sdk / relative).mkdir(parents=True)
            for name in names:
                (sdk / relative / name).write_bytes(elf_fixture(cudnn_platform or platform if sdk == cudnn else platform))
            (sdk / "LICENSE.txt").write_text("SDK license")
        if cross:
            (cuda / "lib64").mkdir()
            (cuda / "lib64/libnvrtc.so.13").write_bytes(elf_fixture("linux/amd64"))
        destination = root / "package"
        tools = Mock(cuda_home=cuda, cudnn_home=cudnn, cross=Mock() if cross else None)

        def inspect_library(command: list[str], **kwargs: object) -> str:
            self.assertEqual(command[0], "objdump")
            name = Path(command[-1]).name
            dependencies = "  NEEDED libc.so.6\n"
            if name == "libonnxruntime_providers_cuda.so":
                dependencies += "  NEEDED libcublas.so.13\n  NEEDED libcuda.so.1\n"
            if name == "libcublas.so.13":
                dependencies += "  NEEDED libnvJitLink.so.13\n"
            return f"  SONAME {name}\n{dependencies}"

        with patch("scripts.build_onnxruntime.subprocess.check_output", side_effect=inspect_library):
            build._cuda_runtime(destination, tools)
        libraries = {path.name for path in (destination / "runtime/lib").iterdir()}
        self.assertEqual(libraries, {"libonnxruntime_providers_shared.so", "libonnxruntime_providers_cuda.so",
                                    "libnvrtc.so.13", "libnvrtc-builtins.so.13.2", "libcublas.so.13",
                                    "libcudnn.so.9", "libcudnn_graph.so.9", "libnvJitLink.so.13"})
        self.assertEqual((destination / "runtime/licenses/cuda-LICENSE.txt").read_text(), "SDK license")
        self.assertEqual((destination / "runtime/licenses/cudnn-LICENSE.txt").read_text(), "SDK license")

    def test_cuda_smoke_loads_packaged_providers_and_can_require_gpu_inference(self) -> None:
        build = RuntimeBuild(BuildSpec.create("linux/amd64", repo=ROOT), self.root)
        build.build_dir.mkdir(parents=True)
        destination = self.root / "package"
        (destination / "runtime/lib").mkdir(parents=True)
        for name in ("libonnxruntime_providers_shared.so", "libonnxruntime_providers_cuda.so"):
            (destination / "runtime/lib" / name).write_bytes(b"provider fixture")
        tools = Mock(cxx=Mock(command=["kache", "clang++-22"]), cross=None)
        with patch("scripts.build_onnxruntime.run") as command:
            build._smoke(destination, tools)
            load = command.call_args
            self.assertEqual(load.args[0][-1], "cuda-load")
            self.assertEqual(load.kwargs["env"]["LD_LIBRARY_PATH"], str(destination / "runtime/lib"))
            executable = Path(load.args[0][0])
            self.assertEqual((executable.parent / "libonnxruntime_providers_cuda.so").resolve(),
                             destination / "runtime/lib/libonnxruntime_providers_cuda.so")
            command.reset_mock()
            build._smoke(destination, tools, gpu=True)
            self.assertEqual(command.call_args.args[0][-1], "cuda")
            models = [Path(call.args[0][1]).name for call in command.call_args_list
                      if Path(call.args[0][0]).name == "nervix-onnx-smoke"]
            self.assertEqual(models, ["output.onnx", "convolution-output.onnx"])

    def test_gpu_verification_uses_a_cached_package_without_the_cuda_sdk(self) -> None:
        build = RuntimeBuild(BuildSpec.create("linux/amd64", repo=self.repo), self.root)
        build.package_dir.mkdir(parents=True)
        construct_package(build.package_dir, platform=build.spec.platform)
        build._seal(build.package_dir)
        build.checksum()
        with patch("scripts.build_onnxruntime.native_platform", return_value="linux/amd64"), \
                patch("scripts.build_onnxruntime.HostToolchain.discover") as discover:
            with patch.object(build, "_smoke") as smoke:
                build.verify()
        discover.assert_called_once_with("linux/amd64", require_cuda=False)
        smoke.assert_called_once_with(build.package_dir, discover.return_value, gpu=True)

    def test_arm64_gpu_verification_requires_a_native_arm64_host(self) -> None:
        build = RuntimeBuild(BuildSpec.create("linux/arm64", repo=self.repo), self.root)
        build.package_dir.mkdir(parents=True)
        construct_package(build.package_dir, platform=build.spec.platform)
        build._seal(build.package_dir)
        build.checksum()
        with patch("scripts.build_onnxruntime.native_platform", return_value="linux/amd64"):
            with patch("scripts.build_onnxruntime.HostToolchain.discover") as compiler:
                with self.assertRaisesRegex(BuildError, "GPU verification requires a matching Linux host"):
                    build.verify()
        compiler.assert_not_called()

    def test_extraction_cannot_escape_the_stage_directory(self) -> None:
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w:gz") as archive:
            entry = tarfile.TarInfo("../../../escaped")
            entry.size = 7
            archive.addfile(entry, io.BytesIO(b"fixture"))
        payload = stream.getvalue()
        pin_checksum(self.spec, hashlib.sha256(payload).hexdigest())
        self.mock_download(payload)
        with patch.object(self.build, "_build") as builder:
            with self.assertRaisesRegex(BuildError, "extraction"):
                self.build.prepare()
        builder.assert_not_called()
        self.assertFalse(self.build.package_dir.exists())
        self.assertFalse((self.root / "escaped").exists())

    def test_concurrent_manual_builds_compile_one_complete_package(self) -> None:
        barrier = threading.Barrier(2)

        def prepare() -> Path:
            barrier.wait(timeout=10)
            return RuntimeBuild(self.spec, self.root).build_source()

        with patch.object(RuntimeBuild, "_build", side_effect=construct_package) as builder:
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(prepare) for _ in range(2)]
                paths = [future.result(timeout=10) for future in futures]
        builder.assert_called_once()
        self.assertEqual(paths, [self.build.package_dir / "lib"] * 2)
        self.build.validate_package()

    def test_concurrent_fetches_download_one_complete_artifact(self) -> None:
        self.build_package()
        archive = self.build.archive()
        download = self.mock_download(archive.read_bytes())
        root = self.root / "downloaded"
        barrier = threading.Barrier(2)

        def fetch() -> Path:
            barrier.wait(timeout=10)
            return RuntimeBuild(self.spec, root).prepare()

        with patch.object(RuntimeBuild, "_build") as builder:
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(fetch) for _ in range(2)]
                paths = [future.result(timeout=10) for future in futures]
        builder.assert_not_called()
        download.assert_called_once()
        self.assertEqual(paths, [RuntimeBuild(self.spec, root).package_dir / "lib"] * 2)

    def test_changed_builder_inputs_invalidate_the_cache(self) -> None:
        checkout = self.root / "repo"
        for name in self.spec.identity["build_files"]:
            target = checkout / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)
        shutil.copyfile(ROOT / "scripts/onnxruntime/manifest.toml", checkout / "scripts/onnxruntime/manifest.toml")
        before = BuildSpec.create("linux/amd64", repo=checkout)
        with (checkout / "scripts/onnxruntime/aggregate.cmake").open("a") as stream:
            stream.write("\n# changed build input\n")
        after = BuildSpec.create("linux/amd64", repo=checkout)
        self.assertNotEqual(before.fingerprint, after.fingerprint)

    def test_r2_publication_uses_the_fixed_destination_with_s3_credentials(self) -> None:
        credentials = {"R2_ACCESS_KEY_ID": "fixture-key", "R2_SECRET_ACCESS_KEY": "fixture-secret"}
        with patch.dict(os.environ, credentials, clear=True):
            with patch("boto3.client") as create_client:
                remote = R2Publisher.configured()
        self.assertIsNotNone(remote)
        self.assertIs(remote.client, create_client.return_value)
        self.assertEqual(remote.bucket, "nervix-artifacts")
        self.assertEqual(create_client.call_args.args, ("s3",))
        options = create_client.call_args.kwargs
        self.assertEqual(options["endpoint_url"],
                         "https://93f53d256da23587269279513de3fc80.r2.cloudflarestorage.com")
        self.assertEqual(options["region_name"], "auto")
        self.assertEqual(options["aws_access_key_id"], "fixture-key")
        self.assertEqual(options["aws_secret_access_key"], "fixture-secret")

    def test_r2_publication_requires_complete_credentials(self) -> None:
        with patch.dict(os.environ, {"R2_ACCESS_KEY_ID": "fixture"}, clear=True):
            with self.assertRaisesRegex(BuildError, "R2_SECRET_ACCESS_KEY"):
                R2Publisher.configured()
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(BuildError, "R2_ACCESS_KEY_ID.*R2_SECRET_ACCESS_KEY"):
                R2Publisher.configured()

    def test_unchanged_sources_are_reused_without_fetching(self) -> None:
        self.build.source_dir.mkdir(parents=True)
        with patch("scripts.build_onnxruntime.subprocess.check_output",
                   side_effect=[self.spec.configuration["revision"], ""]):
            with patch("scripts.build_onnxruntime.run") as command:
                self.build._source()
        command.assert_not_called()

    def test_modified_sources_are_rejected_before_building(self) -> None:
        self.build.source_dir.mkdir(parents=True)
        with patch("scripts.build_onnxruntime.subprocess.check_output",
                   side_effect=[self.spec.configuration["revision"], " M CMakeLists.txt\n"]):
            with patch("scripts.build_onnxruntime.run") as command:
                with self.assertRaisesRegex(BuildError, "checkout contains changes"):
                    self.build._source()
        command.assert_not_called()


@unittest.skipUnless(shutil.which("just") and shutil.which("uv"), "requires just and uv")
class RecipePreparationTests(unittest.TestCase):
    def test_install_reuses_the_runtime_and_passes_cargo_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = fixture_repository(root / "repository")
            build = RuntimeBuild(BuildSpec.create("native", repo=repo), root / "stage")
            build.package_dir.mkdir(parents=True)
            construct_package(build.package_dir, platform=build.spec.platform)
            build._seal(build.package_dir)
            build.checksum()
            fake_bin = root / "bin"
            fake_bin.mkdir()
            log = root / "commands.jsonl"
            cargo = fake_bin / "cargo"
            cargo.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
library = pathlib.Path(os.environ['ORT_LIB_PATH'])
assert (library / 'libonnxruntime.a').is_file()
assert (library.parent / 'manifest.json').is_file()
with pathlib.Path(os.environ['RECIPE_TEST_LOG']).open('a') as stream:
    stream.write(json.dumps({'command': 'cargo', 'arguments': sys.argv[1:], 'library': str(library),
                            'target': os.environ['CARGO_TARGET_DIR']}) + '\\n')
""")
            cargo.chmod(0o755)
            trunk = fake_bin / "trunk"
            trunk.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
with pathlib.Path(os.environ['RECIPE_TEST_LOG']).open('a') as stream:
    stream.write(json.dumps({'command': 'trunk', 'arguments': sys.argv[1:]}) + '\\n')
""")
            trunk.chmod(0o755)
            for name in ("docker", "git"):
                command = fake_bin / name
                command.write_text("#!/bin/sh\nexit 99\n")
                command.chmod(0o755)
            environment = os.environ.copy()
            environment.pop("CARGO_TARGET_DIR", None)
            for name in ("CI", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
                environment.pop(name, None)
            environment.update({
                "PATH": f"{fake_bin}:{environment['PATH']}",
                "NERVIX_ONNXRUNTIME_DIR": str(build.stage_root), "RECIPE_TEST_LOG": str(log),
            })
            for _ in range(2):
                result = subprocess.run(["just", "install", "--force", "--offline"], cwd=repo,
                                        env=environment, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
            commands = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual([command["command"] for command in commands],
                             ["trunk", "cargo", "cargo", "cargo"] * 2)
            installed = [command for command in commands if command["command"] == "cargo"]
            self.assertEqual([command["arguments"] for command in installed], [
                ["install", "--locked", "--path", path, "--force", "--offline"]
                for path in (".", "crates/nervix-cli", "crates/nspl-format")
            ] * 2)
            self.assertEqual({command["library"] for command in installed},
                             {str(build.package_dir / "lib")})
            self.assertEqual({command["target"] for command in installed}, {str(repo / "target")})
            build.validate_package()

    def test_image_build_uses_the_prepared_package(self) -> None:
        for platform in ("linux/amd64", "linux/aarch64"):
            with self.subTest(platform=platform):
                self.assert_image_build(platform)

    def assert_image_build(self, platform: str) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = fixture_repository(root / "repository")
            spec = BuildSpec.create(platform, repo=repo)
            build = RuntimeBuild(spec, root / "stage")
            build.package_dir.mkdir(parents=True)
            construct_package(build.package_dir, platform=build.spec.platform)
            build._seal(build.package_dir)
            build.checksum()
            fake_bin = root / "bin"
            fake_bin.mkdir()
            log = root / "docker.json"
            docker = fake_bin / "docker"
            docker.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
arguments = sys.argv[1:]
if arguments == ['run', '--privileged', '--rm', 'tonistiigi/binfmt', '--install', 'all']:
    sys.exit(0)
assert arguments[:2] == ['buildx', 'build'], arguments
context = arguments[arguments.index('--build-context') + 1]
package = pathlib.Path(context.removeprefix('onnxruntime='))
assert (package / 'manifest.json').is_file()
assert (package / 'lib/libonnxruntime.a').is_file()
assert (package / 'runtime/lib/libonnxruntime_providers_cuda.so').is_file()
pathlib.Path(os.environ['DOCKER_TEST_LOG']).write_text(json.dumps(arguments))
""")
            docker.chmod(0o755)
            environment = os.environ.copy()
            for name in ("CI", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
                environment.pop(name, None)
            environment.update({
                "PATH": f"{fake_bin}:{environment['PATH']}",
                "NERVIX_ONNXRUNTIME_DIR": str(build.stage_root), "DOCKER_TEST_LOG": str(log),
                "KACHE_S3_BUCKET": "fixture", "KACHE_S3_REGION": "fixture",
                "KACHE_S3_ENDPOINT": "https://fixture.r2.cloudflarestorage.com",
                "KACHE_S3_ACCESS_KEY": "fixture", "KACHE_S3_SECRET_KEY": "fixture",
            })
            recipe = ["just", "docker-build-debian", "trixie", "23", "fixture:debian", platform]
            environment.update({
                "CI": "true", "CC": "unavailable-clang", "CXX": "unavailable-clang++",
            })
            for _ in range(2):
                result = subprocess.run(recipe, cwd=repo, env=environment,
                                        capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                arguments = json.loads(log.read_text())
                self.assertIn(f"onnxruntime={build.package_dir}", arguments)
                self.assertIn(f"KACHE_S3_ENDPOINT={environment['KACHE_S3_ENDPOINT']}", arguments)
                self.assertEqual(arguments[arguments.index("--platform") + 1], spec.platform)


if __name__ == "__main__":
    unittest.main()
