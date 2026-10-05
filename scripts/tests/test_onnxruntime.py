from __future__ import annotations

import json
import os
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[2]
DOWNLOADER = REPOSITORY / "scripts/download_onnxruntime.sh"


class OnnxRuntimeDownloadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.work = self.root / "working directory"
        self.work.mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.root / "calls.jsonl"
        self.environment = os.environ | {
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "DOWNLOAD_CALLS": str(self.calls),
        }
        self.fake_executable(
            "curl",
            "import shutil\n"
            "shutil.copyfile(os.environ['DOWNLOAD_ARCHIVE'], sys.argv[sys.argv.index('-o') + 1])\n",
        )

    def fake_executable(self, name: str, action: str = "") -> None:
        path = self.bin / name
        path.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "with open(os.environ['DOWNLOAD_CALLS'], 'a') as calls:\n"
            "    calls.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            + action
        )
        path.chmod(0o755)

    def run_download(self, *arguments: str, **environment: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(DOWNLOADER), *arguments],
            cwd=self.work,
            env=self.environment | environment,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_downloads_target_packages_and_reuses_the_installed_library(self) -> None:
        for platform, flavor, library in (
            ("linux-x64", "cpu", "libonnxruntime.so"),
            ("linux-aarch64", "cpu", "libonnxruntime.so"),
            ("osx-arm64", "cpu", "libonnxruntime.dylib"),
            ("linux-x64", "gpu_cuda13", "libonnxruntime.so"),
        ):
            with self.subTest(platform=platform, flavor=flavor):
                suffix = "" if flavor == "cpu" else f"-{flavor}"
                package = f"onnxruntime-{platform}{suffix}-1.30.0"
                source = self.root / package / "lib"
                source.mkdir(parents=True)
                (source / f"{library}.1.30.0").write_bytes(b"runtime fixture")
                (source / library).symlink_to(f"{library}.1.30.0")
                archive = self.root / f"{package}.tgz"
                with tarfile.open(archive, "w:gz") as output:
                    output.add(source.parent, arcname=package)
                selection = ("--platform", platform, "--flavor", flavor)
                environment = {"DOWNLOAD_ARCHIVE": str(archive)}
                downloaded = self.run_download(*selection, **environment)
                self.assertEqual(downloaded.returncode, 0, downloaded.stderr)
                library_path = Path(downloaded.stdout.strip())
                self.assertEqual(library_path, self.work / ".nervix-deps/onnxruntime" / package / "lib" / library)
                self.assertEqual(library_path.read_bytes(), b"runtime fixture")
                self.assertTrue(library_path.is_symlink())
                calls = self.calls.read_text().splitlines()
                self.assertIn(
                    f"https://github.com/microsoft/onnxruntime/releases/download/v1.30.0/{package}.tgz",
                    json.loads(calls[-1]),
                )
                for arguments in ((), ("--print-path",)):
                    repeated = self.run_download(*selection, *arguments, **environment)
                    self.assertEqual(repeated.returncode, 0, repeated.stderr)
                    self.assertEqual(repeated.stdout, downloaded.stdout)
                    self.assertEqual(self.calls.read_text().splitlines(), calls)

    def test_rejects_unpublished_gpu_platforms_before_downloading(self) -> None:
        for platform in ("linux-aarch64", "osx-arm64"):
            with self.subTest(platform=platform):
                result = self.run_download(
                    "--platform", platform, "--flavor", "gpu_cuda13",
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(platform, result.stderr)
                self.assertIn("gpu_cuda13", result.stderr)
                self.assertFalse(self.calls.exists())

    def test_print_path_is_absolute_without_downloading(self) -> None:
        result = self.run_download(
            "--print-path", "--platform", "linux-x64",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            Path(result.stdout.strip()),
            self.work / ".nervix-deps/onnxruntime/onnxruntime-linux-x64-1.30.0/lib/libonnxruntime.so",
        )
        self.assertFalse(self.calls.exists())

    def test_download_recipe_selects_flavor_and_platform(self) -> None:
        package = "onnxruntime-linux-x64-gpu_cuda13-1.30.0"
        source = self.root / package / "lib"
        source.mkdir(parents=True)
        (source / "libonnxruntime.so").write_bytes(b"runtime fixture")
        archive = self.root / f"{package}.tgz"
        with tarfile.open(archive, "w:gz") as output:
            output.add(source.parent, arcname=package)
        (self.work / "scripts").symlink_to(REPOSITORY / "scripts", target_is_directory=True)
        (self.work / "rust-toolchain.toml").symlink_to(REPOSITORY / "rust-toolchain.toml")
        result = subprocess.run(
            [
                "just", "--justfile", str(REPOSITORY / "justfile"),
                "--working-directory", str(self.work),
                "download-onnxruntime", "gpu_cuda13", "linux-x64",
            ],
            cwd=REPOSITORY,
            env=self.environment | {"DOWNLOAD_ARCHIVE": str(archive)},
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            Path(result.stdout.strip()),
            self.work / ".nervix-deps/onnxruntime" / package / "lib/libonnxruntime.so",
        )

    def test_image_recipes_select_the_target_and_build_options(self) -> None:
        self.fake_executable("docker")
        environment = self.environment | {
            "KACHE_S3_BUCKET": "test-bucket",
            "KACHE_S3_REGION": "auto",
            "KACHE_S3_ENDPOINT": "https://test.example.com",
            "KACHE_S3_ACCESS_KEY": "test-key",
            "KACHE_S3_SECRET_KEY": "test-secret",
        }
        for recipe, target in (("docker-build-debian", "cpu"), ("docker-build-cuda", "cuda")):
            with self.subTest(recipe=recipe):
                self.calls.unlink(missing_ok=True)
                result = subprocess.run(
                    [
                        "just", recipe, "23", "nervix:test", "linux/amd64",
                        "true", "type=local,src=cache input", "type=local,dest=cache output",
                    ],
                    cwd=REPOSITORY,
                    env=environment,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                commands = [json.loads(line) for line in self.calls.read_text().splitlines()]
                builds = [arguments for arguments in commands if arguments[:2] == ["buildx", "build"]]
                self.assertEqual(len(builds), 1)
                arguments = builds[0]
                self.assertEqual(arguments[:2], ["buildx", "build"])
                self.assertIn("LLVM_VERSION=23", arguments)
                self.assertEqual(arguments[arguments.index("--target") + 1], target)
                self.assertIn("nervix:test", arguments)
                self.assertIn("--push", arguments)
                self.assertIn("--cache-from=type=local,src=cache input", arguments)
                self.assertIn("--cache-to=type=local,dest=cache output", arguments)

    def test_cuda_recipe_rejects_a_platform_without_an_upstream_gpu_build(self) -> None:
        self.fake_executable("docker")
        result = subprocess.run(
            ["just", "docker-build-cuda", "23", "nervix:test-cuda", "linux/arm64"],
            cwd=REPOSITORY,
            env=self.environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("linux/amd64", result.stderr)
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
