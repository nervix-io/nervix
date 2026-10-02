from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import tomllib
import unittest

from scripts.build_onnxruntime import BuildSpec, RuntimeBuild, file_digest, native_platform
from scripts.tests.test_build_onnxruntime import construct_package, fixture_repository


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(shutil.which("just") and shutil.which("uv"), "requires just and uv")
class ArtifactRecipeTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.repo = fixture_repository(self.root / "repository")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.log = self.root / "publication.jsonl"
        uv = self.bin / "uv"
        uv.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
sys.path.insert(0, str(pathlib.Path.cwd()))
from scripts import build_onnxruntime as builder
from scripts.onnxruntime import artifacts
assert sys.argv[1:6] == ['run', '--locked', 'python', '-m', 'scripts.onnxruntime.artifacts']
class Cache:
    def restore(self, spec, destination):
        return None
class Publisher:
    bucket = 'fixture'
    def key(self, spec):
        return 'onnxruntime/' + spec.object_key
    def publish(self, build):
        archive = pathlib.Path(os.environ['RECIPE_ARCHIVE'])
        build.write_archive(str(archive))
        import hashlib
        assert hashlib.sha256(archive.read_bytes()).hexdigest() == build.spec.artifact_checksum
        with pathlib.Path(os.environ['RECIPE_PUBLICATION']).open('a') as stream:
            stream.write(json.dumps({'key': self.key(build.spec), 'package': str(build.package_dir)}) + '\\n')
if os.environ.get('RECIPE_PUBLIC_URL'):
    builder.R2_PUBLIC_URL = os.environ['RECIPE_PUBLIC_URL']
else:
    builder.R2Cache = Cache
def publisher():
    if os.environ.get('RECIPE_ALLOW_PUBLICATION') != 'true':
        raise builder.BuildError('R2 publishing permissions unavailable')
    return Publisher()
builder.R2Publisher.configured = publisher
def compile_artifact(self, destination):
    (destination / 'lib').mkdir()
    (destination / 'include').mkdir()
    (destination / 'lib/libonnxruntime.a').write_bytes(b'!<arch>\\n' + os.environ.get('RECIPE_GENERATION', 'fixture').encode())
    (destination / 'include/onnxruntime_c_api.h').write_text('fixture header')
    (destination / 'LICENSE').write_text('fixture license')
    (destination / 'ThirdPartyNotices.txt').write_text('fixture notices')
    if self.spec.cuda_enabled:
        runtime = destination / 'runtime/lib'
        runtime.mkdir(parents=True)
        for name in ('libonnxruntime_providers_shared.so', 'libonnxruntime_providers_cuda.so',
                     'libcudnn_graph.so.9', 'libnvrtc-builtins.so.13.2'):
            (runtime / name).write_bytes(b'fixture library')
    with pathlib.Path(os.environ['RECIPE_COMPILATION']).open('a') as stream:
        stream.write(self.spec.platform + '\\n')
artifacts.ManagedRuntimeBuild._build = compile_artifact
if os.environ.get('RECIPE_REQUIRE_REUSE') == 'true':
    def archive(self):
        raise AssertionError('completed artifact should not be compressed again')
    builder.RuntimeBuild.archive = archive
sys.argv = ['onnxruntime', *sys.argv[6:]]
sys.exit(artifacts.main())
""")
        uv.chmod(0o755)
        docker = self.bin / "docker"
        docker.write_text("#!/bin/sh\nexit 99\n")
        docker.chmod(0o755)
        self.environment = os.environ.copy()
        for name in ("CI", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
            self.environment.pop(name, None)
        self.environment.update({
            "PATH": f"{self.bin}:{self.environment['PATH']}",
            "NERVIX_ONNXRUNTIME_DIR": str(self.root / "stage"),
            "RECIPE_PUBLICATION": str(self.log), "RECIPE_ARCHIVE": str(self.root / "package.tar.gz"),
            "RECIPE_COMPILATION": str(self.root / "compilation.log"),
            "RECIPE_ALLOW_PUBLICATION": "false",
            "CC": "unavailable-clang", "CXX": "unavailable-clang++",
            "KACHE_S3_BUCKET": "fixture", "KACHE_S3_REGION": "fixture",
            "KACHE_S3_ENDPOINT": "https://fixture.r2.cloudflarestorage.com",
            "KACHE_S3_ACCESS_KEY": "fixture", "KACHE_S3_SECRET_KEY": "fixture",
        })

    def test_development_and_ci_require_published_artifacts_before_building_the_product(self) -> None:
        recipes = [
            ["build-server"], ["server"], ["tests-deps"], ["test"], ["docker-build-debian"],
            ["test-admission-runtime"],
            ["test-runtime"], ["test-capability-docs"], ["test-runtime-state-capabilities"],
            ["test-endpoint-intake"], ["bench-endpoint-routing"],
            ["bench-admitted-work"], ["bench-state-replication"], ["bench-task-handles"],
            ["coverage-task-handles"], ["coverage-runtime"], ["bench-smoke"], ["ratchet"],
            ["coverage-scenarios", str(self.root / "scenarios.lcov")],
            ["coverage-scenarios-append", str(self.root / "scenarios.lcov")],
            ["test-package-test", "nervix-server", "scenarios"],
        ]
        for ci in ("", "true"):
            self.environment["CI"] = ci
            for recipe in recipes:
                with self.subTest(ci=ci, recipe=recipe):
                    result = subprocess.run(["just", *recipe], cwd=self.repo, env=self.environment,
                                            capture_output=True, text=True, timeout=30)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("normal development requires a published R2 artifact", result.stderr)
                    self.assertIn("ask a maintainer", result.stderr)
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())

    def test_package_clippy_prepares_the_runtime_before_compiling_the_server(self) -> None:
        cargo_log = self.root / "cargo.json"
        self.environment["RECIPE_CARGO_LOG"] = str(cargo_log)
        cargo = self.bin / "cargo"
        cargo.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
pathlib.Path(os.environ['RECIPE_CARGO_LOG']).write_text(json.dumps(sys.argv[1:]))
sys.exit(0)
""")
        cargo.chmod(0o755)
        for ci in ("", "true"):
            self.environment["CI"] = ci
            with self.subTest(ci=ci, package="nervix-server"):
                result = subprocess.run(["just", "cargo-clippy-package", "nervix-server"],
                                        cwd=self.repo, env=self.environment, capture_output=True, text=True, timeout=30)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("normal development requires a published R2 artifact", result.stderr)
                self.assertFalse(cargo_log.exists())
            with self.subTest(ci=ci, package="nervix-cli"):
                result = subprocess.run(["just", "cargo-clippy-package", "nervix-cli"],
                                        cwd=self.repo, env=self.environment, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                arguments = json.loads(cargo_log.read_text())
                self.assertEqual(arguments[:3], ["clippy", "--package", "nervix-cli"])
                cargo_log.unlink()
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())

    def test_validation_fetches_the_runtime_before_starting_parallel_workers(self) -> None:
        result = subprocess.run(["just", "--dump", "--dump-format", "json"], cwd=self.repo,
                                env=self.environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        recipes = json.loads(result.stdout)["recipes"]
        for name, workers in (
            ("cargo-clippy", "clippy-targets"), ("cargo-clippy-loom", "loom-clippy-targets"),
            ("lint", "lint-targets"), ("validate", "validate-targets"), ("validate-ci", "validate-ci-targets"),
        ):
            with self.subTest(recipe=name):
                dependencies = recipes[name]["dependencies"]
                self.assertEqual(dependencies[0]["recipe"], "fetch-onnxruntime")
                self.assertEqual(dependencies[1]["recipe"], "run-with-jobs")
                self.assertEqual(dependencies[1]["arguments"], [workers, ["variable", "jobs"]])
        for name in ("validate-targets", "validate-ci-targets"):
            self.assertIn("test-onnxruntime-tooling", [item["recipe"] for item in recipes[name]["dependencies"]])

    def test_recipes_select_the_same_home_cache_from_every_workspace(self) -> None:
        second_repo = fixture_repository(self.root / "second-workspace")
        environment = self.environment.copy()
        environment.pop("NERVIX_ONNXRUNTIME_DIR")
        for repo in (self.repo, second_repo):
            result = subprocess.run(["just", "--evaluate", "ORT_LIB_PATH"], cwd=repo,
                                    env=environment, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            spec = BuildSpec.create("native", repo=repo)
            expected = Path.home() / ".cache/nervix-build/onnxruntime/packages" / spec.fingerprint / "lib"
            self.assertEqual(Path(result.stdout.strip()), expected)

    def test_fetch_reuses_a_pinned_artifact_without_host_compilers(self) -> None:
        build = RuntimeBuild(BuildSpec.create("native", repo=self.repo), self.root / "stage")
        build.package_dir.mkdir(parents=True)
        construct_package(build.package_dir, platform=build.spec.platform)
        build._seal(build.package_dir)
        build.checksum()
        for ci in ("", "true"):
            self.environment["CI"] = ci
            result = subprocess.run(["just", "fetch-onnxruntime"], cwd=self.repo, env=self.environment,
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(str(build.package_dir / "lib"), result.stdout)
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())

    def test_fetch_downloads_public_artifacts_and_reuses_them_across_workspaces_without_credentials(self) -> None:
        spec = BuildSpec.create("native", repo=self.repo)
        producer = RuntimeBuild(spec, self.root / "producer")
        producer.package_dir.mkdir(parents=True)
        construct_package(producer.package_dir, platform=spec.platform)
        producer._seal(producer.package_dir)
        producer.checksum()
        second_repo = fixture_repository(self.root / "second-workspace")
        shutil.copyfile(self.repo / "scripts/onnxruntime/checksums.toml",
                        second_repo / "scripts/onnxruntime/checksums.toml")
        payload = producer.archive().read_bytes()
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                requests.append((self.path, self.headers.get("User-Agent")))
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args: object) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        self.environment["RECIPE_PUBLIC_URL"] = f"http://127.0.0.1:{server.server_port}"
        try:
            for ci in ("", "true"):
                self.environment["CI"] = ci
                stage = self.root / ("ci-download" if ci else "development-download")
                self.environment["NERVIX_ONNXRUNTIME_DIR"] = str(stage)
                for repo in (self.repo, second_repo):
                    for _ in range(2):
                        result = subprocess.run(["just", "fetch-onnxruntime"], cwd=repo, env=self.environment,
                                                capture_output=True, text=True, timeout=30)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        build = RuntimeBuild(BuildSpec.create("native", repo=repo), stage)
                        build.validate_package()
                        self.assertEqual(result.stdout.strip(), str(build.package_dir / "lib"))
        finally:
            server.shutdown()
            worker.join(timeout=10)
            server.server_close()
        self.assertEqual(requests, [(f"/onnxruntime/{spec.object_key}", "nervix-onnxruntime-artifacts")] * 2)
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())

    def test_local_artifact_build_is_reused_without_publication_permissions(self) -> None:
        result = subprocess.run(["just", "build-artifacts", "native", "--jobs", "2"], cwd=self.repo,
                                env=self.environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        build = RuntimeBuild(BuildSpec.create("native", repo=self.repo), self.root / "stage")
        build.validate_package()
        self.assertEqual(build.spec.artifact_checksum, file_digest(build.archive()))
        self.environment["RECIPE_REQUIRE_REUSE"] = "true"
        for recipe in (["fetch-onnxruntime"], ["build-onnxruntime", "native"], ["build-artifacts", "native"]):
            result = subprocess.run(["just", *recipe], cwd=self.repo, env=self.environment,
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(str(build.package_dir / "lib"), result.stdout)
        self.assertEqual(Path(self.environment["RECIPE_COMPILATION"]).read_text(), build.spec.platform + "\n")
        self.assertFalse(self.log.exists())

    def test_force_rebuild_replaces_and_repins_each_platform_without_publication(self) -> None:
        for platform in ("linux/amd64", "linux/arm64", "darwin/arm64"):
            with self.subTest(platform=platform):
                self.environment["RECIPE_GENERATION"] = "first"
                result = subprocess.run(["just", "build-onnxruntime", platform], cwd=self.repo,
                                        env=self.environment, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                build = RuntimeBuild(BuildSpec.create(platform, repo=self.repo), self.root / "stage")
                previous_checksum = build.spec.artifact_checksum
                self.environment["RECIPE_GENERATION"] = "rebuilt"
                result = subprocess.run(["just", "build-artifacts", platform, "--force"], cwd=self.repo,
                                        env=self.environment, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotEqual(build.spec.artifact_checksum, previous_checksum)
                self.assertEqual(build.spec.artifact_checksum, file_digest(build.archive()))
                self.assertEqual((build.package_dir / "lib/libonnxruntime.a").read_bytes(), b"!<arch>\nrebuilt")
                self.environment["RECIPE_REQUIRE_REUSE"] = "true"
                for recipe in ("fetch-onnxruntime", "build-onnxruntime"):
                    result = subprocess.run(["just", recipe, platform], cwd=self.repo,
                                            env=self.environment, capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, 0, result.stderr)
                self.environment.pop("RECIPE_REQUIRE_REUSE")
        self.assertEqual(Path(self.environment["RECIPE_COMPILATION"]).read_text().splitlines(),
                         [platform for platform in ("linux/amd64", "linux/arm64", "darwin/arm64") for _ in range(2)])
        self.assertFalse(self.log.exists())

    def test_pin_records_completed_artifacts_without_compilation_or_publication(self) -> None:
        pin_file = self.repo / "scripts/onnxruntime/checksums.toml"
        for platform in ("linux/amd64", "linux/arm64", "darwin/arm64"):
            with self.subTest(platform=platform):
                previous = tomllib.loads(pin_file.read_text())
                build = RuntimeBuild(BuildSpec.create(platform, repo=self.repo), self.root / "stage")
                construct_package(build.package_dir, platform=platform)
                build._seal(build.package_dir)
                recipe = ["just", "pin-onnxruntime", platform]
                result = subprocess.run(recipe, cwd=self.repo, env=self.environment,
                                        capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                pins = tomllib.loads(pin_file.read_text())
                expected = {"fingerprint": build.spec.fingerprint, "sha256": file_digest(build.archive())}
                self.assertEqual(pins, {**previous, platform: expected})
                self.assertEqual(tomllib.loads(result.stdout), {platform: expected})
                archive = build.archive()
                written = archive.stat().st_mtime_ns
                result = subprocess.run(recipe, cwd=self.repo, env=self.environment,
                                        capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(archive.stat().st_mtime_ns, written)
                self.assertEqual(tomllib.loads(pin_file.read_text()), pins)
                replacement = self.root / "rebuilt-package"
                construct_package(replacement, platform=platform)
                (replacement / "lib/libonnxruntime.a").write_bytes(b"!<arch>\nnew compilation")
                build._seal(replacement)
                shutil.rmtree(build.package_dir)
                replacement.rename(build.package_dir)
                result = subprocess.run(recipe, cwd=self.repo, env=self.environment,
                                        capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotEqual(build.spec.artifact_checksum, expected["sha256"])
                self.assertEqual(build.spec.artifact_checksum, file_digest(build.archive()))
                result = subprocess.run(["just", "fetch-onnxruntime", platform], cwd=self.repo,
                                        env=self.environment, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())
        self.assertFalse(self.log.exists())

    def test_pin_rejects_missing_or_damaged_packages_and_ci_without_changing_pins(self) -> None:
        pin_file = self.repo / "scripts/onnxruntime/checksums.toml"
        original = pin_file.read_bytes()
        recipe = ["just", "pin-onnxruntime"]
        result = subprocess.run(recipe, cwd=self.repo, env=self.environment,
                                capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no completed ONNX Runtime package to pin", result.stderr)
        build = RuntimeBuild(BuildSpec.create("native", repo=self.repo), self.root / "stage")
        construct_package(build.package_dir, platform=build.spec.platform)
        build._seal(build.package_dir)
        archive = build.package_dir / "lib/libonnxruntime.a"
        archive.write_bytes(b"!<arch>\nincomplete compilation")
        result = subprocess.run(recipe, cwd=self.repo, env=self.environment,
                                capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("package file checksum mismatch", result.stderr)
        self.assertEqual(pin_file.read_bytes(), original)
        archive.write_bytes(b"!<arch>\nfixture")
        self.environment["CI"] = "true"
        result = subprocess.run(recipe, cwd=self.repo, env=self.environment,
                                capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("pinning artifact checksums is a manual operation and is disabled in CI", result.stderr)
        self.assertEqual(pin_file.read_bytes(), original)
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())
        self.assertFalse(self.log.exists())

    def test_force_is_reserved_for_explicit_source_builds(self) -> None:
        for recipe in ("fetch-onnxruntime", "pin-onnxruntime", "publish-onnxruntime", "verify-onnxruntime"):
            with self.subTest(recipe=recipe):
                result = subprocess.run(["just", recipe, "native", "--force"], cwd=self.repo,
                                        env=self.environment, capture_output=True, text=True, timeout=30)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("--force is only valid for build", result.stderr)
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())
        self.assertFalse(self.log.exists())

    def test_publication_uploads_the_completed_local_build(self) -> None:
        built = subprocess.run(["just", "build-onnxruntime", "native", "--jobs", "2"], cwd=self.repo,
                               env=self.environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(built.returncode, 0, built.stderr)
        self.environment["RECIPE_ALLOW_PUBLICATION"] = "true"
        result = subprocess.run(["just", "publish-onnxruntime", "native", "--jobs", "2"], cwd=self.repo,
                                env=self.environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        build = RuntimeBuild(BuildSpec.create("native", repo=self.repo), self.root / "stage")
        build.prepare()
        self.assertEqual(Path(self.environment["RECIPE_COMPILATION"]).read_text(), build.spec.platform + "\n")
        self.assertIn("published ONNX Runtime to R2", result.stdout)

    def test_publication_requires_a_completed_package(self) -> None:
        result = subprocess.run(["just", "publish-onnxruntime"], cwd=self.repo, env=self.environment,
                                capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no completed ONNX Runtime package to publish", result.stderr)
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())
        self.assertFalse(self.log.exists())

    def test_manual_publish_uses_the_completed_package_and_is_disabled_in_ci(self) -> None:
        self.environment["RECIPE_ALLOW_PUBLICATION"] = "true"
        build = RuntimeBuild(BuildSpec.create("native", repo=self.repo), self.root / "stage")
        build.package_dir.mkdir(parents=True)
        construct_package(build.package_dir, platform=build.spec.platform)
        build._seal(build.package_dir)
        build.checksum()
        recipe = ["just", "publish-onnxruntime", "native"]
        result = subprocess.run(recipe, cwd=self.repo, env=self.environment,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"published ONNX Runtime to R2: fixture/onnxruntime/{build.spec.object_key}", result.stdout)
        pins = tomllib.loads((self.repo / "scripts/onnxruntime/checksums.toml").read_text())
        self.assertEqual(pins[build.spec.platform]["fingerprint"], build.spec.fingerprint)
        self.assertEqual(pins[build.spec.platform]["sha256"], build.spec.artifact_checksum)
        records = self.log.read_text().splitlines()
        self.assertEqual(json.loads(records[0]), {
            "key": f"onnxruntime/{build.spec.object_key}", "package": str(build.package_dir),
        })
        self.assertFalse(Path(self.environment["RECIPE_COMPILATION"]).exists())
        self.environment["CI"] = "true"
        blocked = subprocess.run(recipe, cwd=self.repo, env=self.environment,
                                 capture_output=True, text=True, timeout=30)
        self.assertNotEqual(blocked.returncode, 0)
        self.assertIn("manual operation and is disabled in CI", blocked.stderr)
        self.assertEqual(self.log.read_text().splitlines(), records)

    def test_ci_missing_version_fails_before_the_product_image_build(self) -> None:
        self.environment["CI"] = "true"
        result = subprocess.run(["just", "docker-build-debian"], cwd=self.repo, env=self.environment,
                                capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unavailable in local storage and R2", result.stderr)
        self.assertIn("just publish-onnxruntime linux/amd64", result.stderr)
        self.assertFalse(self.log.exists())

    @unittest.skipUnless(native_platform().startswith("linux/"), "native CUDA verification requires Linux")
    def test_cuda_verification_prepares_the_cached_package_and_requires_gpu_execution(self) -> None:
        build = RuntimeBuild(BuildSpec.create("native", repo=self.repo), self.root / "stage")
        build.package_dir.mkdir(parents=True)
        construct_package(build.package_dir, platform=build.spec.platform)
        build._seal(build.package_dir)
        build.checksum()
        compiler = self.bin / "clang"
        compiler.write_text("""#!/usr/bin/env python3
import os, pathlib, sys
if sys.argv[1:] == ['--version']:
    print('clang version 22.1.8')
else:
    assert sys.argv[-2] == '-o', sys.argv
    executable = pathlib.Path(sys.argv[-1])
    executable.write_text('''#!/usr/bin/env python3
import os, pathlib, sys
assert sys.argv[-1] == 'cuda', sys.argv
runtime = pathlib.Path(os.environ['LD_LIBRARY_PATH'])
assert (pathlib.Path(sys.argv[0]).parent / 'libonnxruntime_providers_cuda.so').resolve() == runtime / 'libonnxruntime_providers_cuda.so'
pathlib.Path(os.environ['RECIPE_PUBLICATION']).write_text('GPU verification requested')
''')
    executable.chmod(0o755)
""")
        compiler.chmod(0o755)
        archiver = self.bin / "llvm-ar"
        archiver.write_text("#!/bin/sh\nprintf 'LLVM version 22.1.8\\n'\n")
        archiver.chmod(0o755)
        self.environment.update({"CC": str(compiler), "CXX": str(compiler),
                                 "CUDACXX": "unavailable-nvcc", "CUDA_HOME": "/unavailable-cuda",
                                 "CUDNN_HOME": "/unavailable-cudnn"})
        result = subprocess.run(["just", "verify-onnxruntime"], cwd=self.repo,
                                env=self.environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"verified ONNX Runtime {build.spec.configuration['version']} {build.spec.platform}", result.stdout)
        self.assertEqual(self.log.read_text(), "GPU verification requested")
        build.validate_package()


if __name__ == "__main__":
    unittest.main()
