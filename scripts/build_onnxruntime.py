#!/usr/bin/env python3
"""Fetch pinned ONNX Runtime artifacts for development and CI from local storage or public R2 HTTPS.

Source compilation and publication are explicit maintainer operations. Build inputs own the
artifact identity, and completed packages are installed atomically only after validation.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from typing import Iterator
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.onnxruntime.toolchain import BuildError, HostToolchain, native_platform


ROOT = Path(__file__).resolve().parents[1]
R2_ACCOUNT_ID = "93f53d256da23587269279513de3fc80"
R2_BUCKET = "nervix-artifacts"
R2_PUBLIC_URL = "https://pub-4668ad14d0814ca58c09a124f6bd96f3.r2.dev"
PLATFORMS = {
    "linux/amd64": "x86_64-unknown-linux-gnu",
    "linux/arm64": "aarch64-unknown-linux-gnu",
    "darwin/arm64": "aarch64-apple-darwin",
}


def run(arguments: list[str], **kwargs: object) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(arguments, check=True, stdout=sys.stderr, **kwargs)
    except (OSError, subprocess.CalledProcessError) as error:
        raise BuildError(f"command failed: {arguments[0]}: {error}") from error


def file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_checksums(repo: Path) -> dict:
    path = repo / "scripts/onnxruntime/checksums.toml"
    try:
        pins = tomllib.loads(path.read_text())
    except FileNotFoundError:
        return {}
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise BuildError(f"cannot read pinned artifact checksums: {path}") from error
    for platform, pin in pins.items():
        if platform not in PLATFORMS or not isinstance(pin, dict) or set(pin) != {"fingerprint", "sha256"}:
            raise BuildError(f"invalid artifact checksum entry: {platform}")
        if any(not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
               for value in pin.values()):
            raise BuildError(f"artifact fingerprint and checksum must be SHA-256 hex digests: {platform}")
    return pins


def atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=path.parent) as temporary:
        staged = Path(temporary) / path.name
        staged.write_text(content)
        staged.replace(path)


def copy_shared_library(source: Path, destination: Path) -> Path:
    """Keep the ELF loader's name while storing each vendor library only once."""
    try:
        output = subprocess.check_output(["objdump", "-p", str(source)], text=True)
    except (OSError, subprocess.CalledProcessError) as error:
        raise BuildError(f"cannot inspect shared library: {source}") from error
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == "SONAME":
            if Path(parts[1]).name != parts[1]:
                raise BuildError(f"shared library SONAME is not a filename: {source}")
            target = destination / parts[1]
            if not target.exists():
                shutil.copyfile(source.resolve(), target)
            return target
    raise BuildError(f"shared library has no SONAME: {source}")


def shared_dependencies(source: Path, platform: str) -> list[str]:
    """Inspect ELF dependencies without executing the target's dynamic loader."""
    try:
        with source.open("rb") as stream:
            header = stream.read(20)
        machine = 62 if platform == "linux/amd64" else 183
        if header[:6] != b"\x7fELF\x02\x01" or int.from_bytes(header[18:20], "little") != machine:
            raise BuildError(f"shared library ELF architecture does not match {platform}: {source}")
        output = subprocess.check_output(["objdump", "-p", str(source)], text=True)
    except (OSError, subprocess.CalledProcessError) as error:
        raise BuildError(f"cannot inspect shared library dependencies: {source}") from error
    needed = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == "NEEDED":
            if Path(parts[1]).name != parts[1]:
                raise BuildError(f"shared library dependency is not a filename: {source}")
            needed.append(parts[1])
    return needed


@contextmanager
def lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


@dataclass(frozen=True)
class BuildSpec:
    repo: Path
    platform: str
    configuration: dict
    identity: dict
    fingerprint: str

    @classmethod
    def create(cls, platform: str = "native", repo: Path = ROOT) -> BuildSpec:
        if platform == "native":
            platform = native_platform()
        platform = {value: key for key, value in PLATFORMS.items()}.get(platform, platform)
        platform = {"linux/aarch64": "linux/arm64"}.get(platform, platform)
        if platform not in PLATFORMS:
            raise BuildError(f"unsupported ONNX Runtime platform: {platform}")
        configuration = tomllib.loads((repo / "scripts/onnxruntime/manifest.toml").read_text())
        identity = {
            "configuration": configuration,
            "platform": platform,
            "build_files": {
                name: file_digest(repo / name)
                for name in [
                    "scripts/build_onnxruntime.py", "scripts/onnxruntime/toolchain.py",
                    "scripts/onnxruntime/aggregate.cmake", "scripts/onnxruntime/smoke.cc",
                    "scripts/train_simple_onnx.py",
                ]
            },
        }
        fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        return cls(repo, platform, configuration, identity, fingerprint)

    @property
    def cuda_enabled(self) -> bool:
        return self.platform.startswith("linux/")

    @property
    def object_key(self) -> str:
        return f"{self.configuration['version']}/{self.platform}/{self.fingerprint}.tar.gz"

    @property
    def artifact_checksum(self) -> str | None:
        pin = read_checksums(self.repo).get(self.platform)
        if pin is None or pin["fingerprint"] != self.fingerprint:
            return None
        return pin["sha256"]

    def pin_checksum(self, checksum: str) -> None:
        if re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
            raise BuildError("the artifact checksum must be a SHA-256 hex digest")
        with lock(self.repo / ".nervix-deps/onnxruntime-checksums.lock"):
            pins = read_checksums(self.repo)
            pins[self.platform] = {"fingerprint": self.fingerprint, "sha256": checksum}
            content = "# SHA-256 pins for manually published ONNX Runtime archives.\n"
            content += "# These pins are excluded from the build identity.\n"
            for platform, pin in sorted(pins.items()):
                content += f'\n[{json.dumps(platform)}]\n'
                content += f'fingerprint = "{pin["fingerprint"]}"\nsha256 = "{pin["sha256"]}"\n'
            atomic_text(self.repo / "scripts/onnxruntime/checksums.toml", content)


class R2Cache:
    """Download published artifacts over public HTTPS without credentials or the S3 SDK."""

    def restore(self, spec: BuildSpec, destination: Path) -> str | None:
        expected = spec.artifact_checksum
        if expected is None:
            raise BuildError(f"no pinned artifact checksum for {spec.platform} {spec.fingerprint}; "
                             f"publish it with just publish-onnxruntime {spec.platform}")
        url = f"{R2_PUBLIC_URL}/onnxruntime/{spec.object_key}"
        try:
            body = urlopen(Request(url, headers={"User-Agent": "nervix-onnxruntime-artifacts"}), timeout=120)
        except HTTPError as error:
            error.close()
            if error.code == 404:
                return None
            raise BuildError(f"R2 artifact download failed: {url}: HTTP {error.code} {error.reason}") from error
        except (URLError, OSError) as error:
            raise BuildError(f"R2 artifact download failed: {url}: {error}") from error
        try:
            with body, tempfile.TemporaryFile() as stream:
                digest = hashlib.sha256()
                while chunk := body.read(1024 * 1024):
                    digest.update(chunk)
                    stream.write(chunk)
                if digest.hexdigest() != expected:
                    raise BuildError(f"R2 package does not match its pinned SHA-256 checksum: expected {expected}, "
                                     f"received {digest.hexdigest()}")
                stream.seek(0)
                with tarfile.open(fileobj=stream, mode="r:gz") as archive:
                    archive.extractall(destination, filter="data")
        except (OSError, tarfile.TarError, URLError) as error:
            raise BuildError(f"R2 package download or extraction failed: {error}") from error
        return expected


class R2Publisher:
    """Authenticate local maintainer uploads to the fixed R2 bucket."""

    def __init__(self, client: object, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    @classmethod
    def configured(cls) -> R2Publisher:
        names = ["R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"]
        values = {name: os.environ.get(name) for name in names}
        missing = [name for name, value in values.items() if not value]
        if missing:
            raise BuildError("R2 publication credentials require " + ", ".join(missing))
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3", endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            region_name="auto", aws_access_key_id=values["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=values["R2_SECRET_ACCESS_KEY"],
            config=Config(retries={"mode": "standard", "total_max_attempts": 4},
                          connect_timeout=15, read_timeout=120),
        )
        return cls(client, R2_BUCKET)

    def key(self, spec: BuildSpec) -> str:
        return f"onnxruntime/{spec.object_key}"

    def publish(self, build: RuntimeBuild) -> None:
        from botocore.exceptions import BotoCoreError, ClientError
        from boto3.exceptions import S3UploadFailedError
        from scripts.onnxruntime.upload import upload_archive

        archive = build.archive()
        checksum = file_digest(archive)
        if checksum != build.spec.artifact_checksum:
            raise BuildError("publication requires the artifact's pinned checksum; use just publish-onnxruntime")
        try:
            upload_archive(self.client, archive, self.bucket, self.key(build.spec), checksum, build.spec.platform)
        except (BotoCoreError, ClientError, S3UploadFailedError) as error:
            raise BuildError(f"R2 publish failed: {error}") from error


class RuntimeBuild:
    """Manage runtime artifacts; only build_source downloads sources or invokes the compiler."""

    def __init__(self, spec: BuildSpec, stage_root: Path, jobs: int = 4) -> None:
        self.spec = spec
        self.stage_root = stage_root.resolve()
        self.jobs = jobs
        self.package_dir = self.stage_root / "packages" / spec.fingerprint
        self.build_dir = self.stage_root / "builds" / spec.fingerprint
        self.source_dir = self.stage_root / "sources" / spec.configuration["revision"]
        self.tools: HostToolchain | None = None
        self.provenance: dict = {}

    def validate_package(self, destination: Path | None = None) -> None:
        destination = self.package_dir if destination is None else destination
        try:
            manifest = json.loads((destination / "manifest.json").read_text())
            if manifest["identity"] != self.spec.identity:
                raise BuildError("package does not match the required build inputs")
            if not isinstance(manifest["toolchain"], dict):
                raise BuildError("package compiler provenance must be an object")
            if not isinstance(manifest["files"], dict):
                raise BuildError("package checksum manifest must contain a file map")
            required = {"lib/libonnxruntime.a", "include/onnxruntime_c_api.h", "LICENSE", "ThirdPartyNotices.txt"}
            if self.spec.cuda_enabled:
                required |= {"runtime/lib/libonnxruntime_providers_shared.so",
                             "runtime/lib/libonnxruntime_providers_cuda.so",
                             "runtime/lib/libcudnn_graph.so.9"}
                if not any(name.startswith("runtime/lib/libnvrtc-builtins.so.13.")
                           for name in manifest["files"]):
                    raise BuildError("package is missing CUDA 13 NVRTC builtins")
            if not required <= manifest["files"].keys():
                raise BuildError("package is missing required artifacts")
            actual = {path.relative_to(destination).as_posix()
                      for path in destination.rglob("*") if path.is_file()}
            if actual != set(manifest["files"]) | {"manifest.json"}:
                raise BuildError("package contains files outside its checksum manifest")
            for name, digest in manifest["files"].items():
                path = destination / name
                if Path(name).is_absolute() or not path.resolve().is_relative_to(destination.resolve()):
                    raise BuildError(f"package path escapes its root: {name}")
                if not path.is_file() or file_digest(path) != digest:
                    raise BuildError(f"package file checksum mismatch: {name}")
            with (destination / "lib/libonnxruntime.a").open("rb") as archive:
                if archive.read(8) != b"!<arch>\n":
                    raise BuildError("libonnxruntime.a is not a static archive")
        except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
            raise BuildError(f"invalid ONNX Runtime package: {error}") from error

    def _seal(self, destination: Path) -> None:
        manifest = {
            "identity": self.spec.identity,
            "toolchain": self.provenance,
            "files": {path.relative_to(destination).as_posix(): file_digest(path)
                      for path in sorted(destination.rglob("*")) if path.is_file()},
        }
        (destination / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
        self.validate_package(destination)

    def _local_package(self, *, require_checksum: bool) -> bool:
        if not self.package_dir.exists():
            return False
        try:
            self.validate_package()
            if require_checksum:
                self._validate_verification()
            return True
        except BuildError as error:
            print(f"discarding incomplete local package: {error}", file=sys.stderr)
            shutil.rmtree(self.package_dir)
            return False

    def _unavailable(self, reason: str = "") -> BuildError:
        return BuildError(f"ONNX Runtime {self.spec.configuration['version']} "
                          f"{self.spec.platform} is unavailable in local storage and R2; {reason}"
                          "normal development requires a published R2 artifact; "
                          f"ask a maintainer to publish it with just publish-onnxruntime {self.spec.platform}")

    def prepare(self, remote: R2Cache | None = None) -> Path:
        """Reuse a checksum-verified package or fetch it from R2; a miss always fails."""
        with lock(self.stage_root / "locks" / f"{self.spec.fingerprint}.lock"):
            if self.spec.artifact_checksum is None:
                raise self._unavailable("no pinned artifact checksum is available for this build; ")
            if self._local_package(require_checksum=True):
                return self.package_dir / "lib"
            remote = remote if remote is not None else R2Cache()
            self.package_dir.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(dir=self.package_dir.parent) as temporary:
                destination = Path(temporary) / "package"
                destination.mkdir()
                restored = remote.restore(self.spec, destination)
                if restored is None:
                    raise self._unavailable()
                self.validate_package(destination)
                self._record_verification(destination, restored)
                destination.rename(self.package_dir)
            return self.package_dir / "lib"

    def build_source(self) -> Path:
        """Maintainer operation: explicitly compile a missing package on the host."""
        if os.environ.get("CI") == "true":
            raise BuildError("ONNX Runtime source compilation is a manual operation and is disabled in CI")
        with lock(self.stage_root / "locks" / f"{self.spec.fingerprint}.lock"):
            if self._local_package(require_checksum=False):
                return self.package_dir / "lib"
            self.package_dir.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(dir=self.package_dir.parent) as temporary:
                destination = Path(temporary) / "package"
                destination.mkdir()
                self._build(destination)
                self._seal(destination)
                (self.stage_root / "verified" / f"{self.spec.fingerprint}.json").unlink(missing_ok=True)
                destination.rename(self.package_dir)
            return self.package_dir / "lib"

    def publish(self, remote: R2Publisher) -> None:
        if os.environ.get("CI") == "true":
            raise BuildError("publishing ONNX Runtime is a manual operation and is disabled in CI")
        with lock(self.stage_root / "locks" / f"{self.spec.fingerprint}.lock"):
            self.validate_package()
            remote.publish(self)

    def verify(self) -> None:
        self.validate_package()
        self._validate_verification()
        if self.spec.cuda_enabled and self.spec.platform != native_platform():
            raise BuildError("CUDA GPU verification requires a matching Linux host and NVIDIA GPU; "
                             f"run just verify-onnxruntime {self.spec.platform} on that platform")
        # Verification links a small host program; the packaged CUDA libraries supply the GPU runtime.
        # Restored packages therefore need neither NVCC nor the producing host's SDKs.
        tools = HostToolchain.discover(self.spec.platform, require_cuda=False)
        self._smoke(self.package_dir, tools, gpu=self.spec.cuda_enabled)

    def write_archive(self, filename: str) -> None:
        self.validate_package()

        def normalize(member: tarfile.TarInfo) -> tarfile.TarInfo:
            member.uid = member.gid = member.mtime = 0
            member.uname = member.gname = ""
            member.mode = 0o755 if member.isdir() else 0o644
            member.pax_headers = {}
            return member

        with Path(filename).open("wb") as stream:
            with gzip.GzipFile(filename="", fileobj=stream, mode="wb", compresslevel=3, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                    for path in sorted(self.package_dir.rglob("*")):
                        archive.add(path, arcname=path.relative_to(self.package_dir), recursive=False, filter=normalize)

    def archive(self) -> Path:
        """Reuse the exact upload bytes while the validated package's manifest is unchanged."""
        self.validate_package()
        archive = self.stage_root / "archives" / f"{self.spec.fingerprint}.tar.gz"
        receipt = archive.with_suffix(".json")
        manifest_checksum = file_digest(self.package_dir / "manifest.json")
        try:
            metadata = json.loads(receipt.read_text())
            if metadata["manifest_sha256"] == manifest_checksum and file_digest(archive) == metadata["sha256"]:
                return archive
        except (OSError, KeyError, TypeError, json.JSONDecodeError):
            pass
        archive.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=archive.parent) as temporary:
            staged = Path(temporary) / "artifact.tar.gz"
            self.write_archive(str(staged))
            checksum = file_digest(staged)
            staged.replace(archive)
        atomic_text(receipt, json.dumps({"manifest_sha256": manifest_checksum, "sha256": checksum}, sort_keys=True) + "\n")
        return archive

    def checksum(self) -> str:
        if os.environ.get("CI") == "true":
            raise BuildError("pinning artifact checksums is a manual operation and is disabled in CI")
        with lock(self.stage_root / "locks" / f"{self.spec.fingerprint}.lock"):
            checksum = file_digest(self.archive())
            self.spec.pin_checksum(checksum)
            self._record_verification(self.package_dir, checksum)
            return checksum

    def _record_verification(self, destination: Path, checksum: str) -> None:
        receipt = self.stage_root / "verified" / f"{self.spec.fingerprint}.json"
        atomic_text(receipt, json.dumps({
            "sha256": checksum, "manifest_sha256": file_digest(destination / "manifest.json"),
        }, sort_keys=True) + "\n")

    def _validate_verification(self) -> None:
        receipt = self.stage_root / "verified" / f"{self.spec.fingerprint}.json"
        expected = self.spec.artifact_checksum
        if expected is None:
            raise BuildError("the package has no pinned artifact checksum")
        if not receipt.exists():
            if file_digest(self.archive()) != expected:
                raise BuildError("the local package does not match its pinned artifact checksum")
            self._record_verification(self.package_dir, expected)
        try:
            metadata = json.loads(receipt.read_text())
            if metadata["sha256"] != expected or metadata["manifest_sha256"] != file_digest(self.package_dir / "manifest.json"):
                raise BuildError("cached package verification does not match the pinned artifact checksum or manifest")
        except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
            raise BuildError(f"invalid artifact verification receipt: {receipt}") from error

    def _source(self) -> None:
        with lock(self.stage_root / "locks/source.lock"):
            if self.source_dir.exists():
                try:
                    revision = subprocess.check_output(
                        ["git", "-C", str(self.source_dir), "rev-parse", "HEAD"], text=True
                    ).strip()
                    changes = subprocess.check_output(
                        ["git", "-C", str(self.source_dir), "status", "--porcelain", "--untracked-files=all"], text=True
                    )
                except (OSError, subprocess.CalledProcessError) as error:
                    raise BuildError(f"invalid source checkout: {self.source_dir}") from error
                if revision != self.spec.configuration["revision"]:
                    raise BuildError(f"source revision mismatch in {self.source_dir}")
                if changes:
                    raise BuildError(f"source checkout contains changes: {self.source_dir}")
                return
            self.source_dir.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(dir=self.source_dir.parent) as temporary:
                checkout = Path(temporary) / "source"
                run(["git", "init", str(checkout)])
                run(["git", "-C", str(checkout), "remote", "add", "origin", self.spec.configuration["repository"]])
                run(["git", "-C", str(checkout), "fetch", "--depth=1", "origin", self.spec.configuration["revision"]])
                run(["git", "-C", str(checkout), "checkout", "--detach", "FETCH_HEAD"])
                if (checkout / "VERSION_NUMBER").read_text().strip() != self.spec.configuration["version"]:
                    raise BuildError("upstream source does not match the pinned runtime version")
                checkout.rename(self.source_dir)

    def _build(self, destination: Path) -> None:
        self.tools = HostToolchain.discover(self.spec.platform)
        self.provenance = self.tools.metadata
        toolchain_identity = hashlib.sha256(json.dumps(self.provenance, sort_keys=True).encode()).hexdigest()
        compiler_identity = hashlib.sha256(json.dumps({
            "revision": self.spec.configuration["revision"],
            "platform": self.spec.platform,
        }, sort_keys=True).encode()).hexdigest()
        # CMake and Ninja track compilation definitions, source dependencies, and commands.
        # Package validation changes can reuse unchanged compiler outputs under their own lock.
        self.build_dir = (self.stage_root / "builds" / compiler_identity / toolchain_identity).resolve()
        with lock(self.stage_root / "locks" / f"compiler-{compiler_identity}-{toolchain_identity}.lock"):
            self._source()
            self.compile(destination)

    def compile(self, destination: Path) -> None:
        tools = self.tools
        if tools is None:
            raise BuildError("resolve the host toolchain before compilation")
        self.build_dir.mkdir(parents=True, exist_ok=True)
        definitions = [
            "CMAKE_BUILD_TYPE=Release", "CMAKE_POSITION_INDEPENDENT_CODE=ON",
            "onnxruntime_BUILD_SHARED_LIB=OFF", "onnxruntime_BUILD_UNIT_TESTS=OFF",
            "onnxruntime_ENABLE_LTO=OFF",
            f"CMAKE_C_FLAGS={os.environ.get('CPPFLAGS', '')} {os.environ.get('CFLAGS', '')}",
            f"CMAKE_CXX_FLAGS={os.environ.get('CPPFLAGS', '')} {os.environ.get('CXXFLAGS', '')}",
            f"CMAKE_PROJECT_TOP_LEVEL_INCLUDES={self.spec.repo}/scripts/onnxruntime/aggregate.cmake",
            f"CMAKE_AR={tools.archiver}",
            *tools.cc.definitions("C"), *tools.cxx.definitions("CXX"),
        ]
        if tools.sdk:
            definitions += [f"CMAKE_OSX_DEPLOYMENT_TARGET={self.spec.configuration['macos_deployment_target']}",
                            "CMAKE_OSX_ARCHITECTURES=arm64", f"CMAKE_OSX_SYSROOT={tools.sdk}"]
        if self.spec.cuda_enabled:
            if tools.cuda is None or tools.cuda_host is None:
                raise BuildError("resolve the host CUDA toolkit before compilation")
            definitions += ["onnxruntime_USE_CUDA=ON", f"onnxruntime_CUDA_HOME={tools.cuda_home}",
                            f"onnxruntime_CUDNN_HOME={tools.cudnn_home}",
                            f"CMAKE_CUDA_ARCHITECTURES={self.spec.configuration['cuda_architectures']}",
                            f"CMAKE_CUDA_HOST_COMPILER={tools.cuda_host.executable}",
                            "onnxruntime_USE_FPA_INTB_GEMM=OFF", *tools.cuda.definitions("CUDA")]
            # CMake's generated NVCC rules do not carry COMPILER_ARG1 into compilation.
            cuda_flags = [*tools.cuda.arguments, *shlex.split(os.environ.get("CUDAFLAGS", ""))]
            cuda_flags += [f"-Xcompiler={argument}" for argument in tools.cuda_host.arguments]
            if tools.cross:
                # CMake's initial NVCC compiler probe links before target linker selection applies.
                cuda_flags += ["-Xcompiler=-fuse-ld=lld", "-Xcompiler=-Qunused-arguments"]
            definitions.append(f"CMAKE_CUDA_FLAGS={shlex.join(cuda_flags)}")
            if tools.cross:
                definitions += tools.cross.definitions(tools.cuda_home, tools.cudnn_home, tools.cuda_host)
                definitions += tools.cc.definitions("ASM")
                definitions.append(f"CMAKE_ASM_FLAGS={os.environ.get('ASMFLAGS', '')}")
        # Match upstream's release build wrapper: vendor warnings remain visible as warnings.
        run(["cmake", "--compile-no-warning-as-error", "-S", str(self.source_dir / "cmake"), "-B", str(self.build_dir),
             "-G", "Ninja", *[f"-D{definition}" for definition in definitions]])
        if self.spec.cuda_enabled:
            self._cuda_host_sources()
        run(["cmake", "--build", str(self.build_dir), "--target", "nervix_static_artifacts", "--parallel", str(self.jobs)])
        (destination / "lib").mkdir()
        (destination / "include").mkdir()
        archive = destination / "lib/libonnxruntime.a"
        libraries = (self.build_dir / "nervix-archives.txt").read_text().splitlines()
        if self.spec.platform.startswith("darwin/"):
            run([*tools.aggregate, "-o", str(archive), *libraries])
        else:
            run([*tools.aggregate, str(archive), *libraries])
        for header in (self.source_dir / "include/onnxruntime/core/session").glob("*.h"):
            shutil.copy2(header, destination / "include" / header.name)
        for name in ("LICENSE", "ThirdPartyNotices.txt"):
            shutil.copy2(self.source_dir / name, destination / name)
        if self.spec.cuda_enabled:
            self._cuda_runtime(destination, tools)
        self._smoke(destination, tools)

    def _cuda_host_sources(self) -> None:
        # Diagnose host LLVM errors before spending time on the complete multi-architecture kernels.
        try:
            targets = subprocess.check_output(
                ["ninja", "-C", str(self.build_dir), "-t", "targets", "all"], text=True,
            )
        except (OSError, subprocess.CalledProcessError) as error:
            raise BuildError("cannot discover CUDA provider host compilation targets") from error
        host_objects = []
        for line in targets.splitlines():
            name, separator, rule = line.partition(": ")
            if separator and name.startswith("CMakeFiles/onnxruntime_providers_cuda.dir/") and rule.startswith("CXX_COMPILER"):
                host_objects.append(name)
        if not host_objects:
            raise BuildError("the CUDA provider has no host compilation targets")
        run(["cmake", "--build", str(self.build_dir), "--target", *host_objects,
             "--parallel", str(self.jobs)])

    def _cuda_runtime(self, destination: Path, tools: HostToolchain) -> None:
        runtime = destination / "runtime/lib"
        runtime.mkdir(parents=True)
        for name in ("libonnxruntime_providers_shared.so", "libonnxruntime_providers_cuda.so"):
            shutil.copy2(self.build_dir / name, runtime / name)
        search = [runtime]
        architecture = "x86_64" if self.spec.platform == "linux/amd64" else "aarch64"
        cuda_target = "x86_64" if self.spec.platform == "linux/amd64" else "sbsa"
        library_directories = ("lib", "lib64", f"lib/{architecture}-linux-gnu",
                               f"targets/{cuda_target}-linux/lib")
        sdk_directories = []
        for root in (tools.cudnn_home, tools.cuda_home):
            if root is None:
                raise BuildError("CUDA runtime packaging requires host toolkit paths")
            if tools.cross and root == tools.cuda_home:
                directories = [root / "targets/sbsa-linux" / name for name in ("lib", "lib64")]
            else:
                directories = [root / relative for relative in library_directories]
            sdk_directories.append(directories)
            search += directories
        # cuDNN components and NVRTC builtins are opened dynamically and have no NEEDED entry.
        for directories, pattern in zip(sdk_directories, ("libcudnn*.so*", "libnvrtc*.so*")):
            for directory in directories:
                for source in directory.glob(pattern):
                    copy_shared_library(source, runtime)
        # The dynamic dependency closure contains CUDA user-space libraries, never the GPU driver.
        pending = list(runtime.iterdir())
        while pending:
            library = pending.pop()
            for name in shared_dependencies(library, self.spec.platform):
                if name.startswith(("libcuda.so", "libnvidia")):
                    continue
                if not name.startswith(("libcu", "libnv", "libonnxruntime")):
                    continue
                target = runtime / name
                if not target.exists():
                    source = next((directory / name for directory in search if (directory / name).is_file()), None)
                    if source is None:
                        raise BuildError(f"CUDA runtime dependency is unavailable in target SDKs: {name}")
                    shutil.copyfile(source.resolve(), target)
                    pending.append(target)
        notices = destination / "runtime/licenses"
        notices.mkdir()
        for directory in Path("/usr/share/doc").iterdir():
            if directory.name.startswith(("cuda-", "libcudnn", "libcublas", "libcufft", "libcurand",
                                          "libcusolver", "libcusparse", "libnvjitlink")):
                source = directory / "copyright"
                if source.is_file():
                    shutil.copyfile(source, notices / f"{directory.name}-copyright")
        license_roots = [tools.cuda_home, tools.cudnn_home]
        if tools.cross:
            license_roots.append(tools.cuda_home / "targets/sbsa-linux")
        for root in license_roots:
            if root is not None:
                for name in ("LICENSE", "LICENSE.txt", "EULA.txt"):
                    source = root / name
                    if source.is_file():
                        shutil.copyfile(source, notices / f"{root.name}-{name}")

    def _smoke(self, destination: Path, tools: HostToolchain, *, gpu: bool = False) -> None:
        if gpu and tools.cross:
            raise BuildError("CUDA GPU verification requires a matching host; QEMU checks CPU inference and provider loading")
        fixture = self.build_dir / "fixtures"
        fixture.mkdir(parents=True, exist_ok=True)
        command = ["python3", str(self.spec.repo / "scripts/train_simple_onnx.py")]
        for option in ("output", "alternate-output", "batch-output", "f64-output", "matrix-output", "dynamic-batch-output", "scalar-output"):
            command += [f"--{option}", str(fixture / f"{option}.onnx")]
        if gpu:
            command += ["--convolution-output", str(fixture / "convolution-output.onnx")]
        run(command)
        smoke_dir = self.build_dir / "smoke"
        smoke_dir.mkdir(exist_ok=True)
        executable = smoke_dir / "nervix-onnx-smoke"
        mode = "cpu"
        environment = os.environ.copy()
        if self.spec.cuda_enabled:
            mode = "cuda" if gpu else "cuda-load"
            runtime = destination / "runtime/lib"
            environment["LD_LIBRARY_PATH"] = str(runtime)
            # A statically linked core discovers its providers beside the executable.
            for name in ("libonnxruntime_providers_shared.so", "libonnxruntime_providers_cuda.so"):
                link = smoke_dir / name
                link.unlink(missing_ok=True)
                link.symlink_to(runtime / name)
        libraries = ["-ldl", "-lpthread", "-lm", "-lrt", "-latomic"]
        if self.spec.platform.startswith("darwin/"):
            libraries = ["-liconv", "-framework", "Foundation", "-isysroot", str(tools.sdk),
                         f"-mmacosx-version-min={self.spec.configuration['macos_deployment_target']}"]
        flags = shlex.split(os.environ.get("CPPFLAGS", "")) + shlex.split(os.environ.get("CXXFLAGS", ""))
        link_flags = shlex.split(os.environ.get("LDFLAGS", ""))
        if tools.cross:
            link_flags.append("-fuse-ld=lld")
        run([*tools.cxx.command, *flags, "-std=c++17", str(self.spec.repo / "scripts/onnxruntime/smoke.cc"),
             "-I", str(destination / "include"), str(destination / "lib/libonnxruntime.a"),
             *libraries, *link_flags, "-o", str(executable)])
        smoke = [str(executable), str(fixture / "output.onnx"), self.spec.configuration["version"], mode]
        if tools.cross:
            # QEMU redirects guest loader paths to the target sysroot. Keep its host process's
            # library environment separate from the target CUDA and C++ runtime libraries.
            sysroot = tools.cross.runtime_root
            directories = [destination / "runtime/lib", sysroot / "lib", sysroot / "usr/lib",
                           sysroot / "lib/aarch64-linux-gnu", sysroot / "usr/lib/aarch64-linux-gnu"]
            prefix = [tools.cross.emulator, "-L", str(sysroot), "-E",
                      "LD_LIBRARY_PATH=" + os.pathsep.join(str(path) for path in directories)]
            smoke = [*prefix, *smoke]
            if "LD_LIBRARY_PATH" in os.environ:
                environment["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"]
            else:
                environment.pop("LD_LIBRARY_PATH", None)
        run(smoke, env=environment)
        if gpu:
            run([str(executable), str(fixture / "convolution-output.onnx"),
                 self.spec.configuration["version"], mode], env=environment)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["fetch", "build-source", "path", "publish", "verify", "checksum"],
                        help="fetch published artifacts for development; build-source, checksum, and publish are maintainer operations")
    parser.add_argument("--platform", default="native")
    parser.add_argument("--stage", type=Path, default=Path(os.environ.get("NERVIX_ONNXRUNTIME_DIR", ROOT / ".nervix-deps/onnxruntime")))
    parser.add_argument("--jobs", type=int, default=min(os.cpu_count() or 1, 4), help="parallel jobs for manual source builds")
    parser.add_argument("--unchecked", action="store_true", help="calculate the library path before preparation")
    arguments = parser.parse_args()
    if arguments.jobs < 1:
        parser.error("--jobs must be positive")
    try:
        spec = BuildSpec.create(arguments.platform)
        build = RuntimeBuild(spec, arguments.stage, arguments.jobs)
        if arguments.operation == "fetch":
            print(build.prepare())
        elif arguments.operation == "build-source":
            print(build.build_source())
        elif arguments.operation == "path":
            if not arguments.unchecked:
                build.validate_package()
            print(build.package_dir / "lib")
        elif arguments.operation == "verify":
            build.verify()
            print(f"verified ONNX Runtime {spec.configuration['version']} {spec.platform}")
        elif arguments.operation == "checksum":
            print(f"pinned SHA-256 {build.checksum()} for {spec.platform} in scripts/onnxruntime/checksums.toml")
        else:
            if os.environ.get("CI") == "true":
                raise BuildError("publishing ONNX Runtime is a manual operation and is disabled in CI")
            remote = R2Publisher.configured()
            build.publish(remote)
            print(f"published ONNX Runtime to R2: {remote.bucket}/{remote.key(spec)}")
    except (BuildError, OSError) as error:
        print(f"ONNX Runtime preparation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
