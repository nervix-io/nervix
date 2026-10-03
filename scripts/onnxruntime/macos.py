"""Own OSXCross compilation and native qualification of the macOS ARM64 artifact.

The compiler and SDK live in a pinned Linux Docker image. Linux can link the verification
program; only an ARM64 macOS host can execute it and produce the receipt required for pinning.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import struct
import tarfile
import tempfile

from scripts.build_onnxruntime import BuildSpec, RuntimeBuild, atomic_text, file_digest, lock, run
from scripts.onnxruntime.toolchain import BuildError, Compiler, HostToolchain, native_platform, output


def artifact_spec(spec: BuildSpec) -> BuildSpec:
    catalog = json.loads((spec.repo / "scripts/onnxruntime/downloads.json").read_text())
    identity = {**spec.identity, "build_files": dict(spec.identity["build_files"]),
                "macos_builder": {**catalog["builder"], **catalog["macos_sdk"]}}
    for name in ("scripts/onnxruntime/macos.py", "scripts/onnxruntime/Dockerfile.macos"):
        identity["build_files"][name] = file_digest(spec.repo / name)
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return replace(spec, identity=identity, fingerprint=fingerprint)


def qualification_path(build: RuntimeBuild) -> Path:
    return build.stage_root / "macos-qualification" / f"{build.spec.fingerprint}.json"


def require_qualification(build: RuntimeBuild) -> None:
    path = qualification_path(build)
    try:
        receipt = json.loads(path.read_text())
        if (receipt["fingerprint"] != build.spec.fingerprint or
                receipt["archive_sha256"] != file_digest(build.archive()) or
                receipt["manifest_sha256"] != file_digest(build.package_dir / "manifest.json") or
                receipt["platform"] != "darwin/arm64" or receipt["inference"] != "passed"):
            raise BuildError("macOS qualification does not match the completed artifact")
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise BuildError("macOS artifacts require native ARM64 inference qualification before pinning; "
                         "run just qualify-onnxruntime-macos on macOS and pass its receipt with --qualification") from error


def qualify_native(build: RuntimeBuild, archive: Path, receipt: Path) -> None:
    if native_platform() != "darwin/arm64":
        raise BuildError("macOS inference qualification requires an ARM64 macOS host")
    version = platform.mac_ver()[0]
    minimum = build.spec.configuration["macos_deployment_target"]
    if tuple(map(int, version.split("."))) < tuple(map(int, minimum.split("."))):
        raise BuildError(f"macOS inference qualification requires macOS {minimum} or newer")
    with tempfile.TemporaryDirectory() as temporary:
        destination = Path(temporary) / "package"
        destination.mkdir()
        with tarfile.open(archive, "r:gz") as bundle:
            bundle.extractall(destination, filter="data")
        build.validate_package(destination)
        program = destination / "verification/nervix-onnx-smoke"
        validate_program(program)
        # ARM64 macOS requires a code signature. Sign a disposable copy of the verified
        # executable so qualification leaves the candidate archive and manifest unchanged.
        executable = Path(temporary) / "nervix-onnx-smoke"
        shutil.copyfile(program, executable)
        executable.chmod(0o755)
        run(["codesign", "--force", "--sign", "-", str(executable)])
        run([str(executable), str(destination / "verification/output.onnx"),
             build.spec.configuration["version"], "cpu"])
        atomic_text(receipt, json.dumps({
            "fingerprint": build.spec.fingerprint, "archive_sha256": file_digest(archive),
            "manifest_sha256": file_digest(destination / "manifest.json"),
            "platform": "darwin/arm64", "macos_version": version, "inference": "passed",
        }, sort_keys=True, indent=2) + "\n")


def validate_program(program: Path) -> None:
    with program.open("rb") as stream:
        header = stream.read(16)
    if len(header) != 16 or struct.unpack("<4I", header) != (0xFEEDFACF, 0x0100000C, 0, 2):
        raise BuildError("macOS verification program must be an ARM64 Mach-O executable")


class DockerBuilder:
    def __init__(self, build: RuntimeBuild) -> None:
        self.build = build

    def compile(self, destination: Path, *, force: bool) -> None:
        build = self.build
        configuration = build.spec.identity["macos_builder"]
        if any("@sha256:" not in configuration[key] for key in ("image", "sdk_image")):
            raise BuildError("macOS builder and SDK images must be pinned by digest")
        host = native_platform()
        if not host.startswith("linux/"):
            host = "linux/amd64"
        dockerfile = build.spec.repo / "scripts/onnxruntime/Dockerfile.macos"
        identity = {"configuration": configuration, "dockerfile": file_digest(dockerfile), "host": host}
        tag = "nervix-onnxruntime-macos:" + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        arguments = ["docker", "buildx", "build", "--load", "--platform", host, "--progress=plain",
                     "--file", str(dockerfile), "--tag", tag]
        for argument, key in (("DEBIAN_IMAGE", "image"), ("LLVM_KEY_SHA256", "llvm_key_sha256"),
                              ("OSX_CROSS_IMAGE", "sdk_image")):
            arguments += ["--build-arg", f"{argument}={configuration[key]}"]
        with lock(build.stage_root / "locks" / f"macos-builder-{tag.split(':')[1]}.lock"):
            run([*arguments, str(dockerfile.parent)])
            image = output(["docker", "image", "inspect", tag, "--format", "{{.Id}}"])
        build._source()
        verification = destination / "verification"
        verification.mkdir()
        fixtures = ["python3", str(build.spec.repo / "scripts/train_simple_onnx.py")]
        for name in ("output", "alternate-output", "batch-output", "f64-output", "matrix-output",
                     "dynamic-batch-output", "scalar-output"):
            fixtures += [f"--{name}", str(verification / f"{name}.onnx")]
        run(fixtures)
        command = ["docker", "run", "--rm", "--init", "--platform", host,
                   "--user", f"{os.getuid()}:{os.getgid()}",
                   "--mount", f"type=bind,source={build.spec.repo.resolve()},target=/workspace,readonly",
                   "--mount", f"type=bind,source={build.stage_root},target=/cache",
                   "--env", f"MACOSX_DEPLOYMENT_TARGET={build.spec.configuration['macos_deployment_target']}",
                   "--env", f"ONNXRUNTIME_BUILDER_IMAGE={image}", image,
                   "python3", "-m", "scripts.onnxruntime.macos", "--destination",
                   str(Path("/cache") / destination.relative_to(build.stage_root)), "--jobs", str(build.jobs)]
        if force:
            command.append("--force")
        run(command)
        build.provenance = json.loads((destination.parent / "provenance.json").read_text())


class ContainerBuild(RuntimeBuild):
    def _smoke(self, destination: Path, tools: HostToolchain, *, gpu: bool = False) -> None:
        program = destination / "verification/nervix-onnx-smoke"
        run([*tools.cxx.command, "-std=c++17", str(self.spec.repo / "scripts/onnxruntime/smoke.cc"),
             "-I", str(destination / "include"), str(destination / "lib/libonnxruntime.a"),
             "-liconv", "-framework", "Foundation", "-o", str(program)])
        validate_program(program)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--jobs", type=int, required=True)
    parser.add_argument("--force", action="store_true")
    arguments = parser.parse_args()
    if not Path("/.dockerenv").is_file() or os.environ.get("CI") == "true":
        parser.error("macOS source compilation is manual and runs inside the pinned Linux Docker builder")
    spec = artifact_spec(BuildSpec.create("darwin/arm64"))
    build = ContainerBuild(spec, Path("/cache"), arguments.jobs)
    cc = Compiler.discover("CC", "aarch64-apple-darwin23.6-clang", "C")
    cxx = Compiler.discover("CXX", "aarch64-apple-darwin23.6-clang++", "CXX")
    sdk = "/osxcross/SDK/" + spec.identity["macos_builder"]["sdk_directory"]
    # The shared producer invokes CMake directly. Supply the environment normally
    # exported by OSXCross's target-specific CMake wrapper, including for try_compile.
    os.environ.update({"OSXCROSS_HOST": "aarch64-apple-darwin23.6", "OSXCROSS_TARGET_DIR": "/osxcross",
                       "OSXCROSS_TARGET": "darwin23.6", "OSXCROSS_SDK": sdk})
    packages = output(["dpkg-query", "--show", "--showformat=${binary:Package}\t${Version}\n"])
    provenance = {"platform": native_platform(), "cc": asdict(cc), "cxx": asdict(cxx), "sdk": sdk,
                  "builder_image": os.environ["ONNXRUNTIME_BUILDER_IMAGE"],
                  "builder": spec.identity["macos_builder"], "packages": dict(
                      line.split("\t", 1) for line in packages.splitlines()),
                  "cmake": output(["cmake", "--version"]), "ninja": output(["ninja", "--version"])}
    build.tools = HostToolchain(cc, cxx, "/usr/bin/llvm-ar-23",
                               ["/osxcross/bin/aarch64-apple-darwin23.6-libtool", "-static"],
                               sdk, None, None, None, None, None, provenance)
    build.provenance = provenance
    compiler_identity = hashlib.sha256(json.dumps(provenance, sort_keys=True).encode()).hexdigest()
    build.build_dir = build.stage_root / "builds" / "macos" / spec.configuration["revision"] / compiler_identity
    with lock(build.stage_root / "locks" / f"macos-compiler-{compiler_identity}.lock"):
        if arguments.force and build.build_dir.exists():
            shutil.rmtree(build.build_dir)
        build.compile(arguments.destination)
    atomic_text(arguments.destination.parent / "provenance.json", json.dumps(provenance, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
