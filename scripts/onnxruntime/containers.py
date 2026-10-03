"""Own the pinned Linux compiler container and its isolated target SDK.

The host selects a package variant and mounts source and output directories. LLVM, target
system packages, and compiler flags come from the container, never the caller's environment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

from scripts.build_onnxruntime import BuildSpec, RuntimeBuild, file_digest, lock, run
from scripts.onnxruntime.bootstrap import Bootstrap, install_linux_sdk
from scripts.onnxruntime.toolchain import BuildError, native_platform


class DockerBuilder:
    def __init__(self, build: RuntimeBuild) -> None:
        self.build = build
        self.configuration = build.spec.identity["builder"]
        image = self.configuration["image"]
        if "@sha256:" not in image:
            raise BuildError("the ONNX Runtime builder base must be pinned by image digest")

    def compile(self, destination: Path, *, force: bool) -> None:
        build = self.build
        host = native_platform()
        if host not in ("linux/amd64", "linux/arm64"):
            host = "linux/amd64"
        if host != build.spec.platform and (host, build.spec.platform) != ("linux/amd64", "linux/arm64"):
            raise BuildError(f"unsupported ONNX Runtime container cross-compilation: {host} to {build.spec.platform}")
        dockerfile = build.spec.repo / "scripts/onnxruntime/Dockerfile"
        identity = {"configuration": self.configuration, "dockerfile": file_digest(dockerfile), "host": host}
        tag = "nervix-onnxruntime-builder:" + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        arguments = ["docker", "buildx", "build", "--load", "--platform", host, "--progress=plain",
                     "--file", str(dockerfile), "--tag", tag]
        for argument, key in (("DEBIAN_IMAGE", "image"), ("LLVM_KEY_SHA256", "llvm_key_sha256")):
            arguments += ["--build-arg", f"{argument}={self.configuration[key]}"]
        with lock(build.stage_root / "locks" / f"builder-{tag.split(':')[1]}.lock"):
            run([*arguments, str(dockerfile.parent)])
            try:
                image_id = subprocess.check_output(["docker", "image", "inspect", tag, "--format", "{{.Id}}"],
                                                   text=True).strip()
            except (OSError, subprocess.CalledProcessError) as error:
                raise BuildError("cannot inspect the completed ONNX Runtime builder image") from error
        build.stage_root.mkdir(parents=True, exist_ok=True)
        environment = {"ONNXRUNTIME_BUILDER_IMAGE": image_id}
        if build.spec.variant == "portable":
            bootstrap = Bootstrap(build.stage_root, build.spec.platform)
            sdk = install_linux_sdk(bootstrap.installer, bootstrap.catalog, build.spec.platform)
            root = Path("/cache") / sdk.relative_to(build.stage_root)
            environment.update(ONNXRUNTIME_SYSROOT=str(root),
                               ONNXRUNTIME_GCC_TOOLCHAIN=str(root / "opt/rh/gcc-toolset-14/root/usr"))
        else:
            environment.update(ONNXRUNTIME_SYSROOT="/", ONNXRUNTIME_GCC_TOOLCHAIN="/usr")
        command = ["docker", "run", "--rm", "--init", "--platform", host,
                   "--user", f"{os.getuid()}:{os.getgid()}",
                   "--mount", f"type=bind,source={build.spec.repo.resolve()},target=/workspace,readonly",
                   "--mount", f"type=bind,source={build.stage_root},target=/cache"]
        for name, value in environment.items():
            command += ["--env", f"{name}={value}"]
        command += [image_id, "python3", "-m", "scripts.onnxruntime.containers",
                    "--platform", build.spec.platform, "--variant", build.spec.variant,
                    "--destination", str(Path("/cache") / destination.relative_to(build.stage_root)),
                    "--jobs", str(build.jobs)]
        if force:
            command.append("--force")
        run(command)
        provenance = destination.parent / "provenance.json"
        try:
            build.provenance = json.loads(provenance.read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise BuildError("the compiler container did not produce build provenance") from error


class ContainerRuntimeBuild(RuntimeBuild):
    def __init__(self, spec: BuildSpec, jobs: int, force: bool) -> None:
        super().__init__(spec, Path("/cache"), jobs)
        self.force = force

    def compile(self, destination: Path) -> None:
        # Only sources and downloaded SDKs survive a requested clean build.
        if self.force and self.build_dir.exists():
            shutil.rmtree(self.build_dir)
        super().compile(destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--variant", choices=("portable", "docker"), required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--jobs", type=int, required=True)
    parser.add_argument("--force", action="store_true")
    arguments = parser.parse_args()
    if not Path("/.dockerenv").is_file():
        parser.error("source compilation must run inside the pinned Docker builder")
    spec = BuildSpec.create(arguments.platform, variant=arguments.variant)
    build = ContainerRuntimeBuild(spec, arguments.jobs, arguments.force)
    bootstrap = Bootstrap(build.stage_root, spec.platform)
    with bootstrap.environment():
        build._build(arguments.destination)
    build.provenance["downloaded_tools"] = bootstrap.installer.receipts
    packages = subprocess.check_output(["dpkg-query", "--show", "--showformat=${binary:Package}\t${Version}\n"],
                                       text=True)
    build.provenance["builder"] = {"image_id": os.environ["ONNXRUNTIME_BUILDER_IMAGE"],
                                    "base": spec.identity["builder"], "variant": spec.variant,
                                    "packages": dict(line.split("\t", 1) for line in packages.splitlines())}
    (arguments.destination.parent / "provenance.json").write_text(json.dumps(build.provenance, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
