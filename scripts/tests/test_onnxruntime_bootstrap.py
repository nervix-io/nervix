from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from scripts.build_onnxruntime import BuildSpec, RuntimeBuild
from scripts.onnxruntime.artifacts import ManagedRuntimeBuild
from scripts.onnxruntime.bootstrap import Bootstrap, Installer
from scripts.onnxruntime.containers import ContainerRuntimeBuild, DockerBuilder
from scripts.onnxruntime.toolchain import BuildError, Compiler, HostToolchain
from scripts.tests.test_build_onnxruntime import construct_package, fixture_repository


def tool_archive(files: dict[str, bytes], mode: str = "w:gz") -> bytes:
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode=mode) as archive:
        for name, contents in files.items():
            member = tarfile.TarInfo(name)
            member.size = len(contents)
            member.mode = 0o755
            archive.addfile(member, io.BytesIO(contents))
    return data.getvalue()


def response(payload: bytes) -> io.BytesIO:
    body = io.BytesIO(payload)
    body.headers = {"Content-Length": str(len(payload))}
    return body


CUDA_SDK_FILES = {
    "cuda_nvcc": ("bin/nvcc",),
    "libnvvm": ("nvvm/bin/cicc", "nvvm/libdevice/libdevice.10.bc"),
    "cuda_crt": ("include/crt/host_config.h",),
    "cuda_cudart": (
        "include/cuda.h", "include/cuda_runtime.h", "include/cuda_fp16.h",
        "include/cuda_bf16.h", "include/cuda_fp8.h", "include/cuda_fp4.h",
        "lib/libcudart.so", "lib/stubs/libcuda.so",
    ),
    "cuda_cccl": ("include/cccl/cuda/std/utility",),
    "cuda_nvrtc": ("include/nvrtc.h", "lib/libnvrtc.so", "lib/libnvrtc-builtins.so"),
    "libcublas": ("include/cublas_v2.h", "include/cublasLt.h", "lib/libcublas.so", "lib/libcublasLt.so"),
    "libcurand": ("include/curand.h", "include/curand_kernel.h", "lib/libcurand.so"),
    "libcufft": ("include/cufft.h", "include/cufftXt.h", "lib/libcufft.so"),
    "libcusparse": ("include/cusparse.h", "lib/libcusparse.so"),
    "libnvjitlink": ("include/nvJitLink.h", "lib/libnvJitLink.so"),
    "cuda_culibos": ("lib/libculibos.a",),
}


class InstallerTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.installer = Installer(self.root / "tools")
        self.payload = tool_archive({"release/bin/compiler": b"fixture compiler"})
        self.asset = {"url": "https://tools.example/compiler.tar.gz",
                      "sha256": hashlib.sha256(self.payload).hexdigest(), "strip_root": True}

    def test_verified_installation_is_shared_and_reused_without_downloads(self) -> None:
        with patch("scripts.onnxruntime.bootstrap.urlopen", return_value=response(self.payload)) as download:
            installed = self.installer.install("compiler", [self.asset], ["bin/compiler"])
        self.assertEqual((installed / "bin/compiler").read_bytes(), b"fixture compiler")
        self.assertTrue((installed / "installation.json").is_file())
        self.assertTrue(download.return_value.closed)
        other = Installer(self.installer.root)
        with patch("scripts.onnxruntime.bootstrap.urlopen", side_effect=AssertionError("unnecessary download")):
            self.assertEqual(other.install("compiler", [self.asset], ["bin/compiler"]), installed)

    def test_missing_tool_is_reinstalled_using_verified_download_bytes(self) -> None:
        with patch("scripts.onnxruntime.bootstrap.urlopen", return_value=response(self.payload)):
            installed = self.installer.install("compiler", [self.asset], ["bin/compiler"])
        (installed / "bin/compiler").unlink()
        with patch("scripts.onnxruntime.bootstrap.urlopen", side_effect=AssertionError("unnecessary download")):
            self.assertEqual(self.installer.install("compiler", [self.asset], ["bin/compiler"]), installed)
        self.assertEqual((installed / "bin/compiler").read_bytes(), b"fixture compiler")

    def test_checksum_failure_leaves_no_installation_and_can_be_retried(self) -> None:
        with patch("scripts.onnxruntime.bootstrap.urlopen", return_value=response(b"corrupt download")):
            with self.assertRaisesRegex(BuildError, "checksum mismatch"):
                self.installer.install("compiler", [self.asset], ["bin/compiler"])
        self.assertEqual(list(self.installer.root.glob("compiler-*")), [])
        with patch("scripts.onnxruntime.bootstrap.urlopen", return_value=response(self.payload)):
            installed = self.installer.install("compiler", [self.asset], ["bin/compiler"])
        self.assertTrue((installed / "bin/compiler").is_file())

    def test_tar_paths_cannot_escape_the_tool_installation(self) -> None:
        payload = tool_archive({"../escaped": b"invalid"})
        asset = {**self.asset, "sha256": hashlib.sha256(payload).hexdigest()}
        with patch("scripts.onnxruntime.bootstrap.urlopen", return_value=response(payload)):
            with self.assertRaisesRegex(BuildError, "cannot unpack build tool"):
                self.installer.install("compiler", [asset], ["bin/compiler"])
        self.assertFalse((self.root / "escaped").exists())

    def test_zip_build_tools_are_installed_as_executable_files(self) -> None:
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as archive:
            member = zipfile.ZipInfo("ninja")
            member.external_attr = 0o100755 << 16
            archive.writestr(member, b"fixture executable")
        asset = {**self.asset, "sha256": hashlib.sha256(payload.getvalue()).hexdigest(), "strip_root": False}
        with patch("scripts.onnxruntime.bootstrap.urlopen", return_value=response(payload.getvalue())):
            installed = self.installer.install("ninja", [asset], ["ninja"])
        self.assertEqual((installed / "ninja").read_bytes(), b"fixture executable")
        self.assertEqual((installed / "ninja").stat().st_mode & 0o111, 0o111)

    def test_image_sysroot_is_private_atomic_and_reused_without_docker(self) -> None:
        asset = {"image": "quay.io/pypa/manylinux_2_28_x86_64@sha256:" + "a" * 64,
                 "platform": "linux/amd64", "paths": ["usr/lib64"]}

        def docker(command: list[str], **kwargs: object) -> None:
            if "cp" in command:
                with tarfile.open(fileobj=kwargs["stdout"], mode="w") as archive:
                    directory = tarfile.TarInfo("lib64")
                    directory.type = tarfile.DIRTYPE
                    directory.mode = 0o555
                    archive.addfile(directory)
                    library = tarfile.TarInfo("lib64/libc.so.6")
                    library.size = len(b"target libc")
                    archive.addfile(library, io.BytesIO(b"target libc"))
                    loader = tarfile.TarInfo("lib64/ld-linux-x86-64.so.2")
                    loader.type = tarfile.SYMTYPE
                    loader.linkname = "/usr/lib64/libc.so.6"
                    archive.addfile(loader)

        with patch("scripts.onnxruntime.bootstrap.executable", return_value="/host/docker"), \
                patch("scripts.onnxruntime.bootstrap.output", return_value="container-id"), \
                patch("scripts.onnxruntime.bootstrap.run") as commands, \
                patch("scripts.onnxruntime.bootstrap.subprocess.run", side_effect=docker):
            installed = self.installer.install_image("sysroot", asset, ["lib64/ld-linux-x86-64.so.2"])
        self.assertEqual((installed / "lib64/ld-linux-x86-64.so.2").read_bytes(), b"target libc")
        self.assertTrue((installed / "lib64/ld-linux-x86-64.so.2").resolve().is_relative_to(installed))
        self.assertIn(["/host/docker", "rm", "container-id"], [call.args[0] for call in commands.call_args_list])
        with patch("scripts.onnxruntime.bootstrap.executable", side_effect=AssertionError("cached SDK needs no Docker")):
            self.assertEqual(self.installer.install_image("sysroot", asset, ["lib64/ld-linux-x86-64.so.2"]), installed)

    def test_incomplete_image_sysroot_does_not_install_and_removes_its_container(self) -> None:
        asset = {"image": "quay.io/pypa/manylinux_2_28_x86_64@sha256:" + "b" * 64,
                 "platform": "linux/amd64", "paths": []}
        with patch("scripts.onnxruntime.bootstrap.executable", return_value="/host/docker"), \
                patch("scripts.onnxruntime.bootstrap.output", return_value="container-id"), \
                patch("scripts.onnxruntime.bootstrap.run") as commands:
            with self.assertRaisesRegex(BuildError, "missing.*stdio.h"):
                self.installer.install_image("sysroot", asset, ["usr/include/stdio.h"])
        self.assertEqual(list(self.installer.root.glob("sysroot-*")), [])
        self.assertIn(["/host/docker", "rm", "container-id"], [call.args[0] for call in commands.call_args_list])


class BootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.environment = patch.dict(os.environ, {name: "" for name in (
            "CI", "CUDA_HOME", "CUDACXX", "CUDNN_HOME", "CUDAHOSTCXX", "CMAKE_CUDA_COMPILER_LAUNCHER",
            "ONNXRUNTIME_SYSROOT", "ONNXRUNTIME_GCC_TOOLCHAIN", "ONNXRUNTIME_QEMU",
        )})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def bootstrap(self, target: str = "linux/amd64") -> Bootstrap:
        with patch("scripts.onnxruntime.bootstrap.native_platform", return_value="linux/amd64"):
            return Bootstrap(self.root, target)

    def cuda_downloads(self, bootstrap: Bootstrap, missing_file: str | None = None) -> dict[str, bytes]:
        downloads = {}
        for component, assets in bootstrap.catalog["cuda"].items():
            for platform, asset in assets.items():
                files = {"release/" + name: (platform + " " + name).encode()
                         for name in CUDA_SDK_FILES[component] if name != missing_file}
                payload = tool_archive(files)
                url = f"https://tools.example/{component}/{platform}/archive.tar.gz"
                assets[platform] = {**asset, "url": url, "sha256": hashlib.sha256(payload).hexdigest()}
                downloads[url] = payload
        return downloads

    def test_cuda_sdk_assembles_cusparse_and_compiler_inputs_for_each_linux_target(self) -> None:
        cxx = Compiler("/host/clang++-21", [], [], "clang version 21.1.8")
        for target in ("linux/amd64", "linux/arm64"):
            with self.subTest(target=target):
                bootstrap = self.bootstrap(target)
                downloads = self.cuda_downloads(bootstrap)
                configured = {"CUDA_HOME": "", "CUDACXX": "", "CUDAHOSTCXX": "", "CUDNN_HOME": "/sdk/cudnn"}
                with patch.dict(os.environ, configured), \
                        patch("scripts.onnxruntime.bootstrap.shutil.which", return_value=None), \
                        patch("scripts.onnxruntime.bootstrap.urlopen",
                              side_effect=lambda request, timeout: response(downloads[request.full_url])):
                    bootstrap.cuda(cxx)
                    root = Path(os.environ["CUDA_HOME"])
                target_dir = "targets/x86_64-linux" if target == "linux/amd64" else "targets/sbsa-linux"
                for name in ("include/cusparse.h", "lib/libcusparse.so", "include/cublasLt.h", "lib/libcublasLt.so"):
                    self.assertEqual((root / target_dir / name).read_bytes(), (target + " " + name).encode())
                self.assertEqual((root / "include/cusparse.h").resolve(), root / target_dir / "include/cusparse.h")
                self.assertEqual((root / "lib64/libcusparse.so").resolve(), root / target_dir / "lib/libcusparse.so")
                self.assertEqual((root / "bin/nvcc").read_bytes(), b"linux/amd64 bin/nvcc")

    def test_cuda_sdk_rejects_missing_provider_headers_and_libraries(self) -> None:
        cxx = Compiler("/host/clang++-21", [], [], "clang version 21.1.8")
        for target in ("linux/amd64", "linux/arm64"):
            for missing_file in ("include/cusparse.h", "lib/libcusparse.so", "include/cublasLt.h", "lib/libnvrtc-builtins.so"):
                with self.subTest(target=target, missing_file=missing_file):
                    bootstrap = self.bootstrap(target)
                    downloads = self.cuda_downloads(bootstrap, missing_file)
                    configured = {"CUDA_HOME": "", "CUDACXX": "", "CUDAHOSTCXX": "", "CUDNN_HOME": "/sdk/cudnn"}
                    with patch.dict(os.environ, configured), \
                            patch("scripts.onnxruntime.bootstrap.shutil.which", return_value=None), \
                            patch("scripts.onnxruntime.bootstrap.urlopen",
                                  side_effect=lambda request, timeout: response(downloads[request.full_url])):
                        with self.assertRaisesRegex(BuildError, "missing.*" + re.escape(missing_file)):
                            bootstrap.cuda(cxx)

    def test_cuda_sdk_repairs_cached_provider_files_without_downloading_again(self) -> None:
        cxx = Compiler("/host/clang++-21", [], [], "clang version 21.1.8")
        for target in ("linux/amd64", "linux/arm64"):
            with self.subTest(target=target):
                bootstrap = self.bootstrap(target)
                downloads = self.cuda_downloads(bootstrap)
                configured = {"CUDA_HOME": "", "CUDACXX": "", "CUDAHOSTCXX": "", "CUDNN_HOME": "/sdk/cudnn"}
                with patch.dict(os.environ, configured), \
                        patch("scripts.onnxruntime.bootstrap.shutil.which", return_value=None), \
                        patch("scripts.onnxruntime.bootstrap.urlopen",
                              side_effect=lambda request, timeout: response(downloads[request.full_url])):
                    bootstrap.cuda(cxx)
                    root = Path(os.environ["CUDA_HOME"])
                with patch("scripts.onnxruntime.bootstrap.shutil.which", return_value=None), \
                        patch("scripts.onnxruntime.bootstrap.urlopen", side_effect=AssertionError("unnecessary download")):
                    with patch.dict(os.environ, configured):
                        bootstrap.cuda(cxx)
                        self.assertEqual(Path(os.environ["CUDA_HOME"]), root)
                    for name in ("include/cusparse.h", "lib64/libcusparse.so"):
                        with self.subTest(file=name):
                            path = root / name
                            contents = path.read_bytes()
                            path.unlink()
                            with patch.dict(os.environ, configured):
                                bootstrap.cuda(cxx)
                                self.assertEqual(Path(os.environ["CUDA_HOME"]), root)
                            self.assertEqual(path.read_bytes(), contents)

    def test_cuda_sdk_component_pin_changes_installation_and_reuses_cached_inputs(self) -> None:
        cxx = Compiler("/host/clang++-21", [], [], "clang version 21.1.8")
        bootstrap = self.bootstrap()
        downloads = self.cuda_downloads(bootstrap)
        configured = {"CUDA_HOME": "", "CUDACXX": "", "CUDAHOSTCXX": "", "CUDNN_HOME": "/sdk/cudnn"}
        with patch.dict(os.environ, configured), \
                patch("scripts.onnxruntime.bootstrap.shutil.which", return_value=None), \
                patch("scripts.onnxruntime.bootstrap.urlopen",
                      side_effect=lambda request, timeout: response(downloads[request.full_url])):
            bootstrap.cuda(cxx)
            initial_root = Path(os.environ["CUDA_HOME"])
        asset = bootstrap.catalog["cuda"]["libcusparse"]["linux/amd64"]
        payload = tool_archive({"release/include/cusparse.h": b"updated sparse header",
                                "release/lib/libcusparse.so": b"updated sparse library"})
        asset["sha256"] = hashlib.sha256(payload).hexdigest()
        with patch.dict(os.environ, configured), \
                patch("scripts.onnxruntime.bootstrap.shutil.which", return_value=None), \
                patch("scripts.onnxruntime.bootstrap.urlopen", return_value=response(payload)) as download:
            bootstrap.cuda(cxx)
            root = Path(os.environ["CUDA_HOME"])
        self.assertNotEqual(root, initial_root)
        self.assertEqual((root / "include/cusparse.h").read_bytes(), b"updated sparse header")
        self.assertEqual((root / "lib64/libcusparse.so").read_bytes(), b"updated sparse library")
        download.assert_called_once()
        self.assertEqual(download.call_args.args[0].full_url, asset["url"])

    def test_completed_runtime_skips_all_toolchain_preparation(self) -> None:
        repo = fixture_repository(self.root / "repo")
        build = ManagedRuntimeBuild(BuildSpec.create("native", repo), self.root / "stage")
        build.package_dir.mkdir(parents=True)
        construct_package(build.package_dir, platform=build.spec.platform)
        build._seal(build.package_dir)
        build.checksum()
        with patch("scripts.onnxruntime.artifacts.ManagedRuntimeBuild._build", side_effect=AssertionError("unnecessary compilation")):
            self.assertEqual(build.build_source(), build.package_dir / "lib")

    def test_configured_versioned_llvm_commands_and_flags_are_preserved(self) -> None:
        binaries = self.root / "bin"
        binaries.mkdir()
        for name in ("clang-23", "clang++-23", "llvm-ar-23"):
            binary = binaries / name
            binary.write_text("#!/bin/sh\nprintf 'clang version 23.0.0\\n'\n")
            binary.chmod(0o755)
        configured = {"CC": f"{binaries}/clang-23",
                      "CXX": f"{binaries}/clang++-23 -O2", "AR": ""}
        bootstrap = self.bootstrap()
        wrapper = os.environ.get("RUSTC_WRAPPER")
        with patch.dict(os.environ, configured), patch.object(bootstrap, "program"), \
                patch.object(bootstrap, "linux_target"), patch.object(bootstrap, "cuda"), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unnecessary tools")):
            with bootstrap.environment():
                self.assertEqual(os.environ["CC"], configured["CC"])
                self.assertEqual(os.environ["CXX"], configured["CXX"])
                self.assertEqual(os.environ["AR"], str(binaries / "llvm-ar-23"))
                self.assertEqual(os.environ.get("RUSTC_WRAPPER"), wrapper)
            self.assertEqual(os.environ["AR"], "")

    def test_arm64_cuda_sdk_uses_host_compiler_tools_and_target_libraries(self) -> None:
        bootstrap = self.bootstrap("linux/arm64")
        cxx = Compiler("/host/clang++-23", [], ["-O2"], "clang version 23.0.0")
        installed = []
        host_command = os.environ.get("CXX")

        def install(name, entries, required, transform=None):
            installed.append((name, entries, required))
            return self.root / name

        with patch.object(bootstrap.installer, "install", side_effect=install), \
                patch("scripts.onnxruntime.bootstrap.shutil.which",
                      side_effect=lambda name: "/host/clang++-21" if name == "clang++-21" else None):
            bootstrap.cuda(cxx)
        entries = installed[0][1]
        self.assertTrue(all("linux-x86_64" in item["url"] for item in entries[:2]))
        self.assertTrue(all("linux-sbsa" in item["url"] for item in entries[2:]))
        self.assertTrue(all(item["destination"] == "targets/sbsa-linux" for item in entries[3:]))
        self.assertIn("cudnn/linux-sbsa/", installed[1][1][0]["url"])
        self.assertEqual(os.environ["CUDAHOSTCXX"], "/host/clang++-21 -O2")
        self.assertEqual(os.environ["CMAKE_CUDA_COMPILER_LAUNCHER"], "")
        self.assertEqual(os.environ.get("CXX"), host_command)

    def test_explicit_sdk_and_cuda_compiler_settings_need_no_downloads(self) -> None:
        bootstrap = self.bootstrap()
        cxx = Compiler("/host/clang++-23", [], [], "clang version 23.0.0")
        configured = {"CUDA_HOME": "/sdk/cuda", "CUDNN_HOME": "/sdk/cudnn", "CUDAHOSTCXX": "/host/clang++-21"}
        nvcc = Compiler("/sdk/cuda/bin/nvcc", [], [], "Cuda compilation tools, release 13.2")
        host = Compiler("/host/clang++-21", [], [], "clang version 21.1.8")
        with patch.dict(os.environ, configured), \
                patch("scripts.onnxruntime.bootstrap.Compiler.discover", side_effect=[nvcc, host]), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unnecessary tools")):
            bootstrap.cuda(cxx)
            self.assertEqual({key: os.environ[key] for key in configured}, configured)

    def test_local_versioned_llvm_supplies_elf_inspection_tools(self) -> None:
        bootstrap = self.bootstrap()
        binary = self.root / "llvm-objdump-23"
        binary.write_text("fixture inspector")
        original = os.environ["PATH"]
        with patch("scripts.onnxruntime.bootstrap.shutil.which",
                   side_effect=lambda name: str(binary) if name == "llvm-objdump-23" else None), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unnecessary tools")):
            bootstrap.objdump()
        directory = Path(os.environ["PATH"].split(os.pathsep)[0])
        self.assertEqual((directory / "objdump").resolve(), binary)
        os.environ["PATH"] = original

    def test_macos_runtime_does_not_need_cuda_tools(self) -> None:
        bootstrap = self.bootstrap("darwin/arm64")
        cxx = Compiler("/Xcode/clang++", [], [], "Apple clang version 17.0.0")
        with patch.object(bootstrap.installer, "install", side_effect=AssertionError("unnecessary tools")):
            bootstrap.cuda(cxx)

    def test_cuda_host_compiler_is_selected_for_the_configured_toolkit(self) -> None:
        bootstrap = self.bootstrap()
        cxx = Compiler("/host/clang++-23", [], [], "clang version 23.0.0")
        nvcc = Compiler("/sdk/cuda/bin/nvcc", [], [], "Cuda compilation tools, release 13.0")
        configured = {"CUDA_HOME": "/sdk/cuda", "CUDNN_HOME": "/sdk/cudnn"}
        with patch.dict(os.environ, configured), patch("scripts.onnxruntime.bootstrap.Compiler.discover", return_value=nvcc), \
                patch("scripts.onnxruntime.bootstrap.shutil.which",
                      side_effect=lambda name: "/host/clang++-20" if name == "clang++-20" else None), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unnecessary tools")):
            bootstrap.cuda(cxx)
            self.assertEqual(os.environ["CUDAHOSTCXX"], "/host/clang++-20")

    def test_missing_local_cuda_host_compiler_fails_before_downloading_sdks(self) -> None:
        bootstrap = self.bootstrap()
        cxx = Compiler("/host/clang++-23", ["/host/kache"], [], "clang version 23.0.0")
        with patch("scripts.onnxruntime.bootstrap.shutil.which", return_value=None), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unexpected tool download")):
            with self.assertRaisesRegex(BuildError, "install.*clang.*21"):
                bootstrap.cuda(cxx)

    def test_missing_local_clang_requires_host_installation(self) -> None:
        bootstrap = self.bootstrap()
        with patch.dict(os.environ, {"CC": "", "CXX": "", "AR": ""}), \
                patch("scripts.onnxruntime.bootstrap.shutil.which", return_value=None), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unexpected tool download")):
            with self.assertRaisesRegex(BuildError, "install.*LLVM.*clang"):
                bootstrap.compilers()

    def test_missing_local_archiver_requires_host_installation(self) -> None:
        bootstrap = self.bootstrap()
        compiler = Compiler("/host/clang-23", [], [], "clang version 23.0.0")
        with patch.dict(os.environ, {"CC": "/host/clang-23", "CXX": "/host/clang++-23", "AR": ""}), \
                patch("scripts.onnxruntime.bootstrap.Compiler.discover", return_value=compiler), \
                patch("scripts.onnxruntime.bootstrap.shutil.which", return_value=None), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unexpected tool download")):
            with self.assertRaisesRegex(BuildError, "install.*LLVM.*llvm-ar"):
                bootstrap.compilers()

    def test_versioned_local_lld_is_used_for_arm64_cross_compilation(self) -> None:
        bootstrap = self.bootstrap("linux/arm64")
        binary = self.root / "ld.lld-21"
        binary.write_text("fixture linker")
        original = os.environ["PATH"]
        with patch.dict(os.environ, {"ONNXRUNTIME_SYSROOT": "/sysroot", "ONNXRUNTIME_QEMU": "/host/qemu"}), \
                patch("scripts.onnxruntime.bootstrap.shutil.which",
                      side_effect=lambda name: str(binary) if name == "ld.lld-21" else None), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unexpected tool download")):
            bootstrap.linux_target()
            directory = Path(os.environ["PATH"].split(os.pathsep)[0])
            self.assertEqual((directory / "ld.lld").resolve(), binary)
        self.assertEqual(os.environ["PATH"], original)

    def test_missing_local_lld_fails_before_downloading_cross_inputs(self) -> None:
        bootstrap = self.bootstrap("linux/arm64")
        with patch("scripts.onnxruntime.bootstrap.shutil.which", return_value=None), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unexpected tool download")):
            with self.assertRaisesRegex(BuildError, "install.*LLVM.*ld.lld"):
                bootstrap.linux_target()

    def test_arm64_cross_execution_uses_the_containers_qemu(self) -> None:
        bootstrap = self.bootstrap("linux/arm64")
        with patch.dict(os.environ, {"ONNXRUNTIME_SYSROOT": "/sysroot", "ONNXRUNTIME_QEMU": ""}), \
                patch("scripts.onnxruntime.bootstrap.shutil.which", return_value="/usr/bin/qemu-aarch64"), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unexpected tool download")):
            bootstrap.linux_target()
            self.assertEqual(os.environ["ONNXRUNTIME_QEMU"], "/usr/bin/qemu-aarch64")

    def test_arm64_cross_execution_requires_the_containers_qemu(self) -> None:
        bootstrap = self.bootstrap("linux/arm64")
        with patch.dict(os.environ, {"ONNXRUNTIME_SYSROOT": "/sysroot", "ONNXRUNTIME_QEMU": ""}), \
                patch("scripts.onnxruntime.bootstrap.shutil.which",
                      side_effect=lambda name: "/usr/bin/ld.lld" if name == "ld.lld" else None), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unexpected tool download")):
            with self.assertRaisesRegex(BuildError, "qemu-user.*apt"):
                bootstrap.linux_target()

    def test_bootstrap_restores_environment_after_a_failure(self) -> None:
        bootstrap = self.bootstrap()
        original = dict(os.environ)

        def prepare(name, minimum=None):
            bootstrap.add_path(self.root)
            raise BuildError("tool download interrupted")

        with patch.object(bootstrap, "program", side_effect=prepare):
            with self.assertRaisesRegex(BuildError, "interrupted"):
                with bootstrap.environment():
                    self.fail("preparation must complete before compilation")
        self.assertEqual(dict(os.environ), original)

    @unittest.skipUnless(shutil.which("clang-23") and shutil.which("clang++-23") and shutil.which("clang++-21")
                         and shutil.which("kache"), "requires installed LLVM 21, LLVM 23, and host wrapper")
    def test_build_environment_selects_compiler_executables_directly(self) -> None:
        bootstrap = self.bootstrap()
        configured = {"CC": "kache clang-23", "CXX": "kache clang++-23 -O2",
                      "CUDAHOSTCXX": "kache clang++-21", "CMAKE_CXX_COMPILER_LAUNCHER": "kache",
                      "CMAKE_CUDA_COMPILER_LAUNCHER": "kache"}
        wrapper = os.environ.get("RUSTC_WRAPPER")
        with patch.dict(os.environ, configured), patch.object(bootstrap, "program"), \
                patch.object(bootstrap, "linux_target"), patch.object(bootstrap, "objdump"), \
                patch.object(bootstrap, "cuda", side_effect=lambda cxx: bootstrap.cuda_host(cxx, 21)):
            with bootstrap.environment():
                self.assertEqual(os.environ["CC"], shutil.which("clang-23"))
                self.assertEqual(os.environ["CXX"], f"{shutil.which('clang++-23')} -O2")
                self.assertEqual(os.environ["CUDAHOSTCXX"], shutil.which("clang++-21"))
                self.assertEqual(os.environ["CMAKE_CXX_COMPILER_LAUNCHER"], "")
                self.assertEqual(os.environ["CMAKE_CUDA_COMPILER_LAUNCHER"], "")
                self.assertEqual(os.environ.get("RUSTC_WRAPPER"), wrapper)
            self.assertEqual({name: os.environ[name] for name in configured}, configured)

    def test_real_cmake_cuda_identification_uses_the_direct_host_compiler(self) -> None:
        tools = Path.home() / ".cache/nervix-build/onnxruntime/tools"
        nvcc = next(tools.glob("cuda-linux-amd64-to-linux-amd64-*/bin/nvcc"), None)
        gcc = next(tools.glob("amd64-sysroot-*/opt/rh/gcc-toolset-14/root/usr"), None)
        if nvcc is None or gcc is None or not all(shutil.which(name) for name in ("clang-23", "clang++-23", "clang++-21", "cmake", "ninja")):
            self.skipTest("requires cached native CUDA and Linux runtime SDKs and installed LLVM 21 and 23")
        bootstrap = self.bootstrap()
        source = self.root / "cuda-probe"
        source.mkdir()
        (source / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.29)\nproject(nervix_cuda_probe LANGUAGES CUDA CXX)\n"
            "file(WRITE ${CMAKE_BINARY_DIR}/selected-host.txt ${CMAKE_CUDA_HOST_COMPILER})\n"
        )
        configured = {"CC": "kache clang-23", "CXX": "kache clang++-23", "CUDAHOSTCXX": "",
                      "CUDA_HOME": str(nvcc.parent.parent), "CUDNN_HOME": str(self.root / "cudnn"),
                      "ONNXRUNTIME_SYSROOT": str(gcc.parents[4]), "ONNXRUNTIME_GCC_TOOLCHAIN": str(gcc)}
        build_dir = self.root / "probe-build"
        with patch.dict(os.environ, configured), \
                patch.object(bootstrap.installer, "install", side_effect=AssertionError("unexpected download")):
            with bootstrap.environment():
                resolved = HostToolchain.discover("linux/amd64")
                cuda_flags = " ".join(f"-Xcompiler={argument}" for argument in
                                      [*resolved.cuda_host.arguments, "-fuse-ld=lld", "-Qunused-arguments"])
                definitions = [*resolved.cxx.definitions("CXX"), *resolved.cuda.definitions("CUDA"),
                               f"CMAKE_CUDA_HOST_COMPILER={resolved.cuda_host.executable}",
                               f"CMAKE_CUDA_FLAGS={cuda_flags}",
                               "CMAKE_CUDA_ARCHITECTURES=75", f"CUDAToolkit_ROOT={nvcc.parent.parent}"]
                result = subprocess.run(["cmake", "-S", str(source), "-B", str(build_dir), "-G", "Ninja",
                                         *["-D" + value for value in definitions]],
                                        capture_output=True, text=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((build_dir / "selected-host.txt").read_text(), shutil.which("clang++-21"))


class ManualRebuildTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.repo = fixture_repository(self.root / "repo")
        self.build = ManagedRuntimeBuild(BuildSpec.create("linux/amd64", self.repo), self.root / "stage")
        environment = patch.dict(os.environ, {"CI": ""})
        environment.start()
        self.addCleanup(environment.stop)
        self.build.package_dir.mkdir(parents=True)
        construct_package(self.build.package_dir)
        self.build._seal(self.build.package_dir)
        self.build.checksum()

    def test_force_build_replaces_the_completed_package_after_validation(self) -> None:
        cached_tool = self.build.stage_root / "tools/compiler"
        cached_tool.parent.mkdir()
        cached_tool.write_text("installed host tool")

        def compile_package(destination: Path) -> None:
            self.build.validate_package()
            self.build._validate_verification()
            construct_package(destination)
            (destination / "lib/libonnxruntime.a").write_bytes(b"!<arch>\nfresh compilation")

        with patch.object(self.build, "_build", side_effect=compile_package) as compiler:
            self.assertEqual(self.build.build_source(force=True), self.build.package_dir / "lib")
        compiler.assert_called_once()
        self.build.checksum()
        self.assertEqual((self.build.package_dir / "lib/libonnxruntime.a").read_bytes(), b"!<arch>\nfresh compilation")
        self.assertEqual(cached_tool.read_text(), "installed host tool")
        with patch.object(self.build, "_build", side_effect=AssertionError("unnecessary compilation")):
            self.assertEqual(self.build.prepare(), self.build.package_dir / "lib")

    def test_failed_force_build_keeps_the_verified_package_and_checksum(self) -> None:
        pin = (self.repo / "scripts/onnxruntime/checksums.toml").read_bytes()
        receipt = self.build.stage_root / "verified" / f"{self.build.spec.fingerprint}.json"
        verified = receipt.read_bytes()
        manifest = (self.build.package_dir / "manifest.json").read_bytes()
        with patch.object(self.build, "_build", side_effect=BuildError("compiler failed")):
            with self.assertRaisesRegex(BuildError, "compiler failed"):
                self.build.build_source(force=True)
        self.assertEqual((self.repo / "scripts/onnxruntime/checksums.toml").read_bytes(), pin)
        self.assertEqual(receipt.read_bytes(), verified)
        self.assertEqual((self.build.package_dir / "manifest.json").read_bytes(), manifest)
        self.assertEqual(self.build.prepare(), self.build.package_dir / "lib")

    def test_force_build_is_disabled_in_ci_before_compilation(self) -> None:
        with patch.dict(os.environ, {"CI": "true"}), \
                patch.object(self.build, "_build", side_effect=AssertionError("unexpected compilation")):
            with self.assertRaisesRegex(BuildError, "disabled in CI"):
                self.build.build_source(force=True)
        self.assertEqual(self.build.prepare(), self.build.package_dir / "lib")

    def test_force_compilation_starts_with_an_empty_tree_and_preserves_source_and_sdk_downloads(self) -> None:
        build = ContainerRuntimeBuild(self.build.spec, 2, force=True)
        build.stage_root = self.root / "stage"
        build.build_dir = build.stage_root / "builds/compiler"
        compiled = build.build_dir / "objects/compiled.o"
        compiled.parent.mkdir(parents=True)
        compiled.write_text("previous compiler output")
        dependency = build.build_dir / "_deps/library/header.h"
        dependency.parent.mkdir(parents=True)
        dependency.write_text("configured dependency")
        source = build.stage_root / "sources/revision/source.cc"
        sdk = build.stage_root / "tools/cuda/header.h"
        for path in (source, sdk):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("verified download")

        def compile_package(destination: Path) -> None:
            self.assertFalse(build.build_dir.exists())
            for path in (source, sdk):
                self.assertEqual(path.read_text(), "verified download")
            compiled.parent.mkdir(parents=True)
            compiled.write_text("fresh compiler output")

        with patch.object(RuntimeBuild, "compile", side_effect=compile_package):
            build.compile(self.root / "destination")
        self.assertEqual(compiled.read_text(), "fresh compiler output")

    def test_linux_builds_use_a_pinned_container_and_isolate_the_callers_toolchain(self) -> None:
        for variant in ("portable", "docker"):
            for platform in ("linux/amd64", "linux/arm64"):
                with self.subTest(platform=platform, variant=variant):
                    spec = BuildSpec.create(platform, self.repo, variant=variant)
                    build = ManagedRuntimeBuild(spec, self.root / "stage", jobs=3)
                    destination = build.stage_root / "packages/temporary/package"
                    destination.mkdir(parents=True, exist_ok=True)
                    sdk = build.stage_root / "tools/target-sdk"
                    commands = []

                    def execute(arguments: list[str]) -> None:
                        commands.append(arguments)
                        if arguments[:2] == ["docker", "run"]:
                            (destination.parent / "provenance.json").write_text(json.dumps({"container": "verified"}))

                    configured = {"CC": "caller-compiler", "CXXFLAGS": "caller-flags",
                                  "CUDA_HOME": "/caller/cuda", "CMAKE_CUDA_COMPILER_LAUNCHER": "caller-cache"}
                    with patch.dict(os.environ, configured), \
                            patch("scripts.onnxruntime.containers.native_platform", return_value="linux/amd64"), \
                            patch("scripts.onnxruntime.containers.install_linux_sdk", return_value=sdk), \
                            patch("scripts.onnxruntime.containers.subprocess.check_output", return_value="sha256:" + "a" * 64), \
                            patch("scripts.onnxruntime.containers.run", side_effect=execute):
                        DockerBuilder(build).compile(destination, force=True)
                    image_build, compilation = commands
                    image_arguments = [image_build[index + 1] for index, value in enumerate(image_build)
                                       if value == "--build-arg"]
                    self.assertEqual(image_arguments, [f"DEBIAN_IMAGE={spec.identity['builder']['image']}",
                                                       f"LLVM_KEY_SHA256={spec.identity['builder']['llvm_key_sha256']}"])
                    self.assertIn("--force", compilation)
                    self.assertIn("type=bind,source=" + str(self.repo) + ",target=/workspace,readonly", compilation)
                    self.assertEqual(compilation[compilation.index("--variant") + 1], variant)
                    self.assertEqual(compilation[compilation.index("--jobs") + 1], "3")
                    expected_root = "/cache/tools/target-sdk" if variant == "portable" else "/"
                    self.assertIn(f"ONNXRUNTIME_SYSROOT={expected_root}", compilation)
                    for value in configured.values():
                        self.assertNotIn(value, " ".join(compilation))
                    self.assertEqual(build.provenance, {"container": "verified"})
