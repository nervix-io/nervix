from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
from shutil import which
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts.build_onnxruntime import BuildSpec, RuntimeBuild
from scripts.onnxruntime.toolchain import BuildError, Compiler, HostToolchain
from scripts.tests.test_build_onnxruntime import construct_cuda_runtime


ROOT = Path(__file__).resolve().parents[2]
CLANG_CXX = which("clang++-23") or which("clang++-22") or which("clang++-21") or which("clang++")


def tool_output(command: list[str], **kwargs: object) -> str:
    if command[0] == "xcrun":
        if command[-1] == "--show-sdk-path":
            return "/Xcode/SDK"
        if command[-1] == "--show-sdk-version":
            return "15.0"
        return f"/Xcode/bin/{command[-1]}"
    name = Path(command[0]).name
    if name.startswith("clang"):
        return "clang version 22.1.0"
    if name.startswith("llvm-ar"):
        return "LLVM version 22.1.0"
    if name == "nvcc":
        return "Cuda compilation tools, release 13.1, V13.1.80"
    if name == "gcc":
        return "gcc version 14.0"
    return f"{name} version 4.2.3"


class NativeToolchainTests(unittest.TestCase):
    def setUp(self) -> None:
        for mocked in (
            patch.dict(os.environ, {name: "" for name in (
                "CI", "CC", "CXX", "AR", "CUDA_HOME", "CUDACXX", "CUDAHOSTCXX", "CUDNN_HOME",
                "CMAKE_C_COMPILER_LAUNCHER", "CMAKE_CXX_COMPILER_LAUNCHER", "CMAKE_CUDA_COMPILER_LAUNCHER",
                "ONNXRUNTIME_SYSROOT", "ONNXRUNTIME_GCC_TOOLCHAIN", "ONNXRUNTIME_QEMU",
            )}),
            patch("scripts.onnxruntime.toolchain.shutil.which",
                  side_effect=lambda name: name if name.startswith("/") else f"/host/bin/{name}"),
            patch("scripts.onnxruntime.toolchain.subprocess.check_output", side_effect=tool_output),
            patch("scripts.onnxruntime.toolchain.platform.system", return_value="Linux"),
            patch("scripts.onnxruntime.toolchain.platform.machine", return_value="x86_64"),
        ):
            mocked.start()
            self.addCleanup(mocked.stop)

    def test_host_compiler_commands_preserve_the_kache_launcher(self) -> None:
        with patch.dict(os.environ, {"CC": "kache clang-22", "CXX": "kache clang++-22 -fno-omit-frame-pointer"}):
            tools = HostToolchain.discover("linux/amd64")
        self.assertEqual(tools.cc.command, ["/host/bin/kache", "/host/bin/clang-22"])
        self.assertEqual(tools.cxx.command, ["/host/bin/kache", "/host/bin/clang++-22", "-fno-omit-frame-pointer"])
        self.assertIn("CMAKE_CXX_COMPILER_LAUNCHER=/host/bin/kache", tools.cxx.definitions("CXX"))
        self.assertIn("CMAKE_CXX_COMPILER_ARG1=-fno-omit-frame-pointer", tools.cxx.definitions("CXX"))
        self.assertEqual(tools.aggregate, ["/host/bin/llvm-ar-22", "qcLs"])

    def test_cmake_launcher_configuration_is_used_for_an_unwrapped_compiler(self) -> None:
        with patch.dict(os.environ, {"CMAKE_CXX_COMPILER_LAUNCHER": "kache;--local"}):
            compiler = Compiler.discover("CXX", "clang++", "CXX")
        self.assertEqual(compiler.command, ["/host/bin/kache", "--local", "/host/bin/clang++"])

    def test_compiler_selection_requires_llvm(self) -> None:
        with patch.dict(os.environ, {"CC": "gcc"}):
            with self.assertRaisesRegex(BuildError, "CC must select LLVM Clang"):
                HostToolchain.discover("linux/amd64")

    def cross_sdk(self, root: Path) -> dict[str, str]:
        sysroot = root / "sysroot"
        gcc = root / "gcc"
        cuda = root / "cuda"
        cudnn = root / "cudnn"
        for path in (sysroot, gcc, cuda / "targets/sbsa-linux/include", cudnn / "lib"):
            path.mkdir(parents=True)
        (cuda / "targets/sbsa-linux/include/cuda_runtime.h").write_text("CUDA header")
        return {"ONNXRUNTIME_SYSROOT": str(sysroot), "ONNXRUNTIME_GCC_TOOLCHAIN": str(gcc),
                "CUDA_HOME": str(cuda), "CUDNN_HOME": str(cudnn)}

    def test_arm64_cross_compilation_preserves_launchers_and_targets_every_compiler(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            environment.update({"CC": "kache clang-22", "CXX": "kache clang++-22",
                                "CUDACXX": "kache nvcc", "CUDAHOSTCXX": "clang++-21 -fno-omit-frame-pointer"})
            with patch.dict(os.environ, environment):
                tools = HostToolchain.discover("linux/arm64")
            for compiler in (tools.cc, tools.cxx, tools.cuda_host):
                self.assertIn("--target=aarch64-linux-gnu", compiler.arguments)
                self.assertIn(f"--sysroot={environment['ONNXRUNTIME_SYSROOT']}", compiler.arguments)
                self.assertIn(f"--gcc-toolchain={environment['ONNXRUNTIME_GCC_TOOLCHAIN']}", compiler.arguments)
            self.assertEqual(tools.cc.launcher, ["/host/bin/kache"])
            self.assertEqual(tools.cxx.launcher, ["/host/bin/kache"])
            self.assertEqual(tools.cuda.launcher, ["/host/bin/kache"])
            self.assertIn("--target-directory=sbsa-linux", tools.cuda.arguments)
            self.assertIn("-fno-omit-frame-pointer", tools.cuda_host.arguments)
            self.assertEqual(tools.metadata["platform"], "linux/amd64")
            self.assertEqual(tools.metadata["cross"]["target"], "linux/arm64")

    def test_arm64_cross_compile_configures_cmake_for_target_libraries_and_host_tools(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            with patch.dict(os.environ, environment):
                tools = HostToolchain.discover("linux/arm64")
            build = RuntimeBuild(BuildSpec.create("linux/arm64", repo=ROOT), Path(temporary))
            build.tools = tools
            with patch("scripts.build_onnxruntime.run", side_effect=BuildError("configuration captured")) as command:
                with self.assertRaisesRegex(BuildError, "configuration captured"):
                    build.compile(Path(temporary) / "package")
            arguments = command.call_args.args[0]
            for definition in ("CMAKE_SYSTEM_NAME=Linux", "CMAKE_SYSTEM_PROCESSOR=aarch64",
                               "CMAKE_FIND_ROOT_PATH_MODE_PROGRAM=NEVER", "CMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY",
                               "CMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY"):
                self.assertIn(f"-D{definition}", arguments)
            self.assertIn(f"-DCMAKE_SYSROOT={environment['ONNXRUNTIME_SYSROOT']}", arguments)
            assembler = next(argument for argument in arguments if argument.startswith("-DCMAKE_ASM_COMPILER_ARG1="))
            self.assertIn("--target=aarch64-linux-gnu", assembler)
            self.assertIn("-DCMAKE_ASM_COMPILER_LAUNCHER=" + ";".join(tools.cc.launcher), arguments)
            self.assertIn("-DCMAKE_LINKER_TYPE=nervix_lld", arguments)
            cuda_link = next(argument for argument in arguments if argument.startswith("-DCMAKE_CUDA_USING_LINKER_nervix_lld="))
            self.assertIn("--target=aarch64-linux-gnu", cuda_link)
            self.assertIn("-fuse-ld=lld", cuda_link)
            cuda_flags = next(argument for argument in arguments if argument.startswith("-DCMAKE_CUDA_FLAGS="))
            self.assertIn("--target-directory=sbsa-linux", cuda_flags)
            self.assertIn("-Xcompiler=--target=aarch64-linux-gnu", cuda_flags)

    def test_arm64_cross_smoke_links_for_arm64_and_runs_through_qemu(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            with patch.dict(os.environ, environment):
                tools = HostToolchain.discover("linux/arm64")
            build = RuntimeBuild(BuildSpec.create("linux/arm64", repo=ROOT), Path(temporary))
            destination = Path(temporary) / "package"
            construct_cuda_runtime(destination)
            with patch("scripts.build_onnxruntime.run") as command:
                build._smoke(destination, tools)
            compile = command.call_args_list[-2].args[0]
            self.assertIn("--target=aarch64-linux-gnu", compile)
            self.assertIn("-fuse-ld=lld", compile)
            execution = command.call_args.args[0]
            self.assertEqual(execution[:3], ["/host/bin/qemu-aarch64", "-L", environment["ONNXRUNTIME_SYSROOT"]])
            self.assertIn(f"LD_LIBRARY_PATH={destination}/runtime/lib", execution[4])
            self.assertEqual(execution[-1], "cuda-load")

    def test_cross_sdk_requires_an_existing_sysroot(self) -> None:
        with patch.dict(os.environ, {"ONNXRUNTIME_SYSROOT": "/unavailable-arm64-sysroot"}):
            with patch("scripts.onnxruntime.toolchain.Compiler.discover") as compiler:
                with self.assertRaisesRegex(BuildError, "ONNXRUNTIME_SYSROOT"):
                    HostToolchain.discover("linux/arm64")
        compiler.assert_not_called()

    def test_cross_sdk_requires_sbsa_cuda_headers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            (Path(environment["CUDA_HOME"]) / "targets/sbsa-linux/include/cuda_runtime.h").unlink()
            with patch.dict(os.environ, environment):
                with self.assertRaisesRegex(BuildError, "CUDA 13 SBSA SDK"):
                    HostToolchain.discover("linux/arm64")

    def test_cross_sdk_requires_the_arm64_cudnn_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            environment["CUDNN_HOME"] = ""
            with patch.dict(os.environ, environment):
                with self.assertRaisesRegex(BuildError, "CUDNN_HOME must select an arm64"):
                    HostToolchain.discover("linux/arm64")

    def test_cross_cmake_requires_llvm_linker_selection_support(self) -> None:
        def version(command: list[str], **kwargs: object) -> str:
            return "cmake version 3.28.0" if command[0] == "cmake" else tool_output(command)

        with tempfile.TemporaryDirectory() as temporary:
            with patch.dict(os.environ, self.cross_sdk(Path(temporary))):
                with patch("scripts.onnxruntime.toolchain.subprocess.check_output", side_effect=version):
                    with self.assertRaisesRegex(BuildError, "CMake 3.29"):
                        HostToolchain.discover("linux/arm64")

    def test_cross_sdk_supports_debian_cross_package_loader_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            environment = self.cross_sdk(Path(temporary))
            loader = Path(environment["ONNXRUNTIME_SYSROOT"]) / "usr/aarch64-linux-gnu/lib/ld-linux-aarch64.so.1"
            loader.parent.mkdir(parents=True)
            loader.write_bytes(b"target loader")
            with patch.dict(os.environ, environment):
                tools = HostToolchain.discover("linux/arm64")
            self.assertEqual(tools.cross.runtime_root, loader.parent.parent)
            self.assertIn(f"CMAKE_CROSSCOMPILING_EMULATOR=/host/bin/qemu-aarch64;-L;{loader.parent.parent}",
                          tools.cross.definitions(tools.cuda_home, tools.cudnn_home, tools.cuda_host))

    def test_ci_does_not_resolve_a_compilation_toolchain(self) -> None:
        with patch.dict(os.environ, {"CI": "true"}):
            with patch("scripts.onnxruntime.toolchain.Compiler.discover") as compiler:
                for target in ("linux/amd64", "linux/arm64"):
                    with self.subTest(target=target), self.assertRaisesRegex(BuildError, "compilation is disabled in CI"):
                        HostToolchain.discover(target)
        compiler.assert_not_called()

    def test_macos_uses_the_apple_sdk_and_host_archive_tools(self) -> None:
        with patch("scripts.onnxruntime.toolchain.platform.system", return_value="Darwin"):
            with patch("scripts.onnxruntime.toolchain.platform.machine", return_value="arm64"):
                tools = HostToolchain.discover("darwin/arm64")
        self.assertEqual(tools.cc.executable, "/Xcode/bin/clang")
        self.assertEqual(tools.cxx.executable, "/Xcode/bin/clang++")
        self.assertEqual(tools.sdk, "/Xcode/SDK")
        self.assertEqual(tools.aggregate, ["/Xcode/bin/libtool", "-static"])
        self.assertEqual(tools.metadata["sdk_version"], "15.0")

    def test_cuda_uses_the_host_toolkit_and_llvm_host_compiler(self) -> None:
        with patch.dict(os.environ, {"CC": "kache clang-22", "CXX": "kache clang++-22",
                                   "CUDA_HOME": "/native/cuda", "CUDNN_HOME": "/native/cudnn",
                                   "CUDACXX": "kache /native/cuda/bin/nvcc", "CUDAHOSTCXX": "clang++-20"}):
            tools = HostToolchain.discover("linux/amd64")
        self.assertEqual(tools.cuda.command, ["/host/bin/kache", "/native/cuda/bin/nvcc"])
        self.assertEqual(tools.cuda_host.executable, "/host/bin/clang++-20")
        self.assertEqual(tools.cuda_home, Path("/native/cuda"))
        self.assertEqual(tools.cudnn_home, Path("/native/cudnn"))

    def test_linux_arm64_uses_the_cuda_toolkit(self) -> None:
        with patch("scripts.onnxruntime.toolchain.platform.machine", return_value="aarch64"):
            with patch.dict(os.environ, {"CUDA_HOME": "/native/cuda", "CUDAHOSTCXX": "clang++-21"}):
                tools = HostToolchain.discover("linux/arm64")
        self.assertEqual(tools.cuda.executable, "/native/cuda/bin/nvcc")
        self.assertEqual(tools.cuda_host.executable, "/host/bin/clang++-21")

    def test_artifact_key_is_shared_across_host_toolchains(self) -> None:
        with patch.dict(os.environ, {"CC": "kache clang-22", "CXX": "kache clang++-22"}):
            first = BuildSpec.create("linux/amd64", repo=ROOT)
        with patch.dict(os.environ, {"CC": "clang-23", "CXX": "clang++-23"}):
            second = BuildSpec.create("linux/amd64", repo=ROOT)
        self.assertEqual(first.object_key, second.object_key)

    @unittest.skipUnless(which("cmake") and which("ninja") and CLANG_CXX,
                         "requires CMake, Ninja, and installed Clang")
    def test_release_build_reports_warnings_and_rejects_compiler_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            build = RuntimeBuild(BuildSpec.create("linux/amd64", repo=ROOT), Path(temporary))
            tools = HostToolchain.discover("linux/amd64")
            build.tools = replace(tools, cxx=replace(tools.cxx, executable=CLANG_CXX))
            source = build.source_dir / "cmake"
            source.mkdir(parents=True)
            (source / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.29)\n"
                "project(nervix_warning_probe LANGUAGES CXX)\n"
                "add_library(vendor OBJECT warning.cc)\n"
                "set_property(TARGET vendor PROPERTY COMPILE_WARNING_AS_ERROR ON)\n"
            )
            warning = source / "warning.cc"
            warning.write_text('#warning "nervix_vendor_warning"\nint value = 1;\n')
            with patch("scripts.build_onnxruntime.run", side_effect=BuildError("configuration captured")) as command:
                with self.assertRaisesRegex(BuildError, "configuration captured"):
                    build.compile(Path(temporary) / "package")
            # This fixture owns its target graph; retain the producer's other configure options.
            configure = [argument for argument in command.call_args.args[0]
                         if not argument.startswith("-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES=")]
            result = subprocess.run(configure, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            compile = ["cmake", "--build", str(build.build_dir)]
            result = subprocess.run(compile, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("nervix_vendor_warning", result.stdout + result.stderr)
            warning.write_text('#error "nervix_compile_error"\n')
            result = subprocess.run(compile, capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("nervix_compile_error", result.stdout + result.stderr)

    def test_cuda_host_sources_are_checked_before_compiling_gpu_kernels(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            build = RuntimeBuild(BuildSpec.create("linux/amd64", repo=ROOT), Path(temporary))
            build.tools = HostToolchain.discover("linux/amd64")
            host_object = "CMakeFiles/onnxruntime_providers_cuda.dir/attention.cc.o"
            device_object = "CMakeFiles/onnxruntime_providers_cuda.dir/attention.cu.o"
            targets = f"{host_object}: CXX_COMPILER\n{device_object}: CUDA_COMPILER\n"

            def compile_command(command: list[str], **kwargs: object) -> None:
                if command[:2] == ["cmake", "--build"]:
                    self.assertIn(host_object, command)
                    raise BuildError("host compilation failed")

            with patch("scripts.build_onnxruntime.subprocess.check_output", return_value=targets) as query:
                with patch("scripts.build_onnxruntime.run", side_effect=compile_command) as compile:
                    with self.assertRaisesRegex(BuildError, "host compilation failed"):
                        build.compile(Path(temporary) / "package")
            query.assert_called_once_with(["ninja", "-C", str(build.build_dir), "-t", "targets", "all"], text=True)
            self.assertEqual(compile.call_count, 2)

    def test_native_build_records_provenance_and_uses_the_selected_cmake_compilers(self) -> None:
        for target, system, machine in (("linux/amd64", "Linux", "x86_64"),
                                        ("linux/arm64", "Linux", "aarch64"),
                                        ("darwin/arm64", "Darwin", "arm64")):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as temporary:
                build = RuntimeBuild(BuildSpec.create(target, repo=ROOT), Path(temporary))
                source = build.source_dir / "include/onnxruntime/core/session"
                source.mkdir(parents=True)
                (source / "onnxruntime_c_api.h").write_text("fixture header")
                for name in ("LICENSE", "ThirdPartyNotices.txt"):
                    (build.source_dir / name).write_text("fixture notice")
                destination = Path(temporary) / "package"
                destination.mkdir()

                def run_command(command: list[str], **kwargs: object) -> None:
                    if command[:2] == ["cmake", "--build"]:
                        (build.build_dir / "nervix-archives.txt").write_text("/fixture/a library.a\n")
                    elif command[0] == "/host/bin/llvm-ar":
                        self.assertEqual(command[1:3], ["qcLs", str(destination / "lib/libonnxruntime.a")])
                        self.assertEqual(command[3:], ["/fixture/a library.a"])
                        (destination / "lib/libonnxruntime.a").write_bytes(b"!<arch>\nfixture")
                    elif command[0] == "/Xcode/bin/libtool":
                        self.assertEqual(command[1:4], ["-static", "-o", str(destination / "lib/libonnxruntime.a")])
                        (destination / "lib/libonnxruntime.a").write_bytes(b"!<arch>\nfixture")

                with patch("scripts.onnxruntime.toolchain.platform.system", return_value=system):
                    with patch("scripts.onnxruntime.toolchain.platform.machine", return_value=machine):
                        with patch.object(build, "_source"), patch.object(build, "_smoke") as smoke:
                            with patch.object(build, "_cuda_host_sources"), patch.object(
                                build, "_cuda_runtime", side_effect=lambda destination, tools: construct_cuda_runtime(destination)
                            ), patch("scripts.build_onnxruntime.run", side_effect=run_command) as command:
                                build._build(destination)
                                build._seal(destination)
                build.validate_package(destination)
                cmake = command.call_args_list[0].args[0]
                self.assertIn(f"-DCMAKE_C_COMPILER={build.tools.cc.executable}", cmake)
                self.assertIn(f"-DCMAKE_CXX_COMPILER={build.tools.cxx.executable}", cmake)
                self.assertEqual(build.provenance["cc"]["version"], "clang version 22.1.0")
                if system == "Darwin":
                    self.assertIn("-DCMAKE_OSX_SYSROOT=/Xcode/SDK", cmake)
                    self.assertIn("-DCMAKE_OSX_DEPLOYMENT_TARGET=14.0", cmake)
                else:
                    self.assertIn("-Donnxruntime_USE_CUDA=ON", cmake)
                    self.assertIn(f"-DCMAKE_CUDA_COMPILER={build.tools.cuda.executable}", cmake)
                smoke.assert_called_once_with(destination, build.tools)


if __name__ == "__main__":
    unittest.main()
