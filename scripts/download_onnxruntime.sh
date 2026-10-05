#!/usr/bin/env bash
set -euo pipefail

# ort rc.13's highest supported C API is 28; package a current runtime providing that API.
version="1.30.0"
root="${PWD}/.nervix-deps/onnxruntime"
mode="download"
platform="host"
flavor="cpu"

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --print-path)
            mode="print-path"
            shift
            ;;
        --platform|--flavor)
            if [[ "$#" -lt 2 || -z "$2" ]]; then
                echo "$1 requires a value" >&2
                exit 1
            fi
            case "$1" in
                --platform) platform="$2" ;;
                --flavor) flavor="$2" ;;
            esac
            shift 2
            ;;
        *)
            echo "unknown argument: $1; expected --flavor, --platform or --print-path" >&2
            exit 1
            ;;
    esac
done

# Docker downloads for its target while running on the build host. Local recipes select the
# whole host platform, using upstream's architecture spellings for each operating system.
if [[ "${platform}" == "host" ]]; then
    case "$(uname -s)/$(uname -m)" in
        Linux/x86_64) platform="linux-x64" ;;
        Linux/aarch64) platform="linux-aarch64" ;;
        Darwin/arm64) platform="osx-arm64" ;;
        *)
            echo "ONNX Runtime publishes no release build for $(uname -s)/$(uname -m); Nervix is" \
                "developed on Linux x86_64, Linux aarch64, and macOS arm64" >&2
            exit 1
            ;;
    esac
fi

case "${platform}/${flavor}" in
    linux-x64/cpu|linux-aarch64/cpu|linux-x64/gpu_cuda13)
        dylib="lib/libonnxruntime.so" ;;
    osx-arm64/cpu)
        dylib="lib/libonnxruntime.dylib" ;;
    *)
        echo "unsupported ONNX Runtime package: ${platform}/${flavor}; CUDA 13 requires linux-x64" >&2
        exit 1
        ;;
esac

package="onnxruntime-${platform}"
if [[ "${flavor}" != "cpu" ]]; then
    package="${package}-${flavor}"
fi
package="${package}-${version}"
install_dir="${root}/${package}"
dylib_path="${install_dir}/${dylib}"

if [[ "${mode}" == "print-path" ]]; then
    printf '%s\n' "${dylib_path}"
    exit 0
fi

if [[ -f "${dylib_path}" ]]; then
    printf '%s\n' "${dylib_path}"
    exit 0
fi

archive="${package}.tgz"
url="https://github.com/microsoft/onnxruntime/releases/download/v${version}/${archive}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

mkdir -p "${root}"
echo "downloading ONNX Runtime ${version} for ${platform}/${flavor}" >&2
if ! curl -L --proto '=https' --tlsv1.2 -sSf "${url}" -o "${tmp_dir}/${archive}"; then
    echo "failed to download ${url}; check that release v${version} publishes ${archive}" >&2
    exit 1
fi
mkdir -p "${install_dir}"
tar -xzf "${tmp_dir}/${archive}" -C "${install_dir}" --strip-components=1

if [[ ! -f "${dylib_path}" ]]; then
    echo "downloaded ONNX Runtime but did not find ${dylib_path}" >&2
    exit 1
fi

printf '%s\n' "${dylib_path}"
