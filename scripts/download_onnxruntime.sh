#!/usr/bin/env bash
set -euo pipefail

version="${ONNXRUNTIME_VERSION:-1.24.2}"
root="${NERVIX_ONNXRUNTIME_DIR:-.nervix-deps/onnxruntime}"
mode="${1:-download}"

case "$(uname -s)" in
    Linux)
        os="linux"
        dylib="lib/libonnxruntime.so"
        ;;
    Darwin)
        os="osx"
        dylib="lib/libonnxruntime.dylib"
        ;;
    *)
        echo "unsupported ONNX Runtime host OS: $(uname -s)" >&2
        exit 1
        ;;
esac

case "$(uname -m)" in
    x86_64|amd64)
        arch="x64"
        ;;
    aarch64|arm64)
        arch="aarch64"
        ;;
    *)
        echo "unsupported ONNX Runtime host architecture: $(uname -m)" >&2
        exit 1
        ;;
esac

package="onnxruntime-${os}-${arch}-${version}"
install_dir="${root}/${package}"
dylib_path="${install_dir}/${dylib}"

if [[ "${mode}" == "--print-path" ]]; then
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
echo "downloading ONNX Runtime ${version} for ${os}/${arch}" >&2
curl -L --proto '=https' --tlsv1.2 -sSf "${url}" -o "${tmp_dir}/${archive}"
mkdir -p "${install_dir}"
tar -xzf "${tmp_dir}/${archive}" -C "${install_dir}" --strip-components=1

if [[ ! -f "${dylib_path}" ]]; then
    echo "downloaded ONNX Runtime but did not find ${dylib_path}" >&2
    exit 1
fi

printf '%s\n' "${dylib_path}"
