#!/usr/bin/env bash
set -euo pipefail

version="${ONNXRUNTIME_VERSION:-1.24.2}"
root="${NERVIX_ONNXRUNTIME_DIR:-.nervix-deps/onnxruntime}"
mode="${1:-download}"

# The recipes export this path as ORT_DYLIB_PATH, which is dlopen'd from whatever directory the
# process later runs in, so the path leaves this script absolute.
case "${root}" in
    /*) ;;
    *) root="${PWD}/${root}" ;;
esac

# Upstream names each release asset for a whole platform, not for an OS and an architecture chosen
# independently: the same architecture is spelled `aarch64` on Linux and `arm64` on macOS. Deciding
# the pair at once keeps combinations upstream never published, such as macOS x86_64, out of the
# URL this builds.
case "$(uname -s)/$(uname -m)" in
    Linux/x86_64)
        platform="linux-x64"
        dylib="lib/libonnxruntime.so"
        ;;
    Linux/aarch64)
        platform="linux-aarch64"
        dylib="lib/libonnxruntime.so"
        ;;
    Darwin/arm64)
        platform="osx-arm64"
        dylib="lib/libonnxruntime.dylib"
        ;;
    *)
        echo "ONNX Runtime publishes no release build for $(uname -s)/$(uname -m); Nervix is" \
            "developed on Linux x86_64, Linux aarch64, and macOS arm64" >&2
        exit 1
        ;;
esac

package="onnxruntime-${platform}-${version}"
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
echo "downloading ONNX Runtime ${version} for ${platform}" >&2
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
