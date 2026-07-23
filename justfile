rust_toolchain_version := shell("toml get -r rust-toolchain.toml toolchain.channel")
build_mode := "debug"
release_flag := if build_mode == "release" { "--release" } else { "" }
cargo_target_dir := env("CARGO_TARGET_DIR", justfile_directory() + "/target")

proto-fmt:
    buf format -w

proto-fmt-check:
    buf format --exit-code

proto-lint:
    buf lint

test-loom: wasm-processor-guests
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    RUSTFLAGS="--cfg runtime_ack_loom" cargo test -q --lib loom_tests

build-deps: generate-test-onnx download-onnxruntime build-web-console wasm-processor-guests

tests-deps: build-deps

test: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --all-targets --all-features --features testing --workspace

test-scenarios *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --features testing --test scenarios -- {{ args }}

test-lib *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --features testing --lib -- {{ args }}

test-coverage: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov --all-targets --all-features --features testing --workspace --lcov --output-path lcov.info
    cargo crap --lcov lcov.info

cargo-fmt:
    cargo +nightly fmt

taplo-format:
    taplo format

[parallel]
fmt: cargo-fmt taplo-format dockerfmt proto-fmt gherkin-fmt autoinherit

cargo-fmt-check:
    cargo +nightly fmt --check

taplo-format-check:
    taplo format --check

[parallel]
fmt-check: cargo-fmt-check taplo-format-check dockerfmt-check proto-fmt-check gherkin-fmt-check autoinherit-check

gherkin-fmt:
    ghokin fmt replace tests/features

gherkin-fmt-check:
    ghokin check tests/features

autoinherit:
    cargo autoinherit

autoinherit-check:
    #!/bin/bash
    set -e
    git diff --exit-code 2>/dev/null >/dev/null  || echo "skip autoinherit on dirty working tree" && exit 0
    cargo autoinherit
    git diff --exit-code

cargo-clippy:
    cargo clippy --all-features --all-targets --workspace

[parallel]
lint-inner: cargo-clippy proto-lint

lint: build-web-console proto-lint

audit:
    cargo audit

validate: fmt lint validate-skill

validate-ci: fmt-check lint validate-skill

test-docs:
    uv run --locked python -m unittest discover -s scripts/tests -p "test_*.py"
    node --test cloudflare/docs-worker/src/index.test.js

book version="": test-docs
    python scripts/build_book.py --version {{ version }}

validate-skill:
    env GH_PROMPT_DISABLED=1 gh skill publish .agents/skills --dry-run

book-pdf version="" output="":
    #!/usr/bin/env bash
    set -euo pipefail
    just book "{{ version }}"
    if ! command -v pandoc >/dev/null 2>&1; then
        echo "pandoc is required for book-pdf" >&2
        exit 1
    fi
    if [[ -n "{{ output }}" ]]; then
        output_path="{{ output }}"
    elif [[ -n "{{ version }}" ]]; then
        output_path="docs/book/nervix-book-{{ version }}.pdf"
    else
        output_path="docs/book/nervix-book.pdf"
    fi
    tmp_html="$(mktemp --suffix=.html)"
    trap 'rm -f "${tmp_html}"' EXIT
    perl -0ne '
        if (m{<main>(.*)</main>}s) {
            print "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"></head><body><main>$1</main></body></html>";
        } else {
            die "failed to extract <main> from docs/book/print.html\n";
        }
    ' docs/book/print.html > "${tmp_html}"
    pandoc \
        --from=html \
        --to=pdf \
        --resource-path=docs/book \
        --output="${output_path}" \
        "${tmp_html}"
    echo "${output_path}"

book-upload prefix="snapshot" bucket="nervix-docs":
    uv run --locked python scripts/upload_book_to_r2.py --bucket {{ bucket }} --prefix {{ prefix }}

publish-dir source target zone_id alias="snapshot" bucket="nervix-docs":
    uv run --locked python scripts/publish_docs_alias.py --source {{ source }} --target {{ target }} --alias {{ alias }} --bucket {{ bucket }} --zone-id {{ zone_id }}

purge-cache zone_id:
    python scripts/purge_cloudflare_cache.py --zone-id {{ zone_id }}

worker-deploy zone_id="":
    #!/usr/bin/env bash
    set -euo pipefail
    npx --yes wrangler deploy --config cloudflare/docs-worker/wrangler.jsonc
    if [[ -n "{{ zone_id }}" ]]; then
        python scripts/purge_cloudflare_cache.py --zone-id {{ zone_id }}
    fi

publish-book target zone_id alias="snapshot" bucket="nervix-docs":
    just book {{ target }}
    just publish-dir docs/book {{ target }} {{ zone_id }} {{ alias }} {{ bucket }}
    just worker-deploy {{ zone_id }}

deps:
    just generate-dev-tls
    docker compose up -d --build --wait --wait-timeout 90
    docker compose run --rm fake-gcs-init
    docker compose run --rm azurite-init

deps-down:
    docker compose down --remove-orphans --volumes

server *args: build-deps
    cargo run -- {{ args }}

client *args: build-deps
    cargo run --package nervix-cli -- {{ args }}

build-web-console:
    #!/usr/bin/env bash
    set -euo pipefail
    cd crates/web-console
    env -u NO_COLOR trunk build --release

build-server:
    CARGO_TARGET_DIR={{cargo_target_dir}}/server cargo build {{release_flag}} --package nervix --bin nervix

build-cli:
    CARGO_TARGET_DIR={{cargo_target_dir}}/cli cargo build {{release_flag}} --package nervix-cli --bin nervix-cli

[parallel]
build-apps: build-cli build-server

build-local-dashboard: build-deps build-apps

wasm-processor-rust-guest:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build \
        --manifest-path examples/wasm-processors/rust-guest/Cargo.toml \
        --target wasm32-unknown-unknown \
        --release
    test -s examples/wasm-processors/rust-guest/target/wasm32-unknown-unknown/release/nervix_wasm_processor_rust_guest.wasm

download-datalake-dbip:
    #!/usr/bin/env bash
    set -euo pipefail
    url="https://download.db-ip.com/free/dbip-city-lite-2026-06.mmdb.gz"
    destination="examples/datalake/geo-wasm-guest/dbip-city-lite-2026-06.mmdb.gz"
    if [[ -s "${destination}" ]]; then
        exit 0
    fi
    mkdir -p "$(dirname "${destination}")"
    tmp="$(mktemp "${destination}.tmp.XXXXXX")"
    trap 'rm -f "${tmp}"' EXIT
    curl -L --proto '=https' --tlsv1.2 -sSf "${url}" -o "${tmp}"
    mv "${tmp}" "${destination}"

wasm-datalake-geo-guest: download-datalake-dbip
    #!/usr/bin/env bash
    set -euo pipefail
    artifact="examples/datalake/geo-wasm-guest/target/wasm32-unknown-unknown/release/nervix_datalake_geo_wasm_guest.wasm"
    resource_dir="examples/datalake/geo-wasm-guest/resource"
    cargo build \
        --manifest-path examples/datalake/geo-wasm-guest/Cargo.toml \
        --target wasm32-unknown-unknown \
        --release
    test -s "${artifact}"
    rm -rf "${resource_dir}"
    mkdir -p "${resource_dir}"
    cp "${artifact}" "${resource_dir}/"
    test "$(find "${resource_dir}" -type f | wc -l)" -eq 1

provision-datalake-iceberg:
    docker compose run --rm -e ICEBERG_INIT_HOLD=false datalake-iceberg-init

duckdb-datalake:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v duckdb >/dev/null 2>&1; then
        echo "duckdb is required to query the datalake Iceberg tables" >&2
        exit 127
    fi
    duckdb :memory: -init examples/datalake/duckdb_iceberg.sql

wasm-processor-go-guest:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v tinygo >/dev/null 2>&1; then
        echo "tinygo is required to build the native non-WASI Go WASM guest" >&2
        echo "standard Go only supports js/wasm and wasip1/wasm outputs" >&2
        exit 127
    fi
    cd examples/wasm-processors/go-guest
    tinygo build \
        -target=wasm-unknown \
        -scheduler=none \
        -opt=z \
        -panic=trap \
        -no-debug \
        -o nervix_wasm_processor_go_guest.wasm \
        .
    test -s nervix_wasm_processor_go_guest.wasm

[parallel]
wasm-processor-guests: wasm-processor-rust-guest wasm-processor-go-guest

generate-dev-tls:
    #!/usr/bin/env bash
    set -euo pipefail
    bash scripts/generate_dev_tls.sh

generate-test-onnx output="tests/fixtures/onnx/simple_score.onnx" batch_output="tests/fixtures/onnx/batch_score.onnx" f64_output="tests/fixtures/onnx/f64_score.onnx" matrix_output="tests/fixtures/onnx/matrix_identity.onnx" dynamic_batch_output="tests/fixtures/onnx/dynamic_batch_score.onnx":
    python3 scripts/train_simple_onnx.py --output {{ output }} --batch-output {{ batch_output }} --f64-output {{ f64_output }} --matrix-output {{ matrix_output }} --dynamic-batch-output {{ dynamic_batch_output }}

download-onnxruntime:
    bash scripts/download_onnxruntime.sh

reset-local-dashboard-state:
    #!/usr/bin/env bash
    set -euo pipefail
    rm -rf .nervix-db

cluster-dashboard: generate-dev-tls build-local-dashboard
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .nervix-db/node1 .nervix-db/node2 .nervix-db/node3
    dashboard_password="${NERVIX_PASSWORD:-${NERVIX_INIT_DEFAULT_USER_PASSWORD:-nervix}}"
    export NERVIX_PASSWORD="${dashboard_password}"
    export NERVIX_INIT_DEFAULT_USER_PASSWORD="${NERVIX_INIT_DEFAULT_USER_PASSWORD:-${dashboard_password}}"
    target_dir="${CARGO_TARGET_DIR:-target}"
    case "${target_dir}" in
        /*) ;;
        *) target_dir="${PWD}/${target_dir}" ;;
    esac
    cli_bin_dir="${target_dir}/cli/{{build_mode}}"
    server_bin_dir="${target_dir}/server/{{build_mode}}"
    export PATH="${cli_bin_dir}:${server_bin_dir}:${PATH}"
    exec zellij --layout .zellij/layouts/local-3-nodes.kdl

dockerfmt:
    #!/usr/bin/env bash
    set -euo pipefail
    for file in Dockerfile*; do
        tmp="$(mktemp)"
        dockerfmt < "${file}" > "${tmp}"
        mv "${tmp}" "${file}"
    done

dockerfmt-check:
    #!/usr/bin/env bash
    set -euo pipefail
    failed=0
    for file in Dockerfile*; do
        tmp="$(mktemp)"
        dockerfmt < "${file}" > "${tmp}"
        if ! cmp -s "${file}" "${tmp}"; then
            echo "dockerfmt check failed for ${file}"
            diff -u "${file}" "${tmp}" || true
            failed=1
        fi
        rm -f "${tmp}"
    done
    exit "${failed}"

docker-prepare-qemu platform="linux/amd64":
    #!/usr/bin/env bash
    set -euo pipefail
    normalized_platform="{{ platform }}"
    if [[ "${normalized_platform}" == "linux/aarch64" ]]; then
        normalized_platform="linux/arm64"
    fi
    host_arch="$(uname -m)"
    case "${host_arch}" in
        x86_64) host_platform="linux/amd64" ;;
        aarch64|arm64) host_platform="linux/arm64" ;;
        *) host_platform="" ;;
    esac
    if [[ -n "${host_platform}" && "${normalized_platform}" != "${host_platform}" ]]; then
        docker run --privileged --rm tonistiigi/binfmt --install all
    fi

docker-build-debian debian_version="trixie" llvm_version="22" tag="nervix:debian" platform="linux/amd64" push="false" cache_from="" cache_to="":
    #!/usr/bin/env bash
    set -euo pipefail
    normalized_platform="{{ platform }}"
    if [[ "${normalized_platform}" == "linux/aarch64" ]]; then
        normalized_platform="linux/arm64"
    fi
    just docker-prepare-qemu "${normalized_platform}"
    output_flag="--load"
    if [[ "{{ push }}" == "true" ]]; then
        output_flag="--push"
    fi
    cache_from_flag=""
    if [[ -n "{{ cache_from }}" ]]; then
        cache_from_flag="--cache-from={{ cache_from }}"
    fi
    cache_to_flag=""
    if [[ -n "{{ cache_to }}" ]]; then
        cache_to_flag="--cache-to={{ cache_to }}"
    fi
    docker buildx build \
        -f Dockerfile.debian \
        --progress=plain \
        --platform "${normalized_platform}" \
        --build-arg RUST_VERSION={{ rust_toolchain_version }} \
        --build-arg DEBIAN_VERSION={{ debian_version }} \
        --build-arg LLVM_VERSION={{ llvm_version }} \
        ${cache_from_flag} \
        ${cache_to_flag} \
        -t {{ tag }} \
        "${output_flag}" \
        .

docker-build-alpine alpine_version="3.23" llvm_version="21" tag="nervix:alpine" platform="linux/amd64" push="false" cache_from="" cache_to="":
    #!/usr/bin/env bash
    set -euo pipefail
    normalized_platform="{{ platform }}"
    if [[ "${normalized_platform}" == "linux/aarch64" ]]; then
        normalized_platform="linux/arm64"
    fi
    just docker-prepare-qemu "${normalized_platform}"
    output_flag="--load"
    if [[ "{{ push }}" == "true" ]]; then
        output_flag="--push"
    fi
    cache_from_flag=""
    if [[ -n "{{ cache_from }}" ]]; then
        cache_from_flag="--cache-from={{ cache_from }}"
    fi
    cache_to_flag=""
    if [[ -n "{{ cache_to }}" ]]; then
        cache_to_flag="--cache-to={{ cache_to }}"
    fi
    docker buildx build \
        -f Dockerfile.alpine \
        --progress=plain \
        --platform "${normalized_platform}" \
        --build-arg RUST_VERSION={{ rust_toolchain_version }} \
        --build-arg ALPINE_VERSION={{ alpine_version }} \
        --build-arg LLVM_VERSION={{ llvm_version }} \
        ${cache_from_flag} \
        ${cache_to_flag} \
        -t {{ tag }} \
        "${output_flag}" \
        .

kube-deps:
    bash scripts/kube_deps.sh

kube-deps-down:
    bash scripts/kube_deps_down.sh

kube-app:
    bash scripts/kube_app.sh

kube-app-down:
    bash scripts/kube_app_down.sh

kube-cli:
    bash scripts/kube_cli.sh

kube-cli-command command:
    bash scripts/kube_cli.sh --command "{{ command }}"
