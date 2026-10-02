set minimum-version := "1.56.0"
set unstable
set lists

export RUSTUP_AUTO_INSTALL := "0"

rust_toolchain_version := shell("toml get -r rust-toolchain.toml toolchain.channel")
rustflags := env('RUSTFLAGS', '')
build_mode := "debug"
release_flag := if build_mode == "release" { "--release" } else { "" }
cargo_target_dir := env("CARGO_TARGET_DIR", justfile_directory() + "/target")
turmoil_failures := cargo_target_dir + "/turmoil-failures"
export NERVIX_ONNXRUNTIME_DIR := env("NERVIX_ONNXRUNTIME_DIR", env("HOME") + "/.cache/nervix-build/onnxruntime")
export ORT_LIB_PATH := shell("python3 -m scripts.onnxruntime.artifacts path --unchecked --stage " + quote(NERVIX_ONNXRUNTIME_DIR))
export ORT_PREFER_DYNAMIC_LINK := "0"
default_jobs := num_jobs() || "4"

# Show the documented recipes, including the Bolero property and fuzz commands.
help:
    just --list

# Exercise ONNX artifact preparation and its local and R2 cache failure paths.
test-onnxruntime-tooling:
    uv run --locked python -m unittest scripts.tests.test_build_onnxruntime scripts.tests.test_onnxruntime_toolchain scripts.tests.test_onnxruntime_bootstrap scripts.tests.test_onnxruntime_recipes
# Start one bounded invocation and retain the caller's repository variable overrides.
[private]
run-with-jobs recipe jobs:
    {{ quote(just_executable()) }} --justfile {{ quote(justfile()) }} --jobs {{ quote(jobs) }} \
        rust_toolchain_version={{ quote(rust_toolchain_version) }} \
        rustflags={{ quote(rustflags) }} \
        build_mode={{ quote(build_mode) }} \
        release_flag={{ quote(release_flag) }} \
        cargo_target_dir={{ quote(cargo_target_dir) }} \
        turmoil_failures={{ quote(turmoil_failures) }} \
        {{ quote(recipe) }}

# Install the qualified CLI version with only the libFuzzer engine.
install-cargo-bolero:
    cargo install --locked --version 0.13.4 --no-default-features --features libfuzzer cargo-bolero

# Run all registered properties with bounded randomized cases and source-adjacent corpus replay.
test-bolero filter="": fetch-onnxruntime build-web-console
    python3 scripts/bolero.py test {{ quote(filter) }}

# List every compiled, registered Bolero target after checking the inventory.
fuzz-list: fetch-onnxruntime
    python3 scripts/bolero.py list

# Run one target through sanitizer-backed libFuzzer. Duration is in seconds.
fuzz target duration="30": fetch-onnxruntime
    python3 scripts/bolero.py fuzz {{ quote(target) }} {{ quote(duration) }}

# Run every target through sanitizer-backed libFuzzer. Duration is per target in seconds.
fuzz-all duration="30": fetch-onnxruntime
    python3 scripts/bolero.py fuzz-all {{ quote(duration) }}

# Replay the exact saved input through its ordinary property assertion.
fuzz-replay target failure: fetch-onnxruntime
    python3 scripts/bolero.py replay {{ quote(target) }} {{ quote(failure) }}

# Minimize a saved failure with libFuzzer and verify the minimized input still fails.
fuzz-reduce target failure: fetch-onnxruntime
    python3 scripts/bolero.py reduce {{ quote(target) }} {{ quote(failure) }}

# Compare the inventory, package declarations, test harness and compiled Bolero targets.
validate-bolero: fetch-onnxruntime
    python3 scripts/bolero.py validate

# Check the dedicated Bolero workflow with the pinned Actions linter.
validate-bolero-workflow: (validate-workflows ".github/workflows/bolero.yaml")

# Check selected Actions workflows, or all workflows when no paths are supplied.
validate-workflows *workflows:
    go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.7 {{ workflows }}

# Qualify nonzero failures, saved crashes, minimization, exact replay and case timeouts.
qualify-bolero: fetch-onnxruntime
    python3 scripts/bolero.py qualify

# Exercise the inventory and runner's validation and failure paths.
test-bolero-runner: build-web-console
    python3 -m unittest scripts.tests.test_bolero

# Measure runner edits without launching unrelated product fuzz campaigns.
coverage-bolero-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/bolero
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file target/bolero/runner.coverage --branch --source=scripts.bolero,scripts.tests.test_bolero -m unittest scripts.tests.test_bolero
    "${coverage[@]}" lcov --data-file target/bolero/runner.coverage -o target/bolero/python-runner.lcov

# Collect runner line coverage while exercising real libFuzzer and its failure qualification.
# The duration is per product target; CI passes 30 on PRs labeled `fuzz`.
coverage-bolero duration="2": fetch-onnxruntime build-web-console
    #!/usr/bin/env bash
    set -euo pipefail
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" erase
    "${coverage[@]}" run --branch --source=scripts.bolero -m unittest scripts.tests.test_bolero
    "${coverage[@]}" run --branch -a scripts/bolero.py test
    "${coverage[@]}" run --branch -a scripts/bolero.py fuzz-all {{ quote(duration) }}
    "${coverage[@]}" run --branch -a scripts/bolero.py qualify
    mkdir -p target/bolero
    "${coverage[@]}" lcov -o target/bolero/python.lcov
    "${coverage[@]}" report --fail-under=80

# Fetch published ONNX Runtime and build Nervix's test programs, fixtures, and assets.
tests-deps: fetch-onnxruntime generate-test-onnx build-web-console wasm-processor-guests build-nspl-format build-test-cli

build-test-cli:
    CARGO_TARGET_DIR={{ cargo_target_dir }} cargo build --package nervix-cli --bin nervix-cli

test: fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    # Execution-mode features replace primitives and are valid only inside their runner, and the
    # modes cannot be enabled together. Test the packages that own a mode with ordinary primitives
    # here; `test-shuttle`, `test-loom`, `test-turmoil` and `test-primitives` exercise the modes.
    mode_packages=(
        nervix-client-core
        'nervix-connector*'
        nervix-consensus
        nervix-execution
        nervix-interconnect
        nervix-model-harness
        nervix-primitives
        nervix-server
        nervix-wasm
    )
    workspace_exclusions=()
    for package in "${mode_packages[@]}"; do
        workspace_exclusions+=(--exclude "${package}")
    done
    cargo test --all-targets --all-features --workspace "${workspace_exclusions[@]}"
    cargo test --all-targets --features testing --package nervix-server
    cargo test --all-targets \
        --package nervix-client-core \
        --package 'nervix-connector*' \
        --package nervix-consensus \
        --package nervix-execution \
        --package nervix-interconnect \
        --package nervix-model-harness \
        --package nervix-wasm
    cargo test --all-targets --features native --package nervix-primitives
    just test-capability-docs
    just test-turmoil

# Check capability examples with rustdoc, including runtime exports enabled only for tests.
test-capability-docs: fetch-onnxruntime build-web-console
    cargo test --package nervix-server --package nervix-roto --features nervix-server/testing --doc

test-scenarios *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    cargo test --features testing --test scenarios -- {{ args }}

# Focused columnar admission kernel and vocabulary tests.
test-admission-kernels *args:
    cargo test --package nervix-simd-kernels --lib -- {{ args }}
    cargo test --package nervix-models --lib -- {{ args }}

bench-window-admission *args:
    cargo bench --package nervix-simd-kernels --bench window_admission -- {{ args }}

# Measure the checked integer SIMD kernels beside the lane loop they replaced, over one 1,024-lane
# run each. Extra arguments are forwarded to Criterion.
bench-checked-lanes *args:
    cargo bench --package nervix-simd-kernels --bench checked_lanes -- {{ args }}

# Compare constant division at every width with scalar reciprocal and checked lane loops.
bench-constant-division *args:
    cargo bench --package nervix-simd-kernels --bench constant_division -- {{ args }}

bench-constant-division-x86-64-v3 *args:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/simd-kernels-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --package nervix-simd-kernels --bench constant_division -- {{ args }}

# The same measurement built for the x86-64-v3 payload the Docker image ships, in its own target
# directory: the lane loop compiles for AVX2 as the payload's does, and the kernels still select
# their level from the CPU at run time.
bench-checked-lanes-x86-64-v3 *args:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/simd-kernels-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --package nervix-simd-kernels --bench checked_lanes -- {{ args }}

test-admission-runtime *args: fetch-onnxruntime
    cargo test --package nervix-server --features testing --lib -- {{ args }}

# Focused ordinary-mode regressions for runtime owners.
test-runtime *args: fetch-onnxruntime build-web-console wasm-processor-guests
    cargo test --package nervix-server --features testing --lib -- {{ args }}

# Measure the endpoint's actual request routing and per-thread allocations on the same host.
bench-endpoint-routing: fetch-onnxruntime build-web-console
    cargo test --package nervix-server --features testing,benchmarks --lib endpoint_routing_cost -- --ignored --nocapture

test-endpoint-intake *args: fetch-onnxruntime build-web-console
    cargo test --package nervix-server --features testing --lib -- {{ args }}

test-web-console:
    CARGO_TARGET_DIR={{ cargo_target_dir }} cargo test --package nervix-web-console --bin nervix-web-console

test-harness-liveness *args: tests-deps
    cargo test --features testing --test harness_liveness -- {{ args }}

test-scenarios-reuse *args: fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export NERVIX_TESTCONTAINERS_MODE=reusable
    cargo test --features testing --test scenarios -- {{ args }}

# Measure the public client protocol against a release nervix-server process.
# The JSON report and raw Prometheus scrape record the workload, toolchain, hardware and limits.
client-wire-baseline samples="100" upload_samples="5" payload_bytes="1024" output="target/client-wire-baseline": build-web-console fetch-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release --package nervix-server --bin nervix-server
    export NERVIX_CLIENT_WIRE_BASELINE_SERVER={{ quote(cargo_target_dir + "/release/nervix-server") }}
    export NERVIX_CLIENT_WIRE_BASELINE_SERVER_PROFILE=release
    export NERVIX_CLIENT_WIRE_BASELINE_SAMPLES={{ quote(samples) }}
    export NERVIX_CLIENT_WIRE_BASELINE_UPLOAD_SAMPLES={{ quote(upload_samples) }}
    export NERVIX_CLIENT_WIRE_BASELINE_PAYLOAD_BYTES={{ quote(payload_bytes) }}
    export NERVIX_CLIENT_WIRE_BASELINE_OUTPUT={{ quote(output) }}
    cargo test --features testing --test scenarios -- \
        --input tests/features/runtime/client_wire_baseline.feature \
        --tags @client_wire_baseline \
        --concurrency 1 \
        --retry 0

# Capture the web console images the book publishes. The capture tool starts a real nervix-server,
# seeds it with nervix-cli, and drives the console in a browser, so the images are build output
# rather than repository content and the book stages them from target/.
docs-screenshots: fetch-onnxruntime build-web-console
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --package nervix-server --bin nervix-server --package nervix-cli --bin nervix-cli
    npm --prefix scripts/console-screenshots ci
    node scripts/console-screenshots/capture.mjs \
      --server "{{ cargo_target_dir }}/debug/nervix-server" \
      --cli "{{ cargo_target_dir }}/debug/nervix-cli" \
      --output "{{ cargo_target_dir }}/docs-screenshots"

test-lib *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    cargo test --features testing --lib -- {{ args }}

# Run the Arrow-to-Row correctness cases and print the typed encoding's allocation evidence.
test-subscription-rows: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    cargo test --features benchmarks,testing --lib subscription_row:: -- --nocapture

# Type-check one workspace package and all of its targets without building binaries. Extra
# arguments are forwarded to Cargo, so a server check can add `--features testing`.
check-package package *args: (prepare-server-package package)
    cargo check --package {{ package }} --all-targets {{ args }}

# Type-check only the library target of one package, leaving its tests, benches and binaries out.
check-package-lib package *args: (prepare-server-package package)
    cargo check --package {{ package }} --lib {{ args }}

# Run the unit tests of one workspace package whose tests need no server test dependencies.
test-package-lib package *args: (prepare-server-package package)
    cargo test --package {{ package }} --lib -- {{ args }}

# Run one integration test target of one workspace package whose tests need no server test
# dependencies, such as the vocabulary's representation properties.
test-package-test package test *args: (prepare-server-package package)
    cargo test --package {{ package }} --test {{ test }} -- {{ args }}

# Run the unit tests in the binary targets of one workspace package, such as the web console's
# view logic in its `main.rs`.
test-package-bins package *args: (prepare-server-package package)
    cargo test --package {{ package }} --bins -- {{ args }}

# Run the bounded-execution unit tests, which live in the nervix-execution crate rather than the
# server lib.
test-execution *args:
    cargo test --package nervix-execution --lib -- {{ args }}

# Run the primitive boundary's conformance checks once per execution mode. Each mode runs the same
# contract scripts of every family against its own backend and checks that it selected that backend,
# so an operation a backend lacks or answers differently fails here. The ordinary build with the
# `test-util` capability measures every timer on a paused clock. The Shuttle build also shows that
# its adapters let the scheduler reach a publication between a read and a waiter's registration,
# that its timers are scheduling points whose timeouts a check triggers, and that a socket fails a
# check; the Turmoil build that sockets, name lookup, timers and admitted CPU jobs belong to the
# simulated host that uses them; and the Loom build that it takes the ordinary libraries for the
# families Loom does not model. The documentation tests show that a runtime attribute refuses a
# crate path. Every mode is its own build, because Cargo would unify the features of one.
# The portable surface is also built for the browser target.
test-primitives: test-primitives-ordinary test-primitives-modeled test-primitives-compile

# The conformance checks in ordinary native execution: the portable surface alone, then with the
# native families, then with the `test-util` capability, which measures every timer on a paused
# clock. `coverage-native-extras test-primitives` runs these under instrumentation.
test-primitives-ordinary:
    cargo test --package nervix-primitives --lib
    cargo test --package nervix-primitives --features native --lib
    cargo test --package nervix-primitives --features 'native test-util' --lib

# The conformance checks under each model checker's backend, each mode its own build.
test-primitives-modeled:
    cargo test --package nervix-primitives --features 'shuttle native' --lib
    cargo test --package nervix-primitives --features 'loom native' --lib
    cargo test --package nervix-primitives --features 'turmoil native' --lib

# The conformance checks that compile rather than run: the documentation tests that a runtime
# attribute refuses a crate path, and the portable surface's browser build.
test-primitives-compile:
    cargo test --package nervix-primitives --features native --doc
    cargo check --package nervix-primitives --lib --target wasm32-unknown-unknown

# Explore the filtered execution, interconnect and server invariants under Shuttle, then replay
# randomized schedules to detect uncontrolled nondeterminism in every check. The former Loom
# recipe is retired: acknowledgement races, the relay dispatch gate and the relay fan-out exercise
# production types. A non-empty `filter` runs only the checks whose full names contain it.
test-shuttle filter="": build-web-console wasm-processor-guests fetch-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    shuttle_packages=(nervix-execution nervix-interconnect nervix-client-core nervix-server)
    for shuttle_package in "${shuttle_packages[@]}"; do
        just test-shuttle-package "${shuttle_package}" {{ quote(filter) }}
        SHUTTLE_CHECK_NONDETERMINISM=1 \
            just test-shuttle-package "${shuttle_package}" {{ quote(filter) }}
    done

# Explore one package's filtered invariants under Shuttle. Each test gets its own process so a
# persisted schedule identifies its package and test. The server's invariants need the build
# dependencies that `test-shuttle` prepares. A non-empty `filter` runs only the checks whose full
# names contain it.
test-shuttle-package package filter="": (prepare-server-package package)
    #!/usr/bin/env bash
    set -euo pipefail
    shuttle_package={{ quote(package) }}
    filter={{ quote(filter) }}
    trace_root="{{ cargo_target_dir }}/shuttle-failures"
    mkdir -p "${trace_root}"
    shuttle_test_list="$(
        cargo test --package "${shuttle_package}" --features shuttle --lib shuttle_ -- \
            --list --format terse | sed -n 's/: test$//p'
    )"
    if [[ -z "${shuttle_test_list}" ]]; then
        echo "no shuttle_ tests found in ${shuttle_package}" >&2
        exit 1
    fi
    mapfile -t shuttle_tests <<< "${shuttle_test_list}"
    for shuttle_test in "${shuttle_tests[@]}"; do
        if [[ "${shuttle_test}" != *"${filter}"* ]]; then
            continue
        fi
        trace_directory="${trace_root}/${shuttle_package}/${shuttle_test}"
        mkdir -p "${trace_directory}"
        SHUTTLE_TRACE_DIR="${trace_directory}" \
            cargo test --package "${shuttle_package}" --features shuttle --lib \
                "${shuttle_test}" -- --exact --test-threads=1
    done

# Replay a schedule emitted under target/shuttle-failures. Its parent directory is the exact test
# name written by `test-shuttle`, so the schedule cannot accidentally run against another invariant.
test-shuttle-replay schedule: build-web-console wasm-processor-guests fetch-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    schedule={{ quote(schedule) }}
    if [[ ! -f "${schedule}" ]]; then
        echo "Shuttle schedule does not exist: ${schedule}" >&2
        exit 1
    fi
    schedule="$(realpath "${schedule}")"
    shuttle_test="$(basename "$(dirname "${schedule}")")"
    shuttle_package="$(basename "$(dirname "$(dirname "${schedule}")")")"
    SHUTTLE_TRACE_FILE="${schedule}" \
        cargo test --package "${shuttle_package}" --features shuttle --lib \
            "${shuttle_test}" -- --exact --test-threads=1 --nocapture

# Explore every registered Loom model of a production owner to exhaustion, each in its own process
# and with Loom's primitives selected through its package's `loom` feature. The inventory in
# crates/model-harness/loom-inventory.toml registers each model by the invariant it checks; the
# whole run fails when a registered invariant is missing, ignored or incomplete, or when a model is
# unregistered. A non-empty `filter` runs the models whose test name or invariant contains it and
# fails when it selects none. A failed model leaves its Loom checkpoint, output and metadata under
# target/loom-failures for `test-loom-replay`.
test-loom filter="": fetch-onnxruntime
    python3 -m unittest --quiet scripts.tests.test_loom_models
    python3 -m scripts.loom_models --target-dir {{ quote(cargo_target_dir) }} run {{ quote(filter) }}

# Replay a failure `test-loom` recorded: Loom resumes from the checkpoint of the failed execution,
# with location tracking and tracing enabled, so that execution runs first.
test-loom-replay failure: fetch-onnxruntime
    python3 -m scripts.loom_models --target-dir {{ quote(cargo_target_dir) }} replay {{ quote(failure) }}

# Show that each Loom model detects the ordering fault it exists for. Every registered weakening is
# applied to a copy of the working tree, the model must fail with its registered message, and the
# checkpoint of that failure must replay it.
test-loom-qualification: fetch-onnxruntime
    python3 -m scripts.loom_models --target-dir {{ quote(cargo_target_dir) }} qualify

# Run the Turmoil suite: the primitive boundary's simulated-host checks, the execution and library
# simulation checks, then every interconnect scenario over its committed regression seeds. Tokio's
# unstable runtime knobs seed per-host scheduling and turn unhandled task panics into runtime
# failures; the cfg is scoped to this test mode, and ordinary and Shuttle builds keep their flags.
# After the build, the tests run inside a real-time budget of `budget_seconds` and end with status
# 124 when it expires. A failed scenario leaves a failure record under target/turmoil-failures for
# `test-turmoil-replay`.
test-turmoil budget_seconds="480":
    #!/usr/bin/env bash
    set -euo pipefail
    turmoil_rustflags="--cfg tokio_unstable ${RUSTFLAGS:-}"
    export NERVIX_TURMOIL_FAILURES={{ quote(turmoil_failures) }}
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-primitives --features 'turmoil native' --lib
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-execution --features turmoil --lib
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-interconnect --features turmoil --lib --test simulation
    deadline=$((SECONDS + {{ budget_seconds }}))
    within_budget() {
        local remaining=$((deadline - SECONDS))
        local status=0
        if ((remaining > 0)); then
            RUSTFLAGS="${turmoil_rustflags}" timeout --kill-after=30 "${remaining}" "$@" \
                || status=$?
        else
            status=124
        fi
        if ((status == 124)); then
            echo "the Turmoil suite exceeded its {{ budget_seconds }}s real-time budget;" \
                "in-progress records under ${NERVIX_TURMOIL_FAILURES} name the unfinished runs" >&2
        fi
        return "${status}"
    }
    within_budget cargo test --package nervix-primitives --features 'turmoil native' --lib -- \
        simulated_host turmoil_mode --test-threads=1
    within_budget cargo test --package nervix-execution --features turmoil --lib -- \
        --test-threads=1
    within_budget cargo test --package nervix-interconnect --features turmoil --lib -- \
        wire::simulation_checks authentication::simulation_tests --test-threads=1
    within_budget cargo test --package nervix-interconnect --features turmoil --test simulation -- \
        --test-threads=1

# Run the interconnect's Turmoil simulation scenarios. Extra arguments filter or configure the test
# binary, so one test can run without the execution and library checks.
test-turmoil-simulation *args:
    #!/usr/bin/env bash
    set -euo pipefail
    export RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}"
    export NERVIX_TURMOIL_FAILURES={{ quote(turmoil_failures) }}
    cargo test --package nervix-interconnect --features turmoil --test simulation -- --test-threads=1 {{ args }}

# Replay one Turmoil failure record in a fresh process with exactly its recorded inputs: the seed,
# epoch, topology, network parameters, bounds and any injected failure. The record names the
# package, test target and test, so nothing else runs. The replay reports how the recorded build
# differs from this one, compares its outcome and semantic trace with the record, and fails when
# the recorded failure reproduces.
test-turmoil-replay record:
    #!/usr/bin/env bash
    set -euo pipefail
    record="$(realpath -e {{ quote(record) }})"
    mapfile -t selection < <(
        python3 -c 'import json, sys; scenario = json.load(open(sys.argv[1]))["scenario"]; print(scenario["package"], scenario["target"], scenario["test"], sep="\n")' "${record}"
    )
    if ((${#selection[@]} != 3)); then
        echo "${record} is not a Turmoil failure record" >&2
        exit 1
    fi
    output="$(mktemp)"
    trap 'rm -f "${output}"' EXIT
    status=0
    NERVIX_TURMOIL_REPLAY="${record}" RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}" \
        cargo test --package "${selection[0]}" --features turmoil --test "${selection[1]}" -- \
            "${selection[2]}" --exact --include-ignored --test-threads=1 --nocapture 2>&1 \
        | tee "${output}" \
        || status=$?
    if ! grep -Fq 'turmoil replay of' "${output}"; then
        echo "no scenario in ${selection[2]} matches ${record}; it may come from another revision" >&2
        exit 1
    fi
    exit "${status}"

# Prove the replay path end to end: inject a harness failure into one scenario, require exactly one
# failure record, and reproduce it in a fresh process through `test-turmoil-replay`.
test-turmoil-replay-check:
    #!/usr/bin/env bash
    set -euo pipefail
    check="$(mktemp -d)"
    trap 'rm -rf "${check}"' EXIT
    records="${check}/records"
    if NERVIX_TURMOIL_FAILURES="${records}" NERVIX_TURMOIL_INJECT_FAILURE=5s \
        RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}" \
        cargo test --package nervix-interconnect --features turmoil --test simulation -- \
            transport::network_disruption_respects_deadlines_and_repairs_authenticated_service \
            --exact --test-threads=1 >"${check}/injected.log" 2>&1; then
        cat "${check}/injected.log"
        echo "the injected harness failure did not fail its scenario" >&2
        exit 1
    fi
    mapfile -t found < <(find "${records}" -name '*.json')
    if ((${#found[@]} != 1)); then
        cat "${check}/injected.log"
        echo "expected one failure record, found ${#found[@]}" >&2
        exit 1
    fi
    if just test-turmoil-replay "${found[0]}" >"${check}/replay.log" 2>&1; then
        cat "${check}/replay.log"
        echo "the replay of an injected failure passed" >&2
        exit 1
    fi
    if ! grep -Fq 'turmoil replay: reproduced the recorded outcome and trace' \
        "${check}/replay.log"; then
        cat "${check}/replay.log"
        echo "the replay did not reproduce the recorded failure" >&2
        exit 1
    fi
    grep -F 'turmoil replay' "${check}/replay.log"

# Explore every interconnect scenario over `count` consecutive seeds from `first` instead of its
# committed regression seeds, running each seed twice, inside a real-time budget of
# `budget_seconds`; the recipe ends with status 124 when the budget expires. A failure leaves a
# record like any other run. A seed that exposes a defect joins its scenario's committed seeds
# with the fix.
test-turmoil-sweep first="1000" count="64" budget_seconds="1500":
    #!/usr/bin/env bash
    set -euo pipefail
    turmoil_rustflags="--cfg tokio_unstable ${RUSTFLAGS:-}"
    export NERVIX_TURMOIL_FAILURES={{ quote(turmoil_failures) }}
    first={{ quote(first) }}
    end=$((first + {{ count }}))
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-interconnect --features turmoil --test simulation
    status=0
    NERVIX_TURMOIL_SWEEP="${first}..${end}" RUSTFLAGS="${turmoil_rustflags}" \
        timeout --kill-after=30 {{ budget_seconds }} \
        cargo test --package nervix-interconnect --features turmoil --test simulation -- \
            --test-threads=1 || status=$?
    if ((status == 124)); then
        echo "the seed sweep exceeded its {{ budget_seconds }}s real-time budget;" \
            "in-progress records under ${NERVIX_TURMOIL_FAILURES} name the unfinished runs" >&2
    fi
    exit "${status}"

# Run the expression VM unit tests, which live in the nervix-vm crate rather than the server lib.
test-vm *args:
    cargo test --package nervix-vm --lib -- {{ args }}

# Run the WASM host, guest SDK and ABI protocol unit tests, which live in their own crates rather
# than the server lib. The host tests drive the bundled Rust and Go reference guests.
test-wasm *args: wasm-processor-guests
    cargo test --package nervix-wasm --package nervix-wasm-sdk --package nervix-wasm-protocol --lib -- {{ args }}

# Run the consensus unit tests, which live in the nervix-consensus crate rather than the server lib.
test-consensus *args:
    cargo test --package nervix-consensus --lib -- {{ args }}

# Run the interconnect unit tests, which live in the nervix-interconnect crate rather than the
# server lib.
test-interconnect *args:
    cargo test --package nervix-interconnect --lib -- {{ args }}

# Run the resolver's unit tests and its focused protocol tests, which ask local DNS authorities the
# tests start themselves.
test-dns *args:
    cargo test --package nervix-dns --all-targets -- {{ args }}

# Run the connector unit tests, which live in the nervix-connector contract crate and in every
# nervix-connector-* integration crate rather than the server lib.
test-connectors *args:
    cargo test --package 'nervix-connector*' --lib -- {{ args }}

# Run the session wire codec tests and the gRPC and WebSocket sessions that carry its frames.
test-client-wire *args:
    cargo test --package nervix-client-wire --all-features --all-targets -- {{ args }}

# Run native client session unit tests without enabling the modeled Shuttle build.
test-client-core *args:
    cargo test --package nervix-client-core --features arrow,autocomplete --lib -- {{ args }}

# Rewrite the client wire conformance corpus from the encoder's current output. Review the
# regenerated `corpus.report` before committing it: every client implementation is held to it.
update-client-wire-corpus:
    NERVIX_UPDATE_CLIENT_WIRE_CORPUS=1 cargo test --package nervix-client-wire --lib -- \
        tests::conformance

# Build what the cross-language client probes run: the shared Rust binding, the C and C++ probes
# linked against it, the Go probe with its generated FlatBuffers code, and the TypeScript probe
# bundled with its generated code for Node.js and Bun. Every generated file lands under the
# artifacts directory, never in the source tree.
build-client-conformance:
    #!/usr/bin/env bash
    set -euo pipefail
    artifacts={{ quote(cargo_target_dir + "/client-conformance") }}
    library_dir={{ quote(cargo_target_dir + "/debug") }}
    schema="${PWD}/crates/client-wire/schema/session.fbs"
    mkdir -p "${artifacts}"
    cargo build --package nervix-client-ffi
    cc -std=c11 -Wall -Wextra -Werror -pthread -I crates/client-ffi/include \
        tests/client_conformance/c/probe.c \
        -L "${library_dir}" -lnervix_client_ffi -Wl,-rpath,"${library_dir}" \
        -o "${artifacts}/c-probe"
    c++ -std=c++17 -Wall -Wextra -Werror -pthread -I crates/client-ffi/include \
        tests/client_conformance/cpp/probe.cpp \
        -L "${library_dir}" -lnervix_client_ffi -Wl,-rpath,"${library_dir}" \
        -o "${artifacts}/cpp-probe"
    rm -rf "${artifacts}/go" && mkdir -p "${artifacts}/go"
    cp tests/client_conformance/go/go.mod tests/client_conformance/go/go.sum \
        tests/client_conformance/go/*.go "${artifacts}/go/"
    flatc --go -o "${artifacts}/go" "${schema}"
    (cd "${artifacts}/go" && go build -o "${artifacts}/go-probe" .)
    rm -rf "${artifacts}/node-src" "${artifacts}/node" && mkdir -p "${artifacts}/node-src"
    cp tests/client_conformance/node/package.json tests/client_conformance/node/package-lock.json \
        tests/client_conformance/node/probe.ts "${artifacts}/node-src/"
    flatc --ts -o "${artifacts}/node-src/generated" "${schema}"
    (cd "${artifacts}/node-src" && npm ci --no-audit --no-fund && \
        ./node_modules/.bin/esbuild probe.ts --bundle --platform=node --format=esm \
            --target=es2022 --outfile="${artifacts}/node/probe.mjs")

# Run the cross-language client probes against in-process clusters. Every probe prints the same
# report, which the scenario compares with its one expected report. Select runtimes with a tag
# expression, for example `just test-client-conformance '@client_probe_python or @client_probe_java'`.
test-client-conformance tags="@client_conformance_toolchain" *args: tests-deps build-client-conformance
    #!/usr/bin/env bash
    set -euo pipefail
    export NERVIX_CLIENT_CONFORMANCE_DIR={{ quote(cargo_target_dir + "/client-conformance") }}
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/debug/libnervix_client_ffi.so") }}
    cargo test --features testing --test scenarios -- \
        --input tests/features/runtime/client_conformance.feature \
        --tags {{ quote(tags) }} {{ args }}

test-runtime-state-capabilities: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    cargo test --features testing --package nervix-server --doc runtime::

# Validate the small unsafe boundary used by deduplicator expiration tracking.
test-expiry-map:
    cargo test --package nervix-expiry-map

test-expiry-map-miri:
    cargo +nightly miri test --package nervix-expiry-map

test-expiry-map-mutants:
    cargo mutants --package nervix-expiry-map --timeout 120

# Walk the NSPL completion graph and fail on any branch that cannot be completed by accepting the
# suggestions the parser itself offers. Kept out of `just test` because it saturates every core for
# tens of seconds; CI runs it once the main tests have passed.
nspl-completion-walk *args:
    cargo test -p nervix-nspl --test completion_walk -- {{ args }}

# The same walk with deduplication relaxed, reaching branches the gating budget stops short of.
# Reports without failing: this is for finding new ones, not for gating.
nspl-completion-walk-deep *args:
    cargo test -p nervix-nspl --test completion_walk -- \
      --report-only --signature-window 8 --max-states 1000000 {{ args }}

test-coverage: fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    # Merge ordinary-mode coverage for the packages that own an execution mode into the workspace
    # profile without running modeled primitives outside their runner. Model checks are not
    # product coverage, so no Shuttle, Loom or Turmoil build contributes to it.
    mode_packages=(
        nervix-client-core
        'nervix-connector*'
        nervix-consensus
        nervix-execution
        nervix-interconnect
        nervix-model-harness
        nervix-primitives
        nervix-server
        nervix-wasm
    )
    workspace_exclusions=()
    for package in "${mode_packages[@]}"; do
        workspace_exclusions+=(--exclude "${package}")
    done
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --all-targets --all-features --workspace \
        "${workspace_exclusions[@]}"
    # These server targets cover every server test except the scenario suite, which runs in its
    # own job.
    cargo llvm-cov --no-report --lib --bins --benches --test harness_liveness \
        --features testing --package nervix-server
    cargo llvm-cov --no-report --all-targets \
        --package nervix-client-core \
        --package 'nervix-connector*' \
        --package nervix-consensus \
        --package nervix-execution \
        --package nervix-interconnect \
        --package nervix-model-harness \
        --package nervix-wasm
    cargo llvm-cov --no-report --all-targets --features native --package nervix-primitives
    cargo llvm-cov report --workspace --lcov --output-path lcov-workspace.info
    cargo llvm-cov report --package nervix-cli --package nervix-web-console \
        --package nervix-server --lcov --output-path lcov.info

test-scenarios-coverage: fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    cargo llvm-cov clean --workspace
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --test scenarios
    cargo llvm-cov report --workspace --lcov --output-path lcov-workspace.info
    cargo llvm-cov report --package nervix-cli --package nervix-web-console \
        --package nervix-server --lcov --output-path lcov.info

# Remove collected profiles and instrumented workspace artifacts before collecting a new revision.
# Append recipes retain artifacts, so a source move can otherwise leave stale line mappings beside
# the current ones in a report.
coverage-clean-workspace:
    cargo llvm-cov clean --workspace

# Rewrite lcov.info from the profiles the last coverage recipe collected, over the sources of every
# workspace package, so crate lines the server's tests executed are measured as CI measures them.
coverage-report-workspace *args:
    cargo llvm-cov report --workspace --lcov --output-path lcov.info {{ args }}

# Check the same merged workspace coverage and complexity limit locally as CI does.
check-coverage report="lcov-workspace.info":
    cargo crap --lcov {{ quote(report) }} --min 30 --threshold 30

# Measure changed server and CLI lines against the server's unit tests and selected Cucumber
# features while iterating. The scenarios run the public CLI, so it is built instrumented and handed
# to them exactly as `test-coverage` does. That recipe remains the CI gate for workspace coverage
# and CRAP.
test-coverage-feature +features: fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    cargo llvm-cov clean --workspace
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    for feature in {{ features }}; do
        cargo llvm-cov --no-report --features testing --package nervix-server \
            --test scenarios -- --input "${feature}" --concurrency 1
    done
    cargo llvm-cov report --package nervix-cli --package nervix-server --lcov \
        --output-path lcov.info

# Add client and vocabulary tests to an existing coverage profile without clearing server and
# public-scenario coverage collected by `test-coverage-feature`.
test-coverage-client-packages:
    cargo llvm-cov --no-report --all-targets \
        --package nervix-client-core --package nervix-client-wire \
        --package nervix-models --package nervix-cli --package nervix-web-console

# Measure browser and CLI binary tests together with their public session scenarios.
test-coverage-clients: fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --bins \
        --package nervix-web-console --package nervix-cli
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    for feature in \
        tests/features/web-console/connection_status.feature \
        tests/features/web-console/nspl_repl.feature \
        tests/features/web-console/domain_clock.feature \
        tests/features/tools/cli_session.feature; do
        cargo llvm-cov --no-report --features testing --package nervix-server \
            --test scenarios -- --input "${feature}" --concurrency 1
    done
    just coverage-clients-report

coverage-clients-report: fetch-onnxruntime
    cargo llvm-cov report --package nervix-cli --package nervix-web-console \
        --package nervix-server --lcov --output-path lcov.info

# Refresh the native console's coverage after a focused change without rebuilding the server
# browser scenario harness; its recorded browser and CLI coverage remains in the same target.
coverage-web-console-unit:
    cargo llvm-cov --no-report --bins --package nervix-web-console
    just coverage-clients-report

# Build the standalone CLI with the same coverage flags as its binary unit tests. Cargo's
# all-targets test pass alone leaves only the test executable, which public scenarios do not run.
coverage-cli-binary:
    #!/usr/bin/env bash
    set -euo pipefail
    source <(cargo llvm-cov show-env --sh 2>/dev/null)
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} \
        cargo build --package nervix-cli --bin nervix-cli

coverage-clients-units:
    cargo llvm-cov --no-report --bins \
        --package nervix-web-console --package nervix-cli
    just coverage-clients-report

# Measure the visual create patch across its Model, language, wire, browser, and server owners,
# including the public browser scenarios that exercise the attached transaction prefix and the
# subscription tab lifecycle.
coverage-visual-create output="target/visual-create.lcov": fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --lib \
        --package nervix-models --package nervix-client-wire --package nervix-nspl
    cargo llvm-cov --no-report --bin nervix-web-console --package nervix-web-console
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    for feature in visual_create_schema visual_create_relay visual_create_codec visual_create_client_endpoint visual_create_lookup_udf visual_create_ingestor visual_create_processors; do
        cargo llvm-cov --no-report --features testing --package nervix-server \
            --test scenarios -- --input "tests/features/web-console/${feature}.feature" \
            --concurrency 1 --retry 0
    done
    just coverage-visual-create-report {{ quote(output) }}

# Write the LCOV report of the profiles `coverage-visual-create` collected, over the packages the
# visual create forms span.
coverage-visual-create-report output="target/visual-create.lcov": fetch-onnxruntime
    cargo llvm-cov report --lcov --output-path {{ quote(output) }} \
        --package nervix-models --package nervix-client-wire --package nervix-nspl \
        --package nervix-web-console --package nervix-server

# Write the line coverage of the unit tests of the packages named in `args`, such as
# `--package nervix-vm --package nervix-nspl`, as LCOV to `output`. It checks the patch coverage of
# a change to those packages without the scenario suite that `test-coverage` runs.
coverage-lib output *args: fetch-onnxruntime
    cargo llvm-cov --lib --lcov --output-path {{ output }} {{ args }}

# Measure the server owner regressions with the same dependencies and environment as test-runtime.
coverage-runtime output="target/task-handles-runtime.lcov" *args: fetch-onnxruntime build-web-console wasm-processor-guests
    cargo llvm-cov --package nervix-server --features testing --lib --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} {{ args }}

# Measure selected public scenarios with the same arguments as `test-scenarios`.
coverage-scenarios output *args: fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
    cargo llvm-cov --features testing --test scenarios --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ args }}

# Add selected scenarios to the current coverage profiles without rebuilding unchanged artifacts.
coverage-scenarios-append output *args: fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-nspl-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-nspl-format") }}
    cargo llvm-cov --no-clean --features testing --test scenarios --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ args }}

# Measure the Redis DNS connector, its shared TLS/DNS code, and public source/sink scenarios.
coverage-redis output="target/redis-dns.lcov": fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --lib \
        --package nervix-dns \
        --package nervix-connector \
        --package nervix-connector-redis
    cargo llvm-cov --no-report --lib --package nervix-server -- redis_
    cargo llvm-cov --no-report --lib --package nervix-server -- sources_that_resolve_names_report_missing_node_dns_as_start_failure
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --test scenarios -- \
        --input tests/features/runtime/redis_dns_resolution.feature --name Redis --retry 0 --concurrency 1
    just coverage-redis-report {{ quote(output) }}

coverage-redis-report output="target/redis-dns.lcov": fetch-onnxruntime
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-server \
        --package nervix-dns \
        --package nervix-connector \
        --package nervix-connector-redis

coverage-redis-units-append output="target/redis-dns.lcov":
    cargo llvm-cov --no-clean --lib --package nervix-connector-redis
    just coverage-redis-report {{ quote(output) }}

coverage-redis-server-units-append output="target/redis-dns.lcov": fetch-onnxruntime
    cargo llvm-cov --no-clean --lib --package nervix-server -- redis_
    cargo llvm-cov --no-clean --lib --package nervix-server -- sources_that_resolve_names_report_missing_node_dns_as_start_failure
    just coverage-redis-report {{ quote(output) }}

# Collect the changed DNS client units and their public one-/three-node paths into one LCOV
# profile so patch coverage can be checked before opening the PR.
coverage-dns-clients output="target/dns-clients.lcov": fetch-onnxruntime tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --all-targets \
        --package nervix-dns \
        --package nervix-client-core \
        --package nervix-client-ffi \
        --package nervix-cli \
        --package nervix-connector \
        --package nervix-connector-http \
        --package nervix-connector-prometheus \
        --package nervix-connector-sentry \
        --package nervix-connector-otel \
        --package nervix-connector-iceberg \
        --package nervix-connector-rabbitmq \
        --package nervix-connector-redis \
        --package nervix-connector-mqtt \
        --package nervix-connector-syslog \
        --package nervix-connector-websockets \
        --package nervix-connector-clickhouse \
        --package nervix-connector-sqs \
        --package nervix-interconnect
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    run_scenario() {
        cargo llvm-cov --no-report --features testing --package nervix-server --test scenarios -- \
            --input "$1" --name "$2" --retry 0 --concurrency 1
    }
    run_scenario tests/features/runtime/http_client_ingestion.feature 'DNS.*fixture'
    run_scenario tests/features/runtime/prometheus_ingestion.feature 'Prometheus.*delivers'
    run_scenario tests/features/runtime/sentry_emission.feature 'Sentry.*publishes'
    run_scenario tests/features/runtime/otel_emission.feature 'OTEL.*trace'
    run_scenario tests/features/runtime/otel_emission.feature 'OTEL.*metric.*HTTP'
    run_scenario tests/features/runtime/client_wire_qualification.feature 'Subscription.restoration'
    run_scenario tests/features/tools/cli_session.feature 'CLI.connects.by.hostname'
    run_scenario tests/features/tools/cli_session.feature 'CLI.rejects.a.TLS'
    run_scenario tests/features/runtime/iceberg_emission.feature 'DNS.*fixture|Iceberg.*holds.*ACK'
    run_scenario tests/features/runtime/rabbitmq_dns_resolution.feature 'RabbitMQ|AMQPS'
    run_scenario tests/features/runtime/redis_dns_resolution.feature 'Redis'
    run_scenario tests/features/runtime/mqtt_dns_resolution.feature 'MQTT'
    run_scenario tests/features/runtime/syslog_dns_resolution.feature 'Syslog'
    run_scenario tests/features/runtime/websocket_client_ingestion.feature 'Websocket client ingestor connects'
    run_scenario tests/features/runtime/websocket_client_tls_resource_mounts.feature 'Websocket client keeps'
    run_scenario tests/features/runtime/websocket_dns_resolution.feature 'WebSocket clients reconnect'
    run_scenario tests/features/runtime/clickhouse_dns_resolution.feature 'ClickHouse'
    run_scenario tests/features/runtime/sqs_dns_resolution.feature 'SQS'
    just coverage-dns-clients-report {{ quote(output) }}

# Export the profiles collected by `coverage-dns-clients` without rebuilding its test binaries.
coverage-dns-clients-report output="target/dns-clients.lcov": fetch-onnxruntime
    cargo llvm-cov report --lcov --output-path {{ quote(output) }} \
        --package nervix-server \
        --package nervix-dns \
        --package nervix-client-core \
        --package nervix-client-ffi \
        --package nervix-cli \
        --package nervix-connector \
        --package nervix-connector-http \
        --package nervix-connector-prometheus \
        --package nervix-connector-sentry \
        --package nervix-connector-otel \
        --package nervix-connector-iceberg \
        --package nervix-connector-rabbitmq \
        --package nervix-connector-redis \
        --package nervix-connector-mqtt \
        --package nervix-connector-syslog \
        --package nervix-connector-websockets \
        --package nervix-connector-clickhouse \
        --package nervix-connector-sqs \
        --package nervix-interconnect

# Measure the Shuttle-only test paths, which production-mode workspace coverage cannot compile.
# The same checks run under ordinary and nondeterminism-detection schedules, with one test thread
# so Shuttle's scheduler state is not shared between tests.
coverage-shuttle output: build-web-console wasm-processor-guests fetch-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    cargo llvm-cov clean --workspace
    for shuttle_package in nervix-execution nervix-interconnect nervix-server; do
        SHUTTLE_REPORT_STEPS=1 cargo llvm-cov test --no-report \
            --package "${shuttle_package}" --features shuttle --lib shuttle_ -- --test-threads=1
        SHUTTLE_CHECK_NONDETERMINISM=1 cargo llvm-cov test --no-report \
            --package "${shuttle_package}" --features shuttle --lib shuttle_ -- --test-threads=1
    done
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }}

# Write line coverage for binary unit tests, such as the CLI's main target.
coverage-bins output *args: fetch-onnxruntime
    cargo llvm-cov --bins --lcov --output-path {{ output }} {{ args }}

# Exercise the CLI binary through the public transaction, clock, and REPL reconnect scenarios with
# LLVM coverage.
coverage-cli-process output="target/cli-process.lcov":
    #!/usr/bin/env bash
    set -euo pipefail
    coverage_dir="{{ cargo_target_dir }}/cli-process"
    mkdir -p "$coverage_dir"
    CARGO_TARGET_DIR="$coverage_dir" RUSTFLAGS="-C instrument-coverage ${RUSTFLAGS:-}" \
        cargo build --package nervix-cli --bin nervix-cli
    rm -f "$coverage_dir"/cli-*.profraw
    LLVM_PROFILE_FILE="$coverage_dir/cli-%p-%m.profraw" \
        NERVIX_TEST_CLI_PATH="$coverage_dir/debug/nervix-cli" \
        just test-scenarios --input tests/features/runtime/nspl_transactions.feature --name CLI
    LLVM_PROFILE_FILE="$coverage_dir/cli-%p-%m.profraw" \
        NERVIX_TEST_CLI_PATH="$coverage_dir/debug/nervix-cli" \
        just test-scenarios --input tests/features/tools/cli_session.feature --name clock
    LLVM_PROFILE_FILE="$coverage_dir/cli-%p-%m.profraw" \
        NERVIX_TEST_CLI_PATH="$coverage_dir/debug/nervix-cli" \
        just test-scenarios --input tests/features/tools/cli_session.feature --name keeps.printing
    llvm_bin="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | awk '/^host:/{print $2}')/bin"
    "$llvm_bin/llvm-profdata" merge -sparse "$coverage_dir"/*.profraw -o "$coverage_dir/merged.profdata"
    "$llvm_bin/llvm-cov" export "$coverage_dir/debug/nervix-cli" \
        --instr-profile="$coverage_dir/merged.profdata" --format=lcov > {{ quote(output) }}

# Measure the Turmoil runner's changed lines without running the full scenario suite.
coverage-turmoil output:
    #!/usr/bin/env bash
    set -euo pipefail
    export RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}"
    cargo llvm-cov --no-report \
        --package nervix-primitives --features 'turmoil native' --lib -- \
        simulated_host turmoil_mode --test-threads=1
    cargo llvm-cov --no-report \
        --package nervix-execution --features turmoil --lib
    cargo llvm-cov --no-report \
        --package nervix-interconnect --features turmoil --lib -- \
        wire::simulation_checks authentication::simulation_tests --test-threads=1
    cargo llvm-cov --no-report \
        --package nervix-interconnect --features turmoil --test simulation -- --test-threads=1
    cargo llvm-cov report --no-default-ignore-filename-regex \
        --lcov --output-path {{ quote(output) }}

# Run the extra checks that execute Nervix code natively in ordinary mode, `bench-smoke`,
# `test-primitives` and `nspl-completion-walk`, exactly as their recipes do but with LLVM source
# coverage, and fail as they do. Prerequisites build outside the instrumentation, and the parts of a
# check that compile, target the browser or run a model checker stay uninstrumented. Each producer
# writes lcov.info, completion.json, executions.jsonl and export.log to a fresh
# target/native-coverage/<producer>/<mode>/<toolchain>/<attempt>/, and CI runs one per step.
# Run all three: `just coverage-native-extras`; one: `just coverage-native-extras bench-smoke`.
coverage-native-extras *producers:
    python3 scripts/native_coverage.py --target-dir {{ quote(cargo_target_dir) }} run {{ producers }}

# Exercise the native coverage collector: its producer inventory, source policy, selection and
# failure handling, then instrumented runs of a fixture crate through the real toolchain.
test-native-coverage:
    NERVIX_NATIVE_COVERAGE_TOOLCHAIN_TESTS=required python3 -m unittest --quiet scripts.tests.test_native_coverage

# Run every Criterion suite with the release profile. Extra arguments are forwarded to Criterion.
# The server benches link the console the server serves, so the console is built first rather than
# left to whatever ran before them.
bench *args: fetch-onnxruntime build-web-console
    cargo bench --package nervix-server --bench relay_interaction --features benchmarks -- {{ args }}
    cargo bench --package nervix-branch-instances --bench owned_branches -- {{ args }}
    cargo bench --package nervix-server --bench subscription_row_encoding --features benchmarks -- {{ args }}
    cargo bench --package nervix-server --bench wasm_checkpoint --features benchmarks -- {{ args }}
    cargo bench --package nervix-server --bench state_replication --features benchmarks -- {{ args }}
    cargo bench --package nervix-columnar-json --bench json_encode -- {{ args }}
    cargo bench --package nervix-vm --bench vm -- {{ args }}

# Exercise every Criterion body once without spending CI's smoke-test budget on release codegen.
bench-smoke: fetch-onnxruntime build-web-console bench-smoke-bodies

# The Criterion bodies `bench-smoke` exercises, without the console build that precedes them there.
# `coverage-native-extras` builds the console outside its instrumentation and then runs these in it.
bench-smoke-bodies:
    cargo bench --profile dev --package nervix-server --bench relay_interaction --features benchmarks -- --test
    cargo bench --profile dev --package nervix-server --bench admitted_work --features benchmarks -- --test
    cargo bench --profile dev --package nervix-branch-instances --bench owned_branches -- --test
    cargo bench --profile dev --package nervix-server --bench subscription_row_encoding --features benchmarks -- --test
    cargo bench --profile dev --package nervix-server --bench wasm_checkpoint --features benchmarks -- --test
    cargo bench --profile dev --package nervix-server --bench task_handles --features benchmarks -- target/task-handles-smoke.json
    cargo bench --profile dev --package nervix-server --bench state_replication --features benchmarks -- --test
    cargo bench --profile dev --package nervix-columnar-json --bench json_encode -- --test
    cargo bench --profile dev --package nervix-simd-kernels --bench constant_division -- --test
    cargo bench --profile dev --package nervix-vm --bench vm -- --test

# Measure the data-plane work a node admits through its bounded executor, as the runtime submits it:
# one branched input prepared into its branch batches, and an emitter batch encoded through a JAQ
# transformation. Extra arguments are forwarded to Criterion.
bench-admitted-work *args: fetch-onnxruntime build-web-console
    cargo bench --package nervix-server --bench admitted_work --features benchmarks -- {{ args }}

# Run only the relay-interaction Criterion suite, including the delivery a node input records for
# one batch at 1, 64, and 1,024 rows. Extra arguments are forwarded to Criterion.
bench-relay-interaction *args: fetch-onnxruntime build-web-console
    cargo bench --package nervix-server --bench relay_interaction --features benchmarks -- {{ args }}

# Measure the branch owner a relay owner task holds: batches for established branches, which
# publish nothing, and branch churn, which creates, evicts and publishes once per batch.
bench-branch-instances *args:
    cargo bench --package nervix-branch-instances --bench owned_branches -- {{ args }}

# Build the SIMD kernel crate's optimized unit-test binary for the x86-64-v3 payload the Docker
# image ships, in its own target directory, so the generated instructions of each dispatch level can
# be inspected with objdump without the host's native CPU tuning.
build-simd-kernels-x86-64-v3:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/simd-kernels-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo test --release --package nervix-simd-kernels --lib --no-run

# Measure one batch of schemaful JSON rows, including the escape classification made once per
# Arrow batch. The suite compares the column writer against serde's per-row reference encoding.
bench-json-encode *args:
    cargo bench --package nervix-columnar-json --bench json_encode -- {{ args }}

# Measure direct Arrow-to-Row subscription encoding. The suite reports encoded bytes before
# Criterion measures CPU; its unit probe measures allocations.
bench-subscription-rows *args: fetch-onnxruntime
    cargo bench --package nervix-server --bench subscription_row_encoding --features benchmarks -- {{ args }}

# Write raw component timing and retained-allocation samples for Arrow-to-Row delivery.
client-wire-cost output="target/client-wire-cost.json": fetch-onnxruntime
    cargo bench --package nervix-server --bench client_wire_cost --features benchmarks -- {{ quote(output) }}

# Run the component benchmark with coverage instrumentation for changed benchmark lines.
coverage-client-wire-cost output="target/client-wire-cost.lcov" report="target/client-wire-cost-coverage.json":
    cargo llvm-cov --bench client_wire_cost --features benchmarks --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ quote(report) }}

# Check the typed Arrow workload's selection, null, redaction, branch and frame-limit assertions.
test-client-wire-bench-fixture: fetch-onnxruntime
    cargo test --package nervix-server --features benchmarks --lib subscription_row::benchmark::tests -- --nocapture

# Capture raw timings through the exported Rust binding's C ABI on a 100-row frame.
client-wire-binding-cost output="target/client-wire-binding-cost.json":
    NERVIX_CLIENT_WIRE_BINDING_COST_OUTPUT="$(realpath -m {{ quote(output) }})" cargo test --package nervix-client-ffi --lib tests::profiles_bulk_binding_access_and_retain_release -- --exact --nocapture

# Measure CPython ctypes and GC overhead on the same live rows used by conformance.
client-wire-binding-host-cost output_dir="target/client-wire-binding-host":
    NERVIX_CLIENT_WIRE_BINDING_PROFILE_DIR="$(realpath -m {{ quote(output_dir) }})" just test-client-conformance '@client_probe_python' --concurrency 1 --retry 0

# Compare the same native command workload over plaintext and TLS with one-node test clusters.
client-wire-tls-cost output_dir="target/client-wire-tls-cost":
    NERVIX_CLIENT_WIRE_TLS_OUTPUT_DIR={{ quote(output_dir) }} just test-scenarios --input tests/features/runtime/client_wire_tls_cost.feature --tags @client_wire_tls_cost --concurrency 1 --retry 0

# Measure runtime-state replication on the owner and on a replica: a Kafka offset commit its one
# replica acknowledges, and the branch lifecycle check a replica makes before it installs each
# branch checkpoint, for lifecycles of 16, 128 and 1,024 branches.
bench-state-replication *args: fetch-onnxruntime
    cargo bench --package nervix-server --bench state_replication --features benchmarks -- {{ args }}

# Measure durable WASM guest-state checkpoints against unsynchronized writes of the same states. The
# store lives under the crate target directory, so the synchronization cost is that of its storage.
bench-wasm-checkpoint *args: fetch-onnxruntime
    cargo bench --package nervix-server --bench wasm_checkpoint --features benchmarks -- {{ args }}

# Run only the expression VM Criterion suite. Extra arguments are forwarded to Criterion, so a
# group filter and `--save-baseline` or `--baseline` compare VM kernels without the relay suite.
bench-vm *args:
    cargo bench --package nervix-vm --bench vm -- {{ args }}

# Build the VM Criterion binary for the x86-64-v3 payload the Docker image ships, in its own target
# directory, so the checked lanes and their failure packing can be inspected with objdump without
# the host's native CPU tuning.
build-vm-bench-x86-64-v3:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/vm-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --package nervix-vm --bench vm --no-run

# Run the same VM Criterion harness with one-shot allocation and output-size probes. The
# instrumentation is compiled only for this recipe; use `bench-vm` for timing comparisons.
bench-vm-alloc *args:
    cargo bench --package nervix-vm --bench vm --features benchmark-allocations -- {{ args }}

# Build the reusable harness and forward its CLI arguments. This is enough for container subjects
# such as Vector; local Nervix has a dedicated recipe below because it also builds the server.
benchmark *args:
    cargo build --release --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" {{ args }}

# Focused validation for the benchmark framework without building product binaries.
test-benchmark-framework *args:
    cargo test --package nervix-benchmark {{ args }}

# Build the pinned Flink image with its matching Kafka SQL connector.
benchmark-flink-image:
    docker build --file "{{ justfile_directory() }}/benches/flink/Dockerfile" \
        --tag nervix-benchmark-flink:2.0.1 \
        "{{ justfile_directory() }}/benches/flink"

# Build and benchmark the current local Nervix checkout.
benchmark-nervix-local benchmark_name="kafka-filter-map" *args: fetch-onnxruntime build-web-console
    cargo build --release \
        --package nervix-server --bin nervix-server \
        --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" run {{ quote(benchmark_name) }} \
        --implementation nervix --nervix-mode local \
        --server-binary "{{ cargo_target_dir }}/release/nervix-server" {{ args }}

# Run a tagged Nervix server image; the harness configures it through the client-core API.
benchmark-nervix-image image benchmark_name="kafka-filter-map" *args:
    cargo build --release --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" run {{ quote(benchmark_name) }} \
        --implementation nervix --nervix-mode image --nervix-image {{ quote(image) }} {{ args }}

# Build once, then run every declared workload implementation sequentially with local Nervix.
benchmark-all-local *args: fetch-onnxruntime build-web-console benchmark-flink-image
    cargo build --release \
        --package nervix-server --bin nervix-server \
        --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" run-all \
        --nervix-mode local \
        --server-binary "{{ cargo_target_dir }}/release/nervix-server" {{ args }}

# Local same-hardware A/B: build the baseline ref and the current tree once each into cached
# binaries under target/ab/, then interleave runs per arm so machine drift cancels out. This is
# how performance claims are established; CI benchmark comments are only a same-run smoke signal.
benchmark-ab baseline_ref runs="3" benchmark_name="kafka-filter-map" *args: fetch-onnxruntime build-web-console
    #!/usr/bin/env bash
    set -euo pipefail
    baseline_commit="$(git rev-parse --verify --end-of-options {{ quote(baseline_ref) }}'^{commit}')" || {
        echo "error:" {{ quote(baseline_ref) }} "does not resolve to a commit" >&2
        exit 1
    }
    ab_root="{{ cargo_target_dir }}/ab"
    case "${ab_root}" in /*) ;; *) ab_root="${PWD}/${ab_root}" ;; esac
    baseline_binary="${ab_root}/${baseline_commit}/nervix-server"
    if [[ ! -x "${baseline_binary}" ]]; then
        worktree="${ab_root}/worktree"
        git worktree prune
        if [[ -e "${worktree}" ]]; then
            git worktree remove --force "${worktree}" || { rm -rf "${worktree}"; git worktree prune; }
        fi
        git worktree add --detach "${worktree}" "${baseline_commit}"
        (cd "${worktree}" \
            && export CARGO_TARGET_DIR="${ab_root}/build" \
            && just build-web-console \
            && cargo build --release --package nervix-server --bin nervix-server)
        install -D "${ab_root}/build/release/nervix-server" "${baseline_binary}"
        git worktree remove --force "${worktree}"
    fi
    cargo build --release \
        --package nervix-server --bin nervix-server \
        --package nervix-benchmark --bins
    install -D "{{ cargo_target_dir }}/release/nervix-server" "${ab_root}/candidate/nervix-server"
    "{{ cargo_target_dir }}/release/nervix-benchmark" run-ab {{ quote(benchmark_name) }} \
        --baseline-binary "${baseline_binary}" \
        --candidate-binary "${ab_root}/candidate/nervix-server" \
        --baseline-label {{ quote(baseline_ref) }}" @ ${baseline_commit:0:12}" \
        --candidate-label "working tree" \
        --runs {{ quote(runs) }} {{ args }}

# Build only the benchmark harness, then run it against an already-built Nervix image. The harness
# configures the server directly through client-core and never rebuilds a product binary.
benchmark-ci nervix_image artifacts_root *args: benchmark-flink-image
    #!/usr/bin/env bash
    set -euo pipefail
    test -S /var/run/docker.sock
    docker image inspect {{ quote(nervix_image) }} >/dev/null
    cargo build --release --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" \
        --repository-root "{{ justfile_directory() }}" \
        run-all --nervix-mode image --nervix-image {{ quote(nervix_image) }} \
        --artifacts-root {{ quote(artifacts_root) }} {{ args }}

cargo-fmt:
    cargo +nightly fmt

taplo-format:
    taplo format

[parallel]
fmt: cargo-fmt fmt-typed-ratchet taplo-format dockerfmt gherkin-fmt nspl-fmt autoinherit

cargo-fmt-check:
    cargo +nightly fmt --check

taplo-format-check:
    taplo format --check

[parallel]
fmt-check: cargo-fmt-check fmt-check-typed-ratchet taplo-format-check dockerfmt-check gherkin-fmt-check nspl-fmt-check autoinherit-check

gherkin-fmt:
    ghokin fmt replace tests/features

gherkin-fmt-check:
    ghokin check tests/features

# Format every NSPL file in the repository.
nspl-fmt:
    cargo run -q --package nervix-nspl-format -- .

# Report every NSPL file in the repository that is not formatted.
nspl-fmt-check:
    cargo run -q --package nervix-nspl-format -- --check .

build-nspl-format:
    cargo build --package nervix-nspl-format

autoinherit:
    cargo autoinherit

autoinherit-check:
    #!/bin/bash
    set -e
    git diff --exit-code 2>/dev/null >/dev/null  || echo "skip autoinherit on dirty working tree" && exit 0
    cargo autoinherit
    git diff --exit-code

# Each package/configuration is a separate parallel dependency. The one Cargo recipe below
# hashes its arguments and toolchain into an isolated directory, and `just --jobs N` bounds it.
clippy_all_features_packages := [
    "nervix-approx-into",
    "nervix-arbitrary",
    "nervix-backup",
    "nervix-benchmark",
    "nervix-bounded-write",
    "nervix-branch-instances",
    "nervix-checkpoint-replication",
    "nervix-cli",
    "nervix-client-ffi",
    "nervix-client-wire",
    "nervix-columnar-json",
    "nervix-dataflow-graph",
    "nervix-dns",
    "nervix-expiry-map",
    "nervix-jaq",
    "nervix-models",
    "nervix-nspl",
    "nervix-nspl-format",
    "nervix-primitives-macros",
    "nervix-recovery",
    "nervix-roto",
    "nervix-simd-kernels",
    "nervix-test-environment",
    "nervix-vm",
    "nervix-wasm-protocol",
    "nervix-wasm-sdk",
    "nervix-web-console",
]

clippy_connector_packages := [
    "nervix-connector",
    "nervix-connector-clickhouse",
    "nervix-connector-http",
    "nervix-connector-iceberg",
    "nervix-connector-kafka",
    "nervix-connector-mongodb",
    "nervix-connector-mqtt",
    "nervix-connector-mysql",
    "nervix-connector-nats",
    "nervix-connector-otel",
    "nervix-connector-postgres",
    "nervix-connector-prometheus",
    "nervix-connector-pulsar",
    "nervix-connector-rabbitmq",
    "nervix-connector-redis",
    "nervix-connector-sentry",
    "nervix-connector-sqs",
    "nervix-connector-syslog",
    "nervix-connector-websockets",
    "nervix-connector-zeromq",
]

clippy_shuttle_packages := [
    "nervix-client-core",
    "nervix-connector",
    "nervix-connector-clickhouse",
    "nervix-connector-http",
    "nervix-connector-iceberg",
    "nervix-connector-mongodb",
    "nervix-connector-mqtt",
    "nervix-connector-mysql",
    "nervix-connector-nats",
    "nervix-connector-otel",
    "nervix-connector-postgres",
    "nervix-connector-pulsar",
    "nervix-connector-rabbitmq",
    "nervix-connector-redis",
    "nervix-connector-sentry",
    "nervix-connector-sqs",
    "nervix-connector-syslog",
    "nervix-connector-zeromq",
    "nervix-execution",
    "nervix-interconnect",
    "nervix-wasm",
]

[private, parallel]
clippy-targets: ordinary-clippy-targets shuttle-clippy-targets turmoil-clippy-targets loom-clippy-targets

[private, parallel]
ordinary-clippy-targets: \
    *(clippy-target *clippy_all_features_packages ["--all-features", "--all-targets"]) \
    *(clippy-target *clippy_connector_packages ["--all-targets"]) \
    *(clippy-target *["nervix-execution", "nervix-interconnect", "nervix-model-harness", "nervix-wasm"] ["--all-targets"]) \
    (clippy-target "nervix-server" ["--all-targets", "--features", "benchmarks testing"]) \
    (clippy-target "nervix-client-core" ["--all-targets", "--features", "autocomplete"]) \
    (clippy-target "nervix-consensus" ["--all-targets", "--features", "testing"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "native"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "native test-util"]) \
    *(clippy-target *["nervix-cli", "nervix-server", "nervix-nspl-format", "nervix-web-console"] ["--all-targets"]) \
    (clippy-target "nervix-client-wire" ["--target", "wasm32-unknown-unknown"])

[private, parallel]
shuttle-clippy-targets: \
    *(clippy-target *clippy_shuttle_packages ["--lib", "--features", "shuttle"]) \
    *(clippy-target *["nervix-connector-kafka", "nervix-consensus", "nervix-server"] ["--lib", "--features", "shuttle testing"]) \
    *(clippy-target *["nervix-connector-prometheus", "nervix-connector-websockets"] ["--lib", "--features", "nervix-connector/shuttle nervix-primitives/shuttle"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "shuttle native"])

[private, parallel]
turmoil-clippy-targets: \
    *(clippy-target *["nervix-execution", "nervix-interconnect"] ["--all-targets", "--features", "turmoil"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "turmoil native"])

# Lint the Loom models and harness, the primitive boundary, and each library as it ships and
# in test mode. The same parallel dependencies run in the full validation matrix.
cargo-clippy-loom jobs=default_jobs: fetch-onnxruntime (run-with-jobs "loom-clippy-targets" jobs)

[private, parallel]
loom-clippy-targets: \
    *(clippy-target *["nervix-execution", "nervix-model-harness"] ["--all-targets", "--features", "loom"]) \
    (clippy-target "nervix-primitives" ["--all-targets", "--features", "loom native"]) \
    *(clippy-target *["nervix-consensus", "nervix-server"] ["--lib", "--features", "loom"]) \
    (clippy-target "nervix-server" ["--lib", "--profile", "test", "--features", "loom"]) \
    (clippy-target "nervix-consensus" ["--lib", "--profile", "test", "--features", "loom testing"])

# The shared Clippy command accepts one package and its Cargo arguments. Target, feature,
# profile and toolchain differences identify separate build directories. Keep kache configured.
[private]
clippy-target package args toolchain="": (prepare-server-package package)
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/clippy/" + package + "/" + sha256(show([args, toolchain]))) }} RUSTFLAGS={{ quote("-Dwarnings " + rustflags) }} cargo {{ if toolchain == "" { "" } else { quote("+" + toolchain) } }} clippy --package {{ quote(package) }} {{ quote(args) }} -q

# Lint one package and all of its targets; extra arguments retain their boundaries.
cargo-clippy-package package *args: (clippy-target package ["--all-targets", args])

# Lint every product Clippy target, with four concurrent processes by default.
cargo-clippy jobs=default_jobs: fetch-onnxruntime (run-with-jobs "clippy-targets" jobs)

[private, parallel]
lint-inner: clippy-targets typed-ratchet-clippy-targets

# Lint the product and compiler tooling in one bounded invocation.
lint jobs=default_jobs: fetch-onnxruntime (run-with-jobs "lint-targets" jobs)

[private]
lint-targets: build-web-console lint-inner

audit:
    cargo audit

# Count the architecture debt and fail when a count is above its baseline in debt-baseline.json.
ratchet *args: fetch-onnxruntime typed-ratchet-build
    python3 -m scripts.ratchet {{ args }}

# Focused regression checks for the debt gate and its shared Rust source scanner.
test-ratchet-units:
    python3 -m unittest scripts.tests.test_ratchet

typed-ratchet-build:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/driver cargo +nightly-2026-09-17 build --manifest-path tools/nervix-lint/Cargo.toml --package nervix-lint-driver --package nervix-lint-report

typed-ratchet *args: typed-ratchet-build
    python3 -m scripts.typed_ratchet {{ args }}

typed-ratchet-turmoil *args:
    RUSTFLAGS="--cfg tokio_unstable {{ rustflags }}" python3 -m scripts.typed_ratchet {{ args }}

test-typed-ratchet-compiler:
    just typed-ratchet --fixture-mode ordinary --recompile --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-ordinary.json
    python3 -m unittest scripts.tests.test_typed_ratchet scripts.tests.compiler_fixture_checks.CompilerFixtureTests

test-typed-ratchet-contracts: typed-ratchet-build
    python3 -m unittest scripts.tests.compiler_contract_checks

test-typed-ratchet: test-typed-ratchet-ordinary test-typed-ratchet-product-docs test-typed-ratchet-modeled

test-typed-ratchet-ordinary:
    just test-typed-ratchet-reports
    just test-typed-ratchet-compiler
    just test-typed-ratchet-contracts
    just test-typed-ratchet-docs

test-typed-ratchet-modeled:
    just typed-ratchet --fixture-mode shuttle --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-shuttle.json
    just typed-ratchet --fixture-mode loom --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-loom.json
    just typed-ratchet --fixture-mode turmoil --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-turmoil.json
    python3 -m unittest scripts.tests.compiler_fixture_checks.ModeledFixtureTests

test-typed-ratchet-docs:
    #!/usr/bin/env bash
    set -euo pipefail
    doctest_flags="${RUSTDOCFLAGS:-} -Z unstable-options --persist-doctests {{ cargo_target_dir }}/typed-ratchet/doctests"
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        doctest_flags+=" -C instrument-coverage"
    fi
    RUSTDOCFLAGS="$doctest_flags" CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/docs \
        cargo +nightly-2026-09-17 test --manifest-path tools/nervix-lint/fixtures/Cargo.toml --doc

# Current API examples compile alongside their compile_fail counterparts on the product toolchain.
test-typed-ratchet-product-docs:
    cargo +1.99 test --package nervix-primitives --doc expect_lint
    cargo +1.99 test --package nervix-vm --doc FunctionInjector
    RUSTUP_TOOLCHAIN=1.99 just test-runtime-state-capabilities

qualify-typed-ratchet-cache: typed-ratchet-build
    python3 -m scripts.tests.qualify_typed_ratchet_cache

test-typed-ratchet-reports:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/driver cargo +nightly-2026-09-17 test --manifest-path tools/nervix-lint/Cargo.toml --package nervix-lint-report --lib

fmt-typed-ratchet:
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/Cargo.toml --all
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/fixtures/Cargo.toml --all

fmt-check-typed-ratchet:
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/Cargo.toml --all --check
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/fixtures/Cargo.toml --all --check

# Tooling packages use the same bounded scheduler and command, with their pinned compiler.
lint-typed-ratchet jobs=default_jobs: (run-with-jobs "typed-ratchet-clippy-targets" jobs)

[private]
typed-ratchet-clippy-targets: typed-ratchet-clippy-packages

[private, parallel]
typed-ratchet-clippy-packages: *(clippy-target *["nervix-lint-driver", "nervix-lint-report"] ["--manifest-path", "tools/nervix-lint/Cargo.toml", "--all-targets"] "nightly-2026-09-17")

coverage-typed-ratchet-python: test-typed-ratchet-compiler test-typed-ratchet-modeled
    #!/usr/bin/env bash
    set -euo pipefail
    coverage=(uvx --from coverage==7.11.0 coverage)
    export COVERAGE_FILE="{{ cargo_target_dir }}/typed-ratchet/python.coverage"
    "${coverage[@]}" run --branch --source=scripts.typed_ratchet,scripts.typed_lint_wrapper,scripts.ratchet,scripts.native_coverage,scripts.bolero -m unittest scripts.tests.test_typed_ratchet scripts.tests.compiler_fixture_checks scripts.tests.test_ratchet scripts.tests.test_native_coverage scripts.tests.test_bolero
    "${coverage[@]}" lcov -o "{{ cargo_target_dir }}/typed-ratchet/python.lcov"
    "${coverage[@]}" report

# Format and validate, with four concurrent recipe bodies by default.
validate jobs=default_jobs: fetch-onnxruntime (run-with-jobs "validate-targets" jobs)

[private]
validate-targets: fmt lint-targets test-onnxruntime-tooling validate-skill validate-nspl-docs validate-clock-boundaries validate-typed-errors validate-primitive-boundary validate-shuttle-dependencies validate-turmoil-dependencies validate-loom-dependencies validate-execution-mode-conflicts validate-dns-dependencies ratchet

# Check each connector as a consumer root. Cargo tree limits feature unification to that root;
# the full workspace build alone can hide a missing resolver feature in a leaf connector.
validate-dns-dependencies:
    #!/usr/bin/env bash
    set -euo pipefail
    for package in nervix-connector-http nervix-connector-prometheus nervix-connector-sentry nervix-connector-otel nervix-connector-iceberg; do
        graph="$(cargo tree --package "${package}" --edges normal --format '{p} {f}' --prefix none)"
        if ! rg -q '^reqwest v0\.13\.[0-9]+ .*hickory-dns' <<< "${graph}"; then
            echo "${package} lacks Reqwest 0.13 Hickory DNS" >&2
            exit 1
        fi
        if [[ "${package}" == nervix-connector-iceberg ]] && \
            ! rg -q '^reqwest v0\.12\.[0-9]+ .*hickory-dns' <<< "${graph}"; then
            echo "${package} lacks Reqwest 0.12 Hickory DNS" >&2
            exit 1
        fi
        if [[ "${package}" == nervix-connector-iceberg ]] && \
            rg -q '^reqwest v0\.12\.[0-9]+ [^ ]*__rustls-ring' <<< "${graph}"; then
            echo "${package} selected Reqwest 0.12's Ring TLS provider" >&2
            exit 1
        fi
    done
    # RabbitMQ resolves through the node's Hickory resolver and hands Lapin the transport it
    # established. Neither the connector on its own nor the server may select the process-wide
    # Hickory resolver behind Lapin's own feature, and Lapin's TLS stays on AWS-LC.
    for package in nervix-connector-rabbitmq nervix-server; do
        graph="$(cargo tree --package "${package}" --edges normal --format '{p} {f}' --prefix none)"
        if ! rg -q '^nervix-dns v' <<< "${graph}" || \
            ! rg -q '^hickory-resolver v0\.26\.[0-9]+ .*tokio' <<< "${graph}"; then
            echo "${package} lacks the node resolver RabbitMQ connects through" >&2
            exit 1
        fi
        if rg -q '^(lapin|amq-protocol|amq-protocol-tcp|async-rs) v[^ ]+ .*hickory-dns' <<< "${graph}"; then
            echo "${package} selected Lapin's process-wide Hickory resolver" >&2
            exit 1
        fi
        if ! rg -q '^tcp-stream v[^ ]+ .*rustls--aws_lc_rs' <<< "${graph}" || \
            rg -q '^tcp-stream v[^ ]+ .*rustls--ring' <<< "${graph}"; then
            echo "${package} does not select AWS-LC for RabbitMQ TLS" >&2
            exit 1
        fi
    done
    # Syslog, WebSocket, Redis and MQTT transports resolve through the node resolver, even when each
    # connector is built without the server's feature graph.
    for package in nervix-connector-syslog nervix-connector-websockets nervix-connector-redis nervix-connector-mqtt; do
        graph="$(cargo tree --package "${package}" --edges normal --format '{p} {f}' --prefix none)"
        if ! rg -q '^nervix-dns v' <<< "${graph}" || \
            ! rg -q '^hickory-resolver v0\.26\.[0-9]+ .*tokio' <<< "${graph}"; then
            echo "${package} lacks the node resolver for its outbound connections" >&2
            exit 1
        fi
    done
    graph="$(cargo tree --package nervix-connector-redis --edges normal --format '{p} {f}' --prefix none)"
    if ! rg -q '^redis v1\.[0-9]+\.[0-9]+ .*tokio-rustls-comp' <<< "${graph}" || \
        rg -q '^rustls v[^ ]+ (.*,)?ring(,|$)' <<< "${graph}"; then
        echo "nervix-connector-redis lacks Redis's AWS-LC TLS path" >&2
        exit 1
    fi
    # MQTT dials each resolved address with rumqttc's own per-address dialer from rumqttc-core,
    # and the driver still completes TLS on the stream, on AWS-LC alone.
    graph="$(cargo tree --package nervix-connector-mqtt --edges normal --format '{p} {f}' --prefix none)"
    if ! rg -q '^rumqttc-v5-next v[^ ]+ (.*,)?use-rustls-aws-lc(,|$)' <<< "${graph}" || \
        rg -q '^rustls v[^ ]+ (.*,)?ring(,|$)' <<< "${graph}"; then
        echo "nervix-connector-mqtt lacks rumqttc's AWS-LC TLS path" >&2
        exit 1
    fi
    # ClickHouse and SQS hand the node resolver to their drivers' own DNS hooks, Hyper's connector
    # and Smithy's HTTP client, even when built without the server's feature graph, and complete
    # TLS with AWS-LC alone.
    for package in nervix-connector-clickhouse nervix-connector-sqs; do
        graph="$(cargo tree --package "${package}" --edges normal --format '{p} {f}' --prefix none)"
        if ! rg -q '^nervix-dns v' <<< "${graph}" || \
            ! rg -q '^hickory-resolver v0\.26\.[0-9]+ .*tokio' <<< "${graph}"; then
            echo "${package} lacks the node resolver for its outbound connections" >&2
            exit 1
        fi
        if rg -q '^rustls v[^ ]+ (.*,)?ring(,|$)' <<< "${graph}"; then
            echo "${package} selected Rustls's Ring provider" >&2
            exit 1
        fi
    done
    graph="$(cargo tree --package nervix-connector-sqs --edges normal --format '{p} {f}' --prefix none)"
    if ! rg -q '^aws-smithy-http-client v[^ ]+ (.*,)?rustls-aws-lc(,|$)' <<< "${graph}" || \
        rg -q '^aws-smithy-http-client v[^ ]+ (.*,)?(rustls-ring|legacy-rustls-ring|s2n-tls)(,|$)' <<< "${graph}"; then
        echo "nervix-connector-sqs does not select AWS-LC alone for its Smithy HTTP client" >&2
        exit 1
    fi
    # MongoDB keeps its driver's Hickory SRV and TXT discovery; the driver still resolves the
    # addresses it connects to through Tokio.
    graph="$(cargo tree --package nervix-connector-mongodb --edges normal --format '{p} {f}' --prefix none)"
    if ! rg -q '^mongodb v3\.[0-9]+\.[0-9]+ .*dns-resolver' <<< "${graph}"; then
        echo "nervix-connector-mongodb lacks MongoDB's Hickory SRV and TXT discovery" >&2
        exit 1
    fi

# Check formatting and validate with the same concurrency default as local validation.
validate-ci jobs=default_jobs: fetch-onnxruntime (run-with-jobs "validate-ci-targets" jobs)

[private]
validate-ci-targets: fmt-check lint-targets test-onnxruntime-tooling validate-skill validate-nspl-docs validate-clock-boundaries validate-typed-errors validate-primitive-boundary validate-shuttle-dependencies validate-turmoil-dependencies validate-loom-dependencies validate-execution-mode-conflicts validate-dns-dependencies ratchet

# Hold every atomic to nervix-primitives and every mode feature to its owner. The check rejects a
# direct, renamed, grouped, qualified, glob, alias or macro path to another backend's atomics, a
# selected atomic held by a static or constructed in a const context, an unmodeled atomic without
# its permission, a stale permission, a `loom` dependency outside its owner and harness, and a mode
# feature that is not forwarded. The check's own tests run first, so a rule that stopped rejecting
# its bypass fails here too.
validate-primitive-boundary:
    python3 -m unittest --quiet scripts.tests.test_check_primitive_boundary
    python3 -m scripts.check_primitive_boundary

# Shuttle's runner and synchronization wrappers belong only to modeled builds. Production package
# graphs use the real synchronization crates directly and contain no Shuttle package.
validate-shuttle-dependencies:
    #!/usr/bin/env bash
    set -euo pipefail
    production_dependencies="$(
        cargo tree --workspace --edges normal --no-default-features --prefix none
    )"
    if printf '%s\n' "${production_dependencies}" \
        | grep -E '^shuttle([[:space:]-]|$)'; then
        echo "the production workspace includes a Shuttle package" >&2
        exit 1
    fi

# The normal workspace graph, with or without default features, must not pull the optional
# simulation scheduler into production.
validate-turmoil-dependencies:
    #!/usr/bin/env bash
    set -euo pipefail
    production_graphs=(
        "$(cargo tree --workspace --edges normal --prefix none)"
        "$(cargo tree --workspace --edges normal --no-default-features --prefix none)"
    )
    for production_graph in "${production_graphs[@]}"; do
        if printf '%s\n' "${production_graph}" | grep -E '^turmoil([[:space:]-]|$)'; then
            echo "the production workspace includes Turmoil" >&2
            exit 1
        fi
    done

# Loom belongs only to modeled builds. Neither the workspace nor any package built on its own, the way
# a consumer builds it, contains Loom in its normal dependency graph, with default features or
# without them.
validate-loom-dependencies:
    #!/usr/bin/env bash
    set -euo pipefail
    mapfile -t packages < <(
        cargo metadata --no-deps --format-version 1 \
            | python3 -c 'import json, sys; print("\n".join(p["name"] for p in json.load(sys.stdin)["packages"]))'
    )
    roots=(--workspace)
    for package in "${packages[@]}"; do
        roots+=("--package ${package}")
    done
    for root in "${roots[@]}"; do
        for defaults in "" --no-default-features; do
            graph="$(cargo tree ${root} --edges normal ${defaults} --prefix none)"
            if printf '%s\n' "${graph}" | grep -E '^loom v'; then
                echo "the normal dependency graph of ${root} ${defaults} includes Loom" >&2
                exit 1
            fi
        done
    done
    echo "no ordinary graph of the workspace or its ${#packages[@]} packages contains Loom"

# Keep one precise diagnostic when execution modes are selected together. nervix-primitives owns
# the rejection, so every pair of `loom`, `shuttle` and `turmoil`, and all three, fail there with the
# modes named, including when separate dependencies enable them.
validate-execution-mode-conflicts:
    #!/usr/bin/env bash
    set -euo pipefail
    diagnostics="$(mktemp)"
    trap 'rm -f "${diagnostics}"' EXIT
    expect_conflict() {
        local package="$1" features="$2"
        shift 2
        if cargo check --package "${package}" --features "${features}" --lib \
            >"${diagnostics}" 2>&1; then
            echo "${package} with ${features}: the modes unexpectedly compiled together" >&2
            exit 1
        fi
        for pair in "$@"; do
            if ! grep -Fq "the ${pair} execution modes cannot be enabled together" "${diagnostics}"; then
                cat "${diagnostics}" >&2
                echo "${package} with ${features}: no diagnostic naming ${pair}" >&2
                exit 1
            fi
        done
    }
    expect_conflict nervix-primitives 'loom shuttle' '`loom` and `shuttle`'
    expect_conflict nervix-primitives 'loom turmoil' '`loom` and `turmoil`'
    expect_conflict nervix-primitives 'shuttle turmoil' '`shuttle` and `turmoil`'
    expect_conflict nervix-primitives 'loom shuttle turmoil' \
        '`loom` and `shuttle`' '`loom` and `turmoil`' '`shuttle` and `turmoil`'
    # Two packages each select one mode; Cargo unifies both onto the owner.
    expect_conflict nervix-interconnect 'shuttle nervix-execution/turmoil' \
        '`shuttle` and `turmoil`'
    expect_conflict nervix-execution 'loom nervix-primitives/shuttle' '`loom` and `shuttle`'

validate-clock-boundaries:
    python3 scripts/check_clock_boundaries.py

# Reject `Result<_, String>` in product code. A typed error is a rule, not a count, so there is no
# baseline to raise: any occurrence fails and names the rule.
validate-typed-errors:
    python3 -m scripts.check_typed_errors

toolchains-install:
    rustup toolchain install
    rustup toolchain install nightly-2026-09-17 --profile minimal --component rustc-dev --component rust-src --component rustfmt --component clippy --component llvm-tools

# Run every test target of the NSPL language and its formatter: unit, integration, completion-walk
# unit and documentation tests. The walk itself is a separate gate, `nspl-completion-walk`.
test-nspl *args:
    cargo test --package nervix-nspl --package nervix-nspl-format --all-targets -- {{ args }}

# Parse every runnable NSPL block in the documentation directly through the parser crate. Syntax
# synopses and statement fragments remain NSPL-labelled but opt out explicitly with `nspl,ignore`.
validate-nspl-docs:
    cargo test -p nervix-nspl --test documentation -- --nocapture

# Check the shared R2 configuration used by native CI jobs and Docker builds.
test-kache-ci:
    uv run --locked python -m unittest scripts.tests.test_kache_ci

test-docs:
    uv run --locked python -m unittest discover -s scripts/tests -p "test_*.py"
    node --test cloudflare/docs-worker/src/index.test.js

# Screenshots are recaptured from the current console and the nervix-cli reference chapter is
# rendered from the current binary, so a published book can never describe an older build.
book version="0.1.0-dev": test-docs docs-screenshots
    python scripts/build_book.py --version {{ version }}

validate-skill:
    env GH_PROMPT_DISABLED=1 gh skill publish .agents/skills --dry-run

book-pdf version="0.1.0-dev" output="":
    #!/usr/bin/env bash
    set -euo pipefail
    just book "{{ version }}"
    if ! command -v pandoc >/dev/null 2>&1; then
        echo "pandoc is required for book-pdf" >&2
        exit 1
    fi
    if [[ -n "{{ output }}" ]]; then
        output_path="{{ output }}"
    else
        output_path="docs/book/nervix.pdf"
    fi
    tmp_html="$(mktemp --suffix=.html)"
    tmp_title="$(mktemp --suffix=.tex)"
    trap 'rm -f "${tmp_html}" "${tmp_title}"' EXIT
    python3 scripts/render_pdf_title.py \
        --template docs/theme/nervix-pdf-title.tex \
        --version "{{ version }}" \
        --output "${tmp_title}"
    python3 scripts/prepare_pdf_html.py \
        --print-html docs/book/print.html \
        --summary docs/src/SUMMARY.md \
        --output "${tmp_html}"
    pandoc \
        --from=html \
        --to=pdf \
        --pdf-engine=xelatex \
        --top-level-division=chapter \
        --variable=documentclass:report \
        --include-in-header=docs/theme/nervix-pdf-header.tex \
        --include-before-body="${tmp_title}" \
        --variable=graphics \
        --variable=colorlinks \
        --variable=linkcolor:NervixLink \
        --variable=urlcolor:NervixLink \
        --variable=filecolor:NervixLink \
        --variable=citecolor:NervixLink \
        --variable=toccolor:black \
        --toc \
        --toc-depth=2 \
        --variable=geometry:margin=0.8in \
        --variable=linestretch:1.08 \
        --variable=mainfont:"Noto Serif" \
        --variable=sansfont:"Noto Sans" \
        --variable=monofont:"Noto Sans Mono" \
        --variable=CJKmainfont:"Noto Serif CJK JP" \
        --variable=CJKsansfont:"Noto Sans CJK JP" \
        --variable=CJKmonofont:"Noto Sans Mono CJK JP" \
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
    just book-pdf {{ target }} docs/book/nervix.pdf
    just publish-dir docs/book {{ target }} {{ zone_id }} {{ alias }} {{ bucket }}
    just worker-deploy {{ zone_id }}

deps:
    just generate-dev-tls
    docker compose up -d --build --wait --wait-timeout 90
    docker compose run --rm fake-gcs-init
    docker compose run --rm azurite-init

deps-down:
    docker compose down --remove-orphans --volumes

# Run black-box cluster scenarios against an explicitly supplied, already-built Nervix image.
chaos *args:
    bash scripts/chaos/chaos.sh {{ args }}

server *args: fetch-onnxruntime build-web-console generate-dev-tls
    NERVIX_NODE_ID="${NERVIX_NODE_ID:-node-1}" \
    NERVIX_INTERCONNECT_TLS_CA="${NERVIX_INTERCONNECT_TLS_CA:-tls/dev/ca.pem}" \
    NERVIX_INTERCONNECT_TLS_CERT="${NERVIX_INTERCONNECT_TLS_CERT:-tls/dev/node.pem}" \
    NERVIX_INTERCONNECT_TLS_KEY="${NERVIX_INTERCONNECT_TLS_KEY:-tls/dev/node-key.pem}" \
    cargo run --package nervix-server --bin nervix-server -- {{ args }}

client *args:
    cargo run --package nervix-cli -- {{ args }}

build-web-console:
    #!/usr/bin/env bash
    set -euo pipefail
    cd crates/web-console
    env -u NO_COLOR trunk build --release

build-server: fetch-onnxruntime build-web-console
    CARGO_TARGET_DIR={{ cargo_target_dir }}/server cargo build {{ release_flag }} --package nervix-server --bin nervix-server

build-cli:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/cli cargo build {{ release_flag }} --package nervix-cli --bin nervix-cli

# Install the server, CLI, and NSPL formatter with their required build artifacts.
install *args: (install-server args) (install-cli args) (install-nspl-format args)

install-server *args: fetch-onnxruntime build-web-console
    CARGO_TARGET_DIR={{ quote(cargo_target_dir) }} cargo install --locked --path . {{ args }}

install-cli *args:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir) }} cargo install --locked --path crates/nervix-cli {{ args }}

install-nspl-format *args:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir) }} cargo install --locked --path crates/nspl-format {{ args }}

uninstall:
    cargo uninstall nervix-server nervix-cli nervix-nspl-format

[parallel]
build-apps: build-cli build-server

build-all: generate-dev-tls build-apps

wasm-processor-rust-guest:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build \
        --manifest-path examples/wasm-processors/rust-guest/Cargo.toml \
        --target wasm32-unknown-unknown \
        --release
    test -s examples/wasm-processors/rust-guest/target/wasm32-unknown-unknown/release/nervix_wasm_processor_rust_guest.wasm

# DB-IP publishes one City Lite build per month, so the current month is tried first and the
# previous one is the fallback for the days before a new build appears.
download-datalake-dbip:
    #!/usr/bin/env bash
    set -euo pipefail
    destination="examples/datalake/geo-wasm-guest/dbip-city-lite.mmdb.gz"
    if [[ -s "${destination}" ]]; then
        exit 0
    fi
    year="$(date -u +%Y)"
    month="$(date -u +%m)"
    months=()
    for _ in 1 2; do
        months+=("${year}-${month}")
        if [[ "${month}" == "01" ]]; then
            year="$((10#${year} - 1))"
            month="12"
        else
            month="$(printf '%02d' "$((10#${month} - 1))")"
        fi
    done
    mkdir -p "$(dirname "${destination}")"
    tmp="$(mktemp "${destination}.tmp.XXXXXX")"
    trap 'rm -f "${tmp}"' EXIT
    for release in "${months[@]}"; do
        url="https://download.db-ip.com/free/dbip-city-lite-${release}.mmdb.gz"
        if curl -L --proto '=https' --tlsv1.2 -sSf "${url}" -o "${tmp}"; then
            echo "downloaded DB-IP City Lite ${release}"
            mv "${tmp}" "${destination}"
            exit 0
        fi
    done
    echo "no DB-IP City Lite build available for ${months[*]}" >&2
    exit 1

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
    # TinyGo compiles against the standard library of the Go toolchain that `go` selects, and
    # automatic selection never moves below a newer host Go, so the go.mod toolchain is forced.
    toolchain="$(sed -n 's/^toolchain //p' go.mod)"
    test -n "${toolchain}"
    GOTOOLCHAIN="${toolchain}" tinygo build \
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

generate-test-onnx output="tests/fixtures/onnx/simple_score.onnx" alternate_output="tests/fixtures/onnx/alternate_score.onnx" batch_output="tests/fixtures/onnx/batch_score.onnx" f64_output="tests/fixtures/onnx/f64_score.onnx" matrix_output="tests/fixtures/onnx/matrix_identity.onnx" dynamic_batch_output="tests/fixtures/onnx/dynamic_batch_score.onnx" scalar_output="tests/fixtures/onnx/scalar_identity.onnx":
    python3 scripts/train_simple_onnx.py --output {{ output }} --alternate-output {{ alternate_output }} --batch-output {{ batch_output }} --f64-output {{ f64_output }} --matrix-output {{ matrix_output }} --dynamic-batch-output {{ dynamic_batch_output }} --scalar-output {{ scalar_output }}

# Fetch the published static artifact over public R2 HTTPS without credentials, or reuse its verified local copy; never compile.
fetch-onnxruntime platform="native" *args:
    uv run --locked python -m scripts.onnxruntime.artifacts fetch --platform {{ quote(platform) }} {{ args }}

# Maintainer only: compile directly with local LLVM; use [platform] --force to rebuild a completed artifact.
# Normal development and CI depend on fetch-onnxruntime.
build-onnxruntime platform="native" *args:
    uv run --locked python -m scripts.onnxruntime.artifacts build --platform {{ quote(platform) }} {{ args }}

# Verify the prepared package through native inference; CUDA verification requires a GPU.
verify-onnxruntime platform="native" *args: (fetch-onnxruntime platform args)
    uv run --locked python -m scripts.onnxruntime.artifacts verify --platform {{ quote(platform) }} {{ args }}

# Manual task: compile external artifacts and pin their checksums for local use.
# Normal development and CI recipes never depend on this task.
build-artifacts platform="native" *args: (build-onnxruntime platform args)

# Maintainer only: upload the completed, locally pinned artifact to R2.
publish-onnxruntime platform="native" *args:
    uv run --locked python -m scripts.onnxruntime.artifacts publish --platform {{ quote(platform) }} {{ args }}

[private]
prepare-server-package package:
    @if [ {{ quote(package) }} = nervix-server ]; then just fetch-onnxruntime; fi

# Print the absolute library directory of a validated, prepared ONNX Runtime package.
onnxruntime-path platform="native":
    python3 -m scripts.onnxruntime.artifacts path --platform {{ quote(platform) }}

reset-local-dashboard-state:
    #!/usr/bin/env bash
    set -euo pipefail
    rm -rf .nervix-db

cluster-dashboard: build-all
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
    cli_bin_dir="${target_dir}/cli/{{ build_mode }}"
    server_bin_dir="${target_dir}/server/{{ build_mode }}"
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

[private]
validate-docker-kache:
    #!/usr/bin/env bash
    set -euo pipefail
    : "${KACHE_S3_BUCKET:?KACHE_S3_BUCKET is required}"
    : "${KACHE_S3_REGION:?KACHE_S3_REGION is required}"
    : "${KACHE_S3_ENDPOINT:?KACHE_S3_ENDPOINT is required}"
    : "${KACHE_S3_ACCESS_KEY:?KACHE_S3_ACCESS_KEY is required}"
    : "${KACHE_S3_SECRET_KEY:?KACHE_S3_SECRET_KEY is required}"

docker-build-debian debian_version="trixie" llvm_version="23" tag="nervix:debian" platform="linux/amd64" push="false" cache_from="" cache_to="": validate-docker-kache (fetch-onnxruntime platform) (docker-prepare-qemu platform)
    #!/usr/bin/env bash
    set -euo pipefail
    normalized_platform="{{ platform }}"
    if [[ "${normalized_platform}" == "linux/aarch64" ]]; then
        normalized_platform="linux/arm64"
    fi
    case "${normalized_platform}" in
        linux/amd64|linux/arm64) ;;
        *) echo "unsupported ONNX Runtime platform: ${normalized_platform}" >&2; exit 1 ;;
    esac
    onnx_lib_dir="$(just onnxruntime-path "${normalized_platform}")"
    onnx_package_dir="$(dirname "${onnx_lib_dir}")"
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
        --build-context "onnxruntime=${onnx_package_dir}" \
        --progress=plain \
        --platform "${normalized_platform}" \
        --build-arg "KACHE_VERSION=${KACHE_VERSION:-0.28.1}" \
        --build-arg RUST_VERSION={{ rust_toolchain_version }} \
        --build-arg DEBIAN_VERSION={{ debian_version }} \
        --build-arg LLVM_VERSION={{ llvm_version }} \
        --build-arg "KACHE_S3_BUCKET=${KACHE_S3_BUCKET}" \
        --build-arg "KACHE_S3_REGION=${KACHE_S3_REGION}" \
        --build-arg "KACHE_S3_ENDPOINT=${KACHE_S3_ENDPOINT}" \
        --build-arg "KACHE_S3_ACCESS_KEY=${KACHE_S3_ACCESS_KEY}" \
        --build-arg "KACHE_S3_SECRET_KEY=${KACHE_S3_SECRET_KEY}" \
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

# Record timing and allocations for every repaired recurring task dependency.
bench-task-handles output="target/task-handles.json": fetch-onnxruntime build-web-console
    cargo bench --package nervix-server --bench task_handles --features benchmarks -- {{ quote(output) }}

coverage-task-handles output="target/task-handles.lcov" report="target/task-handles-coverage.json": fetch-onnxruntime
    cargo llvm-cov --bench task_handles --features benchmarks --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ quote(report) }}

# Inspect a benchmark harness from an existing successful instrumented run without rebuilding it.
coverage-task-handles-export executable profile output="target/task-handles-benchmark.lcov":
    #!/usr/bin/env bash
    set -euo pipefail
    llvm_bin="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin"
    "${llvm_bin}/llvm-profdata" merge -sparse {{ quote(profile) }} -o target/task-handles-benchmark.profdata
    "${llvm_bin}/llvm-cov" export {{ quote(executable) }} --instr-profile=target/task-handles-benchmark.profdata --format=lcov > {{ quote(output) }}
