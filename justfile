set minimum-version := "1.56.0"
set unstable
set lists

export RUSTUP_AUTO_INSTALL := "0"

# CI builds do not need incremental compilation artifacts.
export CARGO_INCREMENTAL := if env("CI", []) != [] { "0" } else { env("CARGO_INCREMENTAL", []) }

rust_toolchain_version := shell("python3 -c 'import pathlib, tomllib; print(tomllib.loads(pathlib.Path(\"rust-toolchain.toml\").read_text())[\"toolchain\"][\"channel\"])'")
rustflags := env('RUSTFLAGS', '')
build_mode := "debug"
release_flag := if build_mode == "release" { "--release" } else { "" }
cargo_target_dir := env("CARGO_TARGET_DIR", justfile_directory() + "/target")
# Java fixtures belong to the caller's build root, including when LLVM or Bolero builds Rust
# in a separate instrumented directory.
export NERVIX_CLIENT_JVM_TEST_ARTIFACTS := env("NERVIX_CLIENT_JVM_TEST_ARTIFACTS", cargo_target_dir + "/client-conformance/java")
turmoil_failures := cargo_target_dir + "/turmoil-failures"
coverage_tools_dir := cargo_target_dir + "/llvm-cov-tools"
default_jobs := num_jobs() || "4"

# Show the documented recipes, including the Bolero property and fuzz commands.
help:
    just --list

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

# Build the pinned cargo-crap, which the complexity count and the CRAP report run, into a root of its
# own below the target directory. nervix-ratchet crap declares the version; a build already there
# is kept.
install-cargo-crap: build-ratchet-runner
    {{ quote(cargo_target_dir / "typed-ratchet/source/debug/nervix-ratchet") }} crap install --root {{ quote(justfile_directory()) }} --target-dir {{ quote(cargo_target_dir) }}

# Server library properties embed the same real console assets as the product.
bolero-deps: build-web-console

# Prepare one declared sanitizer target through the shared build path, without a campaign.
prepare-bolero target: bolero-deps
    BOLERO_BUILD_TIMEOUT_SECONDS=7200 python3 -m scripts.bolero prepare {{ quote(target) }}

# Prepare the inventory's declared harnesses that one sanitizer shard, NUMBER/COUNT, builds.
prepare-bolero-shard shard: bolero-deps
    BOLERO_BUILD_TIMEOUT_SECONDS=7200 python3 -m scripts.bolero prepare-shard {{ quote(shard) }}

# Show how COUNT sanitizer shards divide the inventory's harnesses and targets.
fuzz-plan count="3":
    python3 -m scripts.bolero plan {{ quote(count) }}

# The registry and window properties share this declared server library and feature set.
prepare-archive-counts-fuzz: (prepare-bolero "registry-archived-models")

# Run all registered properties with bounded randomized cases and source-adjacent corpus replay.
test-bolero filter="": bolero-deps
    python3 -m scripts.bolero test {{ quote(filter) }}

# Run selected registered owners with their canonical case, corpus and execution checks. Global
# compiled discovery stays with validate-bolero and CI; local qualification builds only its owners.
test-bolero-focused +filters: bolero-deps
    python3 -c 'import sys; from scripts.bolero import load_inventory, select, test_targets; inventory = load_inventory(); selected = {target.id: target for query in sys.argv[1:] for target in select(inventory, query)}; test_targets(inventory, tuple(selected.values()))' {{ quote(filters) }}

# Compile every ordinary harness `test-bolero` runs, as it builds them, without running a property.
build-bolero filter="": bolero-deps
    python3 -m scripts.bolero build {{ quote(filter) }}

# List every compiled, registered Bolero target after checking the inventory.
fuzz-list: bolero-deps
    python3 -m scripts.bolero list

# Run one target through sanitizer-backed libFuzzer. Duration is in seconds.
fuzz target duration="30": bolero-deps
    python3 -m scripts.bolero fuzz {{ quote(target) }} {{ quote(duration) }}

# Run every target, or one shard NUMBER/COUNT of them, through sanitizer-backed libFuzzer.
# Duration is per target in seconds.
fuzz-all duration="30" shard="1/1": bolero-deps
    python3 -m scripts.bolero fuzz-all {{ quote(duration) }} --shard {{ quote(shard) }}

# Replay the exact saved input through its ordinary property assertion.
fuzz-replay target failure: bolero-deps
    python3 -m scripts.bolero replay {{ quote(target) }} {{ quote(failure) }}

# Replay the exact saved input once through its instrumented sanitizer build, which a sanitizer
# finding needs: the ordinary build does not detect it.
fuzz-replay-sanitizer target failure: bolero-deps
    python3 -m scripts.bolero replay-sanitizer {{ quote(target) }} {{ quote(failure) }}

# Check that the shard campaign records under `evidence` ran every registered target once, then
# write their combined record and merged Rust source report to `output`.
combine-bolero-fuzz evidence output:
    python3 -m scripts.bolero combine {{ quote(evidence) }} {{ quote(output) }}

# Minimize a saved failure with libFuzzer and verify the minimized input still fails.
fuzz-reduce target failure: bolero-deps
    python3 -m scripts.bolero reduce {{ quote(target) }} {{ quote(failure) }}

# Compare the inventory, package declarations, test harness and compiled Bolero targets.
validate-bolero: bolero-deps
    python3 -m scripts.bolero validate

# Check the dedicated Bolero workflow with the pinned Actions linter.
validate-bolero-workflow: (validate-workflows ".github/workflows/bolero.yaml")

# Check selected Actions workflows, or all workflows when no paths are supplied.
validate-workflows *workflows:
    go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 {{ workflows }}

# Exercise deployment identity, rollout observations, failure cleanup and the publication gate.
test-operator-deployment:
    python3 -m unittest scripts.tests.test_operator_deployment

# Deploy the shipped operator/chart against one immutable server image, then remove the test cluster.
operator-deployment-check image revision architecture operator_source output:
    python3 -m scripts.operator_deployment --image {{ quote(image) }} --revision {{ quote(revision) }} --architecture {{ quote(architecture) }} --operator-source {{ quote(operator_source) }} --output {{ quote(output) }}

# Keep the deployment controller's ordinary Python counters beside the other tooling reports.
coverage-operator-deployment:
    mkdir -p "{{ cargo_target_dir }}/operator-deployment-coverage"
    uvx --from coverage==7.11.0 coverage run --data-file "{{ cargo_target_dir }}/operator-deployment-coverage/python.coverage" --branch --source=scripts.operator_deployment,scripts.tests.test_operator_deployment -m unittest scripts.tests.test_operator_deployment
    uvx --from coverage==7.11.0 coverage lcov --data-file "{{ cargo_target_dir }}/operator-deployment-coverage/python.coverage" -o "{{ cargo_target_dir }}/operator-deployment-coverage/python.lcov"

# Exercise exact-source CI selection and strict property gates, including validation-only reruns.
test-ci-selection:
    python3 -m unittest scripts.tests.test_ci_test_selection scripts.tests.test_bolero_coverage

# Keep Python policy/contract coverage separate from product property campaigns.
coverage-ci-selection:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/ci-selection
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file target/ci-selection/python.coverage --branch --source=scripts.ci_test_selection,scripts.tests.test_ci_test_selection,scripts.tests.test_bolero_coverage -m unittest scripts.tests.test_ci_test_selection scripts.tests.test_bolero_coverage
    "${coverage[@]}" lcov --data-file target/ci-selection/python.coverage -o target/ci-selection/python.lcov
    "${coverage[@]}" report --data-file target/ci-selection/python.coverage --include=scripts/ci_test_selection.py --fail-under=90

# Qualify every way a run must fail: a property mismatch, a sanitizer finding, a case timeout, an
# engine failure, discovery of ignored, unregistered and missing targets, a selection of nothing
# and a corrupt replay record, with saved inputs, minimization and exact replay.
qualify-bolero: bolero-deps
    python3 -m scripts.bolero qualify

# Exercise the inventory and runner's validation and failure paths, and the runner load recorder
# the sanitizer shards keep their measurements with.
test-bolero-runner: build-web-console
    python3 -m unittest scripts.tests.test_bolero scripts.tests.test_bolero_coverage scripts.tests.test_ci_test_selection scripts.tests.test_record_runner_load scripts.tests.test_wasm_bindgen

# Measure runner edits without launching unrelated product fuzz campaigns.
coverage-bolero-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/bolero
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file target/bolero/runner.coverage --branch --source=scripts.bolero,scripts.bolero_coverage,scripts.build_web_console,scripts.wasm_bindgen,scripts.tests.test_bolero,scripts.tests.test_bolero_coverage -m unittest scripts.tests.test_bolero scripts.tests.test_bolero_coverage scripts.tests.test_wasm_bindgen
    "${coverage[@]}" lcov --data-file target/bolero/runner.coverage -o target/bolero/python-runner.lcov

# Collect distinct Python runner and live Rust source reports, then qualify failure retention.
# The duration is per product target; CI passes 30 and its shard NUMBER/COUNT on PRs labeled `fuzz`.
coverage-bolero duration="2" shard="1/1": bolero-deps
    #!/usr/bin/env bash
    set -euo pipefail
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" erase
    "${coverage[@]}" run --branch --source=scripts.bolero,scripts.bolero_coverage,scripts.build_web_console,scripts.wasm_bindgen,scripts.tests.test_bolero,scripts.tests.test_bolero_coverage -m unittest scripts.tests.test_bolero scripts.tests.test_bolero_coverage scripts.tests.test_wasm_bindgen
    "${coverage[@]}" run --branch -a --source=scripts.bolero,scripts.bolero_coverage,scripts.build_web_console,scripts.wasm_bindgen,scripts.tests.test_bolero,scripts.tests.test_bolero_coverage -m scripts.bolero fuzz-all {{ quote(duration) }} --shard {{ quote(shard) }}
    "${coverage[@]}" run --branch -a --source=scripts.bolero,scripts.bolero_coverage,scripts.build_web_console,scripts.wasm_bindgen,scripts.tests.test_bolero,scripts.tests.test_bolero_coverage -m scripts.bolero qualify
    mkdir -p target/bolero
    "${coverage[@]}" lcov -o target/bolero/python.lcov
    "${coverage[@]}" report --fail-under=80

# Qualify the restore installation properties while measuring the runner during focused iteration.
coverage-bolero-restore duration="30": build-web-console
    #!/usr/bin/env bash
    set -euo pipefail
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" erase
    "${coverage[@]}" run --branch --source=scripts.bolero -m unittest scripts.tests.test_bolero scripts.tests.test_bolero_coverage
    "${coverage[@]}" run --branch -a -m scripts.bolero test restore-installation
    for target in restore-installation-wire restore-installation-storage; do
        "${coverage[@]}" run --branch -a -m scripts.bolero fuzz "${target}" {{ quote(duration) }}
    done
    "${coverage[@]}" run --branch -a -m scripts.bolero qualify
    mkdir -p target/bolero
    "${coverage[@]}" lcov -o target/bolero/python.lcov
    "${coverage[@]}" report --fail-under=80

build-deps: generate-test-onnx download-onnxruntime build-web-console wasm-processor-guests

tests-deps: build-deps build-plexus-format build-test-cli build-paced-simulation build-deadlock-report

build-deadlock-report:
    CARGO_TARGET_DIR={{ cargo_target_dir }} cargo build --package nervix-deadlock --features report-tool --bin nervix-deadlock-report

deadlock-report *args:
    cargo run --quiet --package nervix-deadlock --features report-tool --bin nervix-deadlock-report -- {{ args }}

build-test-cli:
    CARGO_TARGET_DIR={{ cargo_target_dir }} cargo build --package nervix-cli --bin nervix-cli

# Build the runnable drivers and install the native Python package before any scenario starts.
build-paced-simulation: build-paced-simulation-python
    CARGO_TARGET_DIR={{ quote(cargo_target_dir) }} cargo build --package nervix-paced-simulation

# Build the Python driver's wheel in the selected Rust profile and target directory. The wheel
# is the generated input carried to scenario shards; their install step compiles nothing.
build-paced-simulation-python profile="dev" target_dir=cargo_target_dir:
    #!/usr/bin/env bash
    set -euo pipefail
    artifacts={{ quote(cargo_target_dir + "/paced-simulation") }}
    uv venv --clear --quiet --python 3.12 "${artifacts}/python"
    rm -rf "${artifacts}/wheels"
    CARGO_TARGET_DIR={{ quote(target_dir) }} uvx --from maturin==1.16.0 maturin build \
        --manifest-path crates/client-python/Cargo.toml --profile {{ quote(profile) }} \
        --interpreter "${artifacts}/python/bin/python" --out "${artifacts}/wheels"
    just cargo_target_dir={{ quote(cargo_target_dir) }} install-paced-simulation-python

# Install only the built wheel and the Python coverage runtime. This also runs after restoring
# the scenario archive, so its shards need neither Rust nor a wheel build.
install-paced-simulation-python:
    #!/usr/bin/env bash
    set -euo pipefail
    artifacts={{ quote(cargo_target_dir + "/paced-simulation") }}
    wheels=("${artifacts}"/wheels/nervix_client-*.whl)
    if [[ "${#wheels[@]}" != 1 || ! -f "${wheels[0]}" ]]; then
        echo "expected exactly one built Python driver wheel in ${artifacts}/wheels" >&2
        exit 1
    fi
    uv venv --clear --quiet --python 3.12 "${artifacts}/python"
    uv pip install --quiet --python "${artifacts}/python/bin/python" \
        coverage==7.11.0 "${wheels[0]}"
    "${artifacts}/python/bin/python" -c 'import nervix_client'

# Run the unit tests of both paced simulation drivers.
test-paced-simulation *args: test-paced-simulation-python
    cargo test --package nervix-paced-simulation -- {{ args }}

test-paced-simulation-python: build-paced-simulation-python
    {{ quote(cargo_target_dir + "/paced-simulation/python/bin/python") }} -m unittest discover -s examples/paced-simulation/python -p 'test_*.py'

# Deterministic application-open coverage, including refusal and deadline outcomes.
coverage-paced-simulation-python: build-paced-simulation-python
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{ quote(cargo_target_dir + "/paced-simulation") }}
    export COVERAGE_FILE="{{ cargo_target_dir }}/paced-simulation/python.coverage"
    {{ quote(cargo_target_dir + "/paced-simulation/python/bin/python") }} -m coverage run --branch \
        --source=examples/paced-simulation/python,scripts/paced_simulation_coverage \
        -m unittest discover -s examples/paced-simulation/python -p 'test_*.py'
    uvx --from coverage==7.11.0 coverage lcov -o "{{ cargo_target_dir }}/paced-simulation/python.lcov"

# Run the paced simulation's Rust driver against a running node, for example
# `just paced-simulation --server http://127.0.0.1:47391 --ticks 100`.
paced-simulation *args:
    cargo run --release --package nervix-paced-simulation -- {{ args }}

# Run the Python driver through the native package, with the Rust driver's application options.
paced-simulation-python *args:
    just cargo_target_dir={{ quote(cargo_target_dir) }} build-paced-simulation-python release
    {{ quote(cargo_target_dir + "/paced-simulation/python/bin/python") }} examples/paced-simulation/python/paced_simulation.py {{ quote(args) }}

test: tests-deps build-client-jvm-tests
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    # Execution-mode features replace primitives and are valid only inside their runner, and the
    # modes cannot be enabled together. Test the packages that own a mode with ordinary primitives
    # here; `test-shuttle`, `test-loom`, `test-turmoil`, `test-deloxide` and `test-primitives`
    # exercise the modes.
    mode_packages=(
        nervix-client-core
        'nervix-connector*'
        nervix-consensus
        nervix-deadlock
        nervix-execution
        nervix-interconnect
        nervix-model-harness
        nervix-paced-simulation
        nervix-primitives
        nervix-rate-limit
        nervix-scenarios
        nervix-server
        nervix-wasm
    )
    workspace_exclusions=()
    for package in "${mode_packages[@]}"; do
        workspace_exclusions+=(--exclude "${package}")
    done
    cargo test --all-targets --all-features --workspace "${workspace_exclusions[@]}"
    cargo test --lib --features testing --package nervix-server
    cargo test --all-targets --package nervix-scenarios
    cargo test --all-targets \
        --package nervix-client-core \
        --package 'nervix-connector*' \
        --package nervix-consensus \
        --package nervix-deadlock \
        --package nervix-execution \
        --package nervix-interconnect \
        --package nervix-model-harness \
        --package nervix-paced-simulation \
        --package nervix-rate-limit \
        --package nervix-wasm
    cargo test --all-targets --features native --package nervix-primitives
    just test-capability-docs
    just test-turmoil
    just cargo_target_dir={{ quote(cargo_target_dir) }} test-client-python-api
    just test-client-suite-actions

# Check capability examples with rustdoc, including runtime exports enabled only for tests.
test-capability-docs: build-web-console
    cargo test --package nervix-server --package nervix-roto --package nervix-vm --package nervix-models \
        --package nervix-consensus --package nervix-dns --package nervix-recovery \
        --package nervix-simd-kernels --features nervix-server/testing --doc

# Exercise scenario argument forwarding without compiling or starting dependencies.
test-scenario-arguments:
    python3 -m unittest scripts.tests.test_scenario_arguments

# Qualify the generated inputs and binary archive restored by scenario shards.
test-scenario-shards:
    python3 -m unittest scripts.tests.test_scenario_shards

# Prepare the public scenario executable without spending a runtime check's execution budget.
build-scenarios: tests-deps
    cargo test --package nervix-scenarios --test scenarios --no-run

test-scenarios *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- {{ quote(args) }}

# Endpoint inspection/refusal cases use in-process nodes and the native session client.
test-endpoint-heads *args: build-deps build-plexus-format build-test-cli build-deadlock-report
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input tests/features/runtime/kinded_endpoint_heads.feature {{ quote(args) }}

# Exercise emitter destination keys and partitions without unrelated client-tool preparation.
test-emitter-destinations *args: build-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input {{ quote(justfile_directory() / "tests/features/runtime/emitter_destinations*.feature") }} {{ quote(args) }}

# Qualify SYSLOG's existing batch outline and exact single-message and singleton-batch bytes.
test-syslog-encoding *args: build-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input {{ quote(justfile_directory() / "tests/features/runtime/{emitter_batch_containers,syslog_encoding}.feature") }} --tags '@emitter_batch_syslog or @syslog_encoding' {{ quote(args) }}

# These checks reuse the scenario executable for both embedded nodes and a server process.
test-inferencer-state *args: build-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input tests/features/runtime/inferencer_state.feature {{ quote(args) }}

# Exercise processor and generator partition ownership through the public interface.
test-processor-partitions *args: build-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input tests/features/runtime/processor_partitions.feature {{ quote(args) }}

# Observe batching through the public session and the owning node's metric endpoint.
test-emitter-batch-metrics *args: build-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input {{ quote(justfile_directory() / "tests/features/runtime/{emitter_batch_metrics,emitter_batch_metrics}.feature") }} {{ quote(args) }}

# Native measurements keep their existing real transport and payload assertions.
test-emitter-native-metrics *args: build-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input {{ quote(justfile_directory() / "tests/features/runtime/{client_emitters,emitter_batch_databases,http_emitter,iceberg_emission,otel_export_retries}.feature") }} --tags '@emitter_payload_observations' {{ quote(args) }}

# Complete batch values and explicit sensitivity leakage through one- and three-node public graphs.
build-emitter-batch-boundaries: build-deps
    cargo test --package nervix-scenarios --test scenarios --no-run

test-emitter-batch-boundaries *args: build-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- \
        --input 'tests/features/runtime/{emitter_batch_containers,sensitive_data}.feature' \
        --name 'ON EMITTING BATCH maps, wraps and expands the array of member values|Sensitive external emission requires explicit leakage' \
        {{ quote(args) }}

# Exercise NATS authentication through real brokers and one-/three-node public graphs.
test-nats-authentication *args: build-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input tests/features/runtime/nats_authentication.feature {{ quote(args) }}

# Exercise node clock delivery without preparing unrelated client drivers or CLI fixtures.
test-domain-clock-delivery *args: build-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input tests/features/runtime/domain_clock_delivery_attachment.feature {{ quote(args) }}

# Qualify request-based command progress and clock attachment with in-process node prerequisites.
test-progress-qualification *args: build-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- \
        --input 'tests/features/{cluster/application_progress,cluster/interconnect_health,runtime/domain_clock_delivery_attachment}.feature' \
        {{ quote(args) }}

# Run the native-client and CLI foreign-reference witnesses with only the tools they start.
test-client-foreign-execution: (test-client-recovery-witnesses "client-foreign-execution" "(A native command|The CLI) recovers its own execution after a foreign command reply")

# Run the native-client and CLI reply-kind witnesses with only the tools they start.
test-client-reply-kind: (test-client-recovery-witnesses "client-reply-kind" "(A native command|The CLI) recovers its own execution after a reply of another kind")

# Its output stays under the target directory's `witnesses` folder.
# Run the four one- and three-node native-client and CLI outlines `name` selects, with their tools.
test-client-recovery-witnesses witnesses name: build-web-console download-onnxruntime build-test-cli
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    mkdir -p {{ quote(cargo_target_dir + "/" + witnesses) }}
    output="$(mktemp {{ quote(cargo_target_dir + "/" + witnesses + "/run.XXXXXXXX.log") }})"
    result=0
    cargo test --package nervix-scenarios --test scenarios -- --name {{ quote(name) }} --retry 0 | tee "$output" || result=$?
    python3 - "$output" {{ quote(witnesses) }} <<'PY'
    import re
    import sys
    from pathlib import Path
    output = re.sub(r"\x1b\[[0-9;]*m", "", Path(sys.argv[1]).read_text())
    counts = re.findall(r"^(\d+) scenarios?(?: \(|$)", output, re.MULTILINE)
    if counts != ["4"]:
        raise SystemExit(f"{sys.argv[2]} witnesses must execute four scenarios; reported counts: {counts}")
    PY
    exit "$result"

# Exercise real native/CLI restores through a bounded reply-corrupting peer without preparing
# unrelated simulation drivers. The ordinary scenario runner and its lifecycle stay in charge.
test-restore-codec-replies *args: build-deps build-test-cli build-deadlock-report
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    mkdir -p {{ quote(cargo_target_dir + "/restore-codec-replies") }}
    restore_codec_log="$(mktemp {{ quote(cargo_target_dir + "/restore-codec-replies/run.XXXXXX.log") }})"
    cargo test --package nervix-scenarios --test scenarios -- --tags @client_restore_codec_rejection {{ quote(args) }} 2>&1 | tee "$restore_codec_log"
    python3 - "$restore_codec_log" <<'PY'
    import re
    import sys
    from pathlib import Path
    output = re.sub(r"\x1b\[[0-9;]*m", "", Path(sys.argv[1]).read_text())
    counts = re.findall(r"^(\d+) scenarios?(?: \(|$)", output, re.MULTILINE)
    if counts != ["8"]:
        raise SystemExit(f"restore reply witnesses must execute eight scenarios; reported counts: {counts}")
    PY

# Exercise later archive failures after a completed restore lost its answer, using only the
# tools these native/CLI cases start and requiring all four topology/client examples.
test-restore-attempt-history *args: build-deps build-test-cli build-deadlock-report
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    mkdir -p {{ quote(cargo_target_dir + "/restore-attempt-history") }}
    restore_history_log="$(mktemp {{ quote(cargo_target_dir + "/restore-attempt-history/run.XXXXXX.log") }})"
    cargo test --package nervix-scenarios --test scenarios -- --tags @client_restore_uncertainty {{ quote(args) }} 2>&1 | tee "$restore_history_log"
    python3 - "$restore_history_log" <<'PY'
    import re
    import sys
    from pathlib import Path
    output = re.sub(r"\x1b\[[0-9;]*m", "", Path(sys.argv[1]).read_text())
    counts = re.findall(r"^(\d+) scenarios?(?: \(|$)", output, re.MULTILINE)
    if counts != ["4"]:
        raise SystemExit(f"restore attempt witnesses must execute four scenarios; reported counts: {counts}")
    PY

# Exercise restore reply validation through the native client and CLI on one and three nodes.
# These cases start no paced driver or diagnostic report subprocess.
test-client-restore-reply: build-web-console download-onnxruntime build-test-cli
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- \
        --input tests/features/runtime/client_restore_reply_validation.feature \
        --concurrency 2 --retry 0

# Prepare codec checks separately so compilation has its own bounded phase.
build-codec-parser-heads:
    cargo test --package nervix-plexus --test kinded_codec_heads --no-run

# Collect the complete codec-head parser and canonical-renderer assertions as ordinary coverage.
coverage-codec-heads output="target/codec-heads.lcov":
    cargo llvm-cov --package nervix-plexus --test kinded_codec_heads --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }}

# Unify the native tool graph so each shared vocabulary dependency is prepared once.
build-codec-tools:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir) }} cargo build --package nervix-plexus-format --bin nervix-plexus-format --package nervix-cli --bin nervix-cli --package nervix-deadlock --features nervix-deadlock/report-tool --bin nervix-deadlock-report

# Prepare the ordinary assets and tools the codec public scenarios execute.
build-codec-heads: generate-test-onnx download-onnxruntime build-web-console wasm-processor-guests build-codec-tools
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios --no-run

# Public codec-head inspection and completion through the real native session and CLI.
test-codec-heads: (test-codec-scenarios (justfile_directory() + "/tests/features/runtime/kinded_codec_heads.feature"))

# Qualify endpoint and codec heads together with processor/emitter body acknowledgement modes.
[positional-arguments]
test-kinded-heads *args: build-codec-heads
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- \
        --input {{ quote(justfile_directory() / "tests/features/runtime/{kinded_endpoint_heads,kinded_codec_heads,acknowledgement_clause}.feature") }} \
        --concurrency 4 --retry 0 "$@"

# Exercise CLI stream recovery and terminal endings without unrelated client probes.
test-cli-streams *args: build-deps build-test-cli
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input {{ quote(justfile_directory() / "tests/features/tools/cli_session*.feature") }} --tags '@cli_stream_termination or @cli_stream_recovery' {{ quote(args) }}

# Observe interactive output without sending keys to wake the prompt or preparing unrelated guests.
test-cli-repl tags="@cli_repl_output" *args: build-web-console download-onnxruntime build-test-cli
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- --input {{ quote(justfile_directory() / "tests/features/tools/cli_session*.feature") }} --tags {{ quote(tags) }} {{ quote(args) }}

# Select codec round trips or browser cases with the same ordinary runner and prepared tools.
test-codec-scenarios input *args: build-codec-heads
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- \
        --input {{ quote(input) }} --concurrency 2 --retry 0 {{ quote(args) }}

# Qualify the real failed-step after hook; deliberate Cucumber failures stay failures.
# This supervision check has its own verdict, outside ordinary coverage reports.
test-backup-cut-cleanup: build-deps build-test-cli
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/debug/nervix-cli") }}
    evidence="$(mktemp -d {{ quote(cargo_target_dir + "/backup-cut-cleanup.XXXXXXXX") }})"
    # Reuse the ordinary scenario build in CI without mixing qualification profiles into it.
    instrumentation="$(CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} cargo llvm-cov show-env --sh)"
    eval "$instrumentation"
    export LLVM_PROFILE_FILE="$evidence/qualification-%p-%m.profraw"
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} \
        cargo test --package nervix-scenarios --test scenarios --no-run --message-format=json > "$evidence/build.jsonl"
    python3 -m scripts.backup_cut_cleanup "$evidence"

# Qualify ten consecutive three-node native metadata restores, retaining every attempt without retries.
test-native-metadata-restore-repeat: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    evidence_root={{ quote(cargo_target_dir + "/native-metadata-restore") }}
    mkdir -p "$evidence_root"
    attempt=$(mktemp -d "$evidence_root/run.XXXXXXXX")
    git rev-parse HEAD > "$attempt/source-revision.txt"
    git diff HEAD -- > "$attempt/source.patch"
    for repetition in {1..10}; do
        log="$attempt/repetition-$repetition.log"
        status=0
        cargo test --package nervix-scenarios --test scenarios -- \
            --input tests/features/cluster/backup_metadata.feature \
            --name '^Native lifecycle and Kafka metadata above the bulk budget on 3 nodes restore exactly$' \
            --concurrency 4 --retry 0 > "$log" 2>&1 || status=$?
        if [[ -f tests/logs/scenarios.log ]]; then
            cp tests/logs/scenarios.log "$attempt/repetition-$repetition-scenarios.log"
        fi
        if [[ "$status" != 0 ]] || ! rg -q '^1 scenario \(1 passed\)$' "$log"; then
            cat "$log"
            printf 'Native metadata restore repetition %s failed; evidence: %s\n' "$repetition" "$attempt" >&2
            exit 1
        fi
        printf 'Native metadata restore repetition %s/10 passed\n' "$repetition"
    done
    printf '10 consecutive three-node scenarios passed with concurrency 4 and retry 0\n' > "$attempt/completion.txt"
    printf 'Native metadata restore qualification complete; evidence: %s\n' "$attempt"

# Replay a compiled scenario binary, including a saved pre-fix reproducer, without rebuilding it.
[positional-arguments]
test-scenarios-binary binary *args: download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    exec "$@"

# Replay a compiled unit-test executable against its frozen source without a Cargo rebuild.
[positional-arguments]
test-unit-binary binary *args:
    #!/usr/bin/env bash
    set -euo pipefail
    exec "$@"

# Replay a compiled ONNX inspection owner with an explicit runtime library.
test-onnx-owner-binary binary library *args:
    ORT_DYLIB_PATH={{ quote(library) }} {{ quote(binary) }} {{ quote(args) }}

# Prepare diagnostic scenario executables for a supervised public replay.
build-diagnostic-scenarios selection: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    case {{ quote(selection) }} in
        deloxide|deloxide-order|deloxide-stress) ;;
        *) echo "expected deloxide, deloxide-order or deloxide-stress" >&2; exit 2 ;;
    esac
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/deloxide") }} cargo test --no-run --package nervix-server --features {{ quote("testing," + selection) }} --lib
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/deloxide") }} cargo test --no-run --package nervix-scenarios --features {{ quote(selection) }} --test scenarios

# Reproduce a focused public scenario in one diagnostic selection, with the same prerequisites
# as the complete lane. This does not replace the lane's inventory and evidence qualification.
test-diagnostic-scenarios selection feature *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    case {{ quote(selection) }} in
        deloxide|deloxide-order|deloxide-stress) ;;
        *) echo "expected deloxide, deloxide-order or deloxide-stress" >&2; exit 2 ;;
    esac
    diagnostic_feature="$(realpath -- {{ quote(feature) }})"
    if [[ ! -f "$diagnostic_feature" ]]; then
        echo "expected an existing feature file" >&2
        exit 2
    fi
    # A glob component makes Cucumber walk the containing directory and match this exact file.
    diagnostic_input="${diagnostic_feature%/*}/{${diagnostic_feature##*/}}"
    diagnostic_attempts={{ quote(cargo_target_dir + "/deloxide/focused/" + selection) }}
    mkdir -p "$diagnostic_attempts"
    diagnostic_attempt="$(mktemp -d "$diagnostic_attempts/run.XXXXXX")"
    export NERVIX_DEADLOCK_EVIDENCE="${NERVIX_DEADLOCK_EVIDENCE:-$diagnostic_attempt/evidence}"
    mkdir -p "$NERVIX_DEADLOCK_EVIDENCE"
    echo "diagnostic scenario attempt: $diagnostic_attempt"
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/debug/nervix-cli") }}
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/debug/nervix-deadlock-report") }}
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/deloxide") }} cargo test --package nervix-scenarios --features {{ quote(selection) }} --test scenarios -- --input "$diagnostic_input" {{ quote(args) }} 2>&1 | tee "$diagnostic_attempt/scenarios.log"
    if ! rg --quiet '^[1-9][0-9]* scenarios?([ (]|$)' "$diagnostic_attempt/scenarios.log"; then
        echo "diagnostic scenario selection executed no scenarios" >&2
        exit 1
    fi

# Run focused harness regressions against a diagnostic build without changing its execution mode.
test-diagnostic-harness selection *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    case {{ quote(selection) }} in
        deloxide|deloxide-order|deloxide-stress) ;;
        *) echo "expected deloxide, deloxide-order or deloxide-stress" >&2; exit 2 ;;
    esac
    harness_attempts={{ quote(cargo_target_dir + "/deloxide/focused/" + selection) }}
    mkdir -p "$harness_attempts"
    harness_attempt="$(mktemp -d "$harness_attempts/harness.XXXXXX")"
    echo "diagnostic harness attempt: $harness_attempt"
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/deloxide") }} cargo test --package nervix-scenarios --features {{ quote(selection) }} --test harness_liveness -- {{ quote(args) }} 2>&1 | tee "$harness_attempt/harness.log"
    if ! rg --quiet '^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;' "$harness_attempt/harness.log"; then
        echo "diagnostic harness selection completed no tests or ignored selected tests" >&2
        exit 1
    fi

# Run focused library owners in a diagnostic selection and qualify their retained process evidence.
test-diagnostic-lib selection test *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    case {{ quote(selection) }} in
        deloxide|deloxide-order|deloxide-stress) ;;
        *) echo "expected deloxide, deloxide-order or deloxide-stress" >&2; exit 2 ;;
    esac
    library_attempts={{ quote(cargo_target_dir + "/deloxide/focused/" + selection) }}
    mkdir -p "$library_attempts"
    library_attempt="$(mktemp -d "$library_attempts/library.XXXXXX")"
    export NERVIX_DEADLOCK_EVIDENCE="${NERVIX_DEADLOCK_EVIDENCE:-$library_attempt/evidence}"
    if [[ -e "$NERVIX_DEADLOCK_EVIDENCE" ]] && { [[ ! -d "$NERVIX_DEADLOCK_EVIDENCE" ]] || [[ -n "$(ls -A -- "$NERVIX_DEADLOCK_EVIDENCE")" ]]; }; then
        echo "diagnostic library evidence directory must be empty" >&2
        exit 2
    fi
    mkdir -p "$NERVIX_DEADLOCK_EVIDENCE"
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    echo "diagnostic library attempt: $library_attempt"
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/deloxide") }} cargo test --package nervix-server --features {{ quote("testing," + selection) }} --lib -- --exact {{ quote(test) }} {{ quote(args) }} 2>&1 | tee "$library_attempt/library.log"
    if ! rg --quiet '^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;' "$library_attempt/library.log"; then
        echo "diagnostic library selection completed no tests or ignored selected tests" >&2
        exit 1
    fi
    shopt -s nullglob
    library_evidence=("$NERVIX_DEADLOCK_EVIDENCE"/deadlock-*.rkyv)
    if [[ ${#library_evidence[@]} -eq 0 ]]; then
        echo "diagnostic library selection retained no process evidence" >&2
        exit 1
    fi
    for evidence_file in "${library_evidence[@]}"; do
        {{ quote(cargo_target_dir + "/debug/nervix-deadlock-report") }} qualify "$evidence_file"
    done

# Focused kernel tests: every SIMD level the host supports and the forced scalar fallback,
# beside the crate's doctests.
test-simd-kernels *args:
    cargo test --package nervix-simd-kernels -- {{ args }}

# Focused columnar admission kernel and vocabulary tests.
test-admission-kernels *args:
    cargo test --package nervix-simd-kernels --lib -- {{ args }}
    cargo test --package nervix-models --lib -- {{ args }}

bench-window-admission *args:
    cargo bench --package nervix-simd-kernels --bench window_admission -- {{ args }}

# Measure admitting a window's retained rows into its distinct, quantile and top-k pane sketches,
# from a new pane for every row to a thousand rows per pane.
bench-window-sketches *args: build-web-console
    cargo bench --package nervix-server --bench window_sketches --features benchmarks -- {{ args }}

# Measure the checked integer SIMD kernels beside the lane loop they replaced, over one 1,024-lane
# run each. Extra arguments are forwarded to Criterion.
bench-checked-lanes *args:
    cargo bench --package nervix-simd-kernels --bench checked_lanes -- {{ args }}

# Compare constant division at every width with scalar reciprocal and checked lane loops.
bench-constant-division *args:
    cargo bench --package nervix-simd-kernels --bench constant_division -- {{ args }}

# Compare the byte-class scans and the XML character check with the scalar loops they replaced.
bench-byte-classes *args:
    cargo bench --package nervix-simd-kernels --bench byte_classes -- {{ args }}

bench-byte-classes-x86-64-v3 *args:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/simd-kernels-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --package nervix-simd-kernels --bench byte_classes -- {{ args }}

# Measure RFC 6587 stream framing over a mebibyte of frames read in a connection's 8 KiB reads.
bench-syslog-framing *args:
    cargo bench --package nervix-connector-syslog --bench stream_framing --features benchmarks -- {{ args }}

# Measure folding one acknowledgement batch's Kafka positions to each partition's latest offset and
# a rejection's to each partition's earliest message, over repeated and distinct partitions.
bench-kafka-offsets *args:
    cargo bench --package nervix-connector-kafka --bench partition_offsets --features benchmarks -- {{ args }}

bench-constant-division-x86-64-v3 *args:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/simd-kernels-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --package nervix-simd-kernels --bench constant_division -- {{ args }}

# The same measurement built for the x86-64-v3 payload the Docker image ships, in its own target
# directory: the lane loop compiles for AVX2 as the payload's does, and the kernels still select
# their level from the CPU at run time.
bench-checked-lanes-x86-64-v3 *args:
    CARGO_TARGET_DIR="{{ cargo_target_dir }}/simd-kernels-x86-64-v3" RUSTFLAGS="-C target-cpu=x86-64-v3" cargo bench --package nervix-simd-kernels --bench checked_lanes -- {{ args }}

test-admission-runtime *args: download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib -- {{ args }}

# Explore one runtime owner without unrelated inventory packages. The complete inventory,
# completion and nondeterminism gate remains test-shuttle in CI.
test-runtime-shuttle check: download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/shuttle") }} SHUTTLE_TRACE_DIR={{ quote(cargo_target_dir + "/shuttle-failures/nervix-server/" + check) }} cargo test --package nervix-server --features shuttle --lib {{ quote(check) }} -- --exact --nocapture --test-threads=1

# Explore one registered runtime model with its production harness. The complete inventory and
# weakening/replay qualification remain test-loom and test-loom-qualification in CI.
test-runtime-loom check:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{ quote(cargo_target_dir + "/loom-owner-checks") }}
    checkpoint_dir="$(mktemp -d {{ quote(cargo_target_dir + "/loom-owner-checks/run.XXXXXX") }})"
    printf 'Loom owner checkpoint directory: %s\n' "$checkpoint_dir"
    LOOM_CHECKPOINT_FILE="$checkpoint_dir/checkpoint.json" LOOM_CHECKPOINT_INTERVAL=1 cargo test --profile loom --package nervix-server --features loom --lib {{ quote(check) }} -- --exact --nocapture --test-threads=1

# Focused ordinary-mode regressions for runtime owners.
test-runtime *args: build-web-console wasm-processor-guests download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib -- {{ args }}

# Measure established row publication and immutable captures with bounded allocation evidence.
bench-materialized-state: build-web-console wasm-processor-guests download-onnxruntime bench-materialized-state-bodies

bench-materialized-state-bodies:
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib materialized_owner_cost -- --ignored --nocapture

# Measure the endpoint's actual request routing and per-thread allocations on the same host.
bench-endpoint-routing: build-web-console download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing,benchmarks --lib endpoint_routing_cost -- --ignored --nocapture

test-endpoint-intake *args: build-web-console download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib -- {{ args }}

test-web-console *args:
    CARGO_TARGET_DIR={{ cargo_target_dir }} cargo test --package nervix-web-console --bin nervix-web-console -- {{ args }}

test-harness-liveness *args: tests-deps
    cargo test --package nervix-scenarios --test harness_liveness -- {{ args }}

# Exercise command-probe causality through its ordinary owner and the server's assets.
test-command-probes *args: build-web-console wasm-processor-guests download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-scenarios --test harness_liveness -- command_probe_tests {{ quote(args) }}

# Exercise the two committed-batch recovery fixtures through the in-process cluster.
test-command-recovery-scenarios: build-web-console wasm-processor-guests download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-scenarios --test scenarios -- \
        --input tests/features/runtime/client_wire_failures.feature \
        --tags '@client_wire_transaction_batch_finalization_recovery or @client_wire_applying_probe_recovery' \
        --concurrency 1 --retry 0

# Exercise list fields of every schemaful wire format through the in-process cluster: AVRO item
# types, list-element ENCODE rules and the refusals of lists a wire type cannot carry.
[positional-arguments]
test-wire-schema-lists *args: build-web-console wasm-processor-guests download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- \
        --input tests/features/runtime/wire_schema_lists.feature "$@"

# Verify every processor/emitter acknowledgement clause through public CREATE and SHOW CREATE.
[positional-arguments]
test-acknowledgement-clauses *args: generate-test-onnx download-onnxruntime build-web-console wasm-processor-guests build-test-cli
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- \
        --input tests/features/runtime/acknowledgement_clause.feature \
        --tags @acknowledgement_clause --concurrency 4 --retry 0 "$@"

# Exercise explicit COMMIT outcome recovery and a recorded failed prefix through in-process nodes.
test-explicit-commit-recovery: build-web-console wasm-processor-guests download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-scenarios --test scenarios -- \
        --input {{ quote(justfile_directory() / "tests/features/runtime/{explicit_commit_recovery,plexus_transactions}.feature") }} \
        --tags '@explicit_commit_recovery or @transaction_failed_resume' \
        --concurrency 1 --retry 0

# Exercise upload identity conflicts and retained completion through the public session API.
test-resource-upload-identity-scenarios: build-web-console wasm-processor-guests download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-scenarios --test scenarios -- \
        --input tests/features/cluster/resource_describe.feature \
        --name 'Upload retry reports one assigned version|An upload whose installing call loses its reply reports its one assigned version' \
        --concurrency 2 --retry 0

# Exercise multi-span cluster durations with only the in-process server's prerequisites.
test-cluster-duration-scenarios: build-web-console wasm-processor-guests download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-scenarios --test scenarios -- \
        --input tests/features/cluster/cluster_parameters.feature \
        --tags @multi_span_cluster_duration --concurrency 2 --retry 0

# Exercise cluster parameter completion and the refusal of a repeated parameter through the
# session protocol and the Rust client, with only the in-process server's prerequisites.
test-cluster-parameter-completion-scenarios: build-web-console wasm-processor-guests download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-scenarios --test scenarios -- \
        --input tests/features/cluster/cluster_parameters.feature \
        --tags @cluster_parameter_completion --concurrency 2 --retry 0

# Check external dependency startup and observation, including fresh Pulsar namespace readiness.
test-environment *args:
    cargo test --package nervix-test-environment {{ args }}

# Measure native consensus transport regressions through the configured compiler cache.
coverage-consensus output *args:
    cargo llvm-cov --no-clean --package nervix-consensus --features testing --lib --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ args }}

# Measure the Rust client library's unit tests, for a patch confined to it, through the configured
# compiler cache.
coverage-client-core output *args:
    cargo llvm-cov --no-clean --package nervix-client-core --lib --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ args }}

# Measure selected console request-owner regressions without running unrelated browser tests.
coverage-web-console output *args:
    cargo llvm-cov --package nervix-web-console --bin nervix-web-console --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ quote(args) }}

# Measure one connector crate's unit tests, for a patch confined to it, through the configured
# compiler cache.
coverage-connector package output *args:
    cargo llvm-cov --no-clean --package {{ package }} --lib --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ args }}

# Collect focused unit coverage for the cryptography boundary and its TLS/signing consumers.
coverage-crypto output *args:
    cargo llvm-cov --no-clean --package nervix-crypto --package nervix-interconnect --package nervix-connector --package nervix-connector-nats --lib --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ args }}

test-scenarios-reuse *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    export NERVIX_TESTCONTAINERS_MODE=reusable
    cargo test --package nervix-scenarios --test scenarios -- {{ quote(args) }}

# Capture the web console images the book publishes. The capture tool starts a real nervix-server,
# seeds it with nervix-cli, and drives the console in a browser, so the images are build output
# rather than repository content and the book stages them from target/.
docs-screenshots: build-web-console
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
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --features testing --lib -- {{ args }}

# Run the Arrow-to-Row correctness cases and print the typed encoding's allocation evidence.
test-subscription-rows: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --features benchmarks,testing --lib subscription_row:: -- --nocapture

# Type-check one workspace package and all of its targets without building binaries. Extra
# arguments are forwarded to Cargo, so a server check can add `--features testing`.
check-package package *args:
    cargo check --package {{ package }} --all-targets {{ args }}

# Type-check only the library target of one package, leaving its tests, benches and binaries out.
check-package-lib package *args:
    cargo check --package {{ package }} --lib {{ args }}

# Run the unit tests of one workspace package whose tests need no server test dependencies.
test-package-lib package *args:
    cargo test --package {{ package }} --lib -- {{ args }}

# Package-scoped local evidence, using the canonical runners and complete registered owner records.
# Normal CI keeps the full model inventories. Deloxide/fuzz CI remain disabled by policy.
test-rate-limit-properties:
    python3 -m scripts.rate_limit_checks properties --target-dir {{ quote(cargo_target_dir + "/rate-limit/properties") }}

# Explore only the registered engine owner through the canonical model runners.
test-rate-limit-model mode:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/rate-limit/" + mode) }} python3 -m scripts.rate_limit_checks {{ quote(mode) }} --target-dir {{ quote(cargo_target_dir + "/rate-limit/" + mode) }}

test-rate-limit-diagnostic selection: build-deadlock-report
    NERVIX_PREPARED_TARGET_DIR={{ quote(cargo_target_dir) }} python3 -m scripts.rate_limit_checks {{ quote(selection) }} --target-dir {{ quote(cargo_target_dir + "/rate-limit/" + selection) }}

# Format the NATS driver package in its checkout.
fmt-nats-driver checkout:
    cargo +nightly fmt --manifest-path {{ quote(checkout + "/Cargo.toml") }} --package async-nats

# Qualify the pinned NATS driver's flush, ordered headers and borrowed delivery metadata parser.
test-nats-driver checkout *args:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/nats-driver") }} cargo test --manifest-path {{ quote(checkout + "/async-nats/Cargo.toml") }} --package async-nats --no-default-features --features aws-lc-rs,jetstream,server_2_10,server_2_11 --lib -- {{ args }}

# Format the maintained NKey codec package in its checkout.
fmt-nkeys-codec checkout:
    cargo +nightly fmt --manifest-path {{ quote(checkout + "/Cargo.toml") }}

# Qualify the maintained NKey format boundary without enabling its signing backend.
test-nkeys-codec checkout *args:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/nkeys-codec") }} cargo test --manifest-path {{ quote(checkout + "/Cargo.toml") }} --no-default-features --lib -- {{ args }}

# Preserve the upstream NKey signing API alongside the independent format-only graph.
test-nkeys-signing checkout *args:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/nkeys-codec") }} cargo test --manifest-path {{ quote(checkout + "/Cargo.toml") }} --features xkeys --lib -- {{ args }}

# Qualify the pinned Pulsar driver's metadata boundary with the product's enabled features.
test-pulsar-driver checkout *args:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/pulsar-driver") }} cargo test --manifest-path {{ quote(checkout + "/Cargo.toml") }} --no-default-features --features compression,tokio-rustls-runtime --lib -- {{ args }}

# Qualify the pinned Kafka driver's AWS-LC backend through its own tests with the product's enabled
# features: librdkafka built against aws-lc-sys, an AWS-LC TLS context and an SSL client.
test-kafka-driver checkout *args:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/kafka-driver") }} cargo test --manifest-path {{ quote(checkout + "/Cargo.toml") }} --package rdkafka-sys --no-default-features --features aws-lc,cmake_build,curl-static,curl-rustls,libz-static,static-linking --test aws_lc -- {{ args }}
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/kafka-driver") }} cargo test --manifest-path {{ quote(checkout + "/Cargo.toml") }} --package rdkafka --no-default-features --features aws-lc,cmake_build,curl-static,curl-rustls,libz-static,static-linking,tokio --test ssl_backends -- {{ args }}

# Run the vocabulary settings' compiling and class-rejection API examples.
test-settings:
    cargo test --package nervix-models --lib settings::
    cargo test --package nervix-models --doc SettingDefinition

# Run one integration test target of one workspace package whose tests need no server test
# dependencies, such as the vocabulary's representation properties.
test-package-test package test *args:
    cargo test --package {{ package }} --test {{ test }} -- {{ args }}

# Run the unit tests in the binary targets of one workspace package, such as the web console's
# view logic in its `main.rs`.
test-package-bins package *args:
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

# The conformance checks under each other execution mode's backend, each mode its own build: the
# model checkers, the simulator, and the diagnostic mode's tracked locks.
test-primitives-modeled: test-primitives-shuttle test-primitives-loom test-primitives-turmoil test-primitives-deloxide

test-primitives-shuttle:
    cargo test --package nervix-primitives --features 'shuttle native' --lib

test-primitives-loom:
    cargo test --package nervix-primitives --features 'loom native' --lib

test-primitives-turmoil:
    cargo test --package nervix-primitives --features 'turmoil native' --lib

test-primitives-deloxide:
    cargo test --package nervix-primitives --features 'deloxide native' --lib
    cargo test --package nervix-primitives --features 'deloxide-order native' --lib
    cargo test --package nervix-primitives --features 'deloxide-stress native' --lib

# The conformance checks that compile rather than run: the documentation tests that a runtime
# attribute refuses a crate path, that a product binary has the forms each mode builds, and that the
# diagnostic mode's tracked locks refuse the operations they cannot track; and the portable
# surface's browser build.
test-primitives-compile diagnostic_modes="deloxide deloxide-order deloxide-stress":
    #!/usr/bin/env bash
    set -euo pipefail
    cargo test --package nervix-primitives --features native --doc
    read -r -a modes <<< {{ quote(diagnostic_modes) }}
    for mode in "${modes[@]}"; do
        case "$mode" in
            deloxide|deloxide-order|deloxide-stress) ;;
            *) echo "unknown diagnostic execution mode: $mode" >&2; exit 2 ;;
        esac
        cargo test --package nervix-primitives --features "$mode native" --doc
    done
    cargo check --package nervix-primitives --lib --target wasm32-unknown-unknown

# Run the Deloxide diagnostic lane in its active-only selection: every workload
# tests/deloxide-inventory.toml registers for the `deloxide` build, after the prerequisites. The
# lane builds every selected executable first under its build budget, then starts the workload
# budget. Builds use target/deloxide, so the diagnostic server
# binary never replaces the ordinary one, and refuses a registered workload that is missing or
# ignored and a probe, conformance check, owner test or tagged scenario that is not registered. The
# probes of nervix-deadlock run every workload in a disposable child that must report its cycle,
# record its evidence and end as its contract says; the tracked locks' conformance checks of
# nervix-primitives and the diagnostic owner tests each install their detector in a fresh process;
# and the scenario binary runs the `@deadlock_diagnostics`, `@restore_installation`,
# `@client_ingestor_alter_drain`, `@deadlock_reports`, `@memory_pressure_pause`,
# `@client_io_03_consumer_restore`, `@client_io_03_generation`, `@remote_ack_owners`, `@udf_column_builder`,
# `@vhost_tls_rebinding`, `@inferencer_branch_batches` and `@inferencer_inspection_admission`
# scenarios and then the
# `@paced_simulation_reopen` scenarios with the diagnostic Rust paced driver, without retries, on
# in-process nodes and real diagnostic server processes of one and three nodes. Every process runs
# in a session of its own within its bound and its stage's budget, and the lane fails on an
# active deadlock (status 3), a diagnostic failure (4), a signal, a timeout or an expired budget
# (124), a leftover process, incomplete accounting, and missing, partly written or nonqualifying
# evidence. A fresh attempt under target/deloxide/test-deloxide/deloxide keeps every log, artifact,
# evidence file and finding description, and lane.json with the exact commands, bounds and
# outcomes. Under the
# native coverage collector the same lane builds instrumented and records its completion there.
test-deloxide: tests-deps test-deloxide-workloads

# The same lane in the `deloxide-order` selection, whose builds add historical lock-order
# instrumentation and run the order-only probes.
test-deloxide-order: tests-deps test-deloxide-order-workloads

# The same lane in the `deloxide-stress` selection: the active-only build with Deloxide's bounded
# scheduling disturbance compiled in. A thread that holds a tracked lock waits before one in twenty
# of its further acquisitions, between 20µs and 200µs, as `StressConfiguration::LANE` records in
# every process's evidence. Every probe, conformance check and owner test of the active-only
# selection runs again under that disturbance, together with the probe that proves it is applied,
# and the lifecycle scenarios tagged `@deloxide_stress` and the paced-driver scenarios run one at a
# time, so the transactions, drains, reconnections, reopens and shutdowns they drive meet orders of
# nested acquisitions an idle schedule rarely takes. Deloxide draws its delays from entropy it
# seeds itself, so a replay repeats the workload and the configuration, not the schedule.
test-deloxide-stress: tests-deps test-deloxide-stress-workloads

# The stressed build's capture, restore and checkpoint-transfer scenarios, tagged
# `@deloxide_stress_restore`, in the `deloxide-stress-restore` selection: the same build and
# disturbance as `test-deloxide-stress`, its probes, and these scenarios one at a time, in a lane
# with a budget of its own, because the stressed scenarios of both selections together would take
# most of one lane's budget.
test-deloxide-stress-restore: tests-deps test-deloxide-stress-restore-workloads

# Measure a selection's diagnostic builds without running workloads. The retained record has the
# verdict `built`, so it cannot qualify as completed workload or native coverage evidence.
build-deloxide-lane selection="deloxide": tests-deps
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} build {{ quote(selection) }}

# The part of each selection's lane that executes Nervix code. It has no dependencies, so the native
# coverage collector runs exactly the lane inside its instrumentation.
test-deloxide-workloads:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} run deloxide

test-deloxide-order-workloads:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} run deloxide-order

test-deloxide-stress-workloads:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} run deloxide-stress

test-deloxide-stress-restore-workloads:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} run deloxide-stress-restore

# Each selection's diagnostic executables, built as its workloads recipe builds them, without
# running a workload. Like the workloads recipes they have no dependencies, so the native coverage
# collector builds exactly the lane's executables inside its instrumentation before CI runs the lane.
build-deloxide-executables:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} build deloxide

build-deloxide-order-executables:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} build deloxide-order

build-deloxide-stress-executables:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} build deloxide-stress

build-deloxide-stress-restore-executables:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} build deloxide-stress-restore

# Prove the lane's supervision on real failing processes: probe workloads that deadlock, fail their
# diagnostics, hang on an untracked wait, abort, retain an unreviewed potential cycle or overflow
# their retention must each fail the lane with its own class, keep their output and evidence, and
# leave no process behind; a clean control must pass; and the recorded deadlock must replay. The
# order-only cases run in the `deloxide-order` selection.
test-deloxide-qualification selection="deloxide-order": build-deadlock-report
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} qualify {{ quote(selection) }}

# Run one launch a lane attempt recorded again, with exactly its command, environment and bound, in a
# fresh attempt: `just test-deloxide-replay target/deloxide/test-deloxide/deloxide/run.XXXX/lane.json scenarios`.
# Schedules and timing are not recorded, so a replay can take another interleaving.
test-deloxide-replay record launch:
    python3 -m scripts.deloxide_lane --target-dir {{ quote(cargo_target_dir) }} replay {{ quote(record) }} {{ quote(launch) }}

# Hold every source file that acquires a tracked blocking lock in a diagnostic build to its owner
# record in tests/deloxide-inventory.toml, using the compiler catalog `just ratchet` writes.
validate-deloxide-applicability: ratchet
    python3 -m scripts.deloxide_lane applicability --catalog {{ quote(cargo_target_dir + "/typed-ratchet/gate.json") }}

# Check workload registration without building or running a diagnostic lane.
validate-deloxide-inventory:
    python3 -m scripts.deloxide_lane validate

# Focused disposable-process reproducer and diagnostic checks, preserving configured kache.
test-deadlock-probes features="deloxide" *args:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/deloxide") }} cargo test --package nervix-deadlock --features {{ quote(features) }} --test active_cycles {{ args }}

# The packages whose `shuttle_` checks `test-shuttle` explores, as the Shuttle inventory lists them,
# and whose test builds `shuttle-clippy-targets` lints. scripts/tests/test_shuttle_checks.py holds
# this list to the inventory.
shuttle_test_packages := ["nervix-execution", "nervix-interconnect", "nervix-client-core", "nervix-connector", "nervix-connector-kafka", "nervix-connector-mqtt", "nervix-connector-rabbitmq", "nervix-connector-redis", "nervix-connector-sqs", "nervix-connector-syslog", "nervix-connector-websockets", "nervix-consensus", "nervix-server", "nervix-deadlock", "nervix-rate-limit"]

# Explore every registered Shuttle check of a production owner, each in its own process: under the
# exploration it declares, then under the uncontrolled-nondeterminism detector. The inventory in
# crates/model-harness/shuttle-inventory.toml registers each check by package and test; the whole
# run fails when a registered check is missing, ignored or did not complete its exploration, or when
# a check is unregistered. A non-empty `filter` runs the checks whose full names contain it, in
# every package, and fails when it selects none at all. A failed check leaves its persisted
# schedule, output and metadata under target/shuttle-failures for `test-shuttle-replay`. The server's
# checks need the build dependencies this recipe prepares.
test-shuttle filter="": build-web-console wasm-processor-guests download-onnxruntime (test-shuttle-checks filter)

[private]
test-shuttle-checks filter="":
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    python3 -m unittest --quiet scripts.tests.test_shuttle_checks scripts.tests.test_rate_limit_checks
    evidence="${NERVIX_MODEL_EVIDENCE:-}"
    report=()
    if [[ -n "$evidence" ]]; then report=(--report "$evidence"); fi
    python3 -m scripts.shuttle_checks --target-dir {{ quote(cargo_target_dir) }} run {{ quote(filter) }} "${report[@]}"

# Compile every package `test-shuttle-checks` explores, as it builds them, without running a check.
[private]
build-shuttle-checks:
    python3 -m scripts.shuttle_checks build

# Replay a schedule `test-shuttle` persisted under target/shuttle-failures in a fresh process. Its
# parent directories name the exact package and check, so the schedule cannot run against another
# invariant.
test-shuttle-replay schedule: build-web-console wasm-processor-guests download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    python3 -m scripts.shuttle_checks --target-dir {{ quote(cargo_target_dir) }} replay {{ quote(schedule) }}

# Prove schedule persistence and replay end to end: fail one execution check deliberately after its
# invariant held, require exactly one persisted schedule, and require it to reproduce the failure in
# a fresh process.
test-shuttle-replay-check:
    python3 -m scripts.shuttle_checks --target-dir {{ quote(cargo_target_dir) }} replay-check

# Explore every registered Loom model of a production owner to exhaustion, each in its own process
# and with Loom's primitives selected through its package's `loom` feature. The inventory in
# crates/model-harness/loom-inventory.toml registers each model by the invariant it checks; the
# whole run fails when a registered invariant is missing, ignored or incomplete, or when a model is
# unregistered. A non-empty `filter` runs the models whose test name or invariant contains it and
# fails when it selects none. A failed model leaves its Loom checkpoint, output and metadata under
# target/loom-failures for `test-loom-replay`. The server's models need the web console its library
# embeds.
test-loom filter="": build-web-console (test-loom-models filter)

[private]
test-loom-models filter="":
    #!/usr/bin/env bash
    set -euo pipefail
    python3 -m unittest --quiet scripts.tests.test_loom_models
    evidence="${NERVIX_MODEL_EVIDENCE:-}"
    report=()
    if [[ -n "$evidence" ]]; then report=(--report "$evidence"); fi
    python3 -m scripts.loom_models --target-dir {{ quote(cargo_target_dir) }} run {{ quote(filter) }} "${report[@]}"

# Compile every package `test-loom-models` explores, as it builds them, without running a model.
[private]
build-loom-models:
    python3 -m scripts.loom_models --target-dir {{ quote(cargo_target_dir) }} build

# Measure the native runner's inventory, completion and failure artifact paths.
coverage-loom-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/loom-coverage
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file target/loom-coverage/runner.coverage --branch --source=scripts.loom_models,scripts.tests.test_loom_models -m unittest scripts.tests.test_loom_models
    "${coverage[@]}" lcov --data-file target/loom-coverage/runner.coverage -o target/loom-coverage/python-runner.lcov

# Replay a failure `test-loom` recorded: Loom resumes from the checkpoint of the failed execution,
# with location tracking and tracing enabled, so that execution runs first.
test-loom-replay failure: build-web-console
    python3 -m scripts.loom_models --target-dir {{ quote(cargo_target_dir) }} replay {{ quote(failure) }}

# Show that each qualified Loom model detects its ordering or stack capacity fault. Every weakening is
# applied to a copy of the working tree, the model must fail with its registered message, and the
# checkpoint of that failure must replay it. `shard`, NUMBER/COUNT, qualifies one part of the
# distinct weakenings, so CI can split their rebuilds across jobs; the default qualifies them all.
test-loom-qualification shard="1/1": build-web-console
    python3 -m scripts.loom_models --target-dir {{ quote(cargo_target_dir) }} qualify --shard {{ quote(shard) }}

# Run the Turmoil suite: the primitive boundary's simulated-host checks, the execution and library
# simulation checks, then every interconnect scenario over its committed regression seeds. Tokio's
# unstable runtime knobs seed per-host scheduling and turn unhandled task panics into runtime
# failures; the cfg is scoped to this test mode, and ordinary and Shuttle builds keep their flags.
# After the build, the tests run inside a real-time budget of `budget_seconds` and end with status
# 124 when it expires. A failed scenario leaves a failure record under target/turmoil-failures for
# `test-turmoil-replay`. The suite reports how many tests each invocation and the whole suite
# discovered, selected, executed and completed. tests/turmoil-inventory.toml registers the tests
# each invocation runs and leaves ignored, and the suite fails when a registered test did not run or
# ran ignored, when an invariant ran unregistered, and when an invocation executed no test. Each
# invocation's output stays under target/turmoil-suite.
test-turmoil budget_seconds="480":
    #!/usr/bin/env bash
    set -euo pipefail
    python3 -m unittest --quiet scripts.tests.test_libtest_accounting
    turmoil_rustflags="--cfg tokio_unstable ${RUSTFLAGS:-}"
    export NERVIX_TURMOIL_FAILURES={{ quote(turmoil_failures) }}
    just build-turmoil-simulations
    logs={{ quote(cargo_target_dir + "/turmoil-suite") }}
    rm -rf "${logs}"
    mkdir -p "${logs}"
    deadline=$((SECONDS + {{ budget_seconds }}))
    within_budget() {
        local log="$1"
        shift
        local remaining=$((deadline - SECONDS))
        local status=0
        if ((remaining > 0)); then
            set +e
            RUSTFLAGS="${turmoil_rustflags}" timeout --kill-after=30 "${remaining}" "$@" 2>&1 \
                | tee "${logs}/${log}.log"
            status="${PIPESTATUS[0]}"
            set -e
        else
            status=124
        fi
        if ((status == 124)); then
            echo "the Turmoil suite exceeded its {{ budget_seconds }}s real-time budget;" \
                "in-progress records under ${NERVIX_TURMOIL_FAILURES} name the unfinished runs" >&2
        fi
        return "${status}"
    }
    within_budget primitives \
        cargo test --package nervix-primitives --features 'turmoil native' --lib -- \
            simulated_host turmoil_mode --test-threads=1
    within_budget execution \
        cargo test --package nervix-execution --features turmoil --lib -- --test-threads=1
    within_budget interconnect-library \
        cargo test --package nervix-interconnect --features turmoil --lib -- \
            wire::simulation_checks authentication::simulation_tests --test-threads=1
    within_budget interconnect-simulation \
        cargo test --package nervix-interconnect --features turmoil --test simulation -- \
            --test-threads=1
    python3 -m scripts.libtest_accounting turmoil --inventory tests/turmoil-inventory.toml \
        "${logs}/primitives.log" "${logs}/execution.log" "${logs}/interconnect-library.log" \
        "${logs}/interconnect-simulation.log"

# Compile every test target `test-turmoil` runs, with its flags, and run none of them.
[private]
build-turmoil-simulations:
    #!/usr/bin/env bash
    set -euo pipefail
    turmoil_rustflags="--cfg tokio_unstable ${RUSTFLAGS:-}"
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-primitives --features 'turmoil native' --lib
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-execution --features turmoil --lib
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-interconnect --features turmoil --lib --test simulation

# Run the interconnect's Turmoil simulation scenarios. Extra arguments filter or configure the test
# binary, so one test can run without the execution and library checks. A selection that executes
# no test fails.
test-turmoil-simulation *args:
    #!/usr/bin/env bash
    set -euo pipefail
    export RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}"
    export NERVIX_TURMOIL_FAILURES={{ quote(turmoil_failures) }}
    logs="$(mktemp -d)"
    trap 'rm -rf "${logs}"' EXIT
    set +e
    cargo test --package nervix-interconnect --features turmoil --test simulation -- \
        --test-threads=1 {{ args }} 2>&1 | tee "${logs}/simulation.log"
    status="${PIPESTATUS[0]}"
    set -e
    if ((status != 0)); then
        exit "${status}"
    fi
    python3 -m scripts.libtest_accounting turmoil-simulation "${logs}/simulation.log"

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
# `budget_seconds`; the recipe ends with status 124 when the budget expires. A nonempty `filter`
# selects the scenario tests whose names contain it, as a libtest filter does. A failure leaves a
# record like any other run. A seed that exposes a defect joins its scenario's committed seeds
# with the fix. A sweep that executes no scenario fails.
test-turmoil-sweep first="1000" count="64" budget_seconds="1500" filter="":
    #!/usr/bin/env bash
    set -euo pipefail
    turmoil_rustflags="--cfg tokio_unstable ${RUSTFLAGS:-}"
    export NERVIX_TURMOIL_FAILURES={{ quote(turmoil_failures) }}
    first={{ quote(first) }}
    end=$((first + {{ count }}))
    filter_args=()
    if [[ -n {{ quote(filter) }} ]]; then
        filter_args=({{ quote(filter) }})
    fi
    RUSTFLAGS="${turmoil_rustflags}" cargo test --no-run \
        --package nervix-interconnect --features turmoil --test simulation
    logs="$(mktemp -d)"
    trap 'rm -rf "${logs}"' EXIT
    set +e
    NERVIX_TURMOIL_SWEEP="${first}..${end}" RUSTFLAGS="${turmoil_rustflags}" \
        timeout --kill-after=30 {{ budget_seconds }} \
        cargo test --package nervix-interconnect --features turmoil --test simulation -- \
            "${filter_args[@]}" --test-threads=1 2>&1 | tee "${logs}/sweep.log"
    status="${PIPESTATUS[0]}"
    set -e
    if ((status == 124)); then
        echo "the seed sweep exceeded its {{ budget_seconds }}s real-time budget;" \
            "in-progress records under ${NERVIX_TURMOIL_FAILURES} name the unfinished runs" >&2
    fi
    if ((status != 0)); then
        exit "${status}"
    fi
    python3 -m scripts.libtest_accounting turmoil-sweep "${logs}/sweep.log"

# Run the expression VM unit tests, which live in the nervix-vm crate rather than the server lib.
test-vm *args:
    cargo test --package nervix-vm --lib -- {{ args }}

# Run the WASM host, guest SDK and ABI protocol unit tests, which live in their own crates rather
# than the server lib. The host tests drive the bundled Rust and Go reference guests.
test-wasm *args: wasm-processor-guests
    cargo test --package nervix-wasm --package nervix-wasm-sdk --package nervix-wasm-protocol --lib -- {{ args }}
    cargo test --package nervix-wasm-protocol --test representations -- {{ args }}

# Protocol boundary checks do not need compiled processor guests.
test-wasm-protocol *args:
    cargo test --package nervix-wasm-protocol -- {{ args }}

# Run the consensus unit tests, which live in the nervix-consensus crate rather than the server lib.
test-consensus *args:
    cargo test --package nervix-consensus --lib -- {{ args }}

# Run vocabulary unit tests and representation properties.
test-models *args:
    cargo test --package nervix-models --all-targets -- {{ args }}

# Run vocabulary API examples and compile-time refusals, optionally matching one API.
test-models-docs filter="":
    cargo test --package nervix-models --doc {{ quote(filter) }}

# Run vocabulary tests with their declared Arrow schema boundary enabled.
test-models-arrow *args:
    cargo test --package nervix-models --all-targets --features arrow -- {{ args }}

# Exact OTEL field contracts, their Arrow admission and the declaration-driven Plexus grammar.
test-otel-declarations:
    cargo test --package nervix-models --lib --features arrow connector::otel::
    cargo test --package nervix-plexus --lib emitter::

# Run the cryptography boundary's provider, signing, digest and randomness unit tests.
test-crypto *args:
    cargo test --package nervix-crypto --lib -- {{ args }}

# Run the boundary's registered properties with canonical corpus and case accounting. They have
# no server or console assets, so this focused owner does not need bolero-deps.
test-crypto-properties:
    python3 -c 'from scripts.bolero import load_inventory, select, test_targets; inventory = load_inventory(); selected = select(inventory, "crypto-"); test_targets(inventory, selected)'

# Qualify digest output, password salts and transport identity through their existing public cases.
test-crypto-scenarios *args: build-deps build-plexus-format build-test-cli build-deadlock-report
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-scenarios --test scenarios -- \
        --input {{ quote(justfile_directory() / "tests/features/{runtime/bytes_functions,cluster/authentication,cluster/internal_tls}.feature") }} \
        --name 'Binary values survive conversion and hashing across the public data path|Encoding and hashing sensitive bytes keeps subscription values masked|Client authentication requires a valid user password|Resources replicate over the authenticated interconnect|Interconnect peers connect with certificate identities|Interconnect certificate authority rotates without restarting the cluster' \
        --concurrency 2 --retry 0 {{ quote(args) }}

# Run the node transport's production owners and their protocol fixtures.
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

# Run the MySQL declaration consumers, statement builder and binding owners without a server.
test-mysql *args:
    cargo test --package nervix-connector-mysql --lib -- {{ args }}

# Run the WebSockets transport and signaling owners without unrelated connector test roots.
test-websockets *args:
    cargo test --package nervix-connector-websockets --lib -- {{ args }}

# Diagnose one WebSockets owner check with its replayable schedule. `test-shuttle` remains the
# inventory, completion and nondeterminism gate for qualification.
test-websockets-shuttle check:
    SHUTTLE_TRACE_DIR={{ quote(cargo_target_dir + "/shuttle-failures/nervix-connector-websockets/" + check) }} cargo test --package nervix-connector-websockets --features shuttle --lib {{ quote(check) }} -- --exact --nocapture --test-threads=1

# Explore one client owner and its nondeterminism check without unrelated server test roots.
# The inventory and complete package selection remain qualified by test-shuttle in CI.
test-client-shuttle check:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/shuttle") }} python3 -m scripts.client_recovery_checks shuttle --filter {{ quote(check) }} --target-dir {{ quote(cargo_target_dir + "/shuttle") }}

# Qualify the complete registered client feature scope, with canonical process supervision.
test-client-recovery-diagnostic selection: tests-deps
    NERVIX_PREPARED_TARGET_DIR={{ quote(cargo_target_dir) }} python3 -m scripts.client_recovery_checks {{ quote(selection) }} --target-dir {{ quote(cargo_target_dir + "/client-recovery") }}

test-client-recovery-runner:
    python3 -m unittest --quiet scripts.tests.test_client_recovery_checks

# Run the session wire codec tests and the gRPC and WebSocket sessions that carry its frames.
test-client-wire *args:
    cargo test --package nervix-client-wire --all-features --all-targets -- {{ args }}

# Run native client session unit tests without enabling the modeled Shuttle build.
test-client-core *args:
    cargo test --package nervix-client-core --features arrow,autocomplete --lib -- {{ args }}

# Run the C ABI's unit tests, which read every accessor as a host does through the header's types.
test-client-ffi *args:
    cargo test --package nervix-client-ffi --lib -- {{ args }}

# Run the CLI's unit tests: argument parsing, statement dispatch and how it renders outcomes and
# session events. Its public session behavior is covered by the `tools/` scenarios.
test-nervix-cli *args:
    cargo test --package nervix-cli --all-targets -- {{ args }}

# Rewrite the client wire conformance corpus from the encoder's current output. Review the
# regenerated `corpus.report` before committing it: every client implementation is held to it.
update-client-wire-corpus:
    NERVIX_UPDATE_CLIENT_WIRE_CORPUS=1 cargo test --package nervix-client-wire --lib -- \
        tests::conformance

# Build the Python package `nervix_client` with maturin into a wheel under the conformance
# artifacts, and install it into two CPython 3.12 virtual environments: `python`, beside pyarrow,
# polars, pytest and mypy, which the probe and the package's suite run in, and `python-bare`, which
# holds the package and nothing else, for the boundary tier's packaging test. CPython 3.12 is the
# oldest the abi3 wheel supports.
build-client-python:
    #!/usr/bin/env bash
    set -euo pipefail
    artifacts={{ quote(cargo_target_dir + "/client-conformance") }}
    environment="${artifacts}/python"
    bare="${artifacts}/python-bare"
    rm -rf "${environment}" "${bare}" "${artifacts}/wheels"
    uv venv --quiet --python 3.12 "${environment}"
    uv venv --quiet --python 3.12 "${bare}"
    CARGO_TARGET_DIR={{ quote(cargo_target_dir) }} uvx --from maturin==1.16.0 maturin build \
        --manifest-path crates/client-python/Cargo.toml --interpreter "${environment}/bin/python" \
        --out "${artifacts}/wheels"
    uv pip install --quiet --python "${environment}/bin/python" pyarrow==26.0.0 polars==2.1.0 \
        pytest==9.2.0 mypy==2.4.1 "${artifacts}"/wheels/nervix_client-*.whl
    uv pip install --quiet --python "${bare}/bin/python" "${artifacts}"/wheels/nervix_client-*.whl

# Run the Python package's suite on its boundary tier, which needs no cluster, and hold its
# results to the boundary tests `tests/client-suites-inventory.toml` registers. `just test` runs it.
# The tier's packaging test loads the package from `python-bare`.
test-client-python-api: build-client-python
    just cargo_target_dir={{ quote(cargo_target_dir) }} test-client-python-api-run

# Run the boundary tier from the environment `build-client-python` built, compiling nothing.
test-client-python-api-run:
    #!/usr/bin/env bash
    set -euo pipefail
    artifacts={{ quote(cargo_target_dir + "/client-conformance") }}
    results="${artifacts}/python-boundary-results.txt"
    status=0
    (cd tests/client_conformance/python/suite && PYTHONDONTWRITEBYTECODE=1 \
        NERVIX_SUITE_BARE_PYTHON="${artifacts}/python-bare/bin/python" \
        "${artifacts}/python/bin/python" -m pytest boundary) | tee "${results}" || status=$?
    uv run --locked python -m scripts.client_suites check-run --host python --tier boundary \
        "${results}"
    exit "${status}"

# Run one host's cluster tier on one- and three-node clusters, building only what that host's
# suite starts: its artifacts and the scenario suite. `just test-client-conformance` runs every
# host's rows beside the probes.
test-client-python-cluster *args: tests-deps build-client-python
    cargo test --no-run --package nervix-scenarios --test scenarios
    just cargo_target_dir={{ quote(cargo_target_dir) }} test-client-conformance-run '@client_suite_python' {{ args }}

# Package the Java 21 API and this target's JNI library into one self-loading jar.
build-client-jvm:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir) }} cargo build --package nervix-client-jvm
    just cargo_target_dir={{ quote(cargo_target_dir) }} package-client-jvm

# Package Java sources with the JNI library the native build already produced.
package-client-jvm:
    #!/usr/bin/env bash
    set -euo pipefail
    artifacts={{ quote(cargo_target_dir + "/client-conformance/java") }}
    classes="${artifacts}/classes"
    rm -rf "${classes}" && mkdir -p "${classes}/io/nervix/client/native"
    javac --release 21 -Xlint:all -Werror -d "${classes}" crates/client-jvm/java/io/nervix/client/*.java
    case "$(uname -s)" in
        Darwin) library=libnervix_client_jvm.dylib ;;
        *) library=libnervix_client_jvm.so ;;
    esac
    cp {{ quote(cargo_target_dir + "/debug") }}/"${library}" "${classes}/io/nervix/client/native/"
    jar --create --file "${artifacts}/nervix-client.jar" -C "${classes}" .
    mkdir -p "${artifacts}/probe"
    javac --release 21 -Xlint:all -Werror -cp "${artifacts}/nervix-client.jar" \
        -d "${artifacts}/probe" tests/client_conformance/java/Probe.java

# Compile only the JVM boundary fixtures beside the already built jar.
build-client-jvm-tests: build-client-jvm
    just cargo_target_dir={{ quote(cargo_target_dir) }} package-client-jvm-tests

# Compile the current Java boundary fixtures against the packaged jar.
package-client-jvm-tests:
    #!/usr/bin/env bash
    set -euo pipefail
    artifacts={{ quote(cargo_target_dir + "/client-conformance/java") }}
    mkdir -p "${artifacts}/tests"
    javac --release 21 -Xlint:all -Werror -cp "${artifacts}/nervix-client.jar" \
        -d "${artifacts}/tests" crates/client-jvm/tests/java/io/nervix/client/*.java

# Hold the Java 21 API to its host ownership and bulk-column contracts.
test-client-jvm-api: build-client-jvm-tests
    just cargo_target_dir={{ quote(cargo_target_dir) }} test-client-jvm-api-run

# Run prepared Java boundary fixtures without rebuilding their native prerequisite.
test-client-jvm-api-run:
    #!/usr/bin/env bash
    set -euo pipefail
    artifacts={{ quote(cargo_target_dir + "/client-conformance/java") }}
    java -Xcheck:jni -cp "${artifacts}/nervix-client.jar:${artifacts}/tests" \
        io.nervix.client.BoundaryTests
    java -Xcheck:jni -cp "${artifacts}/tests" io.nervix.client.ReentrantClose \
        "${artifacts}/nervix-client.jar"

# Exercise the actual JNI implementations and Java classes in one embedded JVM.
test-client-jvm: test-client-jvm-api
    CARGO_TARGET_DIR={{ quote(cargo_target_dir) }} BOLERO_RANDOM_SEED="${BOLERO_RANDOM_SEED:-2026110404}" cargo test --package nervix-client-jvm --features jvm-tests --lib

# Run one JNI regression after build-client-jvm-tests prepared its Java classes.
test-client-jvm-case filter:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir) }} cargo test --package nervix-client-jvm --features jvm-tests --lib -- {{ quote(filter) }} --exact --nocapture

# Replay a named, provenance-recorded JNI test executable without Cargo rebuilding its inputs.
test-client-jvm-executable executable *args:
    BOLERO_RANDOM_SEED="${BOLERO_RANDOM_SEED:-2026110404}" {{ quote(executable) }} {{ args }}

# Collect the JNI implementation counters from the embedded JVM's current native method table.
coverage-client-jvm output="lcov-client-jvm.info": build-client-jvm-tests
    CARGO_TARGET_DIR={{ quote(cargo_target_dir) }} cargo llvm-cov --package nervix-client-jvm --features jvm-tests --lib --lcov --output-path {{ quote(output) }}

# Build what the cross-language client probes run: the shared Rust binding, the C and C++ probes
# linked against it, the JNI jar and Java probe, the Python package in its virtual environment,
# the Go probe with its generated FlatBuffers code, and the TypeScript probe bundled with its generated code for Node.js
# and Bun. Every generated file lands under the artifacts directory, never in the source tree.
build-client-conformance: build-client-python build-client-jvm
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

# Build the probes, the client library and the scenario suite the conformance run starts, so
# `test-client-conformance-run` compiles nothing when it follows.
build-client-conformance-tests: tests-deps build-client-conformance
    cargo test --no-run --package nervix-scenarios --test scenarios

# Run the cross-language client probes against in-process clusters. Every probe prints the same
# report, which the scenario compares with its one expected report. Select runtimes with a tag
# expression, for example `just test-client-conformance '@client_probe_python or @client_probe_java'`.
test-client-conformance tags="@client_conformance_toolchain" *args: build-client-conformance-tests
    just test-client-conformance-run {{ quote(tags) }} {{ args }}

# Exercise upload completion, retained retries and typed identity conflicts in every binding host.
test-client-upload-conflict-conformance: build-web-console wasm-processor-guests download-onnxruntime build-client-conformance
    just test-client-conformance-run '@client_upload_failure' --concurrency 2 --retry 0

# Run the conformance scenarios from what `build-client-conformance-tests` built. The input is an
# absolute pattern because the scenario binary walks a relative one from the working directory, the
# whole build directory included, before it starts a scenario.
test-client-conformance-run tags="@client_conformance_toolchain" *args:
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    export NERVIX_CLIENT_CONFORMANCE_DIR={{ quote(cargo_target_dir + "/client-conformance") }}
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/debug/libnervix_client_ffi.so") }}
    cargo test --package nervix-scenarios --test scenarios -- \
        --input {{ quote(justfile_directory() / "tests/features/runtime/client_{conformance,suites}.feature") }} \
        --tags {{ quote(tags) }} {{ quote(args) }}

test-runtime-state-capabilities: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --features testing --package nervix-server --doc runtime::

# Validate the small unsafe boundary used by deduplicator expiration tracking.
test-expiry-map:
    cargo test --package nervix-expiry-map

# Bolero reads its checked-in corpus from the source tree, which Miri's isolation forbids, so the
# Miri run lifts isolation and bounds the property's random cases beside that corpus.
test-expiry-map-miri *args:
    MIRIFLAGS="-Zmiri-disable-isolation" BOLERO_RANDOM_ITERATIONS=4 BOLERO_RANDOM_MAX_LEN=512 \
        cargo +nightly miri test --package nervix-expiry-map -- {{ args }}

test-expiry-map-mutants:
    cargo mutants --package nervix-expiry-map --timeout 120

# Walk the Plexus completion graph and fail on any branch that cannot be completed by accepting the
# suggestions the parser itself offers. Kept out of `just test` because it saturates every core for
# tens of seconds; CI runs it once the main tests have passed.
plexus-completion-walk *args:
    cargo test -p nervix-plexus --test completion_walk -- {{ args }}

# The same walk with deduplication relaxed, reaching branches the gating budget stops short of.
# Reports without failing: this is for finding new ones, not for gating.
plexus-completion-walk-deep *args:
    cargo test -p nervix-plexus --test completion_walk -- \
      --report-only --signature-window 8 --max-states 1000000 {{ args }}

# Build every test binary the workspace coverage run executes, under the instrumentation of
# `cargo llvm-cov` and into its target directory, so `test-coverage-run` compiles nothing.
test-coverage-build: coverage-clean-workspace test-coverage-binaries

# Build the workspace's test binaries beside what the target directory already holds, and record
# the test executables a run starts. CI builds them after the scenario suite, so both share one
# target directory and their dependencies compile once.
test-coverage-binaries: tests-deps build-client-jvm-tests
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    # Merge ordinary-mode coverage for the packages that own an execution mode into the workspace
    # profile without running modeled primitives outside their runner. Model checks are not
    # product coverage, so no Shuttle, Loom, Turmoil or diagnostic build contributes to it.
    mode_packages=(
        nervix-client-core
        'nervix-connector*'
        nervix-consensus
        nervix-deadlock
        nervix-execution
        nervix-interconnect
        nervix-model-harness
        nervix-paced-simulation
        nervix-primitives
        nervix-rate-limit
        nervix-scenarios
        nervix-server
        nervix-wasm
    )
    workspace_exclusions=()
    for package in "${mode_packages[@]}"; do
        workspace_exclusions+=(--exclude "${package}")
    done
    # The environment and target directory `cargo llvm-cov` builds with, so the report finds every
    # binary current.
    source <(cargo llvm-cov show-env --sh 2>/dev/null)
    export CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }}
    messages={{ quote(justfile_directory() + "/target/unit-tests/cargo-messages.jsonl") }}
    mkdir -p "$(dirname "${messages}")"
    : > "${messages}"
    coverage() { cargo test --no-run --message-format=json-render-diagnostics "$@" >> "${messages}"; }
    coverage --all-targets --all-features --workspace "${workspace_exclusions[@]}"
    # The server's own tests and the harness regressions: every test but the scenario suite, which
    # runs in its own job.
    coverage --lib --benches --features testing --package nervix-server
    coverage --test harness_liveness --package nervix-scenarios
    coverage --all-targets \
        --package nervix-client-core \
        --package 'nervix-connector*' \
        --package nervix-consensus \
        --package nervix-deadlock \
        --package nervix-execution \
        --package nervix-interconnect \
        --package nervix-model-harness \
        --package nervix-paced-simulation \
        --package nervix-rate-limit \
        --package nervix-wasm
    coverage --all-targets --features native --package nervix-primitives
    python3 -m scripts.unit_test_archive plan

# Run the workspace's tests from the binaries `test-coverage-build` left or a restored archive
# holds, and write their coverage. It compiles nothing: it starts each test executable directly, so
# a run needs neither the build nor a cache.
test-coverage-run:
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    # The profiles go where `cargo llvm-cov report` reads them, beside the executables.
    source <(CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} cargo llvm-cov show-env --sh 2>/dev/null)
    python3 -m scripts.unit_test_archive run
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path lcov-workspace.info
    # The checkout path the report's sources lie below, which `coverage-merge` strips from them.
    pwd -P > coverage-source-root.txt

test-coverage: test-coverage-build test-coverage-run

# Write the archive the tests job restores: the executables `test-coverage-binaries` left and the
# generated inputs a test reads, which `scripts/unit_test_archive.py` names.
test-coverage-pack archive:
    python3 -m scripts.unit_test_archive pack {{ quote(archive) }}

# Restore the archive `test-coverage-pack` wrote into a checkout of the same revision and path,
# checking every file it holds and the shared libraries its executables load.
test-coverage-unpack archive:
    python3 -m scripts.unit_test_archive unpack {{ quote(archive) }}

# Hold the unit archive and its build/restore workflow to the generated-input contract.
test-unit-test-archive:
    python3 -m unittest scripts.tests.test_unit_test_archive

# Build the scenario suite and the binaries its scenarios start, under the instrumentation of
# `cargo llvm-cov`, so `test-scenarios-coverage-run` compiles nothing. Cargo holds a target directory
# for one build at a time, so each helper binary builds beside the suite in a directory of its own,
# and is then installed into the target directory of `cargo llvm-cov`, where the suite starts it and
# `cargo llvm-cov report` reads it.
test-scenarios-coverage-build: build-deps coverage-clean-workspace (run-with-jobs "test-scenarios-coverage-binaries" "6")
    #!/usr/bin/env bash
    set -euo pipefail
    suite={{ quote(cargo_target_dir + "/llvm-cov-target/debug") }}
    tools={{ quote(coverage_tools_dir) }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-plexus-format") }} "${suite}/nervix-plexus-format"
    install -m 755 "${tools}/cli/debug/nervix-cli" "${suite}/nervix-cli"
    install -m 755 "${tools}/deadlock-report/debug/nervix-deadlock-report" "${suite}/nervix-deadlock-report"
    install -m 755 "${tools}/paced-simulation/debug/nervix-paced-simulation" "${suite}/nervix-paced-simulation"
    install -m 755 "${tools}/paced-simulation/debug/libnervix_client_python.so" "${suite}/libnervix_client_python.so"
    install -m 755 "${tools}/paced-simulation/debug/libnervix_client_ffi.so" "${suite}/libnervix_client_ffi.so"

[private, parallel]
test-scenarios-coverage-binaries: test-scenarios-coverage-suite build-plexus-format build-test-cli (coverage-cli-binary (coverage_tools_dir / "cli")) (coverage-deadlock-report-binary (coverage_tools_dir / "deadlock-report")) (coverage-paced-simulation-binaries (coverage_tools_dir / "paced-simulation"))

[private]
test-scenarios-coverage-suite:
    eval "$(cargo llvm-cov show-env --sh 2>/dev/null)" && \
        CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} \
        cargo test --no-run --package nervix-scenarios --test scenarios

# Run the scenario suite from the binaries `test-scenarios-coverage-build` left, and write the
# coverage of the suite and of the paced-simulation Python driver.
test-scenarios-coverage-run: install-paced-simulation-python
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    export NERVIX_PACED_SIMULATION_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-paced-simulation") }}
    export NERVIX_PACED_SIMULATION_PYTHON={{ quote(cargo_target_dir + "/paced-simulation/python/bin/python") }}
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/llvm-cov-target/debug/libnervix_client_ffi.so") }}
    collection_dir="$(mktemp -d {{ quote(cargo_target_dir + "/paced-simulation-coverage.XXXXXX") }})"
    export COVERAGE_FILE="${collection_dir}/python.coverage"
    export PYTHONPATH="{{ justfile_directory() }}/scripts/paced_simulation_coverage${PYTHONPATH:+:${PYTHONPATH}}"
    export COVERAGE_PROCESS_START="{{ justfile_directory() }}/scripts/paced_simulation_coverage/coverage.ini"
    uv run --no-project --with coverage==7.11.0 cargo llvm-cov --no-report \
        --package nervix-scenarios --test scenarios
    uvx --from coverage==7.11.0 coverage combine --data-file "${collection_dir}/python.coverage"
    mkdir -p {{ quote(cargo_target_dir + "/paced-simulation") }}
    uvx --from coverage==7.11.0 coverage lcov --data-file "${collection_dir}/python.coverage" \
        -o "{{ cargo_target_dir }}/paced-simulation/python.lcov"
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path lcov-workspace.info
    pwd -P > coverage-source-root.txt

test-scenarios-coverage: test-scenarios-coverage-build test-scenarios-coverage-run

# Write the archive the scenario shards restore: the executables `test-scenarios-coverage-build`
# left and the generated inputs a scenario run reads, which `scripts/scenario_shards.py` names.
test-scenarios-coverage-pack archive:
    python3 -m scripts.scenario_shards pack {{ quote(archive) }}

# Restore the archive `test-scenarios-coverage-pack` wrote into a checkout of the same revision and
# path, checking every file it holds and the shared libraries its executables load.
test-scenarios-coverage-unpack archive:
    python3 -m scripts.scenario_shards unpack {{ quote(archive) }}
    just cargo_target_dir={{ quote(cargo_target_dir) }} install-paced-simulation-python

# Run one shard, written INDEX/COUNT, of the scenario suite from the executables a restored archive
# holds, and write the coverage of the shard and of the paced-simulation Python driver. It compiles
# nothing: it runs the scenario executable directly, so a shard needs neither the build nor a cache.
test-scenarios-coverage-shard shard:
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    export NERVIX_PACED_SIMULATION_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-paced-simulation") }}
    export NERVIX_PACED_SIMULATION_PYTHON={{ quote(cargo_target_dir + "/paced-simulation/python/bin/python") }}
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/llvm-cov-target/debug/libnervix_client_ffi.so") }}
    collection_dir="$(mktemp -d {{ quote(cargo_target_dir + "/paced-simulation-coverage.XXXXXX") }})"
    export COVERAGE_FILE="${collection_dir}/python.coverage"
    export PYTHONPATH="{{ justfile_directory() }}/scripts/paced_simulation_coverage${PYTHONPATH:+:${PYTHONPATH}}"
    export COVERAGE_PROCESS_START="{{ justfile_directory() }}/scripts/paced_simulation_coverage/coverage.ini"
    # The profiles go where `cargo llvm-cov report` reads them, beside the restored executables.
    source <(CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} cargo llvm-cov show-env --sh 2>/dev/null)
    executable="$(python3 -m scripts.scenario_shards executable)"
    uv run --no-project --with coverage==7.11.0 "${executable}" --shard {{ quote(shard) }}
    # A shard may hold no run of the Python driver, and then it has no Python coverage to report.
    if compgen -G "${collection_dir}/python.coverage.*" > /dev/null; then
        uvx --from coverage==7.11.0 coverage combine --data-file "${collection_dir}/python.coverage"
        mkdir -p {{ quote(cargo_target_dir + "/paced-simulation") }}
        uvx --from coverage==7.11.0 coverage lcov --data-file "${collection_dir}/python.coverage" \
            -o "{{ cargo_target_dir }}/paced-simulation/python.lcov"
    else
        echo "Shard {{ shard }} ran no paced-simulation Python driver, so it reports no Python coverage."
    fi
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path lcov-workspace.info
    pwd -P > coverage-source-root.txt

# Remove collected profiles and instrumented workspace artifacts before collecting a new revision.
# Append recipes retain artifacts, so a source move can otherwise leave stale line mappings beside
# the current ones in a report.
coverage-clean-workspace:
    cargo llvm-cov clean --workspace

# Rewrite lcov-workspace.info and its checkout path from the profiles the last coverage recipe
# collected, over the sources of every workspace package, so crate lines the server's tests executed
# are measured as CI measures them. `coverage-merge` then maps it onto the repository.
coverage-report-workspace *args:
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path lcov-workspace.info {{ args }}
    pwd -P > coverage-source-root.txt

# Merge ordinary coverage into the one repository-relative lcov-workspace.info that every coverage
# consumer reads, and record its inputs, counts and dropped records in
# target/coverage-merge/record.json. An input is a producer's artifact directory or one report, each
# with the coverage-source-root.txt its producer wrote; CI names the three ordinary artifacts, and
# names tooling-only extras with --python-input while preserving required Rust inputs. With
# no inputs this merges the lcov-workspace.info that test-coverage wrote, in place. It fails,
# writing no report, when the report would be incomplete.
coverage-merge *inputs:
    python3 -m scripts.coverage_merge --output lcov-workspace.info --record {{ quote(cargo_target_dir + "/coverage-merge/record.json") }} {{ inputs }}

# Exercise the merge's source mapping, its integrity failures and its CI contract.
test-coverage-merge:
    python3 -m unittest scripts.tests.test_coverage_merge

# Retain the merge's own measured line coverage beside the other Python tooling reports.
coverage-merge-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ cargo_target_dir }}/coverage-merge"
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file "{{ cargo_target_dir }}/coverage-merge/python.coverage" --branch --source=scripts.coverage_merge,scripts.tests.test_coverage_merge -m unittest scripts.tests.test_coverage_merge
    "${coverage[@]}" lcov --data-file "{{ cargo_target_dir }}/coverage-merge/python.coverage" -o "{{ cargo_target_dir }}/coverage-merge/python.lcov"

# Record the selection the full coverage union is derived from in
# target/coverage-full/selection.json: the run's event and pull request label snapshot, whether the
# tested diff required the suites and the Bolero call, the run, its attempt, the tested revision and,
# in CI, each producer job's result from the needs context. Without arguments it records a local
# selection, whose labels name the label-enabled producers collected locally.
coverage-full-select *args:
    python3 -m scripts.coverage_full select --output {{ quote(cargo_target_dir + "/coverage-full/selection.json") }} {{ quote(args) }}

# Combine every coverage producer the recorded selection requires into one workspace line-coverage
# union, target/coverage-full/lcov-full.info, with its record, its contributor breakdown and the lines
# each contribution covered beyond the ordinary report. An input is a downloaded artifact directory,
# an attempt directory, the ordinary lcov-workspace.info or one record; --selection <path> names a
# retained selection instead. It reads reports and records only, so repeating it over retained
# artifacts at the tested revision gives the same lines, and it fails, writing no report, when an
# input is missing, stale, foreign, corrupt, incomplete or not selected.
coverage-full *inputs:
    python3 -m scripts.coverage_full union --selection {{ quote(cargo_target_dir + "/coverage-full/selection.json") }} --directory {{ quote(cargo_target_dir + "/coverage-full") }} {{ quote(inputs) }}

# Exercise the union's selection, validation, attribution and CI contract.
test-coverage-full:
    python3 -m unittest scripts.tests.test_coverage_full scripts.tests.test_ci_execution_policy

# Retain the union's own measured line coverage beside the other Python tooling reports.
coverage-full-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ cargo_target_dir }}/coverage-full"
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file "{{ cargo_target_dir }}/coverage-full/python.coverage" --branch --source=scripts.coverage_full,scripts.tests.test_coverage_full,scripts.tests.test_ci_execution_policy -m unittest scripts.tests.test_coverage_full scripts.tests.test_ci_execution_policy
    "${coverage[@]}" lcov --data-file "{{ cargo_target_dir }}/coverage-full/python.coverage" -o "{{ cargo_target_dir }}/coverage-full/python.lcov"

# Report CRAP over the merged ordinary workspace report as the coverage job does: the pinned cargo-crap
# scores the repository's Rust sources against the report and writes target/crap-report/crap.json
# and crap.md. No score fails the recipe. It fails only when the analysis is vacuous: the merge record
# does not state a complete merge at this revision, the report names a source this checkout does not
# track, a Rust file the report holds was not analysed, or the analysis matched none of them. Name a
# retained coverage-merged artifact's report and record, in a checkout of the revision the record
# names, to reproduce that run's report.
check-coverage report="lcov-workspace.info" record=(cargo_target_dir + "/coverage-merge/record.json"): install-cargo-crap
    python3 -m scripts.crap_report --report {{ quote(report) }} --record {{ quote(record) }} --output {{ quote(cargo_target_dir + "/crap-report") }}

# Exercise the CRAP report's analysis, its vacuity verdict, its reports and its CI contract.
test-crap-report: install-cargo-crap
    python3 -m unittest scripts.tests.test_crap_report

# Retain the CRAP report's own measured line coverage beside the other Python tooling reports.
coverage-crap-report-runner: install-cargo-crap
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ cargo_target_dir }}/crap-report"
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file "{{ cargo_target_dir }}/crap-report/python.coverage" --branch --source=scripts.crap_report,scripts.tests.test_crap_report -m unittest scripts.tests.test_crap_report
    "${coverage[@]}" lcov --data-file "{{ cargo_target_dir }}/crap-report/python.coverage" -o "{{ cargo_target_dir }}/crap-report/python.lcov"

# Measure added executable lines; --pr <number> updates an advisory GitHub PR comment.
coverage-patch base="origin/main" report="lcov-workspace.info" *args:
    python3 -m scripts.patch_coverage --base {{ quote(base) }} --report {{ quote(report) }} --output {{ quote(cargo_target_dir + "/patch-coverage.md") }} {{ args }}

# Measure ordinary harness regressions separately from product coverage and diagnostic profiles.
coverage-harness-liveness *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    harness_reports={{ quote(cargo_target_dir + "/harness-coverage-reports") }}
    mkdir -p "$harness_reports"
    harness_report="$(mktemp -d "$harness_reports/run.XXXXXX")"
    echo "harness coverage attempt: $harness_report"
    export CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/harness-coverage") }}
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --package nervix-scenarios --test harness_liveness -- {{ quote(args) }} 2>&1 | tee "$harness_report/tests.log"
    if ! rg --quiet '^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;' "$harness_report/tests.log"; then
        echo "harness coverage selection completed no tests or ignored selected tests" >&2
        exit 1
    fi
    cargo llvm-cov report --package nervix-scenarios --no-default-ignore-filename-regex --lcov --output-path "$harness_report/lcov.info"
    echo "harness coverage report: $harness_report/lcov.info"

# Export the current ordinary harness profiles after a replay, without another test invocation.
coverage-harness-liveness-report output:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/harness-coverage") }} cargo llvm-cov report --package nervix-scenarios --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }}

# Fetch artifacts of a CI run from their bucket, with the R2_ARTIFACTS_* variables set: pass
# `--name NAME` or `--pattern GLOB`, and `--path DIR`.
ci-artifact-download run *args:
    python3 -m scripts.ci_artifacts download --run {{ quote(run) }} {{ args }}

# Exercise the CI artifacts' transfer against a local fake of their bucket: path selection, parts,
# resumed, paced and retried requests, progress reports, and the workflow steps that use them.
test-ci-artifacts:
    python3 -m unittest scripts.tests.test_ci_artifacts

# Judge whether a pull request's head revision holds the complete evidence a release needs: every
# check tests/release-evidence.toml requires passed on that revision and the label of every
# label-gated method is on the pull request. Writes the register, with every gap an owner of tracked
# locks declares, under target/release-evidence/<revision>/ and exits 1 when the evidence is
# incomplete.
release-evidence pr:
    python3 -m scripts.release_evidence check --pr {{ quote(pr) }}

# Exercise the release evidence gate on recorded check runs, without GitHub.
test-release-evidence:
    python3 -m unittest --quiet scripts.tests.test_release_evidence

# Retain the release evidence gate's own measured line coverage beside the other Python tooling.
coverage-release-evidence:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ cargo_target_dir }}/release-evidence-coverage"
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file "{{ cargo_target_dir }}/release-evidence-coverage/python.coverage" --branch --source=scripts.release_evidence,scripts.tests.test_release_evidence -m unittest scripts.tests.test_release_evidence
    "${coverage[@]}" lcov --data-file "{{ cargo_target_dir }}/release-evidence-coverage/python.coverage" -o "{{ cargo_target_dir }}/release-evidence-coverage/python.lcov"

# Exercise line accounting, Git source changes and advisory comment publication.
test-patch-coverage:
    python3 -m unittest scripts.tests.test_patch_coverage

# Retain the reporter's own measured line coverage alongside other Python tooling reports.
coverage-patch-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ cargo_target_dir }}/patch-coverage"
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file "{{ cargo_target_dir }}/patch-coverage/python.coverage" --branch --source=scripts.patch_coverage,scripts.tests.test_patch_coverage -m unittest scripts.tests.test_patch_coverage
    "${coverage[@]}" lcov --data-file "{{ cargo_target_dir }}/patch-coverage/python.coverage" -o "{{ cargo_target_dir }}/patch-coverage/python.lcov"

# Measure changed server and CLI lines against the server's unit tests and selected Cucumber
# features while iterating. The scenarios run the public CLI, so it is built instrumented and handed
# to them exactly as `test-coverage` does. CI collects workspace coverage with that recipe, and the
# merged report it feeds is what the advisory CRAP report reads.
test-coverage-feature +features: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-plexus-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-plexus-format") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    for feature in {{ features }}; do
        cargo llvm-cov --no-report --package nervix-scenarios \
            --test scenarios -- --input "${feature}" --concurrency 1
    done
    cargo llvm-cov report --package nervix-cli --package nervix-server --lcov \
        --output-path lcov.info

# Add client and vocabulary tests to an existing coverage profile without clearing server and
# public-scenario coverage collected by `test-coverage-feature`.
test-coverage-client-packages:
    cargo llvm-cov --no-report --all-targets \
        --package nervix-client-core --package nervix-client-wire --package nervix-client-ffi \
        --package nervix-client-host --package nervix-client-python --package nervix-models \
        --package nervix-cli --package nervix-web-console

# Add focused backup, wire, state, and primitive tests to a backup feature's coverage profile.
test-coverage-backup-packages:
    cargo llvm-cov --no-report --lib \
        --package nervix-backup --package nervix-plexus --package nervix-models \
        --package nervix-client-wire --package nervix-consensus --package nervix-interconnect \
        --package nervix-wasm-protocol --package nervix-wasm-sdk --package nervix-primitives

# Add the unit tests of one workspace package to the instrumented profile, such as a connector crate
# whose patch the scenario suite exercises only in part.
test-coverage-package-lib package *args:
    cargo llvm-cov --no-report --lib --package {{ package }} {{ args }}

# Extend an existing instrumented profile with focused public regressions after a correction.
test-coverage-scenario-filter feature filter:
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --package nervix-scenarios \
        --test scenarios -- --input {{ quote(feature) }} --name {{ quote(filter) }} \
        --concurrency 1 --retry 0

# Measure browser and CLI binary tests together with their public session scenarios.
test-coverage-clients: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --bins \
        --package nervix-web-console --package nervix-cli
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-plexus-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-plexus-format") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    for feature in \
        tests/features/web-console/connection_status.feature \
        tests/features/web-console/plexus_repl.feature \
        tests/features/web-console/domain_clock.feature \
        tests/features/tools/cli_session.feature; do
        cargo llvm-cov --no-report --package nervix-scenarios \
            --test scenarios -- --input "${feature}" --concurrency 1
    done
    just coverage-clients-report

coverage-clients-report:
    cargo llvm-cov report --package nervix-cli --package nervix-web-console \
        --package nervix-server --lcov --output-path lcov.info

# Refresh the native console's coverage after a focused change without rebuilding the server
# browser scenario harness; its recorded browser and CLI coverage remains in the same target.
coverage-web-console-unit:
    cargo llvm-cov --no-report --bins --package nervix-web-console
    just coverage-clients-report

# Build the standalone CLI with the same coverage flags as its binary unit tests. Cargo's
# all-targets test pass alone leaves only the test executable, which public scenarios do not run.
coverage-cli-binary target_dir=(cargo_target_dir + "/llvm-cov-target"):
    eval "$(cargo llvm-cov show-env --sh 2>/dev/null)" && \
        CARGO_TARGET_DIR={{ quote(target_dir) }} cargo build --package nervix-cli --bin nervix-cli

# The local diagnostic report workflow uses this ordinary instrumented executable.
coverage-deadlock-report-binary target_dir=(cargo_target_dir + "/llvm-cov-target"):
    eval "$(cargo llvm-cov show-env --sh --no-rustc-wrapper 2>/dev/null)" && \
        CARGO_TARGET_DIR={{ quote(target_dir) }} \
        cargo build --package nervix-deadlock --features report-tool --bin nervix-deadlock-report

# Instrument both drivers' Rust owners. The package's extension is kept beside the executables
# for the ordinary report, and its wheel is installed for the Python process that writes profiles.
# The C ABI remains available to C/C++ conformance probes of a general scenario selection.
coverage-paced-simulation-binaries target_dir=(cargo_target_dir + "/llvm-cov-target"):
    #!/usr/bin/env bash
    set -euo pipefail
    eval "$(cargo llvm-cov show-env --sh 2>/dev/null)"
    CARGO_TARGET_DIR={{ quote(target_dir) }} cargo build --package nervix-paced-simulation --package nervix-client-ffi
    just cargo_target_dir={{ quote(cargo_target_dir) }} build-paced-simulation-python dev {{ quote(target_dir) }}

coverage-clients-units:
    cargo llvm-cov --no-report --bins \
        --package nervix-web-console --package nervix-cli
    just coverage-clients-report

# Measure the visual create patch across its Model, language, wire, browser, and server owners,
# including the public browser scenarios that exercise the attached transaction prefix and the
# subscription tab lifecycle.
coverage-visual-create output="target/visual-create.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --lib \
        --package nervix-models --package nervix-client-wire --package nervix-plexus
    cargo llvm-cov --no-report --bin nervix-web-console --package nervix-web-console
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    for feature in visual_create_schema visual_create_relay visual_create_codec visual_create_client_endpoint visual_create_lookup_udf visual_create_ingestor visual_create_processors; do
        cargo llvm-cov --no-report --package nervix-scenarios \
            --test scenarios -- --input "tests/features/web-console/${feature}.feature" \
            --concurrency 1 --retry 0
    done
    just coverage-visual-create-report {{ quote(output) }}

# Write the LCOV report of the profiles `coverage-visual-create` collected, over the packages the
# visual create forms span.
coverage-visual-create-report output="target/visual-create.lcov":
    cargo llvm-cov report --lcov --output-path {{ quote(output) }} \
        --package nervix-models --package nervix-client-wire --package nervix-plexus \
        --package nervix-web-console --package nervix-server

# Write the line coverage of the unit tests of the packages named in `args`, such as
# `--package nervix-vm --package nervix-plexus`, as LCOV to `output`. It checks the patch coverage of
# a change to those packages without the scenario suite that `test-coverage` runs.
coverage-lib output *args:
    cargo llvm-cov --lib --lcov --output-path {{ output }} {{ args }}

# Append the OTEL and related connector-admission scenarios to current-source ordinary profiles.
# Start a new source generation with coverage-clean-workspace before collecting its owners.
coverage-otel output="target/otel.lcov": build-web-console
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov --no-clean --package nervix-scenarios --test scenarios \
        --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- \
        --input {{ quote(justfile_directory() / "tests/features/runtime/otel*.feature") }} \
        --concurrency 4 --retry 0
    cargo llvm-cov --no-clean --package nervix-scenarios --test scenarios \
        --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- \
        --input {{ quote(justfile_directory() / "tests/features/runtime/connector_validation*.feature") }} \
        --concurrency 4 --retry 0

# Complete public archive representation and restore/re-export coverage, including test sources.
coverage-backup-archives output="target/backup-archives.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --lib --package nervix-backup
    cargo llvm-cov --no-report --lib --package nervix-plexus -- backup
    cargo llvm-cov --no-report --package nervix-scenarios --test scenarios -- \
        --input tests/features/cluster/backup.feature \
        --name 'A quiesced backup restores two WASM branches and Kafka domain offsets' \
        --concurrency 2 --retry 0
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- materialized_snapshot
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- backup
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- runtime::state_store::checkpoint_reader
    cargo llvm-cov --no-report --package nervix-scenarios --test scenarios -- \
        --input tests/features/cluster/backup_materialized.feature \
        --name 'A resumed materialized cut preserves interleaved branches and generators through restart|Materialized generations larger than the bulk budget resume on every assigned owner and replica' \
        --concurrency 2 --retry 0
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-backup --package nervix-plexus --package nervix-server

# Deduplicator and window archive coverage: their records and Arrow groups, capture, restore
# conversion into staged pieces and installation, segmented persistence of large windows, the
# framing check their Arrow sections open through, and the public scenarios that resume, re-export
# and skip them, including state above the bulk budget and a conversion refused for room.
coverage-backup-branch-state output="target/backup-branch-state.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    just coverage-cli-binary
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --lib --package nervix-backup
    cargo llvm-cov --no-report --lib --package nervix-models -- window_model_digest
    cargo llvm-cov --no-report --lib --package nervix-expiry-map
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- \
        deduplicator window_ backup restore branch_state runtime_ack snapshot_staging \
        materialized_snapshot checkpoint_stream arrow_body client_batch
    cargo llvm-cov --no-report --package nervix-scenarios --test scenarios -- \
        --input 'tests/features/cluster/backup_branch_state*.feature' --concurrency 2 --retry 0
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-backup --package nervix-models --package nervix-server --package nervix-cli \
        --package nervix-expiry-map

# Append current checkpoint-reader and backup ownership tests to retained archive profiles.
coverage-backup-archives-state-append output="target/backup-archives.lcov":
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov --no-report --lib --package nervix-backup
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- materialized_snapshot
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- backup
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- runtime::state_store::checkpoint_reader
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-backup --package nervix-plexus --package nervix-server

# Ordinary representation coverage includes the protocol's integration-test target and the
# native host, SDK, archive descriptors and current checkpoint storage codecs.
coverage-wasm output="target/wasm-representations.lcov": build-web-console wasm-processor-guests download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --lib --package nervix-wasm --package nervix-wasm-sdk
    cargo llvm-cov --no-report --lib --tests --package nervix-wasm-protocol
    cargo llvm-cov --no-report --lib --package nervix-backup -- wasm_properties
    cargo llvm-cov --no-report --features testing --lib --package nervix-server -- wasm_properties
    cargo llvm-cov --no-report --package nervix-scenarios --test scenarios -- \
        --input tests/features/runtime/wasm_processor.feature --name '^Malformed.*output' \
        --concurrency 1 --retry 0
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-arbitrary --package nervix-wasm-protocol --package nervix-wasm-sdk \
        --package nervix-wasm --package nervix-backup --package nervix-server

# Measure the count adapter through vocabulary properties, production storage codecs, language
# archives and window snapshots. These ordinary tests need no external services or containers.
coverage-archive-counts output="target/archive-counts.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --package nervix-models --test representations
    cargo llvm-cov --no-report --package nervix-consensus --lib
    cargo llvm-cov --no-report --package nervix-interconnect --lib
    cargo llvm-cov --no-report --package nervix-plexus --lib -- bolero_
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- \
        registry::storage::tests
    cargo llvm-cov --no-report --features testing --package nervix-server --lib -- \
        runtime::window_state::tests
    cargo llvm-cov --no-report --package nervix-scenarios --test scenarios -- \
        --input tests/features/runtime/session_subscription_options.feature \
        --name Blocking.sampled.session --retry 0
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-models --package nervix-arbitrary --package nervix-consensus \
        --package nervix-interconnect --package nervix-server

# Retain collected profiles and export the vocabulary and consensus test lines CI also reports.
coverage-archive-counts-tests-append output="target/archive-counts-tests.lcov":
    cargo llvm-cov --no-report --package nervix-models --test representations
    cargo llvm-cov --no-report --package nervix-consensus --lib
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }}

# Retain the current package profiles while collecting both server archive owners.
coverage-archive-counts-server-append output="target/archive-counts.lcov": download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov --no-clean --features testing --package nervix-server --lib -- \
        registry::storage::tests
    cargo llvm-cov --no-clean --features testing --package nervix-server --lib -- \
        runtime::window_state::tests
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-models --package nervix-arbitrary --package nervix-consensus \
        --package nervix-interconnect --package nervix-server

# Measure the server owner regressions with the same dependencies and environment as test-runtime.
coverage-runtime output="target/task-handles-runtime.lcov" *args: build-web-console wasm-processor-guests download-onnxruntime
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo llvm-cov --package nervix-server --features testing --lib --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} {{ args }}

# Ordinary coverage for delivery correlations, membership publication and authenticated relay owners.
coverage-remote-owners output="target/remote-owners.lcov": build-web-console wasm-processor-guests download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    just coverage-clean-workspace
    stages="$(mktemp -d {{ quote(cargo_target_dir + "/remote-owner-coverage.XXXXXX") }})"
    cargo llvm-cov --no-report --package nervix-server --features testing --lib -- remote_
    # Later invocations retain the first profile; --no-report and --no-clean cannot be combined.
    cargo llvm-cov --no-clean --lcov --output-path "${stages}/cluster.lcov" \
        --package nervix-server --features testing --lib -- cluster::tests
    cargo llvm-cov --no-clean --lcov --output-path "${stages}/interconnect.lcov" \
        --package nervix-interconnect --lib
    cargo llvm-cov --no-clean --lcov --output-path "${stages}/ack-cost.lcov" \
        --package nervix-server --features testing --lib -- remote_ack_owner_cost --ignored --nocapture
    cargo llvm-cov --no-clean --lcov --output-path "${stages}/frame-cost.lcov" \
        --package nervix-interconnect --lib -- remote_relay_frame_cost --ignored --nocapture
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }}

# Measure selected public scenarios with the same arguments as `test-scenarios`.
coverage-scenarios output *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    just coverage-paced-simulation-binaries
    export NERVIX_PACED_SIMULATION_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-paced-simulation") }}
    export NERVIX_PACED_SIMULATION_PYTHON={{ quote(cargo_target_dir + "/paced-simulation/python/bin/python") }}
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/llvm-cov-target/debug/libnervix_client_ffi.so") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-plexus-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-plexus-format") }}
    cargo llvm-cov --package nervix-scenarios --test scenarios --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ quote(args) }}

# Add selected scenarios to the current coverage profiles without rebuilding unchanged artifacts.
coverage-scenarios-append output *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    just coverage-paced-simulation-binaries
    export NERVIX_PACED_SIMULATION_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-paced-simulation") }}
    export NERVIX_PACED_SIMULATION_PYTHON={{ quote(cargo_target_dir + "/paced-simulation/python/bin/python") }}
    export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/llvm-cov-target/debug/libnervix_client_ffi.so") }}
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-plexus-format") }} \
        {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-plexus-format") }}
    cargo llvm-cov --no-clean --package nervix-scenarios --test scenarios --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ quote(args) }}

# Measure both published paced drivers through their public scenarios, including Python threads
# and Rust unit tests. The Python report is written beside `output` with a `.python.lcov` suffix.
# A compiled scenario binary can run the instrumented drivers without rebuilding the server.
coverage-paced-simulation output="target/paced-simulation.lcov" scenario_binary="" *args: tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    collection_dir="$(mktemp -d {{ quote(cargo_target_dir + "/paced-simulation-coverage.XXXXXX") }})"
    export COVERAGE_FILE="${collection_dir}/python.coverage"
    export PYTHONPATH="{{ justfile_directory() }}/scripts/paced_simulation_coverage${PYTHONPATH:+:${PYTHONPATH}}"
    export COVERAGE_PROCESS_START="{{ justfile_directory() }}/scripts/paced_simulation_coverage/coverage.ini"
    if [[ -n {{ quote(scenario_binary) }} ]]; then
        just coverage-paced-simulation-binaries
        (
            source <(CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/llvm-cov-target") }} \
                cargo llvm-cov show-env --sh 2>/dev/null)
            export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/debug/nervix-cli") }}
            export NERVIX_PACED_SIMULATION_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-paced-simulation") }}
            export NERVIX_PACED_SIMULATION_PYTHON={{ quote(cargo_target_dir + "/paced-simulation/python/bin/python") }}
            export NERVIX_CLIENT_LIBRARY={{ quote(cargo_target_dir + "/llvm-cov-target/debug/libnervix_client_ffi.so") }}
            install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-plexus-format") }} \
                {{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-plexus-format") }}
            uv run --no-project --with coverage==7.11.0 just test-scenarios-binary {{ quote(scenario_binary) }} \
                --input tests/features/runtime/paced_simulation.feature {{ args }}
        )
    else
        uv run --no-project --with coverage==7.11.0 just coverage-scenarios {{ quote(output) }} \
            --input tests/features/runtime/paced_simulation.feature {{ args }}
    fi
    uvx --from coverage==7.11.0 coverage combine --data-file "${collection_dir}/python.coverage"
    uvx --from coverage==7.11.0 coverage lcov --data-file "${collection_dir}/python.coverage" \
        -o {{ quote(output + ".python.lcov") }}
    just coverage-paced-simulation-report {{ quote(output) }}

# Add the driver's unit coverage to already collected public-driver profiles and export them.
coverage-paced-simulation-report output="target/paced-simulation.lcov":
    cargo llvm-cov --no-clean --package nervix-paced-simulation --lib --lcov --output-path {{ quote(output) }}
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov \
        --output-path {{ quote(output) }}

# Measure the Redis DNS connector, its shared TLS/DNS code, and public source/sink scenarios.
coverage-redis output="target/redis-dns.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --lib \
        --package nervix-dns \
        --package nervix-connector \
        --package nervix-connector-redis
    cargo llvm-cov --no-report --lib --package nervix-server -- redis_
    cargo llvm-cov --no-report --lib --package nervix-server -- sources_that_resolve_names_report_missing_node_dns_as_start_failure
    just coverage-cli-binary
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --package nervix-scenarios --test scenarios -- \
        --input tests/features/runtime/redis_dns_resolution.feature --name Redis --retry 0 --concurrency 1
    just coverage-redis-report {{ quote(output) }}

coverage-redis-report output="target/redis-dns.lcov":
    cargo llvm-cov report --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} \
        --package nervix-server \
        --package nervix-dns \
        --package nervix-connector \
        --package nervix-connector-redis

coverage-redis-units-append output="target/redis-dns.lcov":
    cargo llvm-cov --no-clean --lib --package nervix-connector-redis
    just coverage-redis-report {{ quote(output) }}

coverage-redis-server-units-append output="target/redis-dns.lcov":
    cargo llvm-cov --no-clean --lib --package nervix-server -- redis_
    cargo llvm-cov --no-clean --lib --package nervix-server -- sources_that_resolve_names_report_missing_node_dns_as_start_failure
    just coverage-redis-report {{ quote(output) }}

# Collect the changed DNS client units and their public one-/three-node paths into one LCOV
# profile so patch coverage can be checked before opening the PR.
coverage-dns-clients output="target/dns-clients.lcov": tests-deps
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --all-targets \
        --package nervix-dns \
        --package nervix-client-core \
        --package nervix-client-ffi \
        --package nervix-client-host \
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
    just coverage-deadlock-report-binary
    export NERVIX_DEADLOCK_REPORT_TOOL={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-deadlock-report") }}
    export NERVIX_TEST_CLI_PATH={{ quote(cargo_target_dir + "/llvm-cov-target/debug/nervix-cli") }}
    cargo llvm-cov --no-report --features testing --package nervix-server --lib
    run_scenario() {
        cargo llvm-cov --no-report --package nervix-scenarios --test scenarios -- \
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
coverage-dns-clients-report output="target/dns-clients.lcov":
    cargo llvm-cov report --lcov --output-path {{ quote(output) }} \
        --package nervix-server \
        --package nervix-dns \
        --package nervix-client-core \
        --package nervix-client-ffi \
        --package nervix-client-host \
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
# The canonical runner owns packages, per-check exploration, paired nondeterminism and replay.
coverage-shuttle output filter="":
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run test-shuttle --output {{ quote(output) }} --filter {{ quote(filter) }}

# Run the canonical current-source Loom inventory; weakening qualification remains independent.
coverage-loom output filter="":
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run test-loom --output {{ quote(output) }} --filter {{ quote(filter) }}

# Write line coverage for binary unit tests, such as the CLI's main target.
coverage-bins output *args:
    cargo llvm-cov --bins --lcov --output-path {{ output }} {{ args }}

# Exercise the CLI binary through the public transaction, clock, subscription lifetime and REPL
# reconnect scenarios with LLVM coverage.
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
        just test-scenarios --input tests/features/runtime/plexus_transactions.feature --name CLI
    LLVM_PROFILE_FILE="$coverage_dir/cli-%p-%m.profraw" \
        NERVIX_TEST_CLI_PATH="$coverage_dir/debug/nervix-cli" \
        just test-scenarios --input tests/features/tools/cli_session.feature --name clock
    LLVM_PROFILE_FILE="$coverage_dir/cli-%p-%m.profraw" \
        NERVIX_TEST_CLI_PATH="$coverage_dir/debug/nervix-cli" \
        just test-scenarios --input tests/features/tools/cli_session.feature --name 'keeps.printing|subscription'
    llvm_bin="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | awk '/^host:/{print $2}')/bin"
    "$llvm_bin/llvm-profdata" merge -sparse "$coverage_dir"/*.profraw -o "$coverage_dir/merged.profdata"
    "$llvm_bin/llvm-cov" export "$coverage_dir/debug/nervix-cli" \
        --instr-profile="$coverage_dir/merged.profdata" --format=lcov > {{ quote(output) }}

# Collect the canonical Turmoil suite's product-source coverage with retained attempt evidence.
coverage-turmoil output:
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run test-turmoil --output {{ quote(output) }}

# Run the eligible native extras in their declared modes, with the canonical Shuttle and Loom
# runners, the canonical Turmoil suite, and each primitive conformance mode.
# `test-primitives` selects all native conformance.
# Prerequisites build outside instrumentation. Compile/browser checks and weakening qualification
# run independently. Each producer
# writes lcov.info, completion.json, executions.jsonl and export.log to a fresh
# target/native-coverage/<producer>/<mode>/<toolchain>/<attempt>/, and CI runs one per step.
# Run all: `just coverage-native-extras`; one: `just coverage-native-extras bench-smoke`.
coverage-native-extras *producers:
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run {{ producers }}

# Compile the named producers' instrumented executables without running them: their prepare
# recipes, then each one's build recipe in the instrumented build `coverage-native-extras` collects
# from, which then compiles nothing. CI builds `test-shuttle`, `test-loom`, `test-turmoil` and each
# Deloxide lane selection this way, and publishes its cache before the run.
coverage-native-extras-build +producers:
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} build {{ producers }}

# CI calls these only after the owning phase succeeds. Exported coverage and compiler evidence
# live outside these Cargo build outputs; failed phases keep their outputs for diagnosis.
clean-ci-docs:
    cargo clean --target-dir {{ quote(cargo_target_dir / "typed-ratchet/docs") }}

clean-ci-benchmarks:
    cargo clean --profile dev --target-dir {{ quote(cargo_target_dir / "native-coverage-build") }}

# Exercise the native coverage collector: its producer inventory, source policy, selection and
# failure handling, then instrumented runs of a fixture crate through the real toolchain.
test-native-coverage:
    NERVIX_NATIVE_COVERAGE_TOOLCHAIN_TESTS=required python3 -m unittest --quiet scripts.tests.test_native_coverage scripts.tests.test_model_coverage scripts.tests.test_turmoil_coverage

# Focused CI contract and patch-publication regressions for the Turmoil coverage producer.
test-turmoil-coverage-runner:
    python3 -m unittest scripts.tests.test_turmoil_coverage scripts.tests.test_patch_coverage

# Exercise the Deloxide lane runner: its inventory, discovery, supervision of real processes,
# accounting, record, replay, qualification classes, applicability check and CI contract.
# A unittest target selects a focused class or method; the default keeps the complete module.
test-deloxide-lane-runner target="scripts.tests.test_deloxide_lane":
    python3 -m unittest --quiet {{ quote(target) }}

# Verify interruption, timeout and process-group cleanup through real child processes.
test-deloxide-supervision:
    python3 -m unittest scripts.tests.test_deloxide_lane.SupervisionTests

# Line coverage of collection and canonical runner changes, with actual command/artifact fixtures,
# including Turmoil's CI contract and the Deloxide lane's supervision of real processes.
coverage-model-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/model-coverage
    coverage=(uvx --from coverage==7.11.0 coverage)
    export COVERAGE_FILE="{{ cargo_target_dir }}/model-coverage/python.coverage"
    NERVIX_NATIVE_COVERAGE_TOOLCHAIN_TESTS=required "${coverage[@]}" run --branch --source=scripts.native_coverage,scripts.coverage_workspace_wrapper,scripts.model_evidence,scripts.shuttle_checks,scripts.loom_models,scripts.deloxide_lane -m unittest scripts.tests.test_native_coverage scripts.tests.test_model_coverage scripts.tests.test_turmoil_coverage scripts.tests.test_shuttle_checks scripts.tests.test_loom_models
    # The supervisor reaps its interpreter's adopted children. Give its real-process fixtures
    # their own interpreter so another fixture's helper cannot become a reported leftover.
    "${coverage[@]}" run --append --branch --source=scripts.native_coverage,scripts.coverage_workspace_wrapper,scripts.model_evidence,scripts.shuttle_checks,scripts.loom_models,scripts.deloxide_lane -m unittest scripts.tests.test_deloxide_lane
    "${coverage[@]}" lcov -o "{{ cargo_target_dir }}/model-coverage/python.lcov"
    "${coverage[@]}" report

# Run every Criterion suite with the release profile. Extra arguments are forwarded to Criterion.
# The server benches link the console the server serves, so the console is built first rather than
# left to whatever ran before them.
bench *args: build-web-console
    cargo bench --package nervix-primitives --features native --bench ordinary_cost -- {{ args }}
    cargo bench --package nervix-server --bench relay_interaction --features benchmarks -- {{ args }}
    cargo bench --package nervix-branch-instances --bench owned_branches -- {{ args }}
    cargo bench --package nervix-server --bench subscription_row_encoding --features benchmarks -- {{ args }}
    cargo bench --package nervix-server --bench wasm_checkpoint --features benchmarks -- {{ args }}
    cargo bench --package nervix-server --bench state_replication --features benchmarks -- {{ args }}
    cargo bench --package nervix-columnar-json --bench json_encode -- {{ args }}
    cargo bench --package nervix-vm --bench vm -- {{ args }}

# Exercise every Criterion body once without spending CI's smoke-test budget on release codegen.
bench-smoke: build-web-console wasm-processor-guests download-onnxruntime bench-smoke-bodies

# The Criterion bodies `bench-smoke` exercises, without the console build that precedes them there.
# `coverage-native-extras` builds the console outside its instrumentation and then runs these in it.
bench-smoke-bodies:
    cargo bench --profile dev --package nervix-primitives --features native --bench ordinary_cost -- --test
    cargo bench --profile dev --package nervix-server --bench relay_interaction --features benchmarks -- --test
    cargo bench --profile dev --package nervix-server --bench admitted_work --features benchmarks -- --test
    cargo bench --profile dev --package nervix-branch-instances --bench owned_branches -- --test
    cargo bench --profile dev --package nervix-server --bench subscription_row_encoding --features benchmarks -- --test
    cargo bench --profile dev --package nervix-server --bench wasm_checkpoint --features benchmarks -- --test
    cargo bench --profile dev --package nervix-server --bench task_handles --features benchmarks -- target/task-handles-smoke.json
    cargo bench --profile dev --package nervix-server --bench state_replication --features benchmarks -- --test
    cargo bench --profile dev --package nervix-server --bench window_sketches --features benchmarks -- --test
    cargo bench --profile dev --package nervix-columnar-json --bench json_encode -- --test
    cargo bench --profile dev --package nervix-simd-kernels --bench constant_division -- --test
    cargo bench --profile dev --package nervix-simd-kernels --bench byte_classes -- --test
    cargo bench --profile dev --package nervix-connector-syslog --bench stream_framing --features benchmarks -- --test
    cargo bench --profile dev --package nervix-connector-kafka --bench partition_offsets --features benchmarks -- --test
    cargo bench --profile dev --package nervix-vm --bench vm -- --test
    just bench-retained-channels-bodies
    just bench-remote-owners-bodies
    just bench-materialized-state-bodies

# Measure the data-plane work a node admits through its bounded executor, as the runtime submits it:
# one branched input prepared into its branch batches, and an emitter batch encoded through a JAQ
# transformation. Extra arguments are forwarded to Criterion.
bench-admitted-work *args: build-web-console
    cargo bench --package nervix-server --bench admitted_work --features benchmarks -- {{ args }}

# Real RabbitMQ write costs. Serialize measurements across checkouts of this repository.
bench-rabbitmq-publish *args:
    #!/usr/bin/env bash
    set -euo pipefail
    benchmark_lock="$(git rev-parse --path-format=absolute --git-common-dir)/nervix-benchmark.lock"
    flock --exclusive --timeout 60 "$benchmark_lock" \
        cargo bench --package nervix-connector-rabbitmq --features benchmarks --bench publish -- {{ quote(args) }}

# Exercise the same real-broker fixture and publication/count oracles once per benchmark body.
test-rabbitmq-publish-benchmark:
    cargo bench --profile dev --package nervix-connector-rabbitmq --features benchmarks --bench publish -- --test

# Measure ordinary native atomic, publication, lock and reference-count costs on this host.
# Model and diagnostic builds have different instrumentation and are not performance baselines.
bench-primitives *args:
    cargo bench --package nervix-primitives --features native --bench ordinary_cost -- {{ args }}

# Cover the primitive cost probe's executable paths without treating instrumented time as a
# performance measurement.
coverage-bench-primitives output="target/primitives-ordinary-cost.lcov":
    cargo llvm-cov --package nervix-primitives --features native --bench ordinary_cost --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- --test

# Exercise the admission benchmark's current emitter context while measuring its line coverage.
coverage-admitted-work output="target/admitted-work.lcov": build-web-console
    cargo llvm-cov --bench admitted_work --features benchmarks --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- --test

# Run only the relay-interaction Criterion suite, including the delivery a node input records for
# one batch at 1, 64, and 1,024 rows. Extra arguments are forwarded to Criterion.
bench-relay-interaction *args: build-web-console
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
bench-subscription-rows *args:
    cargo bench --package nervix-server --bench subscription_row_encoding --features benchmarks -- {{ args }}

# Write raw component timing and retained-allocation samples for Arrow-to-Row delivery.
client-wire-cost output="target/client-wire-cost.json":
    cargo bench --package nervix-server --bench client_wire_cost --features benchmarks -- {{ quote(output) }}

# Measure each stage a native client runs before it sends a command: building the statement
# grammar, parsing with a grammar built for the call or held across calls, the execution identity
# and the wire request. It runs in the release profile and in the debug profile the scenario harness
# builds its clients with.
client-wire-command-cost output_dir="target/client-wire-command-cost":
    cargo bench --package nervix-client-core --bench command_preparation -- "$(realpath -m {{ quote(output_dir + "/release.json") }})"
    cargo test --package nervix-client-core --bench command_preparation -- "$(realpath -m {{ quote(output_dir + "/debug.json") }})"

# Run the component benchmark with coverage instrumentation for changed benchmark lines.
coverage-client-wire-cost output="target/client-wire-cost.lcov" report="target/client-wire-cost-coverage.json":
    cargo llvm-cov --bench client_wire_cost --features benchmarks --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ quote(report) }}

# Check the typed Arrow workload's selection, null, redaction, branch and frame-limit assertions.
test-client-wire-bench-fixture:
    cargo test --package nervix-server --features benchmarks --lib subscription_row::benchmark::tests -- --nocapture

# Capture raw timings through the exported Rust binding's C ABI on a 100-row frame.
client-wire-binding-cost output="target/client-wire-binding-cost.json":
    NERVIX_CLIENT_WIRE_BINDING_COST_OUTPUT="$(realpath -m {{ quote(output) }})" cargo test --package nervix-client-ffi --lib tests::profiles_bulk_binding_access_and_retain_release -- --exact --nocapture

# Measure the Python package's host and GC costs on the same live rows used by conformance.
client-wire-binding-host-cost output_dir="target/client-wire-binding-host":
    NERVIX_CLIENT_WIRE_BINDING_PROFILE_DIR="$(realpath -m {{ quote(output_dir) }})" just test-client-conformance '@client_probe_python' --concurrency 1 --retry 0

# Compare the same native command and fresh connections over plaintext and TLS gRPC, in alternating
# pairs against two release servers that differ only in their transport.
client-wire-tls-cost output_dir="target/client-wire-tls-cost": build-web-console download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo build --release --package nervix-server --bin nervix-server
    export NERVIX_CLIENT_WIRE_TLS_SERVER={{ quote(cargo_target_dir + "/release/nervix-server") }}
    export NERVIX_CLIENT_WIRE_TLS_OUTPUT_DIR={{ quote(output_dir) }}
    cargo test --package nervix-scenarios --test scenarios -- \
        --input tests/features/runtime/client_wire_tls_cost.feature \
        --tags @client_wire_tls_cost \
        --concurrency 1 \
        --retry 0

# Measure runtime-state replication on the owner and on a replica: a Kafka offset commit its one
# replica acknowledges, and the branch lifecycle check a replica makes before it installs each
# branch checkpoint, for lifecycles of 16, 128 and 1,024 branches.
bench-state-replication *args: build-web-console
    cargo bench --package nervix-server --bench state_replication --features benchmarks -- {{ args }}

# Measure durable WASM guest-state checkpoints against unsynchronized writes of the same states. The
# store lives under the crate target directory, so the synchronization cost is that of its storage.
bench-wasm-checkpoint *args:
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

# Measure the load driver's tests whose names contain `filter` with ordinary native coverage; by
# default its preparation controls, and every test with an empty filter.
coverage-benchmark-load output="target/benchmark-load.lcov" filter="empty_topic_setup":
    cargo llvm-cov --package nervix-benchmark --bin nervix-benchmark-load --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ quote(filter) }}

# Build the pinned Flink image with its matching Kafka SQL connector.
benchmark-flink-image:
    docker build --file "{{ justfile_directory() }}/benches/flink/Dockerfile" \
        --tag nervix-benchmark-flink:2.0.1 \
        "{{ justfile_directory() }}/benches/flink"

# Build and benchmark the current local Nervix checkout.
benchmark-nervix-local benchmark_name="kafka-filter-map" *args: build-web-console
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

# Exercise the six rewritten codec graphs through the real workload driver without timing claims.
[positional-arguments]
benchmark-codec-heads image artifacts=(cargo_target_dir + "/codec-benchmark-smoke") *workloads:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --package nervix-benchmark --bins
    shift 2
    if [[ "$#" == 0 ]]; then
        set -- hot-path-ingest hot-path-processor hot-path-relay-fanout hot-path-remote-delivery kafka-dedup-window kafka-filter-map
    fi
    for workload in "$@"; do
        "{{ cargo_target_dir }}/debug/nervix-benchmark" run "${workload}" \
            --implementation nervix --nervix-mode image --nervix-image {{ quote(image) }} \
            --duration-seconds 1 --warmup-seconds 1 --partitions 2 \
            --artifacts-root {{ quote(artifacts) }}
    done

# Build once, then run every declared workload implementation sequentially with local Nervix.
benchmark-all-local *args: build-web-console benchmark-flink-image
    cargo build --release \
        --package nervix-server --bin nervix-server \
        --package nervix-benchmark --bins
    "{{ cargo_target_dir }}/release/nervix-benchmark" run-all \
        --nervix-mode local \
        --server-binary "{{ cargo_target_dir }}/release/nervix-server" {{ args }}

# Local same-hardware A/B: build the baseline ref and the current tree once each into cached
# binaries under target/ab/, then interleave runs per arm so machine drift cancels out. This is
# how performance claims are established; CI benchmark comments are only a same-run smoke signal.
benchmark-ab baseline_ref runs="3" benchmark_name="kafka-filter-map" *args: build-web-console
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

# Format explicit modules during a focused edit without traversing sibling module trees.
cargo-fmt-files +files:
    rustup run nightly rustfmt --edition 2024 --config skip_children=true {{ files }}

cargo-fmt:
    cargo +nightly fmt

# Format selected Rust files without revisiting their child modules.
fmt-rust-files +files:
    rustup run nightly rustfmt --config skip_children=true {{ quote(files) }}

# Format one package during a focused implementation cycle.
cargo-fmt-package package:
    cargo +nightly fmt --package {{ package }}

# Format one Rust module and its children during a focused edit.
rustfmt-file file:
    rustfmt +nightly {{ quote(file) }}

# Ripgrep honors the repository ignore rules, so generated target trees are not formatted.
taplo-format:
    git ls-files -z --cached --others --exclude-standard '*.toml' | xargs -0 -r taplo format

[parallel]
fmt: cargo-fmt fmt-typed-ratchet taplo-format dockerfmt gherkin-fmt plexus-fmt autoinherit

cargo-fmt-check:
    cargo +nightly fmt --check

# Check one Rust file with the workspace's formatter and edition.
rustfmt-check path:
    rustfmt +nightly --edition 2024 --check {{ quote(path) }}

taplo-format-check:
    git ls-files -z --cached --others --exclude-standard '*.toml' | xargs -0 -r taplo format --check

[parallel]
fmt-check: cargo-fmt-check fmt-check-typed-ratchet taplo-format-check dockerfmt-check gherkin-fmt-check plexus-fmt-check autoinherit-check

gherkin-fmt:
    ghokin fmt replace tests/features
    ghokin fmt replace crates/rate-limit/tests/features

gherkin-fmt-check:
    ghokin check tests/features
    ghokin check crates/rate-limit/tests/features

# Format every Plexus file in the repository.
plexus-fmt:
    cargo run -q --package nervix-plexus-format -- .

# Report every Plexus file in the repository that is not formatted.
plexus-fmt-check:
    cargo run -q --package nervix-plexus-format -- --check .

# Format only the supplied Plexus documents.
[positional-arguments]
plexus-fmt-files +files:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo run -q --package nervix-plexus-format -- "$@"

build-plexus-format:
    cargo build --package nervix-plexus-format

autoinherit:
    cargo autoinherit

autoinherit-check:
    #!/bin/bash
    set -e
    git diff --exit-code 2>/dev/null >/dev/null  || echo "skip autoinherit on dirty working tree" && exit 0
    cargo autoinherit
    git diff --exit-code

# Clippy runs in lanes. A lane holds the configurations that can reuse one another's build artifacts:
# one execution mode, one target and toolchain, and packages that share most of their dependency
# graph. It lints them one after another in a build directory of its own, so each finds fresh what
# the configurations before it compiled alike, and none waits for another lane's Cargo lock or holds
# a `--jobs` slot while it waits. Lanes run in parallel, as many at once as `--jobs` allows, so the
# longest lane bounds a run. A family too large for one lane splits into numbered sublanes of at most
# eight configurations, closer packages together, trading the reuse between sublanes for parallelism.
clippy_build_directory(lane) := cargo_target_dir / "clippy" / lane

# The packages linted with every feature, by lane: the Plexus language and its clients, the
# libraries and engines below them, and the cluster's network stack.
clippy_language_packages_1 := ["nervix-plexus", "nervix-plexus-format", "nervix-roto", "nervix-vm", "nervix-web-console"]

clippy_language_packages_2 := ["nervix-benchmark", "nervix-cli", "nervix-client-ffi", "nervix-client-host", "nervix-client-jvm", "nervix-client-python"]

clippy_foundation_packages_1 := ["nervix-approx-into", "nervix-bounded-write", "nervix-crypto", "nervix-dataflow-graph", "nervix-expiry-map", "nervix-primitives-macros", "nervix-recovery"]

clippy_foundation_packages_2 := [
    "nervix-arbitrary",
    "nervix-backup",
    "nervix-branch-instances",
    "nervix-checkpoint-replication",
    "nervix-client-wire",
    "nervix-columnar-json",
    "nervix-jaq",
    "nervix-models",
]

clippy_foundation_packages_3 := ["nervix-simd-kernels", "nervix-wasm-protocol", "nervix-wasm-sdk"]

clippy_cluster_packages := ["nervix-dns", "nervix-test-environment"]

# The connector packages, by lane: the contract and the HTTP services, the databases, the message
# brokers, and the streams and stores of messages.
clippy_service_connector_packages_1 := ["nervix-connector", "nervix-connector-http", "nervix-connector-otel", "nervix-connector-prometheus", "nervix-connector-sentry"]

clippy_service_connector_packages_2 := ["nervix-connector-clickhouse", "nervix-connector-iceberg", "nervix-connector-mongodb", "nervix-connector-mysql", "nervix-connector-postgres"]

clippy_messaging_connector_packages_1 := ["nervix-connector-kafka", "nervix-connector-mqtt", "nervix-connector-nats", "nervix-connector-pulsar", "nervix-connector-rabbitmq"]

clippy_messaging_connector_packages_2 := ["nervix-connector-redis", "nervix-connector-sqs", "nervix-connector-syslog", "nervix-connector-websockets", "nervix-connector-zeromq"]

# The libraries linted under the Shuttle mode, by lane: the connectors, and the runtime below them.
clippy_shuttle_connector_packages_1 := ["nervix-connector-fake", "nervix-connector", "nervix-connector-http", "nervix-connector-otel", "nervix-connector-sentry", "nervix-connector-clickhouse"]

clippy_shuttle_connector_packages_2 := ["nervix-connector-iceberg", "nervix-connector-mongodb", "nervix-connector-mysql", "nervix-connector-postgres", "nervix-connector-mqtt", "nervix-connector-nats"]

clippy_shuttle_connector_packages_3 := [
    "nervix-connector-pulsar",
    "nervix-connector-rabbitmq",
    "nervix-connector-redis",
    "nervix-connector-sqs",
    "nervix-connector-syslog",
    "nervix-connector-zeromq",
    "nervix-connector-websockets",
]

clippy_shuttle_runtime_packages := ["nervix-client-core", "nervix-execution", "nervix-interconnect", "nervix-wasm", "nervix-rate-limit"]

# Every package `test-shuttle` explores, in two lanes: the runtime and server, and the connectors.
# scripts/tests/test_shuttle_checks.py holds the two to an exact partition of shuttle_test_packages.
clippy_shuttle_test_packages_1 := ["nervix-execution", "nervix-interconnect", "nervix-client-core", "nervix-consensus", "nervix-server", "nervix-deadlock", "nervix-rate-limit"]

clippy_shuttle_test_packages_2 := [
    "nervix-connector",
    "nervix-connector-kafka",
    "nervix-connector-mqtt",
    "nervix-connector-rabbitmq",
    "nervix-connector-redis",
    "nervix-connector-sqs",
    "nervix-connector-syslog",
    "nervix-connector-websockets",
]

[private, parallel]
clippy-targets: ordinary-clippy-targets shuttle-clippy-targets turmoil-clippy-targets loom-clippy-targets deloxide-clippy-targets

[private, parallel]
ordinary-clippy-targets: \
    clippy-lane-ordinary-server \
    clippy-lane-ordinary-foundation-1 \
    clippy-lane-ordinary-foundation-2 \
    clippy-lane-ordinary-foundation-3 \
    clippy-lane-ordinary-language-1 \
    clippy-lane-ordinary-language-2 \
    clippy-lane-ordinary-service-connectors-1 \
    clippy-lane-ordinary-service-connectors-2 \
    clippy-lane-ordinary-messaging-connectors-1 \
    clippy-lane-ordinary-messaging-connectors-2 \
    clippy-lane-wasm32

# The server's lane starts with the cluster's network stack, every package of which it compiles.
[private]
clippy-lane-ordinary-server: \
    *(clippy-target "ordinary-server" *clippy_cluster_packages ["--all-features", "--all-targets"]) \
    (clippy-target "ordinary-server" "nervix-interconnect" ["--all-targets"]) \
    (clippy-target "ordinary-server" "nervix-consensus" ["--all-targets", "--features", "testing"]) \
    (clippy-target "ordinary-server" "nervix-server" ["--all-targets"]) \
    (clippy-target "ordinary-server" "nervix-server" ["--all-targets", "--package", "nervix-scenarios", "--features", "benchmarks testing"])

[private]
clippy-lane-ordinary-foundation-1: \
    (clippy-target "ordinary-foundation-1" "nervix-primitives" ["--all-targets", "--features", "native"]) \
    (clippy-target "ordinary-foundation-1" "nervix-primitives" ["--all-targets", "--features", "native test-util"]) \
    *(clippy-target "ordinary-foundation-1" *clippy_foundation_packages_1 ["--all-features", "--all-targets"])

[private]
clippy-lane-ordinary-foundation-2: *(clippy-target "ordinary-foundation-2" *clippy_foundation_packages_2 ["--all-features", "--all-targets"])

[private]
clippy-lane-ordinary-foundation-3: \
    *(clippy-target "ordinary-foundation-3" *clippy_foundation_packages_3 ["--all-features", "--all-targets"]) \
    *(clippy-target "ordinary-foundation-3" *["nervix-execution", "nervix-model-harness", "nervix-rate-limit", "nervix-wasm"] ["--all-targets"]) \
    (clippy-target "ordinary-foundation-3" "nervix-deadlock" ["--all-targets", "--features", "report-tool"])

[private]
clippy-lane-ordinary-language-1: \
    *(clippy-target "ordinary-language-1" *clippy_language_packages_1 ["--all-features", "--all-targets"]) \
    *(clippy-target "ordinary-language-1" *["nervix-plexus-format", "nervix-web-console"] ["--all-targets"])

[private]
clippy-lane-ordinary-language-2: \
    *(clippy-target "ordinary-language-2" *clippy_language_packages_2 ["--all-features", "--all-targets"]) \
    (clippy-target "ordinary-language-2" "nervix-client-core" ["--all-targets", "--features", "autocomplete"]) \
    (clippy-target "ordinary-language-2" "nervix-paced-simulation" ["--all-targets"]) \
    (clippy-target "ordinary-language-2" "nervix-cli" ["--all-targets"])

[private]
clippy-lane-ordinary-service-connectors-1: \
    *(clippy-target "ordinary-service-connectors-1" *clippy_service_connector_packages_1 ["--all-targets"]) \
    (clippy-target "ordinary-service-connectors-1" "nervix-connector-fake" ["--all-targets"])

[private]
clippy-lane-ordinary-service-connectors-2: *(clippy-target "ordinary-service-connectors-2" *clippy_service_connector_packages_2 ["--all-targets"])

[private]
clippy-lane-ordinary-messaging-connectors-1: *(clippy-target "ordinary-messaging-connectors-1" *clippy_messaging_connector_packages_1 ["--all-targets"])

[private]
clippy-lane-ordinary-messaging-connectors-2: *(clippy-target "ordinary-messaging-connectors-2" *clippy_messaging_connector_packages_2 ["--all-targets"])

[private]
clippy-lane-wasm32: (clippy-target "wasm32" "nervix-client-wire" ["--target", "wasm32-unknown-unknown"])

# Lint every Shuttle build: each library under the mode, the primitive boundary, and each
# package `test-shuttle` explores in test mode. The full validation matrix runs the same targets.
cargo-clippy-shuttle jobs=default_jobs: (run-with-jobs "shuttle-clippy-targets" jobs)

[private, parallel]
shuttle-clippy-targets: \
    clippy-lane-shuttle-tests-1 \
    clippy-lane-shuttle-tests-2 \
    clippy-lane-shuttle-connectors-1 \
    clippy-lane-shuttle-connectors-2 \
    clippy-lane-shuttle-connectors-3 \
    clippy-lane-shuttle-runtime \
    clippy-lane-shuttle-server

# The `--profile test` targets lint each package `test-shuttle` explores as the runner builds it,
# with its checks compiled. The library targets never compile them, so a warning in a check would
# otherwise pass validation and the Shuttle job alike.
[private]
clippy-lane-shuttle-tests-1: *(clippy-target "shuttle-tests-1" *clippy_shuttle_test_packages_1 ["--lib", "--profile", "test", "--features", "shuttle"])

[private]
clippy-lane-shuttle-tests-2: *(clippy-target "shuttle-tests-2" *clippy_shuttle_test_packages_2 ["--lib", "--profile", "test", "--features", "shuttle"])

[private]
clippy-lane-shuttle-connectors-1: \
    *(clippy-target "shuttle-connectors-1" *clippy_shuttle_connector_packages_1 ["--lib", "--features", "shuttle"]) \
    (clippy-target "shuttle-connectors-1" "nervix-connector-prometheus" ["--lib", "--features", "nervix-connector/shuttle nervix-primitives/shuttle"])

[private]
clippy-lane-shuttle-connectors-2: *(clippy-target "shuttle-connectors-2" *clippy_shuttle_connector_packages_2 ["--lib", "--features", "shuttle"])

[private]
clippy-lane-shuttle-connectors-3: *(clippy-target "shuttle-connectors-3" *clippy_shuttle_connector_packages_3 ["--lib", "--features", "shuttle"])

[private]
clippy-lane-shuttle-runtime: \
    (clippy-target "shuttle-runtime" "nervix-primitives" ["--all-targets", "--features", "shuttle native"]) \
    *(clippy-target "shuttle-runtime" *clippy_shuttle_runtime_packages ["--lib", "--features", "shuttle"])

# Testing changes every package that forwards it, so these share their lane with one another only.
[private]
clippy-lane-shuttle-server: *(clippy-target "shuttle-server" *["nervix-connector-kafka", "nervix-consensus", "nervix-server"] ["--lib", "--features", "shuttle testing"])

[private]
turmoil-clippy-targets: \
    (clippy-target "turmoil" "nervix-primitives" ["--all-targets", "--features", "turmoil native"]) \
    *(clippy-target "turmoil" *["nervix-execution", "nervix-interconnect"] ["--all-targets", "--features", "turmoil"]) \
    (clippy-target "turmoil" "nervix-rate-limit" ["--lib", "--features", "turmoil"])

# Lint the Loom models and harness, the primitive boundary, and each library as it ships and
# in test mode. The same lanes run in the full validation matrix.
cargo-clippy-loom jobs=default_jobs: (run-with-jobs "loom-clippy-targets" jobs)

[private, parallel]
loom-clippy-targets: clippy-lane-loom-1 clippy-lane-loom-2

[private]
clippy-lane-loom-1: \
    (clippy-target "loom-1" "nervix-primitives" ["--all-targets", "--features", "loom native"]) \
    *(clippy-target "loom-1" *["nervix-execution", "nervix-model-harness", "nervix-interconnect", "nervix-connector-fake"] ["--all-targets", "--features", "loom"])

[private]
clippy-lane-loom-2: \
    (clippy-target "loom-2" "nervix-rate-limit" ["--lib", "--features", "loom"]) \
    (clippy-target "loom-2" "nervix-rate-limit" ["--lib", "--profile", "test", "--features", "loom"]) \
    *(clippy-target "loom-2" *["nervix-consensus", "nervix-server"] ["--lib", "--features", "loom"]) \
    (clippy-target "loom-2" "nervix-server" ["--lib", "--profile", "test", "--features", "loom"]) \
    (clippy-target "loom-2" "nervix-consensus" ["--lib", "--profile", "test", "--features", "loom testing"])

# Lint the diagnostic mode: the tracked locks and the detector, the deadlock diagnostics with their
# probes, and the server as a diagnostic node, alone and with its tests and scenario binary. Each
# selection forwards a feature of its own through every package, so each is a lane.
cargo-clippy-deloxide jobs=default_jobs: (run-with-jobs "deloxide-clippy-targets" jobs)

[private, parallel]
deloxide-clippy-targets: (clippy-lane-deloxide "deloxide") (clippy-lane-deloxide "deloxide-order") (clippy-lane-deloxide "deloxide-stress")

[private]
clippy-lane-deloxide selection: \
    (clippy-target selection "nervix-primitives" ["--all-targets", "--features", selection + " native"]) \
    (clippy-target selection "nervix-rate-limit" ["--lib", "--profile", "test", "--features", selection]) \
    (clippy-target selection "nervix-deadlock" ["--all-targets", "--features", selection]) \
    (clippy-target selection "nervix-paced-simulation" ["--all-targets", "--features", selection]) \
    (clippy-target selection "nervix-server" ["--lib", "--bins", "--features", selection]) \
    (clippy-target selection "nervix-server" ["--all-targets", "--package", "nervix-scenarios", "--features", selection + " testing"])

# The shared Clippy command lints one package and its Cargo arguments in its lane's build
# directory. Keep kache configured.
[private]
clippy-target lane package args toolchain="":
    CARGO_TARGET_DIR={{ quote(clippy_build_directory(lane)) }} RUSTFLAGS={{ quote("-Dwarnings " + rustflags) }} cargo {{ if toolchain == "" { "" } else { quote("+" + toolchain) } }} clippy --package {{ quote(package) }} {{ quote(args) }} -q

# Lint one package and all of its targets; extra arguments retain their boundaries. Focused checks
# share one directory of their own, so a package checked again, or one beside it, reuses its build.
cargo-clippy-package package *args: (clippy-target "package" package ["--all-targets", args])

# Lint one library, including its test profile, without selecting product binaries in a model mode.
cargo-clippy-package-lib package *args: (clippy-target "package" package ["--lib", args])

# Lint every product Clippy target, with four concurrent processes by default.
cargo-clippy jobs=default_jobs: (run-with-jobs "clippy-targets" jobs)

[private, parallel]
lint-inner: clippy-targets typed-ratchet-clippy-targets

# Lint the product and compiler tooling in one bounded invocation.
lint jobs=default_jobs: (run-with-jobs "lint-targets" jobs)

[private]
lint-targets: build-web-console lint-inner

audit:
    cargo audit

# Count the architecture debt and fail when a count is above its baseline in debt-baseline.json.
ratchet *args: typed-ratchet-build typed-ratchet-source-build install-cargo-crap
    #!/usr/bin/env bash
    set -euo pipefail
    gate=({{ quote(cargo_target_dir / "typed-ratchet/source/debug/nervix-ratchet") }} gate {{ args }})
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        exec python3 scripts/native_coverage.py exec "${gate[@]}"
    fi
    exec "${gate[@]}"

# Focused regression checks for the debt gate, its shared Rust source scanner and its pinned
# complexity analyzer.
test-ratchet-units: typed-ratchet-source-build install-cargo-crap
    just ratchet-tests lib
    just ratchet-tests audit
    just test-typed-ratchet-reports
    python3 -m unittest scripts.tests.test_validation_entrypoint scripts.tests.test_clock_boundaries

# Exercise the audit command boundary without compiling product targets or the analysis driver.
test-ratchet-audit *args:
    just ratchet-tests lib audit::tests {{ args }}
    just ratchet-tests audit {{ args }}

# Structural Rust properties use a stable source parser, without the compiler driver.
typed-ratchet-source-build:
    #!/usr/bin/env bash
    set -euo pipefail
    source_toolchain="1.99"
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        source_toolchain="nightly-2026-09-17"
    fi
    CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/source cargo +"$source_toolchain" build --manifest-path tools/nervix-lint/Cargo.toml --package nervix-lint-report --package nervix-ratchet

typed-ratchet-build: build-ratchet-runner
    CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/driver cargo +nightly-2026-09-17 build --manifest-path tools/nervix-lint/Cargo.toml --package nervix-lint-driver --package nervix-lint-report

typed-ratchet *args: typed-ratchet-build
    #!/usr/bin/env bash
    set -euo pipefail
    runner={{ quote(cargo_target_dir / "typed-ratchet/source/debug/nervix-ratchet") }}
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        exec python3 scripts/native_coverage.py exec "$runner" analyze {{ args }}
    fi
    exec "$runner" analyze {{ args }}

typed-ratchet-turmoil *args:
    #!/usr/bin/env bash
    set -euo pipefail
    export RUSTFLAGS="--cfg tokio_unstable {{ rustflags }}"
    runner={{ quote(cargo_target_dir / "typed-ratchet/source/debug/nervix-ratchet") }}
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        exec python3 scripts/native_coverage.py exec "$runner" analyze {{ args }}
    fi
    exec "$runner" analyze {{ args }}

# Report review candidates for code shaped around inaccurate lints: repeated keyed lookups, key
# allocations, borrowed copies, duplicated branches, forwarding wrappers, discarded errors,
# disguised clamps and relocated acquisitions, each attributed to the commits and lint history
# that introduced it. It reads the complete compiler evidence `just ratchet` uses and writes
# audit.json and audit.md under target/lint-audit; `--inventory` audits despite open diagnostics.
lint-audit *args: typed-ratchet-build
    #!/usr/bin/env bash
    set -euo pipefail
    audit=({{ quote(cargo_target_dir / "typed-ratchet/source/debug/nervix-ratchet") }} audit {{ args }})
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        exec python3 scripts/native_coverage.py exec "${audit[@]}"
    fi
    exec "${audit[@]}"

test-typed-ratchet-compiler:
    just typed-ratchet --fixture-mode ordinary --recompile --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-ordinary.json
    just test-ratchet-runner
    just ratchet-tests fixtures

test-typed-ratchet-contracts *args: typed-ratchet-build
    just prepare-ratchet-contracts
    just ratchet-tests contracts {{ args }}

# Error propagation uses the actual node startup and DNS hooks, plus both simulation drivers.
test-typed-error-runtime: build-web-console download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    cargo test --package nervix-server --lib --features testing application::startup::tests::
    cargo test --package nervix-server --lib --features testing application::tracing_setup::tests::
    just test-paced-simulation

# Append native failure-path coverage to the public scenario collection.
coverage-typed-error-runtime: build-web-console download-onnxruntime
    #!/usr/bin/env bash
    set -euo pipefail
    export ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)"
    for owner in application::startup::tests:: application::tracing_setup::tests:: registry::restore_plan::tests:: runtime::state_store::tests::; do
        cargo llvm-cov --no-report --package nervix-server --lib --features testing -- "$owner"
    done
    just coverage-paced-simulation-report target/typed-error-runtime.lcov

test-typed-ratchet: test-typed-ratchet-ordinary test-typed-ratchet-product-docs test-typed-ratchet-modeled

test-typed-ratchet-ordinary:
    just test-typed-ratchet-reports
    just test-ratchet-units
    just test-typed-ratchet-compiler
    just test-typed-ratchet-contracts
    just test-typed-ratchet-docs

test-typed-ratchet-modeled:
    just typed-ratchet --fixture-mode shuttle --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-shuttle.json
    just typed-ratchet --fixture-mode loom --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-loom.json
    just typed-ratchet --fixture-mode turmoil --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-turmoil.json
    just typed-ratchet --fixture-mode deloxide --inventory --output {{ quote(cargo_target_dir) }}/typed-ratchet/fixture-deloxide.json
    just prepare-ratchet-contracts shuttle
    just prepare-ratchet-contracts loom
    just prepare-ratchet-contracts turmoil
    just prepare-ratchet-contracts deloxide
    just ratchet-tests modeled

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
    cargo +1.99 test --package nervix-models --doc SettingDefinition
    cargo +1.99 test --package nervix-primitives --doc expect_lint
    cargo +1.99 test --package nervix-vm --doc FunctionInjector
    cargo +1.99 test --package nervix-dns --doc DnsLookupReport
    cargo +1.99 test --package nervix-recovery --doc Discarded
    RUSTUP_TOOLCHAIN=1.99 just test-runtime-state-capabilities

qualify-typed-ratchet-cache: typed-ratchet-build
    #!/usr/bin/env bash
    set -euo pipefail
    qualification_command=({{ quote(cargo_target_dir / "typed-ratchet/source/debug/nervix-ratchet") }} qualify-cache --root {{ quote(justfile_directory()) }} --target-dir {{ quote(cargo_target_dir) }})
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        exec python3 scripts/native_coverage.py exec "${qualification_command[@]}"
    fi
    exec "${qualification_command[@]}"

test-typed-ratchet-reports:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/driver cargo +nightly-2026-09-17 test --manifest-path tools/nervix-lint/Cargo.toml --package nervix-lint-report --lib

# Focused deterministic tests of the analysis runner and host memory admission.
test-ratchet-runner *args:
    just ratchet-tests lib {{ args }}
    just ratchet-tests wrapper {{ args }}

# Focused local tooling counters for patch review; the existing native producer collects CI.
coverage-ratchet-runner *args:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/coverage cargo +1.99 llvm-cov test --manifest-path tools/nervix-lint/Cargo.toml --package nervix-ratchet --lib --test wrapper --test audit --no-report -- {{ args }}
    just coverage-ratchet-report-owner

[private]
coverage-ratchet-report-owner:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/coverage cargo +1.99 llvm-cov test --manifest-path tools/nervix-lint/Cargo.toml --package nervix-lint-report --lib --no-clean --no-default-ignore-filename-regex --lcov --output-path lcov-workspace.info
    # Cached objects name the isolated tooling workspace by its compiler source root.
    sed -i {{ quote("s|^SF:/src/|SF:" + justfile_directory() + "/tools/nervix-lint/|") }} lcov-workspace.info
    pwd -P > coverage-source-root.txt

[private]
ratchet-tests selection *args:
    #!/usr/bin/env bash
    set -euo pipefail
    runner_toolchain="1.99"
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        runner_toolchain="nightly-2026-09-17"
    fi
    if [[ {{ quote(selection) }} == lib ]]; then
        targets=(--lib)
    else
        targets=(--test {{ quote(selection) }})
    fi
    NERVIX_RATCHET_TARGET_DIR={{ quote(cargo_target_dir) }} \
        CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/tests \
        cargo +"$runner_toolchain" test --manifest-path tools/nervix-lint/Cargo.toml --package nervix-ratchet "${targets[@]}" -- {{ args }}

prepare-ratchet-contracts mode="ordinary": typed-ratchet-build
    #!/usr/bin/env bash
    set -euo pipefail
    contract_command=({{ quote(cargo_target_dir / "typed-ratchet/source/debug/nervix-ratchet") }} prepare-contracts --root {{ quote(justfile_directory()) }} --target-dir {{ quote(cargo_target_dir) }} --mode {{ quote(mode) }})
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        exec python3 scripts/native_coverage.py exec "${contract_command[@]}"
    fi
    exec "${contract_command[@]}"

build-ratchet-runner:
    #!/usr/bin/env bash
    set -euo pipefail
    runner_toolchain="1.99"
    if [[ -n "${NERVIX_NATIVE_COVERAGE_ATTEMPT:-}" ]]; then
        runner_toolchain="nightly-2026-09-17"
    fi
    CARGO_TARGET_DIR={{ cargo_target_dir }}/typed-ratchet/source cargo +"$runner_toolchain" build --manifest-path tools/nervix-lint/Cargo.toml --package nervix-ratchet

fmt-typed-ratchet:
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/Cargo.toml --package nervix-lint-driver --package nervix-lint-report --package nervix-ratchet
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/fixtures/Cargo.toml --package nervix-lint-fixtures
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/fixture-macros/Cargo.toml

fmt-check-typed-ratchet:
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/Cargo.toml --package nervix-lint-driver --package nervix-lint-report --package nervix-ratchet --check
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/fixtures/Cargo.toml --package nervix-lint-fixtures --check
    cargo +nightly-2026-09-17 fmt --manifest-path tools/nervix-lint/fixture-macros/Cargo.toml --check

# Tooling packages use the same bounded scheduler and command, with their pinned compiler.
lint-typed-ratchet jobs=default_jobs: (run-with-jobs "typed-ratchet-clippy-targets" jobs)

[private]
typed-ratchet-clippy-targets: typed-ratchet-clippy-packages

# The compiler tooling is a workspace of its own on the analysis nightly, so it is a lane of its own.
[private]
typed-ratchet-clippy-packages: *(clippy-target "typed-ratchet" *["nervix-lint-driver", "nervix-lint-report", "nervix-ratchet"] ["--manifest-path", "tools/nervix-lint/Cargo.toml", "--all-targets"] "nightly-2026-09-17")

coverage-typed-ratchet-python: typed-ratchet-source-build install-cargo-crap test-typed-ratchet-compiler test-typed-ratchet-modeled
    #!/usr/bin/env bash
    set -euo pipefail
    coverage=(uvx --from coverage==7.11.0 coverage)
    export COVERAGE_FILE="{{ cargo_target_dir }}/typed-ratchet/python.coverage"
    "${coverage[@]}" run --branch --source=scripts.rust_source,scripts.check_typed_errors,scripts.check_clock_boundaries,scripts.native_coverage,scripts.bolero,scripts.bolero_rustc,scripts.bolero_coverage,scripts.build_web_console,scripts.tests.test_bolero,scripts.tests.test_bolero_coverage -m unittest scripts.tests.test_validation_entrypoint scripts.tests.test_check_typed_errors scripts.tests.test_clock_boundaries scripts.tests.test_native_coverage scripts.tests.test_bolero scripts.tests.test_bolero_coverage
    "${coverage[@]}" lcov -o "{{ cargo_target_dir }}/typed-ratchet/python.lcov"
    "${coverage[@]}" report

# Format and validate, with four concurrent recipe bodies by default.
validate jobs=default_jobs: (run-with-jobs "validate-targets" jobs)

[private]
validate-targets: fmt lint-targets validate-skill validate-client-suites validate-plexus-docs validate-connector-capabilities validate-connector-matrix validate-clock-boundaries validate-typed-errors validate-primitive-boundary validate-execution-mode-dependencies validate-execution-mode-conflicts validate-dns-dependencies ratchet validate-deloxide-applicability test-go-ci-tool

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

# Check formatting and validate with the same concurrency default as local validation. The checks run
# beside each other, each Cargo build in a target directory of its own, so a check never waits for
# another's directory lock; `jobs` bounds how many recipes run at once. The ratchet's tools build
# beside them, and the ratchet runs after them, before `validate-deloxide-applicability` reads its
# gate: its analyses admit themselves against the host's memory as if nothing else ran, and beside
# the lints the kernel killed its server analysis for memory (Check 41066649586).
#
# `shard`, `1/2` or `2/2`, runs one of the two parts the checks job's matrix divides them into, on a
# runner of its own; without it every check runs. The first part checks formatting, the ordinary,
# Turmoil and Deloxide lints and the source, dependency and documentation validations. The second
# lints the Shuttle and Loom modes and the compiler tooling, builds the ratchet's tools and runs the
# ratchet once its own lints have finished. Together the two parts are every check `validate` runs,
# which scripts/tests/test_validation_entrypoint.py holds the checks job's matrix to.
validate-ci jobs=default_jobs shard="": (run-with-jobs validate_ci_recipe(shard) jobs)

validate_ci_recipe(shard) := if shard == "" { "validate-ci-targets" } else if shard == "1/2" { "validate-ci-shard-1" } else if shard == "2/2" { "validate-ci-shard-2" } else { error("validate-ci takes shard 1/2 or 2/2, or none for every check, not " + shard) }

[private]
validate-ci-targets: validate-ci-checks ratchet validate-deloxide-applicability

[private, parallel]
validate-ci-checks: validate-ci-shard-1-checks validate-ci-shard-2-checks

[private]
validate-ci-shard-1: validate-ci-shard-1-checks

[private]
validate-ci-shard-2: validate-ci-shard-2-checks ratchet validate-deloxide-applicability

[private, parallel]
validate-ci-shard-1-checks: fmt-check validate-ci-shard-1-lints validate-skill validate-client-suites validate-plexus-docs validate-connector-capabilities validate-connector-matrix validate-clock-boundaries validate-typed-errors validate-primitive-boundary validate-execution-mode-dependencies validate-execution-mode-conflicts validate-dns-dependencies test-go-ci-tool

[private, parallel]
validate-ci-shard-2-checks: validate-ci-shard-2-lints typed-ratchet-build typed-ratchet-source-build install-cargo-crap

# Each part's lints build the browser console first, which the server and its clients embed.
[private]
validate-ci-shard-1-lints: build-web-console validate-ci-shard-1-lanes

[private, parallel]
validate-ci-shard-1-lanes: ordinary-clippy-targets turmoil-clippy-targets deloxide-clippy-targets

[private]
validate-ci-shard-2-lints: build-web-console validate-ci-shard-2-lanes

[private, parallel]
validate-ci-shard-2-lanes: shuttle-clippy-targets loom-clippy-targets typed-ratchet-clippy-targets

# Hold every governed primitive to nervix-primitives and every mode feature to its owner. The check
# rejects a direct, renamed, grouped, qualified, glob, alias or macro path to another backend's
# primitives, shared ownership and the `futures` crates' synchronization included; a manifest that
# renames a governed crate; a mode selected by a bare `cfg` or a global `--cfg`; the analysis cfg
# selecting code or set by a build; a selected atomic held by a static, the owner's own included,
# or constructed in a const context; a family Loom does not model in Loom model code; an unmodeled
# primitive without its permission; a stale or misplaced permission; a `loom` dependency outside
# its owner and harness; and a mode feature that is not forwarded. Guest code compiled into user WASM guests is outside
# the source rules, and so is what a build wrote into a Cargo build directory. The check's own
# tests run first, so a rule that stopped rejecting its bypass fails here too.
validate-primitive-boundary:
    python3 -m unittest --quiet scripts.tests.test_check_primitive_boundary
    python3 -m scripts.check_primitive_boundary

# Measure the primitive source validator and its fixtures, including exact thread owner declarations.
coverage-primitive-boundary:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ cargo_target_dir }}/primitive-boundary"
    coverage=(uvx --from coverage==7.11.0 coverage)
    export COVERAGE_FILE="{{ cargo_target_dir }}/primitive-boundary/python.coverage"
    "${coverage[@]}" run --branch --source=scripts.check_primitive_boundary,scripts.rust_source,scripts.tests.test_check_primitive_boundary -m unittest scripts.tests.test_check_primitive_boundary
    "${coverage[@]}" run --branch -a --source=scripts.check_primitive_boundary,scripts.rust_source -m scripts.check_primitive_boundary
    "${coverage[@]}" lcov -o "{{ cargo_target_dir }}/primitive-boundary/python.lcov"
    "${coverage[@]}" json -o "{{ cargo_target_dir }}/primitive-boundary/python.json"
    "${coverage[@]}" report

# Keep every model checker, simulator and modeled wrapper out of every ordinary dependency graph, and
# the portable graphs portable. The workspace and every package built on its own, the way a consumer
# builds it, with default features and without them, contain no Loom, Shuttle, Shuttle wrapper or
# Turmoil, and enable no execution mode or paused-clock capability of the primitive boundary. The
# vocabulary, and the browser console and wire crate for the browser's target, contain no async
# runtime or network library and never the boundary's `native` capability. The check's own tests run
# first.
validate-execution-mode-dependencies:
    python3 -m unittest --quiet scripts.tests.test_check_mode_dependencies
    python3 -m scripts.check_mode_dependencies

# Keep one precise diagnostic when an execution mode is selected where it cannot run.
# nervix-primitives owns the rejection, so every pair of `loom`, `shuttle`, `turmoil` and
# `deloxide`, and all four, fail there with the modes named, including when separate dependencies
# enable them. A mode or the `native` capability requested for the browser's target fails with the
# boundary's own diagnostic as the first error, before any dependency that cannot build there, and
# so does the diagnostic mode requested without the `native` capability it tracks. A product
# binary built with a modeled mode fails where it declares itself one, naming the binary and the
# mode, and one built with the diagnostic mode fails there unless it declares a diagnostic form.
validate-execution-mode-conflicts:
    #!/usr/bin/env bash
    set -euo pipefail
    # The diagnostics are read as text, so Cargo must not color them even where the caller asks.
    export CARGO_TERM_COLOR=never
    # Beside the other validation builds, in a directory whose lock no other check takes.
    export CARGO_TARGET_DIR={{ quote(cargo_target_dir / "validate" / "mode-conflicts") }}
    diagnostics="$(mktemp)"
    trap 'rm -f "${diagnostics}"' EXIT
    expect_failure() {
        local description="$1"
        shift
        if "$@" >"${diagnostics}" 2>&1; then
            echo "${description}: the build unexpectedly compiled" >&2
            exit 1
        fi
    }
    expect_first_error() {
        local description="$1" message="$2"
        local first
        first="$(grep -m 1 '^error' "${diagnostics}" || true)"
        if [[ "${first}" != *"${message}"* ]]; then
            cat "${diagnostics}" >&2
            echo "${description}: the first error is not \`${message}\`" >&2
            exit 1
        fi
    }
    expect_conflict() {
        local package="$1" features="$2"
        shift 2
        expect_failure "${package} with ${features}" \
            cargo check --package "${package}" --features "${features}" --lib
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
    expect_conflict nervix-primitives 'loom deloxide native' '`loom` and `deloxide`'
    expect_conflict nervix-primitives 'shuttle deloxide native' '`shuttle` and `deloxide`'
    expect_conflict nervix-primitives 'turmoil deloxide native' '`turmoil` and `deloxide`'
    expect_conflict nervix-primitives 'loom shuttle turmoil deloxide native' \
        '`loom` and `shuttle`' '`loom` and `turmoil`' '`shuttle` and `turmoil`' \
        '`loom` and `deloxide`' '`shuttle` and `deloxide`' '`turmoil` and `deloxide`'
    # Two packages each select one mode; Cargo unifies both onto the owner.
    expect_conflict nervix-interconnect 'shuttle nervix-execution/turmoil' \
        '`shuttle` and `turmoil`'
    expect_conflict nervix-execution 'loom nervix-primitives/shuttle' '`loom` and `shuttle`'
    expect_conflict nervix-deadlock 'deloxide nervix-primitives/shuttle' '`shuttle` and `deloxide`'
    # Stress disturbs only the active-only selection.
    expect_failure "nervix-primitives with deloxide-order and deloxide-stress" \
        cargo check --package nervix-primitives --features 'deloxide-order deloxide-stress native' --lib
    expect_first_error "nervix-primitives with deloxide-order and deloxide-stress" \
        'the `deloxide-order` and `deloxide-stress` diagnostic selections cannot be enabled together'
    expect_failure "nervix-primitives with deloxide alone" \
        cargo check --package nervix-primitives --features deloxide --lib
    expect_first_error "nervix-primitives with deloxide alone" \
        'the `deloxide` diagnostic mode tracks the thread-blocking locks of the `native` capability'
    for mode in loom shuttle turmoil deloxide; do
        expect_failure "nervix-primitives with ${mode} for the browser" \
            cargo check --package nervix-primitives --features "${mode}" --lib \
                --target wasm32-unknown-unknown
        expect_first_error "nervix-primitives with ${mode} for the browser" \
            'execution modes run on native targets only'
    done
    expect_failure "nervix-primitives with native for the browser" \
        cargo check --package nervix-primitives --features native --lib \
            --target wasm32-unknown-unknown
    expect_first_error "nervix-primitives with native for the browser" \
        'the `native` capability provides operating-system threads'
    for mode in loom shuttle turmoil; do
        expect_failure "nervix-plexus-format with ${mode}" \
            cargo check --package nervix-plexus-format --bin nervix-plexus-format \
                --features "nervix-primitives/${mode}"
        expect_first_error "nervix-plexus-format with ${mode}" \
            "nervix-plexus-format is a product binary and builds only for ordinary execution; a build that selects the \`${mode}\` execution mode is a test artifact"
    done
    expect_failure "nervix-plexus-format with deloxide" \
        cargo check --package nervix-plexus-format --bin nervix-plexus-format \
            --features 'nervix-primitives/deloxide nervix-primitives/native'
    expect_first_error "nervix-plexus-format with deloxide" \
        "nervix-plexus-format is a product binary without a diagnostic form; a build that selects the \`deloxide\` diagnostic mode builds only a binary that declares one"
    echo "every mode conflict, browser-target request, diagnostic mode without its capability and product binary without its form fails with its diagnostic"

validate-clock-boundaries:
    python3 -m scripts.check_clock_boundaries

# Reject `Result<_, String>` in product code. A typed error is a rule, not a count, so there is no
# baseline to raise: any occurrence fails and names the rule.
validate-typed-errors:
    python3 -m scripts.check_typed_errors

toolchains-install:
    rustup toolchain install
    rustup toolchain install nightly-2026-09-17 --profile minimal --component rustc-dev --component rust-src --component rustfmt --component clippy --component llvm-tools --target wasm32-unknown-unknown

# Run every test target of the Plexus language and its formatter: unit, integration, completion-walk
# unit and documentation tests. The walk itself is a separate gate, `plexus-completion-walk`.
test-plexus *args:
    cargo test --package nervix-plexus --package nervix-plexus-format --all-targets -- {{ args }}

# Verify the backup and restore completion clauses and their public suggestion ordering.
test-plexus-backup-completion:
    cargo test --package nervix-plexus --lib backup::tests::completion_offers_each_

# Parse every runnable Plexus block in the documentation directly through the parser crate. Syntax
# synopses and statement fragments remain Plexus-labelled but opt out explicitly with `plexus,ignore`.
validate-plexus-docs:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir / "validate" / "plexus-docs") }} cargo test -p nervix-plexus --test documentation -- --nocapture

# Hold tests/connector-capability-inventory.toml to the connector entities' declarations and the
# scenario suite: every capability an entity declares names a scenario that observes it at run
# time, every entry names a declared capability once, and every scenario it names exists.
validate-connector-capabilities:
    CARGO_TARGET_DIR={{ quote(cargo_target_dir / "validate" / "connector-capabilities") }} cargo test -p nervix-models --test capability_inventory

# Read the vocabulary directly; no Rust-source or manual-table parser supplies capability facts.
connector-declarations:
    python3 scripts/connector_matrix.py --declarations {{ quote(cargo_target_dir + "/connector-matrix/declarations.json") }} --export-only

# Regenerate the public Plexus capability matrix and checked manual regions.
connector-matrix: connector-declarations
    python3 scripts/connector_matrix.py --declarations {{ quote(cargo_target_dir + "/connector-matrix/declarations.json") }}

# A declaration or manual edit without regeneration fails this check.
validate-connector-matrix: test-connector-matrix validate-connector-matrix-render

# This whole compiled export and comparison is the native coverage execution, after its unit tests.
[private]
validate-connector-matrix-render:
    python3 scripts/connector_matrix.py --declarations {{ quote(cargo_target_dir + "/connector-matrix/declarations.json") }} --export --check

test-connector-matrix:
    python3 -m unittest scripts.tests.test_connector_matrix

# Ordinary Python coverage for the renderer, its current compiled input and its drift checks.
coverage-connector-matrix: connector-declarations
    #!/usr/bin/env bash
    set -euo pipefail
    coverage=(uvx --from coverage==7.11.0 coverage)
    export COVERAGE_FILE="{{ cargo_target_dir }}/connector-matrix/python.coverage"
    "${coverage[@]}" run --branch --source=scripts.connector_matrix,scripts.tests.test_connector_matrix -m unittest scripts.tests.test_connector_matrix
    "${coverage[@]}" run --append --branch --source=scripts.connector_matrix -m scripts.connector_matrix --declarations "{{ cargo_target_dir }}/connector-matrix/declarations.json" --check
    "${coverage[@]}" lcov -o "{{ cargo_target_dir }}/connector-matrix/python.lcov"
    "${coverage[@]}" report

# Check the shared kache remote of CI jobs and image builds: its configuration, the start of a
# job's daemon, and the bounded publication of what the job built.
test-kache-ci:
    uv run --locked python -m unittest scripts.tests.test_kache_ci scripts.tests.test_runner_diagnostics scripts.tests.test_record_runner_load

# Exercise bounded runner report publication without the remote-cache fixtures.
test-runner-diagnostics:
    uv run --locked python -m unittest scripts.tests.test_runner_diagnostics

# Retain the kache remote contract's own measured line coverage beside the other Python tooling.
coverage-kache-ci:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ cargo_target_dir }}/kache-ci"
    coverage=(uvx --from coverage==7.11.0 coverage)
    "${coverage[@]}" run --data-file "{{ cargo_target_dir }}/kache-ci/python.coverage" --branch --source=scripts.kache_remote,scripts.runner_diagnostics,scripts.record_runner_load,scripts.tests.test_kache_ci,scripts.tests.test_runner_diagnostics,scripts.tests.test_record_runner_load -m unittest scripts.tests.test_kache_ci scripts.tests.test_runner_diagnostics scripts.tests.test_record_runner_load
    "${coverage[@]}" lcov --data-file "{{ cargo_target_dir }}/kache-ci/python.coverage" -o "{{ cargo_target_dir }}/kache-ci/python.lcov"

# Exercise the pinned Go formatter bootstrap and its CI wiring without network access.
test-go-ci-tool:
    python3 -m unittest scripts.tests.test_go_ci_tool

# Run one Python script owner or named test without the full documentation suite.
test-script-unit test:
    uv run --locked python -m unittest {{ quote(test) }}

# Measure the installer's Python lines separately from Rust's ordinary LCOV.
coverage-go-ci-tool:
    mkdir -p target/go-ci-tool
    COVERAGE_FILE=target/go-ci-tool/.coverage uv run --no-project --with coverage==7.11.0 coverage run --branch --source=scripts.install_go_ci_tool -m unittest scripts.tests.test_go_ci_tool
    COVERAGE_FILE=target/go-ci-tool/.coverage uv run --no-project --with coverage==7.11.0 coverage report -m --include=scripts/install_go_ci_tool.py

# Every script test runs here, including the advisory CRAP report and CI-entrypoint checks, so
# this builds the Rust gate and installs its pinned complexity analyzer.
test-docs: typed-ratchet-source-build install-cargo-crap
    uv run --locked python -m unittest discover -s scripts/tests -p "test_*.py"
    node --test cloudflare/docs-worker/src/index.test.js

# Screenshots are recaptured from the current console and the nervix-cli and nervix-server
# reference chapters are rendered from the current binaries, so a published book can never describe
# an older build.
book version="0.1.0-dev": test-docs docs-screenshots
    python3 scripts/build_book.py --version {{ version }}

validate-skill:
    env GH_PROMPT_DISABLED=1 gh skill publish .agents/skills --dry-run

# Hold every host binding to its language-level suite: each public member referenced by a test, as
# mypy types the suite, the inventory to the suite's tests, the typed stub to the binding's source,
# and the guide to the shared section list with every code block an anchored suite test. The same
# mypy run checks the suite and the paced simulation driver with `--strict`.
validate-client-suites:
    uv run --locked python -m scripts.client_suites validate

# Exercise the host suite's bounded action adapter over real pipes, without a cluster.
test-client-suite-actions:
    uv run --locked python -m unittest scripts.tests.test_client_suite_actions

# Collect the line coverage of the client suite check and its tests.
coverage-client-suites-runner:
    mkdir -p target/client-suites
    COVERAGE_FILE=target/client-suites/.coverage uv run --locked --with coverage==7.11.0 coverage run --branch --source=scripts.client_suites,scripts.client_suites_mypy_plugin -m unittest scripts.tests.test_client_suites
    COVERAGE_FILE=target/client-suites/.coverage uv run --locked --with coverage==7.11.0 coverage report -m

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
    python3 scripts/purge_cloudflare_cache.py --zone-id {{ zone_id }}

worker-deploy zone_id="":
    #!/usr/bin/env bash
    set -euo pipefail
    npx --yes wrangler deploy --config cloudflare/docs-worker/wrangler.jsonc
    if [[ -n "{{ zone_id }}" ]]; then
        python3 scripts/purge_cloudflare_cache.py --zone-id {{ zone_id }}
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
# The script replaces the recipe's shell, so the TERM that just passes on when it is cancelled
# reaches the controller, whose exit trap heals, captures evidence and cleans up.
chaos *args:
    exec bash scripts/chaos/chaos.sh {{ args }}

# Prove the network preflight and its retained failure outcomes through the external controller.
test-chaos-network-preflight *cases:
    exec bash scripts/chaos/tests/network-preflight-self-test.sh {{ cases }}

# Retain source coverage of network-preflight failure recording and live fault controls.
coverage-chaos-network-preflight image="":
    #!/usr/bin/env bash
    set -euo pipefail
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    CHAOS_NETWORK_PREFLIGHT_NERVIX_IMAGE={{ quote(image) }} kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/chaos.sh") }},{{ quote(invocation_directory() / "scripts/chaos/network-faults.sh") }},{{ quote(invocation_directory() / "scripts/chaos/run-baseline.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/network-preflight-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-network-preflight-coverage") }} scripts/chaos/chaos.sh self-test

# Exercise the actual rate-probe owner with delayed connects, partial bytes and unreachable peers.
test-chaos-rate-probe:
    exec bash scripts/chaos/tests/rate-probe-self-test.sh

# Judge mixed-instability holds over a lossy link from retained public statuses without a product image.
test-chaos-hold-quorum:
    exec bash scripts/chaos/tests/hold-quorum-self-test.sh

# Judge a failover-length pause's emitter progress and source consumer-group handoff from retained
# pause timelines without a product image.
test-chaos-pause-delivery:
    exec bash scripts/chaos/tests/pause-delivery-self-test.sh

# Retain shell coverage of the failover-length pause's emitter progress and consumer-group handoff controls.
coverage-chaos-pause-delivery:
    #!/usr/bin/env bash
    set -euo pipefail
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/pause-resume-scenario.sh") }},{{ quote(invocation_directory() / "scripts/chaos/consumer-group.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/pause-delivery-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-pause-delivery-coverage") }} scripts/chaos/tests/pause-delivery-self-test.sh

# Exercise bounded packet sampling and the directed-link effect limits without a product image.
test-chaos-degraded-evidence:
    exec bash scripts/chaos/tests/degraded-self-test.sh

# Pin suite preparation and baseline execution accounting and the production output-boundary
# observations.
test-chaos-output-boundary selection="all":
    exec bash scripts/chaos/tests/output-boundary-self-test.sh {{ quote(selection) }}

# Check when a run adopts the verifier self-check its suite executed instead of executing its own.
test-chaos-verifier-self-check:
    exec bash scripts/chaos/tests/verifier-self-check-self-test.sh

# Preserve the package probe's native refusal and exact retained container lifetime.
test-chaos-image-package:
    exec bash scripts/chaos/tests/image-package-self-test.sh

# Publish the checkout's identity on both local image overlays, with dirty source distinguished.
test-chaos-local-image:
    exec bash scripts/chaos/tests/local-image-self-test.sh

# Bound startup artifact publication by the controller's actual remaining budget.
test-chaos-startup-status:
    exec bash scripts/chaos/tests/startup-status-self-test.sh

# Verify fresh public requests in run-owned observation helpers, including input and failures.
test-chaos-observation-helpers:
    exec bash scripts/chaos/tests/observation-helpers-self-test.sh

# Derive the backup run's graph from the checked-in baseline fixture, as every verifier self-check does.
test-chaos-backup-fixture:
    exec bash scripts/chaos/tests/backup-fixture-self-test.sh

# Check the backup fault and fresh-restore options and every outcome the backup fault verdict
# assigns, as every verifier self-check does.
test-chaos-backup-fault:
    exec bash scripts/chaos/tests/backup-fault-self-test.sh

# Measure the backup fault verdict's outcome checks and the backup scenarios' option checks.
coverage-chaos-backup-fault:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v kcov >/dev/null 2>&1
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/backup-fault-scenario.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/backup-fault-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-backup-fault-coverage") }} scripts/chaos/tests/backup-fault-self-test.sh

# Check the restore scenarios' options, the padded guest's bounds, the sampler's records, the
# staging verdicts and every outcome the restore verdict assigns, as every verifier self-check does.
test-chaos-restore:
    exec bash scripts/chaos/tests/restore-self-test.sh

# Measure the restore scenarios' option checks, staging verdicts and outcome verdict.
coverage-chaos-restore:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v kcov >/dev/null 2>&1
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/restore-scenario.sh") }},{{ quote(invocation_directory() / "scripts/chaos/restore-fault-scenario.sh") }},{{ quote(invocation_directory() / "scripts/chaos/fixtures/wasm/padded-state-wat.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/restore-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-restore-coverage") }} scripts/chaos/tests/restore-self-test.sh

# Check the deduplicator and window outputs the backup state verdict accepts and each lost or
# doubled state it refuses, as every verifier self-check does.
test-chaos-backup-state:
    exec bash scripts/chaos/tests/backup-state-self-test.sh

# Measure the backup state verdict's acceptances and refusals.
coverage-chaos-backup-state:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v kcov >/dev/null 2>&1
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/verify-backup-state.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/backup-state-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-backup-state-coverage") }} scripts/chaos/tests/backup-state-self-test.sh

# Measure the backup graph substitution and its refusals of a graph without exactly one clause.
coverage-chaos-backup-fixture:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v kcov >/dev/null 2>&1
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/backup-scenario.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/backup-fixture-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-backup-fixture-coverage") }} scripts/chaos/tests/backup-fixture-self-test.sh

# Measure successful publication and decisive native/deadline/reporting refusals.
coverage-chaos-startup-status:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v kcov >/dev/null 2>&1
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/startup-status.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/startup-status-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-startup-status-coverage") }} scripts/chaos/tests/startup-status-self-test.sh

# Requires kcov; retains coverage of the output-boundary owner and its focused controls.
coverage-chaos-output-boundary:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v kcov >/dev/null 2>&1
    # kcov's Bash descriptor exchange cannot handle an inherited limit near INT_MAX.
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/output-boundary.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/output-boundary-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-output-boundary-coverage") }} scripts/chaos/tests/output-boundary-self-test.sh

# Measure package-command refusals, retained state and successful probes without Docker.
coverage-chaos-image-package:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v kcov >/dev/null 2>&1
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/image-package-check.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/image-package-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-image-package-coverage") }} scripts/chaos/tests/image-package-self-test.sh

# Measure the full baseline controller against a supplied immutable image, including its self-check.
coverage-chaos-baseline image nodes="3" timeout="840" run_id="baseline-coverage":
    #!/usr/bin/env bash
    set -euo pipefail
    command -v kcov >/dev/null 2>&1
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/run-baseline.sh") }},{{ quote(invocation_directory() / "scripts/chaos/output-boundary.sh") }},{{ quote(invocation_directory() / "scripts/chaos/image-package-check.sh") }},{{ quote(invocation_directory() / "scripts/chaos/startup-status.sh") }},{{ quote(invocation_directory() / "scripts/chaos/backup-scenario.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/self-test.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/output-boundary-self-test.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/image-package-self-test.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/startup-status-self-test.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/backup-fixture-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-baseline-coverage") }} scripts/chaos/run-baseline.sh --scenario baseline --image {{ quote(image) }} --nodes {{ quote(nodes) }} --timeout {{ quote(timeout) }} --artifacts {{ quote(cargo_target_dir / "chaos-baseline-coverage-runs") }} --run-id {{ quote(run_id) }}

# Retain shell coverage of packet sampling, evidence parsing and effect controls.
coverage-chaos-degraded-evidence:
    #!/usr/bin/env bash
    set -euo pipefail
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/link-degradation.sh") }},{{ quote(invocation_directory() / "scripts/chaos/verify-degraded-evidence.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/degraded-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-degraded-evidence-coverage") }} scripts/chaos/tests/degraded-self-test.sh

# Judge retained public clock transcripts through the external verifier.
verify-chaos-clock *args:
    exec bash scripts/chaos/verify-clock-evidence.sh {{ args }}

# Run the state, clock and window verifier controls without the other external harness checks.
test-chaos-stateful-verifiers:
    exec bash scripts/chaos/tests/stateful-self-test.sh

# Validate suite selection, budgets and the controller against its deterministic stand-ins.
test-chaos-suites:
    exec bash scripts/chaos/tests/suite-self-test.sh

# Run the endpoint route and WebSocket verifier controls without the other external harness checks.
test-chaos-endpoint-verifiers:
    exec bash scripts/chaos/tests/endpoint-self-test.sh

# Retain shell coverage of the endpoint route and WebSocket verifier controls.
coverage-chaos-endpoint-verifiers:
    #!/usr/bin/env bash
    set -euo pipefail
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/verify-endpoint-evidence.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/endpoint-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-endpoint-verifier-coverage") }} scripts/chaos/tests/endpoint-self-test.sh

# Run the materialized, generator, window, rotation, expiry, missing-topic and force-flush verifier
# controls without the other external harness checks.
test-chaos-lifetimes-verifiers:
    exec bash scripts/chaos/tests/lifetimes-self-test.sh

# Retain shell coverage of the lifetimes verifier controls.
coverage-chaos-lifetimes-verifiers:
    #!/usr/bin/env bash
    set -euo pipefail
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/verify-lifetimes-evidence.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/lifetimes-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-lifetimes-verifier-coverage") }} scripts/chaos/tests/lifetimes-self-test.sh

# Retain shell coverage of the state, clock and window verifier controls.
coverage-chaos-stateful-verifiers:
    #!/usr/bin/env bash
    set -euo pipefail
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/verify-clock-evidence.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/stateful-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-stateful-verifier-coverage") }} scripts/chaos/tests/stateful-self-test.sh

# Requires kcov; retains shell Cobertura coverage for the probe and its regression controls.
coverage-chaos-rate-probe:
    #!/usr/bin/env bash
    set -euo pipefail
    # kcov's Bash descriptor exchange cannot handle an inherited limit near INT_MAX.
    descriptor_limit="$(ulimit -n)"
    if [[ "${descriptor_limit}" == unlimited ]] || ((descriptor_limit > 65536)); then
        ulimit -n 65536
    fi
    kcov --clean --include-path={{ quote(invocation_directory() / "scripts/chaos/link-degradation.sh") }},{{ quote(invocation_directory() / "scripts/chaos/tests/rate-probe-self-test.sh") }} {{ quote(cargo_target_dir / "chaos-rate-probe-coverage") }} scripts/chaos/tests/rate-probe-self-test.sh

# Regenerate the prebuilt WASM guest the stateful chaos scenarios upload, after a guest ABI change.
# The chaos runner itself never builds guest code; `cargo test --package nervix-wasm` checks that
# the checked-in module still equals what this writes.
chaos-wasm-fixture:
    cargo test --package nervix-wasm --test chaos_branch_counter -- --ignored write_the_branch_counter_module

server *args: build-deps generate-dev-tls
    NERVIX_NODE_ID="${NERVIX_NODE_ID:-node-1}" \
    NERVIX_INTERCONNECT_TLS_CA="${NERVIX_INTERCONNECT_TLS_CA:-tls/dev/ca.pem}" \
    NERVIX_INTERCONNECT_TLS_CERT="${NERVIX_INTERCONNECT_TLS_CERT:-tls/dev/node.pem}" \
    NERVIX_INTERCONNECT_TLS_KEY="${NERVIX_INTERCONNECT_TLS_KEY:-tls/dev/node-key.pem}" \
    cargo run --package nervix-server --bin nervix-server -- {{ args }}

client *args: build-deps
    cargo run --package nervix-cli -- {{ args }}

build-web-console:
    python3 -m scripts.build_web_console

# Keep the console build's wasm-bindgen CLI selection, installation and verification counters
# beside the other tooling reports.
coverage-wasm-bindgen:
    #!/usr/bin/env bash
    set -euo pipefail
    coverage=(uvx --from coverage==7.11.0 coverage)
    mkdir -p "{{ cargo_target_dir }}/wasm-bindgen-coverage"
    "${coverage[@]}" run --data-file "{{ cargo_target_dir }}/wasm-bindgen-coverage/python.coverage" --branch --source=scripts.wasm_bindgen,scripts.tests.test_wasm_bindgen -m unittest scripts.tests.test_wasm_bindgen
    "${coverage[@]}" lcov --data-file "{{ cargo_target_dir }}/wasm-bindgen-coverage/python.coverage" -o "{{ cargo_target_dir }}/wasm-bindgen-coverage/python.lcov"

build-server:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/server cargo build {{ release_flag }} --package nervix-server --bin nervix-server

# Reuse an already available packaged image's runtime libraries for a local Chaos candidate.
# The two binaries are built from this checkout in the ordinary target directory, then stripped
# into a small, temporary Docker context so the source tree is never sent to the builder.
build-chaos-local-image tag="nervix:backup-local" base="nervix:chaos-current":
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --package nervix-server --bin nervix-server --package nervix-cli --bin nervix-cli
    revision="$(git rev-parse HEAD)"
    checkout_status="$(git status --porcelain --untracked-files=no)"
    if [[ -n "${checkout_status}" ]]; then
        revision="${revision}-dirty"
    fi
    stage="$(mktemp -d {{ quote(cargo_target_dir + "/backup-chaos-image.XXXXXX") }})"
    trap 'rm -rf "${stage}"' EXIT
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-server") }} "${stage}/nervix-server"
    install -m 755 {{ quote(cargo_target_dir + "/debug/nervix-cli") }} "${stage}/nervix-cli"
    llvm-strip-23 "${stage}/nervix-server" "${stage}/nervix-cli"
    cat >"${stage}/Dockerfile" <<'EOF'
    ARG BASE
    FROM ${BASE}
    COPY nervix-server /usr/local/bin/nervix-server
    COPY nervix-cli /usr/local/bin/nervix-cli
    EOF
    docker build --build-arg BASE={{ quote(base) }} \
        --label "org.opencontainers.image.revision=${revision}" \
        --tag {{ quote(tag) }} "${stage}"

# Overlay a diagnostic server of one Deloxide selection, built in release mode from this checkout,
# with this checkout's CLI and the evidence report tool, on an already available packaged image, for
# a local Chaos run of a diagnostic image. Its labels name the selection and this checkout's revision,
# as `docker-build-diagnostic` does for the image CI runs.
build-chaos-diagnostic-image tag="nervix:diagnostic-local" base="ghcr.io/nervix-io/nervix:debian-latest" selection="deloxide-order": build-web-console
    #!/usr/bin/env bash
    set -euo pipefail
    case {{ quote(selection) }} in
        deloxide | deloxide-order | deloxide-stress) ;;
        *) echo "unknown diagnostic selection: {{ selection }}" >&2; exit 2 ;;
    esac
    CARGO_TARGET_DIR={{ quote(cargo_target_dir + "/deloxide") }} cargo build --release \
        --package nervix-server --bin nervix-server --features {{ quote(selection) }}
    cargo build --release --package nervix-cli --bin nervix-cli
    cargo build --release --package nervix-deadlock --features report-tool --bin nervix-deadlock-report
    revision="$(git rev-parse HEAD)"
    checkout_status="$(git status --porcelain --untracked-files=no)"
    if [[ -n "${checkout_status}" ]]; then
        revision="${revision}-dirty"
    fi
    stage="$(mktemp -d {{ quote(cargo_target_dir + "/diagnostic-chaos-image.XXXXXX") }})"
    trap 'rm -rf "${stage}"' EXIT
    install -m 755 {{ quote(cargo_target_dir + "/deloxide/release/nervix-server") }} "${stage}/nervix-server"
    install -m 755 {{ quote(cargo_target_dir + "/release/nervix-cli") }} "${stage}/nervix-cli"
    install -m 755 {{ quote(cargo_target_dir + "/release/nervix-deadlock-report") }} "${stage}/nervix-deadlock-report"
    llvm-strip-23 "${stage}/nervix-server" "${stage}/nervix-cli" "${stage}/nervix-deadlock-report"
    cat >"${stage}/Dockerfile" <<'EOF'
    ARG BASE
    FROM ${BASE}
    COPY nervix-server nervix-cli nervix-deadlock-report /usr/local/bin/
    EOF
    docker build --build-arg BASE={{ quote(base) }} \
        --label io.nervix.diagnostic.selection={{ quote(selection) }} \
        --label "org.opencontainers.image.revision=${revision}" \
        --tag {{ quote(tag) }} "${stage}"

# Build a diagnostic node: nervix-server in the `deloxide` mode, whose tracked locks report an active
# deadlock with evidence and end the process. Its own target directory keeps it from replacing the
# ordinary binary; it is a diagnostic artifact, never a release product.
build-diagnostic-server selection="deloxide":
    CARGO_TARGET_DIR={{ cargo_target_dir }}/deloxide cargo build {{ release_flag }} --package nervix-server --bin nervix-server --features {{ quote(selection) }}

build-cli:
    CARGO_TARGET_DIR={{ cargo_target_dir }}/cli cargo build {{ release_flag }} --package nervix-cli --bin nervix-cli

[parallel]
build-apps: build-cli build-server

build-all: generate-dev-tls build-deps build-apps

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

generate-test-onnx output="tests/fixtures/onnx/simple_score.onnx" alternate_output="tests/fixtures/onnx/alternate_score.onnx" batch_output="tests/fixtures/onnx/batch_score.onnx" f64_output="tests/fixtures/onnx/f64_score.onnx" matrix_output="tests/fixtures/onnx/matrix_identity.onnx" dynamic_batch_output="tests/fixtures/onnx/dynamic_batch_score.onnx" scalar_output="tests/fixtures/onnx/scalar_identity.onnx" running_sum_output="tests/fixtures/onnx/running_sum.onnx" checked_running_sum_output="tests/fixtures/onnx/checked_running_sum.onnx":
    python3 scripts/train_simple_onnx.py --output {{ output }} --alternate-output {{ alternate_output }} --batch-output {{ batch_output }} --f64-output {{ f64_output }} --matrix-output {{ matrix_output }} --dynamic-batch-output {{ dynamic_batch_output }} --scalar-output {{ scalar_output }} --running-sum-output {{ running_sum_output }} --checked-running-sum-output {{ checked_running_sum_output }}

# Download a CPU or CUDA 13 runtime; `host` selects the current machine's platform.
download-onnxruntime flavor="cpu" platform="host":
    bash scripts/download_onnxruntime.sh --flavor {{ quote(flavor) }} --platform {{ quote(platform) }}

# Check runtime package selection and the CPU/CUDA image build targets.
test-onnxruntime:
    uv run --locked python -m unittest scripts.tests.test_onnxruntime scripts.tests.test_train_simple_onnx

# Check the caller-selected destinations of every generated ONNX model.
test-onnx-fixtures:
    python3 -m unittest scripts.tests.test_train_simple_onnx

# Build both final targets with fixture binaries and check layer sharing and runtime dependencies.
test-onnxruntime-docker:
    #!/usr/bin/env bash
    set -euo pipefail
    just docker-prepare-qemu linux/amd64
    mkdir -p target
    fixture_context="$(mktemp -d "${PWD}/target/onnxruntime-docker.XXXXXX")"
    trap 'rm -rf "${fixture_context}"' EXIT
    mkdir -p "${fixture_context}/artifacts/nervix-server.bundle"
    for binary in nervix-server nervix-cli nervix-plexus-format; do
        cat > "${fixture_context}/artifacts/${binary}" <<'SH'
    #!/bin/sh
    basename "$0"
    SH
        chmod +x "${fixture_context}/artifacts/${binary}"
    done
    printf 'Sonic bundle fixture\n' > "${fixture_context}/artifacts/nervix-server.bundle/manifest"
    for variant in cpu cuda; do
        base=debian-base
        if [[ "${variant}" == cuda ]]; then
            base=cuda-base
        fi
        docker buildx build -f Dockerfile.debian --target "${base}" --platform linux/amd64 \
            --load --tag "nervix:onnxruntime-${variant}-base-check" .
        image="nervix:onnxruntime-${variant}-check"
        docker buildx build -f Dockerfile.debian --target "${variant}" --platform linux/amd64 \
            --build-context "builder=${fixture_context}" --load --tag "${image}" .
        test "$(docker run --rm "${image}")" = nervix-server
        docker run --rm --entrypoint /bin/sh "${image}" -c '
            set -eu
            if test -f /usr/local/lib/libonnxruntime_providers_cuda.so; then
                ldconfig -p | grep -F libcudnn.so.9
            fi
            for library in /usr/local/lib/libonnxruntime*.so /usr/lib/x86_64-linux-gnu/libcudnn*.so.9; do
                test -f "${library}" || continue
                dependencies="$(ldd "${library}")"
                printf "%s\n%s\n" "${library}" "${dependencies}"
                # The NVIDIA container runtime injects the host driver when --gpus is used.
                missing="$(printf "%s\n" "${dependencies}" | awk '\''/not found/ && $1 != "libcuda.so.1"'\'')"
                test -z "${missing}"
            done
        '
    done
    uv run --locked python - <<'PY'
    import json
    import subprocess

    def layers(image):
        result = subprocess.check_output(["docker", "image", "inspect", image], text=True)
        return json.loads(result)[0]["RootFS"]["Layers"]

    def filesystem_history(image):
        result = subprocess.check_output([
            "docker", "image", "history", "--no-trunc", "--human=false", "--format", "json", image,
        ], text=True)
        rows = [json.loads(line) for line in result.splitlines()]
        return [row["CreatedBy"] for row in reversed(rows) if int(row["Size"]) > 0]

    debian = layers("nervix:onnxruntime-cpu-base-check")
    cuda = layers("nervix:onnxruntime-cuda-base-check")
    if cuda[:len(debian)] != debian or len(cuda) != len(debian) + 1:
        raise SystemExit("CUDA must add exactly one filesystem layer directly above Debian")
    for variant, base in (("cpu", debian), ("cuda", cuda)):
        image = f"nervix:onnxruntime-{variant}-check"
        runtime = layers(image)
        if runtime[:len(base)] != base or len(runtime) <= len(base):
            raise SystemExit(f"{variant}: Nervix runtime layers must follow the selected base")
        first_runtime_layer = " ".join(filesystem_history(image)[len(base)].split())
        command = first_runtime_layer.partition(" /bin/sh -c ")[2]
        if not command.startswith("apt-get update && apt-get upgrade -y"):
            raise SystemExit(f"{variant}: Debian updates must immediately follow the selected base")

    if layers("nervix:onnxruntime-cpu-check")[-4:] != layers("nervix:onnxruntime-cuda-check")[-4:]:
        raise SystemExit("CPU and CUDA targets must share the Nervix binary and bundle layers")

    cuda_base = "nervix:onnxruntime-cuda-base-check"
    held = subprocess.check_output([
        "docker", "run", "--rm", "--entrypoint", "apt-mark", cuda_base, "showhold",
    ], text=True).splitlines()
    if not held:
        raise SystemExit("CUDA packages must be held at their pinned versions")

    def cuda_versions(image):
        return subprocess.check_output([
            "docker", "run", "--rm", "--entrypoint", "dpkg-query", image,
            "-W", "-f=${Package}=${Version}\\n", *held,
        ], text=True)
    if cuda_versions(cuda_base) != cuda_versions("nervix:onnxruntime-cuda-check"):
        raise SystemExit("Debian upgrades must preserve the pinned CUDA package versions")
    print("Verified CPU/CUDA final targets, shared Nervix layers, Debian updates, and CUDA pins")
    PY

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
    # Inside a zellij session, `--layout` adds the tabs to that session, whose panes do not see the
    # environment exported above, so the dashboard always starts a session of its own.
    exec zellij --new-session-with-layout .zellij/layouts/local-3-nodes.kdl

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

# Build the CPU image for amd64 or arm64.
docker-build-debian llvm_version="23" tag="nervix:debian" platform="linux/amd64" push="false" cache_from="" cache_to="":
    just docker-build-linux cpu {{ quote(llvm_version) }} \
        {{ quote(tag) }} {{ quote(platform) }} {{ quote(push) }} {{ quote(cache_from) }} {{ quote(cache_to) }} ""

# Build Debian trixie -> CUDA 13/cuDNN 9 -> Nervix, reusing CUDA across application builds.
docker-build-cuda llvm_version="23" tag="nervix:cuda" platform="linux/amd64" push="false" cache_from="" cache_to="":
    just docker-build-linux cuda {{ quote(llvm_version) }} \
        {{ quote(tag) }} {{ quote(platform) }} {{ quote(push) }} {{ quote(cache_from) }} {{ quote(cache_to) }} ""

# Build the Deloxide diagnostic image of the CPU runtime: the diagnostic server of one selection,
# deloxide, deloxide-order or deloxide-stress, with the ordinary CLI and formatter and the evidence
# report tool. Its io.nervix.diagnostic.selection label makes every chaos run judge the deadlock
# evidence of its nodes; it is a test artifact and never a release.
docker-build-diagnostic selection="deloxide-order" llvm_version="23" tag="nervix:diagnostic" platform="linux/amd64" push="false" cache_from="" cache_to="":
    just docker-build-linux diagnostic {{ quote(llvm_version) }} \
        {{ quote(tag) }} {{ quote(platform) }} {{ quote(push) }} {{ quote(cache_from) }} {{ quote(cache_to) }} \
        {{ quote(selection) }}

[private]
docker-build-linux image_target llvm_version tag platform push cache_from cache_to diagnostic_selection:
    #!/usr/bin/env bash
    set -euo pipefail
    case {{ quote(image_target) }}:{{ quote(diagnostic_selection) }} in
        cpu: | cuda: | diagnostic:deloxide | diagnostic:deloxide-order | diagnostic:deloxide-stress) ;;
        *)
            echo "the {{ image_target }} image cannot be built with diagnostic selection '{{ diagnostic_selection }}'" >&2
            exit 1
            ;;
    esac
    # Every image names the revision it was built from, so two images of one revision can be told
    # apart from two of different ones.
    revision="$(git rev-parse HEAD)"
    if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
        revision="${revision}-dirty"
    fi
    normalized_platform={{ quote(platform) }}
    if [[ "${normalized_platform}" == "linux/aarch64" ]]; then
        normalized_platform="linux/arm64"
    fi
    if [[ {{ quote(image_target) }} == "cuda" && "${normalized_platform}" != "linux/amd64" ]]; then
        echo "CUDA images require linux/amd64; ONNX Runtime publishes no GPU archive for ${normalized_platform}" >&2
        exit 1
    fi
    just docker-prepare-qemu "${normalized_platform}"
    output_flag="--load"
    if [[ {{ quote(push) }} == "true" ]]; then
        output_flag="--push"
    fi
    cache_flags=()
    if [[ -n {{ quote(cache_from) }} ]]; then
        cache_flags+=(--cache-from={{ quote(cache_from) }})
    fi
    if [[ -n {{ quote(cache_to) }} ]]; then
        cache_flags+=(--cache-to={{ quote(cache_to) }})
    fi
    : "${KACHE_S3_BUCKET:?KACHE_S3_BUCKET is required}"
    : "${KACHE_S3_REGION:?KACHE_S3_REGION is required}"
    : "${KACHE_S3_ENDPOINT:?KACHE_S3_ENDPOINT is required}"
    : "${KACHE_S3_ACCESS_KEY:?KACHE_S3_ACCESS_KEY is required}"
    : "${KACHE_S3_SECRET_KEY:?KACHE_S3_SECRET_KEY is required}"
    docker buildx build \
        -f Dockerfile.debian \
        --target {{ quote(image_target) }} \
        --progress=plain \
        --platform "${normalized_platform}" \
        --build-arg "KACHE_VERSION=${KACHE_VERSION:-2.0.0}" \
        --build-arg RUST_VERSION={{ rust_toolchain_version }} \
        --build-arg LLVM_VERSION={{ quote(llvm_version) }} \
        --build-arg "KACHE_S3_BUCKET=${KACHE_S3_BUCKET}" \
        --build-arg "KACHE_S3_REGION=${KACHE_S3_REGION}" \
        --build-arg "KACHE_S3_ENDPOINT=${KACHE_S3_ENDPOINT}" \
        --build-arg "KACHE_S3_ACCESS_KEY=${KACHE_S3_ACCESS_KEY}" \
        --build-arg "KACHE_S3_SECRET_KEY=${KACHE_S3_SECRET_KEY}" \
        --build-arg NERVIX_DIAGNOSTIC_SELECTION={{ quote(diagnostic_selection) }} \
        --label "org.opencontainers.image.revision=${revision}" \
        "${cache_flags[@]}" \
        -t {{ quote(tag) }} \
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
bench-task-handles output="target/task-handles.json": build-web-console
    cargo bench --package nervix-server --bench task_handles --features benchmarks -- {{ quote(output) }}

coverage-task-handles output="target/task-handles.lcov" report="target/task-handles-coverage.json":
    cargo llvm-cov --bench task_handles --features benchmarks --no-default-ignore-filename-regex --lcov --output-path {{ quote(output) }} -- {{ quote(report) }}

# Inspect a benchmark harness from an existing successful instrumented run without rebuilding it.
coverage-task-handles-export executable profile output="target/task-handles-benchmark.lcov":
    #!/usr/bin/env bash
    set -euo pipefail
    llvm_bin="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin"
    "${llvm_bin}/llvm-profdata" merge -sparse {{ quote(profile) }} -o target/task-handles-benchmark.profdata
    "${llvm_bin}/llvm-cov" export {{ quote(executable) }} --instr-profile=target/task-handles-benchmark.profdata --format=lcov > {{ quote(output) }}

# Same-host retained relay selection, branch churn and authenticated pool leasing measurements.
bench-retained-channels: build-web-console wasm-processor-guests download-onnxruntime bench-retained-channels-bodies

# Native bodies also exercised by the coverage-producing benchmark smoke check.
bench-retained-channels-bodies:
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib relay_channel_cost -- --ignored --nocapture
    cargo test --package nervix-interconnect --lib established_pool_cost -- --ignored --nocapture

# Delivery correlation and authenticated frame costs, including retained state and reclamation.
bench-remote-owners: build-web-console wasm-processor-guests download-onnxruntime bench-remote-owners-bodies

[private]
bench-remote-owners-bodies:
    ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" cargo test --package nervix-server --features testing --lib remote_ack_owner_cost -- --ignored --nocapture
    cargo test --package nervix-interconnect --lib remote_relay_frame_cost -- --ignored --nocapture

# Native diagnostic owners and probes, composed separately from the ordinary report command.
test-deadlock-evidence-order:
    cargo test --package nervix-deadlock --features deloxide-order --lib
    cargo test --package nervix-primitives --features 'native deloxide-order' --lib
    cargo test --package nervix-deadlock --features deloxide-order --test active_cycles

test-deadlock-report:
    cargo test --package nervix-deadlock --features report-tool --test report_cli

# Collect both focused checks in their own mode's build and retain canonical completion evidence.
# The combined report is labeled diagnostic-evidence, outside ordinary coverage and its CRAP report.
coverage-deadlock output="target/deadlock.lcov":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{ quote(cargo_target_dir + "/diagnostic-evidence") }}
    reports="$(mktemp -d {{ quote(cargo_target_dir + "/diagnostic-evidence/coverage.XXXXXX") }})"
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run test-deadlock-evidence-order --output "${reports}/order.lcov"
    python3 -m scripts.native_coverage --target-dir {{ quote(cargo_target_dir) }} run test-deadlock-report --output "${reports}/report.lcov"
    cat "${reports}/order.lcov" "${reports}/report.lcov" > "${reports}/combined.lcov"
    mkdir -p "$(dirname {{ quote(output) }})"
    mv "${reports}/combined.lcov" {{ quote(output) }}
