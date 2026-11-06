//! Repository test harness, outside the product layer order.
//! Owns: every identity input's invalidation and startup refusal examples.
//! Depends on: production identity capture and private process observations.
//! Must not know: a real compiler or host configuration.

use std::{collections::BTreeMap, fs};

use meticulous::{OptionExt as _, ResultExt as _};

use super::*;
use crate::{
    error::Error,
    runner::Runner,
    test_support::{Harness, executable},
};

fn prepare(fixture: &mut Harness) {
    let root = fixture.directory.path().to_owned();
    let declaration = root.join("tools/nervix-lint/configurations.toml");
    fs::create_dir_all(
        declaration
            .parent()
            .assured("the declaration has its tooling parent"),
    )
    .assured("the declaration directory can be created");
    fs::write(
        declaration,
        r#"[[configuration]]
name='ordinary'
manifest='Cargo.toml'
packages=[]
[admission]
threshold_gib=4
wait_limit_minutes=30
reservation=[]
"#,
    )
    .assured("the current declaration can be written");
    let driver = root.join("target/typed-ratchet/driver/debug/nervix-lint-driver");
    fs::create_dir_all(driver.parent().assured("the driver has its binary parent"))
        .assured("the binary directory can be created");
    fs::write(driver, "driver bytes").assured("the driver digest fixture can be written");
    executable(
        &fixture.path("bin/rustup"),
        r#"#!/usr/bin/env python3
import os, sys
print(os.environ['NERVIX_TEST_ROOT'] + '/target/sysroot' if '--print' in sys.argv else os.environ.get('NERVIX_TEST_COMPILER', 'qualified compiler'))
"#,
    );
    fixture.set(
        "HOME",
        &root.join("target/private-home").display().to_string(),
    );
    fixture.set(
        "CARGO_HOME",
        &root
            .join("target/private-home/.cargo")
            .display()
            .to_string(),
    );
}

fn capture(fixture: &Harness) -> Runner {
    Runner::from_environment(
        fixture.directory.path(),
        &fixture.path(""),
        false,
        fixture.runner.environment.clone(),
    )
    .assured("the isolated current runner inputs are valid")
}

#[test]
fn cargo_build_profiles_are_separate_from_collected_runtime_profiles() {
    let parent_profile = std::env::var_os("LLVM_PROFILE_FILE");
    for collected in [false, true] {
        let mut fixture = Harness::new();
        prepare(&mut fixture);
        fixture.set("LLVM_PROFILE_FILE", "runtime-%p-%m.profraw");
        if collected {
            fixture.set("NERVIX_NATIVE_COVERAGE_ATTEMPT", "current-attempt");
        }
        let captured = capture(&fixture);
        fixture.runner.environment = captured.environment;
        fixture.runner.inputs = captured.inputs;
        fixture
            .runner
            .analyze_one(&fixture.configuration, false)
            .assured("the process fixture completes its analysis");
        let environment: serde_json::Value = io::read(&fixture.path("ordinary.environment.json"))
            .assured("the Cargo child captured its environment");
        let expected = if collected {
            "/dev/null"
        } else {
            "runtime-%p-%m.profraw"
        };
        assert_eq!(environment["LLVM_PROFILE_FILE"], expected);
        assert_eq!(std::env::var_os("LLVM_PROFILE_FILE"), parent_profile);
    }
}

#[test]
fn every_authored_input_extension_driver_compiler_and_binary_digest_invalidates() {
    let mut fixture = Harness::new();
    prepare(&mut fixture);
    for name in [
        "Cargo.toml",
        "Cargo.lock",
        "schema.fbs",
        "protocol.proto",
        "inventory.json",
        "tools/nervix-lint/report/src/rules.rs",
    ] {
        let path = fixture.directory.path().join(name);
        fs::create_dir_all(path.parent().assured("the authored file has a parent"))
            .assured("the authored directory can be created");
        fs::write(path, "current input\n").assured("the authored input can be written");
    }
    let original = capture(&fixture);
    for name in [
        "src/lib.rs",
        "Cargo.toml",
        "Cargo.lock",
        "schema.fbs",
        "protocol.proto",
        "inventory.json",
        "tools/nervix-lint/report/src/rules.rs",
        "tools/nervix-lint/configurations.toml",
        "target/typed-ratchet/driver/debug/nervix-lint-driver",
    ] {
        let path = fixture.directory.path().join(name);
        let bytes = fs::read(&path).assured("the authored input can be read");
        let mut changed = bytes.clone();
        changed.extend_from_slice(b"\n# changed input\n");
        fs::write(&path, changed).assured("the authored input can be changed");
        assert_ne!(
            capture(&fixture).identity,
            original.identity,
            "identity input {name}"
        );
        fs::write(path, bytes).assured("the authored input can be restored");
    }
    fixture.set("NERVIX_TEST_COMPILER", "another compiler");
    assert_ne!(capture(&fixture).identity, original.identity);
    let path = original.work.join("identity.json");
    let payload: Identity = io::read(&path).assured("the identity payload was published");
    let mut binary = payload.clone();
    binary.workspace_wrapper.push_str(" changed binary");
    assert_ne!(
        io::fingerprint(&binary).assured("the changed binary payload hashes"),
        io::fingerprint(&payload).assured("the current payload hashes")
    );
}

#[test]
fn cargo_configuration_and_each_compiler_environment_family_invalidates() {
    let mut fixture = Harness::new();
    prepare(&mut fixture);
    let original = capture(&fixture);
    for key in [
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "KACHE_KEY_SALT",
        "RUSTC_WRAPPER",
        "RUSTC",
        "CARGO_BUILD_TARGET",
        "CARGO_BUILD_RUSTFLAGS",
        "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS",
        "CARGO_PROFILE_DEV_DEBUG_ASSERTIONS",
    ] {
        let mut environment = fixture.runner.environment.clone();
        environment.insert(key.into(), "qualified value".into());
        let changed = Runner::from_environment(
            fixture.directory.path(),
            &fixture.path(""),
            false,
            environment,
        )
        .assured("the isolated compiler environment is captured without executing its wrapper");
        assert_ne!(
            changed.identity, original.identity,
            "environment input {key}"
        );
    }
    for path in [
        fixture.directory.path().join(".cargo/config.toml"),
        fixture.path("private-home/.cargo/config"),
    ] {
        fs::create_dir_all(path.parent().assured("the Cargo config has its parent"))
            .assured("the Cargo config directory can be created");
        fs::write(path, "[build]\nrustc-wrapper='kache'\n")
            .assured("the local Cargo configuration can be written");
        assert_ne!(capture(&fixture).identity, original.identity);
    }
    let inventory = Runner::from_environment(
        fixture.directory.path(),
        &fixture.path(""),
        true,
        fixture.runner.environment.clone(),
    )
    .assured("inventory has its own analysis mode");
    assert_ne!(inventory.identity, original.identity);
}

#[test]
fn an_existing_workspace_wrapper_and_a_missing_driver_fail_before_compilation() {
    let mut fixture = Harness::new();
    prepare(&mut fixture);
    fixture.set("RUSTC_WORKSPACE_WRAPPER", "another workspace owner");
    let result = Runner::from_environment(
        fixture.directory.path(),
        &fixture.path(""),
        false,
        fixture.runner.environment.clone(),
    );
    assert!(matches!(
        result
            .err()
            .assured("the competing wrapper is refused")
            .current_context(),
        Error::WorkspaceWrapper
    ));
    fs::remove_file(fixture.path("typed-ratchet/driver/debug/nervix-lint-driver"))
        .assured("the fixture driver can be removed");
    let result = Runner::from_environment(
        fixture.directory.path(),
        &fixture.path(""),
        false,
        BTreeMap::new(),
    );
    assert!(matches!(
        result
            .err()
            .assured("the absent driver is refused")
            .current_context(),
        Error::MissingDriver { .. }
    ));
    assert_eq!(fixture.runs(), 0);
}
