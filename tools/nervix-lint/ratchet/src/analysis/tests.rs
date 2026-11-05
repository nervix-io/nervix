//! Repository test harness, outside the product layer order.
//! Owns: completion reuse, reanalysis and failed-measurement process evidence.
//! Depends on: production orchestration and simulated Cargo observations.
//! Must not know: real compiler behavior or product execution.

use std::fs;

use meticulous::{OptionExt as _, ResultExt as _};

use crate::{
    admission::Policy,
    error::{Error, Issue},
    evidence::Completion,
    io,
    test_support::Harness,
};

#[test]
fn complete_evidence_reuses_without_cargo_but_fresh_observations_run_cargo() {
    let mut fixture = Harness::new();
    let first = fixture
        .runner
        .analyze_one(&fixture.configuration, false)
        .assured("the complete current process evidence validates");
    let reused = fixture
        .runner
        .analyze_one(&fixture.configuration, false)
        .assured("complete evidence can be reused");
    assert_eq!(first, reused);
    assert_eq!(fixture.runs(), 1);
    fixture.set("NERVIX_TEST_FRESH", "1");
    let fresh = fixture
        .runner
        .analyze_one(&fixture.configuration, true)
        .assured("Cargo-fresh artifacts retain their complete side reports");
    assert_eq!(fixture.runs(), 2);
    assert_eq!(first.reports, fresh.reports);
    assert!(fresh.compilations.is_empty());
    let environment: serde_json::Value = io::read(&fixture.path("ordinary.environment.json"))
        .assured("the process fixture captured its environment");
    assert_eq!(environment["RUSTC_WRAPPER"], "kache");
    assert_eq!(
        environment["RUSTC_WORKSPACE_WRAPPER"],
        fixture.runner.wrapper.display().to_string()
    );
    let policy: Policy = serde_json::from_str(
        environment["NERVIX_LINT_ADMISSION"]
            .as_str()
            .assured("the policy is a JSON string"),
    )
    .assured("the actual child policy decodes");
    assert_eq!(policy, fixture.runner.policy);
}

#[test]
fn one_missing_side_report_reestablishes_in_a_new_namespace_and_cleans_only_its_target() {
    let mut fixture = Harness::new();
    fixture.configuration.target = Some("wasm32-unknown-unknown".into());
    fixture.set("NERVIX_TEST_REPORT_AFTER", "2");
    let evidence = fixture
        .runner
        .analyze_one(&fixture.configuration, false)
        .assured("one missing side report can be re-established");
    assert_eq!(fixture.runs(), 2);
    assert!(
        evidence
            .report_directory
            .to_string_lossy()
            .contains("/reanalysis/")
    );
    assert!(
        evidence
            .cache_namespace
            .starts_with("qualified:nervix-analysis:current worktree fingerprint:")
    );
    let clean: serde_json::Value =
        io::read(&fixture.path("clean.json")).assured("the cleaning command was captured");
    let args = clean["args"]
        .as_array()
        .assured("Cargo clean arguments are an array");
    for expected in [
        "--target",
        "wasm32-unknown-unknown",
        "--target-dir",
        "--package",
        "example",
    ] {
        assert!(args.contains(&serde_json::Value::from(expected)));
    }
    assert!(args.contains(&serde_json::Value::from(
        evidence.build_directory.display().to_string()
    )));
    assert_eq!(clean["wrapper"], "kache");
}

#[test]
fn repeated_missing_reports_and_changed_inputs_fail_without_publishing_completion() {
    for mode in ["NERVIX_TEST_REPORT_AFTER", "NERVIX_TEST_CHANGE_SOURCE"] {
        let mut fixture = Harness::new();
        fixture.set(
            mode,
            if mode == "NERVIX_TEST_REPORT_AFTER" {
                "99"
            } else {
                "1"
            },
        );
        assert!(
            fixture
                .runner
                .analyze_one(&fixture.configuration, false)
                .is_err()
        );
        assert!(
            !fixture
                .runner
                .work
                .join("ordinary/completion.json")
                .exists()
        );
    }
}

#[test]
fn tampered_completion_is_refused_and_changed_artifacts_require_reanalysis() {
    let fixture = Harness::new();
    let evidence = fixture
        .runner
        .analyze_one(&fixture.configuration, false)
        .assured("current complete evidence validates");
    let path = fixture.runner.work.join("ordinary/completion.json");
    let mut tampered = evidence.clone();
    tampered.reports.clear();
    io::atomic_json(&path, &tampered).assured("the changed current evidence can be written");
    let error = fixture
        .runner
        .analyze_one(&fixture.configuration, false)
        .err()
        .assured("disagreeing completion evidence is refused");
    assert!(matches!(
        error.current_context(),
        Error::Evidence {
            issue: Issue::Completion,
            ..
        }
    ));
    let mut mismatched = evidence.clone();
    mismatched.configuration.features.push("shuttle".into());
    io::atomic_json(&path, &mismatched).assured("the mismatched current completion can be written");
    assert!(
        fixture
            .runner
            .analyze_one(&fixture.configuration, false)
            .is_err()
    );
    io::atomic_json(&path, &evidence).assured("the current evidence can be restored");
    fs::write(
        fixture.path("libexample-ordinary.rmeta"),
        "changed metadata",
    )
    .assured("the artifact can be changed");
    fixture
        .runner
        .analyze_one(&fixture.configuration, false)
        .assured("a changed artifact is analyzed again");
    assert_eq!(fixture.runs(), 2);
}

#[test]
fn a_failed_measurement_discards_the_analysis_and_the_next_run_measures_again() {
    let mut fixture = Harness::new();
    fixture.set("NERVIX_TEST_PEAK", "8192");
    assert!(
        fixture
            .runner
            .analyze_one(&fixture.configuration, false)
            .is_err()
    );
    assert!(
        !fixture
            .runner
            .work
            .join("ordinary/completion.json")
            .exists()
    );
    assert!(fixture.path("clean.json").exists());
    fixture.set("NERVIX_TEST_PEAK", "1024");
    let completion: Completion = fixture
        .runner
        .analyze_one(&fixture.configuration, false)
        .assured("the next analysis measures the compilation again");
    assert_eq!(completion.compilations.len(), 1);
    assert_eq!(fixture.runs(), 2);
}

#[test]
fn a_new_cargo_attempt_removes_previous_compilation_failures() {
    let mut fixture = Harness::new();
    let evidence = fixture
        .runner
        .analyze_one(&fixture.configuration, false)
        .assured("the first analysis completes");
    let record = evidence
        .report_directory
        .join("compilations/example-ordinary.json");
    fixture.set("NERVIX_TEST_OMIT_RECORD", "1");
    fixture.set("NERVIX_TEST_FAIL", "ordinary");
    assert!(
        fixture
            .runner
            .analyze_one(&fixture.configuration, true)
            .is_err()
    );
    assert!(!record.exists());
    assert!(
        !fixture
            .runner
            .work
            .join("ordinary/completion.json")
            .exists()
    );
}
