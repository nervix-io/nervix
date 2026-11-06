//! Repository test harness, outside the product layer order.
//! Owns: concurrent child ordering, fail-closed scheduling and child-evidence examples.
//! Depends on: production matrix orchestration and isolated process observations.
//! Must not know: real compiler execution or product graph behavior.

use std::{num::NonZeroUsize, process::Command};

use meticulous::ResultExt as _;

use super::*;
use crate::test_support::Harness;

fn options() -> Analyze {
    Analyze {
        root: None,
        target_dir: None,
        configuration: Vec::new(),
        turmoil_child: false,
        fixture_mode: None,
        fresh: false,
        recompile: false,
        inventory: false,
        output: None,
        show: false,
        jobs: NonZeroUsize::new(2),
    }
}

#[test]
fn configurations_run_beside_each_other_and_publish_in_declared_order() {
    use std::cell::Cell;
    let fixture = Harness::new();
    let template = fixture
        .runner
        .analyze_one(&fixture.configuration, false)
        .assured("the current completion fixture validates");
    let mut second = fixture.configuration.clone();
    second.name = "testing".into();
    let started = Cell::new(0);
    let second_finished = Cell::new(false);
    let pauses = Cell::new(0);
    let evidence = schedule(
        &[fixture.configuration.clone(), second],
        2,
        |configuration| {
            started.set(started.get() + 1);
            Ok(Started::Running(configuration.clone()))
        },
        |configuration| {
            if configuration.name == "ordinary" && !second_finished.get() {
                return Ok(None);
            }
            assert_eq!(
                started.get(),
                2,
                "the second configuration starts before the first is awaited"
            );
            second_finished.set(true);
            let mut completion = template.clone();
            completion.configuration = configuration.clone();
            Ok(Some(completion))
        },
        || {
            assert_eq!(started.get(), 2);
            pauses.set(pauses.get() + 1);
            assert!(pauses.get() < 4);
        },
    )
    .assured("concurrent jobs may complete in reverse order");
    assert_eq!(
        evidence
            .iter()
            .map(|item| item.configuration.name.as_str())
            .collect::<Vec<_>>(),
        ["ordinary", "testing"]
    );
}

#[test]
fn a_failed_configuration_starts_no_later_one_and_awaits_every_started_child() {
    for jobs in [1, 2] {
        let mut fixture = Harness::new();
        fixture.set("NERVIX_TEST_FAIL", "ordinary");
        fixture.set("NERVIX_TEST_DELAY", "0.1");
        let mut second = fixture.configuration.clone();
        second.name = "testing".into();
        let mut third = fixture.configuration.clone();
        third.name = "later".into();
        let mut options = options();
        options.jobs = NonZeroUsize::new(jobs);
        assert!(
            options
                .collect(
                    &fixture.runner,
                    &[fixture.configuration.clone(), second, third],
                    "current sources"
                )
                .is_err()
        );
        assert!(!fixture.path("later.started").exists());
        if jobs == 1 {
            assert!(!fixture.path("testing.started").exists());
        } else {
            assert!(fixture.path("testing.finished").exists());
        }
    }
}

#[test]
fn job_limits_honor_explicit_then_environment_then_default_and_refuse_invalid_counts() {
    let mut options = options();
    assert_eq!(
        options
            .jobs_from(Ok("8".into()))
            .assured("the explicit bound wins"),
        2
    );
    options.jobs = None;
    assert_eq!(
        options
            .jobs_from(Err(std::env::VarError::NotPresent))
            .assured("the default is bounded"),
        4
    );
    assert_eq!(
        options
            .jobs_from(Ok("3".into()))
            .assured("the environment chooses the bound"),
        3
    );
    for invalid in ["0", "-1", "no"] {
        assert!(options.jobs_from(Ok(invalid.into())).is_err());
    }
}

#[test]
fn partial_unknown_and_invalid_turmoil_child_requests_cannot_be_a_gate() {
    let mut fixture = Harness::new();
    let path = fixture
        .directory
        .path()
        .join("tools/nervix-lint/configurations.toml");
    std::fs::create_dir_all(path.parent().assured("the declaration has its parent"))
        .assured("the declaration directory can be written");
    std::fs::write(
        path,
        r#"[[configuration]]
name='ordinary'
manifest='Cargo.toml'
packages=[]
[[configuration]]
name='turmoil'
manifest='Cargo.toml'
packages=[]
features=['turmoil']
[admission]
threshold_gib=4
wait_limit_minutes=30
reservation=[]
"#,
    )
    .assured("the current matrix declaration can be written");
    let mut options = options();
    options.configuration = vec!["ordinary".into()];
    assert!(options.configurations(&fixture.runner).is_err());
    options.inventory = true;
    assert_eq!(
        options
            .configurations(&fixture.runner)
            .assured("selected configurations are inventory")
            .len(),
        1
    );
    options.configuration = vec!["unknown".into()];
    assert!(options.configurations(&fixture.runner).is_err());
    options.inventory = false;
    options.turmoil_child = true;
    options.configuration = vec!["ordinary".into()];
    assert!(options.configurations(&fixture.runner).is_err());
    options.configuration = vec!["turmoil".into()];
    assert!(options.configurations(&fixture.runner).is_err());
    fixture.set("RUSTFLAGS", "--cfg tokio_unstable");
    assert_eq!(
        options
            .configurations(&fixture.runner)
            .assured("only the recipe's declared Turmoil child is a partial gate")[0]
            .name,
        "turmoil"
    );
}

#[test]
fn turmoil_results_must_match_the_full_parent_configuration_and_source_verdict() {
    let fixture = Harness::new();
    let evidence = fixture
        .runner
        .analyze_one(&fixture.configuration, false)
        .assured("the child evidence fixture completes");
    let path = fixture.path("turmoil.json");
    let original = MatrixReport {
        identity: "child identity".into(),
        source_identity: "current sources".into(),
        compiler: fixture.runner.compiler.clone(),
        root: fixture.runner.root.clone(),
        configurations: vec![fixture.configuration.clone()],
        evidence: vec![evidence],
        findings: Vec::new(),
        complete: true,
        matrix_complete: false,
        diagnostics_checked: true,
    };
    for field in [
        "valid",
        "complete",
        "compiler",
        "root",
        "diagnostics",
        "source",
        "configuration",
        "evidence",
    ] {
        let mut report = original.clone();
        match field {
            "valid" => {}
            "complete" => report.complete = false,
            "compiler" => report.compiler.clear(),
            "root" => report.root = fixture.path("another"),
            "diagnostics" => report.diagnostics_checked = false,
            "source" => report.source_identity.clear(),
            "configuration" => report.configurations[0].features.push("shuttle".into()),
            "evidence" => report.evidence[0].configuration.target = Some("other-target".into()),
            _ => unreachable!(),
        }
        io::atomic_json(&path, &report).assured("the child result can be written");
        let mut child = Command::new("true")
            .spawn()
            .assured("the isolated successful child can start");
        child.wait().assured("the isolated child is reaped");
        let mut job = Job::Turmoil {
            child,
            configuration: fixture.configuration.clone(),
            output: path.clone(),
        };
        let result = job.poll(&fixture.runner, "current sources", false);
        assert_eq!(result.is_ok(), field == "valid", "child field {field}");
    }
}
