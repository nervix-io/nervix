//! Repository test harness, outside the product layer order.
//! Owns: complete/current evidence and compilation-measurement refusal examples.
//! Depends on: production validation owners and temporary current-shape artifacts.
//! Must not know: host load, real compiler execution or product graph policy.

use std::{collections::BTreeMap, fs};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_lint_report::CompilerReport;
use tempfile::TempDir;

use super::*;
use crate::{
    admission::{AdmissionOutcome, AdmissionRecord, GIB},
    evidence::{CargoTarget, Compilation, Ending},
};

struct Fixture {
    directory: TempDir,
    runner: Runner,
    configuration: Configuration,
    expected: BTreeMap<String, ExpectedTarget>,
    messages: Vec<CargoMessage>,
    report: CompilerReport,
    report_path: PathBuf,
    records: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().assured("a temporary fixture directory can be created");
        let root = directory.path().to_owned();
        let source = root.join("src/lib.rs");
        fs::create_dir_all(source.parent().assured("the source has its src parent"))
            .assured("the source directory can be created");
        fs::write(&source, "pub fn empty() {}\n").assured("the source can be written");
        let artifact = root.join("libexample-abcd.rmeta");
        fs::write(&artifact, b"metadata").assured("the current artifact can be written");
        let records = root.join("reports/compilations");
        fs::create_dir_all(&records).assured("the report directories can be created");
        let report_path = root.join("reports/example-abcd.json");
        let configuration = Configuration {
            name: "ordinary".into(),
            manifest: "Cargo.toml".into(),
            packages: vec!["example".into()],
            kinds: vec!["lib".into()],
            features: Vec::new(),
            target: None,
        };
        let mut reservations = BTreeMap::new();
        reservations.insert(
            Compiled {
                crate_name: "example".into(),
                kind: "lib".into(),
            },
            16 * GIB,
        );
        let runner = Runner {
            root: root.clone(),
            target: root.join("target"),
            driver: root.join("driver"),
            wrapper: root.join("wrapper"),
            policy: Policy {
                directory: root.join("queue"),
                threshold_bytes: 4 * GIB,
                wait_limit_seconds: 60.0,
                reservations,
            },
            compiler: "qualified compiler".into(),
            identity: "current worktree fingerprint".into(),
            inputs: BTreeMap::new(),
            environment: BTreeMap::from([("NERVIX_LINT_MODE".into(), "gate".into())]),
            work: root.join("target/evidence"),
        };
        let report = CompilerReport {
            complete: true,
            identity: runner.identity.clone(),
            compiler: runner.compiler.clone(),
            configuration: configuration.name.clone(),
            crate_name: "example".into(),
            crate_source: source.display().to_string(),
            arguments: Vec::new(),
            findings: Vec::new(),
            audit: Vec::new(),
            excluded_generated: 0,
            excluded_external: 0,
        };
        io::atomic_json(&report_path, &report).assured("the current side report can be written");
        let expected = BTreeMap::from([(
            "example-id::example::lib".into(),
            ExpectedTarget {
                package: "example".into(),
                source: source.clone(),
                kind: "lib".into(),
                required: true,
            },
        )]);
        let messages = vec![CargoMessage::CompilerArtifact(CargoArtifact {
            package_id: "example-id".into(),
            target: CargoTarget {
                name: "example".into(),
                kind: vec!["lib".into()],
                src_path: source,
            },
            filenames: vec![artifact],
            fresh: false,
        })];
        Self {
            directory,
            runner,
            configuration,
            expected,
            messages,
            report,
            report_path,
            records,
        }
    }

    fn reports(&self) -> PathBuf {
        self.directory.path().join("reports")
    }
    fn validate(&self) -> Result<Validated> {
        self.runner.validate_reports(
            &self.configuration,
            &self.expected,
            &self.messages,
            &self.reports(),
        )
    }
    fn measure(&self) -> Result<BTreeMap<String, Measurement>> {
        self.runner.measure_compilations(
            &self.configuration,
            &self.expected,
            &self.messages,
            &self.reports(),
        )
    }

    fn record(&self, peak: u64) {
        let record = Compilation {
            crate_name: Some("example".into()),
            crate_types: vec!["lib".into()],
            extra_filename: "-abcd".into(),
            source: Some(self.directory.path().join("src/lib.rs")),
            ending: Ending::Exit { status: 0 },
            peak_resident_bytes: peak,
            started_unix_ns: 1_000,
            seconds: 2.5,
            oom_kills: Some(0),
            admission: Some(AdmissionRecord {
                reservation_bytes: 16 * GIB,
                outcome: AdmissionOutcome::Fit,
                waited_seconds: 0.0,
                left_bytes: Some(i128::from(32 * GIB)),
                holders: 0,
                error: None,
            }),
        };
        io::atomic_json(&self.records.join("example-abcd.json"), &record)
            .assured("the compilation record can be written");
    }
}

#[test]
fn zero_sites_still_require_complete_declared_target_coverage() {
    let mut fixture = Fixture::new();
    let evidence = fixture
        .validate()
        .assured("the current complete report validates");
    assert_eq!(evidence.reports.len(), 1);
    assert!(
        evidence.reports["example-id::example::lib"]
            .findings
            .is_empty()
    );
    fixture.messages.clear();
    let error = fixture
        .validate()
        .err()
        .assured("a missing declared target is refused");
    assert!(format!("{error:?}").contains("example-id::example::lib"));
}

#[test]
fn a_missing_or_ambiguous_side_report_is_never_zero_findings() {
    let fixture = Fixture::new();
    fs::remove_file(&fixture.report_path).assured("the fixture removes its own report");
    assert!(fixture.validate().is_err());
    io::atomic_json(&fixture.report_path, &fixture.report)
        .assured("the side report can be restored");
    let mut messages = fixture.messages.clone();
    let CargoMessage::CompilerArtifact(artifact) = &mut messages[0] else {
        panic!("the fixture contains an artifact");
    };
    let other = fixture.directory.path().join("example-efgh.rmeta");
    fs::write(&other, b"metadata").assured("the second metadata artifact can be written");
    artifact.filenames.push(other);
    io::atomic_json(
        &fixture.reports().join("example-efgh.json"),
        &fixture.report,
    )
    .assured("the ambiguous report can be written");
    assert!(
        fixture
            .runner
            .validate_reports(
                &fixture.configuration,
                &fixture.expected,
                &messages,
                &fixture.reports()
            )
            .is_err()
    );
}

#[test]
fn partial_stale_foreign_compiler_configuration_target_and_source_reports_fail() {
    let fixture = Fixture::new();
    for field in [
        "complete",
        "identity",
        "compiler",
        "configuration",
        "crate_name",
        "crate_source",
    ] {
        let mut report = fixture.report.clone();
        match field {
            "complete" => report.complete = false,
            "identity" => report.identity = "another worktree".into(),
            "compiler" => report.compiler = "another compiler".into(),
            "configuration" => report.configuration = "shuttle".into(),
            "crate_name" => report.crate_name = "another_target".into(),
            "crate_source" => {
                let source = fixture.directory.path().join("different.rs");
                fs::write(&source, "pub fn different() {}\n")
                    .assured("the alternative current source can be written");
                report.crate_source = source.display().to_string();
            }
            _ => unreachable!(),
        }
        io::atomic_json(&fixture.report_path, &report).assured("the changed report can be written");
        assert!(
            fixture.validate().is_err(),
            "the {field} mismatch must fail"
        );
    }
}

#[test]
fn missing_metadata_fails_even_with_a_complete_side_report() {
    let fixture = Fixture::new();
    fs::remove_file(fixture.directory.path().join("libexample-abcd.rmeta"))
        .assured("the fixture removes its own metadata");
    assert!(fixture.validate().is_err());
}

#[test]
fn required_gate_refuses_warnings_while_inventory_keeps_them() {
    use crate::evidence::{Diagnostic, DiagnosticCode};
    let mut fixture = Fixture::new();
    let CargoMessage::CompilerArtifact(artifact) = &fixture.messages[0] else {
        panic!("the fixture contains an artifact");
    };
    fixture.messages.push(CargoMessage::CompilerMessage {
        target: artifact.target.clone(),
        message: Diagnostic {
            level: "warning".into(),
            code: Some(DiagnosticCode {
                code: "nervix::sync_acquisition".into(),
            }),
            rendered: None,
            message: "a current acquisition".into(),
        },
    });
    assert!(fixture.validate().is_err());
    fixture
        .runner
        .environment
        .insert("NERVIX_LINT_MODE".into(), "inventory".into());
    assert!(fixture.validate().is_ok());
}

#[test]
fn measurements_preserve_complete_records_and_enforce_declared_bounds() {
    let mut fixture = Fixture::new();
    fixture.record(15 * GIB);
    let measured = fixture
        .measure()
        .assured("the measured compilation fits its reservation");
    assert_eq!(
        measured["example-id::example::lib"].peak_resident_bytes,
        15 * GIB
    );
    assert_eq!(measured["example-id::example::lib"].seconds, 2.5);
    fixture.record(17 * GIB);
    assert!(fixture.measure().is_err());
    fixture.runner.policy.reservations.clear();
    fixture.record(5 * GIB);
    assert!(fixture.measure().is_err());
    fixture.record(3 * GIB);
    assert!(fixture.measure().is_ok());
}

#[test]
fn missing_ambiguous_or_failed_compilation_records_fail() {
    let fixture = Fixture::new();
    assert!(fixture.measure().is_err());
    fixture.record(GIB);
    let path = fixture.records.join("example-abcd.json");
    let mut record: Compilation = io::read(&path).assured("the current record is readable");
    record.ending = Ending::Exit { status: 1 };
    io::atomic_json(&path, &record).assured("the failed record can be written");
    assert!(fixture.measure().is_err());
    fixture.record(GIB);
    let mut messages = fixture.messages.clone();
    let CargoMessage::CompilerArtifact(artifact) = &mut messages[0] else {
        panic!("the fixture contains an artifact");
    };
    artifact
        .filenames
        .push(fixture.directory.path().join("libexample-efgh.rmeta"));
    io::atomic_json(&fixture.records.join("example-efgh.json"), &record)
        .assured("the second record can be written");
    assert!(
        fixture
            .runner
            .measure_compilations(
                &fixture.configuration,
                &fixture.expected,
                &messages,
                &fixture.reports()
            )
            .is_err()
    );
}

#[test]
fn fresh_artifacts_and_external_dependencies_do_not_require_new_measurements() {
    let mut fixture = Fixture::new();
    let CargoMessage::CompilerArtifact(artifact) = &mut fixture.messages[0] else {
        panic!("the fixture contains an artifact");
    };
    artifact.fresh = true;
    assert!(
        fixture
            .measure()
            .assured("a fresh artifact did not compile again")
            .is_empty()
    );
    let CargoMessage::CompilerArtifact(artifact) = &mut fixture.messages[0] else {
        panic!("the fixture contains an artifact");
    };
    artifact.fresh = false;
    artifact.package_id = "external-package".into();
    assert!(
        fixture
            .measure()
            .assured("a non-workspace dependency has no workspace-wrapper record")
            .is_empty()
    );
}

#[test]
fn workspace_dependencies_outside_required_roots_still_obey_the_admission_policy() {
    let mut fixture = Fixture::new();
    fixture.expected.clear();
    fixture.record(17 * GIB);
    assert!(fixture.measure().is_err());
}

#[test]
fn metadata_requires_selected_libraries_and_binaries_but_only_observes_other_libraries() {
    use serde_json::json;

    use crate::test_support::Harness;
    let fixture = Harness::new();
    let packages: Vec<_> = ["one", "two"].into_iter().map(|name| json!({"id":name, "name":name, "targets":[{"name":name, "kind":["lib"], "src_path":format!("{name}/lib.rs")}, {"name":format!("{name}-cli"), "kind":["bin"], "src_path":format!("{name}/main.rs")}, {"name":"build-script-build", "kind":["custom-build"], "src_path":format!("{name}/build.rs")}]})).collect();
    io::atomic_json(
        &fixture.path("metadata.json"),
        &json!({"workspace_members":["one", "two"], "packages":packages}),
    )
    .assured("the current workspace metadata serializes");
    let mut configuration = fixture.configuration.clone();
    configuration.packages = vec!["one".into()];
    configuration.kinds = vec!["lib".into(), "bin".into()];
    let expected = fixture
        .runner
        .expected_targets(&configuration)
        .assured("selected roots have their declared target kinds");
    assert_eq!(expected.len(), 3);
    assert_eq!(
        expected.values().filter(|target| target.required).count(),
        2
    );
    configuration.packages = vec!["unknown".into()];
    assert!(fixture.runner.expected_targets(&configuration).is_err());
    configuration.packages.clear();
    io::atomic_json(
        &fixture.path("metadata.json"),
        &json!({"workspace_members":[], "packages":[]}),
    )
    .assured("the empty workspace metadata serializes");
    assert!(fixture.runner.expected_targets(&configuration).is_err());
}
