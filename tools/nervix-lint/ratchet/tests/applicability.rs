//! Repository process-test harness, outside the product layer order.
//! Owns: diagnostic lock-owner applicability through the production command.
//! Depends on: the current matrix/report types and isolated input files.
//! Must not know: compiler implementation or diagnostic workload execution.

use std::{collections::BTreeMap, fs, path::Path, process::Command};

use meticulous::ResultExt as _;
use nervix_lint_report::{Acquisition, AuthoredSite, Finding, SiteId, SourceSpan};
use nervix_ratchet::{configuration::Configuration, evidence::MatrixReport};
use tempfile::TempDir;

struct Fixture {
    directory: TempDir,
    report: MatrixReport,
}

impl Fixture {
    fn new() -> Self {
        let directory =
            tempfile::tempdir().assured("the isolated applicability fixture is writable");
        fs::create_dir(directory.path().join("tests"))
            .assured("the fixture inventory directory is writable");
        fs::write(
            directory.path().join("tests/deloxide-inventory.toml"),
            "[[owner]]\npath = 'src/store.rs'\nworkloads = ['owner.store']\n\n[[owner]]\npath = \
             'crates/client/src/lib.rs'\ngap = 'Not reached by this fixture.'\n",
        )
        .assured("the current owner declarations can be written");
        let configurations = vec![
            Configuration::fixture("ordinary"),
            Configuration::fixture("deloxide"),
            Configuration::fixture("deloxide-order"),
        ];
        let report = MatrixReport {
            identity: "current-fixture".into(),
            source_identity: "current-source".into(),
            compiler: "fixture compiler".into(),
            root: directory.path().into(),
            configurations,
            evidence: Vec::new(),
            findings: Vec::new(),
            complete: true,
            matrix_complete: true,
            diagnostics_checked: true,
        };
        Self { directory, report }
    }

    fn root(&self) -> &Path {
        self.directory.path()
    }

    fn acquisition(&mut self, configuration: &str, receiver: &str, path: &str) {
        let site = SiteId {
            path: path.into(),
            start: 1,
        };
        let finding = Finding {
            rule: "data_plane_lock_acquisitions".into(),
            receiver: receiver.into(),
            receiver_type: "Mutex<()>".into(),
            receiver_expression: "state".into(),
            operation: "lock".into(),
            definition: "fixture::lock".into(),
            owner: "fixture::owner".into(),
            acquisition: Acquisition::Exclusive,
            context: None,
            span: SourceSpan {
                site: site.clone(),
                end: 2,
                line: 1,
                column: 1,
                end_line: 1,
                end_column: 2,
                source: "x".into(),
            },
            expansion: Vec::new(),
        };
        self.report.findings.push(AuthoredSite {
            site,
            configurations: BTreeMap::from([(format!("{configuration}:fixture"), vec![finding])]),
        });
    }

    fn run(&self) -> std::process::Output {
        fs::write(
            self.root().join("gate.json"),
            serde_json::to_vec(&self.report).assured("the current matrix report serializes"),
        )
        .assured("the fixture report can be written");
        self.command()
    }

    fn command(&self) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_nervix-ratchet"))
            .args(["applicability", "--root"])
            .arg(self.root())
            .arg("--catalog")
            .arg(self.root().join("gate.json"))
            .output()
            .assured("the built applicability command can execute")
    }
}

const MUTEX: &str = "nervix_primitives::sync::blocking::tracked::Mutex";
const RWLOCK: &str = "nervix_primitives::sync::blocking::tracked::RwLock";

#[test]
fn tracked_owners_are_compared_once_across_diagnostic_configurations() {
    let mut fixture = Fixture::new();
    fixture.acquisition("fixture-deloxide", MUTEX, "src/store.rs");
    fixture.acquisition("fixture-deloxide-order", RWLOCK, "crates/client/src/lib.rs");
    fixture.acquisition("fixture-deloxide-order", MUTEX, "src/store.rs");
    fixture.acquisition("fixture-ordinary", MUTEX, "src/ordinary.rs");
    fixture.acquisition("fixture-deloxide", "dashmap::DashMap", "src/map.rs");
    let output = fixture.run();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).assured("the command prints UTF-8"),
        "deloxide applicability: 2 owners of tracked locks, 1 reached by registered workloads, 1 \
         with a recorded gap\n"
    );
}

#[test]
fn missing_and_unused_owner_records_keep_both_diagnostics() {
    let mut fixture = Fixture::new();
    fixture.acquisition("fixture-deloxide", MUTEX, "src/store.rs");
    fixture.acquisition("fixture-deloxide", MUTEX, "src/new_owner.rs");
    let output = fixture.run();
    assert_eq!(output.status.code(), Some(1));
    let errors = String::from_utf8(output.stderr).assured("the command prints UTF-8");
    assert!(
        errors.contains(
            "src/new_owner.rs acquires tracked blocking locks in a diagnostic build and has no \
             [[owner]] record"
        ),
        "{errors}"
    );
    assert!(
        errors
            .contains("records the owner crates/client/src/lib.rs, which acquires no tracked lock"),
        "{errors}"
    );
    assert!(output.stdout.is_empty());
}

#[test]
fn every_testing_diagnostic_selection_contributes_its_owners() {
    for mode in ["deloxide", "deloxide-order", "deloxide-stress"] {
        let mut fixture = Fixture::new();
        let mut configuration = Configuration::fixture(mode);
        configuration.name = "testing-diagnostic".into();
        configuration.features.push("testing".into());
        fixture.report.configurations.push(configuration);
        fixture.acquisition("fixture-deloxide", MUTEX, "src/store.rs");
        fixture.acquisition("fixture-deloxide-order", RWLOCK, "crates/client/src/lib.rs");
        fixture.acquisition("testing-diagnostic", MUTEX, "src/testing_owner.rs");
        let output = fixture.run();
        assert_eq!(output.status.code(), Some(1));
        let errors = String::from_utf8(output.stderr).assured("the command prints UTF-8");
        assert!(
            errors.contains("src/testing_owner.rs acquires tracked blocking locks"),
            "{mode}: {errors}"
        );
        assert_eq!(errors.lines().count(), 1, "{mode}: {errors}");
    }
}

#[test]
fn missing_diagnostic_configuration_empty_owner_path_and_invalid_json_are_refused() {
    let mut fixture = Fixture::new();
    fixture.report.configurations = vec![Configuration::fixture("ordinary")];
    let output = fixture.run();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("no diagnostic configuration"));

    fixture
        .report
        .configurations
        .push(Configuration::fixture("deloxide"));
    fixture.acquisition("fixture-deloxide", MUTEX, "");
    let output = fixture.run();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("names no source file"));

    fs::write(fixture.root().join("gate.json"), "{")
        .assured("the deliberately malformed report can be written");
    let output = fixture.command();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid JSON"));
}
