//! The ordinary local report tool, through its actual process and evidence boundary.

use std::{
    num::NonZeroU64,
    path::Path,
    process::{Command, Output},
    time::UNIX_EPOCH,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_deadlock::{DeadlockEvidence, ProcessRecord, RecordedFinding};
use nervix_primitives::deadlock::{
    Access, BlockedAttempt, BoundedText, DiagnosticSelection, LockKind, LockLifetime, LockSite,
    OrderEdge, OrderLock, OrderWitness, PotentialCycle, SourceSite, TrackedLockId, TrackedThreadId,
};

fn cycle(access: Access) -> RecordedFinding {
    let source = SourceSite {
        file: BoundedText::new(file!()),
        line: line!(),
        column: 1,
    };
    let lock = |id| OrderLock {
        id: TrackedLockId::new(NonZeroU64::new(id).assured("fixture identity is positive")),
        site: Some(LockSite {
            kind: LockKind::RwLock,
            constructed_at: source.clone(),
        }),
        lifetime: LockLifetime::Ended,
    };
    let first = lock(1);
    let second = lock(2);
    let witness = OrderWitness {
        thread: TrackedThreadId::new(NonZeroU64::MIN),
        name: None,
        held: BlockedAttempt {
            access,
            at: source.clone(),
        },
        requested: BlockedAttempt { access, at: source },
        attempts: NonZeroU64::MIN,
        held_count: NonZeroU64::MIN,
    };
    let edges = vec![
        OrderEdge::new(first.clone(), second.clone(), vec![witness.clone()], 0)
            .assured("one witness fits"),
        OrderEdge::new(second, first, vec![witness], 0).assured("one witness fits"),
    ];
    nervix_primitives::deadlock::Finding::PotentialCycle(
        PotentialCycle::new(UNIX_EPOCH, edges, 0).assured("two complete edges fit"),
    )
    .into()
}

fn evidence(finding: Option<RecordedFinding>) -> DeadlockEvidence {
    DeadlockEvidence::new(
        ProcessRecord {
            id: 1,
            program: Some(BoundedText::new("local-report-fixture")),
            started_at: UNIX_EPOCH,
            selection: DiagnosticSelection::OrderAnalysis,
        },
        finding.into_iter().collect(),
    )
    .assured("one finding fits")
}

fn run(path: &Path, action: &str, arguments: &[&str], expected: i32) -> Output {
    let output = Command::new(env!("CARGO_BIN_EXE_nervix-deadlock-report"))
        .arg(action)
        .arg(path)
        .args(arguments)
        .output()
        .assured("the report tool executes");
    assert_eq!(
        output.status.code(),
        Some(expected),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[test]
fn local_report_selection_preserves_evidence_and_review_requires_complete_proof() {
    let root = tempfile::tempdir().assured("a disposable local directory exists");
    let path = root.path().join("run.rkyv");
    let original = evidence(Some(cycle(Access::Exclusive)));
    original
        .write_file(&path)
        .assured("fixture evidence is written");
    run(&path, "qualify", &[], 5);
    for source in ["all", "active", "potential"] {
        let export = root.path().join(format!("{source}.rkyv"));
        let output = run(
            &path,
            "inspect",
            &[
                "--source",
                source,
                "--export",
                export.to_str().assured("test path is UTF-8"),
            ],
            0,
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("OrderAnalysis"));
        let exported =
            DeadlockEvidence::read_file(&export).assured("the selected artifact is readable");
        if source == "all" {
            assert_eq!(exported, original);
        } else {
            run(&export, "qualify", &[], 5);
        }
    }
    let reviewed = root.path().join("reviewed.rkyv");
    let output_path = reviewed.to_str().assured("test path is UTF-8");
    for basis in ["correction", "non-overlap", "lifecycle"] {
        run(&path, "triage", &["--finding", "0", "--basis", basis, "--reason", "Owner review supplies the disposition; evidence is retained", "--regression", "report_cli::local_report_selection_preserves_evidence_and_review_requires_complete_proof", "--output", output_path], 0);
        run(&reviewed, "qualify", &[], 0);
    }
    run(&path, "triage", &["--finding", "0", "--basis", "shared-readers", "--reason", "exclusive access is present", "--regression", "report_cli::local_report_selection_preserves_evidence_and_review_requires_complete_proof", "--output", output_path], 4);
    run(
        &path,
        "triage",
        &[
            "--finding",
            "2",
            "--basis",
            "non-overlap",
            "--reason",
            "missing finding",
            "--regression",
            "retained-test",
            "--output",
            output_path,
        ],
        4,
    );
    run(
        &path,
        "triage",
        &[
            "--finding",
            "0",
            "--basis",
            "non-overlap",
            "--reason",
            "",
            "--regression",
            "retained-test",
            "--output",
            output_path,
        ],
        4,
    );
    assert_eq!(
        DeadlockEvidence::read_file(&path).assured("the original remains readable"),
        original
    );
    evidence(Some(cycle(Access::Shared)))
        .write_file(&path)
        .assured("shared evidence is written");
    run(&path, "triage", &["--finding", "0", "--basis", "shared-readers", "--reason", "all observed acquisitions are shared", "--regression", "report_cli::local_report_selection_preserves_evidence_and_review_requires_complete_proof", "--output", output_path], 0);
    run(&reviewed, "qualify", &[], 0);
}

#[test]
fn local_report_failures_are_distinct_from_a_clean_observation() {
    let root = tempfile::tempdir().assured("a disposable local directory exists");
    let path = root.path().join("run.rkyv");
    run(&path, "inspect", &[], 4);
    std::fs::write(&path, b"truncated diagnostic evidence")
        .assured("a damaged current artifact is written");
    run(&path, "qualify", &[], 4);
    evidence(None)
        .write_file(&path)
        .assured("empty current observation is written");
    run(&path, "qualify", &[], 0);
    run(
        &path,
        "inspect",
        &[
            "--export",
            root.path().to_str().assured("test path is UTF-8"),
        ],
        4,
    );
    run(
        &path,
        "triage",
        &[
            "--finding",
            "0",
            "--basis",
            "lifecycle",
            "--reason",
            "no potential finding is present",
            "--regression",
            "retained-test",
            "--output",
            root.path().to_str().assured("test path is UTF-8"),
        ],
        4,
    );
}
