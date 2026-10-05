//! Public local inspection, export and review of diagnostic evidence.
//!
//! Layer: test harness.
//! - **Owns.** A disposable evidence fixture and the report tool's public workflow.
//! - **Depends on.** The current diagnostic evidence and the ordinary report executable.
//! - **Must not know.** Detector internals or how to provoke a node deadlock.

use std::{
    num::NonZeroU64,
    path::PathBuf,
    process::{Command, Output},
    time::UNIX_EPOCH,
};

use cucumber::{given, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_deadlock::{DeadlockEvidence, ProcessRecord, RecordedFinding};
use nervix_primitives::deadlock::{
    Access, BlockedAttempt, BoundedText, DiagnosticSelection, Finding, LockKind, LockLifetime,
    LockSite, OrderEdge, OrderLock, OrderWitness, PotentialCycle, SourceSite, TrackedLockId,
    TrackedThreadId,
};

use super::ScenarioWorld;

#[derive(Debug)]
pub(super) struct ReportFixture {
    directory: tempfile::TempDir,
    original: DeadlockEvidence,
    exported: Option<Output>,
}

impl ReportFixture {
    fn file(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }

    fn command(&self) -> Command {
        let binary = match std::env::var_os("NERVIX_DEADLOCK_REPORT_TOOL") {
            Some(binary) => PathBuf::from(binary),
            None => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target/debug/nervix-deadlock-report"),
        };
        Command::new(binary)
    }

    fn read(&self, name: &str) -> DeadlockEvidence {
        let bytes = std::fs::read(self.file(name)).assured("the public tool wrote its export");
        DeadlockEvidence::decode(&bytes).assured("the export is current valid evidence")
    }
}

impl ScenarioWorld {
    fn report_fixture(&self) -> &ReportFixture {
        self.deadlock_report
            .as_ref()
            .verified("a preceding step provisioned diagnostic evidence")
    }
}

#[given("diagnostic evidence containing a potential lock-order cycle")]
fn given_potential_cycle(world: &mut ScenarioWorld) {
    let id = |number| NonZeroU64::new(number).assured("fixture identities are positive");
    let source = |line| SourceSite {
        file: BoundedText::new("fixture/owner.rs"),
        line,
        column: 2,
    };
    let lock = |number| OrderLock {
        id: TrackedLockId::new(id(number)),
        site: Some(LockSite {
            kind: LockKind::Mutex,
            constructed_at: source(10),
        }),
        lifetime: LockLifetime::Ended,
    };
    let witness = OrderWitness {
        thread: TrackedThreadId::new(id(1)),
        name: Some(BoundedText::new("fixture-owner")),
        held: BlockedAttempt {
            access: Access::Exclusive,
            at: source(20),
        },
        requested: BlockedAttempt {
            access: Access::Exclusive,
            at: source(30),
        },
        attempts: id(3),
        held_count: id(1),
    };
    let edges = vec![
        OrderEdge::new(lock(1), lock(2), vec![witness.clone()], 0).assured("bounded fixture"),
        OrderEdge::new(lock(2), lock(1), vec![witness], 0).assured("bounded fixture"),
    ];
    let cycle = PotentialCycle::new(UNIX_EPOCH, edges, 0).assured("complete current cycle");
    let original = DeadlockEvidence::new(
        ProcessRecord {
            id: 42,
            program: Some(BoundedText::new("report-fixture")),
            started_at: UNIX_EPOCH,
            selection: DiagnosticSelection::OrderAnalysis,
        },
        vec![RecordedFinding::from(Finding::PotentialCycle(cycle))],
    )
    .assured("bounded fixture");
    let fixture = ReportFixture {
        directory: tempfile::tempdir().assured("disposable fixture"),
        original,
        exported: None,
    };
    std::fs::write(
        fixture.file("input.rkyv"),
        fixture.original.encode().assured("current fixture encodes"),
    )
    .assured("fixture is provisioned explicitly");
    world.deadlock_report = Some(fixture);
}

#[when("the engineer exports the potential findings")]
fn when_export_potential(world: &mut ScenarioWorld) {
    let fixture = world
        .deadlock_report
        .as_mut()
        .verified("a preceding step provisioned evidence");
    let output = fixture
        .command()
        .arg("inspect")
        .arg(fixture.file("input.rkyv"))
        .args(["--source", "potential", "--export"])
        .arg(fixture.file("export.rkyv"))
        .output()
        .assured("the report tool runs");
    assert!(output.status.success(), "{output:?}");
    fixture.exported = Some(output);
}

#[then("the export names the lock instances, acquisition modes and source sites")]
fn then_export_is_actionable(world: &mut ScenarioWorld) {
    let fixture = world.report_fixture();
    let output = fixture
        .exported
        .as_ref()
        .verified("a preceding step exported the evidence");
    let text = String::from_utf8_lossy(&output.stdout);
    for expected in [
        "potential lock-order",
        "lock 1",
        "lock 2",
        "exclusive",
        "fixture/owner.rs:20:2",
        "fixture/owner.rs:30:2",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    assert_eq!(
        fixture.read("export.rkyv"),
        fixture
            .original
            .selected(nervix_deadlock::FindingSelection::Potential)
    );
}

#[then("the diagnostic evidence does not qualify before triage")]
fn then_unreviewed_does_not_qualify(world: &mut ScenarioWorld) {
    let fixture = world.report_fixture();
    let output = fixture
        .command()
        .arg("qualify")
        .arg(fixture.file("input.rkyv"))
        .output()
        .assured("the report tool runs");
    assert_eq!(output.status.code(), Some(5), "{output:?}");
}

#[when("the engineer records a non-overlap proof and retained regression")]
fn when_review_non_overlap(world: &mut ScenarioWorld) {
    let fixture = world.report_fixture();
    let output = fixture
        .command()
        .arg("triage")
        .arg(fixture.file("input.rkyv"))
        .args([
            "--finding",
            "0",
            "--basis",
            "non-overlap",
            "--reason",
            "the owning operation serializes both paths",
            "--regression",
            "tests/features/cluster/deadlock_reports.feature: Export potential findings and \
             retain an explicit non-overlap review",
            "--output",
        ])
        .arg(fixture.file("reviewed.rkyv"))
        .output()
        .assured("the report tool runs");
    assert!(output.status.success(), "{output:?}");
}

#[then("the reviewed diagnostic evidence qualifies and retains the complete cycle")]
fn then_reviewed_qualifies(world: &mut ScenarioWorld) {
    let fixture = world.report_fixture();
    let output = fixture
        .command()
        .arg("qualify")
        .arg(fixture.file("reviewed.rkyv"))
        .output()
        .assured("the report tool runs");
    assert!(output.status.success(), "{output:?}");
    let reviewed = fixture.read("reviewed.rkyv");
    let [
        RecordedFinding::Potential {
            cycle: original, ..
        },
    ] = fixture.original.findings()
    else {
        panic!("the fixture has one potential cycle")
    };
    let [
        RecordedFinding::Potential {
            cycle: exported, ..
        },
    ] = reviewed.findings()
    else {
        panic!("the public review retains one potential cycle")
    };
    assert_eq!(exported, original);
}
