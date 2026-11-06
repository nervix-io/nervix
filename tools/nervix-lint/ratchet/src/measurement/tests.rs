//! Repository test harness, outside the product layer order.
//! Owns: exact compilation failure and admission reporting witnesses.
//! Depends on: production formatting and current serialized records.
//! Must not know: real compiler outcomes or host load.

use super::*;
use crate::{
    admission::AdmissionRecord,
    evidence::{CargoTarget, Diagnostic},
    io,
};

fn record() -> Compilation {
    Compilation {
        crate_name: Some("example".into()),
        crate_types: vec!["lib".into()],
        extra_filename: "-abcd".into(),
        source: Some("src/lib.rs".into()),
        ending: Ending::Signal {
            signal: 9,
            name: Some("SIGKILL".into()),
        },
        peak_resident_bytes: 14 * GIB,
        started_unix_ns: 1,
        seconds: 412.0,
        oom_kills: Some(1),
        admission: None,
    }
}

#[test]
fn failures_distinguish_signals_compiler_diagnostics_driver_and_outer_failures() {
    let directory = tempfile::tempdir().assured("the measurement fixture can create its directory");
    let path = directory.path().join("compilations/example-abcd.json");
    let mut record = record();
    io::atomic_json(&path, &record).assured("the current failure record serializes");
    let lines = failure_lines(&[], directory.path()).assured("the failure records are readable");
    assert_eq!(
        lines,
        [
            "example (lib): the analysis compiler was terminated by signal 9 (SIGKILL) after 412s \
             at a peak resident size of 14.0 GiB; the kernel's out-of-memory killer recorded 1 \
             kill in its cgroup while it ran; it wrote no compiler report"
        ]
    );
    record.ending = Ending::Signal {
        signal: 15,
        name: None,
    };
    record.crate_types.clear();
    record.oom_kills = Some(0);
    io::atomic_json(&path, &record).assured("the unnamed signal record serializes");
    assert_eq!(
        failure_lines(&[], directory.path()).assured("the unnamed signal is readable"),
        [
            "example (unknown): the analysis compiler was terminated by signal 15 (unnamed) after \
             412s at a peak resident size of 14.0 GiB; it wrote no compiler report"
        ]
    );
    record.crate_types = vec!["lib".into()];
    record.ending = Ending::Exit { status: 1 };
    io::atomic_json(&path, &record).assured("the exit record serializes");
    let error = CargoMessage::CompilerMessage {
        target: CargoTarget {
            name: "example".into(),
            kind: vec!["lib".into()],
            src_path: "src/lib.rs".into(),
        },
        message: Diagnostic {
            level: "error".into(),
            code: None,
            message: "mismatched types".into(),
            rendered: None,
        },
    };
    assert_eq!(
        failure_lines(&[error], directory.path()).assured("the compiler error is counted"),
        ["example (lib): the analysis compiler failed with 1 compiler error (exit status 1)"]
    );
    assert!(
        failure_lines(&[], directory.path()).assured("the driver failure is identified")[0]
            .contains("without a compiler error, so the analysis driver itself failed")
    );
    record.ending = Ending::Exit { status: 0 };
    io::atomic_json(&path, &record).assured("the successful record serializes");
    assert!(
        failure_lines(&[], directory.path()).assured("outer failure attribution remains")[0]
            .starts_with("no analysis compilation ended unsuccessfully")
    );
}

#[test]
fn operational_lines_name_the_largest_compilation_and_every_wait_or_nonfit() {
    let mut measured = BTreeMap::new();
    for (index, outcome, waited, peak) in [
        (0, AdmissionOutcome::Fit, 0.0, 2 * GIB),
        (1, AdmissionOutcome::Fit, 3.0, 3 * GIB),
        (2, AdmissionOutcome::Alone, 0.0, 5 * GIB),
        (3, AdmissionOutcome::WaitLimit, 60.0, 4 * GIB),
        (4, AdmissionOutcome::QueueLocked, 60.0, GIB),
        (5, AdmissionOutcome::Unavailable, 0.0, GIB),
    ] {
        let name = format!("example_{index}");
        measured.insert(
            name.clone(),
            Measurement {
                crate_name: name,
                kind: "lib".into(),
                peak_resident_bytes: peak,
                started_unix_ns: index,
                seconds: 10.0,
                admission: Some(AdmissionRecord {
                    reservation_bytes: 8 * GIB,
                    outcome,
                    waited_seconds: waited,
                    left_bytes: Some(-1),
                    holders: 1,
                    error: None,
                }),
            },
        );
    }
    let output = lines(&Configuration::fixture("ordinary"), &measured);
    assert_eq!(output.len(), 6);
    assert!(output[0].contains("example_2 (lib), peaked at 5.0 GiB in 10 s"));
    assert!(!output.iter().any(|line| line.contains("example_0")));
    for outcome in ["fit", "alone", "wait_limit", "queue_locked", "unavailable"] {
        assert!(
            output
                .iter()
                .any(|line| line.contains(&format!("({outcome})")))
        );
    }
}
