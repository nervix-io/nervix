//! Repository test harness, outside the product layer order.
//! Owns: compiler option, record requirement and cleanup witnesses.
//! Depends on: production wrapper owners and private child processes.
//! Must not know: a real compiler or shared environment mutation.

use super::*;

#[test]
fn every_compiler_option_spelling_preserves_the_compiled_entity() {
    for arguments in [
        vec![
            "--crate-name",
            "example",
            "--crate-type",
            "lib,rlib",
            "-C",
            "extra-filename=-abcd",
            "src/lib.rs",
        ],
        vec![
            "--crate-name=example",
            "--crate-type=lib",
            "--crate-type=rlib",
            "-Cextra-filename=-abcd",
            "src/lib.rs",
        ],
    ] {
        let arguments: Vec<_> = arguments.into_iter().map(OsString::from).collect();
        let compiled = CompilationArguments::parse(&arguments);
        assert_eq!(compiled.crate_name.as_deref(), Some("example"));
        assert_eq!(compiled.crate_types, ["lib", "rlib"]);
        assert_eq!(compiled.extra_filename, "-abcd");
        assert_eq!(compiled.source, Some("src/lib.rs".into()));
    }
}

#[test]
fn ending_keeps_named_and_unnamed_signals_and_exit_statuses() {
    assert_eq!(
        ending(ExitStatus::from_raw(37 << 8)),
        Ending::Exit { status: 37 }
    );
    assert_eq!(
        ending(ExitStatus::from_raw(9)),
        Ending::Signal {
            signal: 9,
            name: Some("SIGKILL".into())
        }
    );
    assert_eq!(
        ending(ExitStatus::from_raw(64)),
        Ending::Signal {
            signal: 64,
            name: None
        }
    );
}

#[test]
fn records_require_both_a_report_directory_and_a_compiled_crate() {
    let mut record = Compilation {
        crate_name: Some("example".into()),
        crate_types: vec!["lib".into()],
        extra_filename: "-abcd".into(),
        source: Some("src/lib.rs".into()),
        ending: Ending::Exit { status: 0 },
        peak_resident_bytes: 1,
        started_unix_ns: 1,
        seconds: 0.1,
        oom_kills: None,
        admission: None,
    };
    assert!(
        publish_in(None, &record)
            .assured("no report directory is intentionally unrecorded")
            .is_none()
    );
    assert!(
        publish_in(Some(OsStr::new("")), &record)
            .assured("an empty report directory is intentionally unrecorded")
            .is_none()
    );
    record.crate_name = None;
    let directory = tempfile::tempdir().assured("the record fixture can create its directory");
    assert!(
        publish_in(Some(directory.path().as_os_str()), &record)
            .assured("an unnamed compiler query is intentionally unrecorded")
            .is_none()
    );
    record.crate_name = Some("example".into());
    let path = publish_in(Some(directory.path().as_os_str()), &record)
        .assured("a named compilation publishes its record")
        .assured("the record has its destination");
    assert_eq!(
        io::read::<Compilation>(&path).assured("the current record decodes"),
        record
    );
}

#[test]
fn wrapper_cleanup_reaps_a_driver_before_releasing_earlier_owned_state() {
    let child = Command::new("sleep")
        .arg("30")
        .spawn()
        .assured("the bounded isolated driver can start");
    let pid = Pid::from_raw(i32::try_from(child.id()).assured("the child ID fits pid_t"));
    let driver = Driver(child);
    drop(driver);
    assert_eq!(signal::kill(pid, None), Err(nix::errno::Errno::ESRCH));
}
