//! Repository process-test harness, outside the product layer order.
//! Owns: wrapper descriptor, exit, signal, parent-death and admission-lifetime witnesses.
//! Depends on: the production binary and bounded scripted child processes.
//! Must not know: real compiler behavior, product execution or host-load assumptions.

use std::{
    collections::BTreeMap,
    fs,
    io::Write as _,
    os::unix::{fs::PermissionsExt as _, process::ExitStatusExt as _},
    path::Path,
    process::{Child, Command, Output, Stdio},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{thread::sleep, time::Instant};
use nervix_ratchet::{
    admission::{Compiled, GIB, Policy},
    evidence::{Compilation, Ending},
};
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use tempfile::TempDir;

const DRIVER: &str = r#"#!/usr/bin/python3
import json, os, pathlib, resource, signal, sys
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
for number in (signal.SIGHUP, signal.SIGINT, signal.SIGQUIT, signal.SIGTERM):
    signal.signal(number, signal.SIG_DFL)
root = pathlib.Path(os.environ['FIXTURE_ROOT'])
(root / 'arguments.json').write_text(json.dumps(sys.argv[1:]))
temporary = root / 'started.tmp'
temporary.write_text(str(os.getpid()) + '\n')
temporary.replace(root / 'started')
if os.environ.get('FIXTURE_ACTION') == 'block':
    sys.stdin.buffer.read(1)
if os.environ.get('FIXTURE_ACTION') == 'descriptor':
    os.write(int(os.environ['FIXTURE_FD']), b'jobserver-inherited\n')
sys.exit(int(os.environ.get('FIXTURE_STATUS', '0')))
"#;

#[test]
fn an_unwritable_compilation_record_keeps_the_drivers_observed_ending() {
    let fixture = Fixture::new(false);
    let path = fixture.directory.path().join("unwritable-reports");
    fs::write(&path, "a file cannot contain compilation records")
        .assured("the publication failure fixture can be written");
    let output = fixture
        .command()
        .env("NERVIX_LINT_REPORTS", path)
        .env("FIXTURE_STATUS", "37")
        .output()
        .assured("the wrapper process can run");
    assert_eq!(output.status.code(), Some(37));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("cannot write the compilation record")
    );
    assert!(!fixture.policy.directory.exists());
}

struct Fixture {
    directory: TempDir,
    policy: Policy,
}

impl Fixture {
    fn new(reserved: bool) -> Self {
        let directory = tempfile::tempdir().assured("the fixture can create a private directory");
        let driver = directory.path().join("driver");
        fs::write(&driver, DRIVER).assured("the scripted fixture driver can be written");
        fs::set_permissions(&driver, fs::Permissions::from_mode(0o700))
            .assured("the fixture driver can be made executable");
        let mut reservations = BTreeMap::new();
        if reserved {
            reservations.insert(
                Compiled {
                    crate_name: "example".into(),
                    kind: "lib".into(),
                },
                GIB,
            );
        }
        let policy = Policy {
            directory: directory.path().join("queue"),
            threshold_bytes: 0,
            wait_limit_seconds: 1.0,
            reservations,
        };
        Self { directory, policy }
    }

    fn command(&self) -> Command {
        let root = self.directory.path();
        let mut command = Command::new(env!("CARGO_BIN_EXE_nervix-ratchet"));
        command
            .args([
                "/rustc",
                "--crate-name",
                "example",
                "src/lib.rs",
                "--crate-type=lib",
                "-Cextra-filename=-abcd",
            ])
            .current_dir(root)
            .env("NERVIX_LINT_DRIVER", root.join("driver"))
            .env("NERVIX_LINT_REPORTS", root.join("reports"))
            .env(
                "NERVIX_LINT_ADMISSION",
                serde_json::to_string(&self.policy).assured("the current policy serializes"),
            )
            .env("NERVIX_LINT_CONFIGURATION", "testing")
            .env("NERVIX_LINT_ROOT", root)
            .env("FIXTURE_ROOT", root)
            .env_remove("NERVIX_NATIVE_COVERAGE_ATTEMPT");
        command
    }

    fn record(&self) -> Compilation {
        nervix_ratchet::io::read(
            &self
                .directory
                .path()
                .join("reports/compilations/example-abcd.json"),
        )
        .assured("the wrapper published its current compilation record")
    }

    fn started(&self, child: &mut Child) -> i32 {
        let path = self.directory.path().join("started");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Ok(record) = fs::read_to_string(&path)
                && record.ends_with('\n')
            {
                return record
                    .trim()
                    .parse()
                    .assured("the whole readiness record contains the driver PID");
            }
            assert!(
                child
                    .try_wait()
                    .assured("the wrapper's status is observable")
                    .is_none(),
                "the wrapper ended before the driver was ready"
            );
            assert!(
                Instant::now() < deadline,
                "the driver never published a complete readiness record"
            );
            sleep(Duration::from_millis(10));
        }
    }
}

#[test]
fn native_collection_records_the_cargo_wrapper_and_its_driver_once_each() {
    let fixture = Fixture::new(false);
    let root = fixture.directory.path();
    fs::create_dir_all(root.join("scripts"))
        .assured("the collector fixture has its script directory");
    fs::write(
        root.join("scripts/native_coverage.py"),
        r#"import json, os, pathlib, sys
root = pathlib.Path(os.environ['FIXTURE_ROOT'])
assert sys.argv[1] == 'exec'
with (root / 'collected.jsonl').open('a') as log:
    log.write(json.dumps(sys.argv[2:]) + '\n')
os.execvpe(sys.argv[2], sys.argv[2:], os.environ)
"#,
    )
    .assured("the exec-only collector fixture can be written");
    let output = fixture
        .command()
        .env("NERVIX_NATIVE_COVERAGE_ATTEMPT", "current-attempt")
        .env("NERVIX_RATCHET_ROOT", root)
        .env("FIXTURE_STATUS", "23")
        .output()
        .assured("the Cargo wrapper completes through the collector");
    assert_eq!(output.status.code(), Some(23), "{output:?}");
    let text = fs::read_to_string(root.join("collected.jsonl"))
        .assured("the collector recorded the wrapper and driver");
    let collected: Vec<Vec<String>> = text
        .lines()
        .map(|line| {
            serde_json::from_str(line).assured("the collector writes a complete argument list")
        })
        .collect();
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0][0], env!("CARGO_BIN_EXE_nervix-ratchet"));
    assert_eq!(collected[0][1], "wrap");
    assert_eq!(collected[0][2], "/rustc");
    assert_eq!(collected[1][0], root.join("driver").display().to_string());
    assert_eq!(
        &collected[0][2..],
        &collected[1][1..],
        "collection preserves every driver argument"
    );
    assert_eq!(fixture.record().ending, Ending::Exit { status: 23 });
}

fn wait(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().assured("the child status is observable") {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().assured("the timed-out child is still running");
            child.wait().assured("the killed child can be reaped");
            panic!("the wrapper did not end within its process-test budget");
        }
        sleep(Duration::from_millis(10));
    }
}

#[test]
fn every_driver_exit_keeps_arguments_record_and_status() {
    for status in [0, 37] {
        let fixture = Fixture::new(false);
        let output = fixture
            .command()
            .env("FIXTURE_STATUS", status.to_string())
            .output()
            .assured("the wrapper starts");
        assert_eq!(
            output.status.code(),
            Some(status),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fixture.record().ending, Ending::Exit { status });
        let arguments: Vec<String> =
            nervix_ratchet::io::read(&fixture.directory.path().join("arguments.json"))
                .assured("the driver recorded its arguments");
        assert_eq!(
            arguments,
            [
                "/rustc",
                "--crate-name",
                "example",
                "src/lib.rs",
                "--crate-type=lib",
                "-Cextra-filename=-abcd"
            ]
        );
        assert_eq!(
            fixture.record().source.as_deref(),
            Some(Path::new("src/lib.rs"))
        );
        assert!(fixture.record().admission.is_none());
    }
}

#[test]
fn the_driver_inherits_every_non_cloexec_descriptor() {
    use std::{
        io::Read as _,
        os::fd::{AsRawFd as _, FromRawFd as _},
    };
    let fixture = Fixture::new(false);
    let (reading, writing) = nix::unistd::pipe().assured("the descriptor fixture creates its pipe");
    let output = fixture
        .command()
        .env("FIXTURE_ACTION", "descriptor")
        .env("FIXTURE_FD", writing.as_raw_fd().to_string())
        .output()
        .assured("the descriptor wrapper starts");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    drop(writing);
    use std::os::fd::IntoRawFd as _;
    // SAFETY: ownership of this unique descriptor moves from OwnedFd into File exactly once.
    let mut reading = unsafe { fs::File::from_raw_fd(reading.into_raw_fd()) };
    let mut bytes = Vec::new();
    reading
        .read_to_end(&mut bytes)
        .assured("the fixture's writer has ended");
    assert_eq!(bytes, b"jobserver-inherited\n");
}

#[test]
fn forwarding_each_supported_signal_keeps_the_exact_child_ending() {
    for signal in [
        Signal::SIGHUP,
        Signal::SIGINT,
        Signal::SIGQUIT,
        Signal::SIGTERM,
    ] {
        let fixture = Fixture::new(false);
        let mut child = fixture
            .command()
            .env("FIXTURE_ACTION", "block")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .assured("the wrapper starts");
        fixture.started(&mut child);
        kill(
            Pid::from_raw(i32::try_from(child.id()).assured("a child PID fits pid_t")),
            signal,
        )
        .assured("the running wrapper receives its signal");
        let status = wait(&mut child);
        let number = libc_signal(signal);
        assert_eq!(status.signal(), Some(number));
        assert_eq!(
            fixture.record().ending,
            Ending::Signal {
                signal: number,
                name: Some(signal.to_string())
            }
        );
    }
}

fn libc_signal(signal: Signal) -> i32 {
    match signal {
        Signal::SIGHUP => libc::SIGHUP,
        Signal::SIGINT => libc::SIGINT,
        Signal::SIGQUIT => libc::SIGQUIT,
        Signal::SIGTERM => libc::SIGTERM,
        _ => unreachable!(),
    }
}

#[test]
fn a_declared_compilation_holds_its_place_until_the_driver_ends() {
    let fixture = Fixture::new(true);
    let mut child = fixture
        .command()
        .env("FIXTURE_ACTION", "block")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .assured("the reserved wrapper starts");
    let pid = fixture.started(&mut child);
    let queue = nervix_ratchet::admission_queue::Queue::open(&fixture.policy.directory)
        .assured("the shared queue can be opened");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let locked = queue
            .deciding(deadline)
            .assured("the queue decision can be locked");
        let leases = queue.live().assured("the admitted lease is readable");
        assert_eq!(leases.len(), 1);
        assert!(leases[0].admitted_ns.is_some());
        let named = leases[0].driver_pid == Some(u32::try_from(pid).assured("the PID is positive"));
        drop(locked);
        if named {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the wrapper never named its running driver"
        );
        sleep(Duration::from_millis(10));
    }
    child
        .stdin
        .as_mut()
        .assured("the fixture owns the driver's lifetime input")
        .write_all(b"x")
        .assured("the running driver can be released");
    assert!(wait(&mut child).success());
    assert!(
        queue
            .live()
            .assured("the released lease is reclaimed")
            .is_empty()
    );
    assert!(fixture.record().admission.is_some());
}

#[test]
fn a_driver_that_cannot_start_releases_its_reserved_place() {
    let fixture = Fixture::new(true);
    fs::remove_file(fixture.directory.path().join("driver"))
        .assured("the fixture removes its own driver");
    let output = fixture
        .command()
        .output()
        .assured("the wrapper starts even though its driver cannot");
    assert!(!output.status.success());
    let queue = nervix_ratchet::admission_queue::Queue::open(&fixture.policy.directory)
        .assured("the queue remains usable");
    assert!(
        queue
            .live()
            .assured("the failed driver's place is released")
            .is_empty()
    );
}

#[cfg(target_os = "linux")]
#[test]
fn the_driver_dies_with_its_wrapper_and_is_reaped_by_its_test_supervisor() {
    let fixture = Fixture::new(false);
    let supervisor = r#"
import ctypes, json, os, pathlib, signal, subprocess, sys, time
assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
root = pathlib.Path(os.environ['FIXTURE_ROOT'])
driver = None
wrapper = subprocess.Popen(sys.argv[1:], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    deadline = time.monotonic() + 60
    while True:
        try:
            text = (root / 'started').read_text()
        except FileNotFoundError:
            text = ''
        if text.endswith('\n'):
            driver = int(text)
            break
        assert wrapper.poll() is None and time.monotonic() < deadline
        time.sleep(.01)
    wrapper.kill()
    assert wrapper.wait(timeout=60) == -signal.SIGKILL
    while True:
        found, status = os.waitpid(driver, os.WNOHANG)
        if found:
            assert os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGKILL
            driver = None
            break
        assert time.monotonic() < deadline
        time.sleep(.01)
    print(json.dumps({'wrapper_killed': True, 'driver_reaped': True}))
finally:
    if wrapper.poll() is None:
        wrapper.kill()
        wrapper.wait(timeout=60)
    if driver is not None:
        try:
            os.kill(driver, signal.SIGKILL)
            os.waitpid(driver, 0)
        except ProcessLookupError:
            pass
"#;
    let wrapped = fixture.command();
    let mut command = Command::new("python3");
    command
        .arg("-c")
        .arg(supervisor)
        .arg(wrapped.get_program())
        .args(wrapped.get_args())
        .envs(
            wrapped
                .get_envs()
                .filter_map(|(key, value)| value.map(|value| (key, value))),
        )
        .env("FIXTURE_ACTION", "block");
    command.env_remove("NERVIX_NATIVE_COVERAGE_ATTEMPT");
    let Output {
        status,
        stdout,
        stderr,
    } = command
        .output()
        .assured("the isolated process supervisor starts");
    assert!(status.success(), "{}", String::from_utf8_lossy(&stderr));
    let result: serde_json::Value =
        serde_json::from_slice(&stdout).assured("the supervisor reports complete cleanup");
    assert_eq!(
        result,
        serde_json::json!({"wrapper_killed": true, "driver_reaped": true})
    );
}
