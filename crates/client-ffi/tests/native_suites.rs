//! Outside the layer order: the native application suite harness.
//!
//! Owns bounded execution of the prepared C/C++ boundary programs and their inventory verdict.
//! Depends on the prepared host binaries, the shared library and the existing suite result judge.
//! Must not know what a host case asserts, compile a program or install a dependency.

use std::{path::PathBuf, time::Duration};

use nervix_primitives::{task::consume_budget, time::timeout};
use tokio::process::Command;

fn required_path(name: &str) -> PathBuf {
    let Some(path) = std::env::var_os(name) else {
        panic!("{name} is missing; run this suite through its just recipe");
    };
    PathBuf::from(path)
}

async fn boundary(host: &str) {
    let root = required_path("NERVIX_CLIENT_SUITE_ROOT");
    let artifacts = required_path("NERVIX_CLIENT_C_TEST_ARTIFACTS");
    for suffix in ["", "-sanitized"] {
        consume_budget().await;
        let executable = artifacts.join(format!("{host}-suite{suffix}"));
        assert!(
            executable.is_file(),
            "{} was not prepared",
            executable.display()
        );
        let mut command = Command::new(&executable);
        command
            .arg("boundary")
            .current_dir(&root)
            .kill_on_drop(true);
        let Ok(result) = timeout(Duration::from_secs(180), command.output()).await else {
            panic!(
                "{} exceeded its 180-second boundary budget",
                executable.display()
            );
        };
        let Ok(output) = result else {
            panic!("{} could not start: {result:?}", executable.display());
        };
        let Ok(results) = tempfile::NamedTempFile::new() else {
            panic!("could not create the boundary result file");
        };
        assert!(std::fs::write(results.path(), &output.stdout).is_ok());
        let mut judge = Command::new("python3");
        judge
            .args([
                "-m",
                "scripts.client_suites",
                "check-run",
                "--host",
                host,
                "--tier",
                "boundary",
            ])
            .arg(results.path())
            .current_dir(&root)
            .kill_on_drop(true);
        let Ok(judged) = timeout(Duration::from_secs(30), judge.output()).await else {
            panic!("the {host} inventory judge exceeded its 30-second budget");
        };
        let Ok(judged) = judged else {
            panic!("the {host} inventory judge could not start: {judged:?}");
        };
        assert!(
            output.status.success() && judged.status.success(),
            "{} failed\nstdout:\n{}\nstderr:\n{}\ninventory:\n{}",
            executable.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&judged.stderr),
        );
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }
}

#[nervix_primitives::test]
async fn c_application_suite() {
    boundary("c").await;
}

#[nervix_primitives::test]
async fn cpp_application_suite() {
    boundary("cpp").await;
}
