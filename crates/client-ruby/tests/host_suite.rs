//! Execute Ruby's prepared boundary suite as an application loads the native gem.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** A bounded Ruby child and exact boundary-inventory accounting.
//! - **Depends on.** The prepared gem, minitest sources and the existing suite result judge.
//! - **Must not know.** Binding implementation, cluster state or how to build an artifact.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "runs the prepared Ruby host-language boundary suite"
    )
)]

use std::{path::PathBuf, time::Duration};

use nervix_primitives::time::timeout;
use tokio::process::Command;

#[nervix_primitives::test]
async fn ruby_boundary_suite() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let artifacts = match std::env::var_os("NERVIX_CLIENT_RUBY_TEST_ARTIFACTS") {
        Some(path) => PathBuf::from(path),
        None => root.join("target/client-conformance"),
    };
    let mut ruby = Command::new("ruby");
    ruby.arg("-I")
        .arg(artifacts.join("ruby/lib"))
        .arg(root.join("tests/client_conformance/ruby/suite/runner.rb"))
        .arg("boundary")
        .env("NERVIX_CLIENT_CONFORMANCE_DIR", &artifacts)
        .kill_on_drop(true);
    let run = ruby.output();
    let output = match timeout(Duration::from_secs(300), run).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => panic!("starting the prepared Ruby suite failed: {error}"),
        Err(_) => panic!("the Ruby boundary suite exceeded its 300-second process budget"),
    };
    let directory = match tempfile::tempdir() {
        Ok(directory) => directory,
        Err(error) => panic!("creating the boundary result directory failed: {error}"),
    };
    let results = directory.path().join("results.txt");
    if let Err(error) = std::fs::write(&results, &output.stdout) {
        panic!("retaining the Ruby boundary results failed: {error}");
    }
    let mut judge = Command::new("python3");
    judge
        .args([
            "-m",
            "scripts.client_suites",
            "check-run",
            "--host",
            "ruby",
            "--tier",
            "boundary",
        ])
        .arg(&results)
        .current_dir(&root)
        .kill_on_drop(true);
    let judging = judge.output();
    let judged = match timeout(Duration::from_secs(30), judging).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => panic!("starting the boundary inventory judge failed: {error}"),
        Err(_) => panic!("the boundary inventory judge exceeded its 30-second process budget"),
    };
    assert!(
        output.status.success() && judged.status.success(),
        "Ruby status: {}; inventory status: {}\n{}\n{}\n{}",
        output.status,
        judged.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&judged.stderr),
    );
}
