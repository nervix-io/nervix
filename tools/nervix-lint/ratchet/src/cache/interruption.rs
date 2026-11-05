//! Repository qualification tooling, outside the product layer order.
//! Owns: an isolated analysis process group's interruption and recovery probe.
//! Depends on: operating-system child lifetimes and production completion evidence.
//! Must not know: product task cancellation or graph execution.

use std::{
    fs::{self, File},
    os::unix::process::CommandExt as _,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::Duration,
};

use error_stack::ResultExt as _;
use meticulous::ResultExt as _;
use nervix_primitives::{thread::sleep, time::Instant};
use nix::{
    errno::Errno,
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use serde::Serialize;

use super::Problem;
use crate::{
    configuration::Configuration,
    error::{Error, Result},
    evidence::Completion,
    runner::Runner,
};

#[derive(Serialize)]
pub struct Interruption {
    interrupted_output_absent: bool,
    resumed: Completion,
    log: PathBuf,
}

fn signal(child: &Child, signal: Signal) -> Result<()> {
    let id = i32::try_from(child.id()).assured("Linux child process IDs fit pid_t");
    match killpg(Pid::from_raw(id), signal) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(error) => Err(error_stack::Report::new(error).change_context(Error::Qualification)),
    }
}

fn interrupt(child: &mut Child, log: &Path) -> Result<ExitStatus> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let text = fs::read_to_string(log).change_context(Error::Qualification)?;
        if text.contains("typed ratchet: fixture-ordinary: cargo") {
            break;
        }
        Problem::InterruptionReadiness.require(
            child
                .try_wait()
                .change_context(Error::Qualification)?
                .is_none()
                && Instant::now() < deadline,
        )?;
        sleep(Duration::from_millis(10));
    }
    signal(child, Signal::SIGTERM)?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().change_context(Error::Qualification)? {
            return Ok(status);
        }
        Problem::InterruptionReadiness.require(Instant::now() < deadline)?;
        sleep(Duration::from_millis(10));
    }
}

pub fn qualify(root: &Path, target: &Path, directory: &Path) -> Result<Interruption> {
    let output = directory.join("interrupted.json");
    let log = directory.join("interrupted.log");
    let runner = Runner::new(root, target, false)?;
    let stream = File::create(&log).change_context(Error::Qualification)?;
    let error_stream = stream.try_clone().change_context(Error::Qualification)?;
    let mut child = Command::new(&runner.wrapper)
        .arg("analyze")
        .arg("--root")
        .arg(root)
        .arg("--target-dir")
        .arg(target)
        .args(["--fixture-mode", "ordinary", "--recompile", "--output"])
        .arg(&output)
        .current_dir(root)
        .envs(&runner.environment)
        .stdout(Stdio::from(stream))
        .stderr(Stdio::from(error_stream))
        .process_group(0)
        .spawn()
        .change_context(Error::Qualification)?;
    let ending = interrupt(&mut child, &log);
    if ending.is_err() {
        signal(&child, Signal::SIGKILL)?;
        child.wait().change_context(Error::Qualification)?;
    }
    Problem::InterruptedEvidence.require(!ending?.success())?;
    let configuration = Configuration::fixture("ordinary");
    Problem::InterruptedEvidence.require(
        !output.exists()
            && !runner
                .work
                .join(&configuration.name)
                .join("completion.json")
                .exists(),
    )?;
    let resumed = runner.analyze_one(&configuration, true)?;
    Problem::InterruptedEvidence.require(resumed.complete)?;
    Ok(Interruption {
        interrupted_output_absent: true,
        resumed,
        log,
    })
}
