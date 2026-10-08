//! The CLI's final delivery of a downloaded archive to standard output.
//!
//! Layer: test harness.
//! - **Owns.** Standard outputs whose reader closed before the CLI wrote to them, staging
//!   directories the CLI cannot use, and the checks of the reports and kept archives those
//!   failures leave.
//! - **Depends on.** The public CLI, the harness's own session, and the archive format's reader.
//! - **Must not know.** How the CLI stages or copies an archive.

use std::process::Stdio;

use super::*;

/// The archive a failed delivery kept, as the CLI's report names it.
struct KeptArchive {
    reference: String,
    archive: String,
}

/// The arguments of a cluster backup to standard output, reported in `format`.
fn cluster_backup_to_standard_output(format: &str) -> Vec<String> {
    vec![
        "backup".to_string(),
        "cluster".to_string(),
        "--output".to_string(),
        "-".to_string(),
        "--format".to_string(),
        format.to_string(),
    ]
}

/// Runs `command` to its end, with standard output and standard error as the step set them.
async fn run_cli_with_streams(world: &mut ScenarioWorld, mut command: tokio::process::Command) {
    command.stdin(Stdio::null()).kill_on_drop(true);
    let child = command.spawn().assured("the CLI process starts");
    let output = nervix_primitives::time::timeout(CLI_BACKUP_TIMEOUT, child.wait_with_output())
        .await
        .assured("the CLI finishes within its budget")
        .assured("the CLI's exit and standard error are collected");
    world.last_cli_output = Some(output);
}

#[when(
    expr = "the CLI backs up the cluster from node {string} to a standard output whose reader has \
            closed, reporting {word}"
)]
async fn when_cli_backs_up_to_closed_standard_output(
    world: &mut ScenarioWorld,
    node: String,
    format: String,
) {
    // The CLI stages its archive under the scenario's directory, which removes what it keeps.
    let staging = archive_path(world, "cli-staging");
    std::fs::create_dir(&staging).assured("the scenario creates its CLI staging directory once");
    let (reader, writer) = std::io::pipe().assured("the harness creates a pipe");
    // Standard output has no reader before the CLI starts, so the CLI's first write of the
    // archive fails however long the backup and its download take.
    drop(reader);
    let mut command = cli_command(world, &node, cluster_backup_to_standard_output(&format));
    command
        .env("TMPDIR", &staging)
        .stdout(Stdio::from(writer))
        .stderr(Stdio::piped());
    run_cli_with_streams(world, command).await;
}

#[when(
    expr = "the CLI backs up the cluster from node {string} to standard output without a usable \
            staging directory, reporting {word}"
)]
async fn when_cli_backs_up_without_staging(
    world: &mut ScenarioWorld,
    node: String,
    format: String,
) {
    // Nothing creates this directory, so the CLI cannot create its staging directory inside it.
    let missing = archive_path(world, "missing-staging");
    let mut command = cli_command(world, &node, cluster_backup_to_standard_output(&format));
    command
        .env("TMPDIR", &missing)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    run_cli_with_streams(world, command).await;
}

/// The reference and kept archive a JSON `WRITE_FAILED` report names.
fn json_kept_archive(stderr: &str) -> KeptArchive {
    let line = stderr
        .lines()
        .next()
        .assured("the CLI reports on standard error");
    let report: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|error| {
        panic!("standard error starts with the JSON report ({error}): {stderr}")
    });
    assert_eq!(report["error"]["code"], "WRITE_FAILED", "{report}");
    let Some(reference) = report["error"]["execution_reference"].as_str() else {
        panic!("the report names the backup's execution reference: {report}");
    };
    let Some(archive) = report["error"]["archive"].as_str() else {
        panic!("the report names the archive the CLI kept: {report}");
    };
    KeptArchive {
        reference: reference.to_string(),
        archive: archive.to_string(),
    }
}

/// The reference and kept archive a text report's recovery line names.
fn text_kept_archive(stderr: &str) -> KeptArchive {
    let mut lines = stderr.lines();
    let error = lines.next().assured("the CLI reports on standard error");
    assert!(
        error.starts_with("error: the backup archive could not be written to standard output"),
        "{stderr}"
    );
    let recovery = lines
        .next()
        .assured("the error line is followed by its recovery");
    let Some(named) = recovery.strip_prefix("recover backup ") else {
        panic!("the report says how to recover the backup: {stderr}");
    };
    let Some((reference, rest)) = named.split_once(" from its verified archive at '") else {
        panic!("the recovery names the backup's reference and its kept archive: {stderr}");
    };
    let Some((archive, _)) = rest.split_once("'; ") else {
        panic!("the recovery quotes the kept archive's path: {stderr}");
    };
    KeptArchive {
        reference: reference.to_string(),
        archive: archive.to_string(),
    }
}

#[then(
    expr = "the CLI's {word} report names the undelivered archive's execution reference and kept \
            copy"
)]
fn then_report_names_reference_and_kept_copy(world: &mut ScenarioWorld, format: String) {
    let output = last_cli_output(world);
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    // A Rust program ignores SIGPIPE, so the closed reader fails the CLI's write instead of ending
    // the process: the CLI reports the failure itself and exits with the status of an error.
    assert_eq!(
        output.status.code(),
        Some(1),
        "the CLI exits after reporting its failure: status {}; stderr: {stderr}",
        output.status
    );
    let kept = match format.as_str() {
        "json" => json_kept_archive(&stderr),
        "text" => text_kept_archive(&stderr),
        other => panic!("the scenario names a report format the CLI prints, not '{other}'"),
    };
    CommandExecutionReference::parse(&kept.reference)
        .assured("the report names a valid execution reference");
    world
        .placeholders
        .insert("backup_reference".to_string(), kept.reference);
    world
        .placeholders
        .insert("kept_archive".to_string(), kept.archive);
}

#[when(expr = "the kept archive is moved to {string}")]
fn when_kept_archive_is_moved(world: &mut ScenarioWorld, file: String) {
    let kept = PathBuf::from(&world.placeholders["kept_archive"]);
    let working = archive_path(world, &file);
    std::fs::rename(&kept, &working).assured("the CLI keeps the archive it did not deliver");
}

#[then(
    expr = "backup archive {string} is the one its execution reference recovers, with the same \
            summary and cuts and no new capture"
)]
async fn then_archive_is_the_recovered_backup(world: &mut ScenarioWorld, file: String) {
    let reference = world.placeholders["backup_reference"].clone();
    let archive = archive_path(world, &file);
    let bytes = std::fs::read(&archive).assured("the scenario delivered the kept archive");
    let description = nervix_backup::describe_archive(bytes.as_slice())
        .assured("the kept archive verifies completely");
    // The CLI's backup to another destination: recovery binds what a backup captures, not where
    // its archive goes.
    let recovery = nervix_models::Backup {
        scope: nervix_models::BackupScope::Cluster,
        destination: archive_path(world, "recovered.nvxb").display().to_string(),
        resources: nervix_models::BackupResources::Included,
        capture: nervix_models::BackupCapture::Quiesced { timeout: None },
    };
    let leader = current_leader_node(world).await;
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .assured("the recovery session opens");
    let recovered = session
        .run_command_result_with_reference(&recovery.to_canonical_nspl(), &reference)
        .await
        .assured("the recovery is answered");
    assert_eq!(
        recovered.origin,
        nervix_client_core::OutcomeOrigin::Recovered,
        "the reference recovers the recorded backup instead of capturing again: {}",
        recovered.message
    );
    assert_eq!(recovered.execution_reference.as_str(), reference);
    let summary = recovered
        .backup
        .assured("the recorded outcome keeps the summary of its archive");
    let length: u64 = bytes.len().arch_into();
    assert_eq!(summary.total_bytes.get(), length);
    assert_eq!(
        summary.digest.to_string(),
        blake3::hash(&bytes).to_hex().as_str(),
        "the kept archive is the one the backup assembled"
    );
    assert_eq!(summary.domains.len(), description.domains.len());
    for domain in &summary.domains {
        let described = description
            .domains
            .iter()
            .find(|entry| entry.record.domain == domain.domain)
            .assured("the kept archive holds every domain the summary names");
        assert_eq!(described.capture.revision, domain.revision);
        assert_eq!(described.capture.cut, domain.cut);
    }
    let started = scenario_domain(world, "{{domain}}");
    let started = summary
        .domains
        .iter()
        .find(|entry| entry.domain == started)
        .assured("the backup covers the scenario's domain");
    assert!(
        matches!(started.cut, nervix_models::BackupCut::Quiesced { .. }),
        "the running domain was captured at a quiesced cut: {:?}",
        started.cut
    );
}

#[then(expr = "the CLI's {word} report of the staging failure names no execution reference")]
fn then_staging_failure_names_no_reference(world: &mut ScenarioWorld, format: String) {
    let output = last_cli_output(world);
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(
        output.status.code(),
        Some(1),
        "the CLI exits after reporting its failure: status {}; stderr: {stderr}",
        output.status
    );
    assert!(
        output.stdout.is_empty(),
        "standard output is kept for the archive alone: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let first = stderr
        .lines()
        .next()
        .assured("the CLI reports on standard error");
    match format.as_str() {
        "json" => {
            let report: serde_json::Value = serde_json::from_str(first).unwrap_or_else(|error| {
                panic!("standard error starts with the JSON report ({error}): {stderr}")
            });
            assert_eq!(report["error"]["code"], "WRITE_FAILED", "{report}");
            assert!(
                report["error"].get("execution_reference").is_none(),
                "no backup was admitted, so none has a reference: {report}"
            );
        }
        "text" => {
            assert!(
                first.starts_with("error: the backup archive could not be staged"),
                "{stderr}"
            );
            assert!(
                !stderr.lines().any(|line| line.starts_with("recover ")),
                "no backup was admitted, so there is nothing to recover: {stderr}"
            );
        }
        other => panic!("the scenario names a report format the CLI prints, not '{other}'"),
    }
}
