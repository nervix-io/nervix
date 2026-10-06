//! Public backup waits across independently bounded domain cuts.
//!
//! Layer: test harness.
//! - **Owns.** Delayed-cut workloads and assertions on command recovery and downloaded archives.
//! - **Depends on.** The public Rust client, CLI, archive reader and controlled capture gates.
//! - **Must not know.** Command storage, mutation leases or private checkpoint representations.

use super::*;

#[when(expr = "backup cut {string} is held for {int} seconds and then released")]
async fn when_backup_cut_is_delayed(world: &mut ScenarioWorld, raw_domain: String, seconds: u64) {
    let domain = scenario_domain(world, &raw_domain);
    nervix_primitives::time::timeout(
        Duration::from_secs(30),
        world.fault_injection.wait_for_backup_cut_pause(&domain),
    )
    .await
    .assured("the backup reaches the controlled cut");
    // Workload duration after the observed gate; readiness is established by the notification.
    nervix_primitives::time::sleep(Duration::from_secs(seconds)).await;
    world.fault_injection.release_backup_cut_pause(&domain);
}

#[then("the CLI backup wait failure exposes its execution reference for recovery")]
fn then_cli_wait_exposes_reference(world: &mut ScenarioWorld) {
    let output = last_cli_output(world);
    assert!(
        !output.status.success(),
        "an unanswered backup ends with failure"
    );
    let report = cli_json(output);
    assert_eq!(report["error"]["code"], "BACKUP_FAILED");
    let reference = report["error"]["execution_reference"]
        .as_str()
        .assured("uncertainty reports the durable reference as structured data");
    let reference = CommandExecutionReference::parse(reference).assured("the reference is valid");
    world
        .placeholders
        .insert("backup_reference".to_string(), reference.to_string());
}

#[then("the recovered CLI backup keeps its execution reference and both quiesced cuts")]
fn then_cli_recovery_keeps_identity_and_cuts(world: &mut ScenarioWorld) {
    let report = cli_json(last_cli_output(world));
    assert_eq!(
        report["execution_reference"],
        world.placeholders["backup_reference"]
    );
    let path = report["output"]
        .as_str()
        .assured("the report names the verified archive");
    let description =
        nervix_backup::describe_archive(std::fs::File::open(path).assured("the archive exists"))
            .assured("the recovered archive verifies");
    for suffix in ["_a", "_b"] {
        let domain = scenario_domain(world, &format!("{{{{backup_base}}}}{suffix}"));
        let captured = description
            .domains
            .iter()
            .find(|entry| entry.record.domain == domain)
            .assured("the archive contains each requested domain");
        let nervix_models::BackupCut::Quiesced {
            engaged_at,
            released_at,
            ..
        } = &captured.capture.cut
        else {
            panic!("each running domain retains its quiesced cut");
        };
        if suffix == "_a" {
            let elapsed = released_at
                .duration_since(*engaged_at)
                .assured("the retained cut release follows engagement");
            assert!(
                elapsed >= Duration::from_secs(2),
                "recovery keeps the completed first cut"
            );
        }
    }
}

#[when("a completed cluster backup is recovered with a different local destination")]
async fn when_backup_is_recovered_to_another_destination(world: &mut ScenarioWorld) {
    let leader = current_leader_node(world).await;
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .assured("the backup session opens");
    let reference = crate::common::raw_session::fresh_execution_reference();
    let initial = session
        .run_command_result_with_reference(
            "BACKUP CLUSTER TO 'first.nvxb' WITHOUT STATE;",
            &reference,
        )
        .await
        .assured("the first backup is answered");
    assert!(initial.backup.is_some(), "{}", initial.message);
    let recovered = session
        .run_command_result_with_reference(
            "BACKUP CLUSTER TO 'recovered.nvxb' WITHOUT STATE;",
            &reference,
        )
        .await
        .assured("the recovery is answered");
    assert_eq!(
        recovered.origin,
        nervix_client_core::OutcomeOrigin::Recovered,
        "{}",
        recovered.message
    );
    assert_eq!(recovered.backup, initial.backup);
    assert_eq!(recovered.execution_reference, initial.execution_reference);
    world.last_backup = Some(TestBackup {
        reference: recovered.execution_reference,
        summary: *recovered
            .backup
            .assured("the retained outcome has its summary"),
    });
}

#[then("the CLI recovers that archive to stdout with the same execution reference")]
async fn then_cli_recovers_archive_to_stdout(world: &mut ScenarioWorld) {
    let backup = last_backup(world).clone();
    let leader = current_leader_node(world).await;
    run_cli(
        world,
        &leader,
        vec![
            "backup".to_string(),
            "cluster".to_string(),
            "--without-state".to_string(),
            "--execution-reference".to_string(),
            backup.reference.to_string(),
            "--output".to_string(),
            "-".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ],
    )
    .await;
    let output = last_cli_output(world);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let description = nervix_backup::describe_archive(output.stdout.as_slice())
        .assured("stdout carries a complete verified archive");
    assert_eq!(description.domains.len(), backup.summary.domains.len());
    assert_eq!(
        blake3::hash(&output.stdout).to_hex().as_str(),
        backup.summary.digest.to_string()
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stderr).assured("the JSON report goes to stderr");
    assert_eq!(report["execution_reference"], backup.reference.as_str());
    assert_eq!(report["output"], "-");
}

#[when(expr = "an NSPL cluster backup crosses two delayed cuts into {string}")]
async fn when_nspl_cluster_backup_crosses_delayed_cuts(world: &mut ScenarioWorld, file: String) {
    let archive = archive_path(world, &file);
    let leader = current_leader_node(world).await;
    let uri = world
        .cluster()
        .grpc_uri(&leader)
        .assured("the leader exists");
    let mut options = client_connect_options(&uri).assured("the client is configured");
    options.request_timeout = Duration::from_secs(20);
    options.retry_timeout = Duration::from_secs(20);
    let client = Client::connect_with_options(&uri, client_domain(&world.domain), options)
        .await
        .assured("the backup client connects");
    let query = format!("BACKUP CLUSTER TO '{}' TIMEOUT 20s;", archive.display());
    let mut task = nervix_primitives::task::spawn(async move { client.execute(query).await });
    for (suffix, seconds) in [("_a", 12), ("_b", 14)] {
        nervix_primitives::task::consume_budget().await;
        let domain = scenario_domain(world, &format!("{{{{backup_base}}}}{suffix}"));
        nervix_primitives::select! {
            result = &mut task => panic!("the backup ended before both cuts: {result:?}"),
            reached = nervix_primitives::time::timeout(
                Duration::from_secs(30),
                world.fault_injection.wait_for_backup_cut_pause(&domain),
            ) => reached.assured("the backup reaches this domain's cut"),
        }
        // This is the injected workload duration after an observed cut, not a readiness sleep.
        nervix_primitives::time::sleep(Duration::from_secs(seconds)).await;
        world.fault_injection.release_backup_cut_pause(&domain);
    }
    let outcome = task
        .await
        .assured("the backup task finishes")
        .assured("the backup receives its outcome beyond ordinary request and retry deadlines");
    assert!(outcome.succeeded(), "{}", outcome.message);
    world.last_command_output = Some(outcome.message);
}

#[then(expr = "backup archive {string} contains both delayed quiesced domain cuts")]
fn then_archive_contains_delayed_cuts(world: &mut ScenarioWorld, file: String) {
    let archive = archive_path(world, &file);
    let description = nervix_backup::describe_archive(
        std::fs::File::open(archive).assured("the client downloaded the archive"),
    )
    .assured("the archive is complete and valid");
    let domains: BTreeMap<_, _> = description
        .domains
        .iter()
        .map(|domain| (domain.record.domain.clone(), &domain.capture.cut))
        .collect();
    for (suffix, seconds) in [("_a", 12), ("_b", 14)] {
        let domain = scenario_domain(world, &format!("{{{{backup_base}}}}{suffix}"));
        let cut = domains
            .get(&domain)
            .assured("the archive contains the domain");
        let nervix_models::BackupCut::Quiesced {
            engaged_at,
            released_at,
            ..
        } = cut
        else {
            panic!("a running domain has a quiesced cut: {cut:?}");
        };
        let elapsed = released_at
            .duration_since(*engaged_at)
            .assured("the cut release follows its engagement");
        assert!(
            elapsed >= Duration::from_secs(seconds),
            "the archive records the injected cut delay"
        );
    }
}
