//! Public backup waits across independently bounded domain cuts.
//!
//! Layer: test harness.
//! - **Owns.** Delayed-cut workloads and assertions on command recovery and downloaded archives.
//! - **Depends on.** The public Rust client, CLI, archive reader, controlled capture gates and the
//!   injected holds of those gates.
//! - **Must not know.** Command storage, mutation leases or private checkpoint representations.

use std::future::Future;

use super::*;
use crate::common::injected_hold::{HeldGate, InjectedHold, hold_in_order};

/// How long a backup may take to reach a controlled cut before the step fails.
const CUT_ARRIVAL_TIMEOUT: Duration = Duration::from_secs(30);

/// The controlled cut of one domain's quiesced backup, where the server waits after draining the
/// domain until the scenario releases it.
struct BackupCutGate {
    fault_injection: FaultInjection,
    domain: DomainName,
}

impl BackupCutGate {
    fn of(world: &ScenarioWorld, raw_domain: &str) -> Self {
        Self {
            fault_injection: world.fault_injection.clone(),
            domain: scenario_domain(world, raw_domain),
        }
    }
}

impl fmt::Display for BackupCutGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "the backup cut of domain '{}'", self.domain)
    }
}

impl HeldGate for BackupCutGate {
    fn reached(&self) -> impl Future<Output = ()> + Send {
        self.fault_injection.wait_for_backup_cut_pause(&self.domain)
    }

    fn release(&self) {
        self.fault_injection.release_backup_cut_pause(&self.domain);
    }
}

#[when(expr = "backup cut {string} is held for {int} seconds and then released")]
async fn when_backup_cut_is_delayed(world: &mut ScenarioWorld, raw_domain: String, seconds: u64) {
    let hold = InjectedHold {
        gate: BackupCutGate::of(world, &raw_domain),
        duration: Duration::from_secs(seconds),
    };
    hold_in_order(vec![hold], CUT_ARRIVAL_TIMEOUT)
        .await
        .assured("the backup reaches the controlled cut");
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
async fn then_cli_recovery_keeps_identity_and_cuts(world: &mut ScenarioWorld) {
    let report = cli_json(last_cli_output(world));
    assert_eq!(
        report["execution_reference"],
        world.placeholders["backup_reference"]
    );
    let path = report["output"]
        .as_str()
        .assured("the report names the verified archive");
    let description = described_archive(PathBuf::from(path)).await;
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
    let archive = output.stdout.clone();
    let summary = backup.summary.clone();
    off_the_runner(move || {
        let description = nervix_backup::describe_archive(archive.as_slice())
            .assured("stdout carries a complete verified archive");
        assert_eq!(description.domains.len(), summary.domains.len());
        assert_eq!(
            blake3::hash(&archive).to_hex().as_str(),
            summary.digest.to_string()
        );
    })
    .await;
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
    let mut holds = Vec::new();
    for (suffix, seconds) in [("_a", 12), ("_b", 14)] {
        holds.push(InjectedHold {
            gate: BackupCutGate::of(world, &format!("{{{{backup_base}}}}{suffix}")),
            duration: Duration::from_secs(seconds),
        });
    }
    let mut held = std::pin::pin!(hold_in_order(holds, CUT_ARRIVAL_TIMEOUT));
    nervix_primitives::select! {
        biased;
        released = &mut held => released.assured("the backup reaches each domain's cut"),
        result = &mut task => panic!("the backup ended before both cuts: {result:?}"),
    }
    let outcome = task
        .await
        .assured("the backup task finishes")
        .assured("the backup receives its outcome beyond ordinary request and retry deadlines");
    assert!(outcome.succeeded(), "{}", outcome.message);
    world.last_command_output = Some(outcome.message);
}

#[when("a bounded native backup wait is recovered under its original reference")]
async fn when_native_backup_wait_is_recovered(world: &mut ScenarioWorld) {
    let initial_archive = archive_path(world, "native-pending.nvxb");
    let recovered_archive = archive_path(world, "native-recovered.nvxb");
    let leader = current_leader_node(world).await;
    let uri = world
        .cluster()
        .grpc_uri(&leader)
        .assured("the leader exists");
    let mut options = client_connect_options(&uri).assured("the client is configured");
    options.request_timeout = Duration::from_secs(20);
    options.retry_timeout = Duration::from_secs(20);
    options.backup_wait_timeout = Duration::from_secs(10);
    let client = Client::connect_with_options(&uri, client_domain(&world.domain), options.clone())
        .await
        .assured("the initial native client connects");
    let mut backup = nervix_models::Backup {
        scope: nervix_models::BackupScope::Cluster,
        destination: initial_archive.display().to_string(),
        resources: nervix_models::BackupResources::Included,
        capture: nervix_models::BackupCapture::Quiesced {
            timeout: Some(Duration::from_secs(30)),
        },
    };
    let execution = client.prepare_execution(backup.to_canonical_nspl()).await;
    let expected = execution.reference().clone();
    let mut task =
        nervix_primitives::task::spawn(async move { client.execute_prepared(&execution).await });
    let first = InjectedHold {
        gate: BackupCutGate::of(world, "{{backup_base}}_a"),
        duration: Duration::from_secs(2),
    };
    let second = scenario_domain(world, "{{backup_base}}_b");
    let mut held = std::pin::pin!(hold_in_order(vec![first], CUT_ARRIVAL_TIMEOUT));
    nervix_primitives::select! {
        biased;
        released = &mut held => {
            released.assured("the native backup reaches the first controlled cut");
        }
        result = &mut task => panic!("the native wait ended before the second cut: {result:?}"),
    }
    nervix_primitives::select! {
        result = &mut task => panic!("the native wait ended before the second cut: {result:?}"),
        reached = nervix_primitives::time::timeout(
            CUT_ARRIVAL_TIMEOUT,
            world.fault_injection.wait_for_backup_cut_pause(&second),
        ) => reached.assured("the native backup reaches the controlled cut"),
    }
    let result = nervix_primitives::time::timeout(Duration::from_secs(12), task)
        .await
        .assured("the bounded native wait ends before the ordinary request budget")
        .assured("the native command task finishes");
    let Err(report) = &result else {
        panic!("the unanswered native backup preserves its uncertain identity: {result:?}");
    };
    let nervix_client_core::ClientError::UncertainCommand { reference } = report.current_context()
    else {
        panic!("the unanswered native backup preserves its uncertain identity: {result:?}");
    };
    let reference = reference.clone();
    assert_eq!(reference, expected);
    assert!(
        !initial_archive.exists(),
        "uncertainty does not download an archive"
    );

    options.backup_wait_timeout = Duration::from_secs(60);
    let recovery_client = Client::connect_with_options(&uri, client_domain(&world.domain), options)
        .await
        .assured("a fresh native client connects for recovery");
    backup.destination = recovered_archive.display().to_string();
    let recovery = recovery_client
        .prepare_backup_with_reference(&backup, &reference)
        .await;
    world.fault_injection.release_backup_cut_pause(&second);
    let outcome = recovery_client
        .execute_prepared(&recovery)
        .await
        .assured("the original native backup is recovered");
    assert!(outcome.succeeded(), "{}", outcome.message);
    assert_eq!(outcome.execution_reference.as_ref(), Some(&expected));
    world.last_backup = Some(TestBackup {
        reference: expected,
        summary: *outcome
            .backup
            .assured("the recovered backup has its summary"),
    });
}

#[then(expr = "backup archive {string} retains both native recovery cuts")]
async fn then_archive_retains_native_recovery_cuts(world: &mut ScenarioWorld, file: String) {
    let description = described_archive(archive_path(world, &file)).await;
    assert_eq!(
        description.domains.len(),
        last_backup(world).summary.domains.len()
    );
    for suffix in ["_a", "_b"] {
        let domain = scenario_domain(world, &format!("{{{{backup_base}}}}{suffix}"));
        let captured = description
            .domains
            .iter()
            .find(|entry| entry.record.domain == domain)
            .assured("the native archive contains each requested domain");
        let nervix_models::BackupCut::Quiesced {
            engaged_at,
            released_at,
            ..
        } = &captured.capture.cut
        else {
            panic!("native recovery retains each quiesced cut");
        };
        if suffix == "_a" {
            assert!(
                released_at
                    .duration_since(*engaged_at)
                    .assured("the cut ends after engagement")
                    >= Duration::from_secs(2),
                "native recovery retains the completed first cut"
            );
        }
    }
}

#[then(expr = "backup archive {string} contains both delayed quiesced domain cuts")]
async fn then_archive_contains_delayed_cuts(world: &mut ScenarioWorld, file: String) {
    let description = described_archive(archive_path(world, &file)).await;
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
