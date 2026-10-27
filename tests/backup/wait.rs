//! Public backup waits across independently bounded domain cuts.
//!
//! Layer: test harness.
//! - **Owns.** Delayed-cut workloads and assertions on command recovery and downloaded archives.
//! - **Depends on.** The public Rust client, CLI, archive reader, and the controlled capture gates
//!   of the fault injector, which hold every cut that reaches them as a scenario declares.
//! - **Must not know.** Command storage, mutation leases or private checkpoint representations.
//!
//! A backup whose outcome was not recorded runs again under its execution reference, and the
//! outcome or archive a scenario observes may be that later run's. A gate therefore holds every
//! cut that reaches it, and times each hold from the cut's own arrival on the cut's own task, so a
//! stalled runner neither lengthens nor ends a hold.

use error_stack::{Report, ResultExt as _};

use super::*;
use crate::common::runner_observation::observe_off_the_runner;

/// How long a backup may take to reach a controlled cut before the step fails. A cut reaches its
/// gate only after its lease, its pause and its drain, each of which a loaded runner delays, and a
/// backup that runs again under its reference reaches a later gate only once the earlier gates have
/// held that run's cuts too. The wait ends as soon as a cut arrives, so the bound is generous, as
/// the backup pause steps' bound is.
const CUT_ARRIVAL_TIMEOUT: Duration = Duration::from_secs(120);

/// What each cut of the independent-budget probe keeps of its budget beyond its hold, for its
/// lease, its pause, its drain and its capture. The budget runs from before the cut's lease, and a
/// leader whose raft lease lapsed refuses every proposal, the cut's lease included, until the
/// lease recovers. Loaded runs keep a backup's lease refused for seconds at a time, so a probe that
/// left a held cut less than several such refusals and a loaded drain could expire it while the
/// product keeps its contract.
const HELD_CUT_MARGIN: Duration = Duration::from_secs(18);

/// The controlled capture gate of one domain's quiesced cuts, where each cut of the domain waits
/// after draining it until the scenario releases the gate or the cut's timed hold ends.
struct CaptureGate {
    fault_injection: FaultInjection,
    domain: DomainName,
}

/// A backup reached no controlled capture gate of a domain within the arrival bound.
#[derive(Debug, thiserror::Error)]
#[error("the backup did not reach the capture of domain '{domain}' within {arrival:?}")]
struct CaptureNotReached {
    domain: DomainName,
    arrival: Duration,
}

impl CaptureGate {
    fn of(world: &ScenarioWorld, raw_domain: &str) -> Self {
        Self {
            fault_injection: world.fault_injection.clone(),
            domain: scenario_domain(world, raw_domain),
        }
    }

    /// Holds every cut that reaches the gate, one already waiting there included, for `hold`
    /// after its own arrival.
    fn hold_each_cut_for(&self, hold: Duration) {
        self.fault_injection
            .hold_backup_cuts_for(&self.domain, BackupCutBoundary::Capture, hold);
    }

    /// Waits up to the arrival bound for the first cut to reach the gate, and returns when it did.
    async fn first_cut_reached(&self) -> Result<Instant, Report<CaptureNotReached>> {
        nervix_primitives::time::timeout(
            CUT_ARRIVAL_TIMEOUT,
            self.fault_injection
                .wait_for_backup_cut_pause(&self.domain, BackupCutBoundary::Capture),
        )
        .await
        .change_context_lazy(|| CaptureNotReached {
            domain: self.domain.clone(),
            arrival: CUT_ARRIVAL_TIMEOUT,
        })
    }

    /// Waits for the first cut to reach the gate and for its `hold` to end. The gate ends the hold
    /// itself, so this only learns when it did.
    async fn first_cut_held(&self, hold: Duration) -> Result<(), Report<CaptureNotReached>> {
        let arrival = self.first_cut_reached().await?;
        if let Some(remaining) = hold.checked_sub(arrival.elapsed()) {
            nervix_primitives::time::sleep(remaining).await;
        }
        Ok(())
    }

    /// Lets every cut waiting at the gate continue, and every later one pass it.
    fn release(&self) {
        self.fault_injection
            .release_backup_cut_pause(&self.domain, BackupCutBoundary::Capture);
    }
}

/// A capture gate and the hold a scenario times for every cut that reaches it.
struct DelayedCut {
    gate: CaptureGate,
    hold: Duration,
}

/// The cut budget a cluster backup gives each of its two domains, and how long the cuts of each
/// domain are held.
#[derive(Debug)]
struct IndependentCutBudgets {
    /// The backup's `TIMEOUT`: the budget of each domain's quiesced cut.
    budget: Duration,
    /// How long the first domain's cuts are held.
    first: Duration,
    /// How long the second domain's cuts are held.
    second: Duration,
}

impl IndependentCutBudgets {
    /// Panics unless the probe tells budgets of their own from one budget the two cuts share, and
    /// leaves each held cut its margin. The two holds together must outlast one budget, so a
    /// shared budget would expire the second cut.
    fn assert_probes_independent_budgets(&self) {
        for hold in [self.first, self.second] {
            let left = self.left_by(hold);
            assert!(
                left >= HELD_CUT_MARGIN,
                "a hold of {hold:?} leaves its cut {left:?} of its budget, less than its \
                 {HELD_CUT_MARGIN:?} margin: {self:?}"
            );
        }
        assert!(
            self.second > self.left_by(self.first),
            "the two holds must together outlast one cut budget, or one budget the two cuts \
             shared would pass the probe as well: {self:?}"
        );
    }

    /// What a hold leaves its cut of the budget.
    fn left_by(&self, hold: Duration) -> Duration {
        let Some(left) = self.budget.checked_sub(hold) else {
            panic!("a hold of {hold:?} outlasts its whole cut budget: {self:?}");
        };
        left
    }
}

/// Holds every cut of the domain that reaches its capture, a cut of a backup that runs again under
/// its execution reference included, for the given seconds after its arrival. The step ends once
/// the first one continues.
#[when(expr = "backup cut {string} is held for {int} seconds and then released")]
async fn when_backup_cut_is_delayed(world: &mut ScenarioWorld, raw_domain: String, seconds: u64) {
    let gate = CaptureGate::of(world, &raw_domain);
    let hold = Duration::from_secs(seconds);
    gate.hold_each_cut_for(hold);
    gate.first_cut_held(hold)
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

/// Backs up the cluster through one Plexus statement whose `TIMEOUT` gives each domain's cut a
/// budget of its own, while every cut of each domain is held for its seconds. The client keeps
/// 20-second request and retry deadlines, so a backup whose holds together outlast them receives its
/// outcome beyond those deadlines.
#[when(
    expr = "a Plexus cluster backup with a {int} second cut budget crosses cuts delayed for {int} \
            and {int} seconds into {string}"
)]
async fn when_plexus_cluster_backup_crosses_delayed_cuts(
    world: &mut ScenarioWorld,
    budget_seconds: u64,
    first_seconds: u64,
    second_seconds: u64,
    file: String,
) {
    let probe = IndependentCutBudgets {
        budget: Duration::from_secs(budget_seconds),
        first: Duration::from_secs(first_seconds),
        second: Duration::from_secs(second_seconds),
    };
    probe.assert_probes_independent_budgets();
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
    let query = format!(
        "BACKUP CLUSTER TO '{}' TIMEOUT {budget_seconds}s;",
        archive.display()
    );
    let mut task = nervix_primitives::task::spawn(async move { client.execute(query).await });
    let mut delayed = Vec::new();
    for (suffix, hold) in [("_a", probe.first), ("_b", probe.second)] {
        let gate = CaptureGate::of(world, &format!("{{{{backup_base}}}}{suffix}"));
        gate.hold_each_cut_for(hold);
        delayed.push(DelayedCut { gate, hold });
    }
    let mut held = std::pin::pin!(async {
        for cut in &delayed {
            cut.gate.first_cut_held(cut.hold).await?;
        }
        Ok::<_, Report<CaptureNotReached>>(())
    });
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

#[when(
    expr = "a bounded native backup wait is recovered under its original reference after a {int} \
            second first cut"
)]
async fn when_native_backup_wait_is_recovered(world: &mut ScenarioWorld, first_cut_seconds: u64) {
    let initial_archive = archive_path(world, "native-pending.nvxb");
    let recovered_archive = archive_path(world, "native-recovered.nvxb");
    let leader = current_leader_node(world).await;
    let uri = world
        .cluster()
        .grpc_uri(&leader)
        .assured("the leader exists");
    let domain = client_domain(&world.domain);
    let mut options = client_connect_options(&uri).assured("the client is configured");
    options.request_timeout = Duration::from_secs(20);
    options.retry_timeout = Duration::from_secs(20);
    options.backup_wait_timeout = Duration::from_secs(10);
    let first = DelayedCut {
        gate: CaptureGate::of(world, "{{backup_base}}_a"),
        hold: Duration::from_secs(first_cut_seconds),
    };
    first.gate.hold_each_cut_for(first.hold);
    let second = CaptureGate::of(world, "{{backup_base}}_b");
    let recovered = observe_off_the_runner(async move {
        let client = Client::connect_with_options(&uri, domain.clone(), options.clone())
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
        let execution = client.prepare_execution(backup.to_canonical_plexus()).await;
        let expected = execution.reference().clone();
        let wait_deadline = nervix_primitives::time::Instant::now() + Duration::from_secs(12);
        let initial_wait = observe_off_the_runner(async move {
            nervix_primitives::time::timeout_at(wait_deadline, client.execute_prepared(&execution))
                .await
                .assured("the bounded native wait ends before the ordinary request budget")
        });
        // Expiry does not cancel admitted work and need not follow the second cut. Keep
        // observing both gates independently of the client's bounded wait.
        first
            .gate
            .first_cut_held(first.hold)
            .await
            .assured("the native backup reaches the first controlled cut");
        second
            .first_cut_reached()
            .await
            .assured("the native backup reaches the second controlled cut");
        let result = initial_wait.await;
        let Err(report) = &result else {
            panic!("the unanswered native backup preserves its uncertain identity: {result:?}");
        };
        let nervix_client_core::ClientError::UncertainCommand { reference } =
            report.current_context()
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
        let recovery_client = Client::connect_with_options(&uri, domain, options)
            .await
            .assured("a fresh native client connects for recovery");
        backup.destination = recovered_archive.display().to_string();
        let recovery = recovery_client
            .prepare_backup_with_reference(&backup, &reference)
            .await;
        second.release();
        let outcome = recovery_client
            .execute_prepared(&recovery)
            .await
            .assured("the original native backup is recovered");
        assert!(outcome.succeeded(), "{}", outcome.message);
        assert_eq!(outcome.execution_reference.as_ref(), Some(&expected));
        assert_eq!(
            outcome.origin,
            Some(nervix_client_core::OutcomeOrigin::Recovered),
            "the fresh client recovers the admitted execution"
        );
        TestBackup {
            reference: expected,
            summary: *outcome
                .backup
                .assured("the recovered backup has its summary"),
        }
    })
    .await;
    world.last_backup = Some(recovered);
}

#[then(
    expr = "backup archive {string} retains both native recovery cuts after a {int} second first \
            cut"
)]
async fn then_archive_retains_native_recovery_cuts(
    world: &mut ScenarioWorld,
    file: String,
    first_cut_seconds: u64,
) {
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
                    >= Duration::from_secs(first_cut_seconds),
                "native recovery retains the completed first cut"
            );
        }
    }
}

#[then(
    expr = "backup archive {string} contains quiesced domain cuts delayed for {int} and {int} \
            seconds"
)]
async fn then_archive_contains_delayed_cuts(
    world: &mut ScenarioWorld,
    file: String,
    first_seconds: u64,
    second_seconds: u64,
) {
    let description = described_archive(archive_path(world, &file)).await;
    let domains: BTreeMap<_, _> = description
        .domains
        .iter()
        .map(|domain| (domain.record.domain.clone(), &domain.capture.cut))
        .collect();
    for (suffix, seconds) in [("_a", first_seconds), ("_b", second_seconds)] {
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
            "the archive records the injected cut delay for {domain}: required at least \
             {seconds}s, recorded {elapsed:?} between {engaged_at:?} and {released_at:?}"
        );
    }
}
