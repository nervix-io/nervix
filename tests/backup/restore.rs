//! Steps that restore archives into a cluster, replace a scenario's cluster with a fresh one, and
//! compare what a restore recreated with what the archive's source held.
//!
//! Layer: test harness.
//! - **Owns.** Restores the CLI runs and the checks of their reports, restore streams a scenario
//!   shapes itself, archives altered to probe a restore's refusals, restore step pauses, the model
//!   and resource descriptions a scenario saves to compare across clusters and domains, and
//!   replacing a scenario's cluster with a fresh one.
//! - **Depends on.** The public CLI, the harness's own restore streams and sessions, the archive
//!   format's reader and writer, the fault injection a scenario arms, and the language layer to
//!   read an archive's NSPL.
//! - **Must not know.** How the server stages, plans or applies a restore.

#[path = "../runtime/native_restore_inspection.rs"]
mod native_restore_inspection;

use std::os::unix::fs::OpenOptionsExt as _;

use nervix_backup::{
    ArchiveLayout, ArchiveRecord, BackupManifest, BranchLifecycleRecord, SectionDigester,
    SectionPath, WasmStateDescriptor,
};
use nervix_client_wire::{
    CommandDisposition, CommandOutcome as WireCommandOutcome, OutcomeOrigin, RestoreDisposition,
    RestoreUploadFailure, UnknownOutcomeCause,
};
use nervix_models::{ResourceName, RestoreStep, SchemaFingerprint};
use nervix_primitives::task::AbortOnDropHandle;

use super::*;
use crate::common::{
    cluster::node_name,
    raw_session::{TestRestore, TestRestoreEnd, send_restore},
};

/// How long a restore waits at an armed pause before a scenario gives up on reaching it.
const RESTORE_PAUSE_TIMEOUT: Duration = Duration::from_secs(120);

#[given("the restore coordinator uses remote placement in a multi-node cluster")]
async fn given_restore_uses_remote_placement(world: &mut ScenarioWorld) {
    if world.cluster().node_ids().len() > 1 {
        let leader = current_leader_node(world).await;
        world
            .cluster()
            .run_command(&leader, &world.domain, &format!("CORDON NODE {leader};"))
            .await
            .assured("the multi-node restore coordinator is excluded from execution placement");
    }
}

fn restored_work_placements(world: &ScenarioWorld) -> BTreeSet<String> {
    let status = world
        .last_command_output
        .as_deref()
        .assured("cluster status is read before inspecting restored placements");
    let mut placements = BTreeSet::new();
    for placement in scheduled_placements_for_domain(status, &world.domain) {
        let (owner, mut replicas) = scheduled_node_placement_from_status(
            status,
            &world.domain,
            &placement.kind,
            &placement.name,
        )
        .assured("each scheduled entry has its public placement");
        replicas.sort_unstable();
        placements.insert(format!(
            "kind={} name={} owner={} replicas={}",
            placement.kind,
            placement.name,
            owner,
            replicas.join(",")
        ));
    }
    placements
}

#[then(
    expr = "the last cluster status placements of restored work are saved as placeholder {string}"
)]
fn then_save_restored_work_placements(world: &mut ScenarioWorld, placeholder: String) {
    let placements = restored_work_placements(world);
    assert!(!placements.is_empty(), "restore installed scheduled work");
    world.placeholders.insert(
        placeholder,
        placements.into_iter().collect::<Vec<_>>().join("\n"),
    );
}

#[then(
    expr = "the last cluster status preserves restored work placements from placeholder {string}"
)]
fn then_restored_work_placements_are_preserved(world: &mut ScenarioWorld, placeholder: String) {
    let current = restored_work_placements(world);
    let expected = world
        .placeholders
        .get(&placeholder)
        .assured("the restored placements are saved before restart");
    for placement in expected.lines() {
        assert!(
            current.contains(placement),
            "restart changed restored ownership or replicas: expected {placement}; got {current:?}"
        );
    }
}

#[given(
    expr = "node {string} has a state-counting WASM fixture with {int} MiB saves in resource \
            directory {string}"
)]
async fn given_large_guest_save(
    world: &mut ScenarioWorld,
    node: String,
    mebibytes: usize,
    placeholder: String,
) {
    assert!(
        (1..=48).contains(&mebibytes),
        "the fixture fits the guest memory limit"
    );
    let bytes = mebibytes
        .checked_mul(1024 * 1024)
        .assured("the fixture size is bounded");
    crate::place_generated_wasm_processor_fixture(
        world,
        &node,
        &placeholder,
        crate::state_counting_wasm_fixture_with_size("filtered_metrics", bytes),
    )
    .await;
}

#[when(
    expr = "round {int} of Kafka messages for {int} restore tenants is published to topic {string}"
)]
async fn when_restore_tenant_round_is_published(
    world: &mut ScenarioWorld,
    round: u32,
    tenants: usize,
    topic: String,
) {
    let topic = expand_placeholders(world, &topic);
    for tenant in 0..tenants {
        let payload =
            serde_json::json!({ "value": round, "tenant": format!("restore-tenant-{tenant}") })
                .to_string();
        world
            .cluster()
            .publish_kafka(&topic, &payload)
            .await
            .assured("one interleaved tenant input is accepted");
    }
}

#[then(
    expr = "within {string} the restore subscription receives one isolated even row for each of \
            {int} tenants"
)]
async fn then_restore_tenants_remain_isolated(
    world: &mut ScenarioWorld,
    duration: String,
    tenants: usize,
) {
    let duration = parse_duration_text(&duration).assured("the scenario duration is valid");
    let deadline = Instant::now() + duration;
    let expected = (0..tenants)
        .map(|tenant| format!("restore-tenant-{tenant}"))
        .collect::<BTreeSet<_>>();
    let mut pending = expected.clone();
    let mut observed = BTreeSet::new();
    let session = world
        .active_session
        .as_mut()
        .assured("the restored output has a subscription");
    while !pending.is_empty() {
        nervix_primitives::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "missing isolated restored rows: {pending:?}"
        );
        let event = session
            .try_next_subscription(deadline.saturating_duration_since(now))
            .await
            .assured("the subscription remains connected");
        let Some(event) = event else {
            let server_error = session
                .try_next_server_error(Duration::ZERO)
                .await
                .assured("the subscription's filed server errors remain readable");
            panic!(
                "missing isolated restored rows: {pending:?}; observed: {observed:?}; server \
                 error: {server_error:?}; delivered rows: {:?}; frames outside subscription \
                 lifetime: {:?}",
                session.delivered_payloads(),
                session.frames_outside_lifetime(),
            );
        };
        observed.insert(event.payload.clone());
        world.last_subscription_payload = Some(event.payload.clone());
        // Domain offsets recover at least once, so a valid row may repeat while another branch
        // is still pending. Every observed row must still belong to an expected branch.
        let matched = expected
            .iter()
            .find(|tenant| {
                event
                    .payload
                    .contains(&format!("key={{\"tenant\":\"{tenant}\"}}"))
            })
            .cloned()
            .unwrap_or_else(|| {
                panic!(
                    "an emitted row belongs to an expected tenant: expected={expected:?}, \
                     payload={}",
                    event.payload
                )
            });
        assert!(
            event.payload.contains(&format!("\"tenant\":\"{matched}\"")),
            "the row's tenant agrees with its branch: {}",
            event.payload
        );
        assert!(
            event.payload.contains("\"note\":\"even\""),
            "the row resumes the saved even batch count: {}",
            event.payload
        );
        pending.retain(|tenant| tenant != &matched);
    }
}

#[given(expr = "restoring domain {string} fails after durable state publication")]
fn given_durable_restore_publication_fails(world: &mut ScenarioWorld, domain: String) {
    world
        .fault_injection
        .fail_after_durable_restore_publication(scenario_domain(world, &domain));
}

#[given(expr = "restoring domain {string} fails before installing its first WASM checkpoint")]
fn given_restored_wasm_checkpoint_fails(world: &mut ScenarioWorld, domain: String) {
    world
        .fault_injection
        .fail_restored_wasm_checkpoint(scenario_domain(world, &domain));
}

#[given(
    expr = "restoring domain {string} fails before installing its first materialized checkpoint"
)]
fn given_restored_materialized_checkpoint_fails(world: &mut ScenarioWorld, domain: String) {
    world
        .fault_injection
        .fail_restored_materialized_checkpoint(scenario_domain(world, &domain));
}

#[given(
    expr = "restoring domain {string} fails before installing its first deduplicator or window \
            checkpoint"
)]
fn given_restored_branch_state_checkpoint_fails(world: &mut ScenarioWorld, domain: String) {
    world
        .fault_injection
        .fail_restored_branch_state_checkpoint(scenario_domain(world, &domain));
}

#[then(expr = "client {string} observes resumed clock progress beyond backup {string}'s frontier")]
fn then_resumed_clock_projects_downtime(world: &mut ScenarioWorld, client: String, file: String) {
    let archive = nervix_backup::describe_archive(
        std::fs::File::open(archive_path(world, &file)).assured("archive opens"),
    )
    .assured("archive verifies");
    let domain = scenario_domain(world, &world.domain);
    let archived = archive
        .domains
        .iter()
        .find(|entry| entry.record.domain == domain)
        .assured("archived domain exists");
    let frontier = archived
        .record
        .logical_frontier
        .assured("paced cut has a frontier");
    let clock = world
        .transaction_clients
        .get(&client)
        .assured("clock client is connected")
        .domain_clock(&domain)
        .assured("client attached the restored domain clock");
    assert!(
        clock.frontier().assured("resumed clock has progressed") > frontier,
        "the archived mapping projects elapsed downtime beyond the saved cut"
    );
}

#[given(expr = "restoring domain {string} by coordinator {string} pauses before state publication")]
fn given_restore_publication_pauses(
    world: &mut ScenarioWorld,
    domain: String,
    coordinator: String,
) {
    world.fault_injection.pause_restore_state_publication(
        scenario_domain(world, &domain),
        node_name(&expand_placeholders(world, &coordinator)),
    );
}

#[then(expr = "restoring domain {string} by coordinator {string} has reached state publication")]
async fn then_restore_publication_pauses(
    world: &mut ScenarioWorld,
    domain: String,
    coordinator: String,
) {
    nervix_primitives::time::timeout(
        RESTORE_PAUSE_TIMEOUT,
        world.fault_injection.wait_for_restore_state_publication(
            &scenario_domain(world, &domain),
            &node_name(&expand_placeholders(world, &coordinator)),
        ),
    )
    .await
    .assured("restore reached its publication boundary");
}

#[when(expr = "state publication of domain {string} by coordinator {string} is released")]
fn when_restore_publication_released(
    world: &mut ScenarioWorld,
    domain: String,
    coordinator: String,
) {
    world.fault_injection.release_restore_state_publication(
        &scenario_domain(world, &domain),
        &node_name(&expand_placeholders(world, &coordinator)),
    );
}

#[then(expr = "state publication of domain {string} by coordinator {string} is refused")]
async fn then_restore_publication_refused(
    world: &mut ScenarioWorld,
    domain: String,
    coordinator: String,
) {
    nervix_primitives::time::timeout(
        RESTORE_PAUSE_TIMEOUT,
        world
            .fault_injection
            .wait_for_restore_state_publication_refusal(
                &scenario_domain(world, &domain),
                &node_name(&expand_placeholders(world, &coordinator)),
            ),
    )
    .await
    .assured("the stale storage mutation was refused");
}

#[then(
    expr = "restore {string} of backup archive {string} completes on node {string} under \
            execution reference {string}"
)]
async fn then_restore_eventually_completes(
    world: &mut ScenarioWorld,
    restore: String,
    file: String,
    node: String,
    reference: String,
) {
    let request = restore_stream_request(world, &restore, &file, &node, &reference);
    let end = nervix_primitives::time::timeout(RESTORE_PAUSE_TIMEOUT, async {
        let mut poll = nervix_primitives::time::interval(Duration::from_millis(100));
        loop {
            nervix_primitives::task::consume_budget().await;
            poll.tick().await;
            let end = stream_restore(&request, None, None).await;
            match &restore_outcome(&end).disposition {
                CommandDisposition::Completed { .. } => break end,
                CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::StillApplying) => {}
                other => panic!("restore recovery did not complete: {other:?}"),
            }
        }
    })
    .await
    .assured("restore completes under the new coordinator");
    world.last_restore_end = Some(end);
}

#[then(
    expr = "backup archives {string} and {string} have identical guest checkpoints and source \
            offsets"
)]
fn then_restored_checkpoints_match(world: &mut ScenarioWorld, before: String, after: String) {
    fn checkpoints(path: &Path) -> BTreeMap<String, Vec<u8>> {
        let copy = copy_of_archive(path);
        let entries = copy
            .sections
            .into_iter()
            .filter(|(path, _)| path.ends_with("/guest.bin") || path.ends_with("/offsets.rkyv"))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            entries.len(),
            3,
            "two guest saves and domain offsets are visible"
        );
        entries
    }
    assert_eq!(
        checkpoints(&archive_path(world, &before)),
        checkpoints(&archive_path(world, &after)),
        "a stale installer did not change published state"
    );
}

/// The bytes of every section an archive holds under one of `directories` of its domains' state.
fn state_sections(path: &Path, directories: &[&str]) -> BTreeMap<String, Vec<u8>> {
    let mut entries = BTreeMap::new();
    for (section, bytes) in copy_of_archive(path).sections {
        let mut selected = false;
        for directory in directories {
            if section.contains(&format!("/state/{directory}/")) {
                selected = true;
            }
        }
        if selected {
            entries.insert(section, bytes);
        }
    }
    assert!(
        !entries.is_empty(),
        "sections under {directories:?} are visible"
    );
    entries
}

#[then(expr = "backup archives {string} and {string} have identical materialized generations")]
fn then_restored_materialized_generations_match(
    world: &mut ScenarioWorld,
    before: String,
    after: String,
) {
    assert_eq!(
        state_sections(&archive_path(world, &before), &["materialized_relay"]),
        state_sections(&archive_path(world, &after), &["materialized_relay"]),
        "a stale installer cannot replace the active materialized generation"
    );
}

#[then(expr = "backup archives {string} and {string} have identical deduplicator and window state")]
fn then_restored_branch_states_match(world: &mut ScenarioWorld, before: String, after: String) {
    let directories = ["deduplicator", "window_processor"];
    assert_eq!(
        state_sections(&archive_path(world, &before), &directories),
        state_sections(&archive_path(world, &after), &directories),
        "a stale installer cannot replace the active deduplicator keys or windows"
    );
}

/// A restore step pause a scenario armed, which it releases by the node it named.
#[derive(Debug, Clone)]
pub(crate) struct ArmedRestorePause {
    node: String,
    step: RestoreStep,
}

/// Every section of an archive: its manifest, each section's bytes by path, and where each section
/// begins in the archive.
#[derive(Default)]
pub(super) struct ArchiveCopy {
    manifest: Option<BackupManifest>,
    pub(super) sections: BTreeMap<String, Vec<u8>>,
    offsets: BTreeMap<String, u64>,
}

impl SectionVisitor for ArchiveCopy {
    fn manifest(
        &mut self,
        manifest: &BackupManifest,
    ) -> Result<(), error_stack::Report<nervix_backup::ArchiveReadError>> {
        self.manifest = Some(manifest.clone());
        Ok(())
    }

    fn section(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), error_stack::Report<nervix_backup::ArchiveReadError>> {
        let path = entry.path.to_string();
        self.offsets.insert(path.clone(), content.archive_offset());
        let bytes = content.read_all(entry, MODELS_LIMIT)?;
        self.sections.insert(path, bytes);
        Ok(())
    }
}

/// Reads and verifies every section of the archive at `path`.
pub(super) fn copy_of_archive(path: &Path) -> ArchiveCopy {
    let file = std::fs::File::open(path).expect("the archive exists");
    let mut copy = ArchiveCopy::default();
    read_archive(std::io::BufReader::new(file), &mut copy)
        .unwrap_or_else(|report| panic!("the archive verifies: {report:?}"));
    copy
}

/// Copies the directory tree at `source` to `target`.
fn copy_directory(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).expect("the copied directory is created");
    let entries = std::fs::read_dir(source).expect("the directory to keep is readable");
    for entry in entries {
        let entry = entry.expect("a directory entry is readable");
        let from = entry.path();
        let to = target.join(entry.file_name());
        if entry
            .file_type()
            .expect("a directory entry has a type")
            .is_dir()
        {
            copy_directory(&from, &to);
        } else {
            std::fs::copy(&from, &to).expect("a file of the directory is copied");
        }
    }
}

#[given(expr = "resource directory {string} is kept beside the scenario's archives")]
fn given_resource_directory_is_kept(world: &mut ScenarioWorld, placeholder: String) {
    let source = resource_directory_path(world, &placeholder);
    let target = archive_path(world, &format!("fixtures/{placeholder}"));
    copy_directory(&source, &target);
    world
        .placeholders
        .insert(placeholder, target.display().to_string());
}

#[given(
    expr = "the cluster is replaced by a fresh {int} node cluster whose nodes are named {string}"
)]
async fn given_cluster_is_replaced(world: &mut ScenarioWorld, node_count: usize, prefix: String) {
    let mut cluster = world
        .cluster
        .take()
        .expect("the scenario runs the cluster it replaces");
    world.transaction_clients.clear();
    let teardown = cluster.shutdown_for_teardown().await;
    let panics = teardown
        .panics()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    assert!(
        panics.is_empty(),
        "the replaced cluster stopped cleanly: {panics:?}"
    );
    // A node that is still stopping keeps running beside the cluster that replaces it, so the
    // replacement has not happened yet.
    let still_stopping = teardown
        .still_stopping()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    assert!(
        still_stopping.is_empty(),
        "the replaced cluster stopped within its cleanup budget: {still_stopping:?}"
    );
    // Dropping the cluster gives back the storage and fixtures its nodes wrote to.
    drop(cluster);
    world.cluster_config.node_name_prefix = prefix;
    crate::given_cluster_is_started(world, node_count).await;
}

#[given(expr = "the path of backup archive {string} is saved as placeholder {string}")]
fn given_archive_path_is_saved(world: &mut ScenarioWorld, file: String, placeholder: String) {
    let path = archive_path(world, &file);
    world
        .placeholders
        .insert(placeholder, path.display().to_string());
}

/// The `SHOW CREATE` statement of every model the archive at `path` holds for `domain`, in
/// archive order.
fn show_create_statements(path: &Path, domain: &DomainName) -> Vec<String> {
    let archived = archived_models(path, domain);
    let statements = parse_client_statements(&archived)
        .unwrap_or_else(|error| panic!("models.nspl parses: {error:?}\n{archived}"));
    let mut shows = Vec::with_capacity(statements.len());
    for statement in statements {
        let ClientStatement::Server(Statement::Create(create)) = statement else {
            panic!("models.nspl holds only model creations:\n{archived}");
        };
        shows.push(format!(
            "SHOW CREATE {} {};",
            create.body.kind().keyword_phrase(),
            create.body.name()
        ));
    }
    shows
}

#[when(
    expr = "the SHOW CREATE output of every model backup archive {string} holds for domain \
            {string} is saved as {string} from node {string}"
)]
async fn when_show_create_output_is_saved(
    world: &mut ScenarioWorld,
    file: String,
    domain: String,
    name: String,
    node: String,
) {
    let path = archive_path(world, &file);
    let domain = scenario_domain(world, &domain);
    let node = expand_placeholders(world, &node);
    let mut outputs = BTreeMap::new();
    for show in show_create_statements(&path, &domain) {
        let output = world
            .cluster()
            .run_command(&node, domain.as_str(), &show)
            .await
            .unwrap_or_else(|error| panic!("{show} runs in domain '{domain}': {error}"));
        outputs.insert(show, output);
    }
    assert!(
        !outputs.is_empty(),
        "the archive holds models of '{domain}'"
    );
    world.saved_model_definitions.insert(name, outputs);
}

#[then(
    expr = "the SHOW CREATE output of every model saved as {string} is the same in domain \
            {string} on node {string}"
)]
async fn then_show_create_output_is_the_same(
    world: &mut ScenarioWorld,
    name: String,
    domain: String,
    node: String,
) {
    let domain = expand_placeholders(world, &domain);
    let node = expand_placeholders(world, &node);
    let saved = world
        .saved_model_definitions
        .get(&name)
        .unwrap_or_else(|| panic!("a preceding step saved the models as '{name}'"))
        .clone();
    for (show, expected) in saved {
        let output = world
            .cluster()
            .run_command(&node, &domain, &show)
            .await
            .unwrap_or_else(|error| panic!("{show} runs in domain '{domain}': {error}"));
        assert_eq!(output, expected, "{show} in domain '{domain}'");
    }
}

/// The line `DESCRIBE RESOURCE` prints for each of `versions`.
fn version_details(output: &str, versions: &[u64]) -> BTreeMap<u64, String> {
    let mut details = BTreeMap::new();
    for version in versions {
        let prefix = format!("- version={version} ");
        let mut found = None;
        for line in output.lines() {
            let line = line.trim();
            if line.starts_with(&prefix) {
                found = Some(line.to_string());
            }
        }
        let Some(line) = found else {
            panic!("DESCRIBE RESOURCE details version {version}:\n{output}");
        };
        details.insert(*version, line);
    }
    details
}

async fn describe_resource(
    world: &ScenarioWorld,
    resource: &str,
    domain: &str,
    node: &str,
) -> String {
    world
        .cluster()
        .run_command(node, domain, &format!("DESCRIBE RESOURCE {resource};"))
        .await
        .unwrap_or_else(|error| panic!("DESCRIBE RESOURCE {resource} runs: {error}"))
}

#[when(
    expr = "the details DESCRIBE RESOURCE {string} prints for versions {string} in domain \
            {string} on node {string} are saved as {string}"
)]
async fn when_resource_details_are_saved(
    world: &mut ScenarioWorld,
    resource: String,
    versions: String,
    domain: String,
    node: String,
    name: String,
) {
    let domain = expand_placeholders(world, &domain);
    let node = expand_placeholders(world, &node);
    let mut numbers = Vec::new();
    for version in versions.split(',') {
        numbers.push(
            version
                .trim()
                .parse::<u64>()
                .expect("the scenario names version numbers"),
        );
    }
    let output = describe_resource(world, &resource, &domain, &node).await;
    let details = version_details(&output, &numbers);
    world.saved_resource_details.insert(name, details);
}

#[then(
    expr = "DESCRIBE RESOURCE {string} in domain {string} on node {string} prints the details \
            saved as {string}"
)]
async fn then_resource_details_are_the_same(
    world: &mut ScenarioWorld,
    resource: String,
    domain: String,
    node: String,
    name: String,
) {
    let domain = expand_placeholders(world, &domain);
    let node = expand_placeholders(world, &node);
    let saved = world
        .saved_resource_details
        .get(&name)
        .unwrap_or_else(|| panic!("a preceding step saved resource details as '{name}'"))
        .clone();
    let output = describe_resource(world, &resource, &domain, &node).await;
    let numbers = saved.keys().copied().collect::<Vec<_>>();
    assert_eq!(
        version_details(&output, &numbers),
        saved,
        "DESCRIBE RESOURCE {resource} in domain '{domain}':\n{output}"
    );
}

#[when(expr = "the CLI restores {string} from {string} on node {string} reporting JSON")]
async fn when_cli_restores(world: &mut ScenarioWorld, scope: String, file: String, node: String) {
    let archive = archive_path(world, &file);
    let mut arguments = vec!["restore".to_string()];
    for argument in expand_placeholders(world, &scope).split_whitespace() {
        arguments.push(argument.to_string());
    }
    arguments.extend([
        "--input".to_string(),
        archive.display().to_string(),
        "--format".to_string(),
        "json".to_string(),
    ]);
    run_cli(world, &node, arguments).await;
}

/// Samples public executor gauges and process heap statistics only while the public CLI restores.
/// Heap statistics include every cluster in this harness process; run one example at a time for
/// attributable measurements. Executor peaks are per node and sampled, rather than continuous.
#[when(
    expr = "the CLI restores {string} from {string} on node {string} reporting JSON with memory \
            measurements"
)]
async fn when_cli_restores_with_memory_measurements(
    world: &mut ScenarioWorld,
    scope: String,
    file: String,
    node: String,
) {
    let nodes = world.cluster().node_ids().len();
    let sampler = super::memory::MemorySampler::start(world).await;
    let began = Instant::now();
    when_cli_restores(world, scope, file.clone(), node).await;
    let elapsed = began.elapsed();
    let sampled = sampler.finish().await;
    sampled.assert_no_bulk_refusal("valid restored state incurs no bulk budget refusal");
    assert_eq!(
        sampled.bulk_peaks.len(),
        nodes,
        "each restore target was sampled"
    );
    let archive_bytes = std::fs::metadata(archive_path(world, &file))
        .assured("the archive remains available")
        .len();
    eprintln!(
        "restore generation measurement: {}",
        sampled.evidence(world, archive_bytes, elapsed)
    );
}

/// The report the last restore the CLI ran printed: the whole document of a restore that
/// completed, or the report a failed step's error carries.
fn cli_restore_report(world: &ScenarioWorld) -> serde_json::Value {
    let document = cli_json(last_cli_output(world));
    if document["error"].is_object() {
        let report = document["error"]["report"].clone();
        assert!(report.is_object(), "the error carries a report: {document}");
        return report;
    }
    document
}

#[then(expr = "the CLI restore execution reference is saved as placeholder {string}")]
fn then_cli_restore_reference_is_saved(world: &mut ScenarioWorld, placeholder: String) {
    let report = cli_restore_report(world);
    let reference = report["execution_reference"]
        .as_str()
        .assured("the CLI restore report carries its exact execution reference");
    world
        .placeholders
        .insert(placeholder, reference.to_string());
}

fn succeeded_restore(world: &ScenarioWorld) -> serde_json::Value {
    let output = last_cli_output(world);
    assert!(
        output.status.success(),
        "the restore failed: status {}; stdout: {}; stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    cli_json(output)
}

/// The entry of `report` for the domain restored as `domain`.
fn reported_domain(report: &serde_json::Value, domain: &str) -> serde_json::Value {
    let domains = report["domains"]
        .as_array()
        .unwrap_or_else(|| panic!("the report lists its domains: {report}"));
    for entry in domains {
        if entry["domain"] == domain {
            return entry.clone();
        }
    }
    panic!("the report restores domain '{domain}': {report}");
}

/// Checks that every step of `report` ended as `outcome`.
fn assert_every_step(report: &serde_json::Value, outcome: &str) {
    let steps = report["steps"]
        .as_array()
        .unwrap_or_else(|| panic!("the report lists its steps: {report}"));
    assert!(!steps.is_empty(), "the report lists its steps: {report}");
    for step in steps {
        assert_eq!(step["outcome"], outcome, "the report: {report}");
    }
}

#[then(
    expr = "the CLI restore succeeded, restoring domain {string} with {int} resource versions and \
            {int} models"
)]
fn then_cli_restore_succeeded(
    world: &mut ScenarioWorld,
    domain: String,
    versions: u64,
    models: u64,
) {
    let domain = expand_placeholders(world, &domain);
    let report = succeeded_restore(world);
    assert_eq!(report["mode"], "apply", "the report: {report}");
    assert_every_step(&report, "applied");
    let restored = reported_domain(&report, &domain);
    assert_eq!(
        restored["resource_versions"], versions,
        "the report: {report}"
    );
    assert_eq!(restored["models"], models, "the report: {report}");
}

#[then(expr = "the CLI restore reports domain {string} as {string} at start version {int}")]
fn then_cli_restore_lifecycle(
    world: &mut ScenarioWorld,
    domain: String,
    status: String,
    version: u64,
) {
    let domain = expand_placeholders(world, &domain);
    let report = succeeded_restore(world);
    let restored = reported_domain(&report, &domain);
    assert_eq!(restored["status"], status, "the report: {report}");
    assert_eq!(restored["start_version"], version, "the report: {report}");
    assert_every_step(
        &report,
        if report["mode"] == "dry_run" {
            "planned"
        } else {
            "applied"
        },
    );
}

#[then(expr = "every node reports the resumed lifecycle archived in {string}")]
async fn then_every_node_reports_the_archived_lifecycle(world: &mut ScenarioWorld, file: String) {
    let description = nervix_backup::describe_archive(
        std::fs::File::open(archive_path(world, &file)).expect("archive opens"),
    )
    .expect("archive describes");
    let archived = &description
        .domains
        .iter()
        .find(|domain| domain.record.domain.as_str() == world.domain)
        .expect("the archive contains the active domain")
        .record;
    for node in world.cluster().node_ids() {
        let output = world
            .cluster()
            .run_command(&node, &world.domain, "DESCRIBE DOMAIN;")
            .await
            .expect("the restored domain describes");
        assert!(output.contains("status: running"), "{node}: {output}");
        assert!(
            output.contains(&format!("start version: {}", archived.start_version)),
            "{node}: {output}"
        );
        assert!(
            output.contains(&format!("start point: {:?}", archived.start_point)),
            "{node}: {output}"
        );
        let state = match &archived.clock {
            Some(mapping) => {
                let nervix_models::DomainPace::Paced { period, skew } = archived.pace else {
                    panic!("a mapped archive is paced");
                };
                nervix_models::DomainClockObservedState::Paced(nervix_models::PacedDomainClock {
                    period,
                    skew,
                    mapping: mapping.clone(),
                })
            }
            None => nervix_models::DomainClockObservedState::Unpaced,
        };
        let expected = nervix_models::DomainClockObservation {
            generation: archived.start_version,
            state,
        };
        let deadline = nervix_primitives::time::Instant::now() + Duration::from_secs(60);
        loop {
            let clock = world
                .cluster()
                .run_command(&node, &world.domain, "ATTACH DOMAIN CLOCK;")
                .await
                .expect("the restored clock attaches");
            if clock.contains(&expected.to_string()) {
                break;
            }
            assert!(
                nervix_primitives::time::Instant::now() < deadline,
                "{node}: {clock}; expected {expected}"
            );
            nervix_primitives::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[then(expr = "the CLI restore warns that saved state has a mismatched schema")]
fn then_cli_restore_warns_about_state_schema(world: &mut ScenarioWorld) {
    let report = succeeded_restore(world);
    let warnings = report["warnings"]
        .as_array()
        .unwrap_or_else(|| panic!("the restore report lists warnings: {report}"));
    assert!(
        warnings.iter().any(|warning| warning
            .as_str()
            .is_some_and(|message| message.contains("archived schema fingerprint does not match"))),
        "the restore reports the skipped state: {report}"
    );
}

/// The warnings a successful restore the CLI ran reported.
fn restore_warnings(world: &ScenarioWorld) -> Vec<String> {
    let report = succeeded_restore(world);
    let warnings = report["warnings"]
        .as_array()
        .unwrap_or_else(|| panic!("the restore report lists warnings: {report}"));
    warnings
        .iter()
        .map(|warning| {
            warning
                .as_str()
                .unwrap_or_else(|| panic!("a warning is text: {report}"))
                .to_string()
        })
        .collect()
}

#[then(expr = "the CLI restore warns {string}")]
fn then_cli_restore_warns(world: &mut ScenarioWorld, expected: String) {
    let expected = expand_placeholders(world, &expected);
    let warnings = restore_warnings(world);
    assert!(
        warnings.iter().any(|warning| warning.contains(&expected)),
        "no restore warning contains {expected:?}: {warnings:?}"
    );
}

#[then(expr = "the CLI restore reports no warnings")]
fn then_cli_restore_reports_no_warnings(world: &mut ScenarioWorld) {
    let warnings = restore_warnings(world);
    assert!(warnings.is_empty(), "the restore warned: {warnings:?}");
}

#[then(
    expr = "the CLI restore succeeded with users {int} created, {int} skipped and {int} replaced"
)]
fn then_cli_restore_succeeded_with_users(
    world: &mut ScenarioWorld,
    created: u64,
    skipped: u64,
    replaced: u64,
) {
    let report = succeeded_restore(world);
    assert_every_step(&report, "applied");
    let users = &report["users"];
    assert_eq!(users["created"], created, "the report: {report}");
    assert_eq!(users["skipped"], skipped, "the report: {report}");
    assert_eq!(users["replaced"], replaced, "the report: {report}");
}

#[then(
    expr = "the CLI dry run planned domain {string} with {int} resource versions and {int} models"
)]
fn then_cli_dry_run_planned(world: &mut ScenarioWorld, domain: String, versions: u64, models: u64) {
    let domain = expand_placeholders(world, &domain);
    let report = succeeded_restore(world);
    assert_eq!(report["mode"], "dry_run", "the report: {report}");
    assert_every_step(&report, "planned");
    let planned = reported_domain(&report, &domain);
    assert_eq!(
        planned["resource_versions"], versions,
        "the report: {report}"
    );
    assert_eq!(planned["models"], models, "the report: {report}");
    assert!(
        planned["planned_models"].is_object(),
        "a dry run reports the domain's model run: {report}"
    );
}

#[then(
    expr = "the CLI restore failed with JSON error code {string} and a message containing {string}"
)]
fn then_cli_restore_failed(world: &mut ScenarioWorld, code: String, message: String) {
    let message = expand_placeholders(world, &message);
    let output = last_cli_output(world);
    assert!(
        !output.status.success(),
        "the restore succeeded: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let document = cli_json(output);
    assert_eq!(
        document["error"]["code"],
        code.as_str(),
        "the report: {document}"
    );
    let reported = document["error"]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("the error has a message: {document}"));
    assert!(
        reported.contains(&message),
        "the error message contains '{message}': {reported}"
    );
}

#[then(expr = "the CLI restore report shows step {string} as {string}")]
fn then_cli_restore_report_shows_step(
    world: &mut ScenarioWorld,
    step_name: String,
    outcome: String,
) {
    let step = expand_placeholders(world, &step_name);
    let report = cli_restore_report(world);
    let steps = report["steps"]
        .as_array()
        .unwrap_or_else(|| panic!("the report lists its steps: {report}"));
    let mut reported = None;
    for entry in steps {
        if entry["step"] == step.as_str() {
            reported = Some(entry["outcome"].clone());
        }
    }
    let Some(reported) = reported else {
        panic!("the report lists step '{step}': {report}");
    };
    assert_eq!(reported, outcome.as_str(), "step '{step}' in {report}");
}

/// What one restore stream a scenario shapes carries, owned so it can be sent from a task.
struct RestoreStreamRequest {
    server: String,
    reference: CommandExecutionReference,
    statement: String,
    archive: Vec<u8>,
}

fn restore_stream_request(
    world: &mut ScenarioWorld,
    restore: &str,
    file: &str,
    node: &str,
    reference: &str,
) -> RestoreStreamRequest {
    let path = archive_path(world, file);
    let archive = std::fs::read(&path).expect("the archive exists");
    let scope = expand_placeholders(world, restore);
    let (scope, lifecycle) = scope
        .strip_suffix(" RESUME")
        .map_or((scope.as_str(), ""), |scope| (scope, " RESUME"));
    let node = expand_placeholders(world, node);
    let reference = crate::command_execution_reference(world, reference);
    RestoreStreamRequest {
        server: world
            .cluster()
            .grpc_uri(&node)
            .expect("the scenario names a cluster node"),
        reference: CommandExecutionReference::parse(reference)
            .expect("scenario execution references are valid"),
        statement: format!("RESTORE {scope} FROM '{}'{lifecycle};", path.display()),
        archive,
    }
}

async fn stream_restore(
    request: &RestoreStreamRequest,
    declared_digest: Option<[u8; 32]>,
    abandon_after_chunks: Option<usize>,
) -> TestRestoreEnd {
    send_restore(
        &request.server,
        TestRestore {
            reference: &request.reference,
            statement: &request.statement,
            archive: &request.archive,
            declared_digest,
            abandon_after_chunks,
        },
    )
    .await
    .expect("the restore stream is sent")
}

#[when(
    expr = "restore {string} of backup archive {string} is streamed to node {string} under \
            execution reference {string}"
)]
async fn when_restore_is_streamed(
    world: &mut ScenarioWorld,
    restore: String,
    file: String,
    node: String,
    reference: String,
) {
    let request = restore_stream_request(world, &restore, &file, &node, &reference);
    world.last_restore_end = Some(stream_restore(&request, None, None).await);
}

#[when(
    expr = "restore {string} of backup archive {string} is streamed to node {string} under a \
            fresh execution reference"
)]
async fn when_restore_is_streamed_under_a_fresh_reference(
    world: &mut ScenarioWorld,
    restore: String,
    file: String,
    node: String,
) {
    let reference = crate::common::raw_session::fresh_execution_reference();
    let request = restore_stream_request(world, &restore, &file, &node, &reference);
    world.last_restore_end = Some(stream_restore(&request, None, None).await);
}

#[when(
    expr = "restore {string} of backup archive {string} is streamed to node {string} under \
            execution reference {string} and abandoned after {int} chunks"
)]
async fn when_restore_is_streamed_and_abandoned(
    world: &mut ScenarioWorld,
    restore: String,
    file: String,
    node: String,
    reference: String,
    chunks: usize,
) {
    let request = restore_stream_request(world, &restore, &file, &node, &reference);
    world.last_restore_end = Some(stream_restore(&request, None, Some(chunks)).await);
}

#[when(
    expr = "restore {string} of backup archive {string} is streamed to node {string} declaring \
            the digest of {string}"
)]
async fn when_restore_is_streamed_declaring_another_digest(
    world: &mut ScenarioWorld,
    restore: String,
    file: String,
    node: String,
    other: String,
) {
    let other = std::fs::read(archive_path(world, &other)).expect("the other archive exists");
    let reference = crate::common::raw_session::fresh_execution_reference();
    let request = restore_stream_request(world, &restore, &file, &node, &reference);
    assert_eq!(
        other.len(),
        request.archive.len(),
        "the declared digest belongs to an archive of the same size"
    );
    let digest = *blake3::hash(&other).as_bytes();
    world.last_restore_end = Some(stream_restore(&request, Some(digest), None).await);
}

#[when(
    expr = "restore {string} of backup archive {string} is streamed to node {string} under \
            execution reference {string} in the background"
)]
async fn when_restore_is_streamed_in_the_background(
    world: &mut ScenarioWorld,
    restore: String,
    file: String,
    node: String,
    reference: String,
) {
    assert!(
        world.background_restore.is_none(),
        "a background restore stream is already running"
    );
    let request = restore_stream_request(world, &restore, &file, &node, &reference);
    world.background_restore = Some(AbortOnDropHandle::new(nervix_primitives::task::spawn(
        async move { stream_restore(&request, None, None).await },
    )));
}

/// The command outcome a restore stream was answered with.
fn restore_outcome(end: &TestRestoreEnd) -> &WireCommandOutcome {
    let reply = match end {
        TestRestoreEnd::Replied(reply) => reply,
        TestRestoreEnd::Abandoned => panic!("the restore stream was abandoned before its answer"),
        TestRestoreEnd::Status(status) => panic!("the restore call failed: {status}"),
    };
    let RestoreDisposition::Outcome(outcome) = &reply.disposition else {
        panic!(
            "the restore stream was answered with an outcome: {:?}",
            reply.disposition
        );
    };
    outcome
}

fn assert_restore_outcome(end: &TestRestoreEnd, disposition: &str, origin: &str) {
    let outcome = restore_outcome(end);
    let matched = matches!(
        (disposition, &outcome.disposition),
        ("completed", CommandDisposition::Completed { .. })
            | ("failed", CommandDisposition::Failed)
            | ("redirected", CommandDisposition::NotLeader(_))
            | (
                "reference conflict",
                CommandDisposition::ExecutionReferenceConflict(_)
            )
            | (
                "still applying",
                CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::StillApplying),
            )
    );
    assert!(
        matched,
        "the restore stream's outcome is '{disposition}': {:?} {}",
        outcome.disposition, outcome.message
    );
    let expected = match origin {
        "executed" => OutcomeOrigin::Executed,
        "recovered" => OutcomeOrigin::Recovered,
        other => panic!("an outcome has no origin '{other}'"),
    };
    assert_eq!(outcome.origin, expected, "{}", outcome.message);
}

#[then(expr = "the restore stream's outcome is {string} as {string}")]
fn then_restore_stream_outcome(world: &mut ScenarioWorld, disposition: String, origin: String) {
    let end = world
        .last_restore_end
        .as_ref()
        .expect("a preceding step streamed a restore");
    assert_restore_outcome(end, &disposition, &origin);
}

#[then(expr = "the background restore stream's outcome is {string} as {string}")]
async fn then_background_restore_stream_outcome(
    world: &mut ScenarioWorld,
    disposition: String,
    origin: String,
) {
    let task = world
        .background_restore
        .take()
        .expect("a preceding step streamed a restore in the background");
    let end = task
        .await
        .expect("the background restore stream does not panic");
    assert_restore_outcome(&end, &disposition, &origin);
    world.last_restore_end = Some(end);
}

#[then("the restore stream's report shows every step applied")]
fn then_restore_stream_report_shows_every_step_applied(world: &mut ScenarioWorld) {
    let end = world
        .last_restore_end
        .as_ref()
        .expect("a preceding step streamed a restore");
    let outcome = restore_outcome(end);
    let Some(report) = outcome.restore.as_deref() else {
        panic!("the outcome carries a restore report: {}", outcome.message);
    };
    assert!(!report.steps.is_empty(), "the report lists its steps");
    for step in &report.steps {
        assert_eq!(
            step.outcome,
            nervix_models::RestoreStepOutcome::Applied,
            "step {} of {report:?}",
            step.step
        );
    }
}

#[then(expr = "the restore stream is refused as {string}")]
fn then_restore_stream_is_refused(world: &mut ScenarioWorld, failure: String) {
    let end = world
        .last_restore_end
        .as_ref()
        .expect("a preceding step streamed a restore");
    let TestRestoreEnd::Replied(reply) = end else {
        panic!("the restore stream was answered: {end:?}");
    };
    let RestoreDisposition::UploadFailed {
        failure: refused, ..
    } = &reply.disposition
    else {
        panic!("the restore stream was refused: {:?}", reply.disposition);
    };
    let expected = match failure.as_str() {
        "InvalidStream" => RestoreUploadFailure::InvalidStream,
        "InvalidStatement" => RestoreUploadFailure::InvalidStatement,
        "SizeMismatch" => RestoreUploadFailure::SizeMismatch,
        "DigestMismatch" => RestoreUploadFailure::DigestMismatch,
        "QuotaExceeded" => RestoreUploadFailure::QuotaExceeded,
        "StagingFailed" => RestoreUploadFailure::StagingFailed,
        other => panic!("a restore stream has no failure '{other}'"),
    };
    assert_eq!(*refused, expected, "{:?}", reply.disposition);
}

#[then("the restore stream was abandoned")]
fn then_restore_stream_was_abandoned(world: &mut ScenarioWorld) {
    let end = world
        .last_restore_end
        .as_ref()
        .expect("a preceding step streamed a restore");
    assert!(
        matches!(end, TestRestoreEnd::Abandoned),
        "the node did not answer the abandoned stream: {end:?}"
    );
}

/// The step of a restore a scenario names for `domain`.
fn restore_step(world: &ScenarioWorld, kind: &str, domain: &str) -> RestoreStep {
    let domain = scenario_domain(world, domain);
    match kind {
        "create domain" => RestoreStep::CreateDomain(domain),
        "import resources" => RestoreStep::ImportResources(domain),
        "apply models" => RestoreStep::ApplyModels(domain),
        other => panic!("a domain has no restore step '{other}'"),
    }
}

#[given(
    expr = "restore step {string} of domain {string} on node {string} pauses before it applies"
)]
fn given_restore_step_pauses(
    world: &mut ScenarioWorld,
    kind: String,
    domain: String,
    node: String,
) {
    let step = restore_step(world, &kind, &domain);
    let node = expand_placeholders(world, &node);
    world
        .fault_injection
        .pause_restore_step_on(node_name(&node), step.clone());
    world.restore_step_pause = Some(ArmedRestorePause { node, step });
}

#[then(expr = "the restore pauses at step {string} of domain {string} on node {string}")]
async fn then_restore_pauses_at_step(
    world: &mut ScenarioWorld,
    kind: String,
    domain: String,
    node: String,
) {
    let step = restore_step(world, &kind, &domain);
    let node = expand_placeholders(world, &node);
    nervix_primitives::time::timeout(
        RESTORE_PAUSE_TIMEOUT,
        world
            .fault_injection
            .wait_for_restore_step_pause(&node_name(&node), &step),
    )
    .await
    .unwrap_or_else(|_| panic!("the restore did not reach step {step} on node '{node}'"));
}

#[when(expr = "the restore step pause on node {string} is released")]
fn when_restore_step_pause_is_released(world: &mut ScenarioWorld, node: String) {
    let node = expand_placeholders(world, &node);
    let pause = world
        .restore_step_pause
        .take()
        .expect("a preceding step armed a restore step pause");
    assert_eq!(pause.node, node, "the pause was armed on another node");
    world
        .fault_injection
        .release_restore_step_pause(&node_name(&pause.node), &pause.step);
}

#[given(
    expr = "restoring domain {string} pauses before it converts its first deduplicator or window \
            state"
)]
fn given_restore_conversion_pauses(world: &mut ScenarioWorld, domain: String) {
    world
        .fault_injection
        .pause_restore_branch_state_conversion(scenario_domain(world, &domain));
}

#[then(
    expr = "the restore of domain {string} pauses before it converts its first deduplicator or \
            window state"
)]
async fn then_restore_conversion_pauses(world: &mut ScenarioWorld, domain: String) {
    let domain = scenario_domain(world, &domain);
    nervix_primitives::time::timeout(
        RESTORE_PAUSE_TIMEOUT,
        world
            .fault_injection
            .wait_for_restore_branch_state_conversion_pause(&domain),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("the restore of '{domain}' did not reach its first deduplicator or window state")
    });
}

#[when(expr = "the paused restore conversion of domain {string} is released")]
fn when_restore_conversion_is_released(world: &mut ScenarioWorld, domain: String) {
    let domain = scenario_domain(world, &domain);
    world
        .fault_injection
        .release_restore_branch_state_conversion_pause(&domain);
}

/// Writes `copy` to `target` with `sections` in place of the sections of the same path.
pub(super) fn write_archive(
    copy: &ArchiveCopy,
    replaced: &BTreeMap<String, Vec<u8>>,
    target: &Path,
) {
    let mut manifest = copy
        .manifest
        .clone()
        .expect("a verified archive has a manifest");
    for entry in &mut manifest.sections {
        if let Some(bytes) = replaced.get(entry.path.as_str()) {
            entry.length = u64::try_from(bytes.len()).expect("a section length fits 64 bits");
            entry.digest = SectionDigester::digest_of(bytes);
        }
    }
    let layout = ArchiveLayout::new(manifest).expect("the altered manifest lays out");
    // An altered archive is as sensitive as the one it was copied from: only its owner reads it.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(target)
        .expect("the altered archive is created");
    layout
        .write_to(&mut file, |entry, sink| {
            let path = entry.path.as_str();
            let bytes = match replaced.get(path) {
                Some(bytes) => bytes,
                None => &copy.sections[path],
            };
            sink.write_all(bytes)
        })
        .unwrap_or_else(|report| panic!("the altered archive is written: {report:?}"));
}

/// Returns `stream` with the body length its first record batch message declares set to
/// `declared`, leaving every other byte, and so the stream's actual body, as it was written.
fn with_declared_record_batch_body(mut stream: Vec<u8>, declared: i64) -> Vec<u8> {
    let mut offset = 0_usize;
    loop {
        let length_start = offset
            .checked_add(4)
            .assured("the archived stream is small");
        let metadata_start = offset
            .checked_add(8)
            .assured("the archived stream is small");
        assert_eq!(
            stream[offset..length_start],
            [0xff; 4],
            "every archived message starts with the continuation marker"
        );
        let length_word: [u8; 4] = stream[length_start..metadata_start]
            .try_into()
            .assured("the length word is the four bytes after the continuation marker");
        let metadata_length = usize::try_from(i32::from_le_bytes(length_word))
            .assured("a written message declares a nonnegative metadata length");
        assert_ne!(
            metadata_length, 0,
            "the archived section reaches a record batch before its end-of-stream marker"
        );
        let metadata_end = metadata_start
            .checked_add(metadata_length)
            .assured("the archived stream is small");
        let message = arrow_ipc::root_as_message(&stream[metadata_start..metadata_end])
            .assured("the archived message verifies before its declaration is altered");
        if message.header_type() == arrow_ipc::MessageHeader::RecordBatch {
            let field = message._tab.vtable().get(arrow_ipc::Message::VT_BODYLENGTH);
            assert_ne!(
                field, 0,
                "the archived record batch declares its body length"
            );
            let table = metadata_start
                .checked_add(message._tab.loc())
                .assured("the message table lies within its metadata");
            let field_start = table
                .checked_add(usize::from(field))
                .assured("the body length field lies within its metadata");
            let field_end = field_start
                .checked_add(8)
                .assured("the body length field lies within its metadata");
            stream[field_start..field_end].copy_from_slice(&declared.to_le_bytes());
            return stream;
        }
        let body_length = usize::try_from(message.bodyLength())
            .assured("a written message declares a nonnegative body length");
        offset = metadata_end
            .checked_add(body_length)
            .assured("the archived stream is small");
    }
}

#[given(
    expr = "backup archive {string} is copied to {string} with a materialized Arrow body \
            declaring {int} bytes"
)]
fn given_archive_with_declared_arrow_body(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    declared: i64,
) {
    let copy = copy_of_archive(&archive_path(world, &source));
    let manifest = copy
        .manifest
        .as_ref()
        .assured("a verified archive has a manifest");
    let entry = manifest
        .sections
        .iter()
        .find(|entry| entry.content == SectionContent::MaterializedColumns)
        .assured("the backed-up materialized relay has an Arrow column section");
    let section = copy
        .sections
        .get(entry.path.as_str())
        .assured("the verified archive holds every section its manifest lists");
    let altered = with_declared_record_batch_body(section.clone(), declared);
    // The rewritten manifest carries the altered section's own digest, so only the Arrow scan
    // can refuse it.
    let replaced = BTreeMap::from([(entry.path.to_string(), altered)]);
    write_archive(&copy, &replaced, &archive_path(world, &target));
}

#[then("every node still answers cluster status")]
async fn then_every_node_answers_cluster_status(world: &mut ScenarioWorld) {
    for node in world.cluster().node_ids() {
        let status = world
            .cluster()
            .status_text(&node, PhaseDeadline::after(STATUS_REQUEST_TIMEOUT))
            .await
            .unwrap_or_else(|report| panic!("node '{node}' answers cluster status: {report:?}"));
        assert!(
            !status.is_empty(),
            "node '{node}' returned an empty cluster status"
        );
    }
}

#[given(
    expr = "backup archive {string} is copied to {string} with native metadata above the bulk \
            budget"
)]
fn given_large_native_metadata(world: &mut ScenarioWorld, source: String, target: String) {
    use nervix_backup::{
        BranchLifecycleEntry, KafkaOffsetsRecord, KafkaPartitionOffset, StateField, StateValue,
    };

    let copy = copy_of_archive(&archive_path(world, &source));
    let mut replaced = BTreeMap::new();
    for (path, bytes) in &copy.sections {
        if path.contains("/state/branch_lifecycle/ingestor/metric_source/") {
            let mut record = BranchLifecycleRecord::decode(path, bytes)
                .assured("the fixture lifecycle is current");
            let last_ingestion = record
                .branches
                .first()
                .assured("the ingestor captured an active branch")
                .last_ingestion;
            for index in 0..1024_u64 {
                record.branches.push(BranchLifecycleEntry {
                    key: Some(vec![StateField {
                        name: "tenant".to_string(),
                        value: StateValue::String(format!(
                            "metadata-{index:04}-{}",
                            "x".repeat(34 * 1024)
                        )),
                    }]),
                    last_ingestion,
                    incarnation: 1000 + index,
                });
            }
            let encoded = record
                .encode()
                .assured("valid large lifecycle metadata encodes");
            assert!(encoded.len() > 32 * 1024 * 1024);
            replaced.insert(path.clone(), encoded);
        }
        if path.contains("/state/kafka_offset/") {
            let mut record =
                KafkaOffsetsRecord::decode(path, bytes).assured("the fixture offsets are current");
            for partition in 0..350_000_i32 {
                record.offsets.push(KafkaPartitionOffset {
                    topic: format!("metadata-{}", "z".repeat(100)),
                    partition,
                    next_offset: i64::from(partition) + 17,
                });
            }
            let encoded = record
                .encode()
                .assured("valid large offset metadata encodes");
            assert!(encoded.len() > 32 * 1024 * 1024);
            replaced.insert(path.clone(), encoded);
        }
    }
    assert_eq!(
        replaced.len(),
        2,
        "both native metadata records were enlarged"
    );
    write_archive(&copy, &replaced, &archive_path(world, &target));
}

#[then(
    expr = "stopped restored nodes preserve every native metadata value from backup archive \
            {string}"
)]
async fn then_native_metadata_matches(world: &mut ScenarioWorld, source: String) {
    use nervix_backup::KafkaOffsetsRecord;

    let source = copy_of_archive(&archive_path(world, &source));
    let mut lifecycles = Vec::new();
    let mut offsets = None;
    for (path, bytes) in &source.sections {
        if path.contains("/state/branch_lifecycle/") {
            lifecycles.push(
                BranchLifecycleRecord::decode(path, bytes).assured("source lifecycle verifies"),
            );
        }
        if path.contains("/state/kafka_offset/") {
            offsets =
                Some(KafkaOffsetsRecord::decode(path, bytes).assured("source offsets verify"));
        }
    }
    assert!(
        !lifecycles.is_empty(),
        "the fixture holds lifecycle records"
    );
    let offsets = offsets.assured("the fixture holds Kafka offsets");
    let domain = scenario_domain(world, &world.domain);
    let nodes = world.cluster().node_ids();
    world
        .cluster_mut()
        .shutdown()
        .await
        .assured("all test node databases close before inspection");
    let mut lifecycle_copies = vec![0; lifecycles.len()];
    let mut offset_copies = 0;
    for node in &nodes {
        nervix_primitives::task::consume_budget().await;
        let path = world
            .cluster()
            .node_base_dir(node)
            .assured("the node directory exists")
            .join("db");
        let (found, offsets_found) = native_restore_inspection::assert_native_checkpoint_values(
            &path,
            &domain,
            &lifecycles,
            &offsets,
        );
        println!(
            "Native checkpoint copies on {node}: lifecycles={found:?}, offsets={offsets_found}"
        );
        for (copies, found) in lifecycle_copies.iter_mut().zip(found) {
            *copies += usize::from(found);
        }
        offset_copies += usize::from(offsets_found);
    }
    let required_copies = if nodes.len() > 1 { 2 } else { 1 };
    for (lifecycle, copies) in lifecycles.iter().zip(lifecycle_copies) {
        assert!(
            copies >= required_copies,
            "every complete lifecycle exists on its primary and replicas: {} {} has {copies}, \
             required {required_copies}",
            lifecycle.owner_kind.as_str(),
            lifecycle.entity
        );
    }
    assert!(
        offset_copies >= required_copies,
        "the complete offset set exists on its primary and replicas"
    );
}

#[given(expr = "backup archive {string} is copied to {string} with mismatched WASM state schemas")]
fn given_archive_with_mismatched_wasm_schemas(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let copy = copy_of_archive(&archive_path(world, &source));
    let mut replaced = BTreeMap::new();
    for (path, bytes) in &copy.sections {
        if path.contains("/state/wasm_processor/") && path.ends_with("/descriptor.rkyv") {
            let mut descriptor = WasmStateDescriptor::decode(path, bytes)
                .expect("the saved WASM descriptor decodes");
            descriptor.schema = SchemaFingerprint::from_digest([0xAA; 32]);
            replaced.insert(
                path.clone(),
                descriptor.encode().expect("the altered descriptor encodes"),
            );
        }
    }
    assert!(
        !replaced.is_empty(),
        "the archive contains WASM guest state"
    );
    write_archive(&copy, &replaced, &archive_path(world, &target));
}

#[given(
    expr = "backup archive {string} is copied to {string} with an unsupported Kafka state version"
)]
fn given_archive_with_unsupported_kafka_state(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let copy = copy_of_archive(&archive_path(world, &source));
    let mut replaced = BTreeMap::new();
    for (path, bytes) in &copy.sections {
        if path.contains("/state/kafka_offset/") && path.ends_with("/offsets.rkyv") {
            let mut changed = bytes.clone();
            changed[10..12].copy_from_slice(&999_u16.to_le_bytes());
            replaced.insert(path.clone(), changed);
        }
    }
    assert!(
        !replaced.is_empty(),
        "the archive contains Kafka domain offsets"
    );
    write_archive(&copy, &replaced, &archive_path(world, &target));
}

#[given(
    expr = "backup archive {string} is copied to {string} with the archived branch incarnations \
            of window processor {string} advanced"
)]
fn given_archive_with_advanced_window_incarnations(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    processor: String,
) {
    let copy = copy_of_archive(&archive_path(world, &source));
    let suffix = format!("/state/branch_lifecycle/window_processor/{processor}/branches.rkyv");
    let mut replaced = BTreeMap::new();
    for (path, bytes) in &copy.sections {
        if !path.ends_with(&suffix) {
            continue;
        }
        let mut lifecycle = BranchLifecycleRecord::decode(path, bytes)
            .expect("the archived branch lifecycle decodes");
        for branch in &mut lifecycle.branches {
            branch.incarnation = branch
                .incarnation
                .checked_add(1)
                .expect("an archived incarnation is below the largest u64");
        }
        replaced.insert(
            path.clone(),
            lifecycle.encode().expect("the advanced lifecycle encodes"),
        );
    }
    assert!(
        !replaced.is_empty(),
        "the archive holds the branch lifecycle of window processor '{processor}'"
    );
    write_archive(&copy, &replaced, &archive_path(world, &target));
}

#[then(expr = "backup archives {string} and {string} keep the WASM processor branch incarnations")]
fn then_wasm_branch_incarnations_match(
    world: &mut ScenarioWorld,
    source: String,
    restored: String,
) {
    assert_eq!(
        wasm_branch_incarnations(&archive_path(world, &source), 2),
        wasm_branch_incarnations(&archive_path(world, &restored), 2),
        "the restored WASM branches keep their incarnations"
    );
}

fn wasm_branch_incarnations(path: &Path, expected: usize) -> BTreeMap<String, u64> {
    let copy = copy_of_archive(path);
    let mut values = BTreeMap::new();
    for (path, bytes) in &copy.sections {
        if path.contains("/state/branch_lifecycle/")
            && path.ends_with("/filter_even_rows/branches.rkyv")
        {
            let lifecycle = BranchLifecycleRecord::decode(path, bytes)
                .expect("the archived branch lifecycle decodes");
            for branch in lifecycle.branches {
                values.insert(format!("{:?}", branch.key), branch.incarnation);
            }
        }
    }
    assert_eq!(
        values.len(),
        expected,
        "all expected WASM branches were captured"
    );
    values
}

#[then(
    expr = "backup archives {string} and {string} keep all {int} WASM processor branch \
            incarnations"
)]
fn then_all_wasm_branch_incarnations_match(
    world: &mut ScenarioWorld,
    source: String,
    restored: String,
    expected: usize,
) {
    assert_eq!(
        wasm_branch_incarnations(&archive_path(world, &source), expected),
        wasm_branch_incarnations(&archive_path(world, &restored), expected),
        "every restored WASM branch keeps its incarnation"
    );
}

#[given(
    expr = "backup archive {string} is copied to {string} with {string} replaced by {string} in \
            the models of domain {string}"
)]
fn given_archive_with_models_replaced(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    needle: String,
    replacement: String,
    domain: String,
) {
    let source = archive_path(world, &source);
    let target = archive_path(world, &target);
    let domain = scenario_domain(world, &domain);
    let copy = copy_of_archive(&source);
    let models_path = SectionPath::domain_models(&domain).to_string();
    let text =
        String::from_utf8(copy.sections[&models_path].clone()).expect("models.nspl is UTF-8");
    assert!(
        text.contains(&needle),
        "the models of '{domain}' hold '{needle}':\n{text}"
    );
    let altered = text.replace(&needle, &replacement);
    let replaced = BTreeMap::from([(models_path, altered.into_bytes())]);
    write_archive(&copy, &replaced, &target);
}

#[given(
    expr = "backup archive {string} is copied to {string} with one byte of version {int} of \
            resource {string} in domain {string} changed"
)]
fn given_archive_with_a_resource_byte_changed(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    version: u64,
    resource: String,
    domain: String,
) {
    let source = archive_path(world, &source);
    let target = archive_path(world, &target);
    let domain = scenario_domain(world, &domain);
    let copy = copy_of_archive(&source);
    let resource = ResourceName::parse(&resource).expect("scenario resources are valid names");
    let version = std::num::NonZeroU64::new(version).expect("versions are numbered from 1");
    let path = SectionPath::resource_archive(&domain, &resource, version).to_string();
    let offset = usize::try_from(copy.offsets[&path]).expect("an offset fits the address space");
    let length = copy.sections[&path].len();
    let mut bytes = std::fs::read(&source).expect("the archive is readable");
    bytes[offset + length / 2] ^= 0xff;
    std::fs::write(&target, bytes).expect("the altered archive is written");
}

fn streamed_restore_report(world: &ScenarioWorld) -> &nervix_models::RestoreReport {
    let end = world
        .last_restore_end
        .as_ref()
        .assured("a preceding step streamed a restore");
    restore_outcome(end)
        .restore
        .as_deref()
        .assured("the completed outcome carries its full report")
}

#[then(expr = "the restore stream's complete report is saved as placeholder {string}")]
fn then_complete_restore_report_is_saved(world: &mut ScenarioWorld, placeholder: String) {
    let report = serde_json::to_string(streamed_restore_report(world))
        .assured("the current complete report serializes");
    world.placeholders.insert(placeholder, report);
}

#[then(expr = "the restore stream's complete report matches placeholder {string}")]
fn then_complete_restore_report_matches(world: &mut ScenarioWorld, placeholder: String) {
    let saved = world
        .placeholders
        .get(&placeholder)
        .assured("a complete report was saved before restart");
    let expected: nervix_models::RestoreReport =
        serde_json::from_str(saved).assured("the saved current report decodes");
    assert_eq!(streamed_restore_report(world), &expected);
}
