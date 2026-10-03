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

use nervix_backup::{ArchiveLayout, BackupManifest, SectionDigester, SectionPath};
use nervix_client_wire::{
    CommandDisposition, CommandOutcome as WireCommandOutcome, OutcomeOrigin, RestoreDisposition,
    RestoreUploadFailure, UnknownOutcomeCause,
};
use nervix_models::{ResourceName, RestoreStep};
use nervix_primitives::task::AbortOnDropHandle;

use super::*;
use crate::common::{
    cluster::node_name,
    raw_session::{TestRestore, TestRestoreEnd, send_restore},
};

/// How long a restore waits at an armed pause before a scenario gives up on reaching it.
const RESTORE_PAUSE_TIMEOUT: Duration = Duration::from_secs(120);

/// A restore step pause a scenario armed, which it releases by the node it named.
#[derive(Debug, Clone)]
pub(crate) struct ArmedRestorePause {
    node: String,
    step: RestoreStep,
}

/// Every section of an archive: its manifest, each section's bytes by path, and where each section
/// begins in the archive.
#[derive(Default)]
struct ArchiveCopy {
    manifest: Option<BackupManifest>,
    sections: BTreeMap<String, Vec<u8>>,
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
fn copy_of_archive(path: &Path) -> ArchiveCopy {
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
    let node = expand_placeholders(world, node);
    let reference = crate::command_execution_reference(world, reference);
    RestoreStreamRequest {
        server: world
            .cluster()
            .grpc_uri(&node)
            .expect("the scenario names a cluster node"),
        reference: CommandExecutionReference::parse(reference)
            .expect("scenario execution references are valid"),
        statement: format!("RESTORE {scope} FROM '{}';", path.display()),
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

/// Writes `copy` to `target` with `sections` in place of the sections of the same path.
fn write_archive(copy: &ArchiveCopy, replaced: &BTreeMap<String, Vec<u8>>, target: &Path) {
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
    let mut file = std::fs::File::create(target).expect("the altered archive is created");
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
