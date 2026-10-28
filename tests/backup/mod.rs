//! Steps that back up a cluster, download and describe its archives, and check what they hold.
//! [`restore`] restores them.
//!
//! Layer: test harness.
//! - **Owns.** The directory a scenario's archives are written to, the backup a scenario runs
//!   through its own session, the downloads it shapes itself, archives it alters to probe the
//!   reader's refusals, and the checks of what an archive holds.
//! - **Depends on.** The public CLI, the harness's own session and download client, the archive
//!   format's reader, and the language layer to parse an archive's Plexus.
//! - **Must not know.** How the server assembles, retains or streams an archive.

use cucumber::{given, then, when};
use nervix_backup::{SectionContent, SectionEntry, SectionReader, SectionVisitor, read_archive};
use nervix_models::{
    BackupArchiveSummary, CommandExecutionReference, DomainName, Statement, Timestamp,
};
use nervix_plexus::client_statement::{ClientStatement, parse_client_statements};
use nervix_server::BackupCutBoundary;

use super::*;
use crate::common::{
    cluster::test_basic_authorization,
    raw_session::{TestDownload, TestDownloadEnd, download_backup},
    runner_offload::off_the_runner,
    suite_watchdog::SUITE_BUDGET,
};

mod branch_keys;
mod branch_state;
mod cli_lifecycle;
pub(crate) mod console;
mod cut_diagnostics;
mod delivery;
mod fidelity;
mod guest_saves;
mod large_branch_state;
mod materialized;
pub(crate) mod memory;
mod native_metadata;
pub(crate) mod restore;
mod snapshot_receive;
mod staging;
mod wait;

/// The backup a scenario ran through its own session.
#[derive(Debug, Clone)]
pub(crate) struct TestBackup {
    reference: CommandExecutionReference,
    summary: BackupArchiveSummary,
}

/// The slowest three-node restore the CLI completed in the scenarios jobs of Check runs
/// 39683355582 and 39683521277, both on slow runners, rounded up: 291 seconds for a 42 MB archive,
/// and up to 260 seconds for the 84 MB keyspace and window archives. Restores that took longer
/// were ended at the former 300-second budget, so none of them is part of the measurement. A
/// policy input: measure it again when the backup scenarios, their archives or the suite's
/// concurrency change.
const SLOWEST_HEALTHY_CLI_RESTORE: Duration = Duration::from_secs(300);
/// How many times the slowest healthy restore one CLI run may take. A leader change while a
/// restore installs its state makes the next leader install it again once the CLI sends the
/// archive again, and six concurrent local copies of the keyspace and window scenario saw a
/// restore start installing its state three times. A policy input.
const CLI_RESTORE_INSTALLATIONS: u32 = 3;
/// How long a backup or restore the CLI runs may take, download included.
const CLI_BACKUP_TIMEOUT: Duration =
    match SLOWEST_HEALTHY_CLI_RESTORE.checked_mul(CLI_RESTORE_INSTALLATIONS) {
        Some(budget) => budget,
        None => panic!("the CLI budget must fit in Duration"),
    };
const _: () = assert!(
    CLI_BACKUP_TIMEOUT.as_nanos() < SUITE_BUDGET.as_nanos(),
    "a CLI run must end inside the suite budget, so its scenario fails with its own reason"
);

/// The largest `models.plexus` a scenario reads back.
const MODELS_LIMIT: u64 = 64 * 1024 * 1024;

/// Where a scenario's archive named `name` lives.
fn archive_path(world: &mut ScenarioWorld, name: &str) -> PathBuf {
    let name = expand_placeholders(world, name);
    let directory = world.backup_directory.get_or_insert_with(|| {
        tempfile::Builder::new()
            .prefix("nervix-backup-")
            .tempdir()
            .expect("the scenario's backup directory is created")
    });
    directory.path().join(name)
}

/// Reads, verifies and describes the archive at `path`. A scenario's archive can hold hundreds of
/// MiB, so the work runs on a blocking task rather than the runner task every scenario shares.
async fn described_archive(path: PathBuf) -> nervix_backup::ArchiveDescription {
    off_the_runner(move || {
        let file = std::fs::File::open(&path)
            .unwrap_or_else(|error| panic!("archive {} opens: {error}", path.display()));
        nervix_backup::describe_archive(file)
            .unwrap_or_else(|report| panic!("archive {} verifies: {report:?}", path.display()))
    })
    .await
}

fn scenario_domain(world: &ScenarioWorld, raw: &str) -> DomainName {
    DomainName::parse(&expand_placeholders(world, raw)).expect("scenario domains are valid names")
}

fn last_backup(world: &ScenarioWorld) -> &TestBackup {
    world
        .last_backup
        .as_ref()
        .expect("a preceding step ran a backup through the scenario's session")
}

fn last_cli_output(world: &ScenarioWorld) -> &Output {
    world
        .last_cli_output
        .as_ref()
        .expect("a preceding step ran the CLI")
}

/// The one JSON document the CLI printed to standard output.
fn cli_json(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let document = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_else(|| {
            panic!(
                "the CLI printed no JSON; stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            )
        });
    serde_json::from_str(document)
        .unwrap_or_else(|error| panic!("the CLI printed {document:?}, which is not JSON: {error}"))
}

/// The CLI with the connection arguments for `node`, followed by `arguments`.
fn cli_command(
    world: &mut ScenarioWorld,
    node: &str,
    arguments: Vec<String>,
) -> tokio::process::Command {
    let node = expand_placeholders(world, node);
    let grpc_uri = world
        .cluster()
        .grpc_uri(&node)
        .expect("the scenario names a cluster node");
    let mut command = tokio::process::Command::new(scenario_cli_binary());
    command.args([
        "--server",
        &grpc_uri,
        "--domain",
        &world.domain,
        "--username",
        TEST_AUTH_USERNAME,
        "--password",
        TEST_AUTH_PASSWORD,
    ]);
    command.args(arguments);
    command.kill_on_drop(true);
    command
}

/// Runs the CLI with `arguments` after the connection arguments for `node`.
async fn run_cli(world: &mut ScenarioWorld, node: &str, arguments: Vec<String>) {
    let mut command = cli_command(world, node, arguments);
    let output = nervix_primitives::time::timeout(CLI_BACKUP_TIMEOUT, command.output())
        .await
        .expect("the CLI finishes within its budget")
        .expect("the CLI process starts");
    world.last_cli_output = Some(output);
}

#[when(expr = "the CLI backs up {string} from node {string} into {string} reporting JSON")]
async fn when_cli_backs_up(world: &mut ScenarioWorld, scope: String, node: String, file: String) {
    let archive = archive_path(world, &file);
    let mut arguments = vec!["backup".to_string()];
    for argument in expand_placeholders(world, &scope).split_whitespace() {
        arguments.push(argument.to_string());
    }
    arguments.extend([
        "--output".to_string(),
        archive.display().to_string(),
        "--format".to_string(),
        "json".to_string(),
    ]);
    run_cli(world, &node, arguments).await;
}

/// Every cut of the domain that reaches its capture waits there, a cut of a backup that runs again
/// under its execution reference included, until the scenario releases the gate or times its
/// holds.
#[given(expr = "the backup cut for domain {string} will pause before its capture")]
fn given_backup_cut_pauses(world: &mut ScenarioWorld, raw_domain: String) {
    let domain = scenario_domain(world, &raw_domain);
    world
        .fault_injection
        .pause_backup_cut_on(domain, BackupCutBoundary::Capture);
}

#[when(
    expr = "the CLI begins backing up {string} from node {string} into {string} in the background"
)]
async fn when_cli_backup_begins_in_background(
    world: &mut ScenarioWorld,
    scope: String,
    node: String,
    file: String,
) {
    assert!(
        world.background_backup.is_none(),
        "a backup is already running"
    );
    let archive = archive_path(world, &file);
    let node = expand_placeholders(world, &node);
    let grpc_uri = world.cluster().grpc_uri(&node).expect("the node exists");
    let selected_domain = world.domain.clone();
    let scope = expand_placeholders(world, &scope);
    let mut command = tokio::process::Command::new(scenario_cli_binary());
    command.args([
        "--server",
        &grpc_uri,
        "--domain",
        &selected_domain,
        "--username",
        TEST_AUTH_USERNAME,
        "--password",
        TEST_AUTH_PASSWORD,
    ]);
    command.arg("backup");
    command.args(scope.split_whitespace());
    command
        .arg("--output")
        .arg(&archive)
        .args(["--format", "json"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let child = command.spawn().assured("the background CLI process starts");
    let raw_pid = child.id().assured("the newly spawned CLI has a process id");
    world.background_backup_pid = Some(nix::unistd::Pid::from_raw(
        i32::try_from(raw_pid).assured("a process id fits the operating system pid type"),
    ));
    world.background_backup = Some(AbortOnDropHandle::new(nervix_primitives::task::spawn(
        async move {
            nervix_primitives::time::timeout(CLI_BACKUP_TIMEOUT, child.wait_with_output())
                .await
                .expect("the background backup finishes within its budget")
                .expect("the CLI process starts")
        },
    )));
}

#[then(expr = "the backup cut for domain {string} has reached its pause")]
async fn then_backup_cut_paused(world: &mut ScenarioWorld, raw_domain: String) {
    let domain = scenario_domain(world, &raw_domain);
    // The cut reaches its pause after its lease, its pause and its drain, each of which a loaded
    // runner delays; the wait ends as soon as the pause is reached.
    nervix_primitives::time::timeout(
        Duration::from_secs(120),
        world
            .fault_injection
            .wait_for_backup_cut_pause(&domain, BackupCutBoundary::Capture),
    )
    .await
    .expect("the backup reaches the pause before its capture");
}

/// Every cut waiting at the domain's capture continues, and every later one passes it.
#[when(expr = "the backup cut pause for domain {string} is released")]
fn when_backup_cut_pause_released(world: &mut ScenarioWorld, raw_domain: String) {
    let domain = scenario_domain(world, &raw_domain);
    world
        .fault_injection
        .release_backup_cut_pause(&domain, BackupCutBoundary::Capture);
}

/// Every cut of the domain that holds its lease waits there, a cut of a backup that runs again
/// under its execution reference included, until the scenario releases the gate.
#[given(expr = "the backup cut for domain {string} will pause once it holds its lease")]
fn given_backup_lease_pauses(world: &mut ScenarioWorld, raw_domain: String) {
    let domain = scenario_domain(world, &raw_domain);
    world
        .fault_injection
        .pause_backup_cut_on(domain, BackupCutBoundary::Lease);
}

#[then(expr = "the backup cut for domain {string} has reached its lease pause")]
async fn then_backup_lease_paused(world: &mut ScenarioWorld, raw_domain: String) {
    let domain = scenario_domain(world, &raw_domain);
    // The cut reaches this pause after its lease wait, which a loaded runner delays; the wait ends
    // as soon as the pause is reached.
    nervix_primitives::time::timeout(
        Duration::from_secs(120),
        world
            .fault_injection
            .wait_for_backup_cut_pause(&domain, BackupCutBoundary::Lease),
    )
    .await
    .expect("the backup holds its lease and reaches the pause before its reach check");
}

/// Every cut waiting once it holds the domain's lease continues, and every later one passes.
#[when(expr = "the backup lease pause for domain {string} is released")]
fn when_backup_lease_pause_released(world: &mut ScenarioWorld, raw_domain: String) {
    let domain = scenario_domain(world, &raw_domain);
    world
        .fault_injection
        .release_backup_cut_pause(&domain, BackupCutBoundary::Lease);
}

#[then("the background CLI backup finishes")]
async fn then_background_backup_finishes(world: &mut ScenarioWorld) {
    let task = world
        .background_backup
        .take()
        .expect("a preceding step started a backup in the background");
    world.last_cli_output = Some(task.await.expect("the background backup does not panic"));
}

#[then(expr = "the background CLI backup remains pending for {string}")]
async fn then_background_backup_remains_pending(world: &mut ScenarioWorld, duration: String) {
    let duration = nervix_models::parse_duration_text(&duration).expect("a valid duration");
    nervix_primitives::time::sleep(duration).await;
    assert!(
        !world
            .background_backup
            .as_ref()
            .expect("a backup is running")
            .is_finished(),
        "the backup completed before the domain mutation lease was released"
    );
}

#[then(expr = "the CLI backup reports at least {int} records dropped during quiesce")]
fn then_backup_reports_dropped_records(world: &mut ScenarioWorld, minimum: u64) {
    let report = cli_json(last_cli_output(world));
    let domains = report["domains"]
        .as_array()
        .expect("the report names domains");
    let dropped = domains
        .iter()
        .filter_map(|domain| domain["cut"]["dropped_records"].as_u64())
        .sum::<u64>();
    assert!(
        dropped >= minimum,
        "quiesce dropped {dropped} records, expected at least {minimum}: {report}"
    );
}

#[then(expr = "the CLI backup succeeded with a JSON report naming domain {string}")]
async fn then_cli_backup_succeeded(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    let output = last_cli_output(world);
    assert!(
        output.status.success(),
        "the backup failed: status {}; stdout: {}; stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report = cli_json(output);
    let named = report["domains"]
        .as_array()
        .expect("the report lists its domains")
        .iter()
        .any(|entry| entry["domain"] == domain.as_str());
    assert!(named, "the report names domain '{domain}': {report}");
    let path = PathBuf::from(
        report["output"]
            .as_str()
            .expect("the report names its output"),
    );
    let reported_length = report["total_bytes"].as_u64();
    let reported_digest = report["blake3"].as_str().map(ToOwned::to_owned);
    off_the_runner(move || {
        let bytes = std::fs::read(&path).expect("the reported archive exists");
        let length = u64::try_from(bytes.len()).expect("an archive length fits 64 bits");
        assert_eq!(
            Some(length),
            reported_length,
            "the archive has its reported size"
        );
        assert_eq!(
            Some(blake3::hash(&bytes).to_hex().as_str()),
            reported_digest.as_deref(),
            "the archive has its reported digest"
        );
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&path)
            .expect("the archive's metadata is readable")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "only the owner may read an archive");
    })
    .await;
}

#[then("the CLI backup succeeded with a JSON report naming no domain")]
fn then_cli_backup_succeeded_without_domains(world: &mut ScenarioWorld) {
    let output = last_cli_output(world);
    assert!(
        output.status.success(),
        "the backup failed: status {}; stdout: {}; stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report = cli_json(output);
    let domains = report["domains"]
        .as_array()
        .expect("the report lists its domains");
    assert!(domains.is_empty(), "the report names no domain: {report}");
}

#[then(expr = "the CLI backup failed with JSON error code {string}")]
fn then_cli_backup_failed(world: &mut ScenarioWorld, code: String) {
    let output = last_cli_output(world);
    assert!(
        !output.status.success(),
        "the backup succeeded: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let report = cli_json(output);
    assert_eq!(
        report["error"]["code"],
        code.as_str(),
        "the report: {report}"
    );
}

#[then(expr = "the CLI backup failure mentions {string}")]
fn then_cli_backup_failure_mentions(world: &mut ScenarioWorld, expected: String) {
    let expected = expand_placeholders(world, &expected);
    let report = cli_json(last_cli_output(world));
    let message = report["error"]["message"]
        .as_str()
        .expect("the backup failure reports a message");
    assert!(message.contains(&expected), "{message}");
}

#[then(expr = "backup archive {string} does not exist")]
fn then_backup_archive_does_not_exist(world: &mut ScenarioWorld, file: String) {
    let path = archive_path(world, &file);
    assert!(!path.exists(), "'{}' exists", path.display());
}

#[then(expr = "backup archive {string} is larger than {int} MiB")]
fn then_backup_archive_is_larger_than(world: &mut ScenarioWorld, file: String, mebibytes: u64) {
    let path = archive_path(world, &file);
    let length = std::fs::metadata(&path).expect("the archive exists").len();
    assert!(
        length > mebibytes * 1024 * 1024,
        "'{}' is only {length} bytes",
        path.display()
    );
}

/// Collects every `models.plexus` of an archive, keyed by its section path.
#[derive(Default)]
struct ModelsCollector {
    documents: BTreeMap<String, String>,
}

impl SectionVisitor for ModelsCollector {
    fn manifest(
        &mut self,
        _manifest: &nervix_backup::BackupManifest,
    ) -> Result<(), error_stack::Report<nervix_backup::ArchiveReadError>> {
        Ok(())
    }

    fn section(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), error_stack::Report<nervix_backup::ArchiveReadError>> {
        if entry.content != SectionContent::Plexus {
            let mut connector = std::io::sink();
            std::io::copy(content, &mut connector).expect("a section can be read through");
            return Ok(());
        }
        let bytes = content.read_all(entry, MODELS_LIMIT)?;
        let text = String::from_utf8(bytes).expect("models.plexus is UTF-8");
        self.documents.insert(entry.path.to_string(), text);
        Ok(())
    }
}

/// The Plexus an archive holds for `domain`, read after the whole archive verifies on a blocking task.
async fn archived_models(path: PathBuf, domain: DomainName) -> String {
    off_the_runner(move || {
        let file = std::fs::File::open(&path).expect("the archive exists");
        let mut collector = ModelsCollector::default();
        read_archive(std::io::BufReader::new(file), &mut collector)
            .unwrap_or_else(|report| panic!("the archive verifies: {report:?}"));
        let models_path = nervix_backup::SectionPath::domain_models(&domain).to_string();
        collector
            .documents
            .remove(&models_path)
            .unwrap_or_else(|| panic!("the archive holds '{models_path}'"))
    })
    .await
}

/// The canonical Plexus of every model `source` creates, in name order.
fn created_models(source: &str) -> Vec<String> {
    let statements = parse_client_statements(source)
        .unwrap_or_else(|error| panic!("the Plexus parses: {error:?}"));
    let mut models = Vec::new();
    for statement in statements {
        let ClientStatement::Server(Statement::Create(create)) = statement else {
            continue;
        };
        let rendered = create
            .body
            .to_canonical_plexus()
            .expect("a parsed model renders as Plexus");
        models.push(rendered);
    }
    models.sort();
    models
}

#[then(
    expr = "backup archive {string} holds for domain {string} exactly the models these Plexus \
            commands create"
)]
async fn then_archive_holds_exactly_the_models(
    world: &mut ScenarioWorld,
    file: String,
    domain: String,
    #[step] step: &Step,
) {
    let path = archive_path(world, &file);
    let domain = scenario_domain(world, &domain);
    let archived = archived_models(path, domain).await;
    let expected = created_models(&expand_placeholders(world, docstring(step)));
    assert_eq!(
        created_models(&archived),
        expected,
        "models.plexus:\n{archived}"
    );
}

#[then(
    expr = "backup archive {string} holds for domain {string} schema {string} and relays {string} \
            numbered contiguously from 1"
)]
async fn then_archive_holds_a_contiguous_run(
    world: &mut ScenarioWorld,
    file: String,
    domain: String,
    schema: String,
    prefix: String,
) {
    let path = archive_path(world, &file);
    let domain = scenario_domain(world, &domain);
    let archived = archived_models(path, domain).await;
    let statements = parse_client_statements(&archived)
        .unwrap_or_else(|error| panic!("models.plexus parses: {error:?}\n{archived}"));
    let mut schema_found = false;
    let mut numbers = BTreeSet::new();
    for statement in statements {
        let ClientStatement::Server(Statement::Create(create)) = statement else {
            panic!("models.plexus holds only model creations:\n{archived}");
        };
        let name = create.body.name().to_string();
        if name == schema {
            schema_found = true;
        } else if let Some(number) = name.strip_prefix(prefix.as_str()) {
            numbers.insert(number.parse::<u64>().expect("relay suffixes are numbers"));
        }
    }
    assert!(
        schema_found,
        "the archive holds schema '{schema}':\n{archived}"
    );
    let expected: BTreeSet<u64> = (1..=u64::try_from(numbers.len()).expect("counts fit")).collect();
    assert_eq!(
        numbers, expected,
        "the archive holds a gap-free run of relays:\n{archived}"
    );
}

#[when(expr = "the CLI describes backup archive {string} as {word}")]
async fn when_cli_describes_backup(world: &mut ScenarioWorld, file: String, format: String) {
    let leader = current_leader_node(world).await;
    describe_backup_through(world, &file, &format, &leader).await;
}

/// Describes the archive through `node`, for a scenario in which another node is down and the
/// cluster's leader cannot be read from every node.
#[when(expr = "the CLI describes backup archive {string} as {word} through node {string}")]
async fn when_cli_describes_backup_through_node(
    world: &mut ScenarioWorld,
    file: String,
    format: String,
    node: String,
) {
    let node = expand_placeholders(world, &node);
    describe_backup_through(world, &file, &format, &node).await;
}

async fn describe_backup_through(world: &mut ScenarioWorld, file: &str, format: &str, node: &str) {
    let path = archive_path(world, file);
    let statement = format!(
        "DESCRIBE BACKUP '{}' FORMAT {};",
        path.display(),
        format.to_uppercase()
    );
    run_cli(world, node, vec!["--command".to_string(), statement]).await;
}

fn described_backup(world: &ScenarioWorld) -> serde_json::Value {
    let output = last_cli_output(world);
    assert!(
        output.status.success(),
        "DESCRIBE BACKUP failed: status {}; stdout: {}; stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    cli_json(output)
}

#[then(expr = "the described backup has exactly {int} {string} state sections")]
fn then_described_backup_has_state_sections(
    world: &mut ScenarioWorld,
    expected: usize,
    kind: String,
) {
    let description = described_backup(world);
    let actual = description["domains"]
        .as_array()
        .expect("the archive lists its domains")
        .iter()
        .flat_map(|domain| {
            domain["runtime_state"]
                .as_array()
                .expect("the domain lists its state")
        })
        .filter(|section| kind == "any" || section["kind"].as_str() == Some(kind.as_str()))
        .count();
    assert_eq!(actual, expected, "the described backup: {description}");
}

#[then(expr = "the described backup records Kafka domain offset {int} for ingestor {string}")]
fn then_described_backup_records_kafka_domain_offset(
    world: &mut ScenarioWorld,
    expected: i64,
    ingestor: String,
) {
    let description = described_backup(world);
    let sections = description["domains"]
        .as_array()
        .expect("the archive lists its domains")
        .iter()
        .flat_map(|domain| {
            domain["runtime_state"]
                .as_array()
                .expect("the domain lists its state")
        })
        .filter(|section| {
            section["kind"].as_str() == Some("kafka_offsets")
                && section["entity"].as_str() == Some(ingestor.as_str())
        })
        .collect::<Vec<_>>();
    let [section] = sections.as_slice() else {
        panic!("the archive holds one Kafka offset section of '{ingestor}': {description}");
    };
    let offsets = section["positions"]
        .as_array()
        .expect("a Kafka offset section lists its positions")
        .iter()
        .map(|position| position["next_offset"].as_i64())
        .collect::<Vec<_>>();
    assert_eq!(
        offsets,
        vec![Some(expected)],
        "the archived domain offsets of '{ingestor}': {description}"
    );
}

#[then(expr = "the described backup lists the scenario user")]
fn then_described_backup_lists_user(world: &mut ScenarioWorld) {
    let description = described_backup(world);
    let listed = description["users"]
        .as_array()
        .expect("a cluster archive lists its users")
        .iter()
        .any(|user| user == TEST_AUTH_USERNAME);
    assert!(listed, "the users: {}", description["users"]);
}

#[then("the described backup holds no users")]
fn then_described_backup_holds_no_users(world: &mut ScenarioWorld) {
    let description = described_backup(world);
    assert!(
        description["users"].is_null(),
        "the users: {}",
        description["users"]
    );
}

/// The `root_checksum` and `manifest_checksum` `DESCRIBE RESOURCE` prints for `version`.
fn described_checksums(output: &str, version: u64) -> (String, String) {
    let prefix = format!("- version={version} ");
    let line = output
        .lines()
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("DESCRIBE RESOURCE lists version {version}:\n{output}"));
    let mut root = None;
    let mut manifest = None;
    for field in line.split_whitespace() {
        if let Some(value) = field.strip_prefix("root_checksum=") {
            root = Some(value.to_string());
        }
        if let Some(value) = field.strip_prefix("manifest_checksum=") {
            manifest = Some(value.to_string());
        }
    }
    (
        root.expect("the version line has a root checksum"),
        manifest.expect("the version line has a manifest checksum"),
    )
}

async fn check_described_version(
    world: &mut ScenarioWorld,
    version: u64,
    resource: String,
    domain: String,
    node: String,
    archive_included: bool,
) {
    let description = described_backup(world);
    let domain = expand_placeholders(world, &domain);
    let node = expand_placeholders(world, &node);
    let described = world
        .cluster()
        .run_command(&node, &domain, &format!("DESCRIBE RESOURCE {resource};"))
        .await
        .expect("DESCRIBE RESOURCE runs");
    let (root_checksum, manifest_checksum) = described_checksums(&described, version);
    let archived_domain = description["domains"]
        .as_array()
        .expect("the description lists its domains")
        .iter()
        .find(|entry| entry["domain"] == domain.as_str())
        .unwrap_or_else(|| panic!("the description holds domain '{domain}': {description}"))
        .clone();
    let archived_version = archived_domain["resource_versions"]
        .as_array()
        .expect("a domain lists its resource versions")
        .iter()
        .find(|entry| entry["resource"] == resource.as_str() && entry["version"] == version)
        .unwrap_or_else(|| panic!("the description holds {resource} version {version}"))
        .clone();
    assert_eq!(archived_version["state"], "completed");
    assert_eq!(
        archived_version["published"]["root_checksum"],
        root_checksum.as_str()
    );
    assert_eq!(
        archived_version["published"]["manifest_checksum"],
        manifest_checksum.as_str()
    );
    if archive_included {
        let archive_bytes = archived_version["published"]["archive_bytes"].clone();
        assert_eq!(archived_version["archive"]["bytes"], archive_bytes);
        assert!(archived_version["archive"]["blake3"].is_string());
    } else {
        assert!(archived_version["archive"].is_null(), "{archived_version}");
    }
}

#[then(
    expr = "the described backup lists version {int} of resource {string} in domain {string} with \
            the checksums DESCRIBE RESOURCE reports on node {string}"
)]
async fn then_described_version_matches(
    world: &mut ScenarioWorld,
    version: u64,
    resource: String,
    domain: String,
    node: String,
) {
    check_described_version(world, version, resource, domain, node, true).await;
}

#[then(
    expr = "the described backup lists version {int} of resource {string} in domain {string} with \
            the checksums DESCRIBE RESOURCE reports on node {string} and without its archive"
)]
async fn then_described_version_matches_without_archive(
    world: &mut ScenarioWorld,
    version: u64,
    resource: String,
    domain: String,
    node: String,
) {
    check_described_version(world, version, resource, domain, node, false).await;
}

#[given(expr = "node {string} has resource directory {string} holding a {int} MiB file")]
async fn given_resource_directory_holding_a_large_file(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
    mebibytes: usize,
) {
    let base_dir = world
        .cluster()
        .node_base_dir(&node_id)
        .expect("the node has a base directory");
    let resource_dir = base_dir.join("fixtures").join(&placeholder);
    let fixture_dir = resource_dir.clone();
    off_the_runner(move || {
        std::fs::create_dir_all(&fixture_dir).expect("the fixture directory is created");
        // Varied bytes, so nothing between the client and the archive can shrink them.
        let mut bytes = Vec::with_capacity(mebibytes * 1024 * 1024);
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        while bytes.len() < mebibytes * 1024 * 1024 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            bytes.extend_from_slice(&state.to_le_bytes());
        }
        std::fs::write(fixture_dir.join("payload.bin"), bytes)
            .expect("the fixture file is written");
    })
    .await;
    world
        .placeholders
        .insert(placeholder, resource_dir.display().to_string());
}

#[when(expr = "the session backs up the cluster on node {string} without downloading its archive")]
async fn when_session_backs_up_the_cluster(world: &mut ScenarioWorld, node: String) {
    let node = expand_placeholders(world, &node);
    let mut session = world
        .cluster()
        .open_session(&node, &world.domain)
        .await
        .expect("the scenario's session opens");
    let query = "BACKUP CLUSTER TO 'unused.nvxb';";
    let mut outcome = session
        .run_command_result(query)
        .await
        .expect("the backup is answered");
    if outcome_awaits_leader(&outcome) {
        let reference = outcome.execution_reference.clone();
        let scope = session.scope();
        let routes = world.cluster().leader_routes();
        let learned =
            learn_command_outcome(&routes, &scope, query, reference.as_str(), outcome).await;
        outcome = learned.outcome;
    }
    let summary = outcome
        .backup
        .as_deref()
        .unwrap_or_else(|| panic!("the backup completed with an archive: {}", outcome.message))
        .clone();
    world.last_backup = Some(TestBackup {
        reference: outcome.execution_reference,
        summary,
    });
}

async fn download_last_backup(
    world: &mut ScenarioWorld,
    node: &str,
    authorization: Option<&str>,
    abandon_after_chunks: Option<usize>,
) {
    let node = expand_placeholders(world, node);
    let server = world
        .cluster()
        .grpc_uri(&node)
        .expect("the scenario names a cluster node");
    let reference = last_backup(world).reference.clone();
    let end = download_backup(
        &server,
        TestDownload {
            reference: &reference,
            authorization,
            abandon_after_chunks,
        },
    )
    .await
    .expect("the download is answered");
    world.last_backup_download = Some(end);
}

#[when(expr = "the backup's archive download from node {string} is abandoned after {int} chunks")]
async fn when_backup_download_is_abandoned(world: &mut ScenarioWorld, node: String, chunks: usize) {
    let authorization = test_basic_authorization();
    download_last_backup(world, &node, Some(&authorization), Some(chunks)).await;
    let end = world
        .last_backup_download
        .as_ref()
        .expect("the download ran");
    assert!(
        matches!(end, TestDownloadEnd::Abandoned),
        "the download was abandoned mid-stream: {end:?}"
    );
}

#[when(expr = "the backup's archive is downloaded from node {string}")]
async fn when_backup_archive_is_downloaded(world: &mut ScenarioWorld, node: String) {
    let authorization = test_basic_authorization();
    download_last_backup(world, &node, Some(&authorization), None).await;
}

#[when(expr = "the backup's archive is downloaded from node {string} without credentials")]
async fn when_backup_archive_is_downloaded_without_credentials(
    world: &mut ScenarioWorld,
    node: String,
) {
    download_last_backup(world, &node, None, None).await;
}

fn last_download(world: &ScenarioWorld) -> &TestDownloadEnd {
    world
        .last_backup_download
        .as_ref()
        .expect("a preceding step downloaded the backup's archive")
}

#[then("the downloaded archive matches the backup's summary")]
fn then_downloaded_archive_matches(world: &mut ScenarioWorld) {
    let summary = last_backup(world).summary.clone();
    let TestDownloadEnd::Complete { start, bytes } = last_download(world) else {
        panic!("the download completed: {:?}", last_download(world));
    };
    assert_eq!(start.total_bytes, summary.total_bytes);
    assert_eq!(start.digest, summary.digest);
    let length = u64::try_from(bytes.len()).expect("an archive length fits 64 bits");
    assert_eq!(length, summary.total_bytes.get());
    assert_eq!(blake3::hash(bytes).as_bytes(), summary.digest.as_bytes());
}

#[then(expr = "the download is refused as {string}")]
fn then_download_is_refused_as(world: &mut ScenarioWorld, failure: String) {
    let TestDownloadEnd::Refused(refused) = last_download(world) else {
        panic!("the download was refused: {:?}", last_download(world));
    };
    assert_eq!(
        format!("{:?}", refused.failure),
        failure,
        "{}",
        refused.message
    );
}

#[then(expr = "the download is redirected to node {string}")]
fn then_download_is_redirected(world: &mut ScenarioWorld, node: String) {
    let node = expand_placeholders(world, &node);
    let TestDownloadEnd::Redirected(redirect) = last_download(world) else {
        panic!("the download was redirected: {:?}", last_download(world));
    };
    let leader = redirect
        .leader
        .as_ref()
        .expect("the redirect names the leader");
    let expected = world
        .cluster()
        .grpc_uri(&node)
        .expect("the scenario names a cluster node");
    let expected = url::Url::parse(&expected).expect("cluster gRPC URIs are URLs");
    assert_eq!(leader.grpc_uri.as_ref(), Some(&expected));
}

#[then(expr = "the download is sent to node {string}, which assembled the archive")]
fn then_download_is_sent_to_holder(world: &mut ScenarioWorld, node: String) {
    let node = expand_placeholders(world, &node);
    let TestDownloadEnd::SentToHolder(holder) = last_download(world) else {
        panic!(
            "the download was sent to the archive's holder: {:?}",
            last_download(world)
        );
    };
    assert_eq!(holder.node.as_str(), node);
    let expected = world
        .cluster()
        .grpc_uri(&node)
        .expect("the scenario names a cluster node");
    let expected = url::Url::parse(&expected).expect("cluster gRPC URIs are URLs");
    assert_eq!(holder.grpc_uri.as_ref(), Some(&expected));
}

#[then("the download is refused as unauthenticated")]
fn then_download_is_refused_unauthenticated(world: &mut ScenarioWorld) {
    let TestDownloadEnd::Status(status) = last_download(world) else {
        panic!("the download call failed: {:?}", last_download(world));
    };
    assert_eq!(status.code(), tonic::Code::Unauthenticated, "{status:?}");
}

#[when(expr = "the CLI backs up {string} from node {string} to standard output")]
async fn when_cli_backs_up_to_standard_output(
    world: &mut ScenarioWorld,
    scope: String,
    node: String,
) {
    let mut arguments = vec!["backup".to_string()];
    for argument in expand_placeholders(world, &scope).split_whitespace() {
        arguments.push(argument.to_string());
    }
    arguments.extend(["--output".to_string(), "-".to_string()]);
    run_cli(world, &node, arguments).await;
}

#[then(
    "the archive the CLI wrote to standard output verifies and its report went to standard error"
)]
async fn then_standard_output_holds_the_archive(world: &mut ScenarioWorld) {
    let output = last_cli_output(world);
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "the backup failed: status {}; stderr: {stderr}",
        output.status
    );
    let archive = output.stdout.clone();
    off_the_runner(move || {
        nervix_backup::describe_archive(archive.as_slice())
            .unwrap_or_else(|report| panic!("standard output holds a valid archive: {report:?}"))
    })
    .await;
    assert!(
        stderr.contains("backed up domain"),
        "the report went to standard error: {stderr}"
    );
}

#[when(
    expr = "the backup's archive is downloaded from node {string} as user {string} with password \
            {string}"
)]
async fn when_backup_archive_is_downloaded_as_user(
    world: &mut ScenarioWorld,
    node: String,
    user: String,
    password: String,
) {
    let authorization = format!(
        "Basic {}",
        BASE64_STANDARD.encode(format!("{user}:{password}"))
    );
    download_last_backup(world, &node, Some(&authorization), None).await;
}

#[when(expr = "the session on node {string} runs {string}")]
async fn when_session_runs(world: &mut ScenarioWorld, node: String, query: String) {
    let node = expand_placeholders(world, &node);
    let query = expand_placeholders(world, &query);
    let mut session = world
        .cluster()
        .open_session(&node, &world.domain)
        .await
        .expect("the scenario's session opens");
    let outcome = session
        .run_command_result(&query)
        .await
        .expect("the command is answered");
    world.last_session_command = Some(outcome);
}

#[then(expr = "the session's command failed with {string}")]
fn then_session_command_failed_with(world: &mut ScenarioWorld, expected: String) {
    let outcome = world
        .last_session_command
        .as_ref()
        .expect("a preceding step ran a command through the scenario's session");
    assert!(
        matches!(
            outcome.disposition,
            nervix_client_wire::CommandDisposition::Failed
        ),
        "the command failed: {outcome:?}"
    );
    assert!(
        outcome.message.contains(&expected),
        "the message names why: {}",
        outcome.message
    );
}

async fn download_by_reference(
    world: &mut ScenarioWorld,
    node: &str,
    reference: &CommandExecutionReference,
) {
    let node = expand_placeholders(world, node);
    let server = world
        .cluster()
        .grpc_uri(&node)
        .expect("the scenario names a cluster node");
    let authorization = test_basic_authorization();
    let end = download_backup(
        &server,
        TestDownload {
            reference,
            authorization: Some(&authorization),
            abandon_after_chunks: None,
        },
    )
    .await
    .expect("the download is answered");
    world.last_backup_download = Some(end);
}

#[when(
    expr = "an archive is downloaded from node {string} under the session command's execution \
            reference"
)]
async fn when_archive_is_downloaded_under_the_session_command(
    world: &mut ScenarioWorld,
    node: String,
) {
    let reference = world
        .last_session_command
        .as_ref()
        .expect("a preceding step ran a command through the scenario's session")
        .execution_reference
        .clone();
    download_by_reference(world, &node, &reference).await;
}

#[when(
    expr = "an archive is downloaded from node {string} under an execution reference no command \
            used"
)]
async fn when_archive_is_downloaded_under_an_unused_reference(
    world: &mut ScenarioWorld,
    node: String,
) {
    let reference =
        CommandExecutionReference::parse(crate::common::raw_session::fresh_execution_reference())
            .expect("a fresh UUIDv7 is an execution reference");
    download_by_reference(world, &node, &reference).await;
}

#[when("the backup's retry validity has ended")]
async fn when_backup_retry_validity_has_ended(world: &mut ScenarioWorld) {
    let retained_until = last_backup(world).summary.retained_until;
    loop {
        let now = Timestamp::now();
        if now > retained_until {
            break;
        }
        nervix_primitives::time::sleep(Duration::from_millis(50)).await;
    }
}

#[when(
    expr = "backup archive {string} is copied to {string} with its manifest's record {word} set \
            to {int}"
)]
async fn when_archive_is_copied_with_a_foreign_header(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    field: String,
    value: u16,
) {
    let source = archive_path(world, &source);
    let target = archive_path(world, &target);
    // The manifest is the first entry: its 512-byte tar header, then its record, whose header is
    // an eight-byte magic, a little-endian kind tag, and a little-endian format version.
    let offset = match field.as_str() {
        "kind" => 512 + 8,
        "version" => 512 + 8 + 2,
        other => panic!("a record header has no field '{other}'"),
    };
    off_the_runner(move || {
        let mut bytes = Vec::new();
        std::fs::File::open(&source)
            .expect("the source archive exists")
            .read_to_end(&mut bytes)
            .expect("the source archive is readable");
        bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
        std::fs::write(&target, bytes).expect("the altered archive is written");
    })
    .await;
}
