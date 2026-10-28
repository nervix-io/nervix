//! Public Iceberg commit proofs, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** The catalog gate a scenario stands in front of the shared REST catalog, the steps
//!   that hold, refuse and pass one table's commits through it and measure the delay between a
//!   refusal and the commit that retries it, and the observations of what the table, the staging
//!   directory and DESCRIBE EMITTER show of a commit.
//! - **Depends on.** The scenario's dependency endpoints, the Iceberg table reads and DESCRIBE steps
//!   of the shared scenario module, and the catalog gate.
//! - **Must not know.** The Iceberg emitter's private state, its staged-file names beyond their
//!   documented shape, or a catalog's internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use std::time::Duration;

use cucumber::{gherkin::Step, given, then, when};
use futures_util::TryStreamExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::{Instant, timeout};

use super::{
    DescribedEmitterRetry, ICEBERG_REST_ADDR, IcebergCatalogGate, ScenarioWorld,
    common::iceberg_catalog_gate::HeldCommitArrival, current_leader_node, docstring,
    expand_placeholders, iceberg_row_matches_expected, iceberg_table_rows, parse_duration_text,
    run_plexus_commands_on_node, rustfs_iceberg_table, staged_iceberg_arrow_ipc_batches,
};

/// The snapshot summary property that counts every row the table holds at that snapshot.
const TOTAL_RECORDS_PROPERTY: &str = "total-records";

/// How often a step that waits for the table or DESCRIBE looks again.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Stands a gate in front of the Iceberg REST catalog that holds every commit to `table` until the
/// scenario decides it, and records in placeholder `iceberg_catalog_gate` the catalog URI that
/// reaches the catalog through it.
#[given(
    expr = "the Iceberg REST catalog is reached through a gate that holds every commit to table \
            {string}"
)]
async fn given_iceberg_catalog_gate(world: &mut ScenarioWorld, table: String) {
    let table = expand_placeholders(world, &table);
    let catalog = world
        .dependencies
        .endpoints()
        .get(ICEBERG_REST_ADDR)
        .assured("the scenario starts Iceberg dependencies before it gates their catalog");
    let catalog = url::Url::parse(catalog).assured("the Iceberg REST catalog endpoint is a URL");
    let gate = IcebergCatalogGate::start(catalog, &table)
        .await
        .unwrap_or_else(|error| panic!("the Iceberg catalog gate could not listen: {error}"));
    world
        .placeholders
        .insert("iceberg_catalog_gate".to_string(), gate.uri());
    world.iceberg_catalog_gate = Some(gate);
}

fn catalog_gate(world: &ScenarioWorld) -> &IcebergCatalogGate {
    world
        .iceberg_catalog_gate
        .as_ref()
        .verified("an Iceberg catalog gate step runs after the scenario stood its gate up")
}

/// Waits, at most `bound`, for the first commit the gate holds that the scenario has not decided.
async fn await_held_commit(world: &ScenarioWorld, bound: Duration) -> HeldCommitArrival {
    let held = catalog_gate(world).held_commit();
    match timeout(bound, held).await {
        Ok(held) => held.assured("the gate keeps its commits while a step waits for one"),
        Err(_elapsed) => panic!("the Iceberg catalog gate held no commit within {bound:?}"),
    }
}

#[then(expr = "within {string} the Iceberg catalog gate holds a commit")]
async fn then_the_catalog_gate_holds_a_commit(world: &mut ScenarioWorld, bound: String) {
    let bound = parse_duration_text(&bound).assured("the scenario declares a valid bound");
    await_held_commit(world, bound).await;
}

/// Waits for the commit that follows a refusal, and requires that it arrived at least `delay` after
/// the refusal. Load delays the commit and the step's observation of it alike, so the measured gap
/// only grows on a busy machine.
#[then(
    expr = "within {string} the Iceberg catalog gate holds a commit no sooner than {string} after \
            it refused one"
)]
async fn then_the_catalog_gate_holds_a_commit_after_its_refusal(
    world: &mut ScenarioWorld,
    bound: String,
    delay: String,
) {
    let bound = parse_duration_text(&bound).assured("the scenario declares a valid bound");
    let delay = parse_duration_text(&delay).assured("the scenario declares a valid delay");
    let held = await_held_commit(world, bound).await;
    let Some(refused_at) = held.previous_refusal else {
        panic!("the Iceberg catalog gate holds a commit, but it refused none before it");
    };
    let Some(gap) = held.arrived_at.checked_duration_since(refused_at) else {
        panic!("the held commit arrived before the commit the gate refused");
    };
    assert!(
        gap >= delay,
        "the commit after the refusal arrived {gap:?} after it, sooner than {delay:?}"
    );
}

#[when("the Iceberg catalog gate refuses the commit it holds")]
fn when_the_catalog_gate_refuses_the_held_commit(world: &mut ScenarioWorld) {
    let refused = catalog_gate(world).refuse_held_commit();
    assert!(
        refused.is_some(),
        "the Iceberg catalog gate holds no commit to refuse"
    );
}

#[when("the Iceberg catalog gate passes the commit it holds")]
fn when_the_catalog_gate_passes_the_held_commit(world: &mut ScenarioWorld) {
    assert!(
        catalog_gate(world).pass_held_commit(),
        "the Iceberg catalog gate holds no commit to pass"
    );
}

#[when("the Iceberg catalog gate passes every commit")]
fn when_the_catalog_gate_passes_every_commit(world: &mut ScenarioWorld) {
    catalog_gate(world).pass_commits();
}

/// What the staging directory holds now. A commit stages what it publishes before it sends the
/// commit, so while the gate holds the commit the count cannot move.
#[then(expr = "the temp directory holds {int} Iceberg Arrow IPC staged batches")]
fn then_the_temp_directory_holds_staged_batches(world: &mut ScenarioWorld, expected: usize) {
    let temp_root = world
        .temp_root
        .as_ref()
        .verified("the scenario configured its temp directory before the cluster started");
    let staged = staged_iceberg_arrow_ipc_batches(temp_root.path());
    assert_eq!(
        staged,
        expected,
        "the temp directory {} holds {staged} staged Iceberg batches",
        temp_root.path().display()
    );
}

/// The rows a docstring lists, one JSON object per line, each naming the fields it requires.
fn expected_rows(world: &ScenarioWorld, step: &Step) -> Vec<serde_json::Value> {
    let mut rows = Vec::new();
    for line in expand_placeholders(world, docstring(step)).lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let row = serde_json::from_str(line)
            .unwrap_or_else(|error| panic!("expected Iceberg row {line} is not JSON: {error}"));
        rows.push(row);
    }
    rows
}

/// Whether `rows` are exactly `expected`: as many rows, each expected row matching a row of its
/// own.
fn holds_exactly(rows: &[serde_json::Value], expected: &[serde_json::Value]) -> bool {
    if rows.len() != expected.len() {
        return false;
    }
    let mut unmatched = rows.to_vec();
    for wanted in expected {
        // A scenario lists a handful of rows, so finding each one's match is a short scan.
        let Some(found) = unmatched
            .iter()
            .position(|row| iceberg_row_matches_expected(row, wanted))
        else {
            return false;
        };
        unmatched.swap_remove(found);
    }
    true
}

/// The rows the table holds now, read through the catalog itself rather than the gate.
async fn table_rows(world: &ScenarioWorld, table: &str) -> Result<Vec<serde_json::Value>, String> {
    iceberg_table_rows(world.dependencies.endpoints(), &world.domain, table).await
}

#[then(expr = "within {string} the Iceberg table {string} holds exactly these rows")]
async fn then_within_the_iceberg_table_holds_exactly(
    world: &mut ScenarioWorld,
    bound: String,
    table: String,
    #[step] step: &Step,
) {
    let bound = parse_duration_text(&bound).assured("the scenario declares a valid bound");
    let table = expand_placeholders(world, &table);
    let expected = expected_rows(world, step);
    let deadline = Instant::now() + bound;
    loop {
        nervix_primitives::task::consume_budget().await;
        let observed = match table_rows(world, &table).await {
            Ok(rows) if holds_exactly(&rows, &expected) => return,
            Ok(rows) => format!("{rows:?}"),
            Err(error) => error,
        };
        assert!(
            Instant::now() < deadline,
            "Iceberg table {table} did not hold exactly {expected:?} within {bound:?}: last \
             observed {observed}"
        );
        nervix_primitives::time::sleep(POLL_INTERVAL).await;
    }
}

/// How many rows the table's current snapshot holds, from its summary, so a check of a table
/// whose rows are large reads no data file.
async fn table_row_count(world: &ScenarioWorld, table: &str) -> Result<u64, String> {
    let loaded = rustfs_iceberg_table(world.dependencies.endpoints(), &world.domain, table).await?;
    let Some(snapshot) = loaded.metadata().current_snapshot() else {
        return Ok(0);
    };
    let Some(total) = snapshot
        .summary()
        .additional_properties
        .get(TOTAL_RECORDS_PROPERTY)
    else {
        return Err(format!(
            "snapshot {} of Iceberg table {table} does not count its rows",
            snapshot.snapshot_id()
        ));
    };
    total
        .parse::<u64>()
        .map_err(|error| format!("Iceberg table {table} counts its rows as '{total}': {error}"))
}

/// Reads the current snapshot's data-file manifest through the catalog. The scan exposes each
/// file's recorded row count and exact Parquet byte length without loading its rows.
async fn table_data_files(world: &ScenarioWorld, table: &str) -> Result<Vec<(u64, u64)>, String> {
    let loaded = rustfs_iceberg_table(world.dependencies.endpoints(), &world.domain, table).await?;
    let scan = loaded
        .scan()
        .build()
        .map_err(|error| format!("failed to plan Iceberg table {table}: {error}"))?;
    let files = scan
        .plan_files()
        .await
        .map_err(|error| format!("failed to read Iceberg manifest of {table}: {error}"))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| format!("failed to list Iceberg data files of {table}: {error}"))?;
    files
        .into_iter()
        .map(|file| {
            file.record_count
                .map(|rows| (rows, file.file_size_in_bytes))
                .ok_or_else(|| {
                    format!("Iceberg data file {} has no row count", file.data_file_path)
                })
        })
        .collect()
}

#[then(
    expr = "within {string} the Iceberg table {string} has {int} data files holding {int} rows, \
            each with at most {int} rows and {int} bytes"
)]
async fn then_iceberg_data_files_obey_batch_bounds(
    world: &mut ScenarioWorld,
    bound: String,
    table: String,
    file_count: usize,
    row_count: u64,
    max_rows: u64,
    max_bytes: u64,
) {
    let bound = parse_duration_text(&bound).assured("the scenario declares a valid bound");
    let table = expand_placeholders(world, &table);
    let deadline = Instant::now() + bound;
    loop {
        nervix_primitives::task::consume_budget().await;
        let observed = table_data_files(world, &table).await;
        if let Ok(files) = &observed {
            let rows: u64 = files.iter().map(|(rows, _)| rows).sum();
            if rows == row_count {
                assert_eq!(
                    files.len(),
                    file_count,
                    "Iceberg table {table} has {rows} rows in files {files:?}"
                );
                assert!(
                    files
                        .iter()
                        .all(|(rows, bytes)| *rows <= max_rows && *bytes <= max_bytes),
                    "Iceberg table {table} has a file beyond {max_rows} rows or {max_bytes} \
                     bytes: {files:?}"
                );
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "Iceberg table {table} did not hold {row_count} rows within {bound:?}: last observed \
             {observed:?}"
        );
        nervix_primitives::time::sleep(POLL_INTERVAL).await;
    }
}

/// How many rows the table holds while a commit it would hold is still to come: a gate hold or a
/// bulk memory hold keeps that commit from happening, so this reads a state that cannot move.
#[then(expr = "the Iceberg table {string} holds {int} rows")]
async fn then_the_iceberg_table_holds_rows(world: &mut ScenarioWorld, table: String, rows: u64) {
    let table = expand_placeholders(world, &table);
    let held = table_row_count(world, &table)
        .await
        .unwrap_or_else(|error| {
            panic!("could not count the rows of Iceberg table {table}: {error}")
        });
    assert_eq!(held, rows, "Iceberg table {table} holds {held} rows");
}

#[then(expr = "within {string} the Iceberg table {string} holds {int} rows")]
async fn then_within_the_iceberg_table_holds_rows(
    world: &mut ScenarioWorld,
    bound: String,
    table: String,
    rows: u64,
) {
    let bound = parse_duration_text(&bound).assured("the scenario declares a valid bound");
    let table = expand_placeholders(world, &table);
    let deadline = Instant::now() + bound;
    loop {
        nervix_primitives::task::consume_budget().await;
        let observed = match table_row_count(world, &table).await {
            Ok(held) if held == rows => return,
            Ok(held) => format!("{held} rows"),
            Err(error) => error,
        };
        assert!(
            Instant::now() < deadline,
            "Iceberg table {table} did not hold {rows} rows within {bound:?}: last observed \
             {observed}"
        );
        nervix_primitives::time::sleep(POLL_INTERVAL).await;
    }
}

/// What DESCRIBE EMITTER reports while an Iceberg commit waits for the whole bulk memory class to
/// read one staged file larger than it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WholeClassWait {
    capacity: u64,
    file_bytes: u64,
}

impl WholeClassWait {
    fn from_describe_output(output: &str) -> Option<Self> {
        let error = DescribedEmitterRetry::from_describe_output(output).transient_error?;
        let wait =
            error.strip_prefix("Iceberg commit waits for the whole bulk memory class of ")?;
        let (capacity, file) = wait.split_once(" bytes for a staged file of ")?;
        let file_bytes = file.strip_suffix(" bytes")?;
        let (Ok(capacity), Ok(file_bytes)) = (capacity.parse::<u64>(), file_bytes.parse::<u64>())
        else {
            panic!("DESCRIBE EMITTER reports the whole-class wait in byte counts, got '{error}'");
        };
        Some(Self {
            capacity,
            file_bytes,
        })
    }
}

/// Waits until DESCRIBE EMITTER reports an Iceberg commit waiting for the whole bulk memory class,
/// requires the staged file it reads to be larger than the class, and saves the file's bytes.
#[then(
    expr = "within {string} DESCRIBE EMITTER {string} on the leader node reports an Iceberg \
            commit waiting for the whole bulk memory class, whose staged file bytes are saved as \
            placeholder {string}"
)]
async fn then_describe_emitter_reports_a_whole_class_wait(
    world: &mut ScenarioWorld,
    bound: String,
    emitter: String,
    placeholder: String,
) {
    let bound = parse_duration_text(&bound).assured("the scenario declares a valid bound");
    let emitter = expand_placeholders(world, &emitter);
    let deadline = Instant::now() + bound;
    loop {
        nervix_primitives::task::consume_budget().await;
        let leader = current_leader_node(world).await;
        let output =
            run_plexus_commands_on_node(world, &leader, &format!("DESCRIBE EMITTER {emitter};"))
                .await
                .unwrap_or_else(|error| panic!("DESCRIBE EMITTER {emitter} failed: {error:?}"));
        world.last_command_output = Some(output.clone());
        if let Some(wait) = WholeClassWait::from_describe_output(&output) {
            assert!(
                wait.file_bytes > wait.capacity,
                "a commit waits for the whole bulk memory class only for a staged file larger \
                 than it, got {wait:?}"
            );
            world
                .placeholders
                .insert(placeholder, wait.file_bytes.to_string());
            return;
        }
        assert!(
            Instant::now() < deadline,
            "DESCRIBE EMITTER {emitter} did not report a commit waiting for the whole bulk memory \
             class within {bound:?}. last output: {output}"
        );
        nervix_primitives::time::sleep(POLL_INTERVAL).await;
    }
}
