//! Public typed stateful processor reads across scenario nodes.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Held-row, deadline, redaction, paging and count comparisons of processor state
//!   inspections through the native client.
//! - **Depends on.** The scenario world, verified inspection envelopes and Row presentation.
//! - **Must not know.** Branch tasks, their buffers or how an owner page crosses the interconnect.

use std::{collections::BTreeMap, time::Duration};

use cucumber::{gherkin::Step, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{Client, CommandOutcome};
use nervix_client_wire::{
    CellView, StateInspection, StateInspectionResult, StateInspectionSection,
};
use nervix_models::parse_duration_text;
use nervix_primitives::time::{Instant, sleep};
use serde_json::Value;

use crate::{ScenarioWorld, docstring, expand_placeholders};

/// Columns whose values are domain times decided when the processor received or will release a
/// row, so a scenario compares them with dedicated steps instead of literal values.
const TIME_COLUMNS: [&str; 4] = ["received_at", "release_at", "timeout_at", "seen_at"];

/// What a saved page remembers to resume the same read.
#[derive(Debug, Default)]
pub(crate) struct Observations {
    continuation: Option<String>,
}

mod partition_moves;
#[path = "published.rs"]
mod published;

/// One displayed row: its branch key and fields as the shared Row presentation renders them.
struct DisplayedRow {
    branch: Value,
    fields: serde_json::Map<String, Value>,
}

fn statement_kind(kind: &str) -> &'static str {
    match kind {
        "reorderer" => "REORDERER",
        "correlator" => "CORRELATOR",
        "deduplicator" => "DEDUPLICATOR",
        "window" => "WINDOW PROCESSOR",
        other => panic!("the scenario names an inspected processor kind, got {other:?}"),
    }
}

fn last_state(world: &ScenarioWorld) -> &StateInspection {
    world
        .last_client_outcome
        .as_ref()
        .assured("a native command completed")
        .state
        .as_deref()
        .assured("the command returned typed state")
}

fn processor_kind(state: &StateInspection) -> nervix_models::ModelKind {
    state
        .entity
        .domain_node()
        .assured("a processor inspection names a domain node")
        .node
        .kind
}

fn displayed_rows(section: &StateInspectionSection) -> Vec<DisplayedRow> {
    let mut rows = Vec::new();
    for frame in &section.frames {
        for line in frame
            .batch()
            .display_lines(&section.schema)
            .assured("verified cells conform to their section schema")
        {
            let (branch, payload) = match line.strip_prefix("key=") {
                Some(branched) => {
                    let (branch, payload) = branched
                        .split_once(" payload=")
                        .assured("the shared Row presentation separates key and fields");
                    (
                        serde_json::from_str(branch).assured("the Row renderer writes JSON"),
                        payload,
                    )
                }
                None => (Value::Null, line.as_str()),
            };
            let Value::Object(fields) =
                serde_json::from_str(payload).assured("the Row renderer writes JSON")
            else {
                panic!("row fields form an object: {payload}");
            };
            rows.push(DisplayedRow { branch, fields });
        }
    }
    rows
}

/// Every row of the named section across the page's units, without its time columns.
fn section_rows(state: &StateInspection, name: &str) -> Vec<Value> {
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("expected a page: {state:?}");
    };
    let mut rows = Vec::new();
    for unit in &page.units {
        assert_eq!(
            unit.origin.node.node_id(),
            &unit.owner,
            "the unit's owner served its page"
        );
        for section in unit.sections.iter().filter(|section| section.name == name) {
            for mut row in displayed_rows(section) {
                for column in TIME_COLUMNS {
                    row.fields.remove(column);
                }
                rows.push(serde_json::json!({"branch": row.branch, "fields": row.fields}));
            }
        }
    }
    rows
}

/// Every row of every section, in page order and with all of its columns.
fn complete_rows(state: &StateInspection) -> Vec<String> {
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("expected a page: {state:?}");
    };
    let mut rows = Vec::new();
    for unit in &page.units {
        for section in &unit.sections {
            if metadata_section(state, &section.name) {
                continue;
            }
            for row in displayed_rows(section) {
                rows.push(
                    serde_json::to_string(
                        &serde_json::json!({"section": section.name, "branch": row.branch, "fields": row.fields}),
                    )
                    .assured("a displayed row serializes"),
                );
            }
        }
    }
    rows
}

fn metadata_section(state: &StateInspection, section: &str) -> bool {
    processor_kind(state) == nervix_models::ModelKind::WindowProcessor
        && matches!(section, "header" | "accumulators")
}

fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

fn rendered_rows(rows: &[Value]) -> Vec<String> {
    sorted(
        rows.iter()
            .map(|row| serde_json::to_string(&ordered_members(row)).assured("a row serializes"))
            .collect(),
    )
}

/// `value` with every object's members ordered by name. A JSON object's members are unordered, and
/// the Row presentation writes them by name while an expected row lists them as written, so rows
/// compare by their members' values alone.
fn ordered_members(value: &Value) -> Value {
    match value {
        Value::Object(members) => {
            let mut by_name = BTreeMap::new();
            for (name, member) in members {
                by_name.insert(name.clone(), ordered_members(member));
            }
            let mut ordered = serde_json::Map::new();
            for (name, member) in by_name {
                ordered.insert(name, member);
            }
            Value::Object(ordered)
        }
        Value::Array(items) => Value::Array(items.iter().map(ordered_members).collect()),
        other => other.clone(),
    }
}

/// The domain times of one column for every row of the named section, keyed by the row's branch.
fn section_times(state: &StateInspection, name: &str, column: &str) -> Vec<(String, i64)> {
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("expected a page: {state:?}");
    };
    let mut times = Vec::new();
    for unit in &page.units {
        for section in unit.sections.iter().filter(|section| section.name == name) {
            let index = section
                .schema
                .fields
                .iter()
                .position(|field| field.name.as_str() == column)
                .unwrap_or_else(|| panic!("section {name:?} has no {column:?} column"));
            for frame in &section.frames {
                let batch = frame.batch();
                let rendered = batch
                    .display_lines(&section.schema)
                    .assured("verified cells conform to their section schema");
                for (row, line) in batch.rows().zip(rendered) {
                    let Some(CellView::Datetime(time)) = row.get(index) else {
                        panic!("{column:?} keeps its datetime type");
                    };
                    let key = match line.strip_prefix("key=") {
                        Some(branched) => branched
                            .split_once(" payload=")
                            .assured("the shared Row presentation separates key and fields")
                            .0
                            .to_string(),
                        None => "null".to_string(),
                    };
                    times.push((key, time.unix_nanos()));
                }
            }
        }
    }
    times
}

fn expected_rows(text: &str) -> Vec<Value> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line.trim()).assured("one expected row object per line"))
        .collect()
}

async fn execute(world: &ScenarioWorld, client: &str, statement: String) -> CommandOutcome {
    let client: Client = world
        .transaction_clients
        .get(client)
        .unwrap_or_else(|| panic!("client {client:?} must be connected"))
        .clone();
    client
        .execute(statement.clone())
        .await
        .unwrap_or_else(|error| panic!("the public read completes: {statement}: {error}"))
}

fn record(world: &mut ScenarioWorld, outcome: CommandOutcome) {
    if outcome.succeeded() {
        world.last_command_output = Some(outcome.message.clone());
        world.last_command_error = None;
    } else {
        world.last_command_output = None;
        world.last_command_error = Some(outcome.message.clone());
    }
    world.last_client_outcome = Some(outcome);
}

fn page_rows(outcome: &CommandOutcome) -> Option<u64> {
    let state = outcome.state.as_deref()?;
    let StateInspectionResult::Page(page) = &state.result else {
        return None;
    };
    Some(page.counts.rows)
}

#[then(expr = "within {string} client {string} sees {word} {string} hold {int} rows")]
async fn holds(
    world: &mut ScenarioWorld,
    duration: String,
    client: String,
    kind: String,
    name: String,
    rows: u64,
) {
    let duration = parse_duration_text(&duration).assured("the scenario names a valid bound");
    let statement = format!(
        "SHOW {} {} STATE FORMAT JSON;",
        statement_kind(&kind),
        expand_placeholders(world, &name)
    );
    let deadline = Instant::now() + duration;
    loop {
        nervix_primitives::task::consume_budget().await;
        let outcome = execute(world, &client, statement.clone()).await;
        let held = page_rows(&outcome);
        if held == Some(rows) {
            record(world, outcome);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{kind} {name:?} never held {rows} rows within the bound; last: {}",
            outcome.message
        );
        sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "the last processor state counts {int} rows in {int} branches")]
fn counts(world: &mut ScenarioWorld, rows: u64, branches: u64) {
    let state = last_state(world);
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("expected a page: {state:?}");
    };
    assert_eq!(page.counts.rows, rows, "{state:?}");
    assert_eq!(page.counts.branches, branches, "{state:?}");
    if processor_kind(state) == nervix_models::ModelKind::WindowProcessor {
        if rows > 0 {
            assert!(
                page.counts.bytes > 0,
                "retained windows have a live state charge"
            );
        }
    } else {
        assert_eq!(page.counts.bytes == 0, rows == 0, "{state:?}");
    }
    let mut units = (0_u64, 0_u64, 0_u64);
    for unit in &page.units {
        units.0 += unit.counts.rows;
        units.1 += unit.counts.branches;
        units.2 += unit.counts.bytes;
        let mut returned = 0_u64;
        let mut omitted = 0_u64;
        for section in &unit.sections {
            if metadata_section(state, &section.name) {
                continue;
            }
            omitted += section.omitted_rows;
            returned += section
                .frames
                .iter()
                .map(|frame| u64::try_from(frame.batch().len()).assured("bounded row count"))
                .sum::<u64>();
        }
        assert!(
            returned + omitted <= unit.counts.rows,
            "a section never returns or omits more rows than its unit holds"
        );
    }
    assert_eq!(
        units,
        (page.counts.rows, page.counts.branches, page.counts.bytes),
        "unit counts add up to the complete counts"
    );
}

#[then(expr = "the last processor state section {string} rows match")]
fn rows_match(world: &mut ScenarioWorld, name: String, #[step] step: &Step) {
    let observed = section_rows(last_state(world), &name);
    let expected = expected_rows(&expand_placeholders(world, docstring(step)));
    assert_eq!(rendered_rows(&observed), rendered_rows(&expected));
}

#[then(
    expr = "within {string} client {string} sees {word} {string} section {string} rows matching"
)]
async fn rows_eventually_match(
    world: &mut ScenarioWorld,
    duration: String,
    client: String,
    kind: String,
    name: String,
    section: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).assured("the scenario names a valid bound");
    let expected = rendered_rows(&expected_rows(&expand_placeholders(world, docstring(step))));
    let statement = format!(
        "SHOW {} {} STATE FORMAT JSON;",
        statement_kind(&kind),
        expand_placeholders(world, &name)
    );
    let deadline = Instant::now() + duration;
    loop {
        nervix_primitives::task::consume_budget().await;
        let outcome = execute(world, &client, statement.clone()).await;
        let matched = outcome.state.as_deref().is_some_and(|state| {
            matches!(&state.result, StateInspectionResult::Page(_))
                && rendered_rows(&section_rows(state, &section)) == expected
        });
        if matched {
            record(world, outcome);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{kind} {name:?} section {section:?} never matched {expected:?} within the bound; \
             last: {}",
            outcome.message
        );
        sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "the last processor state has no {string} rows")]
fn no_rows(world: &mut ScenarioWorld, name: String) {
    let observed = section_rows(last_state(world), &name);
    assert!(observed.is_empty(), "{observed:?}");
}

#[then(
    expr = "every {string} row of the last processor state times out {string} after it was \
            received"
)]
fn timeouts(world: &mut ScenarioWorld, name: String, duration: String) {
    let duration = i64::try_from(
        parse_duration_text(&duration)
            .assured("the scenario names a valid duration")
            .as_nanos(),
    )
    .assured("a scenario duration fits nanoseconds");
    let state = last_state(world);
    let received = section_times(state, &name, "received_at");
    let timeouts = section_times(state, &name, "timeout_at");
    assert!(!received.is_empty(), "the section holds rows");
    assert_eq!(received.len(), timeouts.len());
    for ((_, received), (_, timeout)) in received.iter().zip(&timeouts) {
        assert_eq!(*timeout - *received, duration);
    }
}

#[then(
    expr = "the last processor state releases every {string} row {string} after the earliest held \
            row of its branch was received"
)]
fn releases(world: &mut ScenarioWorld, name: String, duration: String) {
    let duration = i64::try_from(
        parse_duration_text(&duration)
            .assured("the scenario names a valid duration")
            .as_nanos(),
    )
    .assured("a scenario duration fits nanoseconds");
    let state = last_state(world);
    let received = section_times(state, &name, "received_at");
    let releases = section_times(state, &name, "release_at");
    assert!(!received.is_empty(), "the section holds rows");
    let mut earliest = BTreeMap::<String, i64>::new();
    for (branch, time) in &received {
        let entry = earliest.entry(branch.clone()).or_insert(*time);
        *entry = (*entry).min(*time);
    }
    for (branch, release) in &releases {
        let first = earliest
            .get(branch)
            .assured("every released row has a received time in its branch");
        assert_eq!(*release - *first, duration, "branch {branch}");
    }
}

#[then(
    expr = "the last processor state never carries the bytes of {string}, {string}, {string} or \
            {string}"
)]
fn redacted(world: &mut ScenarioWorld, first: String, second: String, third: String, last: String) {
    let StateInspectionResult::Page(page) = &last_state(world).result else {
        panic!("expected a page");
    };
    let mut frames = 0;
    for unit in &page.units {
        for section in &unit.sections {
            for frame in &section.frames {
                frames += 1;
                if let Some(key) = frame.batch().branch_key() {
                    for index in 0..key.len() {
                        let branch_field = section
                            .schema
                            .branch
                            .as_ref()
                            .assured("a keyed frame belongs to a branched section")
                            .fields()[index]
                            .clone();
                        if branch_field.sensitive {
                            assert_eq!(key.get(index), Some(CellView::Redacted));
                        }
                    }
                }
                for row in frame.batch().rows() {
                    for (index, field) in section.schema.fields.iter().enumerate() {
                        if field.sensitive {
                            assert_eq!(row.get(index), Some(CellView::Redacted));
                        }
                    }
                }
                for secret in [&first, &second, &third, &last] {
                    assert!(
                        !frame
                            .bytes()
                            .windows(secret.len())
                            .any(|bytes| bytes == secret.as_bytes()),
                        "sensitive bytes never cross the wire"
                    );
                }
            }
        }
    }
    assert!(frames > 0, "the inspection returned rows to check");
}

#[then(
    expr = "client {string} reads {word} {string} state one row per page with unchanged complete \
            counts"
)]
async fn pages(world: &mut ScenarioWorld, client: String, kind: String, name: String) {
    let name = expand_placeholders(world, &name);
    let kind = statement_kind(&kind);
    let full = execute(
        world,
        &client,
        format!("SHOW {kind} {name} STATE FOR ALL BRANCHES FORMAT JSON;"),
    )
    .await;
    assert!(full.succeeded(), "{}", full.message);
    let full_state = full
        .state
        .as_deref()
        .assured("the read returned typed state");
    let StateInspectionResult::Page(full_page) = &full_state.result else {
        panic!("expected a page: {full_state:?}");
    };
    assert!(
        full_page.continuation.is_none(),
        "the default page holds every row"
    );
    let expected = sorted(complete_rows(full_state));
    assert_eq!(
        u64::try_from(expected.len()).assured("bounded rows"),
        full_page.counts.rows
    );
    let mut observed = Vec::new();
    let mut after = None::<String>;
    loop {
        nervix_primitives::task::consume_budget().await;
        let statement = match &after {
            Some(token) => format!(
                "SHOW {kind} {name} STATE FOR ALL BRANCHES LIMIT 1 AFTER '{}' FORMAT JSON;",
                token.replace('\'', "''")
            ),
            None => format!("SHOW {kind} {name} STATE FOR ALL BRANCHES LIMIT 1 FORMAT JSON;"),
        };
        let outcome = execute(world, &client, statement).await;
        assert!(outcome.succeeded(), "{}", outcome.message);
        let state = outcome
            .state
            .as_deref()
            .assured("the read returned typed state");
        let StateInspectionResult::Page(page) = &state.result else {
            panic!("expected a page: {state:?}");
        };
        assert_eq!(
            page.counts, full_page.counts,
            "complete counts stay unchanged"
        );
        let rows = complete_rows(state);
        assert_eq!(rows.len(), 1, "every page returns exactly one row");
        observed.extend(rows);
        assert!(
            observed.len() <= expected.len(),
            "paging never returns more rows than the read holds"
        );
        match &page.continuation {
            Some(token) => after = Some(token.clone()),
            None => break,
        }
    }
    assert_eq!(sorted(observed), expected);
}

#[when(expr = "client {string} saves a one row page of {word} {string} state")]
async fn save_page(world: &mut ScenarioWorld, client: String, kind: String, name: String) {
    let statement = format!(
        "SHOW {} {} STATE FOR ALL BRANCHES LIMIT 1 FORMAT JSON;",
        statement_kind(&kind),
        expand_placeholders(world, &name)
    );
    let outcome = execute(world, &client, statement).await;
    assert!(outcome.succeeded(), "{}", outcome.message);
    let state = outcome
        .state
        .as_deref()
        .assured("the read returned typed state");
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("expected a page: {state:?}");
    };
    world.processor_state_observations.continuation = Some(
        page.continuation
            .clone()
            .assured("a one-row page of several rows continues"),
    );
    record(world, outcome);
}

#[when(expr = "client {string} resumes the saved {word} {string} state page")]
async fn resume(world: &mut ScenarioWorld, client: String, kind: String, name: String) {
    let token = world
        .processor_state_observations
        .continuation
        .clone()
        .assured("a page was saved before it is resumed");
    let statement = format!(
        "SHOW {} {} STATE FOR ALL BRANCHES LIMIT 1 AFTER '{}' FORMAT JSON;",
        statement_kind(&kind),
        expand_placeholders(world, &name),
        token.replace('\'', "''")
    );
    let outcome = execute(world, &client, statement).await;
    record(world, outcome);
}

#[when(
    expr = "client {string} inspects reorderer {string} after each of these payloads posted to \
            node {string} with host {string} path {string}"
)]
async fn steady_input(
    world: &mut ScenarioWorld,
    client: String,
    name: String,
    node: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let statement = format!(
        "SHOW REORDERER {} STATE FORMAT JSON;",
        expand_placeholders(world, &name)
    );
    let node = expand_placeholders(world, &node);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payloads = expand_placeholders(world, docstring(step));
    for payload in payloads
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        nervix_primitives::task::consume_budget().await;
        world
            .cluster()
            .publish_http(&node, &host, &path, payload)
            .await
            .assured("the scenario node accepts its HTTP intake");
        let outcome = execute(world, &client, statement.clone()).await;
        assert!(
            page_rows(&outcome).is_some(),
            "an inspection during steady input reads a page: {}",
            outcome.message
        );
        record(world, outcome);
    }
}
