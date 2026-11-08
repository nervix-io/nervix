//! Public typed materialized-state observations across scenario nodes.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Exact row, branch, redaction and watermark comparisons through the native client.
//! - **Depends on.** The scenario world, verified inspection envelopes and Row presentation.
//! - **Must not know.** Runtime state stores, snapshot encoding or materialized read internals.

use std::collections::BTreeSet;

use cucumber::{gherkin::Step, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::Client;
use nervix_client_wire::{StateInspection, StateInspectionResult};
use nervix_models::parse_duration_text;
use nervix_primitives::time::{Instant, sleep};
use serde_json::Value;

use crate::{ScenarioWorld, client_connect_options, client_domain, docstring, expand_placeholders};

fn observed_rows(state: &StateInspection) -> Vec<Value> {
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("expected a page: {state:?}");
    };
    let mut rows = Vec::new();
    for unit in &page.units {
        assert_eq!(
            unit.origin.node.node_id(),
            &unit.owner,
            "the actual owner served the page"
        );
        for section in &unit.sections {
            let watermarks = section
                .watermarks
                .assured("materialized rows declare their watermark columns");
            let low = &section.schema.fields
                [usize::try_from(watermarks.low).assured("addressable column")]
            .name;
            let high = &section.schema.fields
                [usize::try_from(watermarks.high).assured("addressable column")]
            .name;
            for frame in &section.frames {
                for line in frame
                    .batch()
                    .display_lines(&section.schema)
                    .assured("verified cells conform")
                {
                    let (branch, payload) = if let Some(branched) = line.strip_prefix("key=") {
                        let (branch, payload) = branched
                            .split_once(" payload=")
                            .assured("the shared Row presentation separates key and fields");
                        (
                            serde_json::from_str(branch).assured("the Row renderer writes JSON"),
                            payload,
                        )
                    } else {
                        (Value::Null, line.as_str())
                    };
                    let mut fields: Value =
                        serde_json::from_str(payload).assured("the Row renderer writes JSON");
                    let fields = fields.as_object_mut().assured("row fields form an object");
                    let low = fields
                        .remove(low.as_str())
                        .assured("the low watermark is present");
                    let high = fields
                        .remove(high.as_str())
                        .assured("the high watermark is present");
                    rows.push(serde_json::json!({"branch":branch,"fields":fields,"watermarks":[low,high]}));
                }
            }
        }
    }
    rows
}

fn expected_rows(text: &str) -> Vec<Value> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line.trim()).assured("one expected branch/fields object per line")
        })
        .collect()
}

fn row_matches(row: &Value, expected: &Value) -> bool {
    if row["branch"] != expected["branch"] {
        return false;
    }
    let fields = row["fields"].as_object().assured("observed fields");
    let exact = expected["fields"].as_object().assured("expected fields");
    let prefixes = expected.get("field_prefixes").and_then(Value::as_object);
    fields.len() == exact.len() + prefixes.map_or(0, |prefixes| prefixes.len())
        && exact
            .iter()
            .all(|(name, value)| fields.get(name) == Some(value))
        && prefixes
            .into_iter()
            .flat_map(|prefixes| prefixes.iter())
            .all(|(name, prefix)| {
                fields
                    .get(name)
                    .and_then(Value::as_str)
                    .zip(prefix.as_str())
                    .is_some_and(|(actual, prefix)| actual.starts_with(prefix))
            })
}

async fn client(world: &ScenarioWorld, node: &str) -> Client {
    let uri = world
        .cluster()
        .grpc_uri(node)
        .assured("the scenario names a live node");
    Client::connect_with_options(
        &uri,
        client_domain(&world.domain),
        client_connect_options(&uri).assured("fixture TLS options"),
    )
    .await
    .assured("the fixture native client connects")
}

#[then(
    expr = "within {string} node {string} eventually reports materialized state for relay \
            {string} containing"
)]
async fn node_reports_materialized_state(
    world: &mut ScenarioWorld,
    duration: String,
    node: String,
    relay: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).assured("feature duration");
    let node = expand_placeholders(world, &node);
    let relay = expand_placeholders(world, &relay);
    let expected = expand_placeholders(world, docstring(step));
    let structured = expected
        .trim()
        .starts_with('{')
        .then(|| expected_rows(&expected));
    let client = client(world, &node).await;
    let deadline = Instant::now()
        .checked_add(duration)
        .assured("the feature's bounded wait fits");
    loop {
        nervix_primitives::task::consume_budget().await;
        let outcome = client
            .execute(format!(
                "SHOW RELAY {relay} MATERIALIZED STATE FORMAT TEXT;"
            ))
            .await
            .assured("the public inspection completes");
        let matched = if outcome.succeeded() {
            match &structured {
                Some(expected) => {
                    let rows = observed_rows(
                        outcome
                            .state
                            .as_deref()
                            .assured("a typed state envelope accompanies TEXT"),
                    );
                    expected
                        .iter()
                        .all(|expected| rows.iter().any(|row| row_matches(row, expected)))
                }
                None => outcome.message.contains(expected.trim()),
            }
        } else {
            false
        };
        world.last_command_output = Some(outcome.message.clone());
        world.last_client_outcome = Some(outcome);
        if matched {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node {node} did not report {expected}: {:?}",
            world.last_command_output
        );
        sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} every node reports the same materialized records for relay {string} \
            with these rows"
)]
async fn every_node_reports_the_same_materialized_records(
    world: &mut ScenarioWorld,
    duration: String,
    relay: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).assured("feature duration");
    let relay = expand_placeholders(world, &relay);
    let expected = expected_rows(&expand_placeholders(world, docstring(step)));
    assert!(!expected.is_empty(), "at least one expected row");
    let mut first = None;
    for node in world.cluster().node_ids() {
        let client = client(world, &node).await;
        let deadline = Instant::now()
            .checked_add(duration)
            .assured("bounded fixture wait");
        loop {
            nervix_primitives::task::consume_budget().await;
            let outcome = client
                .execute(format!(
                    "SHOW RELAY {relay} MATERIALIZED STATE FORMAT TEXT;"
                ))
                .await
                .assured("the public inspection completes");
            if outcome.succeeded() {
                let rows = observed_rows(
                    outcome
                        .state
                        .as_deref()
                        .assured("TEXT carries a typed envelope"),
                );
                if rows.len() == expected.len()
                    && expected
                        .iter()
                        .all(|expected| rows.iter().any(|row| row_matches(row, expected)))
                {
                    let rows = rows
                        .iter()
                        .map(|row| serde_json::to_string(row).assured("bounded JSON observation"))
                        .collect::<BTreeSet<_>>();
                    assert_eq!(
                        rows.len(),
                        expected.len(),
                        "no duplicated materialized rows"
                    );
                    if let Some(first) = &first {
                        assert_eq!(
                            &rows, first,
                            "node {node} reports identical fields, redaction and watermarks"
                        );
                    } else {
                        first = Some(rows);
                    }
                    world.last_command_output = Some(outcome.message.clone());
                    world.last_client_outcome = Some(outcome);
                    break;
                }
            }
            world.last_command_output = Some(outcome.message.clone());
            world.last_client_outcome = Some(outcome);
            assert!(
                Instant::now() < deadline,
                "node {node} did not report the exact expected rows: {:?}",
                world.last_command_output
            );
            sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Observations {
    cursor: Option<String>,
    full_rows: Vec<String>,
    rows: BTreeSet<String>,
    counts: Option<nervix_models::StateInspectionCounts>,
    reader_node: Option<String>,
    remote_baseline: Option<f64>,
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

#[then("the last typed state inspection is empty")]
fn empty(world: &mut ScenarioWorld) {
    let StateInspectionResult::Page(page) = &last_state(world).result else {
        panic!("empty inspection is a page");
    };
    assert_eq!(page.counts.rows, 0);
    assert_eq!(page.counts.branches, 0);
    assert_eq!(page.counts.bytes, 0);
    assert!(page.units.iter().all(|unit| unit.sections.is_empty()));
    assert!(page.continuation.is_none());
}

#[then(expr = "the last typed inspection has {int} returned rows and {int} complete rows")]
fn counts(world: &mut ScenarioWorld, returned: usize, complete: u64) {
    let state = last_state(world);
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("expected page: {state:?}");
    };
    let rows = observed_rows(state);
    assert_eq!(page.counts.rows, complete);
    assert_eq!(rows.len(), returned);
    let branched = page
        .units
        .iter()
        .flat_map(|unit| &unit.sections)
        .any(|section| section.schema.branch.is_some());
    assert_eq!(page.counts.branches, if branched { complete } else { 0 });
    assert_eq!(page.counts.bytes == 0, complete == 0);
    assert_eq!(
        serde_json::from_str::<Value>(&state.to_json()).assured("typed JSON")["counts"]["rows"],
        complete
    );
    for unit in &page.units {
        for section in &unit.sections {
            let n = section
                .frames
                .iter()
                .map(|frame| u64::try_from(frame.batch().len()).assured("bounded row count"))
                .sum::<u64>();
            assert_eq!(section.omitted_rows + n, unit.counts.rows);
        }
    }
}

#[then("the last typed inspection rows match")]
fn rows_match(world: &mut ScenarioWorld, #[step] step: &Step) {
    let rows = observed_rows(last_state(world));
    let expected = expected_rows(&expand_placeholders(world, docstring(step)));
    assert_eq!(rows.len(), expected.len());
    assert!(
        expected
            .iter()
            .all(|expected| rows.iter().any(|row| row_matches(row, expected))),
        "{rows:?}"
    );
}

#[then("the inspection continuation is saved with its complete counts and rows")]
fn save_cursor(world: &mut ScenarioWorld) {
    let state = last_state(world);
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("page");
    };
    let cursor = page
        .continuation
        .clone()
        .assured("the bounded page has a continuation");
    let counts = page.counts;
    let rows = observed_rows(state)
        .iter()
        .map(|row| serde_json::to_string(&values_for_page(row)).assured("bounded observation"))
        .collect();
    world.inspection_observations.cursor = Some(cursor);
    world.inspection_observations.counts = Some(counts);
    world.inspection_observations.rows = rows;
}

fn values_for_page(row: &Value) -> Value {
    serde_json::json!({"branch":row["branch"],"fields":row["fields"]})
}

#[when(expr = "inspection client {string} resumes relay {string} with the saved continuation")]
async fn resume(world: &mut ScenarioWorld, client: String, relay: String) {
    let token = world
        .inspection_observations
        .cursor
        .as_deref()
        .assured("a prior page saved its continuation");
    let query = format!(
        "SHOW RELAY {} MATERIALIZED STATE FOR ALL BRANCHES LIMIT 1 AFTER '{}' FORMAT JSON;",
        expand_placeholders(world, &relay),
        token.replace('\'', "''")
    );
    let client = world
        .transaction_clients
        .get(&client)
        .assured("the inspection client is connected")
        .clone();
    let outcome = client
        .execute(query)
        .await
        .assured("the public resume completes");
    world.last_command_output = Some(outcome.message.clone());
    world.last_client_outcome = Some(outcome);
}

#[then(
    expr = "the completed inspection has exactly {int} distinct rows with unchanged complete \
            counts"
)]
fn complete_pages(world: &mut ScenarioWorld, expected: usize) {
    let state = last_state(world);
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("page");
    };
    assert_eq!(Some(page.counts), world.inspection_observations.counts);
    assert!(page.continuation.is_none(), "every row was read");
    let rows = observed_rows(state);
    for row in rows {
        assert!(
            world.inspection_observations.rows.insert(
                serde_json::to_string(&values_for_page(&row)).assured("bounded observation")
            ),
            "no duplicate across pages"
        );
    }
    assert_eq!(world.inspection_observations.rows.len(), expected);
    assert_eq!(
        world.inspection_observations.rows,
        world
            .inspection_observations
            .full_rows
            .iter()
            .cloned()
            .collect()
    );
}

#[then(expr = "the typed inspection refuses the read with {string}")]
fn refused(world: &mut ScenarioWorld, expected: String) {
    let outcome = world
        .last_client_outcome
        .as_ref()
        .assured("a command completed");
    assert!(
        !outcome.succeeded(),
        "a refused read retains the command failure disposition"
    );
    let state = last_state(world);
    let StateInspectionResult::Rejected(reason) = state.result else {
        panic!("typed refusal: {state:?}");
    };
    assert_eq!(reason.as_ref(), expected);
    let json: Value =
        serde_json::from_str(&outcome.message).assured("refusal is one JSON document");
    assert_eq!(json["rejection"], expected);
}

async fn received_page_bytes(world: &ScenarioWorld, node: &str) -> f64 {
    world
        .cluster()
        .read_observability_metric(
            node,
            "nervix_interconnect_bulk_bytes_total",
            &[
                "class=\"bulk\"".to_string(),
                "direction=\"received\"".to_string(),
            ],
        )
        .await
        .assured("the node exports received bulk bytes")
}

#[when(expr = "inspection client {string} connects away from the last relay owner when possible")]
async fn nonowner(world: &mut ScenarioWorld, name: String) {
    let StateInspectionResult::Page(page) = &last_state(world).result else {
        panic!("page");
    };
    let owner = page
        .units
        .first()
        .assured("one scheduled unit")
        .owner
        .as_str();
    let nodes = world.cluster().node_ids();
    let node = nodes
        .iter()
        .find(|node| node.as_str() != owner)
        .cloned()
        .unwrap_or_else(|| nodes[0].clone());
    let client = client(world, &node).await;
    world.transaction_clients.insert(name, client);
    world.inspection_observations.remote_baseline = Some(received_page_bytes(world, &node).await);
    world.inspection_observations.reader_node = Some(node);
}

#[then("the typed inspection used its unit owner and a remote page when the cluster permits it")]
async fn owner_and_page(world: &mut ScenarioWorld) {
    let node = world
        .inspection_observations
        .reader_node
        .as_deref()
        .assured("reader node");
    let StateInspectionResult::Page(page) = &last_state(world).result else {
        panic!("page");
    };
    for unit in &page.units {
        assert_eq!(unit.owner, *unit.origin.node.node_id());
    }
    if world.cluster().node_ids().len() > 1 {
        assert!(page.units.iter().all(|unit| unit.owner.as_str() != node));
        assert!(
            received_page_bytes(world, node).await
                > world
                    .inspection_observations
                    .remote_baseline
                    .assured("baseline"),
            "a bounded owner page crossed the interconnect bulk stream"
        );
    }
}

#[then("the last inspection JSON is the complete typed envelope")]
fn json_matches(world: &mut ScenarioWorld) {
    let outcome = world.last_client_outcome.as_ref().assured("native outcome");
    let rendered: Value = serde_json::from_str(&outcome.message).assured("one JSON document");
    assert_eq!(
        rendered,
        serde_json::to_value(last_state(world)).assured("the typed envelope is serializable")
    );
}

#[then("the last inspection TEXT uses its typed rows after the owner header")]
fn text_matches(world: &mut ScenarioWorld) {
    let outcome = world.last_client_outcome.as_ref().assured("native outcome");
    let owner = outcome.message.find("owner:").assured("owner header");
    let StateInspectionResult::Page(page) = &last_state(world).result else {
        panic!("page");
    };
    for unit in &page.units {
        for section in &unit.sections {
            for frame in &section.frames {
                for line in frame
                    .batch()
                    .display_lines(&section.schema)
                    .assured("typed rows conform")
                {
                    assert!(
                        outcome
                            .message
                            .find(&line)
                            .is_some_and(|position| position > owner),
                        "the shared Row text follows the owner header"
                    );
                }
            }
        }
    }
}

#[then("the typed inspection preserves redaction and distinct watermark columns")]
fn typed_cells(world: &mut ScenarioWorld) {
    use nervix_client_wire::CellView;
    let StateInspectionResult::Page(page) = &last_state(world).result else {
        panic!("page");
    };
    for unit in &page.units {
        for section in &unit.sections {
            let watermarks = section.watermarks.assured("materialized watermark columns");
            assert_ne!(watermarks.low, watermarks.high);
            assert_eq!(section.schema.fields[0].name.as_str(), "tenant");
            for frame in &section.frames {
                let batch = frame.batch();
                let key = batch
                    .branch_key()
                    .assured("a concrete branch key per frame");
                assert_eq!(key.get(0), Some(CellView::Redacted));
                for row in batch.rows() {
                    for (index, field) in section.schema.fields.iter().enumerate() {
                        if field.sensitive {
                            assert_eq!(row.get(index), Some(CellView::Redacted));
                        }
                    }
                    let Some(CellView::Datetime(low)) =
                        row.get(usize::try_from(watermarks.low).assured("addressable watermark"))
                    else {
                        panic!("low watermark keeps its datetime type");
                    };
                    let Some(CellView::Datetime(high)) =
                        row.get(usize::try_from(watermarks.high).assured("addressable watermark"))
                    else {
                        panic!("high watermark keeps its datetime type");
                    };
                    assert!(low <= high);
                    let payload = section
                        .schema
                        .fields
                        .iter()
                        .position(|field| field.name.as_str() == "ingested_at_low_watermark")
                        .assured("the payload's colliding name is retained");
                    assert!(
                        matches!(row.get(payload), Some(CellView::I64(_))),
                        "the payload column retains its own type"
                    );
                    assert_ne!(
                        payload,
                        usize::try_from(watermarks.low).assured("addressable watermark")
                    );
                }
                for secret in [
                    "alpha-private",
                    "beta-private",
                    "alpha-first",
                    "alpha-final",
                    "beta-first",
                    "beta-final",
                ] {
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
}

#[then("the complete inspection row order is saved")]
fn save_full_rows(world: &mut ScenarioWorld) {
    world.inspection_observations.full_rows = observed_rows(last_state(world))
        .iter()
        .map(|row| serde_json::to_string(&values_for_page(row)).assured("bounded observation"))
        .collect();
}

#[then("the last inspection page is the saved complete inspection's first row")]
fn page_starts_at_first_row(world: &mut ScenarioWorld) {
    let rows = observed_rows(last_state(world));
    assert_eq!(rows.len(), 1);
    assert_eq!(
        serde_json::to_string(&values_for_page(&rows[0])).assured("bounded observation"),
        world.inspection_observations.full_rows[0]
    );
}
