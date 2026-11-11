//! Public WASM guest-state inspections read through Plexus and the envelope's JSON projection.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Polling the WASM state statement and reading branch entries, reset facts and
//!   continuations from its JSON rendering.
//! - **Depends on.** The scenario world, leader command execution and placeholder expansion.
//! - **Must not know.** Checkpoint storage, the owner page operation or the envelope's encoding.

use std::{collections::BTreeSet, time::Duration};

use cucumber::{gherkin::Step, then};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::parse_duration_text;
use nervix_primitives::time::{Instant, sleep};
use serde_json::Value;

use crate::{
    ScenarioWorld, current_leader_node, docstring, expand_placeholders, run_plexus_commands_on_node,
};

fn last_document(world: &ScenarioWorld) -> Value {
    let output = world
        .last_command_output
        .as_deref()
        .assured("a command output exists before it is read");
    serde_json::from_str(output).unwrap_or_else(|error| {
        panic!("the last command output is not one JSON document: {error}\n{output}")
    })
}

/// Every unit's branch entries in page order, as their JSON objects.
fn branch_entries(document: &Value) -> Vec<Value> {
    let units = document["units"]
        .as_array()
        .unwrap_or_else(|| panic!("a WASM state page lists its units: {document}"));
    let mut entries = Vec::new();
    for unit in units {
        let branches = unit["wasm"]["branches"]
            .as_array()
            .unwrap_or_else(|| panic!("every WASM unit lists its branch entries: {unit}"));
        entries.extend(branches.iter().cloned());
    }
    entries
}

/// The identity of each entry: its branch fingerprint, or `unbranched`.
fn branch_identities(document: &Value) -> BTreeSet<String> {
    let mut identities = BTreeSet::new();
    for entry in branch_entries(document) {
        let identity = match &entry["branch"] {
            Value::String(fingerprint) => fingerprint.clone(),
            Value::Null => "unbranched".to_string(),
            other => panic!("a branch entry names a fingerprint or null: {other}"),
        };
        assert!(
            identities.insert(identity.clone()),
            "a page lists each branch once: {identity}"
        );
    }
    identities
}

#[then(expr = "within {string} SHOW WASM PROCESSOR {string} STATE on the leader node contains")]
async fn within_show_wasm_processor_state_contains(
    world: &mut ScenarioWorld,
    duration: String,
    processor: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).assured("the step duration is valid");
    let processor = expand_placeholders(world, &processor);
    let expected = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;
    loop {
        nervix_primitives::task::consume_budget().await;
        let leader = current_leader_node(world).await;
        let observed = match run_plexus_commands_on_node(
            world,
            &leader,
            &format!("SHOW WASM PROCESSOR {processor} STATE;"),
        )
        .await
        {
            Ok(output) => {
                world.last_command_output = Some(output.clone());
                if output.contains(expected.trim()) {
                    return;
                }
                output
            }
            // An owner that is failing over refuses the read until its successor serves the
            // unit, so a refusal is another sample that has not converged yet.
            Err(error) => format!("refused: {error}"),
        };
        assert!(
            Instant::now() < deadline,
            "timed out waiting for SHOW WASM PROCESSOR {processor} STATE to contain {}. last \
             observation: {observed}",
            expected.trim()
        );
        sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "the last command output JSON value at {string} is saved as placeholder {string}")]
fn json_value_is_saved(world: &mut ScenarioWorld, pointer: String, placeholder: String) {
    let document = last_document(world);
    let value = match document.pointer(&pointer) {
        Some(Value::String(value)) => value.clone(),
        _ => panic!("the JSON document has no string at {pointer}: {document}"),
    };
    world.placeholders.insert(placeholder, value);
}

#[then(expr = "the last WASM state inspection JSON lists {int} branch entries")]
fn lists_branch_entries(world: &mut ScenarioWorld, expected: usize) {
    let document = last_document(world);
    assert_eq!(
        branch_entries(&document).len(),
        expected,
        "branch entries returned by the page: {document}"
    );
    assert_eq!(branch_identities(&document).len(), expected);
}

#[then(
    expr = "the last WASM state inspection JSON branch fingerprints are saved as placeholder \
            {string}"
)]
fn branch_fingerprints_are_saved(world: &mut ScenarioWorld, placeholder: String) {
    let document = last_document(world);
    let identities = branch_identities(&document);
    let saved = serde_json::to_string(&identities).assured("branch identities are JSON strings");
    world.placeholders.insert(placeholder, saved);
}

#[then(
    expr = "the last WASM state inspection JSON branch fingerprints are disjoint from placeholder \
            {string}"
)]
fn branch_fingerprints_are_disjoint(world: &mut ScenarioWorld, placeholder: String) {
    let saved = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' was saved"));
    let saved: BTreeSet<String> =
        serde_json::from_str(saved).assured("saved branch identities are one JSON array");
    let document = last_document(world);
    let current = branch_identities(&document);
    assert!(
        saved.is_disjoint(&current),
        "a resumed page lists only branches after the earlier page: earlier {saved:?}, now \
         {current:?}"
    );
}

#[then("the last WASM state inspection JSON reset scope names its only branch entry")]
fn reset_scope_names_the_entry(world: &mut ScenarioWorld) {
    let document = last_document(world);
    let entries = branch_entries(&document);
    let [entry] = entries.as_slice() else {
        panic!("one selected branch entry: {document}");
    };
    let units = document["units"]
        .as_array()
        .assured("the page lists its units");
    let scopes = units
        .iter()
        .filter_map(|unit| unit["wasm"]["reset"]["scope"]["branch"].as_str())
        .collect::<Vec<_>>();
    assert!(
        scopes.contains(&entry["branch"].as_str().assured("a branched entry")),
        "the reset scope names the selected branch: {document}"
    );
}

#[then("the last WASM state inspection JSON reset names a request reference")]
fn reset_names_a_request(world: &mut ScenarioWorld) {
    let document = last_document(world);
    let units = document["units"]
        .as_array()
        .assured("the page lists its units");
    let requests = units
        .iter()
        .filter_map(|unit| unit["wasm"]["reset"]["request"].as_str())
        .collect::<Vec<_>>();
    assert!(
        !requests.is_empty() && requests.iter().all(|request| !request.is_empty()),
        "a reset names the request that published it: {document}"
    );
}
