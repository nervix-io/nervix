//! Public materialized-state observations across the nodes of a scenario cluster.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Comparing the public materialized records and watermarks every node reports.
//! - **Depends on.** The scenario world and public command execution.
//! - **Must not know.** Runtime state stores, snapshot encoding, or materialized read internals.

use std::collections::BTreeSet;

use cucumber::{gherkin::Step, then};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::parse_duration_text;
use nervix_primitives::time::{Instant, sleep};

use crate::{ScenarioWorld, docstring, expand_placeholders, run_plexus_commands_on_node};

#[then(
    expr = "within {string} every node reports the same materialized records for relay {string} \
            with these prefixes"
)]
async fn every_node_reports_the_same_materialized_records(
    world: &mut ScenarioWorld,
    duration: String,
    relay: String,
    #[step] step: &Step,
) {
    let timeout = parse_duration_text(&duration)
        .assured("the feature supplies a supported assertion duration");
    let relay = expand_placeholders(world, &relay);
    let command = format!("SHOW RELAY {relay} MATERIALIZED STATE;");
    let expected = expand_placeholders(world, docstring(step));
    let mut expected_prefixes = BTreeSet::new();
    for line in expected.lines() {
        let prefix = line.trim();
        if !prefix.is_empty() {
            assert!(
                expected_prefixes.insert(prefix.to_owned()),
                "unique record prefixes"
            );
        }
    }
    assert!(
        !expected_prefixes.is_empty(),
        "at least one expected record"
    );
    let mut first_records = None;
    for node in world.cluster().node_ids() {
        nervix_primitives::task::consume_budget().await;
        let deadline = Instant::now()
            .checked_add(timeout)
            .assured("the feature's thirty-second bound fits the process clock");
        loop {
            nervix_primitives::task::consume_budget().await;
            match run_plexus_commands_on_node(world, &node, &command).await {
                Ok(output) => {
                    let mut prefixes = BTreeSet::new();
                    let mut records = BTreeSet::new();
                    for line in output.lines() {
                        let record = line.trim();
                        if record.starts_with("key=") {
                            let Some((prefix, _)) = record.split_once(" low=") else {
                                panic!("a materialized record reports its watermarks: {record}");
                            };
                            prefixes.insert(prefix.to_owned());
                            records.insert(record.to_owned());
                        }
                    }
                    world.last_command_output = Some(output);
                    if prefixes == expected_prefixes {
                        if let Some(first) = &first_records {
                            assert_eq!(
                                &records, first,
                                "node {node} reports the same records and watermarks"
                            );
                        } else {
                            first_records = Some(records);
                        }
                        break;
                    }
                }
                Err(error) => world.last_command_error = Some(error),
            }
            assert!(
                Instant::now() < deadline,
                "node {node} did not report the expected materialized records: {:?}; error: {:?}",
                world.last_command_output,
                world.last_command_error
            );
            sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}
