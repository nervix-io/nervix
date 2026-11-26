//! Public assertions that entity descriptions show the labels their entities carry.
//!
//! Layer: test harness.
//! - **Owns.** The table-driven check that each listed `DESCRIBE` statement shows label text.
//! - **Depends on.** The scenario world and its public command transport.
//! - **Must not know.** Registry state, Model types or how a description is rendered.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "observes entity descriptions through public sessions"
    )
)]

use super::*;

/// Runs every listed description on the leader and requires its output to contain the label text
/// of its row. Every description is checked before the step fails, so one run names each
/// description that omits its entity's labels.
#[then("every one of these descriptions shows its entity's labels")]
async fn descriptions_show_their_labels(world: &mut ScenarioWorld, #[step] step: &Step) {
    let table = step
        .table
        .as_ref()
        .assured("the step lists its descriptions");
    let mut rows = table.rows.iter();
    assert_eq!(
        rows.next()
            .assured("the table names its columns")
            .as_slice(),
        ["description", "labels"]
    );
    let leader = current_leader_node(world).await;
    let mut failures = Vec::new();
    for row in rows {
        nervix_primitives::task::consume_budget().await;
        let [description, labels] = row.as_slice() else {
            panic!("each row names a description statement and the label text it shows");
        };
        let description = expand_placeholders(world, description);
        let labels = expand_placeholders(world, labels);
        let command = format!("{description};");
        match run_plexus_commands_on_node(world, &leader, &command).await {
            Ok(output) => {
                if !output.contains(labels.as_str()) {
                    failures.push(format!("{description} does not show {labels}:\n{output}"));
                }
            }
            Err(error) => failures.push(format!("{description} failed: {error}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of the descriptions do not show their entity's labels:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
