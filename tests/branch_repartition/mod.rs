//! Public branch repartition checks.
//!
//! Outside the layer order: a scenario harness.
//!
//! - **Owns.** Waiting on `DESCRIBE BRANCH` until it reports a branch's partition epoch, the
//!   transition from its previous partition count and the keys that transition still moves, while a
//!   running domain changes the branch's partition count.
//! - **Depends on.** The public command path of the in-process cluster.
//! - **Must not know.** How the runtime moves a key between partitions or decides that an epoch
//!   retires.

use super::*;

/// How often a wait on `DESCRIBE BRANCH` asks again.
const DESCRIBE_BRANCH_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Whether `output` holds `expected` as one of its lines, compared without surrounding spaces.
fn output_has_line(output: &str, expected: &str) -> bool {
    for line in output.lines() {
        if line.trim() == expected {
            return true;
        }
    }
    false
}

/// Whether `output` holds a line that starts with `prefix`, compared without leading spaces.
fn output_has_line_starting_with(output: &str, prefix: &str) -> bool {
    for line in output.lines() {
        if line.trim_start().starts_with(prefix) {
            return true;
        }
    }
    false
}

/// What one `DESCRIBE BRANCH` wait accepts: every expected line, or a settled epoch.
enum DescribeBranchExpectation {
    /// The output holds each of these lines.
    Lines(Vec<String>),
    /// The output names the branch's partition epoch and no transition from a previous count.
    NoTransition,
}

impl DescribeBranchExpectation {
    fn accepts(&self, output: &str) -> bool {
        match self {
            Self::Lines(lines) => {
                for line in lines {
                    if !output_has_line(output, line) {
                        return false;
                    }
                }
                true
            }
            Self::NoTransition => {
                output_has_line_starting_with(output, "partition epoch:")
                    && !output_has_line_starting_with(output, "previous partitions:")
            }
        }
    }
}

/// Asks node `node_id` to describe `branch` until its output meets `expectation`, failing with
/// the last output once `duration` passes.
async fn wait_for_describe_branch(
    world: &mut ScenarioWorld,
    duration: &str,
    branch: &str,
    node_id: &str,
    expectation: DescribeBranchExpectation,
) {
    let duration = parse_duration_text(duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, node_id);
    let branch = expand_placeholders(world, branch);
    let command = format!("DESCRIBE BRANCH {branch};");
    let deadline = PhaseDeadline::after(duration);
    loop {
        nervix_primitives::task::consume_budget().await;
        assert!(
            !deadline.has_passed(),
            "timed out waiting for DESCRIBE BRANCH {branch} on node '{node_id}'. last output: \
             {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        match run_plexus_commands_on_node(world, &node_id, &command).await {
            Ok(output) => {
                let accepted = expectation.accepts(&output);
                world.last_command_output = Some(output);
                if accepted {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(error),
        }
        deadline.pause(DESCRIBE_BRANCH_POLL_INTERVAL).await;
    }
}

#[then(expr = "within {string} DESCRIBE BRANCH {string} on node {string} contains")]
async fn then_within_duration_describe_branch_contains(
    world: &mut ScenarioWorld,
    duration: String,
    branch: String,
    node_id: String,
    #[step] step: &Step,
) {
    let mut lines = Vec::new();
    for line in docstring(step).lines() {
        let line = line.trim();
        if !line.is_empty() {
            lines.push(expand_placeholders(world, line));
        }
    }
    assert!(
        !lines.is_empty(),
        "step docstring must list at least one expected DESCRIBE BRANCH line"
    );
    wait_for_describe_branch(
        world,
        &duration,
        &branch,
        &node_id,
        DescribeBranchExpectation::Lines(lines),
    )
    .await;
}

#[then(
    expr = "within {string} DESCRIBE BRANCH {string} on node {string} reports no partition \
            transition"
)]
async fn then_within_duration_describe_branch_reports_no_transition(
    world: &mut ScenarioWorld,
    duration: String,
    branch: String,
    node_id: String,
) {
    wait_for_describe_branch(
        world,
        &duration,
        &branch,
        &node_id,
        DescribeBranchExpectation::NoTransition,
    )
    .await;
}

/// The partitions `SHOW CLUSTER STATUS` schedules for one processor or relay: while a branch's keys
/// move, an entity that keeps state per key keeps the units of the previous count as holding units.
#[then(expr = "the last cluster status schedules {int} partitions of {word} {string}")]
async fn then_last_cluster_status_schedules_partitions(
    world: &mut ScenarioWorld,
    expected: u64,
    kind: String,
    name: String,
) {
    let name = expand_placeholders(world, &name);
    let status = world
        .last_command_output
        .as_deref()
        .assured("SHOW CLUSTER STATUS ran before its partition assertion");
    let placements = crate::ingestor_instances::UnitPlacements::from_status(
        status,
        &world.domain,
        &kind,
        &name,
        "partition:",
    );
    let expected_partitions = (0..expected).collect::<BTreeSet<_>>();
    assert_eq!(
        placements.indexes(),
        expected_partitions,
        "{kind} '{name}' must be scheduled as partitions {expected_partitions:?}: {status}"
    );
}
