//! Layer: test harness.
//! - **Owns.** Label-selected counter snapshots and inspection through every cluster node.
//! - **Depends on.** The scenario world, native sessions and the public observability endpoint.
//! - **Must not know.** Metric storage, emitter tasks or connector implementations.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests own metric witnesses")
)]

use super::*;

#[when(expr = "emitter {string} answers its next record write with an unknown record")]
fn emitter_answers_with_an_unknown_record(world: &mut ScenarioWorld, emitter: String) {
    let emitter = expand_placeholders(world, &emitter);
    world
        .fault_injection
        .answer_next_emitter_with_unknown_record(&emitter);
}

struct InteractionObservation {
    node: String,
    metric: String,
    labels: Vec<String>,
}

impl InteractionObservation {
    fn from_step(world: &ScenarioWorld, node: &str, metric: &str, step: &Step) -> Self {
        let labels = docstring(step)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| expand_placeholders(world, line))
            .collect();
        Self {
            node: expand_placeholders(world, node),
            metric: expand_placeholders(world, metric),
            labels,
        }
    }

    async fn read(&self, world: &ScenarioWorld) -> f64 {
        world
            .cluster()
            .read_observability_metric(&self.node, &self.metric, &self.labels)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "node '{}' did not expose {} with {:?}: {error}",
                    self.node, self.metric, self.labels
                )
            })
    }
}

#[then(
    expr = "node {string} observability metric {string} with labels is saved as placeholder \
            {string}"
)]
async fn save_interaction_counter(
    world: &mut ScenarioWorld,
    node: String,
    metric: String,
    placeholder: String,
    #[step] step: &Step,
) {
    let observation = InteractionObservation::from_step(world, &node, &metric, step);
    let value = observation.read(world).await;
    assert!(value.is_finite());
    world.placeholders.insert(placeholder, value.to_string());
}

#[then(
    expr = "node {string} observability metric {string} with labels equals saved placeholder \
            {string}"
)]
async fn interaction_counter_equals_saved(
    world: &mut ScenarioWorld,
    node: String,
    metric: String,
    placeholder: String,
    #[step] step: &Step,
) {
    let expected: f64 = world.placeholders[&placeholder]
        .parse()
        .assured("the snapshot step saves a numeric metric value");
    let observation = InteractionObservation::from_step(world, &node, &metric, step);
    assert_eq!(observation.read(world).await, expected);
}

#[then(expr = "DESCRIBE EMITTER {string} from every node contains")]
async fn describe_emitter_from_every_node(
    world: &mut ScenarioWorld,
    emitter: String,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step));
    let emitter = expand_placeholders(world, &emitter);
    for node in world.cluster().node_ids() {
        nervix_primitives::task::consume_budget().await;
        let output =
            run_plexus_commands_on_node(world, &node, &format!("DESCRIBE EMITTER {emitter};"))
                .await
                .unwrap_or_else(|error| {
                    panic!("node '{node}' could not describe emitter: {error}")
                });
        assert!(
            output.contains(expected.trim()),
            "node '{node}' did not return the owner's destination metrics. expected: {expected}; \
             output: {output}"
        );
        world.last_command_output = Some(output);
    }
}
