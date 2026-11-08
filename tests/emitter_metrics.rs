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

#[then(expr = "the last command output emitter is saved as placeholder {string}")]
fn save_described_emitter(world: &mut ScenarioWorld, placeholder: String) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("DESCRIBE output exists");
    let emitter = output
        .lines()
        .find_map(|line| line.trim().strip_prefix("emitter: "))
        .unwrap_or_else(|| panic!("DESCRIBE output has no emitter name: {output}"));
    world
        .placeholders
        .insert(placeholder, emitter.trim().to_owned());
}

#[then(
    expr = "node {string} observability metric {string} with labels eventually becomes positive"
)]
async fn interaction_metric_becomes_positive(
    world: &mut ScenarioWorld,
    node: String,
    metric: String,
    #[step] step: &Step,
) {
    let observation = InteractionObservation::from_step(world, &node, &metric, step);
    let mut last = String::new();
    let completed =
        nervix_primitives::time::timeout(common::status_request::STATUS_WAIT_BUDGET, async {
            loop {
                nervix_primitives::task::consume_budget().await;
                let sample = world
                    .cluster()
                    .read_observability_metric(
                        &observation.node,
                        &observation.metric,
                        &observation.labels,
                    )
                    .await;
                match sample {
                    Ok(value) if value.is_finite() && value > 0.0 => break,
                    Ok(value) => last = value.to_string(),
                    Err(error) => last = error.to_string(),
                }
                nervix_primitives::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
    assert!(
        completed.is_ok(),
        "{} with {:?} did not become positive: {last}",
        observation.metric,
        observation.labels
    );
}

#[then(expr = "node {string} records one completed transport finish for emitter {string}")]
async fn completed_emitter_finish(world: &mut ScenarioWorld, node: String, emitter: String) {
    let node = expand_placeholders(world, &node);
    let emitter = expand_placeholders(world, &emitter);
    let mut last = Vec::new();
    let completed =
        nervix_primitives::time::timeout(common::status_request::STATUS_WAIT_BUDGET, async {
            loop {
                last.clear();
                for outcome in ["nothing_held", "flushed"] {
                    let labels = vec![
                        format!("emitter=\"{emitter}\""),
                        format!("outcome=\"{outcome}\""),
                    ];
                    let value = world
                        .cluster()
                        .read_observability_metric(&node, "nervix_emitter_finishes_total", &labels)
                        .await;
                    last.push(value.map_err(|error| error.to_string()));
                }
                if let [Ok(nothing_held), Ok(flushed)] = last.as_slice() {
                    let finishes = nothing_held + flushed;
                    if finishes == 1.0 {
                        break;
                    }
                }
                nervix_primitives::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
    assert!(
        completed.is_ok(),
        "emitter {emitter} on {node} did not record one completed finish: {last:?}"
    );
}
