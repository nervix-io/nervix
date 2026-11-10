//! Public checks of scheduled execution-unit ownership.
//!
//! Outside the layer order: a scenario harness.
//!
//! - **Owns.** Inspecting instance and partition identities and owners through `SHOW CLUSTER STATUS`.
//! - **Depends on.** Public command output and the scenario world.
//! - **Must not know.** Scheduler decisions, runtime maps or connector internals.

use super::*;

#[then(
    expr = "the last command output metric {string} {string} relay {string} instance {int} on any \
            physical node has numeric values"
)]
async fn instance_metric_has_numeric_values(
    world: &mut ScenarioWorld,
    metric: String,
    direction: String,
    relay: String,
    instance: u64,
    #[step] step: &Step,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    let metric = expand_placeholders(world, &metric);
    let direction = expand_placeholders(world, &direction);
    let relay = expand_placeholders(world, &relay);
    let prefix = format!("{metric} {direction} relay={relay} instance={instance} physical_node=");
    assert_numeric_metric_lines(world, step, &prefix, output);
}

pub(crate) struct UnitPlacements {
    owners: BTreeMap<u64, String>,
}

impl UnitPlacements {
    pub(crate) fn from_status(
        status: &str,
        domain: &str,
        kind: &str,
        name: &str,
        prefix: &str,
    ) -> Self {
        let mut owners = BTreeMap::new();
        for line in status.lines() {
            let Some(line) = line.trim().strip_prefix("- ") else {
                continue;
            };
            let mut fields = BTreeMap::new();
            for field in line.split_whitespace() {
                if let Some((name, value)) = field.split_once('=') {
                    fields.insert(name, value);
                }
            }
            if fields.get("domain") != Some(&domain)
                || fields.get("kind") != Some(&kind)
                || fields.get("name") != Some(&name)
            {
                continue;
            }
            let Some(unit) = fields.get("unit") else {
                continue;
            };
            let Some(index) = unit.strip_prefix(prefix) else {
                continue;
            };
            let Ok(index) = index.parse::<u64>() else {
                panic!("a public instance unit has an unsigned index: {status}");
            };
            let Some(owner) = fields.get("owner") else {
                panic!("a scheduled instance reports its owner: {status}");
            };
            let owner = owner.to_string();
            assert!(
                owners.insert(index, owner).is_none(),
                "{kind} {name} unit {index} must be scheduled exactly once: {status}"
            );
        }
        Self { owners }
    }

    /// The indexes of every scheduled unit, in order.
    pub(crate) fn indexes(&self) -> BTreeSet<u64> {
        self.owners.keys().copied().collect()
    }

    fn assert_spread(&self, instances: u64, owners: usize, status: &str) {
        let actual = self.owners.keys().copied().collect::<BTreeSet<_>>();
        let expected = (0..instances).collect::<BTreeSet<_>>();
        assert_eq!(
            actual, expected,
            "the schedule must contain every declared instance exactly once: {status}"
        );
        let actual_owners = self.owners.values().collect::<BTreeSet<_>>();
        assert_eq!(
            actual_owners.len(),
            owners,
            "declared instances must spread over the eligible owners: {status}"
        );
        for owner in actual_owners {
            assert_ne!(
                owner, "-",
                "every scheduled instance has an owner: {status}"
            );
        }
    }
}

#[then(
    expr = "the last cluster status lists {int} partitions of {word} {string} on {int} distinct \
            owners"
)]
async fn partitioned_node_has_every_unit(
    world: &mut ScenarioWorld,
    partitions: u64,
    kind: String,
    name: String,
    owners: usize,
) {
    let name = expand_placeholders(world, &name);
    let status = world
        .last_command_output
        .as_deref()
        .assured("SHOW CLUSTER STATUS ran before its partition assertion");
    let placements = UnitPlacements::from_status(status, &world.domain, &kind, &name, "partition:");
    placements.assert_spread(partitions, owners, status);
}

#[then(
    expr = "the last cluster status colocates every partition of {word} {string} with the \
            corresponding partition of relay {string}"
)]
async fn processor_partition_follows_its_upstream_partition(
    world: &mut ScenarioWorld,
    kind: String,
    name: String,
    relay: String,
) {
    let name = expand_placeholders(world, &name);
    let relay = expand_placeholders(world, &relay);
    let status = world
        .last_command_output
        .as_deref()
        .assured("SHOW CLUSTER STATUS ran before its locality assertion");
    let processor = UnitPlacements::from_status(status, &world.domain, &kind, &name, "partition:");
    let upstream =
        UnitPlacements::from_status(status, &world.domain, "relay", &relay, "partition:");
    assert!(
        !processor.owners.is_empty(),
        "the processor has partition units: {status}"
    );
    assert_eq!(
        processor.owners, upstream.owners,
        "a pipeline's corresponding partitions must prefer the same owners: {status}"
    );
}

#[then(
    expr = "the owner of every partition of {word} {string} is saved with placeholder prefix \
            {string}"
)]
async fn save_processor_partition_owners(
    world: &mut ScenarioWorld,
    kind: String,
    name: String,
    prefix: String,
) {
    let name = expand_placeholders(world, &name);
    let status = world
        .last_command_output
        .as_deref()
        .assured("the scenario read cluster status");
    let placements = UnitPlacements::from_status(status, &world.domain, &kind, &name, "partition:");
    assert!(
        !placements.owners.is_empty(),
        "the node reports partition owners: {status}"
    );
    for (partition, owner) in placements.owners {
        assert_ne!(owner, "-", "every partition has an owner: {status}");
        world
            .placeholders
            .insert(format!("{prefix}_{partition}"), owner);
    }
}

#[then(
    expr = "a partition owner of {word} {string} other than placeholder {string} is saved as \
            placeholder {string}"
)]
async fn save_processor_partition_owner_except(
    world: &mut ScenarioWorld,
    kind: String,
    name: String,
    excluded: String,
    placeholder: String,
) {
    let name = expand_placeholders(world, &name);
    let excluded = world
        .placeholders
        .get(&excluded)
        .assured("the excluded node was saved");
    let status = world
        .last_command_output
        .as_deref()
        .assured("the scenario read cluster status");
    let placements = UnitPlacements::from_status(status, &world.domain, &kind, &name, "partition:");
    let owner = placements
        .owners
        .into_values()
        .find(|owner| owner != "-" && owner != excluded)
        .assured("a partition owner differs from the excluded node");
    world.placeholders.insert(placeholder, owner);
}

#[then(
    expr = "within {string} node {string} eventually reports {word} {string} partitions moved \
            from node {string} and all other owners unchanged from placeholder prefix {string}"
)]
async fn wait_for_only_affected_processor_partition_owners_to_move(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    kind: String,
    name: String,
    former_owner: String,
    prefix: String,
) {
    let duration = parse_duration_text(&duration).assured("the scenario writes a valid timeout");
    let node_id = expand_placeholders(world, &node_id);
    let name = expand_placeholders(world, &name);
    let former_owner = expand_placeholders(world, &former_owner);
    let before = world
        .placeholders
        .iter()
        .filter_map(|(key, owner)| {
            let partition = key
                .strip_prefix(&format!("{prefix}_"))?
                .parse::<u64>()
                .ok()?;
            Some((partition, owner.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    assert!(
        !before.is_empty(),
        "the scenario saved the previous partition owners"
    );
    let deadline = PhaseDeadline::after(duration);
    loop {
        nervix_primitives::task::consume_budget().await;
        assert!(
            !deadline.has_passed(),
            "only partitions on {former_owner} must move; before={before:?}, output={:?}, \
             error={:?}",
            world.last_command_output,
            world.last_command_error
        );
        match world.cluster().status_text(&node_id, deadline).await {
            Ok(output) => {
                let after =
                    UnitPlacements::from_status(&output, &world.domain, &kind, &name, "partition:");
                let settled = before.len() == after.owners.len()
                    && before.iter().all(|(partition, owner)| {
                        after.owners.get(partition).is_some_and(|current| {
                            current != "-"
                                && if *owner == former_owner {
                                    *current != former_owner
                                } else {
                                    current == owner
                                }
                        })
                    });
                world.last_command_output = Some(output);
                if settled {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(format!("{error:#}")),
        }
        deadline.pause(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "the last cluster status lists {int} instances of ingestor {string} on {int} distinct \
            owners"
)]
async fn then_cluster_status_lists_ingestor_instances(
    world: &mut ScenarioWorld,
    instances: u64,
    ingestor: String,
    owners: usize,
) {
    let ingestor = expand_placeholders(world, &ingestor);
    let status = world
        .last_command_output
        .as_deref()
        .assured("the scenario executes SHOW CLUSTER STATUS before inspecting instance placement");
    let placements =
        UnitPlacements::from_status(status, &world.domain, "ingestor", &ingestor, "instance:");
    placements.assert_spread(instances, owners, status);
}

#[then(
    expr = "the last cluster status colocates every instance of ingestor {string} with the \
            corresponding partition of relay {string}"
)]
async fn then_instances_follow_partition_owners(
    world: &mut ScenarioWorld,
    ingestor: String,
    relay: String,
) {
    let ingestor = expand_placeholders(world, &ingestor);
    let relay = expand_placeholders(world, &relay);
    let status = world.last_command_output.as_deref().assured(
        "the scenario executes SHOW CLUSTER STATUS before comparing source and relay owners",
    );
    let instances =
        UnitPlacements::from_status(status, &world.domain, "ingestor", &ingestor, "instance:");
    let partitions =
        UnitPlacements::from_status(status, &world.domain, "relay", &relay, "partition:");
    assert!(
        !partitions.owners.is_empty(),
        "the relay reports its partition owners: {status}"
    );
    assert_eq!(
        instances.owners, partitions.owners,
        "each instance follows its corresponding partition owner: {status}"
    );
}

#[then(
    expr = "the owner of every instance of ingestor {string} is saved with placeholder prefix \
            {string}"
)]
async fn save_ingestor_instance_owners(
    world: &mut ScenarioWorld,
    ingestor: String,
    prefix: String,
) {
    let status = world
        .last_command_output
        .as_deref()
        .assured("SHOW CLUSTER STATUS precedes saving instance owners");
    let placements =
        UnitPlacements::from_status(status, &world.domain, "ingestor", &ingestor, "instance:");
    assert!(
        !placements.owners.is_empty(),
        "the ingestor reports instance owners: {status}"
    );
    for (index, owner) in placements.owners {
        assert_ne!(owner, "-", "every saved instance has an owner");
        world
            .placeholders
            .insert(format!("{prefix}_{index}"), owner);
    }
}

#[then(
    expr = "an instance owner of ingestor {string} other than placeholder {string} is saved as \
            placeholder {string}"
)]
async fn save_ingestor_instance_owner_other_than(
    world: &mut ScenarioWorld,
    ingestor: String,
    excluded: String,
    placeholder: String,
) {
    let excluded = world
        .placeholders
        .get(&excluded)
        .assured("the leader placeholder is saved before selecting a different instance owner");
    let status = world
        .last_command_output
        .as_deref()
        .assured("SHOW CLUSTER STATUS precedes selecting an instance owner");
    let placements =
        UnitPlacements::from_status(status, &world.domain, "ingestor", &ingestor, "instance:");
    let owner = placements
        .owners
        .values()
        .find(|owner| *owner != excluded && owner.as_str() != "-")
        .assured("spread instances have a live owner other than the leader")
        .clone();
    world.placeholders.insert(placeholder, owner);
}

#[then(
    expr = "within {string} node {string} eventually reports ingestor {string} instances moved \
            from node {string} and all other owners unchanged from placeholder prefix {string}"
)]
async fn wait_for_only_affected_instance_owners_to_move(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    ingestor: String,
    former_owner: String,
    prefix: String,
) {
    let duration = parse_duration_text(&duration).assured("the scenario writes a valid timeout");
    let node_id = expand_placeholders(world, &node_id);
    let former_owner = expand_placeholders(world, &former_owner);
    let before = world
        .placeholders
        .iter()
        .filter_map(|(key, owner)| {
            let index = key
                .strip_prefix(&format!("{prefix}_"))?
                .parse::<u64>()
                .ok()?;
            Some((index, owner.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    assert!(
        !before.is_empty(),
        "the scenario saved every instance owner before the transition"
    );
    let deadline = PhaseDeadline::after(duration);
    loop {
        nervix_primitives::task::consume_budget().await;
        assert!(
            !deadline.has_passed(),
            "only instances on {former_owner} must move; before={before:?}, last output={:?}, \
             last error={:?}",
            world.last_command_output,
            world.last_command_error
        );
        match world.cluster().status_text(&node_id, deadline).await {
            Ok(output) => {
                let after = UnitPlacements::from_status(
                    &output,
                    &world.domain,
                    "ingestor",
                    &ingestor,
                    "instance:",
                );
                let settled = before.len() == after.owners.len()
                    && before.iter().all(|(index, owner)| {
                        after.owners.get(index).is_some_and(|current| {
                            if owner == &former_owner {
                                current != "-" && current != owner
                            } else {
                                current == owner
                            }
                        })
                    });
                world.last_command_output = Some(output);
                if settled {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(format!("{error:#}")),
        }
        deadline.pause(Duration::from_millis(100)).await;
    }
}

fn ready_instance_owners(output: &str) -> BTreeMap<u64, String> {
    let mut owners = BTreeMap::new();
    for line in output.lines() {
        let Some(line) = line.trim().strip_prefix("instance ") else {
            continue;
        };
        let Some((index, fields)) = line.split_once(':') else {
            continue;
        };
        let Ok(index) = index.parse::<u64>() else {
            continue;
        };
        let fields = fields
            .split_whitespace()
            .filter_map(|field| field.split_once('='))
            .collect::<BTreeMap<_, _>>();
        if fields.get("status") == Some(&"running")
            && fields.get("ready") == Some(&"true")
            && let Some(owner) = fields.get("node")
        {
            assert!(
                owners.insert(index, owner.to_string()).is_none(),
                "DESCRIBE reports each instance exactly once: {output}"
            );
        }
    }
    owners
}

#[then(
    expr = "within {string} every instance of ingestor {string} reports running and ready on its \
            scheduled owner"
)]
async fn wait_for_each_instance_ready_on_its_owner(
    world: &mut ScenarioWorld,
    duration: String,
    ingestor: String,
) {
    let duration = parse_duration_text(&duration).assured("the scenario writes a valid timeout");
    let deadline = PhaseDeadline::after(duration);
    let node_id = current_leader_node(world).await;
    let mut last_description = None;
    let mut last_error = None;
    loop {
        nervix_primitives::task::consume_budget().await;
        assert!(
            !deadline.has_passed(),
            "every scheduled instance must report its exact owner and readiness; last \
             output={:?}, last error={:?}",
            last_description,
            last_error
        );
        if let Ok(status) = world.cluster().status_text(&node_id, deadline).await {
            let scheduled = UnitPlacements::from_status(
                &status,
                &world.domain,
                "ingestor",
                &ingestor,
                "instance:",
            );
            match run_plexus_commands_on_node(
                world,
                &node_id,
                &format!("DESCRIBE INGESTOR {ingestor};"),
            )
            .await
            {
                Ok(output) => {
                    let ready = ready_instance_owners(&output);
                    let all_ready = !scheduled.owners.is_empty() && scheduled.owners == ready;
                    last_description = Some(output);
                    if all_ready {
                        return;
                    }
                }
                Err(error) => last_error = Some(error),
            }
        }
        deadline.pause(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "every scheduled instance of ingestor {string} exposes its instance label in Prometheus"
)]
async fn every_instance_exposes_prometheus_identity(world: &mut ScenarioWorld, ingestor: String) {
    let node_id = current_leader_node(world).await;
    let deadline = PhaseDeadline::after(Duration::from_secs(60));
    let status = world
        .cluster()
        .status_text(&node_id, deadline)
        .await
        .assured("the ready cluster serves its current unit schedule");
    let placements =
        UnitPlacements::from_status(&status, &world.domain, "ingestor", &ingestor, "instance:");
    assert!(
        !placements.owners.is_empty(),
        "the ingestor has scheduled instances: {status}"
    );
    for (instance, owner) in placements.owners {
        nervix_primitives::task::consume_budget().await;
        let labels = vec![
            format!("domain=\"{}\"", world.domain),
            format!("ingestor=\"{ingestor}\""),
            format!("instance=\"{instance}\""),
            format!("physical_node_id=\"{owner}\""),
        ];
        world
            .cluster()
            .wait_for_observability_metric_at_least(
                &owner,
                "nervix_ingestor_quiesce_buffered_records",
                &labels,
                0,
                Some(Duration::from_secs(60)),
            )
            .await
            .assured("each owner publishes its exact instance's ingestor metric series");
    }
}
