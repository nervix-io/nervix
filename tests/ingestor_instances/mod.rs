//! Public checks of scheduled execution-unit ownership.
//!
//! Outside the layer order: a scenario harness.
//!
//! - **Owns.** Inspecting instance and partition identities and owners through `SHOW CLUSTER STATUS`,
//!   and the per-instance lines `DESCRIBE INGESTOR` and `DESCRIBE EMITTER` report.
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

#[then(
    expr = "within {string} DESCRIBE INGESTOR {string} metric {string} {string} relay {string} \
            totals {int}"
)]
async fn ingestor_metric_totals_across_instances(
    world: &mut ScenarioWorld,
    duration: String,
    ingestor: String,
    metric: String,
    direction: String,
    relay: String,
    expected: u64,
) {
    let duration = parse_duration_text(&duration).assured("the scenario declares a valid wait");
    let ingestor = expand_placeholders(world, &ingestor);
    let metric = expand_placeholders(world, &metric);
    let direction = expand_placeholders(world, &direction);
    let relay = expand_placeholders(world, &relay);
    let prefix = format!("{metric} {direction} relay={relay} ");
    let node = current_leader_node(world).await;
    let deadline = PhaseDeadline::after(duration);
    let mut last_output = None;
    let mut last_error = None;
    loop {
        nervix_primitives::task::consume_budget().await;
        match run_plexus_commands_on_node(world, &node, &format!("DESCRIBE INGESTOR {ingestor};"))
            .await
        {
            Ok(output) => {
                // DESCRIBE queries each current unit's owner and deduplicates its durable
                // series. Include every instance and predecessor physical-node contribution.
                let mut samples = 0;
                let mut total = 0_u64;
                for line in output
                    .lines()
                    .map(str::trim)
                    .filter(|line| line.starts_with(&prefix))
                {
                    let value = metric_line_value(line, "total")
                        .assured("a matching metric reports its total")
                        .parse::<u64>()
                        .assured("a message metric has an integral nonnegative total");
                    total = total
                        .checked_add(value)
                        .assured("the bounded fixture total fits u64");
                    samples += 1;
                }
                last_output = Some(output);
                if samples > 0 && total == expected {
                    return;
                }
            }
            Err(error) => last_error = Some(error),
        }
        assert!(
            !deadline.has_passed(),
            "DESCRIBE INGESTOR {ingestor} metric '{prefix}' did not total {expected}: \
             output={last_output:?}, error={last_error:?}"
        );
        deadline.pause(Duration::from_millis(100)).await;
    }
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
    expr = "the last cluster status lists {int} instances of {word} {string} on {int} distinct \
            owners"
)]
async fn then_cluster_status_lists_instances(
    world: &mut ScenarioWorld,
    instances: u64,
    kind: String,
    name: String,
    owners: usize,
) {
    let name = expand_placeholders(world, &name);
    let status = world
        .last_command_output
        .as_deref()
        .assured("the scenario executes SHOW CLUSTER STATUS before inspecting instance placement");
    let placements = UnitPlacements::from_status(status, &world.domain, &kind, &name, "instance:");
    placements.assert_spread(instances, owners, status);
}

#[then(
    expr = "the last cluster status colocates every instance of {word} {string} with the \
            corresponding partition of relay {string}"
)]
async fn then_instances_follow_partition_owners(
    world: &mut ScenarioWorld,
    kind: String,
    name: String,
    relay: String,
) {
    let name = expand_placeholders(world, &name);
    let relay = expand_placeholders(world, &relay);
    let status = world.last_command_output.as_deref().assured(
        "the scenario executes SHOW CLUSTER STATUS before comparing instance and relay owners",
    );
    let instances = UnitPlacements::from_status(status, &world.domain, &kind, &name, "instance:");
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
    expr = "the last cluster status colocates every instance of emitter {string} with the \
            partitions it serves of relay {string}"
)]
async fn then_emitter_instances_follow_their_served_partitions(
    world: &mut ScenarioWorld,
    emitter: String,
    relay: String,
) {
    let emitter = expand_placeholders(world, &emitter);
    let relay = expand_placeholders(world, &relay);
    let status = world.last_command_output.as_deref().assured(
        "the scenario executes SHOW CLUSTER STATUS before comparing instance and relay owners",
    );
    let instances =
        UnitPlacements::from_status(status, &world.domain, "emitter", &emitter, "instance:");
    let partitions =
        UnitPlacements::from_status(status, &world.domain, "relay", &relay, "partition:");
    let instance_count = u64::try_from(instances.owners.len())
        .assured("a scenario declares fewer instances than u64 can count");
    assert!(
        instance_count > 0 && !partitions.owners.is_empty(),
        "the emitter reports instance owners and the relay its partition owners: {status}"
    );
    for (partition, partition_owner) in &partitions.owners {
        let instance = partition % instance_count;
        let instance_owner = instances
            .owners
            .get(&instance)
            .assured("every partition maps to one of the declared instances");
        assert_eq!(
            instance_owner, partition_owner,
            "instance {instance} must share the owner of partition {partition} it serves: {status}"
        );
    }
}

#[then(
    expr = "the owner of every instance of {word} {string} is saved with placeholder prefix \
            {string}"
)]
async fn save_instance_owners(
    world: &mut ScenarioWorld,
    kind: String,
    name: String,
    prefix: String,
) {
    let name = expand_placeholders(world, &name);
    let status = world
        .last_command_output
        .as_deref()
        .assured("SHOW CLUSTER STATUS precedes saving instance owners");
    let placements = UnitPlacements::from_status(status, &world.domain, &kind, &name, "instance:");
    assert!(
        !placements.owners.is_empty(),
        "the {kind} reports instance owners: {status}"
    );
    for (index, owner) in placements.owners {
        assert_ne!(owner, "-", "every saved instance has an owner");
        world
            .placeholders
            .insert(format!("{prefix}_{index}"), owner);
    }
}

#[then(
    expr = "an instance owner of {word} {string} other than placeholder {string} is saved as \
            placeholder {string}"
)]
async fn save_instance_owner_other_than(
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
        .assured("the leader placeholder is saved before selecting a different instance owner");
    let status = world
        .last_command_output
        .as_deref()
        .assured("SHOW CLUSTER STATUS precedes selecting an instance owner");
    let placements = UnitPlacements::from_status(status, &world.domain, &kind, &name, "instance:");
    let owner = placements
        .owners
        .values()
        .find(|owner| *owner != excluded && owner.as_str() != "-")
        .assured("spread instances have a live owner other than the leader")
        .clone();
    world.placeholders.insert(placeholder, owner);
}

#[then(
    expr = "within {string} node {string} eventually reports {word} {string} instances moved from \
            node {string} and all other owners unchanged from placeholder prefix {string}"
)]
async fn wait_for_only_affected_instance_owners_to_move(
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
                let after =
                    UnitPlacements::from_status(&output, &world.domain, &kind, &name, "instance:");
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
    let node_id = running_leader_node(world).await;
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

/// One `instance <index>: node=<owner> status=<status> partitions=<list>` line of
/// `DESCRIBE EMITTER`.
struct DescribedEmitterInstance {
    node: String,
    status: String,
    partitions: String,
}

fn described_emitter_instances(output: &str) -> BTreeMap<u64, DescribedEmitterInstance> {
    let mut instances = BTreeMap::new();
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
        let (Some(node), Some(status), Some(partitions)) = (
            fields.get("node"),
            fields.get("status"),
            fields.get("partitions"),
        ) else {
            panic!("an emitter instance line names its node, status and partitions: {output}");
        };
        let described = DescribedEmitterInstance {
            node: node.to_string(),
            status: status.to_string(),
            partitions: partitions.to_string(),
        };
        assert!(
            instances.insert(index, described).is_none(),
            "DESCRIBE EMITTER reports each instance exactly once: {output}"
        );
    }
    instances
}

/// What one emitter instance must report before a `DESCRIBE EMITTER` wait ends.
enum EmitterInstanceExpectation {
    EveryInstanceRunning,
    Idle { instance: u64 },
    Serves { instance: u64, partitions: String },
}

impl EmitterInstanceExpectation {
    fn holds(
        &self,
        scheduled: &BTreeMap<u64, String>,
        described: &BTreeMap<u64, DescribedEmitterInstance>,
    ) -> bool {
        match self {
            Self::EveryInstanceRunning => {
                !scheduled.is_empty()
                    && scheduled.len() == described.len()
                    && scheduled.iter().all(|(index, owner)| {
                        described.get(index).is_some_and(|instance| {
                            instance.node == *owner && instance.status == "running"
                        })
                    })
            }
            Self::Idle { instance } => {
                let (Some(owner), Some(described)) =
                    (scheduled.get(instance), described.get(instance))
                else {
                    return false;
                };
                described.node == *owner
                    && described.status == "idle"
                    && described.partitions == "none"
            }
            Self::Serves {
                instance,
                partitions,
            } => described
                .get(instance)
                .is_some_and(|described| described.partitions == *partitions),
        }
    }
}

async fn wait_for_emitter_instances(
    world: &mut ScenarioWorld,
    duration: String,
    emitter: String,
    expectation: EmitterInstanceExpectation,
) {
    let duration = parse_duration_text(&duration).assured("the scenario writes a valid timeout");
    let emitter = expand_placeholders(world, &emitter);
    let deadline = PhaseDeadline::after(duration);
    // A failover scenario asks after one node stopped, so the leader is the one the running nodes
    // agree on.
    let node_id = world
        .cluster()
        .wait_for_leader_among_running()
        .await
        .expect("the running nodes agree on a leader");
    let mut last_description = None;
    let mut last_error = None;
    loop {
        nervix_primitives::task::consume_budget().await;
        assert!(
            !deadline.has_passed(),
            "the emitter instances must report the expected owner, status and partitions; last \
             output={last_description:?}, last error={last_error:?}"
        );
        if let Ok(status) = world.cluster().status_text(&node_id, deadline).await {
            let scheduled = UnitPlacements::from_status(
                &status,
                &world.domain,
                "emitter",
                &emitter,
                "instance:",
            );
            match run_plexus_commands_on_node(
                world,
                &node_id,
                &format!("DESCRIBE EMITTER {emitter};"),
            )
            .await
            {
                Ok(output) => {
                    let described = described_emitter_instances(&output);
                    let holds = expectation.holds(&scheduled.owners, &described);
                    last_description = Some(output);
                    if holds {
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
    expr = "within {string} every instance of emitter {string} reports running on its scheduled \
            owner"
)]
async fn wait_for_each_emitter_instance_running_on_its_owner(
    world: &mut ScenarioWorld,
    duration: String,
    emitter: String,
) {
    wait_for_emitter_instances(
        world,
        duration,
        emitter,
        EmitterInstanceExpectation::EveryInstanceRunning,
    )
    .await;
}

#[then(
    expr = "within {string} instance {int} of emitter {string} reports idle on its scheduled owner"
)]
async fn wait_for_emitter_instance_idle_on_its_owner(
    world: &mut ScenarioWorld,
    duration: String,
    instance: u64,
    emitter: String,
) {
    wait_for_emitter_instances(
        world,
        duration,
        emitter,
        EmitterInstanceExpectation::Idle { instance },
    )
    .await;
}

#[then(expr = "within {string} instance {int} of emitter {string} serves partitions {string}")]
async fn wait_for_emitter_instance_partitions(
    world: &mut ScenarioWorld,
    duration: String,
    instance: u64,
    emitter: String,
    partitions: String,
) {
    wait_for_emitter_instances(
        world,
        duration,
        emitter,
        EmitterInstanceExpectation::Serves {
            instance,
            partitions,
        },
    )
    .await;
}

#[then(
    expr = "every scheduled instance of emitter {string} exposes its instance label in Prometheus"
)]
async fn every_emitter_instance_exposes_prometheus_identity(
    world: &mut ScenarioWorld,
    emitter: String,
) {
    let emitter = expand_placeholders(world, &emitter);
    let node_id = current_leader_node(world).await;
    let deadline = PhaseDeadline::after(Duration::from_secs(60));
    let status = world
        .cluster()
        .status_text(&node_id, deadline)
        .await
        .assured("the ready cluster serves its current unit schedule");
    let placements =
        UnitPlacements::from_status(&status, &world.domain, "emitter", &emitter, "instance:");
    assert!(
        !placements.owners.is_empty(),
        "the emitter has scheduled instances: {status}"
    );
    for (instance, owner) in placements.owners {
        nervix_primitives::task::consume_budget().await;
        let labels = vec![
            format!("domain=\"{}\"", world.domain),
            format!("emitter=\"{emitter}\""),
            format!("instance=\"{instance}\""),
            format!("physical_node_id=\"{owner}\""),
        ];
        world
            .cluster()
            .wait_for_observability_metric_at_least(
                &owner,
                "nervix_emitter_transient_failure",
                &labels,
                0,
                Some(Duration::from_secs(60)),
            )
            .await
            .assured("each owner publishes its exact instance's emitter metric series");
    }
}
