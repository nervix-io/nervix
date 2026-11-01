//! Public relay partition checks.
//!
//! Outside the layer order: a scenario harness.
//!
//! - **Owns.** Reading the partition units of a relay from `SHOW CLUSTER STATUS`, where each
//!   partition is scheduled with its own owner, the partition and owner `DESCRIBE RELAY … WHERE`
//!   reports for a key, order checks that hold within each key while partitions interleave, and the
//!   scenario control of one partition owner's fan-out.
//! - **Depends on.** The public client, the in-process cluster's status reads, the observed broker
//!   and the testing fault injection.
//! - **Must not know.** How the scheduler places a partition or how the runtime routes a key.

use std::collections::VecDeque;

use meticulous::OptionExt as _;

use super::*;

/// The placement of one partition unit of a relay, as one `SHOW CLUSTER STATUS` schedule line
/// reports it: `- domain=<d> kind=relay name=<n> unit=partition:<p> owner=<o> replicas=<r>`.
struct PartitionPlacement {
    partition: u32,
    owner: String,
}

/// Every partition unit `status` schedules for `relay` in `domain`, in the order the lines appear.
fn relay_partition_placements(status: &str, domain: &str, relay: &str) -> Vec<PartitionPlacement> {
    let mut placements = Vec::new();
    for line in status.lines() {
        let Some(line) = line.trim().strip_prefix("- ") else {
            continue;
        };
        let mut line_domain = None;
        let mut line_kind = None;
        let mut line_name = None;
        let mut partition = None;
        let mut owner = None;
        for field in line.split_whitespace() {
            if let Some(value) = field.strip_prefix("domain=") {
                line_domain = Some(value);
            } else if let Some(value) = field.strip_prefix("kind=") {
                line_kind = Some(value);
            } else if let Some(value) = field.strip_prefix("name=") {
                line_name = Some(value);
            } else if let Some(value) = field.strip_prefix("unit=partition:") {
                partition = Some(
                    value
                        .parse::<u32>()
                        .unwrap_or_else(|error| panic!("unit {value:?} is not a partition: {error}")),
                );
            } else if let Some(value) = field.strip_prefix("owner=") {
                owner = Some(value);
            }
        }
        if line_domain != Some(domain) || line_kind != Some("relay") || line_name != Some(relay) {
            continue;
        }
        let (Some(partition), Some(owner)) = (partition, owner) else {
            continue;
        };
        placements.push(PartitionPlacement {
            partition,
            owner: owner.to_string(),
        });
    }
    placements
}

/// The partition units of `relay` the last command output, a `SHOW CLUSTER STATUS`, schedules.
fn last_status_partitions(world: &ScenarioWorld, relay: &str) -> Vec<PartitionPlacement> {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a cluster status output must exist before reading relay partitions");
    relay_partition_placements(output, &world.domain, relay)
}

/// The placeholder a partition owner is saved under.
fn partition_owner_placeholder(prefix: &str, partition: u32) -> String {
    format!("{prefix}_{partition}")
}

/// What a `DESCRIBE RELAY … WHERE` output reports about one key: whether its branch exists on the
/// owner, and the partition the key lands in.
struct DescribedKey {
    exists: bool,
    partition: u32,
}

fn described_key(output: &str) -> Option<DescribedKey> {
    let mut exists = None;
    let mut partition = None;
    for line in output.lines().map(str::trim) {
        if line == "exists" {
            exists = Some(true);
        } else if line == "not exists" {
            exists = Some(false);
        } else if let Some(value) = line.strip_prefix("partition: ") {
            partition = value.parse::<u32>().ok();
        }
    }
    Some(DescribedKey {
        exists: exists?,
        partition: partition?,
    })
}

/// One expected payload: the fragment naming its key, then every fragment it must contain.
struct KeyedFragments {
    key: String,
    fragments: Vec<String>,
}

/// The docstring's `|`-separated lines grouped by their first fragment, the key, keeping the order
/// the lines list each key's payloads in.
fn ordered_fragments_by_key(
    world: &ScenarioWorld,
    step: &Step,
) -> BTreeMap<String, VecDeque<KeyedFragments>> {
    let mut by_key = BTreeMap::<String, VecDeque<KeyedFragments>>::new();
    for fragments in docstring_fragment_sets(world, step) {
        let key = fragments
            .first()
            .unwrap_or_else(|| panic!("every expected payload line starts with its key fragment"))
            .clone();
        by_key
            .entry(key.clone())
            .or_default()
            .push_back(KeyedFragments { key, fragments });
    }
    assert!(
        !by_key.is_empty(),
        "step docstring must list at least one expected payload"
    );
    by_key
}

/// Matches one received payload against the next expected payload of its key, failing when it
/// belongs to no expected key or arrives out of its key's order.
fn accept_keyed_payload(
    remaining: &mut BTreeMap<String, VecDeque<KeyedFragments>>,
    payload: &str,
    observed: &[String],
) {
    let keys = remaining
        .keys()
        .filter(|key| payload.contains(key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let [key] = keys.as_slice() else {
        panic!(
            "payload {payload:?} must belong to exactly one remaining expected key, matched \
             {keys:?}; observed {observed:?}"
        );
    };
    let expected = remaining
        .get_mut(key)
        .assured("the key was read from the remaining map above");
    let next = expected
        .pop_front()
        .assured("a key leaves the remaining map once its last payload matched");
    assert!(
        next.fragments
            .iter()
            .all(|fragment| payload.contains(fragment.as_str())),
        "the next payload of key {:?} must contain {:?}, got {payload:?}; observed {observed:?}",
        next.key,
        next.fragments
    );
    if expected.is_empty() {
        remaining.remove(key);
    }
}

#[then(expr = "the last cluster status lists {int} partitions of relay {string}")]
async fn then_last_cluster_status_lists_relay_partitions(
    world: &mut ScenarioWorld,
    expected: u32,
    relay: String,
) {
    let relay = expand_placeholders(world, &relay);
    let placements = last_status_partitions(world, &relay);
    let partitions = placements
        .iter()
        .map(|placement| placement.partition)
        .collect::<BTreeSet<_>>();
    let expected_partitions = (0..expected).collect::<BTreeSet<_>>();
    assert_eq!(
        partitions,
        expected_partitions,
        "relay '{relay}' must be scheduled as partitions {expected_partitions:?}; got {partitions:?} \
         in: {:?}",
        world.last_command_output
    );
    assert_eq!(
        placements.len(),
        partitions.len(),
        "every partition of relay '{relay}' must be scheduled once; got {:?}",
        world.last_command_output
    );
}

#[then(
    expr = "the last cluster status places the partitions of relay {string} on at least {int} \
            distinct owners"
)]
async fn then_last_cluster_status_spreads_relay_partitions(
    world: &mut ScenarioWorld,
    relay: String,
    expected_owners: usize,
) {
    let relay = expand_placeholders(world, &relay);
    let placements = last_status_partitions(world, &relay);
    assert!(
        !placements.is_empty(),
        "relay '{relay}' must be scheduled as partitions; got {:?}",
        world.last_command_output
    );
    let owners = placements
        .iter()
        .map(|placement| placement.owner.as_str())
        .filter(|owner| *owner != "-")
        .collect::<BTreeSet<_>>();
    assert!(
        owners.len() >= expected_owners,
        "the partitions of relay '{relay}' must have at least {expected_owners} distinct owners; \
         got {owners:?} in: {:?}",
        world.last_command_output
    );
}

#[when(
    expr = "the owner of every partition of relay {string} in the last cluster status is saved \
            with placeholder prefix {string}"
)]
#[then(
    expr = "the owner of every partition of relay {string} in the last cluster status is saved \
            with placeholder prefix {string}"
)]
async fn the_owner_of_every_relay_partition_is_saved(
    world: &mut ScenarioWorld,
    relay: String,
    prefix: String,
) {
    let relay = expand_placeholders(world, &relay);
    let placements = last_status_partitions(world, &relay);
    assert!(
        !placements.is_empty(),
        "relay '{relay}' must be scheduled as partitions; got {:?}",
        world.last_command_output
    );
    for placement in placements {
        assert_ne!(
            placement.owner, "-",
            "partition {} of relay '{relay}' must have an owner; got {:?}",
            placement.partition, world.last_command_output
        );
        world.placeholders.insert(
            partition_owner_placeholder(&prefix, placement.partition),
            placement.owner,
        );
    }
}

#[then(
    expr = "a node other than placeholder {string} that owns a partition of relay {string} in the \
            last cluster status is saved as placeholder {string}"
)]
async fn then_a_partition_owner_other_than_placeholder_is_saved(
    world: &mut ScenarioWorld,
    excluded: String,
    relay: String,
    placeholder: String,
) {
    let excluded = world
        .placeholders
        .get(&excluded)
        .unwrap_or_else(|| panic!("placeholder '{excluded}' must be saved before selection"))
        .clone();
    let relay = expand_placeholders(world, &relay);
    let owners = last_status_partitions(world, &relay)
        .into_iter()
        .map(|placement| placement.owner)
        .filter(|owner| *owner != "-" && *owner != excluded)
        .collect::<BTreeSet<_>>();
    let owner = owners.into_iter().next().unwrap_or_else(|| {
        panic!(
            "a node other than '{excluded}' must own a partition of relay '{relay}'; got {:?}",
            world.last_command_output
        )
    });
    world.placeholders.insert(placeholder, owner);
}

#[then(
    expr = "within {string} node {string} eventually reports no partition of relay {string} owned \
            by node {string}"
)]
async fn then_within_duration_no_relay_partition_is_owned_by_node(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    relay: String,
    former_owner: String,
) {
    let duration = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let relay = expand_placeholders(world, &relay);
    let former_owner = expand_placeholders(world, &former_owner);
    let placement = PhaseDeadline::after(duration);
    loop {
        nervix_primitives::task::consume_budget().await;
        assert!(
            !placement.has_passed(),
            "timed out waiting for every partition of relay '{relay}' to leave node \
             '{former_owner}'. last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        match world.cluster().status_text(&node_id, placement).await {
            Ok(output) => {
                let placements = relay_partition_placements(&output, &world.domain, &relay);
                let moved = !placements.is_empty()
                    && placements
                        .iter()
                        .all(|placement| placement.owner != "-" && placement.owner != former_owner);
                world.last_command_output = Some(output);
                if moved {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(format!("{error:#}")),
        }
        placement.pause(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports relay {string} keys absent in the \
            partitions node {string} owned under placeholder prefix {string} and present in every \
            other partition"
)]
async fn then_within_duration_failed_partition_keys_are_absent(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    relay: String,
    former_owner: String,
    prefix: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let relay = expand_placeholders(world, &relay);
    let former_owner = expand_placeholders(world, &former_owner);
    let filters = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| expand_placeholders(world, line))
        .collect::<Vec<_>>();
    assert!(
        !filters.is_empty(),
        "step docstring must list at least one WHERE filter"
    );
    let deadline = PhaseDeadline::after(duration);
    loop {
        nervix_primitives::task::consume_budget().await;
        assert!(
            !deadline.has_passed(),
            "timed out waiting for relay '{relay}' to report only the keys of the partitions \
             '{former_owner}' owned as absent. last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        let mut absent_expected = 0usize;
        let mut present_expected = 0usize;
        let mut settled = true;
        for filter in &filters {
            nervix_primitives::task::consume_budget().await;
            let command = format!("DESCRIBE RELAY {relay} WHERE {filter};");
            let output = match run_plexus_commands_on_node(world, &node_id, &command).await {
                Ok(output) => output,
                Err(error) => {
                    world.last_command_error = Some(error);
                    settled = false;
                    break;
                }
            };
            let Some(described) = described_key(&output) else {
                world.last_command_output = Some(output);
                settled = false;
                break;
            };
            let placeholder = partition_owner_placeholder(&prefix, described.partition);
            let owner_before = world
                .placeholders
                .get(&placeholder)
                .unwrap_or_else(|| {
                    panic!("placeholder '{placeholder}' must be saved before this assertion")
                })
                .clone();
            let expected_exists = owner_before != former_owner;
            if expected_exists {
                present_expected += 1;
            } else {
                absent_expected += 1;
            }
            world.last_command_output = Some(output);
            if described.exists != expected_exists {
                settled = false;
                break;
            }
        }
        if settled {
            assert!(
                absent_expected > 0 && present_expected > 0,
                "the filters must reach both a partition '{former_owner}' owned and one it did \
                 not; {absent_expected} were in its partitions and {present_expected} elsewhere"
            );
            return;
        }
        deadline.pause(Duration::from_millis(100)).await;
    }
}

#[then(expr = "within {string} the relay subscription receives payloads keeping the order of each key")]
async fn then_within_duration_the_subscription_keeps_each_key_in_order(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let mut remaining = ordered_fragments_by_key(world, step);
    let deadline = Instant::now() + duration;
    let mut observed = Vec::new();
    while !remaining.is_empty() {
        nervix_primitives::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for subscription payloads; still expected {:?}, observed {observed:?}",
            remaining.keys().collect::<Vec<_>>()
        );
        let wait = deadline.saturating_duration_since(now);
        let session = world
            .active_session
            .as_mut()
            .expect("an active session with subscription must exist");
        let Some(event) = session
            .try_next_subscription(wait)
            .await
            .expect("failed while waiting for subscription payloads")
        else {
            continue;
        };
        let payload = event.payload;
        accept_keyed_payload(&mut remaining, &payload, &observed);
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload);
    }
}

#[then(expr = "within {string} the observed broker receives payloads keeping the order of each key")]
async fn then_within_duration_the_broker_keeps_each_key_in_order(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let mut remaining = ordered_fragments_by_key(world, step);
    let deadline = Instant::now() + duration;
    let mut observed = Vec::new();
    while !remaining.is_empty() {
        nervix_primitives::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for broker payloads; still expected {:?}, observed {observed:?}",
            remaining.keys().collect::<Vec<_>>()
        );
        let wait = deadline.saturating_duration_since(now);
        let Some(payload) = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_payload(wait)
            .await
            .expect("failed while waiting for broker payloads")
        else {
            continue;
        };
        accept_keyed_payload(&mut remaining, &payload, &observed);
        observed.push(payload);
    }
}

#[given(
    expr = "relay owner fan-out of partition {int} of relay {string} in domain {string} is paused \
            before dispatch"
)]
#[when(
    expr = "relay owner fan-out of partition {int} of relay {string} in domain {string} is paused \
            before dispatch"
)]
async fn relay_partition_owner_fanout_is_paused(
    world: &mut ScenarioWorld,
    partition: u32,
    relay: String,
    domain: String,
) {
    let relay = expand_placeholders(world, &relay);
    let domain = expand_placeholders(world, &domain);
    world
        .fault_injection
        .pause_owner_relay_partition_fanout(&domain, &relay, partition);
}

#[then(
    expr = "the relay owner fan-out pause of partition {int} of relay {string} in domain {string} \
            is reached"
)]
async fn then_relay_partition_owner_fanout_pause_is_reached(
    world: &mut ScenarioWorld,
    partition: u32,
    relay: String,
    domain: String,
) {
    let relay = expand_placeholders(world, &relay);
    let domain = expand_placeholders(world, &domain);
    nervix_primitives::time::timeout(
        Duration::from_secs(30),
        world
            .fault_injection
            .wait_for_owner_relay_partition_fanout_pause(&domain, &relay, partition),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "the fan-out pause of partition {partition} of relay '{relay}' in domain '{domain}' \
             was not reached: {error}"
        )
    });
}

#[when(
    expr = "the relay owner fan-out pause of partition {int} of relay {string} in domain {string} \
            is released"
)]
async fn when_relay_partition_owner_fanout_pause_is_released(
    world: &mut ScenarioWorld,
    partition: u32,
    relay: String,
    domain: String,
) {
    let relay = expand_placeholders(world, &relay);
    let domain = expand_placeholders(world, &domain);
    world
        .fault_injection
        .release_owner_relay_partition_fanout_pause(&domain, &relay, partition);
}
