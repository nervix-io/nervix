//! Materialized relay unit state task tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests of one materialized relay unit's state task in a partition
//!   transition: what it parks and what a retired transition does with it.
//! - **Depends on.** The relay state task, its partition moves and the runtime test fixtures.
//! - **Must not know.** Production control-plane orchestration, how the leader retires a
//!   transition, or how a claim crosses nodes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use nervix_interconnect::RuntimeState;
use nervix_models::{ModelKind, ModelName, NodeRef, ParseAsType, SchemaFingerprint, Timestamp};
use nervix_primitives::time::{sleep, timeout};

use super::*;
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{RuntimeValue, test_runtime_row},
};

/// A materialized relay unit whose branch's keys move between units parks a batch of a key whose
/// row may lie in another unit. When the transition retires, every row lies in its partition, so
/// the unit applies the batch where it waits and withdraws its published holdings.
#[nervix_primitives::test]
async fn a_retired_transition_applies_the_batch_its_materialized_unit_parked() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let relay = named::<RelayName>("profiles");
    let unit = Unit::partition(5);
    let partitions = |count: u16| {
        nervix_models::BranchPartitions::Direct(
            std::num::NonZeroU16::new(count).expect("the test counts are positive"),
        )
    };
    // Unit 5 of a branch growing from four partitions to eight: four placed `beta` in unit 1.
    let transition = UnitPartitionTransition {
        branch: named("by_tenant"),
        epoch: nervix_models::PartitionEpochNumber::FIRST.next(),
        current: RelayPartitioning::of_partitions(partitions(8)),
        previous: RelayPartitioning::of_partitions(partitions(4)),
        unit,
    };
    let schema = test_schema(&[("value", ParseAsType::I64)]);
    let mut assignment = runtime.replicated_materialized_stream_state(
        RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeState::MaterializedRelay {
                schema: SchemaFingerprint::from_digest([7; 32]),
            },
            kind: ModelKind::Relay,
            identifier: ModelName::from(&relay),
            unit,
            branch_key: None,
        },
        schema.arrow_schema(),
        None,
        Vec::new(),
        None,
    );
    let state = assignment
        .originator
        .take()
        .expect("branch-local state should grant authoritative access");
    let read = state.read().clone();
    let broadcast = RelayBroadcast::with_capacity(nonzero_capacity(2));
    let task = runtime.spawn_relay_state_task(
        &domain,
        RelayStateTaskSpec {
            relay: relay.clone(),
            unit,
            partitioning: RelayPartitioning::of_partitions(partitions(8)),
            state,
            retention: RelayRetention::default(),
            receiver: RelayRuntimeFanIn::new(broadcast.new_receiver()),
            transition: Some(transition.clone()),
            parked_capacity: 2,
        },
    );
    let held =
        || runtime.partition_transition_holdings(&domain, &transition.branch, transition.epoch);
    timeout(Duration::from_secs(30), async {
        while held().is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the unit publishes its holdings once it starts in the transition");

    let beta = string_branch_key("tenant", "beta");
    broadcast
        .broadcast(
            RelayRecordBatch::single(
                schema.clone(),
                beta.clone(),
                test_runtime_row([("value".to_string(), RuntimeValue::I64(1))])
                    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(1)),
                AckSet::empty(),
            )
            .expect("materialized batch should build"),
        )
        .await
        .expect("materialized batch should queue");
    // No unit of the previous count answers in this runtime, so the batch waits for the catalog
    // of the rows they hold, holding its work.
    let counters = runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Relay, &relay));
    timeout(Duration::from_secs(30), async {
        while counters.outstanding_work() == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the unit takes the batch");
    assert!(
        read.record(&beta).is_none(),
        "a batch whose key may lie in another unit waits for its row"
    );

    task.commands
        .send(RelayStateCommand::TransitionRetired)
        .await
        .expect("the unit takes its retirement");
    timeout(Duration::from_secs(30), async {
        while read.record(&beta).is_none() || counters.outstanding_work() != 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a retired unit applies the batch it parked");
    assert!(
        held().is_empty(),
        "a retired unit withdraws the holdings it published"
    );
    task.stop(Duration::from_secs(1))
        .await
        .expect("relay state task should stop");
}
