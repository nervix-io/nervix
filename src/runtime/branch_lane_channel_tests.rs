//! Delivery channels a branch lane ends together with its concrete branch.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests for the relay channels a processor or entrypoint branch lane retires
//!   when its concrete branch expires or is evicted, and the channels it leaves to their owners.
//! - **Depends on.** The production branch expiry and eviction paths and the runtime's test
//!   fixtures.
//! - **Must not know.** Production control-plane orchestration or external connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use nervix_models::{IngestorName, RelayName, Timestamp};
use nervix_primitives::sync::{Arc, StdArc, mpsc};
use nonzero_ext::nonzero;

use super::*;
use crate::runtime::{
    branch_runtime::{
        evict_branch_instance_instances_to_capacity, expire_branch_instance_instances,
    },
    processor_branch_task::{
        evict_processor_branch_instances_to_capacity, expire_processor_branch_instances,
    },
};

/// The time a branch's last input was accepted at.
fn last_input() -> Timestamp {
    Timestamp::from_unix_nanos(1_000_000_000)
}

/// A time an hour past [`last_input`], well beyond every TTL these tests declare.
fn long_after_last_input() -> Timestamp {
    Timestamp::from_unix_nanos(3_601_000_000_000)
}

/// A domain whose branch lane publishes through one relay, beside another relay with a branch for
/// the same key that the lane does not publish through.
struct LaneRelays {
    runtime: Runtime,
    domain: DomainName,
    template: BranchInstanceTemplate,
    sibling_services: Arc<RelayBoundaryServices>,
}

impl LaneRelays {
    fn new() -> Self {
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let published = named::<RelayName>("routed_orders");
        let published_services = test_relay_boundary_services();
        let sibling_services = test_relay_boundary_services();
        publish_test_relay_routing(
            &runtime,
            &domain,
            [
                (published.clone(), published_services.clone()),
                (named("unique_orders"), sibling_services.clone()),
            ],
        );
        let mut template = junction_branch_template("route_orders", "orders");
        template.relays.insert(
            published,
            RelayProcessorRelayTemplate {
                services: published_services.clone(),
            },
        );
        Self {
            runtime,
            domain,
            template,
            sibling_services,
        }
    }

    /// The ingress channels of `key` on the relay the lane publishes through and on its sibling.
    async fn ingress_slots(
        &self,
        key: &Option<BranchKey>,
        instance: &BranchInstanceRuntime,
    ) -> BranchKeyIngressSlots {
        BranchKeyIngressSlots {
            published: instance
                .lock()
                .await
                .relays
                .get(&named::<RelayName>("routed_orders"))
                .expect("the fixture binds its output relay")
                .retained_ingress()
                .expect("the producer retains its ingress"),
            sibling: self.sibling_services.ingress_slot(key, Unit::SINGLE),
        }
    }

    async fn entrypoint_branch(&self, key: &Option<BranchKey>) -> BranchInstanceRuntime {
        self.template
            .instantiate(
                &self.runtime,
                &self.domain,
                key.clone(),
                1,
                &RetainedStateOpening::RefusedWhenFull,
            )
            .await
            .expect("the junction template instantiates on the test domain")
    }
}

struct BranchKeyIngressSlots {
    published: StdArc<RelayOutboundSlot>,
    sibling: StdArc<RelayOutboundSlot>,
}

impl BranchKeyIngressSlots {
    fn assert_only_the_published_channel_ended(&self) {
        assert!(
            self.published.cancellation().is_cancelled(),
            "the channel the branch lane publishes through ends with its branch"
        );
        assert!(
            !self.sibling.cancellation().is_cancelled(),
            "a relay the branch lane does not publish through keeps its channel for the key"
        );
    }
}

/// Keeps the processor's instantiated producer until the production stop path ends its task.
fn processor_branch_task(instance: BranchInstanceRuntime) -> ProcessorBranchTask {
    let (input, _) = mpsc::channel(1);
    let (commands, mut stop) = mpsc::channel(1);
    let delivery_lifetime = instance.delivery_lifetime.clone();
    let task = nervix_primitives::task::spawn(async move {
        drop(stop.recv().await);
        drop(instance);
    });
    ProcessorBranchTask {
        input,
        commands,
        delivery_lifetime,
        task: nervix_primitives::sync::blocking::Mutex::new(Some(AbortOnDropHandle::new(task))),
    }
}

#[nervix_primitives::test]
async fn an_expiring_processor_branch_ends_only_the_channels_it_publishes_through() {
    let lane = LaneRelays::new();
    let key = string_branch_key("tenant", "acme");
    let branch = lane.entrypoint_branch(&key).await;
    let slots = lane.ingress_slots(&key, &branch).await;
    let mut instances = ProcessorBranches::new();
    instances.insert_restored(
        key.clone(),
        last_input(),
        1,
        ProcessorBranchSlot::Live(processor_branch_task(branch)),
    );

    expire_processor_branch_instances(
        &lane.runtime,
        &lane.domain,
        &lane.template,
        long_after_last_input(),
        Duration::from_secs(1),
        &mut instances,
    )
    .await;

    assert!(!instances.contains_key(&key), "the idle branch expires");
    slots.assert_only_the_published_channel_ended();
}

#[nervix_primitives::test]
async fn an_evicted_processor_branch_ends_only_the_channels_it_publishes_through() {
    let lane = LaneRelays::new();
    let evicted = string_branch_key("tenant", "acme");
    let retained = string_branch_key("tenant", "globex");
    let evicted_branch = lane.entrypoint_branch(&evicted).await;
    let retained_branch = lane.entrypoint_branch(&retained).await;
    let slots = lane.ingress_slots(&evicted, &evicted_branch).await;
    let mut instances = ProcessorBranches::new();
    instances.insert_restored(
        evicted.clone(),
        last_input(),
        1,
        ProcessorBranchSlot::Live(processor_branch_task(evicted_branch)),
    );
    instances.insert_restored(
        retained.clone(),
        long_after_last_input(),
        2,
        ProcessorBranchSlot::Live(processor_branch_task(retained_branch)),
    );

    evict_processor_branch_instances_to_capacity(
        &lane.runtime,
        &lane.domain,
        &lane.template,
        nonzero!(1usize),
        &mut instances,
    )
    .await;

    assert!(
        !instances.contains_key(&evicted),
        "the older branch is evicted"
    );
    assert!(instances.contains_key(&retained), "the newer branch stays");
    slots.assert_only_the_published_channel_ended();
}

#[nervix_primitives::test]
async fn an_expiring_entrypoint_branch_ends_only_the_channels_it_publishes_through() {
    let lane = LaneRelays::new();
    let key = string_branch_key("tenant", "acme");
    let branch = lane.entrypoint_branch(&key).await;
    let slots = lane.ingress_slots(&key, &branch).await;
    let mut instances = BranchInstanceRegistry::new();
    instances.insert_restored(key.clone(), last_input(), 1, branch);

    let expired = expire_branch_instance_instances(
        &lane.runtime,
        &lane.domain,
        &named::<IngestorName>("orders_source"),
        &lane.template,
        long_after_last_input(),
        Duration::from_secs(1),
        &mut instances,
    )
    .await;

    assert_eq!(expired, vec![key]);
    slots.assert_only_the_published_channel_ended();
}

#[nervix_primitives::test]
async fn an_expiring_entrypoint_preserves_a_second_producers_relay_channel() {
    let lane = LaneRelays::new();
    let key = string_branch_key("tenant", "acme");
    let idle = lane.entrypoint_branch(&key).await;
    let active = lane.entrypoint_branch(&key).await;
    let slot = lane.ingress_slots(&key, &idle).await.published;
    let first_delivery = slot.next_delivery();
    let mut instances = BranchInstanceRegistry::new();
    instances.insert_restored(key.clone(), last_input(), 1, idle);

    let expired = expire_branch_instance_instances(
        &lane.runtime,
        &lane.domain,
        &named::<IngestorName>("idle_source"),
        &lane.template,
        long_after_last_input(),
        Duration::from_secs(1),
        &mut instances,
    )
    .await;

    assert_eq!(expired, vec![key.clone()]);
    assert!(
        !slot.cancellation().is_cancelled(),
        "another producer still holds this branch channel"
    );
    let retained = lane.ingress_slots(&key, &active).await.published;
    assert!(StdArc::ptr_eq(&slot, &retained));
    let next_delivery = retained.next_delivery();
    assert_eq!(
        next_delivery.channel_incarnation,
        first_delivery.channel_incarnation
    );
    assert_eq!(next_delivery.sequence, first_delivery.sequence + 1);

    active.into_inner().evict().await;
    assert!(
        slot.cancellation().is_cancelled(),
        "the shared channel ends when its last producer ends"
    );
    let rebound = lane.entrypoint_branch(&key).await;
    let reopened = lane
        .ingress_slots(&key, &rebound)
        .await
        .published
        .next_delivery();
    assert!(reopened.channel_incarnation > next_delivery.channel_incarnation);
    assert_eq!(reopened.sequence, 0);
    drop(rebound);
}

#[nervix_primitives::test]
async fn an_evicted_entrypoint_branch_ends_only_the_channels_it_publishes_through() {
    let lane = LaneRelays::new();
    let evicted = string_branch_key("tenant", "acme");
    let retained = string_branch_key("tenant", "globex");
    let evicted_branch = lane.entrypoint_branch(&evicted).await;
    let retained_branch = lane.entrypoint_branch(&retained).await;
    let slots = lane.ingress_slots(&evicted, &evicted_branch).await;
    let mut instances = BranchInstanceRegistry::new();
    instances.insert_restored(evicted.clone(), last_input(), 1, evicted_branch);
    instances.insert_restored(
        retained.clone(),
        long_after_last_input(),
        2,
        retained_branch,
    );

    let evicted_keys = evict_branch_instance_instances_to_capacity(
        &lane.runtime,
        &lane.domain,
        &named::<IngestorName>("orders_source"),
        &lane.template,
        nonzero!(1usize),
        &mut instances,
    )
    .await;

    assert_eq!(evicted_keys, vec![evicted]);
    slots.assert_only_the_published_channel_ended();
}

/// One fresh diagnostic process installs its detector before any tracked lock exists, then ends
/// processor and entrypoint branches through their expiry and eviction paths, which stop each
/// processor branch task under its tracked task guard.
#[cfg(feature = "deloxide")]
#[test]
fn deloxide_branch_lane_channel_retirement() {
    use meticulous::ResultExt as _;

    let directory =
        std::env::var_os("NERVIX_DEADLOCK_EVIDENCE").map(nervix_deadlock::EvidenceDirectory::new);
    nervix_deadlock::DiagnosticRun::start(directory)
        .assured("the branch lane diagnostic process starts its detector once");
    an_expiring_processor_branch_ends_only_the_channels_it_publishes_through();
    an_evicted_processor_branch_ends_only_the_channels_it_publishes_through();
    an_expiring_entrypoint_branch_ends_only_the_channels_it_publishes_through();
    an_expiring_entrypoint_preserves_a_second_producers_relay_channel();
    an_evicted_entrypoint_branch_ends_only_the_channels_it_publishes_through();
}
