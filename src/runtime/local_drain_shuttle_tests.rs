//! A drain observation of work that moves while it reads, explored under Shuttle.
//!
//! Layer: test harness.
//!
//! - **Owns.** The invariant that one drain observation never reports a domain empty while work
//!   it held when the observation began is still moving through it.
//! - **Depends on.** The production observation order, relay fan-out and transit, node quiesce
//!   counters, and the model harness's Shuttle runner.
//! - **Must not know.** What the work is, which relay or node holds it, or why the node drains.
//!
//! The counts are Shuttle's atomics, so every read and adjustment is a scheduling point, and a
//! check reaches a move between any two reads of the observation.

use meticulous::ResultExt as _;
use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_primitives::sync::Arc;

use super::{
    LocalDomainDrainStatus, NodeQuiesceWorkGuard, OneRelayAndNode, OutputBufferQuiesceGauge, domain,
};

/// A node publishes a batch from its output buffer into a relay while a drain observes the domain.
/// The relay counts the batch before the node lets it go, which a drain that reads relays before
/// nodes can still miss in between, so the observation has to show the batch or the relay's
/// admission.
#[test]
fn shuttle_a_batch_published_while_a_drain_observes_is_never_missing_from_it() {
    check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let sources = Arc::new(OneRelayAndNode::new());
            let mut output = OutputBufferQuiesceGauge::new(sources.counters.clone());
            output.add_batch();
            let publishing_sources = sources.clone();
            let publishing = nervix_primitives::task::spawn(async move {
                publishing_sources.fanout.begin_owner_admission().accept();
                output.remove_batches(1);
            });
            let status = LocalDomainDrainStatus::observe(domain("default"), &*sources);
            publishing
                .await
                .assured("the publishing side only moves one batch into the relay");
            assert!(
                status.holds_admitted_work(),
                "a drain observation reported no work while a batch moved from a node into a relay"
            );
        });
    });
}

/// A consumer takes a batch from a relay into its node while a drain observes the domain. The node
/// counts the batch before the relay lets it go, so the observation has to show it in one or the
/// other.
#[test]
fn shuttle_a_batch_a_consumer_takes_while_a_drain_observes_is_never_missing_from_it() {
    check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let sources = Arc::new(OneRelayAndNode::new());
            let mut consumer = sources.attach_consumer();
            sources.deliver_to_consumers();
            let counters = sources.counters.clone();
            let taking = nervix_primitives::task::spawn(async move {
                let taken = consumer.try_recv_with_quiesce(Some(&counters));
                (consumer, taken)
            });
            let status = LocalDomainDrainStatus::observe(domain("default"), &*sources);
            let taken = taking
                .await
                .assured("the consuming side only takes one batch");
            assert!(
                status.holds_admitted_work(),
                "a drain observation reported no work while a consumer took a batch from a relay"
            );
            drop(taken);
        });
    });
}

/// A relay's owner hands a batch it admitted on to the relay's consumer while a drain observes the
/// domain. The consumer counts the batch before the owner hands it on, so the observation has to
/// show it in the relay's transit or in the consumer's queue.
#[test]
fn shuttle_a_batch_its_owner_hands_on_while_a_drain_observes_is_never_missing_from_it() {
    check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let sources = Arc::new(OneRelayAndNode::new());
            let consumer = sources.attach_consumer();
            sources.fanout.begin_owner_admission().accept();
            let owner_sources = sources.clone();
            let handing_on = nervix_primitives::task::spawn(async move {
                let completion = owner_sources.fanout.begin_owner_batch_completion();
                owner_sources.deliver_to_consumers();
                drop(completion);
            });
            let status = LocalDomainDrainStatus::observe(domain("default"), &*sources);
            handing_on
                .await
                .assured("the owner side only hands one batch on");
            assert!(
                status.holds_admitted_work(),
                "a drain observation reported no work while a relay handed a batch on to its \
                 consumer"
            );
            drop(consumer);
        });
    });
}

/// A force flush resumes a message parked on `REQUIRED WAIT` and then completes its obligation
/// while a drain observes the domain. A parked message does not hold the drain, so once the drain
/// sees the obligation complete it has to see the resumed message as admitted work.
#[test]
fn shuttle_a_message_a_flush_resumed_is_seen_by_a_drain_that_sees_the_flush_complete() {
    check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let sources = Arc::new(OneRelayAndNode::new());
            sources.counters.begin_force_flush_obligation();
            let mut message = NodeQuiesceWorkGuard::begin(sources.counters.clone());
            message.park_for_required_materialized_state();
            let flushing_sources = sources.clone();
            let flushing = nervix_primitives::task::spawn(async move {
                message.resume_from_required_materialized_state();
                flushing_sources.counters.complete_force_flush_obligation();
                message
            });
            let status = LocalDomainDrainStatus::observe(domain("default"), &*sources);
            let resumed = flushing
                .await
                .assured("the flushing side only resumes one message and completes its flush");
            assert!(
                status.force_flush_obligations != 0 || status.holds_admitted_work(),
                "a drain observation saw the flush complete without the message the flush resumed"
            );
            drop(resumed);
        });
    });
}

/// A handoff that sees flush completion must also see work that flush resumed.
#[test]
fn shuttle_an_ownership_handoff_drain_sees_work_resumed_before_flush_completion() {
    check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let sources = Arc::new(OneRelayAndNode::new());
            sources.counters.begin_force_flush_obligation();
            let mut message = NodeQuiesceWorkGuard::begin(sources.counters.clone());
            message.park_for_required_materialized_state();
            let flushing_sources = sources.clone();
            let flushing = nervix_primitives::task::spawn(async move {
                message.resume_from_required_materialized_state();
                flushing_sources.counters.complete_force_flush_obligation();
                message
            });
            let work = sources
                .counters
                .outstanding_work_for(nervix_interconnect::EntityGatePurpose::OwnershipHandoff);
            let resumed = flushing
                .await
                .assured("the real flush resumes one parked message");
            assert!(
                work != 0,
                "an ownership drain must see either the outstanding flush or the work it resumed"
            );
            drop(resumed);
        });
    });
}
