//! The memory-ordering claims of a drain observation, explored by Loom over the production
//! observation order.
//!
//! Layer: test harness.
//!
//! - **Owns.** The invariants that a drain observation acquires what moving work released: a batch
//!   a node publishes into a relay, a batch a consumer takes from a relay, and a batch a relay's
//!   owner hands on to its consumer are each visible to an observation that saw the move's source
//!   let the batch go, and a message a force flush resumed is visible to an observation that saw
//!   the flush complete.
//! - **Depends on.** The production observation order, relay fan-out and transit, node quiesce
//!   counters, and the Loom runner of `nervix-model-harness`.
//! - **Must not know.** What the work is, which relay or node holds it, or why the node drains.
//!
//! One thread moves the work and the main thread observes, so the only synchronization between
//! them is the counts' own. A consumer's queue is a real lock-free queue outside the model, which
//! only the moving thread touches; the counts the observation reads beside it are Loom's.
//! `just test-loom` runs the models, and `just test-loom-qualification` shows that weakening the
//! release or the acquire each model depends on makes it fail.

use meticulous::ResultExt as _;
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_primitives::sync::Arc;

use super::{
    LocalDomainDrainStatus, NodeQuiesceWorkGuard, OneRelayAndNode, OutputBufferQuiesceGauge, domain,
};

const MOVING_BATCH: InvariantId = InvariantId::new("runtime.local-drain.moving-batch");
const TAKEN_BATCH: InvariantId = InvariantId::new("runtime.local-drain.taken-batch");
const HANDED_ON_BATCH: InvariantId = InvariantId::new("runtime.local-drain.handed-on-batch");
const FLUSH_COMPLETION: InvariantId = InvariantId::new("runtime.local-drain.flush-completion");

#[test]
fn loom_a_batch_published_while_a_drain_observes_is_never_missing_from_it() {
    explore(MOVING_BATCH, || {
        let sources = Arc::new(OneRelayAndNode::new());
        let mut output = OutputBufferQuiesceGauge::new(sources.counters.clone());
        output.add_batch();
        let publishing_sources = Arc::clone(&sources);
        let publishing = spawn(move || {
            publishing_sources.fanout.begin_owner_admission().accept();
            output.remove_batches(1);
        });
        let status = LocalDomainDrainStatus::observe(domain("default"), &*sources);
        publishing
            .join()
            .assured("the publishing side only moves one batch into the relay");
        assert!(
            status.holds_admitted_work(),
            "a drain observation reported no work while a batch moved from a node into a relay"
        );
    });
}

#[test]
fn loom_a_batch_a_consumer_takes_while_a_drain_observes_is_never_missing_from_it() {
    explore(TAKEN_BATCH, || {
        let sources = Arc::new(OneRelayAndNode::new());
        let mut consumer = sources.attach_consumer();
        sources.deliver_to_consumers();
        let counters = Arc::clone(&sources.counters);
        let taking = spawn(move || {
            let taken = consumer.try_recv_with_quiesce(Some(&counters));
            (consumer, taken)
        });
        let status = LocalDomainDrainStatus::observe(domain("default"), &*sources);
        let taken = taking
            .join()
            .assured("the consuming side only takes one batch");
        assert!(
            status.holds_admitted_work(),
            "a drain observation reported no work while a consumer took a batch from a relay"
        );
        drop(taken);
    });
}

#[test]
fn loom_a_batch_its_owner_hands_on_while_a_drain_observes_is_never_missing_from_it() {
    explore(HANDED_ON_BATCH, || {
        let sources = Arc::new(OneRelayAndNode::new());
        let consumer = sources.attach_consumer();
        sources.fanout.begin_owner_admission().accept();
        let owner_sources = Arc::clone(&sources);
        let handing_on = spawn(move || {
            let completion = owner_sources.fanout.begin_owner_batch_completion();
            owner_sources.deliver_to_consumers();
            drop(completion);
        });
        let status = LocalDomainDrainStatus::observe(domain("default"), &*sources);
        handing_on
            .join()
            .assured("the owner side only hands one batch on");
        assert!(
            status.holds_admitted_work(),
            "a drain observation reported no work while a relay handed a batch on to its consumer"
        );
        drop(consumer);
    });
}

#[test]
fn loom_a_message_a_flush_resumed_is_seen_by_a_drain_that_sees_the_flush_complete() {
    explore(FLUSH_COMPLETION, || {
        let sources = Arc::new(OneRelayAndNode::new());
        sources.counters.begin_force_flush_obligation();
        let mut message = NodeQuiesceWorkGuard::begin(sources.counters.clone());
        message.park_for_required_materialized_state();
        let flushing_sources = Arc::clone(&sources);
        let flushing = spawn(move || {
            message.resume_from_required_materialized_state();
            flushing_sources.counters.complete_force_flush_obligation();
            message
        });
        let status = LocalDomainDrainStatus::observe(domain("default"), &*sources);
        let resumed = flushing
            .join()
            .assured("the flushing side only resumes one message and completes its flush");
        assert!(
            status.force_flush_obligations != 0 || status.holds_admitted_work(),
            "a drain observation saw the flush complete without the message the flush resumed"
        );
        drop(resumed);
    });
}
