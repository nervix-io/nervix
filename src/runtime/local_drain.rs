//! Observing the work a node has already admitted, and closing its intake.
//!
//! Layer: data plane.
//!
//! - **Owns.** The admitted-work view that planned handoffs and backups drain by, the draining
//!   step they ask of a node, and the intake boundary a stopping node closes.
//! - **Depends on.** Ingestor quiesce controls, generator activity, relay boundary fan-outs, node
//!   quiesce counters, emitter publishing state, in-flight acknowledgement roots and the domain
//!   force-flush coordinator.
//! - **Must not know.** Ownership migration, consensus, cordons, or why the node drains.
//!
//! Broadcasting a shutdown watch cannot drain a graph. Every receiver takes a finite cut of its
//! own input, so an upstream task can still publish after a downstream task took its cut. A drain
//! never relies on a cut: its coordinator requests force-flush generations while any admitted work
//! is visible anywhere in a domain, and declares the domain quiescent only when a generation
//! requested with nothing visible completes with nothing visible still. That confirming generation
//! catches work an upstream flush published after a downstream participant finished its own
//! generation.
//!
//! Work keeps moving between the counts a drain reads, and no single order of reads follows every
//! move: a batch a node takes from a relay is counted by the node before the relay lets it go, while
//! a batch a node publishes is counted by the relay before the node lets it go. Every observation
//! therefore reads each relay's admission sequence before and after everything else, and a relay
//! that admitted a batch in between shows as work still moving rather than as nothing visible.
//!
//! Messages parked on `REQUIRED WAIT` do not hold a drain open: their dependency is absent, and
//! every generation re-evaluates them against the state that is present.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        observer,
        reason = "shutdown and impact inspection observe installed task and acknowledgement owners"
    )
)]

#[cfg(all(test, any(feature = "shuttle", feature = "loom")))]
use super::relay_boundary::RelayConsumerFanout;
use super::*;

/// Whether this node still admits new work into its graphs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum LocalIntake {
    Open,
    /// The node is stopping. Ingestors stop polling and admitting, and generators stop producing.
    /// Intake never reopens.
    Closed,
}

/// What one domain still holds on this node while it drains in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalDomainDrainStatus {
    pub(crate) domain: DomainName,
    /// Ingestors that have not stopped admitting new work.
    pub(crate) admitting_ingestors: usize,
    pub(crate) active_generators: usize,
    /// Unresolved acknowledgement roots, apart from roots whose every pending share waits on
    /// `REQUIRED WAIT`.
    pub(crate) outstanding_acks: usize,
    /// Relays that admitted a batch while the observation read, which work may have reached
    /// after the read that would have counted it.
    pub(crate) admitting_relays: usize,
    pub(crate) buffered_relay_batches: usize,
    /// Mailbox, in-flight, collected and route-buffered work of the domain's graph nodes.
    pub(crate) node_work_items: usize,
    pub(crate) buffered_emitter_messages: usize,
    /// Emitter publishes awaiting emitter confirmation or retrying, counted once per state.
    pub(crate) publishing_emitters: usize,
    pub(crate) required_waits: usize,
    pub(crate) force_flush_obligations: usize,
}

impl LocalDomainDrainStatus {
    /// A status showing nothing, which the tests of what a drain waits for start from.
    #[cfg(test)]
    fn new(domain: DomainName) -> Self {
        Self {
            domain,
            admitting_ingestors: 0,
            active_generators: 0,
            outstanding_acks: 0,
            admitting_relays: 0,
            buffered_relay_batches: 0,
            node_work_items: 0,
            buffered_emitter_messages: 0,
            publishing_emitters: 0,
            required_waits: 0,
            force_flush_obligations: 0,
        }
    }

    fn tally(count: &mut usize, more: usize) {
        *count = count
            .checked_add(more)
            .assured("every count tallies work items this node already holds in memory");
    }

    /// Whether admitted work is still visible in the domain. Force-flush obligations are the
    /// drain's own requests, and parked `REQUIRED WAIT` messages cannot finish while their
    /// dependency is absent, so neither is work the drain waits for.
    pub(crate) fn holds_admitted_work(&self) -> bool {
        self.admitting_ingestors != 0
            || self.active_generators != 0
            || self.outstanding_acks != 0
            || self.admitting_relays != 0
            || self.buffered_relay_batches != 0
            || self.node_work_items != 0
            || self.buffered_emitter_messages != 0
            || self.publishing_emitters != 0
    }

    /// Observes what `domain` holds, reading `sources` in the one order that keeps moving work
    /// visible.
    ///
    /// Force-flush obligations are read first. An obligation seen complete makes everything its
    /// participant did for the flush visible to the reads that follow, including a parked message
    /// the flush resumed. The relays' admission sequences are read next and once more at the end:
    /// a batch that reached a relay or a node only after the read that would have counted it
    /// entered some relay while the observation read, so the two readings differ.
    fn observe(domain: DomainName, sources: &impl LocalDomainDrainSources) -> Self {
        let force_flush_obligations = sources.force_flush_obligations();
        let admissions_before = sources.relay_admissions();
        let admitting_ingestors = sources.admitting_ingestors();
        let active_generators = sources.active_generators();
        let outstanding_acks = sources.outstanding_acks();
        let buffered_relay_batches = sources.buffered_relay_batches();
        let node_work = sources.node_work();
        let buffered_emitter_messages = sources.buffered_emitter_messages();
        let publishing_emitters = sources.publishing_emitters();
        let admissions_after = sources.relay_admissions();
        let admitting_relays = admissions_after.relays_admitting_since(&admissions_before);
        Self {
            domain,
            admitting_ingestors,
            active_generators,
            outstanding_acks,
            admitting_relays,
            buffered_relay_batches,
            node_work_items: node_work.admitted,
            buffered_emitter_messages,
            publishing_emitters,
            required_waits: node_work.parked,
            force_flush_obligations,
        }
    }
}

/// Where a drain observation reads the work one domain holds.
///
/// [`LocalDomainDrainStatus::observe`] owns the order of the reads, and each read answers what its
/// owners hold when it is asked.
trait LocalDomainDrainSources {
    /// Force-flush obligations the domain's nodes took on and have not completed.
    fn force_flush_obligations(&self) -> usize;
    /// The admission sequence of each of the domain's relays.
    fn relay_admissions(&self) -> RelayAdmissions;
    /// Ingestors that have not stopped admitting new work.
    fn admitting_ingestors(&self) -> usize;
    fn active_generators(&self) -> usize;
    /// Unresolved acknowledgement roots, apart from roots whose every pending share waits on
    /// `REQUIRED WAIT`.
    fn outstanding_acks(&self) -> usize;
    fn buffered_relay_batches(&self) -> usize;
    fn node_work(&self) -> LocalNodeWork;
    fn buffered_emitter_messages(&self) -> usize;
    /// Emitter publishes awaiting emitter confirmation or retrying, counted once per state.
    fn publishing_emitters(&self) -> usize;
}

/// The work the domain's graph nodes hold, read node by node.
#[derive(Debug, Default)]
struct LocalNodeWork {
    /// Mailbox, in-flight, collected and route-buffered work.
    admitted: usize,
    /// Messages parked on `REQUIRED WAIT`.
    parked: usize,
}

impl LocalNodeWork {
    /// Adds what one node's counts show now.
    fn count(&mut self, counters: &NodeQuiesceCounters) {
        LocalDomainDrainStatus::tally(&mut self.admitted, counters.admitted_work());
        LocalDomainDrainStatus::tally(&mut self.parked, counters.parked_work());
    }
}

impl Runtime {
    /// Stops every ingestor on this node admitting new work and tells generators to stop
    /// producing. An ingestor prepared after this call starts with its intake already stopped.
    pub(crate) fn close_local_intake(&self) {
        let previous = self.inner.local_intake.send_replace(LocalIntake::Closed);
        if previous == LocalIntake::Closed {
            return;
        }
        let ingestors = self
            .inner
            .ingestor_quiescence
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for ingestor in ingestors {
            self.engage_ingestor_quiesce(
                &ingestor.entity.domain,
                &IngestorName::from(ingestor.entity.identifier()),
                ingestor.unit,
                IngestorQuiesceCause::Shutdown,
            );
        }
    }

    pub(in crate::runtime) fn local_intake_is_closed(&self) -> bool {
        *self.inner.local_intake.borrow() == LocalIntake::Closed
    }

    pub(crate) fn local_domain_drain_status(&self, domain: &DomainName) -> LocalDomainDrainStatus {
        let sources = RuntimeDomainDrainSources {
            runtime: self,
            domain,
        };
        LocalDomainDrainStatus::observe(domain.clone(), &sources)
    }

    /// Takes the draining step a remote drain coordinator asks of this node, and reports the
    /// domain as it reads afterwards.
    ///
    /// As a local drain's draining step does, a node that still holds admitted work requests a
    /// force-flush generation to release it, unless one is already in flight. A node that holds
    /// none requests nothing: the coordinator confirms the drain with a generation of its own,
    /// and a participant that had taken a generation the confirmation superseded would re-read
    /// the dependency of every message parked on `REQUIRED WAIT` for both.
    pub(crate) fn flush_held_domain_work(&self, domain: &DomainName) -> LocalDomainDrainStatus {
        let status = self.local_domain_drain_status(domain);
        if !status.holds_admitted_work() {
            return status;
        }
        self.force_flush_domain_if_idle(domain);
        self.local_domain_drain_status(domain)
    }
}

/// One domain's work as this node's runtime holds it.
struct RuntimeDomainDrainSources<'a> {
    runtime: &'a Runtime,
    domain: &'a DomainName,
}

impl LocalDomainDrainSources for RuntimeDomainDrainSources<'_> {
    fn force_flush_obligations(&self) -> usize {
        let mut obligations = 0;
        for counters in self.runtime.inner.node_quiesce_counters.iter() {
            if &counters.key().entity.domain == self.domain {
                LocalDomainDrainStatus::tally(
                    &mut obligations,
                    counters.value().force_flush_obligations(),
                );
            }
        }
        obligations
    }

    fn relay_admissions(&self) -> RelayAdmissions {
        let mut admissions = RelayAdmissions::default();
        for fanout in self.runtime.inner.relay_boundary_fanouts.iter() {
            if &fanout.key().domain == self.domain {
                admissions.record(fanout.key().clone(), fanout.value().transit().clone());
            }
        }
        admissions
    }

    fn admitting_ingestors(&self) -> usize {
        let mut admitting = 0;
        for ingestor in self.runtime.inner.ingestors.iter() {
            if &ingestor.key().entity.domain != self.domain {
                continue;
            }
            let stopped_admitting = match self.runtime.inner.ingestor_quiescence.get(ingestor.key())
            {
                Some(control) => control.is_quiesced(),
                None => false,
            };
            if !stopped_admitting {
                LocalDomainDrainStatus::tally(&mut admitting, 1);
            }
        }
        admitting
    }

    fn active_generators(&self) -> usize {
        match self
            .runtime
            .inner
            .generator_activity_by_domain
            .get(self.domain)
        {
            Some(activity) => activity.load(Ordering::Acquire),
            None => 0,
        }
    }

    fn outstanding_acks(&self) -> usize {
        match self.runtime.inner.in_flight_by_domain.get(self.domain) {
            // A root whose every pending share waits on `REQUIRED WAIT` is exempt here exactly as
            // it is from a planned handoff.
            Some(tracker) => tracker.outstanding_for_ownership_handoff(),
            None => 0,
        }
    }

    fn buffered_relay_batches(&self) -> usize {
        let mut buffered = 0;
        for fanout in self.runtime.inner.relay_boundary_fanouts.iter() {
            if &fanout.key().domain == self.domain {
                LocalDomainDrainStatus::tally(&mut buffered, fanout.value().outstanding_work_len());
            }
        }
        buffered
    }

    fn node_work(&self) -> LocalNodeWork {
        let mut work = LocalNodeWork::default();
        for counters in self.runtime.inner.node_quiesce_counters.iter() {
            if &counters.key().entity.domain == self.domain {
                work.count(counters.value());
            }
        }
        work
    }

    fn buffered_emitter_messages(&self) -> usize {
        let mut buffered = 0;
        for messages in self.runtime.inner.emitter_buffers.iter() {
            if &messages.key().entity.domain == self.domain {
                LocalDomainDrainStatus::tally(
                    &mut buffered,
                    messages.value().load(Ordering::Acquire),
                );
            }
        }
        buffered
    }

    fn publishing_emitters(&self) -> usize {
        let mut publishing = 0;
        for waits in self.runtime.inner.emitter_confirmation_waits.iter() {
            if &waits.key().entity.domain == self.domain
                && waits.value().load(Ordering::Acquire) != 0
            {
                LocalDomainDrainStatus::tally(&mut publishing, 1);
            }
        }
        for status in self.runtime.inner.emitter_statuses.iter() {
            if &status.key().entity.domain == self.domain
                && let Some(failure) = status.value().snapshot()
                && failure.retry.is_some()
            {
                LocalDomainDrainStatus::tally(&mut publishing, 1);
            }
        }
        publishing
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use super::*;

    fn quiescent_status() -> LocalDomainDrainStatus {
        LocalDomainDrainStatus::new(domain("default"))
    }

    #[test]
    fn parked_waits_and_flush_obligations_are_not_admitted_work() {
        let mut status = quiescent_status();
        status.required_waits = 3;
        status.force_flush_obligations = 2;
        assert!(!status.holds_admitted_work());

        status.buffered_relay_batches = 1;
        assert!(status.holds_admitted_work());
    }

    /// A remote drain coordinator confirms a quiet domain with a generation of its own. A draining
    /// step that also requested one from a node holding no admitted work would make a participant
    /// that took that generation re-read a parked message's dependency for it, and then again for
    /// the confirmation, so the slow read held the cut twice.
    #[test]
    fn a_draining_step_requests_no_generation_from_a_node_holding_no_admitted_work() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let counters = runtime.node_quiesce_counters(
            &domain,
            NodeRef::new(ModelKind::Junction, named::<ModelName>("wait_for_state")),
        );
        let mut participant = runtime.force_flush_participant(&domain, counters);

        let status = runtime.flush_held_domain_work(&domain);

        assert!(!status.holds_admitted_work());
        assert_eq!(status.force_flush_obligations, 0);
        assert!(
            participant
                .pending_completion()
                .assured("the participant stays registered while the test holds it")
                .is_none(),
            "a node holding no admitted work requested a force-flush generation"
        );
    }

    #[test]
    fn a_draining_step_requests_one_generation_while_a_node_holds_admitted_work() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let counters = runtime.node_quiesce_counters(
            &domain,
            NodeRef::new(ModelKind::Junction, named::<ModelName>("route_orders")),
        );
        let mut participant = runtime.force_flush_participant(&domain, counters.clone());
        counters.admit(1);

        let status = runtime.flush_held_domain_work(&domain);

        assert!(status.holds_admitted_work());
        assert_eq!(status.force_flush_obligations, 1);
        let completion = participant
            .pending_completion()
            .assured("the participant stays registered while the test holds it")
            .assured("a node holding admitted work requested a force-flush generation");
        let again = runtime.flush_held_domain_work(&domain);
        assert_eq!(again.force_flush_obligations, 1);
        assert!(
            participant
                .pending_completion()
                .assured("the participant stays registered while the test holds it")
                .is_none(),
            "a draining step requested another generation while one was in flight"
        );
        counters.withdraw_admitted(1);
        assert!(completion.complete());
    }

    #[test]
    fn closing_local_intake_stops_every_registered_ingestor_admitting() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named::<IngestorName>("source");
        let control =
            test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Suspend);
        runtime.inner.ingestor_quiescence.insert(
            DomainNodeUnitRef::new(
                DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone()),
                Unit::SINGLE,
            ),
            control.clone(),
        );

        runtime.close_local_intake();

        assert!(runtime.local_intake_is_closed());
        assert_eq!(control.cause(), Some(IngestorQuiesceCause::Shutdown));
    }
}

/// One relay and one node's counts, as a drain observes them, for the checks that move work between
/// the two while an observation reads them.
#[cfg(all(test, any(feature = "shuttle", feature = "loom")))]
struct OneRelayAndNode {
    relay: DomainNodeRef,
    fanout: RelayConsumerFanout,
    counters: Arc<NodeQuiesceCounters>,
}

#[cfg(all(test, any(feature = "shuttle", feature = "loom")))]
impl OneRelayAndNode {
    fn new() -> Self {
        Self {
            relay: DomainNodeRef::node_in(
                domain("default"),
                ModelKind::Relay,
                named::<RelayName>("orders"),
            ),
            fanout: RelayConsumerFanout::with_capacity(NonZeroUsize::MIN),
            counters: Arc::new(NodeQuiesceCounters::default()),
        }
    }

    /// A batch for the relay, which carries no acknowledgement.
    fn batch() -> RelayRecordBatch {
        RelayRecordBatch::single(
            test_schema(&[("id", ParseAsType::I64)]),
            None,
            test_runtime_row([("id".to_string(), RuntimeValue::I64(1))]),
            AckSet::empty(),
        )
        .assured("a one-field test row matches its one-field test schema")
    }

    /// Registers the relay's one attached consumer, as a node that runs it does.
    fn attach_consumer(&self) -> RelayRuntimeFanIn {
        RelayRuntimeFanIn::new(
            self.fanout
                .runtime_consumer_receiver_for_mode(AckMode::Attached),
        )
    }

    /// Delivers a batch to the relay's attached consumers, as its owner task fans one out. The one
    /// consumer has room, so the delivery completes without waiting.
    fn deliver_to_consumers(&self) {
        let delivery = std::pin::pin!(
            self.fanout
                .attached_runtime_consumers
                .broadcast(Self::batch())
        );
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let delivered = match delivery.poll(&mut context) {
            std::task::Poll::Ready(delivered) => Some(delivered),
            std::task::Poll::Pending => None,
        };
        delivered
            .assured("a consumer with room admits the batch without waiting")
            .assured("the relay's consumer stays registered while the batch is delivered");
    }
}

#[cfg(all(test, any(feature = "shuttle", feature = "loom")))]
impl LocalDomainDrainSources for OneRelayAndNode {
    fn force_flush_obligations(&self) -> usize {
        self.counters.force_flush_obligations()
    }

    fn relay_admissions(&self) -> RelayAdmissions {
        let mut admissions = RelayAdmissions::default();
        admissions.record(self.relay.clone(), self.fanout.transit.clone());
        admissions
    }

    fn admitting_ingestors(&self) -> usize {
        0
    }

    fn active_generators(&self) -> usize {
        0
    }

    fn outstanding_acks(&self) -> usize {
        0
    }

    fn buffered_relay_batches(&self) -> usize {
        self.fanout.outstanding_work_len()
    }

    fn node_work(&self) -> LocalNodeWork {
        let mut work = LocalNodeWork::default();
        work.count(&self.counters);
        work
    }

    fn buffered_emitter_messages(&self) -> usize {
        0
    }

    fn publishing_emitters(&self) -> usize {
        0
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "local_drain_shuttle_tests.rs"]
mod shuttle_tests;

#[cfg(all(test, feature = "loom"))]
#[path = "local_drain_loom_models.rs"]
mod loom_models;
