//! Completing the work a terminating node has already admitted, in place.
//!
//! Layer: data plane.
//!
//! - **Owns.** The intake boundary a terminating node closes over its ingestors and generators, the
//!   force-flush generations that release its buffered work while downstream relays and sinks stay
//!   alive, and the proof that its graphs hold no admitted work before terminal teardown.
//! - **Depends on.** Ingestor quiesce controls, generator activity, relay boundary fan-outs, node
//!   quiesce counters, emitter publishing state, in-flight acknowledgement roots and the domain
//!   force-flush coordinator.
//! - **Must not know.** Ownership migration, consensus, cordons, or why the node is terminating.
//!
//! Broadcasting the terminal shutdown watch cannot drain a graph. Every receiver takes a finite cut
//! of its own input, so an upstream task can still publish after a downstream task took its cut.
//! This drain never relies on a cut. It closes intake, requests force-flush generations while any
//! admitted work is visible anywhere in a domain, and declares the domain quiescent only when a
//! generation requested with nothing visible completes with nothing visible still. That confirming
//! generation catches work an upstream flush published after a downstream participant finished its
//! own generation, and work that moved between two counters while they were being read.
//!
//! Messages parked on `REQUIRED WAIT` do not hold the drain open, exactly as in a planned handoff:
//! their dependency is absent, and every generation re-evaluates them against the state that is
//! present. Terminal teardown negatively acknowledges whatever still waits, so a source with
//! external acknowledgements redelivers it.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        observer,
        reason = "shutdown and impact inspection observe installed task and acknowledgement owners"
    )
)]

use super::*;

/// How often a draining node re-reads its work. The read is in-process, and the interval bounds
/// how long a drain that has already completed waits before terminal teardown starts.
const LOCAL_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Whether this node still admits new work into its graphs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum LocalIntake {
    Open,
    /// The node is terminating. Ingestors stop polling and admitting, generators stop producing,
    /// and work already admitted runs to completion. Intake never reopens.
    Closed,
}

/// How a local drain ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalGraphDrainOutcome {
    /// Every local domain completed the work it had admitted.
    Quiescent,
    /// The drain timeout elapsed while at least one domain still held admitted work.
    Abandoned,
}

/// What one domain still holds on this node while it drains in place.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalDomainDrainStatus {
    domain: DomainName,
    /// Ingestors that have not stopped admitting new work.
    admitting_ingestors: usize,
    active_generators: usize,
    /// Unresolved acknowledgement roots, apart from roots whose every pending share waits on
    /// `REQUIRED WAIT`.
    outstanding_acks: usize,
    buffered_relay_batches: usize,
    /// Mailbox, in-flight, collected and route-buffered work of the domain's graph nodes.
    node_work_items: usize,
    buffered_emitter_messages: usize,
    /// Emitter publishes awaiting sink confirmation or retrying, counted once per state.
    publishing_emitters: usize,
    required_waits: usize,
    force_flush_obligations: usize,
}

impl LocalDomainDrainStatus {
    /// A status showing nothing, which the tests of the drain's decisions start from.
    #[cfg(test)]
    fn new(domain: DomainName) -> Self {
        Self {
            domain,
            admitting_ingestors: 0,
            active_generators: 0,
            outstanding_acks: 0,
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
    fn holds_admitted_work(&self) -> bool {
        self.admitting_ingestors != 0
            || self.active_generators != 0
            || self.outstanding_acks != 0
            || self.buffered_relay_batches != 0
            || self.node_work_items != 0
            || self.buffered_emitter_messages != 0
            || self.publishing_emitters != 0
    }

    /// Reports a domain the drain timeout left behind. A domain that still showed admitted work
    /// names that work. One that showed none ran out of time only for the flush that confirms
    /// nothing is still moving, which is what a timeout too short for a single flush looks like.
    fn report_timeout(&self, timeout: Duration) {
        if self.holds_admitted_work() {
            warn!(
                domain = self.domain.as_str(),
                admitting_ingestors = self.admitting_ingestors,
                active_generators = self.active_generators,
                outstanding_acks = self.outstanding_acks,
                buffered_relay_batches = self.buffered_relay_batches,
                node_work_items = self.node_work_items,
                buffered_emitter_messages = self.buffered_emitter_messages,
                publishing_emitters = self.publishing_emitters,
                required_waits = self.required_waits,
                force_flush_obligations = self.force_flush_obligations,
                timeout = ?timeout,
                "local graph drain timed out with admitted work outstanding"
            );
            return;
        }
        warn!(
            domain = self.domain.as_str(),
            required_waits = self.required_waits,
            force_flush_obligations = self.force_flush_obligations,
            timeout = ?timeout,
            "local graph drain timed out before confirming that no admitted work is still moving"
        );
    }

    /// Observes what `domain` holds by reading each of its `sources` in turn.
    fn observe(domain: DomainName, sources: &impl LocalDomainDrainSources) -> Self {
        let admitting_ingestors = sources.admitting_ingestors();
        let active_generators = sources.active_generators();
        let outstanding_acks = sources.outstanding_acks();
        let buffered_relay_batches = sources.buffered_relay_batches();
        let node_work = sources.node_work();
        let force_flush_obligations = sources.force_flush_obligations();
        let buffered_emitter_messages = sources.buffered_emitter_messages();
        let publishing_emitters = sources.publishing_emitters();
        Self {
            domain,
            admitting_ingestors,
            active_generators,
            outstanding_acks,
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
    /// Ingestors that have not stopped admitting new work.
    fn admitting_ingestors(&self) -> usize;
    fn active_generators(&self) -> usize;
    /// Unresolved acknowledgement roots, apart from roots whose every pending share waits on
    /// `REQUIRED WAIT`.
    fn outstanding_acks(&self) -> usize;
    fn buffered_relay_batches(&self) -> usize;
    fn node_work(&self) -> LocalNodeWork;
    fn buffered_emitter_messages(&self) -> usize;
    /// Emitter publishes awaiting sink confirmation or retrying, counted once per state.
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

/// Where one domain's local drain stands between two observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalDomainDrainProgress {
    /// Admitted work was visible at the last observation.
    Draining,
    /// Nothing was visible, and a generation was requested to prove nothing is still moving.
    Confirming,
    /// A confirming generation completed with nothing visible.
    Quiescent,
}

/// What the drain does for one domain after observing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalDomainDrainStep {
    /// Keep releasing work, requesting a generation unless one is already in flight.
    FlushIfIdle,
    /// Request the generation that confirms nothing is still moving.
    Confirm,
    /// Wait for the confirming generation to complete.
    AwaitConfirmation,
    /// The domain holds no admitted work.
    Done,
}

impl LocalDomainDrainProgress {
    fn advance(&mut self, status: &LocalDomainDrainStatus) -> LocalDomainDrainStep {
        if status.holds_admitted_work() {
            *self = Self::Draining;
            return LocalDomainDrainStep::FlushIfIdle;
        }
        match self {
            Self::Draining => {
                *self = Self::Confirming;
                LocalDomainDrainStep::Confirm
            }
            Self::Confirming => {
                if status.force_flush_obligations != 0 {
                    return LocalDomainDrainStep::AwaitConfirmation;
                }
                *self = Self::Quiescent;
                LocalDomainDrainStep::Done
            }
            Self::Quiescent => LocalDomainDrainStep::Done,
        }
    }
}

impl Runtime {
    /// Closes this node's intake and completes the work its graphs already admitted, while
    /// downstream relays, emitters and acknowledgement paths stay alive. Returns once every local
    /// domain is quiescent, or once `timeout` has elapsed with admitted work still outstanding.
    pub(crate) async fn drain_local_graphs(&self, timeout: Duration) -> LocalGraphDrainOutcome {
        let started = Instant::now();
        self.close_local_intake();
        let mut domains = BTreeMap::new();
        for execution in self.inner.executions.iter() {
            domains.insert(execution.key().clone(), LocalDomainDrainProgress::Draining);
        }
        info!(
            domains = domains.len(),
            "closed local intake; draining local graphs in place"
        );
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut outstanding = Vec::new();
            for (domain, progress) in &mut domains {
                let status = self.local_domain_drain_status(domain);
                match progress.advance(&status) {
                    LocalDomainDrainStep::FlushIfIdle => {
                        self.force_flush_domain_if_idle(domain);
                        outstanding.push(status);
                    }
                    LocalDomainDrainStep::Confirm => {
                        self.force_flush_domain(domain);
                        outstanding.push(status);
                    }
                    LocalDomainDrainStep::AwaitConfirmation => outstanding.push(status),
                    LocalDomainDrainStep::Done => {}
                }
            }
            if outstanding.is_empty() {
                info!(
                    elapsed = ?started.elapsed(),
                    "local graphs completed their admitted work"
                );
                return LocalGraphDrainOutcome::Quiescent;
            }
            let remaining = timeout
                .checked_sub(started.elapsed())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                for status in &outstanding {
                    status.report_timeout(timeout);
                }
                return LocalGraphDrainOutcome::Abandoned;
            }
            sleep(remaining.min(LOCAL_DRAIN_POLL_INTERVAL)).await;
        }
    }

    /// Stops every ingestor on this node admitting new work and tells generators to stop
    /// producing. An ingestor prepared after this call starts with its intake already stopped.
    fn close_local_intake(&self) {
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
                &ingestor.domain,
                &IngestorName::from(ingestor.identifier()),
                IngestorQuiesceCause::Shutdown,
            );
        }
    }

    pub(in crate::runtime) fn local_intake_is_closed(&self) -> bool {
        *self.inner.local_intake.borrow() == LocalIntake::Closed
    }

    fn local_domain_drain_status(&self, domain: &DomainName) -> LocalDomainDrainStatus {
        let sources = RuntimeDomainDrainSources {
            runtime: self,
            domain,
        };
        LocalDomainDrainStatus::observe(domain.clone(), &sources)
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
            if &counters.key().domain == self.domain {
                LocalDomainDrainStatus::tally(
                    &mut obligations,
                    counters.value().force_flush_obligations(),
                );
            }
        }
        obligations
    }

    fn admitting_ingestors(&self) -> usize {
        let mut admitting = 0;
        for ingestor in self.runtime.inner.ingestors.iter() {
            if &ingestor.key().domain != self.domain {
                continue;
            }
            let stopped_admitting = match self.runtime.inner.ingestor_quiescence.get(ingestor.key())
            {
                Some(control) => control.cause() == Some(IngestorQuiesceCause::Shutdown),
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
            if &counters.key().domain == self.domain {
                work.count(counters.value());
            }
        }
        work
    }

    fn buffered_emitter_messages(&self) -> usize {
        let mut buffered = 0;
        for messages in self.runtime.inner.emitter_buffers.iter() {
            if &messages.key().domain == self.domain {
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
            if &waits.key().domain == self.domain && waits.value().load(Ordering::Acquire) != 0 {
                LocalDomainDrainStatus::tally(&mut publishing, 1);
            }
        }
        for status in self.runtime.inner.emitter_statuses.iter() {
            if &status.key().domain == self.domain
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

    #[test]
    fn a_domain_is_quiescent_only_after_a_confirming_generation_completes() {
        let mut progress = LocalDomainDrainProgress::Draining;
        let mut status = quiescent_status();

        assert_eq!(progress.advance(&status), LocalDomainDrainStep::Confirm);
        status.force_flush_obligations = 1;
        assert_eq!(
            progress.advance(&status),
            LocalDomainDrainStep::AwaitConfirmation
        );
        status.force_flush_obligations = 0;
        assert_eq!(progress.advance(&status), LocalDomainDrainStep::Done);
        assert_eq!(progress.advance(&status), LocalDomainDrainStep::Done);
    }

    #[test]
    fn upstream_publication_after_a_downstream_cut_restarts_the_confirmation() {
        let mut progress = LocalDomainDrainProgress::Draining;
        let mut status = quiescent_status();
        status.node_work_items = 1;
        assert_eq!(progress.advance(&status), LocalDomainDrainStep::FlushIfIdle);

        // The downstream participant completed its generation with nothing to release, so every
        // count read zero before the upstream flush of that same generation landed in its relay.
        status.node_work_items = 0;
        assert_eq!(progress.advance(&status), LocalDomainDrainStep::Confirm);
        status.buffered_relay_batches = 1;
        status.force_flush_obligations = 1;
        assert_eq!(progress.advance(&status), LocalDomainDrainStep::FlushIfIdle);

        status.buffered_relay_batches = 0;
        status.force_flush_obligations = 0;
        assert_eq!(progress.advance(&status), LocalDomainDrainStep::Confirm);
        assert_eq!(progress.advance(&status), LocalDomainDrainStep::Done);
    }

    #[nervix_primitives::test]
    async fn closing_local_intake_stops_every_registered_ingestor_admitting() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let ingestor = named::<IngestorName>("source");
        let control =
            test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Suspend);
        runtime.inner.ingestor_quiescence.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone()),
            control.clone(),
        );

        assert_eq!(
            runtime.drain_local_graphs(Duration::ZERO).await,
            LocalGraphDrainOutcome::Quiescent
        );
        assert!(runtime.local_intake_is_closed());
        assert_eq!(control.cause(), Some(IngestorQuiesceCause::Shutdown));
    }
}
