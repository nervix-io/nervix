//! A stopping domain's work on this node: the view the leader's stop owner drains by, and the
//! report of what this node's teardown of a stopped domain cut.
//!
//! Layer: data plane.
//!
//! - **Owns.** The classification of a stopping domain's work into work in motion, which its stop
//!   waits for, and held work, which it does not; the force-flush steps of its drain; and the cut
//!   ledger each teardown of a stop keeps until the stop's owner collects it.
//! - **Depends on.** The local drain sources (relay admissions, node quiesce counters, ingestor
//!   quiesce controls, generator activity), emitter buffers, publishing and retry state, and the
//!   committed domain record this node installed.
//! - **Must not know.** Consensus, the stop's requests or deadlines, or how an answer presents
//!   what was cut.
//!
//! A stop waits for work in motion: an ingestor still admitting, a generator still producing, work
//! a node has in hand or queued, collected and route-buffered work a flush releases, batches a
//! relay still delivers, records an emitter's next flush publishes, and a publish on the wire. It
//! does not wait for held work, which only a timer or further input could move: messages a
//! `REQUIRED WAIT` retains and records an emitter holds for a retry. Every observation reads the
//! domain's relay admission sequences before and after everything else, as the local drain's does,
//! so work that entered a relay while the view read shows as a relay admitting.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        observer,
        reason = "a stop's drain and teardown observe installed task and buffer owners"
    )
)]

use std::collections::BTreeMap;

use nervix_interconnect::domain_stop::{
    StopDrainAction, StopDrainStatus, StopReportAnswer, StopWaitEntry, StopWaitKind,
};
use nervix_models::{StopCut, StopCutKind};

use super::*;

/// What one node's teardown of a stopped domain cut, kept until the stop's owner collects it or
/// the domain starts again.
#[derive(Debug, Clone)]
pub(crate) struct StopLedgerEntry {
    /// The domain's start version when the stop that this teardown serves ended it.
    start_version: u64,
    /// Every cut of the teardown, or `None` while the teardown still runs.
    cuts: Option<Vec<StopCut>>,
}

/// How this node tears a stopped domain down: the ending of the stop that just ended it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct StopTeardown {
    pub(in crate::runtime) start_version: u64,
    /// An enforced stop ends the domain's tasks at once and leaves the last snapshot as it was;
    /// a completed one lets a publish on the wire run to its outcome and writes the final
    /// snapshot.
    pub(in crate::runtime) enforced: bool,
}

/// The entries of one view, summed per entity and kind.
#[derive(Default)]
struct StopViewEntries {
    entries: BTreeMap<(Option<NodeRef>, StopWaitKind), u64>,
}

impl StopViewEntries {
    fn add(&mut self, entity: Option<NodeRef>, kind: StopWaitKind, items: u64) {
        if items == 0 {
            return;
        }
        let total = self.entries.entry((entity, kind)).or_insert(0);
        *total = total
            .checked_add(items)
            .assured("every count tallies work items this node already holds in memory");
    }

    fn into_entries(self) -> Vec<StopWaitEntry> {
        let mut entries = Vec::with_capacity(self.entries.len());
        for ((entity, kind), items) in self.entries {
            entries.push(StopWaitEntry {
                entity,
                kind,
                items,
            });
        }
        entries
    }
}

fn entity_of(node: &DomainNodeRef) -> NodeRef {
    node.node.clone()
}

fn count(value: usize) -> u64 {
    value.arch_into()
}

impl StopTeardown {
    /// The teardown the committed `state` asks of this node: the record ended a stop whose report
    /// is still missing. Any other record asks for none.
    pub(in crate::runtime) fn of(state: &DomainState) -> Option<Self> {
        if state.status != nervix_models::DomainStatus::Stopped {
            return None;
        }
        let ending = state.stop_ending()?;
        if ending.report.is_some() {
            return None;
        }
        Some(Self {
            start_version: state.start_version,
            enforced: ending.ending.enforced(),
        })
    }

    /// The cut a teardown of this ending reports for one entry of the view it took as it began.
    /// Work in motion appears in a completed stop's view only when it moved after its drain
    /// rested, and an enforced stop cuts every kind of it; held work keeps its own kind either
    /// way. A count of the whole domain names no entity and is no cut.
    fn cut_of(self, entry: &StopWaitEntry) -> Option<StopCut> {
        let entity = entry.entity.as_ref()?;
        let kind = match entry.kind {
            StopWaitKind::AdmittingIngestor | StopWaitKind::ActiveGenerator => return None,
            // A batch that entered the relay while the teardown's view read it is in that relay
            // when the relay is cut.
            StopWaitKind::AdmittingRelay if self.enforced => StopCutKind::Enforced,
            StopWaitKind::AdmittingRelay => StopCutKind::RelayBatches,
            StopWaitKind::RequiredWait => StopCutKind::RequiredWaitMessages,
            StopWaitKind::EmitterRetry => StopCutKind::EmitterUndelivered,
            StopWaitKind::RelayBatchesHeld => StopCutKind::RelayBatches,
            StopWaitKind::NodeWorkHeld => StopCutKind::NodeBatches,
            StopWaitKind::ProcessorHeld => match entity.kind {
                nervix_models::ModelKind::Reorderer => StopCutKind::ReordererRows,
                nervix_models::ModelKind::Correlator => StopCutKind::CorrelatorRecords,
                nervix_models::ModelKind::WindowProcessor => StopCutKind::WindowAcknowledgements,
                _ => StopCutKind::NodeBatches,
            },
            StopWaitKind::PublishOnTheWire => StopCutKind::Enforced,
            StopWaitKind::NodeWork if self.enforced => StopCutKind::Enforced,
            StopWaitKind::RelayBatches if self.enforced => StopCutKind::Enforced,
            StopWaitKind::EmitterBuffer if self.enforced => StopCutKind::Enforced,
            StopWaitKind::NodeWork => StopCutKind::NodeBatches,
            StopWaitKind::RelayBatches => StopCutKind::RelayBatches,
            StopWaitKind::EmitterBuffer => StopCutKind::EmitterUndelivered,
        };
        Some(StopCut {
            entity_kind: entity.kind,
            entity: entity.identifier.clone(),
            kind,
            items: entry.items,
        })
    }
}

impl Runtime {
    /// Takes the drain step the stop's owner asks of this node, and reports the stopping domain as
    /// it reads afterwards.
    ///
    /// As the local drain's draining step does, a node that still holds work in motion requests a
    /// force-flush generation to release it unless one is already in flight, and a node that holds
    /// none requests nothing: the owner confirms the drain with a generation of its own.
    pub(crate) fn stop_drain_status(
        &self,
        domain: &DomainName,
        action: StopDrainAction,
    ) -> StopDrainStatus {
        match action {
            StopDrainAction::FlushMovingWork => {
                let status = self.observe_stopping_domain(domain);
                if !status.moving() {
                    return status;
                }
                self.force_flush_domain_if_idle(domain);
                self.observe_stopping_domain(domain)
            }
            StopDrainAction::Confirm => {
                self.force_flush_domain(domain);
                self.observe_stopping_domain(domain)
            }
            StopDrainAction::Observe => self.observe_stopping_domain(domain),
        }
    }

    /// Observes what `domain` holds on this node, in motion and held, entity by entity.
    ///
    /// Force-flush obligations are read first, so an obligation seen complete makes everything its
    /// participant did for the flush visible to the reads that follow. The relays' admission
    /// sequences are read next and once more at the end, as the local drain reads them.
    fn observe_stopping_domain(&self, domain: &DomainName) -> StopDrainStatus {
        let mut flush_obligations = 0_usize;
        for counters in self.inner.node_quiesce_counters.iter() {
            if &counters.key().entity.domain == domain {
                flush_obligations = flush_obligations
                    .checked_add(counters.value().force_flush_obligations())
                    .assured("every obligation is one this node's participants took on");
            }
        }
        let admissions_before = self.domain_relay_admissions(domain);
        let mut view = StopViewEntries::default();
        self.observe_stopping_intake(domain, &mut view);
        for counters in self.inner.node_quiesce_counters.iter() {
            if &counters.key().entity.domain != domain {
                continue;
            }
            let entity = entity_of(&counters.key().entity);
            view.add(
                Some(entity.clone()),
                StopWaitKind::NodeWork,
                count(counters.value().admitted_work()),
            );
            view.add(
                Some(entity.clone()),
                StopWaitKind::RequiredWait,
                count(counters.value().parked_work()),
            );
            view.add(
                Some(entity),
                StopWaitKind::ProcessorHeld,
                count(counters.value().held_rows()),
            );
        }
        for fanout in self.inner.relay_boundary_fanouts.iter() {
            if &fanout.key().domain == domain {
                view.add(
                    Some(entity_of(fanout.key())),
                    StopWaitKind::RelayBatches,
                    count(fanout.value().outstanding_work_len()),
                );
            }
        }
        self.observe_stopping_emitters(domain, &mut view);
        let admissions_after = self.domain_relay_admissions(domain);
        for relay in admissions_after.relays_admitted_since(&admissions_before) {
            view.add(Some(entity_of(&relay)), StopWaitKind::AdmittingRelay, 1);
        }
        StopDrainStatus {
            entries: view.into_entries(),
            flush_obligations: count(flush_obligations),
            motion: admissions_after.total(),
        }
    }

    fn domain_relay_admissions(&self, domain: &DomainName) -> RelayAdmissions {
        let mut admissions = RelayAdmissions::default();
        for fanout in self.inner.relay_boundary_fanouts.iter() {
            if &fanout.key().domain == domain {
                admissions.record(fanout.key().clone(), fanout.value().transit().clone());
            }
        }
        admissions
    }

    /// Ingestors that have not stopped admitting, and generators still producing.
    fn observe_stopping_intake(&self, domain: &DomainName, view: &mut StopViewEntries) {
        for ingestor in self.inner.ingestors.iter() {
            if &ingestor.key().entity.domain != domain {
                continue;
            }
            let stopped_admitting = match self.inner.ingestor_quiescence.get(ingestor.key()) {
                Some(control) => control.is_quiesced(),
                None => false,
            };
            if !stopped_admitting {
                view.add(
                    Some(entity_of(&ingestor.key().entity)),
                    StopWaitKind::AdmittingIngestor,
                    1,
                );
            }
        }
        if let Some(activity) = self.inner.generator_activity_by_domain.get(domain) {
            view.add(
                None,
                StopWaitKind::ActiveGenerator,
                count(activity.load(Ordering::Acquire)),
            );
        }
    }

    /// Each emitter's publish on the wire, and the records it buffers: held while a retry holds
    /// them, and in motion while its next flush publishes them.
    fn observe_stopping_emitters(&self, domain: &DomainName, view: &mut StopViewEntries) {
        for waits in self.inner.emitter_confirmation_waits.iter() {
            if &waits.key().entity.domain == domain {
                view.add(
                    Some(entity_of(&waits.key().entity)),
                    StopWaitKind::PublishOnTheWire,
                    count(waits.value().load(Ordering::Acquire)),
                );
            }
        }
        for messages in self.inner.emitter_buffers.iter() {
            if &messages.key().entity.domain != domain {
                continue;
            }
            let retrying = match self.inner.emitter_statuses.get(messages.key()) {
                Some(status) => status
                    .value()
                    .snapshot()
                    .is_some_and(|failure| failure.retry.is_some()),
                None => false,
            };
            let kind = if retrying {
                StopWaitKind::EmitterRetry
            } else {
                StopWaitKind::EmitterBuffer
            };
            view.add(
                Some(entity_of(&messages.key().entity)),
                kind,
                count(messages.value().load(Ordering::Acquire)),
            );
        }
    }

    /// The stop whose ending the coming teardown of `domain` serves: the committed record ended a
    /// stop whose report is still missing. Any other teardown, of a domain that was not stopped by
    /// a stop or whose report is already committed, serves none.
    pub(in crate::runtime) fn stop_teardown(&self, domain: &DomainName) -> Option<StopTeardown> {
        self.inner.domains.get(domain)?.stop_teardown
    }

    /// Records what the teardown of every domain whose stop `domains` ended is about to cut, while
    /// the domain's tasks still run: the committed revision that carries the ending also installs
    /// the stopped domain clock, which ends a processor's branch on its own and with it the count
    /// of what the branch held. A completed stop's teardown first lets a publish on the wire run to
    /// its outcome. A domain this node runs no execution of, or whose teardown it already recorded,
    /// needs nothing.
    pub(in crate::runtime) async fn capture_stop_teardowns(
        &self,
        domains: &BTreeMap<DomainName, DomainState>,
    ) {
        for (domain, state) in domains {
            nervix_primitives::task::consume_budget().await;
            let Some(teardown) = StopTeardown::of(state) else {
                continue;
            };
            let running = self
                .inner
                .executions
                .get(domain)
                .is_some_and(|execution| !execution.passive_only);
            if !running {
                continue;
            }
            let recorded = self
                .inner
                .stop_ledgers
                .get(domain)
                .is_some_and(|entry| entry.start_version == teardown.start_version);
            if recorded {
                continue;
            }
            self.begin_stop_ledger(domain, teardown);
            if !teardown.enforced {
                Box::pin(self.finish_publishes_on_the_wire(domain)).await;
            }
            self.record_stop_cuts(domain, teardown);
        }
    }

    /// Records that this node's teardown for the stop that ended `domain` at `start_version` has
    /// begun, so the stop's owner waits for its report.
    pub(in crate::runtime) fn begin_stop_ledger(
        &self,
        domain: &DomainName,
        teardown: StopTeardown,
    ) {
        self.inner.stop_ledgers.insert(
            domain.clone(),
            StopLedgerEntry {
                start_version: teardown.start_version,
                cuts: None,
            },
        );
    }

    /// Lets every publish on the wire of `domain`'s emitters run to its outcome. A completed
    /// stop's drain rested with none on the wire, so only an attempt a retry began since then can
    /// be one, and it ends when its destination answers or its own request deadline passes.
    pub(in crate::runtime) async fn finish_publishes_on_the_wire(&self, domain: &DomainName) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut on_the_wire = false;
            for waits in self.inner.emitter_confirmation_waits.iter() {
                if &waits.key().entity.domain == domain
                    && waits.value().load(Ordering::Acquire) != 0
                {
                    on_the_wire = true;
                    break;
                }
            }
            if !on_the_wire {
                return;
            }
            sleep(self.cluster_parameters().domain_drain_poll_interval()).await;
        }
    }

    /// Records what the teardown of `domain` is about to cut: every entry of the domain's view as
    /// the teardown begins, as `teardown` reports it, and every payload an ingestor's quiesce
    /// buffer still retains.
    pub(in crate::runtime) fn record_stop_cuts(&self, domain: &DomainName, teardown: StopTeardown) {
        let view = self.observe_stopping_domain(domain);
        let mut cuts = Vec::new();
        for entry in &view.entries {
            if let Some(cut) = teardown.cut_of(entry) {
                cuts.push(cut);
            }
        }
        for control in self.inner.ingestor_quiescence.iter() {
            if &control.key().entity.domain != domain {
                continue;
            }
            let retained = control.value().counters().buffered_records;
            if retained != 0 {
                cuts.push(StopCut {
                    entity_kind: control.key().entity.kind(),
                    entity: control.key().entity.identifier().clone(),
                    kind: StopCutKind::IngestorPauseBuffer,
                    items: count(retained),
                });
            }
        }
        self.inner.stop_ledgers.insert(
            domain.clone(),
            StopLedgerEntry {
                start_version: teardown.start_version,
                cuts: Some(cuts),
            },
        );
    }

    /// What this node's teardown cut for the stop that ended `domain` at `start_version`.
    pub(crate) fn stop_report(&self, domain: &DomainName, start_version: u64) -> StopReportAnswer {
        if let Some(entry) = self.inner.stop_ledgers.get(domain)
            && entry.start_version == start_version
        {
            return match &entry.cuts {
                Some(cuts) => StopReportAnswer::Reported(cuts.clone()),
                None => StopReportAnswer::Pending,
            };
        }
        // A node that still runs the domain's execution has not torn it down yet; one that runs
        // none holds no report for that stop.
        let running = self
            .inner
            .executions
            .get(domain)
            .is_some_and(|execution| !execution.passive_only);
        if running {
            StopReportAnswer::Pending
        } else {
            StopReportAnswer::Unknown
        }
    }

    /// Forgets the ledger of `domain`'s earlier stop once the domain starts again.
    pub(in crate::runtime) fn forget_stop_ledger(&self, domain: &DomainName, start_version: u64) {
        self.inner
            .stop_ledgers
            .remove_if(domain, |_, entry| entry.start_version < start_version);
    }
}

#[cfg(test)]
#[path = "domain_stop_tests.rs"]
mod tests;
