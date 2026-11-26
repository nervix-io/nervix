//! The save of everything a stopping node holds.
//!
//! Layer: data plane.
//!
//! - **Owns.** A stopping node's save: closing its intake, marking every domain clock it holds as
//!   running on a saving node, draining each domain it runs until the domain's work in motion
//!   rests, the cut of the held work that has no saved form yet, and the final writes of stopping
//!   each domain's execution.
//! - **Depends on.** The stop's view and cut rules of a stopping domain, the local intake
//!   boundary, the domain force-flush coordinator, the acknowledgement roots its ingestors still
//!   confirm, and the teardown of a domain execution.
//! - **Must not know.** Consensus, ownership moves, the shutdown timeout, or how the cut is
//!   presented.
//!
//! The save drains by the rule a domain stop drains by: it waits for work in motion and never for
//! held work, which only a timer or further input could move. Every domain clock reads as stopping
//! from the moment the save begins, so an emitter that cannot deliver a record hands it to its
//! retry schedule instead of retrying inline. A domain rests only once a generation requested with
//! no work in motion visible completes with none visible still, and with no admission or hand-on
//! counted since it was requested. The save puts no limit on that wait: work in motion ends on its
//! own, and a save that never ends is ended by the platform supervising the node.
//!
//! Once a domain rests, the save lets every publish on the wire run to its outcome and every
//! ingestor finish confirming on its transport the positions its graph acknowledged. Held work
//! whose kind has no saved form yet is cut, as a completed stop's teardown cuts it, and reported.
//! Then every domain's execution stops: each emitter finishes what its client holds, and every
//! placement writes its final checkpoint, so what the node holds is on its storage once the save
//! ends.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a node's save drains, cuts and stops the executions it installed, once"
    )
)]

use std::collections::BTreeMap;

use nervix_interconnect::domain_stop::StopDrainStatus;
use nervix_models::StopCut;

use super::{domain_stop::StopTeardown, *};

/// Whether this node still serves, or saves what it holds to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum NodeSave {
    Serving,
    /// The node saves what it holds to stop, and never serves again. Every domain reads as
    /// stopping on it.
    Saving,
}

/// What a node's save cut: for each domain, the held work whose kind has no saved form yet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LocalSaveReport {
    pub(crate) cuts: BTreeMap<DomainName, Vec<StopCut>>,
}

/// Where the save of one domain stands between two views of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SaveDrain {
    /// Work in motion was visible at the last view, or the save has not confirmed the domain yet.
    Releasing,
    /// No work in motion was visible when the generation that confirms nothing still moves was
    /// requested, and the domain's motion stood at `motion` then.
    Confirming { motion: u64 },
    /// A confirming generation completed with no work in motion visible and nothing moved since
    /// it was requested.
    Rested,
}

/// What the save does next for one domain, after a view of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SaveDrainStep {
    /// Release the work in motion with a generation, unless one is already in flight.
    Release,
    /// Request the generation that confirms nothing still moves.
    Confirm,
    /// Wait for the confirming generation to complete.
    AwaitConfirmation,
    /// The domain's work in motion rests.
    Rested,
}

impl SaveDrain {
    /// The step `view` calls for. Work in motion always sends the domain back to releasing, even
    /// one that rested, so the save ends only on a pass in which every domain rests.
    fn step(self, view: &StopDrainStatus) -> SaveDrainStep {
        if view.moving() {
            return SaveDrainStep::Release;
        }
        match self {
            Self::Releasing => SaveDrainStep::Confirm,
            Self::Confirming { .. } if view.flush_obligations != 0 => {
                SaveDrainStep::AwaitConfirmation
            }
            Self::Confirming { motion } if view.motion == motion => SaveDrainStep::Rested,
            // Work moved while the confirming generation ran and is out of sight again, so the
            // generation proves nothing: the save confirms the domain once more.
            Self::Confirming { .. } => SaveDrainStep::Confirm,
            Self::Rested => SaveDrainStep::Rested,
        }
    }
}

impl Runtime {
    /// Saves everything this node holds so it can stop, and returns what the save cut.
    ///
    /// It closes the node's intake, marks every domain clock as running on a saving node, and
    /// drains every domain the node runs until its work in motion rests. It then lets every
    /// publish on the wire run to its outcome, cuts the held work that has no saved form yet, and
    /// stops every domain's execution, which writes its final checkpoints.
    pub(crate) async fn save_local_node(&self) -> LocalSaveReport {
        let started = Instant::now();
        self.close_local_intake();
        self.mark_node_saving();
        let mut drains = BTreeMap::new();
        for execution in self.inner.executions.iter() {
            if !execution.passive_only {
                drains.insert(execution.key().clone(), SaveDrain::Releasing);
            }
        }
        info!(
            domains = drains.len(),
            "closed local intake; saving what this node holds"
        );
        let mut last_views = BTreeMap::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut rested = true;
            for (domain, drain) in &mut drains {
                if !self.advance_save_drain(domain, drain, &mut last_views) {
                    rested = false;
                }
            }
            if rested {
                break;
            }
            sleep(self.cluster_parameters().domain_drain_poll_interval()).await;
        }
        info!(
            elapsed = ?started.elapsed(),
            "the work in motion on this node rests"
        );
        let mut report = LocalSaveReport::default();
        for domain in drains.keys() {
            nervix_primitives::task::consume_budget().await;
            debug!(
                domain = %domain,
                "the save lets every publish of the domain on the wire run to its outcome"
            );
            Box::pin(self.finish_publishes_on_the_wire(domain)).await;
            debug!(
                domain = %domain,
                "the save lets the domain's ingestors confirm the positions its graph acknowledged"
            );
            Box::pin(self.finish_transport_confirmations(domain)).await;
            let Some(start_version) = self.domain_start_version(domain) else {
                continue;
            };
            let teardown = StopTeardown {
                start_version,
                enforced: false,
            };
            let cuts = self.teardown_cuts(domain, teardown);
            if !cuts.is_empty() {
                report.cuts.insert(domain.clone(), cuts);
            }
        }
        debug!("the save stops every domain execution, which writes its final checkpoints");
        Box::pin(self.stop_domain_executions(
            self.emitter_finish_deadline(),
            super::runtime_lifecycle::ExecutionsStop::Save,
        ))
        .await;
        info!(
            elapsed = ?started.elapsed(),
            "this node saved what it holds"
        );
        report
    }

    /// Marks this node as saving what it holds, and every domain clock it holds with it. A clock
    /// created afterwards reads the node's mark as it is created.
    fn mark_node_saving(&self) {
        self.inner.node_save.send_replace(NodeSave::Saving);
        for domain in self.inner.domains.iter() {
            domain.clock.mark_node_saving();
        }
        // A client emitter's delivery owner reads its clock only when something wakes it, so each
        // looks again now, and returns what no consumer can take.
        for endpoint in self.inner.client_emitters.iter() {
            endpoint.observe_domain_lifecycle();
        }
    }

    /// Takes the step a view of `domain` calls for, and reports whether the domain rests. A view
    /// that differs from the last one `last_views` holds for the domain is logged, so a save that
    /// does not end shows what it still waits for.
    fn advance_save_drain(
        &self,
        domain: &DomainName,
        drain: &mut SaveDrain,
        last_views: &mut BTreeMap<DomainName, StopDrainStatus>,
    ) -> bool {
        let view = self.observe_stopping_domain(domain);
        if last_views.get(domain) != Some(&view) {
            debug!(
                domain = %domain,
                drain = ?drain,
                entries = ?view.entries,
                flush_obligations = view.flush_obligations,
                motion = view.motion,
                "the save's view of a domain changed"
            );
            last_views.insert(domain.clone(), view.clone());
        }
        match drain.step(&view) {
            SaveDrainStep::Release => {
                self.force_flush_domain_if_idle(domain);
                *drain = SaveDrain::Releasing;
                false
            }
            SaveDrainStep::Confirm => {
                self.force_flush_domain(domain);
                let confirming = self.observe_stopping_domain(domain);
                *drain = if confirming.moving() {
                    SaveDrain::Releasing
                } else {
                    SaveDrain::Confirming {
                        motion: confirming.motion,
                    }
                };
                false
            }
            SaveDrainStep::AwaitConfirmation => false,
            SaveDrainStep::Rested => {
                *drain = SaveDrain::Rested;
                true
            }
        }
    }

    /// Lets every ingestor of `domain` finish confirming on its transport the positions its graph
    /// acknowledged, such as a Kafka offset commit. The work in motion rested, so every
    /// acknowledgement that resolves has resolved, and each confirmation it started ends when its
    /// transport answers or the confirmation's own deadline passes. Stopping an ingestor gives it
    /// only its task grace, which a slow transport can outlast.
    async fn finish_transport_confirmations(&self, domain: &DomainName) {
        let tracker = match self.inner.in_flight_by_domain.get(domain) {
            Some(tracker) => tracker.value().clone(),
            None => return,
        };
        loop {
            nervix_primitives::task::consume_budget().await;
            if tracker.confirming() == 0 {
                return;
            }
            sleep(self.cluster_parameters().domain_drain_poll_interval()).await;
        }
    }

    /// The generation of `domain` this node installed, which its save tears down.
    fn domain_start_version(&self, domain: &DomainName) -> Option<u64> {
        let entry = self.inner.domains.get(domain)?;
        Some(entry.start_version)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_interconnect::domain_stop::{StopWaitEntry, StopWaitKind};

    use super::*;

    fn view(kinds: &[StopWaitKind], flush_obligations: u64, motion: u64) -> StopDrainStatus {
        let mut entries = Vec::with_capacity(kinds.len());
        for kind in kinds {
            entries.push(StopWaitEntry {
                entity: None,
                kind: *kind,
                items: 1,
            });
        }
        StopDrainStatus {
            entries,
            flush_obligations,
            motion,
        }
    }

    #[test]
    fn a_domain_rests_only_after_a_confirming_generation_completes_with_nothing_moved() {
        let quiet = view(&[], 0, 7);
        assert_eq!(SaveDrain::Releasing.step(&quiet), SaveDrainStep::Confirm);

        let confirming = SaveDrain::Confirming { motion: 7 };
        assert_eq!(
            confirming.step(&view(&[], 1, 7)),
            SaveDrainStep::AwaitConfirmation
        );
        assert_eq!(confirming.step(&quiet), SaveDrainStep::Rested);
        assert_eq!(SaveDrain::Rested.step(&quiet), SaveDrainStep::Rested);
    }

    #[test]
    fn work_that_moved_out_of_sight_during_the_confirmation_is_confirmed_again() {
        let confirming = SaveDrain::Confirming { motion: 7 };

        // An upstream flush of the confirming generation published into a relay after the
        // downstream participant finished its share, and the batch moved on before this view.
        let moved = view(&[], 0, 9);

        assert_eq!(confirming.step(&moved), SaveDrainStep::Confirm);
    }

    #[test]
    fn work_in_motion_sends_even_a_rested_domain_back_to_releasing() {
        for drain in [
            SaveDrain::Releasing,
            SaveDrain::Confirming { motion: 7 },
            SaveDrain::Rested,
        ] {
            for kind in [
                StopWaitKind::AdmittingIngestor,
                StopWaitKind::ActiveGenerator,
                StopWaitKind::NodeWork,
                StopWaitKind::RelayBatches,
                StopWaitKind::AdmittingRelay,
                StopWaitKind::EmitterBuffer,
                StopWaitKind::PublishOnTheWire,
            ] {
                assert_eq!(
                    drain.step(&view(&[kind], 0, 7)),
                    SaveDrainStep::Release,
                    "{kind:?} is work in motion"
                );
            }
        }
    }

    #[test]
    fn held_work_never_holds_the_save_open() {
        let held = view(
            &[
                StopWaitKind::RequiredWait,
                StopWaitKind::EmitterRetry,
                StopWaitKind::RelayBatchesHeld,
                StopWaitKind::NodeWorkHeld,
                StopWaitKind::ProcessorHeld,
            ],
            0,
            7,
        );

        assert_eq!(SaveDrain::Releasing.step(&held), SaveDrainStep::Confirm);
        assert_eq!(
            SaveDrain::Confirming { motion: 7 }.step(&held),
            SaveDrainStep::Rested
        );
    }

    #[nervix_primitives::test]
    async fn a_node_running_nothing_saves_at_once_with_its_intake_closed() {
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

        let report = runtime.save_local_node().await;

        assert_eq!(report, LocalSaveReport::default());
        assert!(runtime.local_intake_is_closed());
        assert_eq!(control.cause(), Some(IngestorQuiesceCause::Shutdown));
        assert_eq!(*runtime.inner.node_save.borrow(), NodeSave::Saving);
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_save_waits_for_an_ingestor_to_confirm_the_positions_its_graph_acknowledged() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let trackers = runtime.ingestor_ack_root_trackers(
            &domain,
            &named::<IngestorName>("source"),
            Unit::SINGLE,
        );
        let (root, completion) = trackers.confirmed_root();
        root.ack_success();

        let confirming = nervix_primitives::task::spawn({
            let runtime = runtime.clone();
            let domain = domain.clone();
            async move { runtime.finish_transport_confirmations(&domain).await }
        });
        nervix_primitives::time::sleep(Duration::from_secs(10)).await;
        assert!(
            !confirming.is_finished(),
            "a save must wait while an ingestor confirms positions on its transport"
        );

        drop(completion);
        confirming
            .await
            .expect("the wait for transport confirmations does not panic");
    }

    #[test]
    fn a_saving_node_makes_every_clock_it_holds_and_creates_read_as_stopping() {
        let runtime = Runtime::new();
        let held = domain("held");
        let mut state = crate::runtime::unpaced_domain_state(held.as_str());
        state.status = nervix_models::DomainStatus::Running;
        state.start_version = 1;
        runtime.sync_domains(&BTreeMap::from([(held.clone(), state.clone())]));
        let clock = runtime
            .domain_clock_lifecycle(&held)
            .assured("the synchronized domain owns its clock")
            .bind()
            .assured("a running unpaced domain installs its clock");
        assert!(!clock.is_stopping());

        runtime.mark_node_saving();

        assert!(clock.is_stopping());
        runtime.sync_domains(&BTreeMap::from([(held.clone(), state.clone())]));
        assert!(
            clock.is_stopping(),
            "a later installation of the committed record keeps the saving mark"
        );
        let created = domain("created");
        let mut created_state = crate::runtime::unpaced_domain_state(created.as_str());
        created_state.status = nervix_models::DomainStatus::Running;
        created_state.start_version = 1;
        runtime.sync_domains(&BTreeMap::from([
            (held.clone(), state),
            (created.clone(), created_state),
        ]));
        let created_clock = runtime
            .domain_clock_lifecycle(&created)
            .assured("the synchronized domain owns its clock")
            .bind()
            .assured("a running unpaced domain installs its clock");
        assert!(created_clock.is_stopping());
    }
}
