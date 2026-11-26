//! The stops of an entity swap that come before its main hold, upstream first.
//!
//! Layer: data plane.
//! - **Owns.** Stopping, before a swap holds any relay, the generator and ingestor units upstream
//!   of the relays it holds, and stopping the reingestors it replaces in the order rows flow
//!   through them, each once the swap holds its input relays.
//! - **Depends on.** The swap's decided stop order, the unit stops of each kind, the local entity
//!   gates and the swap journal that restores what a failed swap stopped.
//! - **Must not know.** How a part finishes the work it holds, placement decisions, or what the
//!   swap replaces under its main hold.
//!
//! A swap holds the input relays of every part it replaces until it ends, and a part that stops
//! delivers what it holds before it ends, with no limit of its own. A part that delivered into a
//! relay the swap already holds would wait for the swap while the swap waited for it, so the swap
//! stops parts in the order rows flow: first, before any hold, the generators it replaces and the
//! ingestor units whose rows reach a relay it holds; then each reingestor it replaces, upstream
//! first, once it holds that reingestor's input relays. A domain's graph has no loops, so a stop
//! never waits for a hold the same swap engaged.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "an entity swap of schedule publication stops each replaced part once per \
                  published revision"
    )
)]

use error_stack::ResultExt as _;

use super::{
    entity_gate::EntityGateHold,
    entity_swap_restore::{EntitySwapJournal, EntitySwapStep, IngestorSwap},
    *,
};

/// What the stops before a swap's main hold read: the parts it replaces and the order it stops
/// them in.
#[derive(Clone, Copy)]
pub(super) struct UpstreamStops<'a> {
    /// Every node the swap replaces on this cluster node, in node order.
    pub(super) entities: &'a [NodeRef],
    pub(super) explicitly_replaced: &'a BTreeSet<NodeRef>,
    /// The reassigned nodes, in node order.
    pub(super) reassignments: &'a [NodeRef],
    pub(super) stop_order: &'a SwapStopOrder,
    /// What the swap holds back, which every hold it engages fences the same way.
    pub(super) targets: &'a EntityGateTargets,
    pub(super) local_node_id: Option<&'a ClusterNodeName>,
}

impl<'a> UpstreamStops<'a> {
    /// Every reingestor the swap replaces, in the order it stops them: first those the decided
    /// order names, upstream first, then, in node order, any it does not name. The order names
    /// every reingestor of the schedule the domain runs, so a reingestor it leaves out is one the
    /// installed execution lacks, which its stop reports.
    fn reingestors_upstream_first(self) -> Vec<&'a NodeRef> {
        let mut ordered = Vec::new();
        let mut named = BTreeSet::new();
        for reingestor in self.stop_order.reingestors() {
            if self.entities.binary_search(reingestor).is_ok() {
                ordered.push(reingestor);
                named.insert(reingestor);
            }
        }
        for entity in self.entities {
            if entity.kind == ModelKind::Reingestor && !named.contains(entity) {
                ordered.push(entity);
            }
        }
        ordered
    }
}

impl Runtime {
    /// Stops, before the swap holds any relay, the generator units it replaces and the ingestor
    /// units it stops whose rows reach a relay it holds. Each finishes what it holds into relays
    /// nobody holds yet, with no limit of its own, and every stop is recorded in `journal` so a
    /// swap that fails afterwards starts the unit again.
    pub(super) async fn stop_swap_sources(
        &self,
        domain: &DomainName,
        revision: &ExecutionRevision,
        stops: UpstreamStops<'_>,
        journal: &mut EntitySwapJournal,
    ) -> error_stack::Result<(), RuntimeError> {
        for entity in stops.entities {
            nervix_primitives::task::consume_budget().await;
            if entity.kind != ModelKind::Generator {
                continue;
            }
            self.stop_swapped_generator_units(
                domain,
                entity,
                revision,
                stops.explicitly_replaced.contains(entity),
                stops.local_node_id,
                journal,
            )
            .await?;
        }
        let mut identities = StagedIdentities::default();
        for ingestor in self.ingestors_stopping_before_holds(domain, revision, stops) {
            nervix_primitives::task::consume_budget().await;
            if ingestor.takes_desired_identity {
                identities.install(
                    self,
                    revision,
                    &NodeRef::new(ModelKind::Ingestor, ModelName::from(&ingestor.name)),
                    journal,
                );
            }
            let step = journal.record(IngestorSwap::new(ingestor.name.clone()));
            for unit in ingestor.units {
                nervix_primitives::task::consume_budget().await;
                self.stop_ingestor_unit(domain, &ingestor.name, unit, RouteStop::Finish)
                    .await
                    .change_context_lazy(|| RuntimeError::build_domain_execution(domain))?;
                journal.step(step).record_stopped(unit);
                journal.record_stopped_before_holds(DomainNodeUnitRef::new(
                    DomainNodeRef::node_in(
                        domain.clone(),
                        ModelKind::Ingestor,
                        ingestor.name.clone(),
                    ),
                    unit,
                ));
            }
        }
        Ok(())
    }

    /// The running ingestor units the swap stops whose rows reach a relay it holds: every unit
    /// of an ingestor it replaces or whose layout `revision` changes, and every unit of a
    /// reassigned ingestor that `revision` no longer runs here.
    fn ingestors_stopping_before_holds(
        &self,
        domain: &DomainName,
        revision: &ExecutionRevision,
        stops: UpstreamStops<'_>,
    ) -> Vec<IngestorUnitStops> {
        /// Why the swap stops an ingestor's units on this node. A later variant wins over an earlier
        /// one for the same ingestor.
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum IngestorStop {
            /// The ingestor was reassigned: the units `revision` no longer runs here stop.
            Leaving,
            /// The swap replaces the ingestor: every unit stops.
            Replaced,
            /// `revision` changes the ingestor's layout: every unit stops, under the identities
            /// installed before the swap, since a unit the new layout removes has none after it.
            Layout,
        }

        let mut candidates = BTreeMap::<IngestorName, IngestorStop>::new();
        let mut propose = |name: IngestorName, stop: IngestorStop| {
            let current = candidates.entry(name).or_insert(stop);
            *current = (*current).max(stop);
        };
        for entity in stops.reassignments {
            if entity.kind == ModelKind::Ingestor {
                propose(
                    IngestorName::from(&entity.identifier),
                    IngestorStop::Leaving,
                );
            }
        }
        for entity in stops.entities {
            if entity.kind == ModelKind::Ingestor {
                propose(
                    IngestorName::from(&entity.identifier),
                    IngestorStop::Replaced,
                );
            }
        }
        for ingestor in self.replaced_ingestor_layouts(domain, Some(revision)) {
            propose(ingestor, IngestorStop::Layout);
        }
        let mut stopping = Vec::new();
        for (name, stop) in candidates {
            if !stops.stop_order.stops_before_holds(&name) {
                continue;
            }
            let desired = revision
                .nodes
                .get(&NodeRef::new(ModelKind::Ingestor, ModelName::from(&name)));
            let mut units = Vec::new();
            for unit in self.running_ingestor_units(domain, &name) {
                let stops_here = match (stop, desired) {
                    (IngestorStop::Replaced | IngestorStop::Layout, _)
                    | (IngestorStop::Leaving, None) => true,
                    (IngestorStop::Leaving, Some(node)) => {
                        !Self::scheduled_unit_executes_locally(node, unit, stops.local_node_id)
                    }
                };
                if stops_here {
                    units.push(unit);
                }
            }
            if !units.is_empty() {
                stopping.push(IngestorUnitStops {
                    name,
                    units,
                    takes_desired_identity: stop != IngestorStop::Layout,
                });
            }
        }
        stopping
    }

    /// Stops each reingestor the swap replaces, upstream first. Where the reingestor runs on this
    /// node, the swap holds its input relays and waits for what entered them, then the reingestor
    /// drains what it already received and its routes finish their deliveries into relays the
    /// swap does not hold yet. Every stop is recorded in `journal`.
    ///
    /// Every reingestor the swap replaces goes through its stop, also where none of it runs here,
    /// so a swap of a reingestor the installed execution lacks fails, naming it, before anything
    /// starts in its place.
    ///
    /// Returns the holds engaged so far, which stay engaged until the swap ends, with the error
    /// when a hold cannot be fenced or a stop fails.
    pub(super) async fn stop_swapped_reingestors(
        &self,
        domain: &DomainName,
        revision: &ExecutionRevision,
        stops: UpstreamStops<'_>,
        journal: &mut EntitySwapJournal,
    ) -> ReingestorStops {
        let mut holds = Vec::new();
        let mut identities = StagedIdentities::default();
        for reingestor in stops.reingestors_upstream_first() {
            nervix_primitives::task::consume_budget().await;
            let runs_here = self.reingestor_runs_here(domain, reingestor);
            if runs_here {
                let relays = self.entity_pause_relays(
                    domain,
                    &EntityGateTargets::Entities(vec![reingestor.clone()]),
                );
                let mut hold = self.engage_entity_unit_gates(
                    domain,
                    &relays,
                    stops.targets,
                    EntityGateLease {
                        deadline: Instant::now() + self.entity_gate_deadline(),
                        reason: "local scheduled node swap",
                    },
                    RelayDispatchGateHold::ProducersAndHandOn,
                );
                let fenced = hold.wait_quiescent().await;
                holds.push(hold);
                if !fenced {
                    return ReingestorStops {
                        holds,
                        outcome: Err(Report::new(ExecutionBuildError::GateFenceTimeout)
                            .change_context(RuntimeError::build_domain_execution(domain))),
                    };
                }
                identities.install(self, revision, reingestor, journal);
            }
            if let Err(error) = self
                .stop_swapped_reingestor(domain, reingestor, revision, journal)
                .await
            {
                return ReingestorStops {
                    holds,
                    outcome: Err(error),
                };
            }
            // Where the reingestor does not run here, the swap reaches its fault once it starts
            // the replacement.
            #[cfg(feature = "testing")]
            if runs_here
                && let Err(error) = self
                    .hold_or_fail_entity_swap(domain, reingestor, stops.local_node_id)
                    .await
            {
                return ReingestorStops {
                    holds,
                    outcome: Err(error),
                };
            }
        }
        ReingestorStops {
            holds,
            outcome: Ok(()),
        }
    }

    /// Whether this node runs any task or branched entrypoint of the reingestor `entity`.
    fn reingestor_runs_here(&self, domain: &DomainName, entity: &NodeRef) -> bool {
        let Some(execution) = self.inner.executions.get(domain) else {
            return false;
        };
        execution.reingestor_tasks.contains_key(entity)
            || execution
                .branched_entrypoints
                .contains_key(&entity.identifier)
    }
}

/// The running units of one ingestor the swap stops before it holds any relay.
struct IngestorUnitStops {
    name: IngestorName,
    units: Vec<Unit>,
    /// Whether the units stop under the identities `revision` gives them, as they would once the
    /// swap installs every identity: those of an ingestor the swap replaces or moves.
    takes_desired_identity: bool,
}

/// The state identities one stage of a swap installed early, for the parts it stops before the
/// swap installs every identity.
///
/// A part writes its last state as it stops, and its replacement reads that state under the
/// identity the swap installs. So a part the swap replaces or moves stops under that identity even
/// when it stops before the swap's main hold. The first identity a stage installs is recorded in
/// the journal, so a swap that fails afterwards installs the previous identities again.
#[derive(Default)]
struct StagedIdentities {
    journaled: bool,
}

impl StagedIdentities {
    fn install(
        &mut self,
        runtime: &Runtime,
        revision: &ExecutionRevision,
        node: &NodeRef,
        journal: &mut EntitySwapJournal,
    ) {
        if !self.journaled {
            journal.record_step(EntitySwapStep::StateIdentities);
            self.journaled = true;
        }
        runtime.install_state_identities_of(revision, node);
    }
}

/// What stopping the replaced reingestors left: the holds engaged on their input relays, which
/// stay engaged until the swap ends, and whether every stop succeeded.
pub(super) struct ReingestorStops {
    pub(super) holds: Vec<EntityGateHold>,
    pub(super) outcome: error_stack::Result<(), RuntimeError>,
}
