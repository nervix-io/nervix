//! Resolved state-replication lifetimes published by state installation.
//!
//! Layer: data plane.
//! - **Owns.** Frame routes to installed state, exact route retirement, and retained assignment slots.
//! - **Depends on.** Replicated state handles, typed placements and publication primitives.
//! - **Must not know.** Execution graphs, schedules, transport framing or state decoding.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "frames read published routes and retained assignments"
    )
)]

use imbl::{GenericHashMap, shared_ptr::DefaultSharedPtr};
use nervix_primitives::publication::{ArcSwap, ArcSwapOption};

use super::*;
use crate::runtime::materialized_state::MaterializedRelayStateRead;

type Routes = GenericHashMap<
    RuntimeStatePlacement,
    Arc<StateReplicationRoute>,
    ahash::RandomState,
    DefaultSharedPtr,
>;
type Assignments =
    GenericHashMap<DomainNodeRef, SharedStateAssignment, ahash::RandomState, DefaultSharedPtr>;
type MaterializedRelays = GenericHashMap<
    DomainNodeRef,
    MaterializedRelayPublication,
    ahash::RandomState,
    DefaultSharedPtr,
>;
type MaterializedRoutes = GenericHashMap<
    Option<BranchKey>,
    Arc<StateReplicationRoute>,
    ahash::RandomState,
    DefaultSharedPtr,
>;

#[derive(Clone, Default)]
struct Routing {
    routes: Routes,
    assignments: Assignments,
    materialized: MaterializedRelays,
}

/// A domain routing revision retains this relay's installed state lifetimes. Branch discovery
/// loads only its immutable index; dependency reads never scan node-wide state registries.
#[derive(Clone)]
pub(in crate::runtime) struct MaterializedRelayPublication {
    current: Arc<ArcSwap<MaterializedRoutes>>,
}

impl MaterializedRelayPublication {
    fn new() -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(MaterializedRoutes::default())),
        }
    }

    pub(in crate::runtime) fn record(
        &self,
        branch: &Option<BranchKey>,
    ) -> Option<MaterializedGenerationRecord> {
        let routes = self.current.load();
        if let Some(route) = routes.get(branch)
            && let Some(state) = route.materialized_read()
        {
            return state.record(branch);
        }
        if branch.is_some()
            && let Some(route) = routes.get(&None)
            && let Some(state) = route.materialized_read()
        {
            return state.record(branch);
        }
        None
    }

    pub(in crate::runtime) fn states(&self) -> Vec<MaterializedRelayStateRead> {
        let mut states = Vec::new();
        for route in self.current.load().values() {
            if let Some(state) = route.materialized_read() {
                states.push(state);
            }
        }
        states
    }

    pub(in crate::runtime) fn has_state_for(&self, branch: &Option<BranchKey>) -> bool {
        let routes = self.current.load();
        if let Some(route) = routes.get(branch)
            && route.is_current()
        {
            return true;
        }
        branch.is_some() && routes.get(&None).is_some_and(|route| route.is_current())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "state installation publishes the relay's exact installed route"
        )
    )]
    fn install(&self, route: &Arc<StateReplicationRoute>) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            if !route.is_current() {
                return next;
            }
            if let Some(previous) = next.insert(route.placement.branch_key.clone(), route.clone())
                && !Arc::ptr_eq(&previous, route)
            {
                previous.end();
            }
            next
        });
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "state retirement withdraws only the exact ended route"
        )
    )]
    fn retire(&self, ended: &Arc<StateReplicationRoute>) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            if let Some(route) = next.get(&ended.placement.branch_key)
                && Arc::ptr_eq(route, ended)
            {
                route.end();
                next.remove(&ended.placement.branch_key);
            }
            next
        });
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "assignment reconciliation reclaims ended relay route publications"
        )
    )]
    fn purge_stale(&self) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.retain(|_, route| route.is_current());
            next
        });
    }
}

/// A resolved checkpoint source. Frames select it at admission and keep its state across
/// replacement. Cold recovery uses the same capture operation. Absence selects storage.
pub(crate) struct StateReplicationRequest {
    pub(in crate::runtime) placement: RuntimeStatePlacement,
    pub(in crate::runtime) state: Option<StdArc<ReplicatedState>>,
}

/// The relationship established when a state is installed. Ending it clears its intake before
/// withdrawing the route; a retained route can never attach to a replacement, even at the same key.
pub(in crate::runtime) struct StateReplicationRoute {
    placement: RuntimeStatePlacement,
    assignment: SharedStateAssignment,
    state: ArcSwapOption<ReplicatedState>,
}

/// The actual installed state, shared with its executing task rather than looked up in a registry.
pub(in crate::runtime) enum ReplicatedState {
    BranchAggregated(Arc<ReplicatedBranchAggregatedState>),
    BranchLru(Arc<ReplicatedBranchLifecycle>),
    Deduplicator(Arc<ReplicatedDeduplicatorState>),
    KafkaOffset(Arc<ReplicatedKafkaOffsetState>),
    MaterializedRelay(Arc<ReplicatedMaterializedRelayState>),
    WasmProcessor(Arc<ReplicatedWasmProcessorState>),
    WindowProcessor(Arc<ReplicatedWindowProcessorState>),
}

impl ReplicatedState {
    pub(in crate::runtime) fn replication(&self) -> &CheckpointReplication {
        match self {
            Self::BranchAggregated(state) => state.replication(),
            Self::BranchLru(state) => state.replication(),
            Self::Deduplicator(state) => state.replication(),
            Self::KafkaOffset(state) => state.replication(),
            Self::MaterializedRelay(state) => state.replication(),
            Self::WasmProcessor(state) => state.replication(),
            Self::WindowProcessor(state) => state.replication(),
        }
    }
}

impl StateReplicationRoute {
    fn materialized_read(&self) -> Option<MaterializedRelayStateRead> {
        let state = self.current_state()?;
        if let ReplicatedState::MaterializedRelay(state) = state.as_ref() {
            return Some(ReplicatedMaterializedRelayState::read(state));
        }
        None
    }
    pub(in crate::runtime) fn state(&self) -> Option<StdArc<ReplicatedState>> {
        self.state.load_full()
    }

    fn current_state(&self) -> Option<StdArc<ReplicatedState>> {
        let state = self.state.load_full()?;
        let assignment = self.assignment.load();
        let assignment = assignment.as_deref()?;
        if !assignment.names(&self.placement) {
            return None;
        }
        Some(state)
    }

    pub(in crate::runtime) fn offer(
        &self,
        replication: &CheckpointReplication,
        lsm: u64,
    ) -> Option<Announcer> {
        let state = self.current_state()?;
        if !std::ptr::eq(state.replication(), replication) {
            return None;
        }
        replication.offer(lsm)
    }

    pub(in crate::runtime) fn is_current(&self) -> bool {
        let assignment = self.assignment.load();
        let Some(assignment) = assignment.as_deref() else {
            return false;
        };
        assignment.names(&self.placement) && self.state.load().is_some()
    }

    pub(in crate::runtime) fn owned_replicas(
        &self,
        local: &ClusterNodeName,
    ) -> Option<BTreeSet<ClusterNodeName>> {
        let assignment = self.assignment.load();
        let assignment = assignment.as_deref()?;
        if !assignment.names(&self.placement) || self.state.load().is_none() {
            return None;
        }
        let owners = assignment.checkpoint_owners.as_ref()?;
        match owners.primary.as_ref() {
            Some(primary) if primary != local => return None,
            None if !owners.executors.contains(local) => return None,
            _ => {}
        }
        Some(owners.replicas.clone())
    }

    pub(in crate::runtime) fn placement(&self) -> &RuntimeStatePlacement {
        &self.placement
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "state ending retires the exact installed route")
    )]
    fn end(&self) {
        self.state.store(None);
    }
}

/// Writers install routes at state lifetime boundaries. Readers select already-resolved handles
/// from one immutable publication; they never consult execution, identity or state registries.
pub(in crate::runtime) struct StateReplicationRouting {
    current: ArcSwap<Routing>,
}

impl Default for StateReplicationRouting {
    fn default() -> Self {
        Self {
            current: ArcSwap::from_pointee(Routing::default()),
        }
    }
}

impl StateReplicationRouting {
    pub(in crate::runtime) fn assigned_request(
        &self,
        placement: &RuntimeStatePlacement,
        local: &ClusterNodeName,
    ) -> Option<StateReplicationRequest> {
        let published = self.current.load();
        let slot = published.assignments.get(&placement.entity())?;
        let assignment = slot.load();
        let assignment = assignment.as_deref()?;
        if !assignment.names(placement) || !assignment.assigned_to(local) {
            return None;
        }
        let state = match published.routes.get(placement) {
            Some(route) => Some(route.current_state()?),
            None => None,
        };
        Some(StateReplicationRequest {
            placement: placement.clone(),
            state,
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the action uses the replication of one resolved state")
    )]
    pub(in crate::runtime) fn with_replication(
        &self,
        placement: &RuntimeStatePlacement,
        action: impl FnOnce(&CheckpointReplication),
    ) {
        let published = self.current.load();
        let Some(route) = published.routes.get(placement) else {
            return;
        };
        let state = route.state.load();
        let Some(state) = state.as_deref() else {
            return;
        };
        let assignment = route.assignment.load();
        let Some(assignment) = assignment.as_deref() else {
            return;
        };
        if !assignment.names(placement) {
            return;
        }
        action(state.replication());
    }

    pub(in crate::runtime) fn acknowledge(
        &self,
        placement: &RuntimeStatePlacement,
        node: &ClusterNodeName,
        lsm: u64,
    ) {
        self.with_replication(placement, |replication| replication.record(node, lsm));
    }

    pub(in crate::runtime) fn resolve(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<Arc<StateReplicationRoute>> {
        self.current.load().routes.get(placement).cloned()
    }

    pub(in crate::runtime) fn assignment(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<SharedStateAssignment> {
        self.assignment_for_entity(&placement.entity())
    }

    pub(in crate::runtime) fn assignment_for_entity(
        &self,
        entity: &DomainNodeRef,
    ) -> Option<SharedStateAssignment> {
        self.current.load().assignments.get(entity).cloned()
    }

    pub(in crate::runtime) fn materialized(
        &self,
        entity: &DomainNodeRef,
    ) -> Option<MaterializedRelayPublication> {
        self.current.load().materialized.get(entity).cloned()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "assignment registration publishes the existing slot before tasks bind"
        )
    )]
    pub(in crate::runtime) fn register_assignment(
        &self,
        entity: &DomainNodeRef,
        assignment: &SharedStateAssignment,
    ) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.assignments.insert(entity.clone(), assignment.clone());
            if entity.kind() == ModelKind::Relay && !next.materialized.contains_key(entity) {
                next.materialized
                    .insert(entity.clone(), MaterializedRelayPublication::new());
            }
            next
        });
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "state installation publishes its retained handle and retires a replaced \
                      route"
        )
    )]
    pub(in crate::runtime) fn install(
        &self,
        placement: RuntimeStatePlacement,
        assignment: SharedStateAssignment,
        state: ReplicatedState,
    ) {
        let materialized = matches!(&state, ReplicatedState::MaterializedRelay(_));
        let route = Arc::new(StateReplicationRoute {
            placement: placement.clone(),
            assignment,
            state: ArcSwapOption::from(Some(StdArc::new(state))),
        });
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            if let Some(previous) = next.routes.insert(placement.clone(), route.clone())
                && !Arc::ptr_eq(&previous, &route)
            {
                previous.end();
            }
            if materialized {
                let entity = placement.entity();
                if !next.materialized.contains_key(&entity) {
                    next.materialized
                        .insert(entity.clone(), MaterializedRelayPublication::new());
                }
            }
            next
        });
        if materialized && let Some(published) = self.materialized(&placement.entity()) {
            published.install(&route);
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "state retirement ends the route before withdrawing it"
        )
    )]
    pub(in crate::runtime) fn retire(&self, placement: &RuntimeStatePlacement) {
        let preceding = self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            if let Some(previous) = next.routes.remove(placement) {
                previous.end();
            }
            next
        });
        if let Some(ended) = preceding.routes.get(placement)
            && let Some(published) = self.materialized(&placement.entity())
        {
            published.retire(ended);
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "schedule replacement retires superseded state lifetimes"
        )
    )]
    pub(in crate::runtime) fn purge_stale(&self, domain: &DomainName) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.routes.retain(|placement, route| {
                if &placement.domain != domain {
                    return true;
                }
                if let Some(state) = route.state()
                    && let ReplicatedState::BranchLru(lifecycle) = state.as_ref()
                {
                    lifecycle.purge_stale_passive_checkpoints();
                }
                if route.is_current() {
                    return true;
                }
                route.end();
                false
            });
            for (entity, published) in next.materialized.iter() {
                if &entity.domain == domain {
                    published.purge_stale();
                }
            }
            next
        });
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "entity teardown ends every route before withdrawing its assignment"
        )
    )]
    pub(in crate::runtime) fn retire_entity(&self, entity: &DomainNodeRef) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.routes.retain(|placement, route| {
                if &placement.entity() != entity {
                    return true;
                }
                route.end();
                false
            });
            if let Some(published) = next.materialized.get(entity) {
                published.purge_stale();
            }
            next
        });
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "entity removal withdraws its assignment after retiring its routes"
        )
    )]
    pub(in crate::runtime) fn withdraw_entity(&self, entity: &DomainNodeRef) {
        self.retire_entity(entity);
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.assignments.remove(entity);
            next.materialized.remove(entity);
            next
        });
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "domain teardown ends retained routes and withdraws their publications"
        )
    )]
    pub(in crate::runtime) fn retire_domain(&self, domain: &DomainName) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.routes.retain(|placement, route| {
                if &placement.domain != domain {
                    return true;
                }
                route.end();
                false
            });
            for (entity, published) in next.materialized.iter() {
                if &entity.domain == domain {
                    published.purge_stale();
                }
            }
            next
        });
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "node teardown ends every retained state route")
    )]
    pub(in crate::runtime) fn clear(&self) {
        self.current.rcu(|current| {
            for route in current.routes.values() {
                route.end();
            }
            for published in current.materialized.values() {
                published.purge_stale();
            }
            Routing::default()
        });
    }
}

impl RuntimeStatePlacement {
    pub(in crate::runtime) fn entity(&self) -> DomainNodeRef {
        DomainNodeRef::node_in(self.domain.clone(), self.kind, self.identifier.clone())
    }
}

impl Runtime {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "state construction binds the existing assignment and actual state once"
        )
    )]
    pub(in crate::runtime) fn publish_state_replication_route(
        &self,
        placement: &RuntimeStatePlacement,
        state: ReplicatedState,
    ) {
        let assignment = self.state_assignment(&placement.entity());
        self.inner
            .state_replication_routing
            .install(placement.clone(), assignment, state);
    }
}

#[cfg(test)]
#[path = "routing_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "routing_shuttle_tests.rs"]
mod shuttle_tests;
