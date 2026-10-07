//! Layer: data plane.
//! Owns: lifecycle invalidation and persisted-state removal for runtime state.
//! May depend on: runtime state carriers, the state store, and vocabulary models.
//! Must not know: control-plane transactions, NSPL parsing, or edge protocols.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "state and branch generation registration installs retained lifecycle owners"
    )
)]

use super::*;

impl Runtime {
    /// Drop the published generation of a concrete window branch after its empty eviction
    /// checkpoint has been flushed. The checkpoint remains available to a later appearance of the
    /// same key, but evicted branches no longer occupy the owner's in-memory state map.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "concrete branch eviction withdraws its exact window placement"
        )
    )]
    pub(in crate::runtime) fn release_evicted_window_state(
        &self,
        domain: &DomainName,
        processor: &ModelName,
        branch: &Option<BranchKey>,
    ) -> error_stack::Result<(), StateIdentityError> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::WindowProcessor,
            ModelKind::WindowProcessor,
            processor,
            branch.clone(),
        )?;
        self.inner.state_replication_routing.retire(&placement);
        self.inner
            .replicated_window_processor_states
            .remove(&placement);
        Ok(())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "domain routing installation retains its materialized membership epoch"
        )
    )]
    pub(in crate::runtime) fn relay_state_epoch(&self, domain: &DomainName) -> Arc<AtomicU64> {
        self.inner
            .relay_state_epochs
            .entry(domain.clone())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    pub(in crate::runtime) fn bump_relay_state_epoch(&self, domain: &DomainName) {
        self.relay_state_epoch(domain)
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::runtime) fn purge_materialized_relay_state(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> error_stack::Result<(), ExecutionBuildError> {
        let placements = self
            .inner
            .replicated_materialized_stream_states
            .iter()
            .filter(|entry| {
                entry.key().domain == *domain
                    && entry.key().kind == ModelKind::Relay
                    && entry.key().identifier == ModelName::from(&*relay)
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner.state_replication_routing.retire(&placement);
            self.inner
                .replicated_materialized_stream_states
                .remove(&placement);
        }
        if let Some(store) = &self.inner.state_store {
            store
                .purge_entity(
                    domain,
                    RuntimeStateKind::MaterializedRelay,
                    ModelKind::Relay,
                    relay,
                )
                .change_context_lazy(|| ExecutionBuildError::PurgeNodeState {
                    node: NodeRef::new(ModelKind::Relay, ModelName::from(relay)),
                })?;
        }
        Ok(())
    }

    pub(in crate::runtime) fn purge_deduplicator_state(
        &self,
        domain: &DomainName,
        deduplicator: &DeduplicatorName,
    ) -> error_stack::Result<(), ExecutionBuildError> {
        let placements = self
            .inner
            .replicated_deduplicator_states
            .iter()
            .filter(|entry| {
                entry.key().domain == *domain
                    && entry.key().kind == ModelKind::Deduplicator
                    && entry.key().identifier == ModelName::from(&*deduplicator)
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for placement in placements {
            self.inner.state_replication_routing.retire(&placement);
            self.inner.replicated_deduplicator_states.remove(&placement);
        }
        if let Some(store) = &self.inner.state_store {
            store
                .purge_entity(
                    domain,
                    RuntimeStateKind::Deduplicator,
                    ModelKind::Deduplicator,
                    deduplicator,
                )
                .change_context_lazy(|| ExecutionBuildError::PurgeNodeState {
                    node: NodeRef::new(ModelKind::Deduplicator, ModelName::from(deduplicator)),
                })?;
        }
        Ok(())
    }
}
