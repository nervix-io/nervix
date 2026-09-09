//! Layer: data plane.
//! Owns: lifecycle invalidation and persisted-state removal for runtime state.
//! May depend on: runtime state carriers, the state store, and vocabulary models.
//! Must not know: control-plane transactions, NSPL parsing, or edge protocols.

use super::*;

impl Runtime {
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
    ) -> Result<(), RuntimeError> {
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
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "failed to purge materialized state for relay '{}': {error}",
                        relay.as_str()
                    ),
                })?;
        }
        Ok(())
    }

    pub(in crate::runtime) fn purge_deduplicator_state(
        &self,
        domain: &DomainName,
        deduplicator: &DeduplicatorName,
    ) -> Result<(), RuntimeError> {
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
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "failed to purge state for deduplicator '{}': {error}",
                        deduplicator.as_str()
                    ),
                })?;
        }
        Ok(())
    }
}
