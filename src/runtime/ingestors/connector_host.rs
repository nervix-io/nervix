//! What one ingestor unit's connector reaches on its node, through the handle it is opened with.
//!
//! Layer: data plane.
//!
//! - **Owns.** The provider behind the `IngestorHost` a unit's connector is composed, opened,
//!   resumed and read with: the status the connector publishes beside its loop's, its error
//!   events, the node's bounded executor, resolver and outward TLS policy, the end of the instance,
//!   its declared event recorders and read-ahead overflow record, and the entity-specific services
//!   its composer prepared.
//! - **Depends on.** The connector contract's host services and the node-wide owners the runtime
//!   holds.
//! - **Must not know.** A broker driver, connector-specific configuration, or how the unit's loop
//!   takes messages in.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{any::Any, time::Duration};

use nervix_connector::{
    ConnectorEventCounter, ConnectorEventProvider, DomainOffsets, IngestorHost,
    IngestorHostServices, IngestorReadAheadOverflow, OutwardTlsPolicy, UndeclaredConnectorEvent,
};
use nervix_dns::DnsResolver;
use nervix_execution::Executor;
use nervix_models::ConnectorEvent;
use nervix_primitives::sync::{CancellationToken, DropGuard, blocking::OnceLock};

use super::super::*;

/// What a unit's composer prepared for its connector beyond the services every ingestor shares.
#[derive(Default)]
pub(super) struct IngestorEntityServices {
    /// The domain-owned offsets of an ingestor whose entity keeps its positions in its domain.
    pub(super) domain_offsets: Option<DomainOffsets>,
    /// The signaling protocol the domain compiled for an ingestor that names one, as the shared
    /// value its connector reads.
    pub(super) signaling_protocol: Option<Box<dyn Any + Send + Sync>>,
}

/// One ingestor unit's connector handle, together with the end of the instance it serves.
///
/// Whoever drives the instance holds this: the composer while it builds and opens the connector,
/// then the loop that runs it. Dropping it ends the instance for its connector, whether the loop
/// ended, its task was aborted, or a start was abandoned after the connector had opened.
pub(super) struct IngestorConnectorHost {
    handle: IngestorHost,
    /// Cancels the token the handle hands its connector.
    _instance_end: DropGuard,
}

impl IngestorConnectorHost {
    /// Pairs `handle` with `instance_end`, the token it hands its connector, which is cancelled
    /// when the pair is dropped.
    pub(super) fn new(handle: IngestorHost, instance_end: CancellationToken) -> Self {
        Self {
            handle,
            _instance_end: instance_end.drop_guard(),
        }
    }

    /// The handle the unit's connector is composed, opened, resumed and read with.
    pub(super) fn handle(&self) -> &IngestorHost {
        &self.handle
    }
}

impl Runtime {
    /// The handle through which `ingestor`'s connector reaches this node, built before anything of
    /// the connector runs.
    ///
    /// Building it registers and resolves nothing: the unit's status, its read-ahead overflow
    /// record, its event recorders and the node's resolver are resolved when the connector asks for
    /// them. An ingestor whose connector fails to open therefore leaves behind exactly what it did
    /// before a connector could reach its host, and a handle built to retire an ingestor's
    /// external state registers nothing for a unit that no longer runs.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "composing an ingestor unit builds its connector's handle once before the \
                      connector opens"
        )
    )]
    pub(super) fn ingestor_connector_host(
        &self,
        ingestor: &IngestorSpec,
        entity: IngestorEntityServices,
    ) -> IngestorConnectorHost {
        let instance_end = CancellationToken::new();
        let handle = IngestorHost::new(RuntimeIngestorConnectorServices {
            runtime: self.clone(),
            domain: ingestor.domain.clone(),
            ingestor: ingestor.name.clone(),
            unit: ingestor.unit,
            kind: ingestor.declared_input.entity_kind(),
            instance_end: instance_end.clone(),
            status: OnceLock::new(),
            entity,
        });
        IngestorConnectorHost::new(handle, instance_end)
    }
}

/// The node's services as one ingestor unit's connector reaches them.
struct RuntimeIngestorConnectorServices {
    runtime: Runtime,
    domain: DomainName,
    ingestor: IngestorName,
    unit: Unit,
    kind: nervix_models::IngestorEntityKind,
    instance_end: CancellationToken,
    /// The status the unit publishes for its `DESCRIBE INGESTOR`, resolved the first time the
    /// connector publishes through it and kept from then on, so no later publication reaches the
    /// node's registry of statuses.
    status: OnceLock<Arc<IngestorStatus>>,
    entity: IngestorEntityServices,
}

impl RuntimeIngestorConnectorServices {
    /// The status this unit publishes for its `DESCRIBE INGESTOR`, which its loop publishes its
    /// own failures through as well.
    fn status(&self) -> Arc<IngestorStatus> {
        if let Some(status) = self.status.get() {
            return status.clone();
        }
        let resolved = nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "the first status a connector publishes resolves its unit's status once; every later \
             publication reuses the retained one",
            self.runtime.ingestor_status(&DomainNodeUnitRef::new(
                DomainNodeRef::node_in(
                    self.domain.clone(),
                    ModelKind::Ingestor,
                    self.ingestor.clone(),
                ),
                self.unit,
            ))
        );
        self.status.set(resolved.clone()).discarded(
            "two publications racing to be first resolved the same registry entry, so the status \
             one of them retained is the status the other resolved",
        );
        resolved
    }
}

impl ConnectorEventProvider for RuntimeIngestorConnectorServices {
    /// An entity that declares no event has no recorder to resolve, and asking for one registers
    /// nothing. A declared event resolves through the unit's one registration of its interaction
    /// children, which returns the children already registered for the unit, so the connector
    /// records through the children its loop registers when the unit starts.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "connector construction resolves its exact unit's declared event children"
        )
    )]
    fn event_counter(
        &self,
        event: ConnectorEvent,
    ) -> error_stack::Result<ConnectorEventCounter, UndeclaredConnectorEvent> {
        if self.kind.capabilities().events.as_slice().is_empty() {
            return Err(UndeclaredConnectorEvent { event }.into());
        }
        let dispatcher = self.runtime.inner.remote_dispatcher.load();
        let (interactions, _decode_rejections) =
            self.runtime.inner.metrics.register_ingestor_interactions(
                &self.domain,
                &self.ingestor,
                self.unit,
                dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
                self.kind,
            );
        interactions.events.counter(event)
    }
}

impl IngestorHostServices for RuntimeIngestorConnectorServices {
    /// Publishes the connector's own status beside the status its loop publishes: each withdraws
    /// only what it published, so neither hides nor clears the other's failure.
    fn record_transient_error(&self, reason: String, retry_after: Duration) {
        self.status().connection.fail_connector(
            reason,
            Some(RuntimeReconnectStatus {
                backoff: retry_after,
                retry_at: Instant::now() + retry_after,
            }),
        );
    }

    fn clear_transient_error(&self) {
        self.status().connection.clear_connector();
    }

    fn report_error(&self, message: String) {
        self.runtime.events().report_error(format!(
            "ingestor error for ingestor '{}' in domain '{}': {message}",
            self.ingestor.as_str(),
            self.domain.as_str(),
        ));
    }

    fn executor(&self) -> Executor {
        self.runtime.executor().clone()
    }

    fn dns(&self) -> Option<DnsResolver> {
        self.runtime.dns().cloned()
    }

    fn tls_policy(&self) -> OutwardTlsPolicy {
        self.runtime.outward_tls().clone()
    }

    fn cancellation(&self) -> CancellationToken {
        self.instance_end.clone()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a connector resolves its unit's overflow recorder once when it opens"
        )
    )]
    fn read_ahead_overflow(&self) -> IngestorReadAheadOverflow {
        let dispatcher = self.runtime.inner.remote_dispatcher.load();
        self.runtime
            .inner
            .metrics
            .ingestor_read_ahead_dropped_payloads(
                &self.domain,
                &self.ingestor,
                self.unit,
                dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
                self.kind,
            )
    }

    fn domain_offsets(&self) -> Option<DomainOffsets> {
        self.entity.domain_offsets.clone()
    }

    fn signaling_protocol(&self) -> Option<&(dyn Any + Send + Sync)> {
        match &self.entity.signaling_protocol {
            Some(protocol) => Some(protocol.as_ref()),
            None => None,
        }
    }
}
