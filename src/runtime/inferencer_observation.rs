//! Layer: data plane.
//! Owns: bounded, cancellable collection of one current inferencer unit's scalar state facts.
//! Depends on: typed execution identities, immutable replication routes and bounded execution.
//! Must not know: Models, Plexus, tensor contents or rendering.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "observes published inference state")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(observer, reason = "collects scalar state summaries")
)]

use error_stack::{Report, ResultExt as _};
use nervix_execution::{CpuClass, MemoryClass};
use nervix_interconnect::RuntimeState;
use nervix_models::{
    ClusterNodeName, DomainNodeUnitRef, InferencerReplicaProgress, InferencerStateSummary,
};

use super::{RemoteDispatcher, Runtime, observability::RuntimeObservationError};

impl Runtime {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "binds inference metrics outside row processing")
    )]
    pub(super) fn inferencer_state_metrics(
        &self,
        domain: &nervix_models::DomainName,
        name: &nervix_models::ModelName,
        unit: nervix_models::Unit,
    ) -> crate::metrics::InferencerStateMetrics {
        let dispatcher = self.inner.remote_dispatcher.load();
        self.inner.metrics.inferencer_state_metrics(
            domain,
            name,
            unit,
            dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
        )
    }

    pub(crate) async fn describe_inferencer_unit_state(
        &self,
        target: DomainNodeUnitRef,
        expected: Option<RuntimeState>,
        bytes_per_branch: u64,
        replicas: Vec<ClusterNodeName>,
    ) -> Result<InferencerStateSummary, Report<RuntimeObservationError>> {
        {
            let execution = self
                .inner
                .executions
                .get(&target.entity.domain)
                .ok_or_else(|| {
                    Report::new(RuntimeObservationError::DomainNotInstantiated {
                        domain: target.entity.domain.clone(),
                    })
                })?;
            let node = execution
                .revision
                .nodes
                .get(&target.entity.node)
                .ok_or_else(|| {
                    Report::new(RuntimeObservationError::InferencerUnitNotCurrent {
                        target: target.clone(),
                    })
                })?;
            let dispatcher = self.inner.remote_dispatcher.load();
            let local = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
            let identity_matches = match (expected, node.inferencer_model) {
                (None, None) => true,
                (Some(RuntimeState::Inferencer { schema, model }), Some(current)) => {
                    schema == node.schema_fingerprint && model == current
                }
                _ => false,
            };
            if !identity_matches || !Self::scheduled_unit_executes_locally(node, target.unit, local)
            {
                return Err(Report::new(
                    RuntimeObservationError::InferencerUnitNotCurrent { target },
                ));
            }
        }
        let replica_bytes = replicas
            .len()
            .checked_mul(
                std::mem::size_of::<InferencerReplicaProgress>()
                    + std::mem::size_of::<ClusterNodeName>(),
            )
            .ok_or_else(|| Report::new(RuntimeObservationError::InferencerCounts))?;
        let mut working = replica_bytes
            .checked_add(std::mem::size_of::<InferencerStateSummary>())
            .ok_or_else(|| Report::new(RuntimeObservationError::InferencerCounts))?;
        for node in &replicas {
            working = working
                .checked_add(node.as_str().len())
                .ok_or_else(|| Report::new(RuntimeObservationError::InferencerCounts))?;
        }
        let working =
            u64::try_from(working).change_context(RuntimeObservationError::InferencerCounts)?;
        let charge = self
            .inner
            .executor
            .reserve(MemoryClass::Bulk, working)
            .await
            .change_context(RuntimeObservationError::InferencerExecution)?;
        let runtime = self.clone();
        self.inner
            .executor
            .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                runtime.inner.state_replication_routing.describe_inferencer(
                    &target,
                    expected,
                    bytes_per_branch,
                    replicas,
                    cancellation,
                )
            })
            .await
            .change_context(RuntimeObservationError::InferencerExecution)?
    }
}
