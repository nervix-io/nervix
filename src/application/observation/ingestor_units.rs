//! Reading one ingestor unit from the node that owns its intake.
//!
//! Layer: control plane.
//! - **Owns.** Local and remote unit status and metric collection for ingestor inspection.
//! - **Depends on.** Typed unit placements, runtime inspection and the interconnect.
//! - **Must not know.** Source tasks, connector drivers or record delivery.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use error_stack::{Report, ResultExt as _};
use nervix_interconnect::DescribeIngestorRequest as RemoteDescribeIngestorRequest;
use nervix_models::{DomainName, IngestorName, ModelKind, ModelName, NodeRef, ScheduledNode};

use super::{
    super::{
        describe_output::runtime_ingestor_describe_from_envelope,
        session_service::SessionServiceImpl,
    },
    ObservationError,
};
use crate::runtime::IngestorDescribe as RuntimeIngestorDescribe;

impl SessionServiceImpl {
    /// What the node that executes an ingestor reports about it, and its metrics. An ingestor no
    /// node executes reports as stopped.
    pub(in crate::application) async fn ingestor_summary(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        ingestor_node: &ScheduledNode,
        unit: nervix_models::Unit,
    ) -> error_stack::Result<(RuntimeIngestorDescribe, Vec<String>), ObservationError> {
        let local_node_id = self.inner.consensus.local_node_id();
        if ingestor_node.unit_executes_on(unit, local_node_id) {
            let summary = self
                .inner
                .runtime
                .describe_local_ingestor(domain, ingestor, unit)
                .change_context(ObservationError::LocalDescribe {
                    domain: domain.clone(),
                    entity: NodeRef::new(ModelKind::Ingestor, ModelName::from(ingestor)),
                })?;
            let metrics = self
                .inner
                .runtime
                .describe_ingestor_unit_metrics(domain, ingestor, unit);
            return Ok((summary, metrics));
        }
        let Some(owner) = ingestor_node
            .unit(unit)
            .and_then(|placement| placement.execution_node())
        else {
            let summary = RuntimeIngestorDescribe {
                running: false,
                ready: false,
                quiesce_state: None,
                quiesce_counters: Default::default(),
                transport_redeliveries_total: 0,
                decode_rejections_total: 0,
                last_decode_rejection: None,
                oversized_payloads_total: 0,
                memory_backpressure_paused: self
                    .inner
                    .runtime
                    .ingestors_paused_for_memory_pressure(),
                transient_error: None,
                reconnect_backoff: None,
                reconnect_wait_millis: None,
                kafka_domain_offsets: None,
                client_producers: None,
            };
            let metrics = self
                .inner
                .runtime
                .describe_ingestor_unit_metrics(domain, ingestor, unit);
            return Ok((summary, metrics));
        };
        let answered = self
            .inner
            .interconnect
            .request(
                owner,
                RemoteDescribeIngestorRequest {
                    domain: domain.clone(),
                    name: ingestor.clone(),
                    unit,
                },
            )
            .await;
        match answered {
            Ok(Ok(summary)) => Ok(runtime_ingestor_describe_from_envelope(summary)),
            Ok(Err(failure)) => Err(Report::new(ObservationError::RemoteDescribeFailure {
                failure,
            })),
            Err(error) => Err(
                error.change_context(ObservationError::RemoteDescribeRequest {
                    domain: domain.clone(),
                    entity: NodeRef::new(ModelKind::Ingestor, ModelName::from(ingestor)),
                    node: owner.clone(),
                }),
            ),
        }
    }
}
