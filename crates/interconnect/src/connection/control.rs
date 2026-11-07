//! Typed control requests over an admitted HTTP/2 connection lease.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Encoding one control round trip and checking an optional authenticated process fence
//!   before the request is sent.
//! - **Depends on.** The parent connection's leases, bounded wire codecs and request admission.
//! - **Must not know.** Domain clocks, graph readiness or the caller's retry policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::time::Duration;

use error_stack::Report;
use nervix_models::ClusterNodeName;
use nervix_primitives::time::Instant;

use super::{CONTROL_PATH, RawRequest, TransportState};
use crate::{ControlEnvelope, RequestSubquota, TransportError, wire};

/// The caller chooses whether this operation names a node or an exact authenticated process.
/// The ordinary node operation retains no additional future state.
pub(crate) trait PeerProcessFence: Copy + Send + Sync {
    fn expected_epoch(self) -> Option<u64>;
}

impl PeerProcessFence for () {
    fn expected_epoch(self) -> Option<u64> {
        None
    }
}

impl PeerProcessFence for u64 {
    fn expected_epoch(self) -> Option<u64> {
        Some(self)
    }
}

impl TransportState {
    pub(crate) async fn round_trip_control<F: PeerProcessFence>(
        &self,
        node_id: &ClusterNodeName,
        control: ControlEnvelope,
        subquota: RequestSubquota,
        timeout_duration: Duration,
        fence: F,
    ) -> Result<wire::Decoded<ControlEnvelope>, Report<TransportError>> {
        let class = control.pool_class();
        let deadline = Instant::now()
            .checked_add(timeout_duration)
            .ok_or_else(|| TransportError::InvalidOptions {
                reason: "request deadline exceeds the monotonic clock range".to_string(),
            })?;
        let lease = self.lease(node_id, class, subquota, deadline).await?;
        if let Some(expected) = fence.expected_epoch() {
            let actual = lease.connection.peer_epoch;
            if expected != actual {
                return Err(Report::new(TransportError::PeerProcessChanged {
                    peer: node_id.clone(),
                    expected,
                    actual,
                }));
            }
        }
        let bytes = wire::encode_rkyv(
            &self.executor,
            class.memory_class(),
            class.cpu_class(),
            class.control_body_limit(&self.executor),
            control,
        )
        .await?;
        let response = lease
            .request_raw(
                self,
                RawRequest {
                    path: CONTROL_PATH,
                    body: Some(bytes),
                    response_class: class,
                    response_limit: class.control_body_limit(&self.executor),
                    timeout: deadline.saturating_duration_since(Instant::now()),
                    headers: &[],
                },
            )
            .await?;
        wire::decode_rkyv::<ControlEnvelope>(
            &self.executor,
            class.memory_class(),
            class.cpu_class(),
            response,
        )
        .await
    }
}
