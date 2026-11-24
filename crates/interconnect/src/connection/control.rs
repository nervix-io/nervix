//! Typed control requests over an admitted HTTP/2 connection lease.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Leasing a control request's stream, checking an optional authenticated process
//!   fence on it, and encoding one control round trip on that lease.
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

use super::{CONTROL_PATH, RawRequest, StreamLease, TransportState};
use crate::{ControlEnvelope, PoolClass, RequestSubquota, TransportError, wire};

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
    /// Leases a stream of `class` and `subquota` toward `node_id` by `deadline`, and refuses a
    /// replacement process when `fence` names the one the request is meant for.
    ///
    /// A request of a reserved kind leases its stream before it encodes its payload and keeps it
    /// until its answer is decoded. The payload's copy goes uncharged while the envelope's encoding
    /// waits for room, and so does the answer's payload while its typed decoding waits. No place of
    /// the incoming queue bounds those copies for a reserved kind, so the streams the peer's
    /// connection has bound them. A request holding such a place leases its stream only to
    /// exchange.
    pub(crate) async fn lease_control<F: PeerProcessFence>(
        &self,
        node_id: &ClusterNodeName,
        class: PoolClass,
        subquota: RequestSubquota,
        deadline: Instant,
        fence: F,
    ) -> Result<StreamLease, Report<TransportError>> {
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
        Ok(lease)
    }

    /// Sends `control` on `lease` and reads its answer, both by `deadline`.
    pub(crate) async fn exchange_control(
        &self,
        lease: &StreamLease,
        control: ControlEnvelope,
        deadline: Instant,
    ) -> Result<wire::Decoded<ControlEnvelope>, Report<TransportError>> {
        let class = control.pool_class();
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
        // The response carries work the peer already did, so a full class holds it back until the
        // request's own deadline rather than discarding it. It was read with the room its decoding
        // takes, so the decoding waits for nothing behind it.
        wire::decode_rkyv_within::<ControlEnvelope>(
            &self.executor,
            class.cpu_class(),
            response.bytes,
            response.decoding,
        )
        .await
    }

    /// The instant a request given `timeout` stops waiting.
    pub(crate) fn request_deadline(
        &self,
        timeout: Duration,
    ) -> Result<Instant, Report<TransportError>> {
        Instant::now().checked_add(timeout).ok_or_else(|| {
            Report::new(TransportError::InvalidOptions {
                reason: "request deadline exceeds the monotonic clock range".to_string(),
            })
        })
    }
}
