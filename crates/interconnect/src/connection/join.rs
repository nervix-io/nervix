//! The join contract one connection binding carries: this node's prerequisite record, the judgment
//! of a peer's, and how far this process has come in joining the cluster.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Building this node's record from its identity, credentials, executor limits and join
//!   terms; judging the record a dialed peer accepts with; answering a dialing peer that cannot run
//!   beside this node and letting that answer leave; and the join progress outbound connections
//!   move once.
//! - **Depends on.** The transport state, the bounded wire codec, the connection-binding messages
//!   and the vocabulary's prerequisite record.
//! - **Must not know.** Gossip, consensus admission, or what a node does when it is refused.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a connection binding is judged and the join progress moves while a connection \
                  is established or refused, outside recurring transport operations"
    )
)]

use std::future::poll_fn;

use bytes::Bytes;
use error_stack::Report;
use h2::server;
use http::StatusCode;
use nervix_execution::{CpuClass, MemoryClass};
use nervix_models::{ClusterNodeName, ConsensusFloors, NodePrerequisites, PrerequisiteMismatch};
use nervix_primitives::{sync::watch, time::timeout};
use tracing::{debug, warn};

use super::{BoundInboundConnection, TransportState, send_response};
use crate::{
    JoinProgress, JoinRefusal, TlsConfigBundle, TransportError,
    wire::{self, ConnectionAccepted, ConnectionAnswer, WIRE_CONTRACT},
};

/// How a valid connection hello was answered.
pub(super) enum InboundBindingAnswer {
    Bound(BoundInboundConnection),
    /// The dialing node cannot run beside this one; it has been told why, and the connection
    /// closes once that answer has left.
    Refused(Report<TransportError>),
}

impl TransportState {
    /// Everything this node brings to the cluster, with the trust root of `tls`.
    pub(super) fn prerequisites_with(&self, tls: &TlsConfigBundle) -> NodePrerequisites {
        let limits = self.executor.limits();
        NodePrerequisites {
            cluster_identity: self.cluster_id.clone(),
            trust_root: tls.trust_root(),
            wire_contract: WIRE_CONTRACT,
            consensus_floors: ConsensusFloors {
                largest_command_bytes: limits.command_bytes.as_u64(),
                largest_management_event_bytes: limits.management_event_bytes.as_u64(),
            },
            durability: self.join_terms.durability,
            timings: self.join_terms.timings,
        }
    }

    /// Everything this node brings to the cluster, with the trust root of its current credentials.
    pub(crate) fn local_prerequisites(&self) -> NodePrerequisites {
        self.prerequisites_with(&self.tls.current().bundle)
    }

    pub(crate) fn subscribe_join_progress(&self) -> watch::Receiver<JoinProgress> {
        self.join_progress.subscribe()
    }

    /// Records the first outbound connection this process completed, unless the join already ended.
    pub(super) fn note_joined(&self, peer: &ClusterNodeName) {
        self.join_progress.send_if_modified(|progress| {
            if *progress != JoinProgress::Joining {
                return false;
            }
            *progress = JoinProgress::Joined {
                through: peer.clone(),
            };
            true
        });
    }

    /// Records an outbound connection that ended in a prerequisite refusal, unless the join
    /// already ended, and reports it.
    pub(super) fn note_join_failure(&self, error: &TransportError) {
        let refusal = match error {
            TransportError::RefusedByPeer { peer, mismatch }
            | TransportError::IncompatiblePeer { peer, mismatch } => JoinRefusal {
                peer: peer.clone(),
                mismatch: mismatch.clone(),
            },
            _ => return,
        };
        warn!(
            peer = %refusal.peer,
            prerequisite = %refusal.mismatch.prerequisite(),
            refusal = %refusal.mismatch,
            "an outbound interconnect connection ended in a prerequisite refusal"
        );
        self.join_progress.send_if_modified(|progress| {
            if *progress != JoinProgress::Joining {
                return false;
            }
            *progress = JoinProgress::Refused(refusal);
            true
        });
    }

    /// The acceptance `peer` answered this node's hello with, once its record shows the two can
    /// run together. A refusal comes back seen from this node.
    pub(super) fn accepted_answer(
        peer: &ClusterNodeName,
        local: &NodePrerequisites,
        answer: ConnectionAnswer,
    ) -> Result<ConnectionAccepted, Report<TransportError>> {
        let accepted = match answer {
            ConnectionAnswer::Accepted(accepted) => accepted,
            ConnectionAnswer::Refused { mismatch } => {
                return Err(Report::new(TransportError::RefusedByPeer {
                    peer: peer.clone(),
                    mismatch: mismatch.reversed(),
                }));
            }
        };
        if accepted.node_id != *peer {
            return Err(Report::new(TransportError::InvalidHandshake(
                "addressed node identity differs".to_string(),
            )));
        }
        if let Err(mismatch) = local.verify_peer(&accepted.prerequisites) {
            return Err(Report::new(TransportError::IncompatiblePeer {
                peer: peer.clone(),
                mismatch,
            }));
        }
        Ok(accepted)
    }

    /// Tells the dialing `peer` why it cannot run beside this node, and returns the refusal this
    /// node reports for the binding.
    pub(super) async fn refuse_binding(
        &self,
        respond: server::SendResponse<Bytes>,
        peer: &ClusterNodeName,
        mismatch: PrerequisiteMismatch,
    ) -> Result<InboundBindingAnswer, Report<TransportError>> {
        let refusal = wire::encode_rkyv(
            &self.executor,
            MemoryClass::Management,
            CpuClass::Control,
            self.executor.limits().management_event_bytes.as_u64(),
            ConnectionAnswer::Refused {
                mismatch: mismatch.clone(),
            },
        )
        .await?;
        send_response(
            respond,
            StatusCode::OK,
            Some(refusal),
            self.options.progress_timeout,
        )
        .await?;
        Ok(InboundBindingAnswer::Refused(Report::new(
            TransportError::IncompatiblePeer {
                peer: peer.clone(),
                mismatch,
            },
        )))
    }

    /// Lets a refusal answer reach the dialing node before the connection closes: its frames leave
    /// only while the connection is polled, and the progress timeout bounds how long that takes.
    pub(super) async fn flush_refusal<T>(&self, connection: &mut server::Connection<T, Bytes>)
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        connection.graceful_shutdown();
        let closed = timeout(
            self.options.progress_timeout,
            poll_fn(|context| connection.poll_closed(context)),
        )
        .await;
        match closed {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                debug!(
                    ?error,
                    "a refused interconnect connection closed with an error"
                );
            }
            Err(_) => {
                debug!(
                    "a refused interconnect connection did not close within its progress timeout"
                );
            }
        }
    }
}
