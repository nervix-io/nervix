//! The node-to-leader messages a stopping node sends to move its own scheduled work.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The drain and cordon-release request a stopping node sends the current leader, the
//!   answers it can receive, and the pool, quota and deadline the request carries.
//! - **Depends on.** The typed request contract.
//! - **Must not know.** How a leader plans a drain, which work it moves, or when a node stops.

use std::time::Duration;

use rkyv::{Archive, Deserialize, Serialize};

use crate::InterconnectRequest;

/// What a stopping node asks the current leader to do about the node's own scheduled work.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum StoppingNodeDrainAction {
    /// Cordon the node and move its scheduled work to other nodes through planned ownership
    /// handoffs.
    Drain,
    /// Clear the cordon the node's own drain set.
    ReleaseCordon,
}

/// A stopping node's request to the current leader about its own scheduled work.
///
/// The request names no node. The leader acts for the node the request's authenticated connection
/// belongs to, so a node can drain only itself, and it needs no user credential to do so.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoppingNodeDrainRequest {
    pub action: StoppingNodeDrainAction,
}

/// What the node that received a [`StoppingNodeDrainRequest`] did.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum StoppingNodeDrainResponse {
    /// The answering node leads and completed the action. The report is its account of the
    /// action, for the stopping node's log.
    Completed { report: String },
    /// The answering node leads and could not complete the action. The report is its account of
    /// the failure, for the stopping node's log.
    Failed { report: String },
    /// The answering node does not lead, so it changed nothing.
    NotLeader,
}

impl InterconnectRequest for StoppingNodeDrainRequest {
    type Response = StoppingNodeDrainResponse;

    const NAME: &'static str = "stopping_node_drain";
    /// Longer than the default shutdown timeout. A stopping node bounds each request by what
    /// remains of its own drain timeout instead.
    const TIMEOUT: Duration = Duration::from_secs(60);
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
mod wire_properties {
    use meticulous::ResultExt as _;
    use nervix_execution::Executor;

    use super::*;
    use crate::request::RkyvMessage;

    /// One generated request action and one generated answer.
    #[derive(Debug, bolero::TypeGenerator)]
    struct DrainWireCase {
        release_cordon: bool,
        answer: DrainAnswerCase,
    }

    #[derive(Debug, bolero::TypeGenerator)]
    enum DrainAnswerCase {
        Completed(String),
        Failed(String),
        NotLeader,
    }

    impl DrainWireCase {
        fn request(&self) -> StoppingNodeDrainRequest {
            let action = if self.release_cordon {
                StoppingNodeDrainAction::ReleaseCordon
            } else {
                StoppingNodeDrainAction::Drain
            };
            StoppingNodeDrainRequest { action }
        }

        fn response(&self) -> StoppingNodeDrainResponse {
            match &self.answer {
                DrainAnswerCase::Completed(report) => StoppingNodeDrainResponse::Completed {
                    report: report.clone(),
                },
                DrainAnswerCase::Failed(report) => StoppingNodeDrainResponse::Failed {
                    report: report.clone(),
                },
                DrainAnswerCase::NotLeader => StoppingNodeDrainResponse::NotLeader,
            }
        }
    }

    /// Encodes `message` as the transport encodes a typed request payload or its answer, and
    /// decodes it the way the receiving node does.
    async fn round_trip<M>(executor: &Executor, message: M) -> M
    where
        M: RkyvMessage,
    {
        let class = StoppingNodeDrainRequest::CLASS;
        let (encoded, _reservation) = message
            .encode_rkyv(executor.clone(), class, class.payload_limit(executor))
            .await
            .assured("a bounded current message encodes within the command payload limit");
        let (decoded, _reservation) = M::decode_rkyv(executor.clone(), class, encoded)
            .await
            .assured("a message the transport encoded decodes");
        decoded
    }

    #[test]
    fn bolero_stopping_node_drain_messages_round_trip() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("the ordinary test runtime builds");
        bolero::check!()
            .with_iterations(128)
            .with_max_len(128)
            .with_type::<DrainWireCase>()
            .for_each(|case| {
                runtime.block_on(async {
                    let executor = Executor::default();
                    let request = case.request();
                    let decoded_request = round_trip(&executor, request.clone()).await;
                    assert_eq!(decoded_request, request);
                    let response = case.response();
                    let decoded_response = round_trip(&executor, response.clone()).await;
                    assert_eq!(decoded_response, response);
                });
            });
    }
}
