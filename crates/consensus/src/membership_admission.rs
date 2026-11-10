//! A learner's replication admission before its promotion to voter.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The blocking learner catch-up operation used by membership reconciliation.
//! - **Depends on.** OpenRaft's membership and replication state and the shared node identity.
//! - **Must not know.** Graph placement, licensing policy or how an admission error is reported.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns learner replication admission before voter promotion"
    )
)]

use openraft::{
    BasicNode,
    error::{ClientWriteError, RaftError},
    raft::ClientWriteResponse,
};

use crate::{ClusterNodeName, NervixRaft, TypeConfig};

#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        library,
        reason = "preserves OpenRaft's learner write result for the consensus reporting boundary"
    )
)]
pub(super) async fn catch_up_learner(
    raft: &NervixRaft,
    node: ClusterNodeName,
    endpoint: String,
) -> Result<ClientWriteResponse<TypeConfig>, RaftError<TypeConfig, ClientWriteError<TypeConfig>>> {
    raft.add_learner(node, BasicNode::new(endpoint), true).await
}
