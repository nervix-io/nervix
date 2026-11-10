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
    error::{ClientWriteError, Fatal, RaftError},
    raft::ClientWriteResponse,
    type_config::async_runtime::watch::WatchReceiver as _,
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
    let response = raft
        .add_learner(node.clone(), BasicNode::new(endpoint), true)
        .await?;
    let mut metrics = raft.metrics();
    loop {
        {
            let observed = metrics.borrow_watched();
            let Some(peers) = observed.replication.as_ref() else {
                return Ok(response);
            };
            let caught_up = if let (Some(Some(matched)), Some(last)) =
                (peers.get(&node), observed.last_log_index)
            {
                last.saturating_sub(matched.index) <= raft.config().replication_lag_threshold
            } else {
                false
            };
            let still_admitting = observed.current_leader.as_ref() == Some(&observed.id)
                && observed
                    .membership_config
                    .membership()
                    .get_node(&node)
                    .is_some();
            // OpenRaft's lag allowance can include an absent match on a short log. An
            // unknown prefix proves no catch-up, even when its numeric distance fits.
            // Check the current lag too: the log can grow while confirmation waits. Preserve
            // the caller's write gate if this node stops leading or the learner leaves.
            if caught_up || !still_admitting {
                return Ok(response);
            }
        }
        nervix_primitives::task::consume_budget().await;
        metrics
            .changed()
            .await
            .map_err(|_| RaftError::Fatal(Fatal::Stopped))?;
    }
}

#[cfg(test)]
mod tests;
