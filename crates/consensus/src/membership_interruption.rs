//! Controlled interruption between the two steps of a voter change.
//!
//! Layer: engines and infrastructure; controls exist only in test builds.
//! - **Owns.** One promotion's interruption after its joint entry is applied, and its witness.
//! - **Depends on.** The consensus owner, its storage pause and asynchronous synchronization.
//! - **Must not know.** Graph execution, discovery policy or client recovery.

use std::collections::BTreeSet;

use nervix_models::ClusterNodeName;
use openraft::{
    error::{ClientWriteError, RaftError},
    raft::ClientWriteResponse,
};

use crate::{NervixRaft, TypeConfig};

#[cfg(not(feature = "testing"))]
#[derive(Clone, Default)]
pub(crate) struct MembershipInterruption(());

#[cfg(not(feature = "testing"))]
impl MembershipInterruption {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            library,
            reason = "preserves OpenRaft's membership write result for the consensus reporting \
                      boundary"
        )
    )]
    pub(crate) async fn change(
        &self,
        raft: &NervixRaft,
        voters: BTreeSet<ClusterNodeName>,
    ) -> Result<ClientWriteResponse<TypeConfig>, RaftError<TypeConfig, ClientWriteError<TypeConfig>>>
    {
        raft.change_membership(voters, true).await
    }
}

#[cfg(feature = "testing")]
pub use enabled::{InterruptedMembership, MembershipInterruption};

#[cfg(feature = "testing")]
mod enabled {
    use meticulous::OptionExt as _;
    use nervix_primitives::sync::{Arc, Mutex};
    use openraft::{metrics::RaftMetrics, type_config::async_runtime::watch::WatchReceiver};

    use super::*;
    use crate::{StorageBoundary, StorageFault};

    /// The actual joint configuration and applied log entry left by the interruption.
    #[derive(Clone, Debug)]
    pub struct InterruptedMembership {
        pub configurations: Vec<BTreeSet<ClusterNodeName>>,
        pub log_index: u64,
    }

    #[derive(Clone, Debug, Default)]
    pub struct MembershipInterruption {
        inner: Arc<Mutex<InterruptionState>>,
    }

    #[derive(Debug, Default)]
    enum InterruptionState {
        #[default]
        Idle,
        Armed {
            joining: ClusterNodeName,
            storage: StorageFault,
        },
        Interrupted {
            joining: ClusterNodeName,
            witness: InterruptedMembership,
        },
    }

    impl MembershipInterruption {
        pub(crate) async fn arm(&self, joining: ClusterNodeName, storage: StorageFault) {
            *self.inner.lock().await = InterruptionState::Armed { joining, storage };
        }

        pub(crate) async fn witness(
            &self,
            joining: &ClusterNodeName,
        ) -> Option<InterruptedMembership> {
            let state = self.inner.lock().await;
            if let InterruptionState::Interrupted {
                joining: interrupted,
                witness,
            } = &*state
                && interrupted == joining
            {
                return Some(witness.clone());
            }
            None
        }

        #[cfg_attr(
            nervix_lint,
            nervix::error_boundary(
                library,
                reason = "preserves OpenRaft's membership write result, including the controlled \
                          pre-proposal refusal"
            )
        )]
        pub(crate) async fn change(
            &self,
            raft: &NervixRaft,
            voters: BTreeSet<ClusterNodeName>,
        ) -> Result<
            ClientWriteResponse<TypeConfig>,
            RaftError<TypeConfig, ClientWriteError<TypeConfig>>,
        > {
            let armed = {
                let mut state = self.inner.lock().await;
                if let InterruptionState::Armed { joining, .. } = &*state
                    && voters.contains(joining)
                {
                    std::mem::take(&mut *state)
                } else {
                    InterruptionState::Idle
                }
            };
            let InterruptionState::Armed { joining, storage } = armed else {
                return raft.change_membership(voters, true).await;
            };

            // The apply worker cannot answer the first step while this pause is held. Poll the
            // caller until both the pause and the proposed joint configuration are observed,
            // then stop polling it before releasing the worker. Its continuation would submit
            // the uniform step; dropping it after joint application refuses that continuation.
            let pause = storage.pause_next("apply".to_string(), StorageBoundary::AfterSync);
            let mut changing = std::pin::pin!(raft.change_membership(voters.clone(), true));
            nervix_primitives::select! {
                result = &mut changing => return result,
                result = async {
                    pause.entered().await;
                    self.wait_for_joint(raft, &voters, false).await
                } => { result?; }
            }
            pause.release();
            let applied = self.wait_for_joint(raft, &voters, true).await?;
            let membership = &applied.membership_config;
            let log_id = membership
                .log_id()
                .as_ref()
                .verified("a proposed joint membership has a log entry");
            let witness = InterruptedMembership {
                configurations: membership.membership().get_joint_config().clone(),
                log_index: log_id.index,
            };
            *self.inner.lock().await = InterruptionState::Interrupted { joining, witness };
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(
                openraft::error::ForwardToLeader::empty(),
            )))
        }

        #[cfg_attr(
            nervix_lint,
            nervix::error_boundary(
                library,
                reason = "a stopped OpenRaft metrics channel ends the controlled membership write \
                          as its library result"
            )
        )]
        async fn wait_for_joint(
            &self,
            raft: &NervixRaft,
            voters: &BTreeSet<ClusterNodeName>,
            applied: bool,
        ) -> Result<RaftMetrics<TypeConfig>, RaftError<TypeConfig, ClientWriteError<TypeConfig>>>
        {
            let mut metrics = raft.metrics();
            loop {
                nervix_primitives::task::consume_budget().await;
                let observed = metrics.borrow_watched().clone();
                let membership = &observed.membership_config;
                let observed_voters = membership.membership().voter_ids().collect::<BTreeSet<_>>();
                if membership.membership().get_joint_config().len() > 1
                    && &observed_voters == voters
                    && (!applied || observed.last_applied.as_ref() >= membership.log_id().as_ref())
                {
                    return Ok(observed);
                }
                metrics
                    .changed()
                    .await
                    .map_err(|_| RaftError::Fatal(openraft::error::Fatal::Stopped))?;
            }
        }
    }
}
