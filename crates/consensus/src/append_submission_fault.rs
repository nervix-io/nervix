//! One failed append submission, armed before a test transfers leadership.
//!
//! Layer: engines and infrastructure; controls exist only in test builds.
//! - **Owns.** One target's armed submission failure and the request that consumed it.
//! - **Depends on.** Consensus request identities and asynchronous synchronization.
//! - **Must not know.** Elections, graph execution or how a client recovers.

#[cfg(any(test, feature = "testing"))]
use nervix_models::ClusterNodeName;

#[cfg(any(test, feature = "testing"))]
use crate::{AppendEntriesRequest, TypeConfig};

#[cfg(not(any(test, feature = "testing")))]
#[derive(Clone, Default)]
pub(crate) struct AppendSubmissionFault(());

#[cfg(any(test, feature = "testing"))]
pub use enabled::{AppendSubmissionFailure, AppendSubmissionFault, AppendSubmissionFaults};

#[cfg(any(test, feature = "testing"))]
mod enabled {
    use nervix_primitives::sync::{Arc, Mutex};

    use super::*;
    use crate::LogIdOf;

    /// The actual nonempty request that consumed a node's one-shot submission failure.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct AppendSubmissionFailure {
        pub target: ClusterNodeName,
        pub previous: Option<LogIdOf>,
        pub last: LogIdOf,
        pub entries: usize,
    }

    #[derive(Clone, Debug, Default)]
    pub struct AppendSubmissionFaults {
        state: Arc<Mutex<SubmissionFaultState>>,
    }

    #[derive(Clone, Debug, Default)]
    pub enum AppendSubmissionFault {
        #[default]
        Inactive,
        Controlled {
            faults: AppendSubmissionFaults,
            target: ClusterNodeName,
        },
    }

    #[derive(Debug, Default)]
    enum SubmissionFaultState {
        #[default]
        Idle,
        Armed(ClusterNodeName),
        Delivered(AppendSubmissionFailure),
    }

    impl AppendSubmissionFaults {
        pub(crate) fn for_target(&self, target: &ClusterNodeName) -> AppendSubmissionFault {
            AppendSubmissionFault::Controlled {
                faults: self.clone(),
                target: target.clone(),
            }
        }

        pub(crate) async fn arm(&self, target: ClusterNodeName) {
            *self.state.lock().await = SubmissionFaultState::Armed(target);
        }

        pub(crate) async fn delivered(&self) -> Option<AppendSubmissionFailure> {
            let state = self.state.lock().await;
            match &*state {
                SubmissionFaultState::Delivered(failure) => Some(failure.clone()),
                SubmissionFaultState::Idle | SubmissionFaultState::Armed(_) => None,
            }
        }
    }

    impl AppendSubmissionFault {
        pub(crate) async fn take(&self, request: &AppendEntriesRequest<TypeConfig>) -> bool {
            let Self::Controlled { faults, target } = self else {
                return false;
            };
            let Some(last) = request.entries.last() else {
                return false;
            };
            let mut state = faults.state.lock().await;
            let SubmissionFaultState::Armed(armed) = &*state else {
                return false;
            };
            if armed != target {
                return false;
            }
            *state = SubmissionFaultState::Delivered(AppendSubmissionFailure {
                target: target.clone(),
                previous: request.prev_log_id.clone(),
                last: last.log_id.clone(),
                entries: request.entries.len(),
            });
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use openraft::{Entry, EntryPayload, Vote, vote::RaftLeaderId as _};

    use super::*;
    use crate::{CommittedLeaderIdOf, LogIdOf};

    #[nervix_primitives::test]
    async fn a_submission_failure_consumes_only_its_target_first_data_request() {
        let source = ClusterNodeName::parse("node-1").assured("valid fixture node");
        let target = ClusterNodeName::parse("node-2").assured("valid fixture node");
        let other = ClusterNodeName::parse("node-3").assured("valid fixture node");
        let previous = LogIdOf::new(
            CommittedLeaderIdOf::<TypeConfig>::new(1, source.clone()),
            263,
        );
        let last = LogIdOf::new(
            CommittedLeaderIdOf::<TypeConfig>::new(1, source.clone()),
            264,
        );
        let mut request = AppendEntriesRequest::<TypeConfig> {
            vote: Vote::new_committed(1, source),
            prev_log_id: Some(previous.clone()),
            entries: Vec::new(),
            leader_commit: Some(previous.clone()),
        };
        let faults = AppendSubmissionFaults::default();
        let fault = faults.for_target(&target);
        faults.arm(target.clone()).await;
        assert!(!fault.take(&request).await, "empty probes continue");
        request.entries.push(Entry {
            log_id: last.clone(),
            payload: EntryPayload::Blank,
        });
        assert!(
            !faults.for_target(&other).take(&request).await,
            "other followers continue"
        );
        assert!(faults.delivered().await.is_none());
        assert!(fault.take(&request).await);
        assert!(!fault.take(&request).await, "later traffic continues");
        assert_eq!(
            faults.delivered().await,
            Some(AppendSubmissionFailure {
                target,
                previous: Some(previous),
                last,
                entries: 1,
            })
        );
    }
}
