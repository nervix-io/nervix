//! One-time admission of ownership-sensitive runtime execution after process startup.
//!
//! Layer: control plane.
//!
//! - **Owns.** The process-local proof that consensus established a linearizable read and applied
//!   its committed log boundary before runtime state may be installed.
//! - **Depends on.** Consensus observation, Tokio synchronization, and process shutdown.
//! - **Must not know.** Runtime graph internals, connector implementations, or scheduling policy.

use std::{sync::OnceLock, time::Duration};

use meticulous::ResultExt as _;
use nervix_consensus::{ConsensusRuntimeState, Observer};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const ADMISSION_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// The one process-start barrier shared by every path that can install cluster runtime state.
pub(in crate::application) struct RuntimeAdmission {
    committed_log_index: OnceLock<u64>,
    attempt: Mutex<()>,
}

impl RuntimeAdmission {
    pub(in crate::application) fn new() -> Self {
        Self {
            committed_log_index: OnceLock::new(),
            attempt: Mutex::new(()),
        }
    }

    /// Return state captured after admission, retrying independently of replicated value changes.
    pub(in crate::application) async fn runtime_state(
        &self,
        consensus: &Observer,
        shutdown: &CancellationToken,
    ) -> Option<ConsensusRuntimeState> {
        if self.committed_log_index.get().is_some() {
            return Some(consensus.current_runtime_state().await);
        }

        let attempt = self.attempt.lock();
        tokio::pin!(attempt);
        let _attempt = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return None,
            attempt = &mut attempt => attempt,
        };
        if self.committed_log_index.get().is_some() {
            return Some(consensus.current_runtime_state().await);
        }

        let mut reported_wait = false;
        loop {
            tokio::task::consume_budget().await;
            let admission = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return None,
                admission = consensus.admitted_runtime_state() => admission,
            };
            match admission {
                Ok(admission) => {
                    let committed_log_index = admission.committed_log_index();
                    self.committed_log_index.set(committed_log_index).assured(
                        "the serialized admission attempt rechecked the unset proof after locking",
                    );
                    info!(
                        committed_log_index,
                        "runtime execution admitted after linearizable consensus catch-up"
                    );
                    return Some(admission.into_runtime_state());
                }
                Err(error) => {
                    if !reported_wait {
                        warn!(
                            error = %error,
                            "runtime execution is waiting for linearizable consensus catch-up"
                        );
                        reported_wait = true;
                    }
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return None,
                        _ = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {}
                    }
                }
            }
        }
    }
}
