//! Admission and serialized installation of ownership-sensitive runtime execution.
//!
//! Layer: control plane.
//!
//! - **Owns.** The process-local linearizable catch-up proof and ordering of coherent runtime-state
//!   installations through local preparation after that proof.
//! - **Depends on.** Consensus observation, Tokio synchronization, and process shutdown.
//! - **Must not know.** Runtime graph internals, connector implementations, or scheduling policy.

use std::{sync::OnceLock, time::Duration};

use meticulous::ResultExt as _;
use nervix_consensus::{ConsensusRuntimeState, Observer};
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const ADMISSION_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// The process-start barrier and runtime-installation sequencer shared by every installation path.
pub(in crate::application) struct RuntimeAdmission {
    committed_log_index: OnceLock<u64>,
    attempt: Mutex<()>,
    installation: Mutex<()>,
}

impl RuntimeAdmission {
    pub(in crate::application) fn new() -> Self {
        Self {
            committed_log_index: OnceLock::new(),
            attempt: Mutex::new(()),
            installation: Mutex::new(()),
        }
    }

    /// Serialize coherent state capture, runtime installation, and local preparation.
    pub(in crate::application) async fn begin_installation(
        &self,
        shutdown: &CancellationToken,
    ) -> Option<MutexGuard<'_, ()>> {
        let installation = self.installation.lock();
        tokio::pin!(installation);
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => None,
            installation = &mut installation => Some(installation),
        }
    }

    /// Return state captured after admission, retrying independently of replicated value changes.
    pub(in crate::application) async fn runtime_state(
        &self,
        consensus: &Observer,
        shutdown: &CancellationToken,
    ) -> Option<ConsensusRuntimeState> {
        if shutdown.is_cancelled() {
            return None;
        }
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
        if shutdown.is_cancelled() {
            return None;
        }
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
