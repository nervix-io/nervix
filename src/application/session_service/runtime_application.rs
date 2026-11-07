//! Applying the committed runtime state on this node: installing the newest admitted revision,
//! waiting for the cluster to prepare it and make it ready, and recording the local progress each
//! step reaches.
//!
//! Layer: edges.
//!
//! - **Owns.** One application of the newest admitted runtime state: the handles it installs with,
//!   the bound that ends its waits, and the prepared and ready phases it records.
//! - **Depends on.** The runtime and its admission, the registry, the cluster view, consensus for
//!   the committed state, the HTTPS listener's certificates and the revision barriers of command
//!   completion.
//! - **Must not know.** How a session request reaches it or how its outcome is presented.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::{Report, ResultExt as _};
use futures_util::future::BoxFuture;
use nervix_consensus::Observer;
use nervix_interconnect::{ApplicationProgressPhase, Transport};
use nervix_primitives::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    application::{
        completion::{
            ApplicationProgressState, ApplicationRevisionParticipants, ApplicationRevisionPhase,
            BarrierNode, wait_for_application_revision,
        },
        runtime_admission::RuntimeAdmission,
        scheduling::RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
        tls::HttpsListenerCertificates,
    },
    cluster,
    registry::Registry,
    runtime::{ApplicationWait, Runtime},
};

/// The node-local handles that install the newest admitted runtime state.
#[derive(Clone, Copy)]
pub(in crate::application) struct RuntimeStateApplication<'a> {
    pub(in crate::application) runtime: &'a Runtime,
    pub(in crate::application) https_certificates: &'a HttpsListenerCertificates,
    pub(in crate::application) cluster: &'a cluster::ClusterHandle,
    pub(in crate::application) interconnect: &'a Transport,
    pub(in crate::application) registry: &'a Registry,
    pub(in crate::application) consensus: &'a Observer,
    pub(in crate::application) progress: &'a ApplicationProgressState,
    pub(in crate::application) admission: &'a RuntimeAdmission,
    pub(in crate::application) shutdown: &'a CancellationToken,
}

/// What ends the waits of one application of the committed runtime state, beside the budget the
/// cluster gives each revision the application installs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum ApplicationBound {
    /// The cluster's budget alone: peer-failure detection followed by readiness propagation.
    ClusterBudget,
    /// The cluster's budget, or `deadline` when it comes first: a caller that must not wait for
    /// the cluster past its own deadline, as a quiesced backup cut applying its pause. The
    /// deadline ends only the application's waits. A local installation or activation that began
    /// before it finishes, so this node never holds part of a revision, and every node keeps
    /// applying the newest committed state through its own reconciliation.
    Deadline(nervix_primitives::time::Instant),
}

impl ApplicationBound {
    /// Resolves once the bound ends the application's current `wait`, with the report of the wait
    /// it ended. It never resolves for the cluster's budget alone, which each of the application's
    /// cluster-wide waits applies itself.
    pub(super) async fn ends(self, wait: ApplicationWait) -> Report<crate::runtime::RuntimeError> {
        match self {
            Self::ClusterBudget => std::future::pending().await,
            Self::Deadline(deadline) => {
                let timer = nervix_primitives::time::sleep_until(deadline);
                tokio::pin!(timer);
                std::future::poll_fn(|context| {
                    // The timer rounds up to a millisecond tick. A ready installation must not
                    // win after the actual deadline, including when it elapsed between polls.
                    if nervix_primitives::time::Instant::now() >= deadline {
                        return std::task::Poll::Ready(());
                    }
                    timer.as_mut().poll(context)
                })
                .await;
                Report::new(crate::runtime::RuntimeError::ApplicationDeadline { wait })
            }
        }
    }
}

/// Apply the newest admitted runtime state with a heap-backed future so command execution keeps a
/// bounded stack regardless of the preparation and readiness paths active inside this operation.
/// `bound` ends the application's waits; see [`ApplicationBound`].
pub(in crate::application) fn apply_current_cluster_runtime_state(
    application: RuntimeStateApplication<'_>,
    bound: ApplicationBound,
) -> BoxFuture<'_, error_stack::Result<(), crate::runtime::RuntimeError>> {
    let RuntimeStateApplication {
        runtime,
        https_certificates,
        cluster,
        interconnect,
        registry,
        consensus,
        progress,
        admission,
        shutdown,
    } = application;
    let barrier_node = BarrierNode {
        cluster,
        consensus,
        interconnect,
        progress,
    };
    Box::pin(async move {
        let local_node_id = consensus.local_node_id();
        loop {
            nervix_primitives::task::consume_budget().await;
            let installation = admission.begin_installation(shutdown);
            tokio::pin!(installation);
            let installation = nervix_primitives::select! {
                biased;
                ended = bound.ends(ApplicationWait::Installation) => return Err(ended),
                installation = &mut installation => installation,
            };
            let Some(installation) = installation else {
                return Ok(());
            };
            #[cfg(feature = "testing")]
            runtime
                .pause_runtime_installation_if_armed(local_node_id)
                .await;
            let Some(state) = admission.runtime_state(consensus, shutdown).await else {
                return Ok(());
            };
            let Some(parameters) = state.parameters else {
                drop(installation);
                nervix_primitives::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = consensus.wait_for_runtime_revision_after(state.revision) => continue,
                }
            };
            if let Some(construction) = runtime.construction_parameters() {
                match construction.catch_up(parameters).change_context(
                    crate::runtime::RuntimeError::ClusterParameterPublicationMismatch {
                        revision: parameters.revision.get(),
                    },
                )? {
                    nervix_models::ClusterParameterCatchUp::Ready => {}
                    nervix_models::ClusterParameterCatchUp::Pending => {
                        drop(installation);
                        nervix_primitives::select! {
                            _ = shutdown.cancelled() => return Ok(()),
                            _ = consensus.wait_for_runtime_revision_after(state.revision) => continue,
                        }
                    }
                }
            }
            cluster.publish_cluster_parameters(parameters, false).await;
            runtime.install_cluster_parameters(parameters.values);
            registry.install_cluster_parameters(parameters.values);
            #[cfg(feature = "testing")]
            if runtime.take_runtime_installation_refusal(local_node_id) {
                return Err(Report::new(
                    crate::runtime::RuntimeError::RuntimeInstallationRefused {
                        node: local_node_id.clone(),
                    },
                ));
            }
            debug!(
                %local_node_id,
                revision = state.revision,
                "installing admitted cluster runtime state"
            );
            registry
                .synchronize_cluster_schedule(&state.schedule)
                .change_context(crate::runtime::RuntimeError::SynchronizeRegistry {
                    revision: state.revision,
                })?;
            let runtime_application = admission
                .apply_planned_cluster_state(runtime, local_node_id, &state)
                .await;
            // The listener presents the certificates of the same revision before this node reports
            // it prepared. A failed installation keeps the certificates already presented and is
            // reported to the command that changed them through its installation barrier.
            if let Err(error) = https_certificates
                .install(state.revision, &state.schedule)
                .await
            {
                warn!(
                    %local_node_id,
                    revision = state.revision,
                    error = format!("{error:#}"),
                    "failed to install the HTTPS listener TLS configuration"
                );
            }
            runtime_application.change_context(crate::runtime::RuntimeError::ApplyRevision {
                revision: state.revision,
            })?;
            #[cfg(feature = "testing")]
            runtime
                .pause_runtime_preparation_if_armed(local_node_id)
                .await;
            cluster.publish_cluster_parameters(parameters, true).await;
            progress.advance(ApplicationProgressPhase::RuntimePrepared, state.revision);
            debug!(
                %local_node_id,
                revision = state.revision,
                "local runtime revision prepared"
            );
            drop(installation);

            let node_unavailability_timeout = cluster.node_unavailability_timeout();
            // Applying a revision gets one start-time operation budget: peer-failure detection followed
            // by readiness propagation. A peer that disconnects after this starts has only the remaining
            // portion of that budget.
            let deadline_overflow = || {
                Report::new(
                    crate::runtime::RuntimeError::RuntimeRevisionReadinessDeadlineOverflow {
                        node_unavailability_timeout,
                        readiness_propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
                    },
                )
            };
            let Some(readiness_timeout) = node_unavailability_timeout
                .checked_add(RUNTIME_REVISION_READINESS_PROPAGATION_BOUND)
            else {
                return Err(deadline_overflow());
            };
            let Some(deadline) =
                nervix_primitives::time::Instant::now().checked_add(readiness_timeout)
            else {
                return Err(deadline_overflow());
            };
            let preparation = wait_for_application_revision(
                barrier_node,
                state.revision,
                ApplicationRevisionPhase::RuntimePrepared,
                ApplicationRevisionParticipants::EveryLiveNode,
                deadline,
            );
            tokio::pin!(preparation);
            let supersession = consensus.wait_for_runtime_revision_after(state.revision);
            tokio::pin!(supersession);
            let preparation_result = nervix_primitives::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                ended = bound.ends(ApplicationWait::Preparation {
                    revision: state.revision,
                }) => return Err(ended),
                newer_revision = &mut supersession => {
                    let Some(newer_revision) = newer_revision else {
                        return Ok(());
                    };
                    debug!(
                        %local_node_id,
                        revision = state.revision,
                        newer_revision,
                        "superseding runtime revision during cluster preparation"
                    );
                    continue;
                }
                result = &mut preparation => result,
            };
            if let Err(timeout) = preparation_result {
                let current_revision = consensus.current_runtime_revision().await;
                if current_revision > state.revision {
                    debug!(
                        %local_node_id,
                        revision = state.revision,
                        newer_revision = current_revision,
                        "superseding runtime revision at cluster preparation deadline"
                    );
                    continue;
                }
                return Err(Report::new(
                    crate::runtime::RuntimeError::RuntimeRevisionPreparation {
                        revision: state.revision,
                        pending_nodes: timeout.pending_nodes,
                    },
                ));
            }
            debug!(
                %local_node_id,
                revision = state.revision,
                "cluster runtime revision prepared"
            );

            let activation = admission.begin_installation(shutdown);
            tokio::pin!(activation);
            let activation = nervix_primitives::select! {
                biased;
                ended = bound.ends(ApplicationWait::Activation {
                    revision: state.revision,
                }) => return Err(ended),
                activation = &mut activation => activation,
            };
            let Some(activation) = activation else {
                return Ok(());
            };
            let current_revision = consensus.current_runtime_revision().await;
            if current_revision > state.revision {
                debug!(
                    %local_node_id,
                    revision = state.revision,
                    newer_revision = current_revision,
                    "superseding runtime revision before source activation"
                );
                continue;
            }
            runtime
                .start_running_domain_ingestors()
                .await
                .change_context(crate::runtime::RuntimeError::StartIngestors {
                    revision: state.revision,
                })?;
            debug!(
                %local_node_id,
                revision = state.revision,
                "started running-domain ingestors"
            );
            progress.advance(ApplicationProgressPhase::RuntimeReady, state.revision);
            drop(activation);
            let readiness = async {
                // A test build can hold this wait while the revision pauses a domain, as a cluster
                // slow to apply a backup cut's pause would; the revision that resumes it passes.
                #[cfg(feature = "testing")]
                runtime
                    .pause_backup_cut_application_if_armed(&state.domains)
                    .await;
                wait_for_application_revision(
                    barrier_node,
                    state.revision,
                    ApplicationRevisionPhase::RuntimeReady,
                    ApplicationRevisionParticipants::EveryLiveNode,
                    deadline,
                )
                .await
            };
            tokio::pin!(readiness);
            let supersession = consensus.wait_for_runtime_revision_after(state.revision);
            tokio::pin!(supersession);
            let readiness_result = nervix_primitives::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                ended = bound.ends(ApplicationWait::Readiness {
                    revision: state.revision,
                }) => return Err(ended),
                newer_revision = &mut supersession => {
                    let Some(newer_revision) = newer_revision else {
                        return Ok(());
                    };
                    debug!(
                        %local_node_id,
                        revision = state.revision,
                        newer_revision,
                        "superseding runtime revision during cluster readiness"
                    );
                    continue;
                }
                result = &mut readiness => result,
            };
            if let Err(timeout) = readiness_result {
                let current_revision = consensus.current_runtime_revision().await;
                if current_revision > state.revision {
                    debug!(
                        %local_node_id,
                        revision = state.revision,
                        newer_revision = current_revision,
                        "superseding runtime revision at cluster readiness deadline"
                    );
                    continue;
                }
                return Err(Report::new(
                    crate::runtime::RuntimeError::RuntimeRevisionReadiness {
                        revision: state.revision,
                        pending_nodes: timeout.pending_nodes,
                    },
                ));
            }
            debug!(
                %local_node_id,
                revision = state.revision,
                "cluster runtime revision ready"
            );
            return Ok(());
        }
    })
}
