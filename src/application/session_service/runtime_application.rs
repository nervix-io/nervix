//! Applying the committed runtime state on this node: installing the newest admitted revision,
//! waiting for the cluster to prepare it and make it ready, and recording the local progress each
//! step reaches.
//!
//! Layer: edges.
//!
//! - **Owns.** One application of the newest admitted runtime state: the installation and
//!   activation sections it runs under the runtime admission's turn, the bound that ends its
//!   waits, and the prepared and ready phases it records.
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
use nervix_consensus::ConsensusRuntimeState;
use nervix_interconnect::ApplicationProgressPhase;
use nervix_models::ClusterParameterRecord;
use tracing::{debug, warn};

use super::SessionServiceImpl;
use crate::{
    application::completion::{
        ApplicationRevisionParticipants, ApplicationRevisionPhase, BarrierNode,
        wait_for_application_revision,
    },
    runtime::ApplicationWait,
};

/// What ends the waits of one application of the committed runtime state, beside the budget the
/// cluster gives each revision the application installs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum ApplicationBound {
    /// The cluster's budget alone: peer-failure detection followed by readiness propagation.
    ClusterBudget,
    /// The cluster's budget, or `deadline` when it comes first: a caller that must not wait for
    /// the cluster past its own deadline, as a quiesced backup cut applying its pause. The
    /// deadline ends only the application's waits. A local installation or activation that began
    /// before it runs to its end in a task of the runtime admission, so this node never holds part
    /// of a revision, and every node keeps applying the newest committed state through its own
    /// reconciliation.
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

/// Where one installation section leaves the application that began it.
enum Installation {
    /// Shutdown began before the section read the committed state.
    ShuttingDown,
    /// Nothing of the committed state can be installed before a revision after this one is
    /// committed: its cluster parameters are not committed yet, or this node's construction has
    /// not caught up with them.
    AwaitingRevisionAfter(u64),
    /// This node installed the committed state and recorded it prepared. The revision is boxed:
    /// it is hundreds of bytes, and the other outcomes carry next to nothing.
    Prepared(Box<PreparedRevision>),
}

/// The committed state an installation section installed and recorded prepared, with the cluster
/// parameters it installed with.
struct PreparedRevision {
    state: ConsensusRuntimeState,
    parameters: ClusterParameterRecord,
}

/// Where one activation section leaves the application that began it.
enum Activation {
    /// A newer revision was committed before the section started any ingestor.
    Superseded,
    /// This node started the ingestors of the prepared state's running domains and recorded it
    /// ready. The state is boxed: it is hundreds of bytes, and the other outcome carries nothing.
    Ready(Box<ConsensusRuntimeState>),
}

impl SessionServiceImpl {
    /// Applies the newest admitted runtime state with a heap-backed future, so command execution
    /// keeps a bounded stack whatever preparation and readiness paths run inside it. `bound` ends
    /// the application's waits; see [`ApplicationBound`]. Each installation and activation it
    /// begins runs to its end in a task of the runtime admission, whatever happens to this future:
    /// a caller that drops it only stops waiting.
    pub(super) fn apply_admitted_runtime_state(
        &self,
        bound: ApplicationBound,
    ) -> BoxFuture<'_, error_stack::Result<(), crate::runtime::RuntimeError>> {
        Box::pin(async move {
            let inner = &self.inner;
            let shutdown = &inner.drain_support_shutdown;
            let consensus = &inner.consensus;
            let local_node_id = consensus.local_node_id();
            let barrier_node = BarrierNode {
                retry_interval: inner.runtime_admission.retry_interval() / 2,
                cluster: &inner.cluster,
                consensus,
                interconnect: &inner.interconnect,
                progress: &inner.application_progress,
            };
            loop {
                nervix_primitives::task::consume_budget().await;
                let installation = inner.runtime_admission.begin_installation(shutdown);
                tokio::pin!(installation);
                let installation = nervix_primitives::select! {
                    biased;
                    ended = bound.ends(ApplicationWait::Installation) => return Err(ended),
                    installation = &mut installation => installation,
                };
                let Some(installation) = installation else {
                    return Ok(());
                };
                let Some(installed) = installation
                    .run(self.clone().install_admitted_state())
                    .await
                else {
                    return Ok(());
                };
                let (state, parameters) = match installed? {
                    Installation::ShuttingDown => return Ok(()),
                    Installation::AwaitingRevisionAfter(revision) => {
                        nervix_primitives::select! {
                            _ = shutdown.cancelled() => return Ok(()),
                            _ = consensus.wait_for_runtime_revision_after(revision) => continue,
                        }
                    }
                    Installation::Prepared(prepared) => {
                        let PreparedRevision { state, parameters } = *prepared;
                        (state, parameters)
                    }
                };

                let readiness_propagation_bound =
                    parameters.values.runtime_readiness_propagation_bound();
                let node_unavailability_timeout = inner.cluster.node_unavailability_timeout();
                // Applying a revision gets one start-time operation budget: peer-failure detection
                // followed by readiness propagation. A peer that disconnects after this starts has
                // only the remaining portion of that budget.
                let deadline_overflow = || {
                    Report::new(
                        crate::runtime::RuntimeError::RuntimeRevisionReadinessDeadlineOverflow {
                            node_unavailability_timeout,
                            readiness_propagation_bound,
                        },
                    )
                };
                let Some(readiness_timeout) =
                    node_unavailability_timeout.checked_add(readiness_propagation_bound)
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
                    return Err(timeout.preparation_error(state.revision));
                }
                debug!(
                    %local_node_id,
                    revision = state.revision,
                    "cluster runtime revision prepared"
                );

                let revision = state.revision;
                let activation = inner.runtime_admission.begin_installation(shutdown);
                tokio::pin!(activation);
                let activation = nervix_primitives::select! {
                    biased;
                    ended = bound.ends(ApplicationWait::Activation { revision }) => return Err(ended),
                    activation = &mut activation => activation,
                };
                let Some(activation) = activation else {
                    return Ok(());
                };
                let Some(activated) = activation
                    .run(self.clone().activate_prepared_state(state))
                    .await
                else {
                    return Ok(());
                };
                let state = match activated? {
                    Activation::Superseded => continue,
                    Activation::Ready(state) => *state,
                };
                let readiness = async {
                    // A test build can hold this wait while the revision pauses a domain, as a
                    // cluster slow to apply a backup cut's pause would; the revision that resumes
                    // it passes.
                    #[cfg(feature = "testing")]
                    inner
                        .runtime
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
                    return Err(timeout.readiness_error(state.revision));
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

    /// One installation section, run under the installation turn its application took: it reads
    /// the newest admitted state, installs its cluster parameters, registry schedule, runtime
    /// revision and listener certificates, and records the revision prepared. It owns its service
    /// handle, so the runtime admission's task runs it to its end whatever happens to the
    /// application that began it.
    async fn install_admitted_state(
        self,
    ) -> error_stack::Result<Installation, crate::runtime::RuntimeError> {
        let inner = &self.inner;
        let runtime = &inner.runtime;
        let consensus = &inner.consensus;
        let local_node_id = consensus.local_node_id();
        #[cfg(feature = "testing")]
        runtime
            .pause_runtime_installation_if_armed(local_node_id)
            .await;
        let Some(state) = inner
            .runtime_admission
            .runtime_state(consensus, &inner.drain_support_shutdown)
            .await
        else {
            return Ok(Installation::ShuttingDown);
        };
        let Some(parameters) = state.parameters else {
            return Ok(Installation::AwaitingRevisionAfter(state.revision));
        };
        if let Some(construction) = runtime.construction_parameters() {
            match construction.catch_up(parameters).change_context(
                crate::runtime::RuntimeError::ClusterParameterPublicationMismatch {
                    revision: parameters.revision.get(),
                },
            )? {
                nervix_models::ClusterParameterCatchUp::Ready => {}
                nervix_models::ClusterParameterCatchUp::Pending => {
                    return Ok(Installation::AwaitingRevisionAfter(state.revision));
                }
            }
        }
        inner
            .cluster
            .publish_cluster_parameters(parameters, false)
            .await;
        inner
            .interconnect
            .install_cluster_parameters(parameters.values);
        runtime.install_cluster_parameters(parameters.values);
        inner.registry.install_cluster_parameters(parameters.values);
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
        inner
            .registry
            .synchronize_cluster_schedule(&state.schedule)
            .change_context(crate::runtime::RuntimeError::SynchronizeRegistry {
                revision: state.revision,
            })?;
        let runtime_application = inner
            .runtime_admission
            .apply_planned_cluster_state(runtime, local_node_id, &state)
            .await;
        // The listener presents the certificates of the same revision before this node reports it
        // prepared. A failed installation keeps the certificates already presented and is reported
        // to the command that changed them through its installation barrier.
        if let Err(error) = inner
            .https_certificates
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
        if let Err(error) = &runtime_application {
            // The barriers waiting for this revision read why this node did not prepare it.
            inner
                .application_progress
                .fail_runtime(state.revision, format!("{error:#}"));
        }
        runtime_application.change_context(crate::runtime::RuntimeError::ApplyRevision {
            node: local_node_id.clone(),
            revision: state.revision,
        })?;
        #[cfg(feature = "testing")]
        runtime
            .pause_runtime_preparation_if_armed(local_node_id)
            .await;
        inner
            .cluster
            .publish_cluster_parameters(parameters, true)
            .await;
        inner
            .application_progress
            .advance(ApplicationProgressPhase::RuntimePrepared, state.revision);
        debug!(
            %local_node_id,
            revision = state.revision,
            "local runtime revision prepared"
        );
        Ok(Installation::Prepared(Box::new(PreparedRevision {
            state,
            parameters,
        })))
    }

    /// One activation section, run under the installation turn its application took once the
    /// cluster prepared `state`: unless a newer revision was committed meanwhile, it starts the
    /// ingestors of the running domains and records the revision ready. Like an installation
    /// section, it runs to its end whatever happens to the application that began it.
    async fn activate_prepared_state(
        self,
        state: ConsensusRuntimeState,
    ) -> error_stack::Result<Activation, crate::runtime::RuntimeError> {
        let inner = &self.inner;
        let local_node_id = inner.consensus.local_node_id();
        let current_revision = inner.consensus.current_runtime_revision().await;
        if current_revision > state.revision {
            debug!(
                %local_node_id,
                revision = state.revision,
                newer_revision = current_revision,
                "superseding runtime revision before source activation"
            );
            return Ok(Activation::Superseded);
        }
        inner
            .runtime
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
        // A test build can hold this node's readiness for a revision that runs a domain, after its
        // ingestors started, as a node slow to become ready would.
        #[cfg(feature = "testing")]
        inner
            .runtime
            .pause_runtime_readiness_if_armed(local_node_id, &state.domains)
            .await;
        inner
            .application_progress
            .advance(ApplicationProgressPhase::RuntimeReady, state.revision);
        Ok(Activation::Ready(Box::new(state)))
    }

    /// Holds this node's background reconciliation before its next attempt while a scenario holds
    /// it once the committed state holds a domain stopped, as a node slow to apply the end of a
    /// stop would. Every other application on the node still applies the committed state.
    #[cfg(feature = "testing")]
    pub(in crate::application) async fn hold_background_reconciliation_if_armed(&self) {
        let faults = self.inner.runtime.fault_injection();
        let local_node_id = self.inner.consensus.local_node_id();
        let Some(domain) = faults.background_reconciliation_hold(local_node_id) else {
            return;
        };
        let Some(committed) = self.inner.consensus.current_domain(&domain).await else {
            return;
        };
        if let nervix_models::DomainStatus::Stopped = committed.status {
            faults.hold_background_reconciliation(local_node_id).await;
        }
    }
}
