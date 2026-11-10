//! Cluster bootstrap and membership reconciliation orchestration.
//!
//! Layer: control plane.
//! - **Owns.** The membership task's cadence, bootstrap, identity/parameter seeds and verdict publication.
//! - **Depends on.** Consensus administration, published licensing, cluster and shutdown handles.
//! - **Must not know.** Graph execution, quota arithmetic, cryptographic encoding or Plexus.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns membership orchestration")
)]

use std::time::Duration;

use nervix_consensus::{Administrator, Proposer};
use nervix_models::ClusterParameterValues;
use nervix_primitives::{
    sync::{Arc, CancellationToken, watch},
    task::{self, JoinHandle},
    time::sleep,
};
use tracing::warn;

use super::{
    cluster_identity, join_supervision,
    shutdown::{ShutdownCoordinator, ShutdownRequestOutcome},
};
use crate::{ConfiguredFaultInjection, cluster::ClusterHandle, licensing::PolicyHandle};

pub(super) struct MembershipReconciliation {
    pub cluster: Arc<ClusterHandle>,
    pub administrator: Administrator,
    pub proposer: Proposer,
    pub shutdown: CancellationToken,
    pub allow_bootstrap: bool,
    pub startup_delay: Duration,
    pub membership_interval: Duration,
    pub parameter_seeds: ClusterParameterValues,
    pub licensing: PolicyHandle,
    pub refusals: watch::Sender<Option<join_supervision::JoinRefusedError>>,
    pub coordinator: ShutdownCoordinator,
    pub fault: ConfiguredFaultInjection,
}

pub(super) fn spawn(arguments: MembershipReconciliation) -> JoinHandle<()> {
    let MembershipReconciliation {
        cluster: cluster_for_membership_reconcile,
        administrator: administrator_for_membership_reconcile,
        proposer: proposer_for_parameter_seeding,
        shutdown: membership_reconcile_shutdown,
        allow_bootstrap,
        startup_delay,
        membership_interval,
        parameter_seeds,
        licensing: membership_licensing,
        refusals: bootstrap_refusal_sender,
        coordinator: bootstrap_refusal_coordinator,
        fault: _fault_for_identity_seeding,
    } = arguments;
    task::spawn(async move {
        if membership_reconcile_shutdown
            .run_until_cancelled(sleep(startup_delay))
            .await
            .is_none()
        {
            return;
        }
        let mut initialized = false;
        loop {
            nervix_primitives::task::consume_budget().await;
            if membership_reconcile_shutdown.is_cancelled() {
                break;
            }
            if allow_bootstrap && !initialized {
                match administrator_for_membership_reconcile
                    .maybe_initialize(|usage| {
                        membership_licensing
                            .admit_snapshot(
                                proposer_for_parameter_seeding
                                    .observer()
                                    .licensing_snapshot(),
                                crate::licensing::QuotaName::ClusterNodes,
                                usage,
                                1,
                                false,
                            )
                            .map(|_| ())
                    })
                    .await
                {
                    Ok(did_initialize) => {
                        initialized = did_initialize;
                    }
                    Err(error) => {
                        if let Some(refusal) =
                            error.downcast_ref::<crate::licensing::LicensingRefusal>()
                        {
                            bootstrap_refusal_sender.send_replace(Some(
                                join_supervision::JoinRefusedError::Licensing(*refusal),
                            ));
                            match bootstrap_refusal_coordinator.request_stop() {
                                ShutdownRequestOutcome::Accepted(_)
                                | ShutdownRequestOutcome::AlreadyRequested(_) => {}
                            }
                            break;
                        }
                        warn!(target: "nervix_server::application", error = %format_args!("{error:#}"), "raft bootstrap attempt failed");
                    }
                }
            }
            if let Err(error) = cluster_identity::seed(
                &proposer_for_parameter_seeding,
                #[cfg(feature = "testing")]
                &_fault_for_identity_seeding,
            )
            .await
            {
                warn!(target: "nervix_server::application", error = %format_args!("{error:#}"), "failed to seed cluster identity");
            }
            if allow_bootstrap
                && proposer_for_parameter_seeding.cluster_identity().is_some()
                && proposer_for_parameter_seeding
                    .cluster_parameters()
                    .is_none()
                && proposer_for_parameter_seeding
                    .current_leader()
                    .await
                    .as_ref()
                    == Some(proposer_for_parameter_seeding.local_node_id())
                && let Err(error) = proposer_for_parameter_seeding
                    .seed_cluster_parameters(parameter_seeds)
                    .await
            {
                warn!(target: "nervix_server::application",
                    error = format!("{error:#}"),
                    "failed to seed cluster parameters"
                );
            }
            if let Some(record) = proposer_for_parameter_seeding.cluster_parameters() {
                cluster_for_membership_reconcile
                    .publish_cluster_parameters(record, false)
                    .await;
            }
            if let Err(error) = administrator_for_membership_reconcile
                .reconcile_nodes(cluster_for_membership_reconcile.gossip_state(), |usage| {
                    membership_licensing
                        .admit_snapshot(
                            proposer_for_parameter_seeding
                                .observer()
                                .licensing_snapshot(),
                            crate::licensing::QuotaName::ClusterNodes,
                            usage,
                            1,
                            false,
                        )
                        .map(|_| ())
                })
                .await
            {
                warn!(target: "nervix_server::application", error = %format_args!("{error:#}"), "raft membership reconciliation failed");
            }
            // Published whether or not a membership change failed: the verdicts were judged
            // before any change was attempted, and a node that stopped leading withdraws them.
            let verdicts = administrator_for_membership_reconcile.admission_verdicts();
            cluster_for_membership_reconcile
                .publish_admission_verdicts(verdicts.as_ref())
                .await;
            nervix_primitives::select! {
                _ = membership_reconcile_shutdown.cancelled() => break,
                _ = sleep(membership_interval) => {}
            }
        }
    })
}
