//! The leader's refresh attempt and independent committed deadline timer.
//!
//! Layer: control plane.
//! - **Owns.** Leadership-scoped request cancellation and deadline/time proposals.
//! - **Depends on.** Consensus observations, derived policy deadlines and bounded request owners.
//! - **Must not know.** Private keys, document parsing or graph execution.

use std::time::Duration;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_consensus::LeaderTenure;
use nervix_models::Timestamp;
use nervix_primitives::{
    sync::CancellationToken,
    task::JoinSet,
    time::{Instant, sleep},
};

use super::{LicensingOperationError, clock};
use crate::{application::session_service::SessionServiceImpl, licensing::policy::PolicyState};

type RefreshAttempts = JoinSet<Result<(), Report<LicensingOperationError>>>;

pub(in crate::application) async fn run(service: SessionServiceImpl, shutdown: CancellationToken) {
    let observer = service.inner.consensus.observer();
    let mut topology = observer.subscribe_topology();
    let mut applied = observer.subscribe_applied();
    let mut requests = RefreshAttempts::new();
    let mut streams = super::stream::StreamTasks::default();
    let (hints, mut hint_changes) = nervix_primitives::sync::watch::channel(());
    let mut notification_due = false;
    let mut connectivity_changes = service.inner.licensing.connectivity_changes();
    let mut clock_changes = service
        .inner
        .runtime
        .fault_injection()
        .licensing_clock_changes();
    let mut tenure = None;
    let mut retry_at = Instant::now();
    let mut announced = None;
    let mut published = None;
    let mut refreshed_revocations = None;
    loop {
        nervix_primitives::task::consume_budget().await;
        let current = service
            .inner
            .consensus
            .current_leader_tenure()
            .filter(|leader| leader.leader_id() == observer.local_node_id());
        if current != tenure {
            requests.abort_all();
            while requests.join_next().await.is_some() {
                nervix_primitives::task::consume_budget().await;
            }
            streams.stop().await;
            notification_due = false;
            hint_changes.borrow_and_update();
            tenure = current;
            retry_at = Instant::now();
            refreshed_revocations = None;
        }
        streams.reconcile(&service, tenure.is_some(), &hints).await;
        let publication = (tenure.clone(), service.inner.licensing.connectivity());
        if published.as_ref() != Some(&publication) {
            service
                .inner
                .cluster
                .publish_licensing_connectivity(
                    tenure.as_ref().map(LeaderTenure::term),
                    publication.1,
                )
                .await;
            published = Some(publication);
        }
        let delay = if tenure.is_some() {
            reconcile(
                &service,
                &mut requests,
                retry_at,
                &mut announced,
                &mut refreshed_revocations,
                &mut notification_due,
            )
            .await
        } else {
            None
        };
        let retry_delay = retry_at.saturating_duration_since(Instant::now());
        let delay = if retry_delay.is_zero() {
            delay
        } else {
            Some(match delay {
                Some(deadline) => deadline.min(retry_delay),
                None => retry_delay,
            })
        };
        let timer = async {
            match delay {
                Some(delay) => sleep(delay).await,
                None => std::future::pending().await,
            }
        };
        let clock_changed = async {
            match &mut clock_changes {
                Some(changes) => match changes.changed().await {
                    Ok(()) => {}
                    Err(error) => {
                        tracing::debug!(%error, "optional licensing fixture clock watch closed; the deadline timer remains active");
                        std::future::pending::<()>().await;
                    }
                },
                None => std::future::pending().await,
            }
        };
        nervix_primitives::select! {
            changed = hint_changes.changed() => {
                if changed.is_err() { break; }
                notification_due = true;
                retry_at = Instant::now();
                service.inner.licensing_requests.expedite();
            },
            completed = streams.tasks.join_next(), if !streams.tasks.is_empty() => {
                if let Some(Err(error)) = completed {
                    tracing::warn!(%error, "licensing stream task ended unexpectedly");
                }
                streams.reset();
            },
            _ = clock_changed => {},
            changed = connectivity_changes.changed() => if changed.is_err() { break; },
            _ = shutdown.cancelled() => break,
            changed = topology.changed() => if !changed { break; },
            changed = applied.changed() => if changed.is_err() { break; },
            completed = requests.join_next(), if !requests.is_empty() => {
                let succeeded = matches!(&completed, Some(Ok(Ok(()))));
                if !succeeded {
                    notification_due = true;
                }
                report_attempt(completed);
                retry_at = Instant::now() + if succeeded && notification_due {
                    Duration::ZERO
                } else {
                    service.cluster_parameters().licensing_request_timeout()
                };
            }
            _ = timer => {},
        }
    }
    requests.abort_all();
    while requests.join_next().await.is_some() {
        nervix_primitives::task::consume_budget().await;
    }
    streams.stop().await;
    service
        .inner
        .cluster
        .publish_licensing_connectivity(None, service.inner.licensing.connectivity())
        .await;
}

async fn reconcile(
    service: &SessionServiceImpl,
    requests: &mut RefreshAttempts,
    retry_at: Instant,
    announced: &mut Option<PolicyState>,
    refreshed_revocations: &mut Option<Vec<u64>>,
    notification_due: &mut bool,
) -> Option<Duration> {
    let policy = crate::licensing::policy::evaluate_policy(
        service.inner.consensus.observer().licensing_snapshot(),
    );
    let initialized = policy.snapshot.initialized.as_ref()?;
    let pending = initialized.rotation.is_some()
        || matches!(&initialized.record.mode,
        nervix_consensus::LicensingMode::Released(release) if release.awaiting());
    if matches!(
        initialized.record.mode,
        nervix_consensus::LicensingMode::Free { .. }
    ) && !pending
    {
        return None;
    }
    let now = clock::observe(service.inner.runtime.fault_injection());
    let mut delay = None;
    if let Some(deadline) = crate::licensing::policy::next_deadline(&policy) {
        if now >= deadline {
            if let Err(error) = service.inner.consensus.advance_licensing_time(now).await {
                tracing::warn!(error = %format_args!("{error:#}"), "licensing time proposal failed");
                delay = Some(service.cluster_parameters().licensing_request_timeout());
            }
        } else {
            let nanos = i128::from(deadline.unix_nanos()) - i128::from(now.unix_nanos());
            delay = Some(Duration::from_nanos(
                u64::try_from(nanos).assured("the future timestamp difference fits u64"),
            ));
        }
    }
    let due = match &initialized.verification {
        Some(Ok(verified)) => {
            verified.signing_key_revoked
                || now.into_datetime() >= verified.decision.timing.refresh_after
        }
        Some(Err(_)) | None => true,
    };
    let revocations = initialized
        .record
        .trust
        .iter()
        .filter(|document| {
            document.typ == nervix_licensing_protocol::document::DocumentType::Revocation
        })
        .map(|document| document.serial)
        .collect::<Vec<_>>();
    let newly_revoked = !pending
        && matches!(&initialized.verification, Some(Ok(verified)) if verified.signing_key_revoked)
        && refreshed_revocations.as_ref() != Some(&revocations);
    if (pending
        || (due || *notification_due)
            && matches!(
                initialized.record.mode,
                nervix_consensus::LicensingMode::Activated(_)
            ))
        && requests.is_empty()
        && (newly_revoked || Instant::now() >= retry_at)
    {
        if newly_revoked {
            *refreshed_revocations = Some(revocations);
        }
        if !pending {
            *notification_due = false;
        }
        let requester = service.clone();
        requests.spawn(async move {
            if pending {
                requester.resume_licensing_operations().await
            } else {
                requester.refresh_license_in_background().await
            }
        });
    }
    if *announced != Some(policy.state) {
        publish_warning(service, policy.state, initialized.record.licensing_time);
        *announced = Some(policy.state);
    }
    delay
}

fn report_attempt(
    completed: Option<
        Result<Result<(), Report<LicensingOperationError>>, nervix_primitives::task::JoinError>,
    >,
) {
    match completed {
        Some(Ok(Err(error))) => {
            tracing::warn!(error = %format_args!("{error:#}"), "licensing refresh failed")
        }
        Some(Err(error)) => {
            tracing::warn!(error = %error, "licensing refresh task ended unexpectedly")
        }
        Some(Ok(Ok(()))) | None => {}
    }
}

#[derive(Debug)]
enum WarningCode {
    RefreshOverdue,
    RefreshLapsed,
    DecisionUnverifiable,
}
struct WarningParameters {
    licensing_time: Timestamp,
}
struct LicensingNotice {
    code: WarningCode,
    params: WarningParameters,
}

impl std::fmt::Display for LicensingNotice {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self.code {
            WarningCode::RefreshOverdue => {
                "Licensing refresh is overdue; existing processing continues. Check service \
                 connectivity and run REFRESH LICENSE."
            }
            WarningCode::RefreshLapsed => {
                "The license offline allowance has expired; new usage is refused while existing \
                 processing continues. Restore service connectivity and run REFRESH LICENSE."
            }
            WarningCode::DecisionUnverifiable => {
                "The committed license decision cannot be verified; the free usage policy applies. \
                 Check the license and run REFRESH LICENSE."
            }
        };
        write!(
            formatter,
            "{message} Committed licensing time: {}.",
            self.params.licensing_time
        )
    }
}

fn publish_warning(service: &SessionServiceImpl, state: PolicyState, licensing_time: Timestamp) {
    let code = match state {
        PolicyState::LicensedOverdue => WarningCode::RefreshOverdue,
        PolicyState::Lapsed => WarningCode::RefreshLapsed,
        PolicyState::Degraded => WarningCode::DecisionUnverifiable,
        _ => return,
    };
    tracing::warn!(?code, %licensing_time, "licensing policy warning");
    let notice = LicensingNotice {
        code,
        params: WarningParameters { licensing_time },
    };
    service.inner.events.relay_warning(
        nervix_client_wire::NoticeKind::Licensing,
        notice.to_string(),
    );
}
