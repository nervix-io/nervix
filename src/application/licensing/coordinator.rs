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
use uuid::Uuid;

use super::{
    LicensingOperationError, clock,
    notifications::{RefreshHint, RefreshTrigger},
};
use crate::{application::session_service::SessionServiceImpl, licensing::policy::PolicyState};

struct RequestAttempt {
    activation_id: Option<Uuid>,
    result: Result<(), Report<LicensingOperationError>>,
}

type RefreshAttempts = JoinSet<RequestAttempt>;

pub(in crate::application) async fn run(service: SessionServiceImpl, shutdown: CancellationToken) {
    let observer = service.inner.consensus.observer();
    let mut topology = observer.subscribe_topology();
    let mut applied = observer.subscribe_applied();
    let mut requests = RefreshAttempts::new();
    let mut streams = super::stream::StreamTasks::default();
    let (hints, mut hint_changes) = nervix_primitives::sync::watch::channel(None);
    let mut notification_due = None;
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
            notification_due = None;
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
                notification_due = *hint_changes.borrow_and_update();
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
                let succeeded = matches!(&completed, Some(Ok(attempt)) if attempt.result.is_ok());
                restore_failed_hint(&completed, &mut notification_due, || {
                    service.inner.consensus.observer().licensing_snapshot().initialized
                        .and_then(|state| activation_id(&state.record.mode))
                });
                report_attempt(completed);
                retry_at = Instant::now() + if succeeded && notification_due.is_some() {
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
    notification_due: &mut Option<RefreshHint>,
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
        *notification_due = None;
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
    let due = RefreshTrigger::Automatic(*notification_due).needs_refresh(initialized, now);
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
    let binding = activation_id(&initialized.record.mode);
    if (pending
        || due
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
        let hint = if pending {
            None
        } else {
            notification_due.take()
        };
        let requester = service.clone();
        requests.spawn(async move {
            let result = if pending {
                requester.resume_licensing_operations().await
            } else {
                requester.refresh_license_in_background(hint).await
            };
            RequestAttempt {
                activation_id: binding,
                result,
            }
        });
    }
    if *announced != Some(policy.state) {
        publish_warning(service, policy.state, initialized.record.licensing_time);
        *announced = Some(policy.state);
    }
    delay
}

fn report_attempt(completed: Option<Result<RequestAttempt, nervix_primitives::task::JoinError>>) {
    match completed {
        Some(Ok(RequestAttempt {
            result: Err(error), ..
        })) => {
            tracing::warn!(error = %format_args!("{error:#}"), "licensing refresh failed")
        }
        Some(Err(error)) => {
            tracing::warn!(error = %error, "licensing refresh task ended unexpectedly")
        }
        Some(Ok(RequestAttempt { result: Ok(()), .. })) | None => {}
    }
}

fn activation_id(mode: &nervix_consensus::LicensingMode) -> Option<Uuid> {
    match mode {
        nervix_consensus::LicensingMode::Activated(activation) => {
            Some(activation.binding.activation_id)
        }
        nervix_consensus::LicensingMode::Released(release) => {
            Some(release.activation.binding.activation_id)
        }
        nervix_consensus::LicensingMode::Free { .. } => None,
    }
}

fn restore_failed_hint(
    completed: &Option<Result<RequestAttempt, nervix_primitives::task::JoinError>>,
    notification_due: &mut Option<RefreshHint>,
    current_binding: impl FnOnce() -> Option<Uuid>,
) {
    if notification_due.is_some() {
        return;
    }
    let binding = match completed {
        Some(Ok(attempt)) if attempt.result.is_err() => attempt.activation_id,
        Some(Err(_)) => current_binding(),
        _ => None,
    };
    *notification_due = binding.map(RefreshHint::invalidation);
}

#[cfg(test)]
mod hint_tests {
    use super::*;

    #[test]
    fn a_failed_request_keeps_its_activation_without_replacing_a_newer_hint() {
        let previous = Uuid::from_u128(1);
        let current = Uuid::from_u128(2);
        let completed = Some(Ok(RequestAttempt {
            activation_id: Some(previous),
            result: Err(Report::new(LicensingOperationError::Request)),
        }));
        let mut hint = Some(RefreshHint::invalidation(current));
        restore_failed_hint(&completed, &mut hint, || {
            panic!("the request retained its binding")
        });
        assert_eq!(hint, Some(RefreshHint::invalidation(current)));
        hint = None;
        restore_failed_hint(&completed, &mut hint, || {
            panic!("the request retained its binding")
        });
        assert_eq!(hint, Some(RefreshHint::invalidation(previous)));
    }

    #[test]
    fn success_never_restores_a_completed_notification() {
        let completed = Some(Ok(RequestAttempt {
            activation_id: Some(Uuid::from_u128(1)),
            result: Ok(()),
        }));
        let mut hint = None;
        restore_failed_hint(&completed, &mut hint, || {
            panic!("a completed request needs no snapshot")
        });
        assert_eq!(hint, None);
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
