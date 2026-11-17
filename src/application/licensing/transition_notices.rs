//! Per-node transition warnings from committed licensing facts.
//!
//! Layer: edges.
//! - **Owns.** Typed transition notice parameters and their client/console text rendering.
//! - **Depends on.** Coherent consensus observations, pure policy and the client event bus.
//! - **Must not know.** HTTP, refresh scheduling, signing keys or graph teardown.

use std::collections::BTreeMap;

use nervix_licensing_vocabulary::{Quota, QuotaName};
use nervix_models::Timestamp;
use nervix_primitives::sync::CancellationToken;
use uuid::Uuid;

use crate::{application::session_service::SessionServiceImpl, licensing::policy::evaluate_policy};

#[derive(Debug, Clone, Copy)]
enum LicensingNoticeCode {
    TransitionStarted,
    TransitionAcknowledged,
    TransitionExtended,
    OverLimit,
}

impl LicensingNoticeCode {
    fn as_str(self) -> &'static str {
        match self {
            Self::TransitionStarted => "transition_started",
            Self::TransitionAcknowledged => "transition_acknowledged",
            Self::TransitionExtended => "transition_extended",
            Self::OverLimit => "over_limit",
        }
    }
}

struct LicensingNotice {
    code: LicensingNoticeCode,
    params: LicensingNoticeParameters,
}

struct LicensingNoticeParameters {
    transition_id: Uuid,
    condition: String,
    previous: Quota,
    target: Quota,
    affected_usage: u64,
    current_usage: u64,
    deadline: Timestamp,
}

impl std::fmt::Display for LicensingNotice {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params = &self.params;
        write!(
            formatter,
            "Nervix licensing: {}. Transition {} changed {} from {:?} to {:?}; affected usage {}, \
             current usage {}, deadline {}. Existing usage is protected until the deadline; \
             growth follows the target quota. After the deadline excess usage is reported as over \
             limit; licensing removes nothing. Reduce usage, restore the plan, or contact the \
             organization administrator.",
            self.code.as_str(),
            params.transition_id,
            params.condition,
            params.previous,
            params.target,
            params.affected_usage,
            params.current_usage,
            params.deadline,
        )
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Observation {
    until: Timestamp,
    acknowledged: bool,
    state: TransitionNoticeState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransitionNoticeState {
    Active,
    Ended,
    OverLimit,
}

pub(in crate::application) async fn run(service: SessionServiceImpl, shutdown: CancellationToken) {
    let observer = service.inner.consensus.observer();
    let mut applied = observer.subscribe_applied();
    let mut announced = BTreeMap::new();
    loop {
        nervix_primitives::task::consume_budget().await;
        let policy = evaluate_policy(observer.licensing_snapshot());
        let mut current = BTreeMap::new();
        if let Some(initialized) = &policy.snapshot.initialized {
            for (transition, until, acknowledged) in policy.transitions() {
                for affected in transition.affected {
                    let Some(name) = QuotaName::ALL
                        .into_iter()
                        .find(|name| name.as_str() == affected.condition)
                    else {
                        continue;
                    };
                    let usage = match name {
                        QuotaName::ClusterNodes => policy.snapshot.usage.cluster_nodes,
                        QuotaName::ClusterDomains => policy.snapshot.usage.cluster_domains,
                    };
                    let state = if initialized.record.licensing_time < until {
                        TransitionNoticeState::Active
                    } else if policy.effective_quota(name).admits(usage) {
                        TransitionNoticeState::Ended
                    } else {
                        TransitionNoticeState::OverLimit
                    };
                    let observation = Observation {
                        until,
                        acknowledged,
                        state,
                    };
                    let key = (transition.transition_id, affected.condition.clone());
                    let code = notice_code(announced.get(&key), observation);
                    current.insert(key, observation);
                    if let Some(code) = code {
                        let notice = LicensingNotice {
                            code,
                            params: LicensingNoticeParameters {
                                transition_id: transition.transition_id,
                                condition: affected.condition,
                                previous: affected.previous,
                                target: affected.target,
                                affected_usage: affected.usage,
                                current_usage: usage,
                                deadline: until,
                            },
                        };
                        tracing::warn!(code = notice.code.as_str(), transition_id = %notice.params.transition_id, "licensing transition warning");
                        service.inner.events.relay_warning(
                            nervix_client_wire::NoticeKind::Licensing,
                            notice.to_string(),
                        );
                    }
                }
            }
        }
        announced = current;
        nervix_primitives::select! {
            _ = shutdown.cancelled() => break,
            result = applied.changed() => { if result.is_err() { break; } },
        }
    }
}

fn notice_code(
    previous: Option<&Observation>,
    current: Observation,
) -> Option<LicensingNoticeCode> {
    if previous == Some(&current) {
        return None;
    }
    match current.state {
        TransitionNoticeState::Ended => return None,
        TransitionNoticeState::OverLimit => return Some(LicensingNoticeCode::OverLimit),
        TransitionNoticeState::Active => {}
    }
    match previous {
        Some(previous) if current.acknowledged && !previous.acknowledged => {
            Some(LicensingNoticeCode::TransitionAcknowledged)
        }
        Some(previous) if current.until > previous.until => {
            Some(LicensingNoticeCode::TransitionExtended)
        }
        None => Some(LicensingNoticeCode::TransitionStarted),
        _ => None,
    }
}
