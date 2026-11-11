//! Pure notification deduplication and refresh decisions from committed observations.
//!
//! Layer: decisions.
//! - **Owns.** The activation-local cursor, retained refresh requirements and their recheck.
//! - **Depends on.** Typed protocol hints, committed licensing observations and timestamps.
//! - **Must not know.** Connections, timers, tasks, mutable owners or policy grants.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        decisions,
        reason = "decides whether a committed licensing observation needs refresh"
    )
)]

use meticulous::OptionExt as _;
use nervix_consensus::{ClusterLicensingState, LicensingMode};
use nervix_licensing_protocol::notifications::Notification;
use nervix_models::Timestamp;
use uuid::Uuid;

#[derive(Default)]
pub(super) struct NotificationCursor {
    pub(super) last: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CommittedRevisions {
    pub(super) decision_revision: u64,
    pub(super) conditions_revision: u64,
    pub(super) trust_serial: u64,
}

impl CommittedRevisions {
    pub(super) fn from_state(state: &ClusterLicensingState) -> Option<Self> {
        let LicensingMode::Activated(activation) = &state.record.mode else {
            return None;
        };
        Some(Self {
            decision_revision: activation.decision.revision,
            conditions_revision: activation.decision.conditions_revision,
            trust_serial: state
                .record
                .trust
                .iter()
                .map(|document| document.serial)
                .max()
                .unwrap_or(0),
        })
    }

    fn covers(&self, announced: &Self) -> bool {
        self.decision_revision >= announced.decision_revision
            && self.conditions_revision >= announced.conditions_revision
            && self.trust_serial >= announced.trust_serial
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RefreshRequirement {
    /// A notification gap can describe a status or key change before a decision is issued.
    Invalidation,
    Revisions(CommittedRevisions),
}

impl RefreshRequirement {
    fn needs_refresh(&self, committed: impl FnOnce() -> CommittedRevisions) -> bool {
        match self {
            Self::Invalidation => true,
            Self::Revisions(announced) => !committed().covers(announced),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RefreshHint {
    pub(super) activation_id: Uuid,
    pub(super) requirement: RefreshRequirement,
}

impl RefreshHint {
    pub(super) fn invalidation(activation_id: Uuid) -> Self {
        Self {
            activation_id,
            requirement: RefreshRequirement::Invalidation,
        }
    }

    fn needs_refresh(&self, state: &ClusterLicensingState) -> bool {
        let LicensingMode::Activated(activation) = &state.record.mode else {
            return false;
        };
        if self.activation_id != activation.binding.activation_id {
            return false;
        }
        self.requirement.needs_refresh(|| {
            CommittedRevisions::from_state(state)
                .verified("the captured immutable observation was matched as activated above")
        })
    }
}

#[derive(Clone, Copy)]
pub(super) enum RefreshTrigger {
    Explicit,
    Automatic(Option<RefreshHint>),
}

impl RefreshTrigger {
    pub(super) fn needs_refresh(&self, state: &ClusterLicensingState, now: Timestamp) -> bool {
        let Self::Automatic(hint) = self else {
            return true;
        };
        let due = match &state.verification {
            Some(Ok(verified)) => {
                verified.signing_key_revoked
                    || now.into_datetime() >= verified.decision.timing.refresh_after
            }
            Some(Err(_)) | None => true,
        };
        if due {
            return true;
        }
        match hint {
            Some(hint) => hint.needs_refresh(state),
            None => false,
        }
    }
}

impl NotificationCursor {
    pub(super) fn observe(
        &mut self,
        hint: &Notification,
        committed: &CommittedRevisions,
    ) -> Option<RefreshRequirement> {
        let revision = hint.revision();
        if self.last.is_some_and(|last| revision <= last) {
            return None;
        }
        // Status/key changes may advance only the notification revision until the next decision
        // is issued. Reconciliation must not discard their replay behind the initial sync.
        let notification_gap = match self.last {
            Some(last) => revision > last,
            None => revision > 0,
        };
        self.last = Some(revision);
        match hint {
            Notification::Sync(state) => {
                if notification_gap {
                    return Some(RefreshRequirement::Invalidation);
                }
                let announced = CommittedRevisions {
                    decision_revision: state.decision_revision,
                    conditions_revision: state.conditions_revision,
                    trust_serial: state.trust_serial,
                };
                if committed.covers(&announced) {
                    None
                } else {
                    Some(RefreshRequirement::Revisions(announced))
                }
            }
            Notification::Decision(_) | Notification::Trust(_) => {
                Some(RefreshRequirement::Invalidation)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_licensing_protocol::notifications::{
        DecisionNotification, NotificationState, TrustNotification,
    };

    use super::*;

    fn committed() -> CommittedRevisions {
        CommittedRevisions {
            decision_revision: 8,
            conditions_revision: 3,
            trust_serial: 2,
        }
    }

    fn state(notification_revision: u64) -> NotificationState {
        NotificationState {
            notification_revision,
            decision_revision: 8,
            conditions_revision: 3,
            trust_serial: 2,
        }
    }

    #[test]
    fn initial_sync_grants_nothing_and_duplicate_or_stale_hints_do_not_refresh() {
        let mut cursor = NotificationCursor::default();
        assert!(
            cursor
                .observe(&Notification::Sync(state(0)), &committed())
                .is_none()
        );
        let hint = Notification::Decision(DecisionNotification {
            notification_revision: 1,
            decision_revision: 8,
            reason: "override".into(),
        });
        assert_eq!(
            cursor.observe(&hint, &committed()),
            Some(RefreshRequirement::Invalidation)
        );
        assert!(cursor.observe(&hint, &committed()).is_none());
        assert!(
            cursor
                .observe(&Notification::Sync(state(0)), &committed())
                .is_none()
        );
        assert_eq!(cursor.last, Some(1));
        assert_eq!(
            cursor.observe(
                &Notification::Trust(TrustNotification {
                    notification_revision: 2,
                    trust_serial: 3,
                }),
                &committed()
            ),
            Some(RefreshRequirement::Invalidation)
        );
    }

    #[test]
    fn reconnect_sync_recovers_each_revision_gap_but_never_rolls_state_back() {
        for gap in 0..3 {
            let mut cursor = NotificationCursor::default();
            let mut remote = state(0);
            match gap {
                0 => remote.decision_revision = 9,
                1 => remote.conditions_revision = 4,
                _ => remote.trust_serial = 3,
            }
            assert!(
                cursor
                    .observe(&Notification::Sync(remote), &committed())
                    .is_some()
            );
            assert_eq!(cursor.last, Some(0));
        }
        let mut cursor = NotificationCursor { last: Some(9) };
        let mut stale = state(8);
        stale.decision_revision = 1;
        stale.conditions_revision = 1;
        stale.trust_serial = 1;
        assert!(
            cursor
                .observe(&Notification::Sync(stale), &committed())
                .is_none()
        );
    }

    #[test]
    fn a_sync_notification_gap_refreshes_even_before_a_new_decision_has_been_issued() {
        let mut cursor = NotificationCursor { last: Some(4) };
        assert_eq!(
            cursor.observe(&Notification::Sync(state(7)), &committed()),
            Some(RefreshRequirement::Invalidation)
        );
        assert!(
            cursor
                .observe(&Notification::Sync(state(7)), &committed())
                .is_none()
        );
    }

    #[test]
    fn a_sync_retains_each_revision_requirement_for_a_fresh_committed_observation() {
        for gap in 0..3 {
            let mut cursor = NotificationCursor::default();
            let mut announced = state(0);
            let mut covered = committed();
            match gap {
                0 => {
                    announced.decision_revision = 9;
                    covered.decision_revision = 9;
                }
                1 => {
                    announced.conditions_revision = 4;
                    covered.conditions_revision = 4;
                }
                _ => {
                    announced.trust_serial = 3;
                    covered.trust_serial = 3;
                }
            }
            let requirement = cursor
                .observe(&Notification::Sync(announced), &committed())
                .assured("one announced revision was advanced above the committed observation");
            assert!(requirement.needs_refresh(committed));
            assert!(!requirement.needs_refresh(|| covered));
            assert!(
                requirement.needs_refresh(committed),
                "checking a covered response must not consume another observation's requirement"
            );
        }
    }

    #[test]
    fn a_notification_gap_still_reconciles_when_a_pending_response_covers_its_numbers() {
        let mut cursor = NotificationCursor::default();
        assert_eq!(
            cursor.observe(&Notification::Sync(state(2)), &committed()),
            Some(RefreshRequirement::Invalidation)
        );
        assert!(RefreshRequirement::Invalidation.needs_refresh(|| {
            panic!("an invalidation neither depends on nor scans the committed revisions")
        }));
    }
}
