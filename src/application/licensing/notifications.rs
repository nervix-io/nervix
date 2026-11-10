//! Pure notification deduplication and comparison with one committed licensing observation.
//!
//! Layer: decisions.
//! - **Owns.** The activation-local notification cursor and refresh-hint decision.
//! - **Depends on.** Typed protocol hints and committed revision numbers.
//! - **Must not know.** Connections, timers, tasks, shared state or policy grants.

use nervix_licensing_protocol::notifications::Notification;

#[derive(Default)]
pub(super) struct NotificationCursor {
    pub(super) last: Option<u64>,
}

pub(super) struct CommittedRevisions {
    pub(super) decision_revision: u64,
    pub(super) conditions_revision: u64,
    pub(super) trust_serial: u64,
}

impl NotificationCursor {
    pub(super) fn observe(&mut self, hint: &Notification, committed: &CommittedRevisions) -> bool {
        let revision = hint.revision();
        if self.last.is_some_and(|last| revision <= last) {
            return false;
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
                notification_gap
                    || state.decision_revision > committed.decision_revision
                    || state.conditions_revision > committed.conditions_revision
                    || state.trust_serial > committed.trust_serial
            }
            Notification::Decision(_) | Notification::Trust(_) => true,
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
        assert!(!cursor.observe(&Notification::Sync(state(0)), &committed()));
        let hint = Notification::Decision(DecisionNotification {
            notification_revision: 1,
            decision_revision: 8,
            reason: "override".into(),
        });
        assert!(cursor.observe(&hint, &committed()));
        assert!(!cursor.observe(&hint, &committed()));
        assert!(!cursor.observe(&Notification::Sync(state(0)), &committed()));
        assert_eq!(cursor.last, Some(1));
        assert!(cursor.observe(
            &Notification::Trust(TrustNotification {
                notification_revision: 2,
                trust_serial: 3,
            }),
            &committed()
        ));
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
            assert!(cursor.observe(&Notification::Sync(remote), &committed()));
            assert_eq!(cursor.last, Some(0));
        }
        let mut cursor = NotificationCursor { last: Some(9) };
        let mut stale = state(8);
        stale.decision_revision = 1;
        stale.conditions_revision = 1;
        stale.trust_serial = 1;
        assert!(!cursor.observe(&Notification::Sync(stale), &committed()));
    }

    #[test]
    fn a_sync_notification_gap_refreshes_even_before_a_new_decision_has_been_issued() {
        let mut cursor = NotificationCursor { last: Some(4) };
        assert!(cursor.observe(&Notification::Sync(state(7)), &committed()));
        assert!(!cursor.observe(&Notification::Sync(state(7)), &committed()));
    }
}
