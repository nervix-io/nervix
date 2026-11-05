//! The node panel beside the connection state: the cluster events of the node the console is
//! connected to.
//!
//! Layer: edges.
//! - **Owns.** The bounded log of membership and leadership notices the console received, the one
//!   entry a membership event repeated within a session shares, and the panel that shows the log.
//! - **Depends on.** Typed server notices.
//! - **Must not know.** Which kinds the session routes here, the REPL transcript, or how the
//!   WebSocket transports a notice.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use indexmap::IndexMap;
use leptos::prelude::*;
use meticulous::OptionExt as _;
use nervix_client_wire::{NoticeKind, NoticeLevel, ServerNotice};

use super::MAX_HISTORY_RECORDS;

/// What identifies one entry. A membership event names the session that received it, so the same
/// event repeated within that session joins its entry; any other event has an entry of its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum EntryKey {
    Membership { session: u64, message: String },
    Single { id: u64 },
}

/// One line of the panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ClusterEvent {
    /// Stable for the life of the entry, so the keyed view never shows another entry's text.
    id: u64,
    kind: NoticeKind,
    level: NoticeLevel,
    message: String,
    /// How many identical events of one session the entry stands for.
    occurrences: u64,
}

impl ClusterEvent {
    fn kind_label(&self) -> &'static str {
        match self.kind {
            NoticeKind::Membership => "membership",
            NoticeKind::Leadership => "leadership",
            NoticeKind::ClusterParameters => "cluster-parameters",
            NoticeKind::Runtime => "runtime",
            NoticeKind::ControlPlane => "control-plane",
        }
    }

    fn level_label(&self) -> &'static str {
        match self.level {
            NoticeLevel::Info => "info",
            NoticeLevel::Warning => "warning",
            NoticeLevel::Error => "error",
        }
    }
}

/// The cluster events the console received across its sessions, oldest first and bounded by
/// [`MAX_HISTORY_RECORDS`], as many as the REPL transcript keeps records. Older entries are dropped,
/// while the count of received events keeps every one.
#[derive(Debug, Clone, Default)]
pub(super) struct ClusterEventLog {
    entries: IndexMap<EntryKey, ClusterEvent>,
    /// Every event received, counting each repetition a collapsed entry stands for.
    received: u64,
    next_id: u64,
}

impl ClusterEventLog {
    /// Records `notice`, received on the session `session`. A membership event identical to one
    /// already recorded for that session counts as a repetition of that entry, which becomes the
    /// newest.
    pub(super) fn record(&mut self, session: u64, notice: ServerNotice) {
        self.received = self
            .received
            .checked_add(1)
            .assured("a console receives fewer than 2^64 cluster events");
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .assured("a console records fewer than 2^64 cluster events");
        let key = match notice.kind {
            NoticeKind::Membership => EntryKey::Membership {
                session,
                message: notice.message.clone(),
            },
            NoticeKind::Leadership
            | NoticeKind::ClusterParameters
            | NoticeKind::Runtime
            | NoticeKind::ControlPlane => EntryKey::Single { id },
        };
        if let Some((index, _, existing)) = self.entries.get_full_mut(&key) {
            existing.occurrences = existing
                .occurrences
                .checked_add(1)
                .assured("an entry stands for fewer than 2^64 events");
            let newest = self
                .entries
                .len()
                .checked_sub(1)
                .verified("the entry found above is in the log");
            self.entries.move_index(index, newest);
            return;
        }
        self.entries.insert(
            key,
            ClusterEvent {
                id,
                kind: notice.kind,
                level: notice.level,
                message: notice.message,
                occurrences: 1,
            },
        );
        if self.entries.len() > MAX_HISTORY_RECORDS {
            self.entries.shift_remove_index(0);
        }
    }

    /// How many events the console received, each repetition included.
    pub(super) fn received(&self) -> u64 {
        self.received
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entries the panel lists, newest first.
    fn newest_first(&self) -> Vec<ClusterEvent> {
        self.entries.values().rev().cloned().collect()
    }
}

/// The **Events** button beside the connection state, with the received count, and the panel it
/// opens.
#[component]
pub(super) fn NodePanel(events: RwSignal<ClusterEventLog>) -> impl IntoView {
    let open = RwSignal::new(false);
    view! {
        <div class="menu-wrap node-panel-wrap">
            <button
                class="node-panel-toggle"
                type="button"
                title="Cluster events of the connected node"
                aria-expanded=move || open.get().to_string()
                data-event-count=move || events.with(|log| log.received().to_string())
                on:click=move |_| open.update(|open| *open = !*open)
            >
                <span>"Events"</span>
                <span class="node-event-count">{move || events.with(ClusterEventLog::received)}</span>
            </button>
            <div class="popup-menu node-panel" class:open=move || open.get()>
                <Show
                    when=move || events.with(ClusterEventLog::is_empty)
                    fallback=move || view! {
                        <For
                            each=move || events.with(ClusterEventLog::newest_first)
                            key=|event| (event.id, event.occurrences)
                            children=move |event| view! { <ClusterEventLine event=event /> }
                        />
                    }
                >
                    <span class="node-panel-empty">"No cluster events yet"</span>
                </Show>
            </div>
        </div>
    }
}

/// One entry of the panel, with its repetition count when it stands for more than one event.
#[component]
fn ClusterEventLine(event: ClusterEvent) -> impl IntoView {
    let repeats = if event.occurrences > 1 {
        Some(view! { <span class="node-event-repeats">{format!("×{}", event.occurrences)}</span> })
    } else {
        None
    };
    let kind = event.kind_label();
    let level = event.level_label();
    let occurrences = event.occurrences.to_string();
    view! {
        <div
            class="node-event"
            data-kind=kind
            data-level=level
            data-occurrences=occurrences
        >
            <span class="node-event-text">{event.message}</span>
            {repeats}
        </div>
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    fn notice(kind: NoticeKind, message: &str) -> ServerNotice {
        ServerNotice {
            level: NoticeLevel::Info,
            kind,
            message: message.to_string(),
        }
    }

    fn lines(log: &ClusterEventLog) -> Vec<(String, u64)> {
        log.newest_first()
            .into_iter()
            .map(|event| (event.message, event.occurrences))
            .collect()
    }

    #[test]
    fn a_membership_event_repeated_within_a_session_is_one_entry_with_its_count() {
        let mut log = ClusterEventLog::default();
        log.record(1, notice(NoticeKind::Membership, "node 'node-3' left"));
        log.record(
            1,
            notice(NoticeKind::Leadership, "raft transition: state=Leader"),
        );
        log.record(1, notice(NoticeKind::Membership, "node 'node-3' left"));

        assert_eq!(log.received(), 3);
        assert_eq!(
            lines(&log),
            vec![
                ("node 'node-3' left".to_string(), 2),
                ("raft transition: state=Leader".to_string(), 1),
            ]
        );
    }

    #[test]
    fn the_same_membership_event_of_another_session_has_its_own_entry() {
        let mut log = ClusterEventLog::default();
        log.record(1, notice(NoticeKind::Membership, "node 'node-3' left"));
        log.record(2, notice(NoticeKind::Membership, "node 'node-3' left"));

        assert_eq!(log.received(), 2);
        assert_eq!(
            lines(&log),
            vec![
                ("node 'node-3' left".to_string(), 1),
                ("node 'node-3' left".to_string(), 1),
            ]
        );
    }

    #[test]
    fn leadership_events_are_never_collapsed() {
        let mut log = ClusterEventLog::default();
        log.record(1, notice(NoticeKind::Leadership, "raft node removed: 3"));
        log.record(1, notice(NoticeKind::Leadership, "raft node removed: 3"));

        assert_eq!(
            lines(&log),
            vec![
                ("raft node removed: 3".to_string(), 1),
                ("raft node removed: 3".to_string(), 1),
            ]
        );
    }

    #[test]
    fn the_log_keeps_its_newest_entries_and_counts_every_event() {
        let mut log = ClusterEventLog::default();
        let total = MAX_HISTORY_RECORDS
            .checked_add(3)
            .assured("the test log bound is small");
        for index in 0..total {
            log.record(
                1,
                notice(NoticeKind::Leadership, &format!("transition {index}")),
            );
        }

        let shown = lines(&log);
        let last = total
            .checked_sub(1)
            .assured("the test records more than one event");
        assert_eq!(shown.len(), MAX_HISTORY_RECORDS);
        assert_eq!(
            shown.first().map(|(message, _)| message.as_str()),
            Some(format!("transition {last}").as_str())
        );
        assert_eq!(
            shown.last().map(|(message, _)| message.as_str()),
            Some("transition 3")
        );
        assert_eq!(
            log.received(),
            u64::try_from(total).assured("the test count fits a u64")
        );
    }

    #[test]
    fn a_repeated_entry_keeps_its_identity() {
        let mut log = ClusterEventLog::default();
        log.record(1, notice(NoticeKind::Membership, "node 'node-2' left"));
        let first = log.newest_first().remove(0).id;
        log.record(
            1,
            notice(NoticeKind::Leadership, "raft transition: state=Follower"),
        );
        log.record(1, notice(NoticeKind::Membership, "node 'node-2' left"));

        let newest = log.newest_first().remove(0);
        assert_eq!(newest.id, first);
        assert_eq!(newest.occurrences, 2);
    }
}
