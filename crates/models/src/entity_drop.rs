//! What dropping an entity discarded: the work a node released as done when it destroyed an entity
//! a change removed.
//!
//! Layer: vocabulary.
//! - **Owns.** The kinds of work a dropped entity can hold when it is destroyed, one node's count of
//!   one kind for one entity, and the words a statement's answer names each kind with.
//! - **Depends on.** Model kinds and names, and serialization.
//! - **Must not know.** How a node tears an entity down, how its report travels, or how an answer
//!   is assembled.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use strum::IntoStaticStr;

use crate::{ModelKind, ModelName};

/// What a dropped entity held when its node destroyed it, by kind. The entity's share of every
/// message of each kind is released as done: a source whose message the other consumers delivered
/// hears it acknowledged.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum DiscardedWorkKind {
    /// Records an emitter had not delivered: buffered, waiting for a retry, staged for a commit,
    /// or carried by an attempt the drop cut off.
    UndeliveredRecords,
    /// Rows a reorderer held until their release time.
    HeldRows,
    /// Rows resting in a window, whose acknowledgements waited for the window to fill.
    WindowRows,
    /// Records a correlator held until their partner arrived.
    WaitingRecords,
    /// Messages a `REQUIRED WAIT` retained until their materialized state was present.
    WaitingMessages,
    /// Payloads an ingestor retained without admitting them: its pause buffer and the message an
    /// entry point held at the limit of branches.
    RetainedPayloads,
    /// Messages a relay held for its consumers.
    RelayMessages,
    /// Messages a node had taken in and not passed on: collected input, input in hand and output
    /// waiting in a route's buffer.
    UnprocessedMessages,
}

impl DiscardedWorkKind {
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    /// The kind as a statement's answer names it.
    pub fn describe(self) -> &'static str {
        match self {
            Self::UndeliveredRecords => "undelivered records",
            Self::HeldRows => "held rows",
            Self::WindowRows => "rows resting in a window",
            Self::WaitingRecords => "waiting records",
            Self::WaitingMessages => "waiting messages",
            Self::RetainedPayloads => "retained payloads",
            Self::RelayMessages => "messages the relay held",
            Self::UnprocessedMessages => "unprocessed messages",
        }
    }
}

/// One dropped entity's discarded work of one kind on one node.
#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct DiscardedWork {
    pub entity_kind: ModelKind,
    pub entity: ModelName,
    pub kind: DiscardedWorkKind,
    /// Records, rows, messages or payloads, as the kind counts them.
    pub items: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::DiscardedWorkKind;

    const KINDS: [DiscardedWorkKind; 8] = [
        DiscardedWorkKind::UndeliveredRecords,
        DiscardedWorkKind::HeldRows,
        DiscardedWorkKind::WindowRows,
        DiscardedWorkKind::WaitingRecords,
        DiscardedWorkKind::WaitingMessages,
        DiscardedWorkKind::RetainedPayloads,
        DiscardedWorkKind::RelayMessages,
        DiscardedWorkKind::UnprocessedMessages,
    ];

    #[test]
    fn every_kind_has_its_own_name_and_answer_text() {
        let names = KINDS
            .iter()
            .map(|kind| kind.as_str())
            .collect::<BTreeSet<_>>();
        let texts = KINDS
            .iter()
            .map(|kind| kind.describe())
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), KINDS.len());
        assert_eq!(texts.len(), KINDS.len());
        assert_eq!(
            DiscardedWorkKind::UndeliveredRecords.as_str(),
            "undelivered_records"
        );
        assert_eq!(
            DiscardedWorkKind::WindowRows.describe(),
            "rows resting in a window"
        );
    }
}
