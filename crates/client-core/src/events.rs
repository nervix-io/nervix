//! What a session delivers without being asked, and the requests that shape it: subscription
//! events and server notices, the statement that opens a subscription, and completion suggestions.
//!
//! - **Owns.** The events a caller reads from a session, the rows of a subscription together with
//!   the relay and schema they belong to, and the subscription request builder.
//! - **Depends on.** The wire contract's events and rows, and the language layer's rendering of a
//!   subscription statement.
//! - **Must not know.** How events are routed off the exchange.

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_client_wire::{
    NoticeLevel, RowConformanceError, RowSchema, ServerNotice, SubscriptionDeliveryLost,
    SubscriptionEnded, SubscriptionHandle, SubscriptionRows, SubscriptionRowsSkipped, Suggestion,
    SuggestionKind, SuggestionStatus,
};
use nervix_models::{RelayName, SubscriptionDeliveryBehavior};
use triomphe::Arc;

use crate::subscriptions::SubscriptionInterruption;

/// One event of a subscription the client holds.
#[derive(Debug, Clone)]
pub enum SubscriptionEvent {
    Rows(SubscriptionRowsEvent),
    /// A dropping subscription discarded rows the session could not take in time.
    DeliveryLost(SubscriptionDeliveryLost),
    /// Rows were skipped; the subscription stays open.
    RowsSkipped(SubscriptionRowsSkipped),
    /// The server closed the subscription. No further rows follow for its generation.
    Ended(SubscriptionEnded),
    /// The exchange ended, leaving a gap before any restoration on a new session.
    Interrupted(SubscriptionInterruption),
    /// The client could not retain more events for this subscription. Its delivery on this
    /// exchange has ended, while unrelated subscriptions continue.
    ConsumerOverflow(SubscriptionHandle),
}

impl SubscriptionEvent {
    /// The subscription the event belongs to.
    pub fn subscription(&self) -> &SubscriptionHandle {
        match self {
            Self::Rows(rows) => rows.rows.subscription(),
            Self::DeliveryLost(lost) => &lost.subscription,
            Self::RowsSkipped(skipped) => &skipped.subscription,
            Self::Ended(ended) => &ended.subscription,
            Self::Interrupted(interrupted) => &interrupted.subscription,
            Self::ConsumerOverflow(handle) => handle,
        }
    }

    pub(crate) fn queued_bytes(&self) -> usize {
        let dynamic_bytes = match self {
            Self::Rows(rows) => rows.rows.frame().len(),
            Self::DeliveryLost(_)
            | Self::RowsSkipped(_)
            | Self::Ended(_)
            | Self::Interrupted(_)
            | Self::ConsumerOverflow(_) => 0,
        };
        dynamic_bytes
            .checked_add(std::mem::size_of::<Self>())
            .assured("an event's decoded frame and in-memory header fit the process address space")
    }
}

/// A batch of a subscription's rows, with the relay they come from and the schema the
/// subscription announced when it opened.
#[derive(Debug, Clone)]
pub struct SubscriptionRowsEvent {
    pub relay: RelayName,
    pub schema: Arc<RowSchema>,
    pub rows: SubscriptionRows,
}

impl SubscriptionRowsEvent {
    /// The display text of every row of the batch, in batch order, held to the announced schema.
    pub fn display_lines(&self) -> Result<Vec<String>, Report<RowConformanceError>> {
        self.rows.batch().display_lines(&self.schema)
    }
}

/// A server condition worth an operator's attention that belongs to no request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerEvent {
    pub level: NoticeLevel,
    pub message: String,
}

impl From<ServerNotice> for ServerEvent {
    fn from(notice: ServerNotice) -> Self {
        Self {
            level: notice.level,
            message: notice.message,
        }
    }
}

impl ServerEvent {
    pub(crate) fn queued_bytes(&self) -> usize {
        self.message
            .len()
            .checked_add(std::mem::size_of::<Self>())
            .assured("a decoded notice and its in-memory header fit the process address space")
    }
}

/// A completion offered at a cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutocompleteSuggestion {
    pub value: String,
    pub kind: SuggestionKind,
    pub edit: crate::wire::TextEdit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutocompleteOutcome {
    pub status: SuggestionStatus,
    pub suggestions: Vec<AutocompleteSuggestion>,
    pub continuation: Option<String>,
}

impl From<Suggestion> for AutocompleteSuggestion {
    fn from(suggestion: Suggestion) -> Self {
        Self {
            value: suggestion.value,
            kind: suggestion.kind,
            edit: suggestion.edit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionRequest {
    pub name: String,
    pub relay: String,
    pub delivery_behavior: SubscriptionDeliveryBehavior,
    pub batch_sample_rate: Option<String>,
    pub where_clause: Option<nervix_models::Expression>,
}

impl SubscriptionRequest {
    pub fn new(name: impl Into<String>, relay: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            relay: relay.into(),
            delivery_behavior: SubscriptionDeliveryBehavior::Blocking,
            batch_sample_rate: None,
            where_clause: None,
        }
    }

    pub fn blocking(mut self) -> Self {
        self.delivery_behavior = SubscriptionDeliveryBehavior::Blocking;
        self
    }

    pub fn dropping(mut self) -> Self {
        self.delivery_behavior = SubscriptionDeliveryBehavior::Dropping;
        self
    }

    pub fn with_batch_sample_rate(mut self, batch_sample_rate: impl Into<String>) -> Self {
        self.batch_sample_rate = Some(batch_sample_rate.into());
        self
    }

    pub fn with_where_clause(mut self, where_clause: nervix_models::Expression) -> Self {
        self.where_clause = Some(where_clause);
        self
    }

    pub fn to_query(&self) -> String {
        nervix_nspl::subscribe::create_subscription_query(
            &self.name,
            &self.relay,
            self.delivery_behavior,
            self.batch_sample_rate.as_deref(),
            self.where_clause.as_ref(),
        )
    }
}
