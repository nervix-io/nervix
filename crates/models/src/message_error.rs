//! The error record every graph node keeps of a failure: its class in the fixed class tree, the
//! node that produced it, the operation that failed and, when an outside system refused the data
//! with a code of its own, that code.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The ten error classes and their typed ancestry, the outside system's code, the
//!   error an operation raises before a node is named on it, the record its node attributes, the
//!   class and node a runtime event reports, and the Arrow shape of the `error` scope.
//! - **Depends on.** Node references, timestamps and Arrow's schema types.
//! - **Must not know.** How an error is handled, routed, logged, counted or carried between nodes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use sorted_vec::SortedSet;
use strum::{AsRefStr, EnumString, IntoStaticStr};
use uuid::Uuid;

use crate::{NodeRef, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FieldPath(String);

impl FieldPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A class of the `REJECTED` group: the data itself is the problem, so sending it again fails the
/// same way.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    EnumString,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
#[serde(rename_all = "snake_case")]
pub enum RejectedErrorClass {
    /// A payload could not be decoded, or a row could not be encoded.
    Codec,
    /// A value did not fit.
    Validation,
    /// An expression could not be computed.
    Evaluation,
    /// The data is fine but larger than a declared limit.
    Limit,
    /// An outside system, a model or a guest said no to this data, for good.
    Refused,
}

impl RejectedErrorClass {
    /// Every class of the group, in the order the class tree lists them.
    pub const ALL: [Self; 5] = [
        Self::Codec,
        Self::Validation,
        Self::Evaluation,
        Self::Limit,
        Self::Refused,
    ];

    /// The position of this class in [`Self::ALL`].
    pub const fn index(self) -> usize {
        match self {
            Self::Codec => 0,
            Self::Validation => 1,
            Self::Evaluation => 2,
            Self::Limit => 3,
            Self::Refused => 4,
        }
    }
}

/// A class of the `FAILED` group: the data may be fine, so sending it again may work.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
    EnumString,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
#[serde(rename_all = "snake_case")]
pub enum FailedErrorClass {
    /// An outside system kept failing and the node gave up retrying.
    Unavailable,
    /// Over a rate limit.
    Throttled,
    /// No node reports the work alive any more.
    Timeout,
    /// Nervix itself cut the work, such as an enforced operation or a shutdown.
    Interrupted,
    /// Nervix, a connector, a model or a guest broke its contract.
    Internal,
}

impl FailedErrorClass {
    /// Every class of the group, in the order the class tree lists them.
    pub const ALL: [Self; 5] = [
        Self::Unavailable,
        Self::Throttled,
        Self::Timeout,
        Self::Interrupted,
        Self::Internal,
    ];
}

/// The group of the class tree a class belongs to, beneath its root `ANY`, which holds both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum MessageErrorClassGroup {
    Rejected,
    Failed,
}

/// One of the ten classes every error has: a fixed tree that users cannot extend, whose root `ANY`
/// holds the two groups the variants name. The variant is the class's typed ancestry, so a reader
/// tells the groups apart by matching, never by its text.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MessageErrorClass {
    Rejected(RejectedErrorClass),
    Failed(FailedErrorClass),
}

impl MessageErrorClass {
    pub const CODEC: Self = Self::Rejected(RejectedErrorClass::Codec);
    pub const VALIDATION: Self = Self::Rejected(RejectedErrorClass::Validation);
    pub const EVALUATION: Self = Self::Rejected(RejectedErrorClass::Evaluation);
    pub const LIMIT: Self = Self::Rejected(RejectedErrorClass::Limit);
    pub const REFUSED: Self = Self::Rejected(RejectedErrorClass::Refused);
    pub const UNAVAILABLE: Self = Self::Failed(FailedErrorClass::Unavailable);
    pub const THROTTLED: Self = Self::Failed(FailedErrorClass::Throttled);
    pub const TIMEOUT: Self = Self::Failed(FailedErrorClass::Timeout);
    pub const INTERRUPTED: Self = Self::Failed(FailedErrorClass::Interrupted);
    pub const INTERNAL: Self = Self::Failed(FailedErrorClass::Internal);

    /// Every class, `REJECTED` before `FAILED`, in the order the class tree lists them.
    pub const ALL: [Self; 10] = [
        Self::CODEC,
        Self::VALIDATION,
        Self::EVALUATION,
        Self::LIMIT,
        Self::REFUSED,
        Self::UNAVAILABLE,
        Self::THROTTLED,
        Self::TIMEOUT,
        Self::INTERRUPTED,
        Self::INTERNAL,
    ];

    /// The position of this class in [`Self::ALL`].
    pub const fn index(self) -> usize {
        match self {
            Self::Rejected(class) => class.index(),
            Self::Failed(FailedErrorClass::Unavailable) => 5,
            Self::Failed(FailedErrorClass::Throttled) => 6,
            Self::Failed(FailedErrorClass::Timeout) => 7,
            Self::Failed(FailedErrorClass::Interrupted) => 8,
            Self::Failed(FailedErrorClass::Internal) => 9,
        }
    }

    /// The group this class belongs to.
    pub const fn group(self) -> MessageErrorClassGroup {
        match self {
            Self::Rejected(_) => MessageErrorClassGroup::Rejected,
            Self::Failed(_) => MessageErrorClassGroup::Failed,
        }
    }

    /// The class's name: what `error.class` reads, a metric's `class` label holds and a log
    /// records, such as `refused`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rejected(class) => class.into(),
            Self::Failed(class) => class.into(),
        }
    }
}

impl From<RejectedErrorClass> for MessageErrorClass {
    fn from(class: RejectedErrorClass) -> Self {
        Self::Rejected(class)
    }
}

impl From<FailedErrorClass> for MessageErrorClass {
    fn from(class: FailedErrorClass) -> Self {
        Self::Failed(class)
    }
}

/// The code an outside system gave a failure, in that system's own spelling, such as the
/// PostgreSQL SQLSTATE `23505`. Nervix never invents one and never rewrites it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExternalErrorCode(String);

impl ExternalErrorCode {
    /// The code `code` spells, or `None` when the outside system's text is empty, which names no
    /// code.
    pub fn new(code: impl Into<String>) -> Option<Self> {
        let code = code.into();
        if code.is_empty() {
            return None;
        }
        Some(Self(code))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
#[serde(rename_all = "snake_case")]
pub enum MessageErrorOperation {
    Admit,
    SourceWhere,
    FilterWhere,
    Inferencer,
    Wasm,
    /// The admission or the aggregate arguments of a window processor's row.
    Window,
    Inherit,
    Set,
    Finalize,
    RouteWhere,
    BranchSet,
    Values,
    Invoke,
    Encode,
    Publish,
    /// A source payload its ingestor's codec refused.
    Decode,
    /// A source payload its ingestor's `RATE LIMIT` can never admit, or whose measure failed.
    RateLimit,
}

impl MessageErrorOperation {
    /// Every operation, in declaration order.
    pub const ALL: [Self; 17] = [
        Self::Admit,
        Self::SourceWhere,
        Self::FilterWhere,
        Self::Inferencer,
        Self::Wasm,
        Self::Window,
        Self::Inherit,
        Self::Set,
        Self::Finalize,
        Self::RouteWhere,
        Self::BranchSet,
        Self::Values,
        Self::Invoke,
        Self::Encode,
        Self::Publish,
        Self::Decode,
        Self::RateLimit,
    ];

    /// The position of this operation in [`Self::ALL`].
    pub const fn index(self) -> usize {
        match self {
            Self::Admit => 0,
            Self::SourceWhere => 1,
            Self::FilterWhere => 2,
            Self::Inferencer => 3,
            Self::Wasm => 4,
            Self::Window => 5,
            Self::Inherit => 6,
            Self::Set => 7,
            Self::Finalize => 8,
            Self::RouteWhere => 9,
            Self::BranchSet => 10,
            Self::Values => 11,
            Self::Invoke => 12,
            Self::Encode => 13,
            Self::Publish => 14,
            Self::Decode => 15,
            Self::RateLimit => 16,
        }
    }
}

/// An error as the operation that failed raises it, before the node that ran the operation names
/// itself on it.
///
/// VM programs, codecs and connectors are engines shared by every node, so an operation knows
/// what failed and why, never for which graph node it ran. The node that handles the error turns it
/// into the [`StructuredMessageError`] its route, log, counter and notice read, through
/// [`Self::attributed_to`]. `Class` is [`MessageErrorClass`] for every error but an emitter's
/// record rejection, whose class the type keeps in the `REJECTED` group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaisedMessageError<Class = MessageErrorClass> {
    pub reference: Uuid,
    pub class: Class,
    pub message: String,
    pub operation: MessageErrorOperation,
    pub operation_index: Option<u32>,
    pub fields: SortedSet<FieldPath>,
    pub occurred_at: Timestamp,
    pub external_code: Option<ExternalErrorCode>,
}

impl<Class: Into<MessageErrorClass>> RaisedMessageError<Class> {
    /// This error as any class.
    pub fn widen(self) -> RaisedMessageError {
        RaisedMessageError {
            reference: self.reference,
            class: self.class.into(),
            message: self.message,
            operation: self.operation,
            operation_index: self.operation_index,
            fields: self.fields,
            occurred_at: self.occurred_at,
            external_code: self.external_code,
        }
    }

    /// The record of this error that `node`, whose operation raised it, keeps.
    pub fn attributed_to(self, node: NodeRef) -> StructuredMessageError {
        StructuredMessageError {
            reference: self.reference,
            class: self.class.into(),
            message: self.message,
            operation: self.operation,
            operation_index: self.operation_index,
            fields: self.fields,
            occurred_at: self.occurred_at,
            node,
            external_code: self.external_code,
        }
    }
}

/// The record of an error the node that produced it keeps: one schema wherever a user reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredMessageError {
    pub reference: Uuid,
    pub class: MessageErrorClass,
    pub message: String,
    pub operation: MessageErrorOperation,
    pub operation_index: Option<u32>,
    pub fields: SortedSet<FieldPath>,
    pub occurred_at: Timestamp,
    /// The graph node whose operation raised the error.
    pub node: NodeRef,
    /// The outside system's own code, when the failure carries one.
    pub external_code: Option<ExternalErrorCode>,
}

impl StructuredMessageError {
    /// The class and node a runtime event that reports this error carries beside its text.
    pub fn reported(&self) -> ReportedError {
        ReportedError {
            class: self.class,
            node: self.node.clone(),
        }
    }
}

/// The class of an error and the graph node that produced it, as a runtime event, the
/// interconnect's runtime error event and a client's server notice carry them beside their text.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ReportedError {
    pub class: MessageErrorClass,
    pub node: NodeRef,
}

/// The names of the `error` scope's fields, which the scope's schema declares and the runtime
/// fills, so the two read one spelling.
pub mod message_error_scope {
    pub const REFERENCE: &str = "reference";
    pub const CLASS: &str = "class";
    pub const MESSAGE: &str = "message";
    pub const OPERATION: &str = "operation";
    pub const OPERATION_INDEX: &str = "operation_index";
    pub const FIELDS: &str = "fields";
    pub const OCCURRED_AT: &str = "occurred_at";
    pub const NODE: &str = "node";
    pub const NODE_KIND: &str = "node_kind";
    pub const EXTERNAL_CODE: &str = "external_code";
}

#[cfg(feature = "arrow")]
impl StructuredMessageError {
    /// The Arrow schema of the `error` scope an error route reads: the one definition that the
    /// registry validates a route against and the runtime compiles it against.
    ///
    /// The node is projected into two text fields, its name and its kind's snake_case name,
    /// because Plexus has no record values and a node's name is unique only within its kind.
    pub fn scope_arrow_schema() -> nervix_primitives::sync::StdArc<arrow_schema::Schema> {
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use message_error_scope::*;
        nervix_primitives::sync::StdArc::new(Schema::new(vec![
            Field::new(REFERENCE, DataType::Utf8, false),
            Field::new(CLASS, DataType::Utf8, false),
            Field::new(MESSAGE, DataType::Utf8, false),
            Field::new(OPERATION, DataType::Utf8, false),
            Field::new(OPERATION_INDEX, DataType::UInt32, true),
            Field::new(
                FIELDS,
                DataType::List(nervix_primitives::sync::StdArc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    false,
                ))),
                false,
            ),
            Field::new(
                OCCURRED_AT,
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                false,
            ),
            Field::new(NODE, DataType::Utf8, false),
            Field::new(NODE_KIND, DataType::Utf8, false),
            Field::new(EXTERNAL_CODE, DataType::Utf8, true),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_class_sits_at_its_index_in_its_group_and_reads_its_own_name() {
        for (index, class) in MessageErrorClass::ALL.into_iter().enumerate() {
            assert_eq!(class.index(), index);
        }
        let names = MessageErrorClass::ALL.map(MessageErrorClass::as_str);
        assert_eq!(
            names,
            [
                "codec",
                "validation",
                "evaluation",
                "limit",
                "refused",
                "unavailable",
                "throttled",
                "timeout",
                "interrupted",
                "internal",
            ]
        );
        for class in RejectedErrorClass::ALL {
            assert_eq!(
                MessageErrorClass::from(class).group(),
                MessageErrorClassGroup::Rejected
            );
        }
        for class in FailedErrorClass::ALL {
            assert_eq!(
                MessageErrorClass::from(class).group(),
                MessageErrorClassGroup::Failed
            );
        }
    }

    #[test]
    fn every_operation_sits_at_its_index() {
        for (index, operation) in MessageErrorOperation::ALL.into_iter().enumerate() {
            assert_eq!(operation.index(), index);
        }
    }

    #[test]
    fn an_empty_outside_code_names_no_code() {
        assert_eq!(ExternalErrorCode::new(""), None);
        assert_eq!(
            ExternalErrorCode::new("23505").map(|code| code.as_str().to_owned()),
            Some("23505".to_owned())
        );
    }

    #[test]
    fn attribution_names_the_node_and_keeps_everything_the_operation_raised() {
        let raised = RaisedMessageError {
            reference: Uuid::from_u128(1),
            class: RejectedErrorClass::Refused,
            message: "refused".to_owned(),
            operation: MessageErrorOperation::Publish,
            operation_index: Some(2),
            fields: SortedSet::from_unsorted(vec![FieldPath::new("input.id")]),
            occurred_at: Timestamp::from_unix_nanos(7),
            external_code: ExternalErrorCode::new("23505"),
        };
        let node = NodeRef::new(
            crate::ModelKind::Emitter,
            crate::ModelName::parse("orders_db").expect("a valid name"),
        );
        let attributed = raised.clone().attributed_to(node.clone());
        assert_eq!(attributed.reference, raised.reference);
        assert_eq!(attributed.class, MessageErrorClass::REFUSED);
        assert_eq!(attributed.message, raised.message);
        assert_eq!(attributed.operation, raised.operation);
        assert_eq!(attributed.operation_index, raised.operation_index);
        assert_eq!(attributed.fields, raised.fields);
        assert_eq!(attributed.occurred_at, raised.occurred_at);
        assert_eq!(attributed.external_code, raised.external_code);
        assert_eq!(attributed.node, node);
        assert_eq!(
            attributed.reported(),
            ReportedError {
                class: MessageErrorClass::REFUSED,
                node,
            }
        );
    }
}
