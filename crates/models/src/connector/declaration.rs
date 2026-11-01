//! The shapes in which an entity declares what a statement can say about it, on either side.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The declaration of the name that locates an entity, of the options a statement
//!   writes for it with their values and choices, of the option choices a mode needs written
//!   beside it, of the envelope clauses and properties an emitter can set, and the capabilities a
//!   statement can make an entity exercise at run time, which a scenario observes.
//! - **Depends on.** The connector kinds and the schema types a declared value has.
//! - **Must not know.** Any one connector or entity, the grammar that spells a declaration, or how
//!   a host executes what an entity declares.
//!
//! Each connector's module writes its entities' declarations in these shapes, and every layer
//! reads them from there: the grammar its labels, generated statements its examples, the registry
//! its rules, documentation all of it. An example is a complete clause written as a statement
//! writes it, so documentation shows it and generated conformance tests parse it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::fmt::{Display, Formatter};

use strum::EnumIter;

use crate::{
    ConnectorKind, DeadlineKey, DeclaredBounds, EmitterPublishingModeKind, IngestDeliveryKind,
    ParseAsType,
};

/// What completion and documentation say about the external names of one kind, which a
/// statement writes as string literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalNameDeclaration {
    /// The label completion offers where a name of this kind is expected, such as `kafka_topic`.
    pub label: &'static str,
    /// One name its rule admits, which documentation and generated statements write.
    pub example: &'static str,
}

/// An external name type: text an external system names an entity by, checked by its rule.
///
/// `external_names!` implements it for every name type it declares, so the grammar reads a
/// name's label from the type it constructs rather than restating it.
pub trait ExternalName: Clone + 'static {
    const DECLARATION: ExternalNameDeclaration;

    /// Checks `raw` against this kind's rule and keeps it exactly as written.
    fn parse(raw: &str) -> error_stack::Result<Self, crate::ExternalNameError>;
}

/// How a statement writes the name that locates an entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocatorName {
    /// A string literal the entity's external-name rule checks, which a connector sends exactly
    /// as written.
    External(ExternalNameDeclaration),
    /// Words a statement writes unquoted, as the entity's grammar reads them, which completion
    /// offers under `label`.
    Unquoted { label: &'static str },
}

/// The name that locates what an entity reads or writes, and the keywords a statement writes
/// before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocatorDeclaration {
    /// The keywords that open the locator, such as `TOPIC` or `INSERT TO TABLE`.
    pub keywords: &'static str,
    pub name: LocatorName,
}

impl LocatorDeclaration {
    /// The locator written in full, as documentation and generated statements write it.
    pub fn example(&self) -> String {
        match self.name {
            LocatorName::External(name) => format!("{} '{}'", self.keywords, name.example),
            LocatorName::Unquoted { .. } => format!("{} {}", self.keywords, UNQUOTED_EXAMPLE),
        }
    }
}

/// The word documentation and generated statements write where an unquoted name locates an
/// entity.
const UNQUOTED_EXAMPLE: &str = "orders";

/// The type of the value a statement writes after an option's keywords.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionValue {
    /// A whole count greater than zero.
    Count,
    /// A duration literal.
    Duration,
    /// A string literal Nervix passes on as written, such as a PromQL query or a location.
    Text,
    /// A string literal an external system names something by, checked by its rule.
    Name(ExternalNameDeclaration),
    /// An expression evaluated for every record.
    Expression,
    /// `{ '<name>' = <expression>, … }`: named expressions.
    Mapping,
    /// `{ '<name>' = '<value>', … }`: named string literals.
    LiteralMapping,
    /// A configured client of one connector.
    Client(ConnectorKind),
    /// A clause of several parts, as the option's example writes them.
    Clause,
    /// A byte size: a whole number greater than zero followed by its unit, such as `16MiB`.
    Size,
}

/// One choice an option offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptionChoice {
    /// What names the choice: its keywords, the literal it writes, or the kind of value it takes
    /// in angle brackets. A requirement and a capability name the choice by it.
    pub name: &'static str,
    /// The value a statement writes with the choice, where it takes one: after the choice's own
    /// words, or before them where the example writes it there.
    pub value: Option<OptionValue>,
    /// The option written in full with this choice, its keywords included.
    pub example: &'static str,
}

/// How an option is written after its keywords.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionForm {
    /// One value of a type, as `example` writes the option in full.
    Value {
        value: OptionValue,
        example: &'static str,
    },
    /// One of a closed set of choices.
    Choice(&'static [OptionChoice]),
}

/// Whether a statement may leave an option out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionPresence {
    /// Every statement writes the option.
    Required,
    /// A statement may leave the option out. `default` names the choice or the value that holds
    /// then, for an option that has one; without one, leaving the option out turns off what it
    /// configures.
    Optional { default: Option<&'static str> },
}

/// One option a statement writes for an entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptionDeclaration {
    /// The keywords that open the option, such as `OFFSET BY` or `INSTANCES`.
    pub keywords: &'static str,
    pub presence: OptionPresence,
    pub form: OptionForm,
}

impl OptionDeclaration {
    /// `INSTANCES <n>`: how many consumers read the entity together, one where a statement leaves
    /// it out.
    pub const INSTANCES: Self = Self {
        keywords: "INSTANCES",
        presence: OptionPresence::Optional { default: Some("1") },
        form: OptionForm::Value {
            value: OptionValue::Count,
            example: "INSTANCES 2",
        },
    };

    /// `EVERY <duration>`: the domain-time cadence on which a polled source reads.
    pub const EVERY: Self = Self {
        keywords: "EVERY",
        presence: OptionPresence::Required,
        form: OptionForm::Value {
            value: OptionValue::Duration,
            example: "EVERY 1s",
        },
    };

    /// This option, written by every statement.
    pub const fn required(self) -> Self {
        Self {
            presence: OptionPresence::Required,
            ..self
        }
    }

    /// The option written in full: once for an option that takes a value, once per choice for
    /// one that offers choices.
    pub fn examples(&self) -> Vec<&'static str> {
        match self.form {
            OptionForm::Value { example, .. } => vec![example],
            OptionForm::Choice(choices) => choices.iter().map(|choice| choice.example).collect(),
        }
    }

    /// The choice named `name`, for an option that offers it.
    ///
    /// An option offers a handful of choices, all declared in the option's own constant, so
    /// scanning them is bounded by the declaration rather than by any statement.
    pub fn choice(&self, name: &str) -> Option<&'static OptionChoice> {
        let OptionForm::Choice(choices) = self.form else {
            return None;
        };
        choices.iter().find(|choice| choice.name == name)
    }

    pub fn is_required(&self) -> bool {
        self.presence == OptionPresence::Required
    }
}

/// A choice of one of an entity's options that a mode needs written beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChoiceRequirement {
    /// The keywords of the option.
    pub option: &'static str,
    /// The name of the choice the statement must make.
    pub choice: &'static str,
}

/// A clause an emitter writes to set what its transport carries beside each payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter)]
pub enum EnvelopeClauseKind {
    Persistent,
    Mandatory,
    Retain,
    Timestamp,
    DeliverAfter,
    Delay,
    ReplyTo,
    MessageId,
    DeduplicationId,
}

impl EnvelopeClauseKind {
    /// The keywords that open the clause.
    pub const fn keywords(self) -> &'static str {
        match self {
            Self::Persistent => "PERSISTENT",
            Self::Mandatory => "MANDATORY",
            Self::Retain => "RETAIN",
            Self::Timestamp => "TIMESTAMP",
            Self::DeliverAfter => "DELIVER AFTER",
            Self::Delay => "DELAY",
            Self::ReplyTo => "REPLY TO",
            Self::MessageId => "MESSAGE ID",
            Self::DeduplicationId => "DEDUPLICATION ID",
        }
    }

    /// The clause written in full, as documentation and generated statements write it.
    pub const fn example(self) -> &'static str {
        match self {
            Self::Persistent => "PERSISTENT",
            Self::Mandatory => "MANDATORY",
            Self::Retain => "RETAIN",
            Self::Timestamp => "TIMESTAMP occurred_at",
            Self::DeliverAfter => "DELIVER AFTER 5s",
            Self::Delay => "DELAY 5s",
            Self::ReplyTo => "REPLY TO reply_queue",
            Self::MessageId => "MESSAGE ID id",
            Self::DeduplicationId => "DEDUPLICATION ID id",
        }
    }

    /// What a statement writes after the clause's keywords.
    pub fn form(self) -> EnvelopeClauseForm {
        match self {
            Self::Persistent | Self::Mandatory | Self::Retain => EnvelopeClauseForm::Flag,
            Self::DeliverAfter | Self::Delay => EnvelopeClauseForm::Duration,
            Self::Timestamp => EnvelopeClauseForm::Expression(ParseAsType::Datetime),
            Self::ReplyTo | Self::MessageId | Self::DeduplicationId => {
                EnvelopeClauseForm::Expression(ParseAsType::String)
            }
        }
    }
}

/// What a statement writes after an envelope clause's keywords.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvelopeClauseForm {
    /// Nothing: writing the clause turns on what it names for every record.
    Flag,
    /// A duration literal, the same for every record.
    Duration,
    /// An expression evaluated for every record, whose value is never null and has exactly this
    /// type.
    Expression(ParseAsType),
}

/// One key of an emitter's closed `PROPERTIES { … }` set, and the exact type of its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropertyDeclaration {
    pub name: &'static str,
    pub ty: PropertyType,
}

/// The exact type of a scalar a transport carries beside a payload: the value of a property, or
/// of a header on a side whose [header model](crate::HeaderModel) declares typed values.
///
/// Such a value is never a float, whose values do not all equal themselves, so two records whose
/// properties and headers are equal can always share a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter)]
pub enum PropertyType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    Bool,
    String,
    Datetime,
    Bytes,
}

impl From<PropertyType> for ParseAsType {
    fn from(ty: PropertyType) -> Self {
        match ty {
            PropertyType::U8 => Self::U8,
            PropertyType::I8 => Self::I8,
            PropertyType::U16 => Self::U16,
            PropertyType::I16 => Self::I16,
            PropertyType::U32 => Self::U32,
            PropertyType::I32 => Self::I32,
            PropertyType::U64 => Self::U64,
            PropertyType::I64 => Self::I64,
            PropertyType::Bool => Self::Bool,
            PropertyType::String => Self::String,
            PropertyType::Datetime => Self::Datetime,
            PropertyType::Bytes => Self::Bytes,
        }
    }
}

/// A property's type is written as the schema type its value has.
impl Display for PropertyType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        ParseAsType::from(*self).fmt(f)
    }
}

/// One thing a statement can make an entity do at run time, which a scenario observes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuntimeCapability {
    /// An ingestor delivering in one declared mode.
    Delivery(IngestDeliveryKind),
    /// An emitter publishing in one declared mode.
    Publishing(EmitterPublishingModeKind),
    /// A statement writing an option that takes a value.
    Option { keywords: &'static str },
    /// A statement making one choice of an option, or leaving the option out where that choice
    /// is its default.
    OptionChoice {
        keywords: &'static str,
        choice: &'static str,
    },
    /// An ingestor's programs reading one typed metadata field.
    Metadata { field: &'static str },
    /// An emitter publishing in batches.
    Batch,
    /// An emitter setting one envelope clause.
    Envelope(EnvelopeClauseKind),
    /// A peer that stops answering during a wait the deadline key bounds surfacing as the
    /// entity's transient status within that key's bound, or, for a listener, which serves many
    /// peers, closing that peer's connection no sooner than the bound.
    StalledPeer(DeadlineKey),
    /// An emitter setting one property.
    Property { name: &'static str },
}

impl RuntimeCapability {
    /// The capabilities a statement exercises by writing the declared options.
    pub(crate) fn of_options(options: &[OptionDeclaration]) -> Vec<Self> {
        let mut capabilities = Vec::new();
        for option in options {
            match option.form {
                OptionForm::Value { .. } => capabilities.push(Self::Option {
                    keywords: option.keywords,
                }),
                OptionForm::Choice(choices) => {
                    for choice in choices {
                        capabilities.push(Self::OptionChoice {
                            keywords: option.keywords,
                            choice: choice.name,
                        });
                    }
                }
            }
        }
        capabilities
    }

    /// The capabilities an entity's bounds give it: a stalled-peer check for each deadline key its
    /// waits name.
    pub(crate) fn of_bounds(bounds: DeclaredBounds) -> Vec<Self> {
        let mut capabilities = Vec::new();
        for key in bounds.deadline_keys() {
            capabilities.push(Self::StalledPeer(key));
        }
        capabilities
    }
}

/// A capability is named the way its statement writes it, so an inventory of the scenarios that
/// observe one reads like the statements they run: `MODE NO ACK`, `OFFSET BY DOMAIN`,
/// `metadata.topic`, `BATCH`.
impl Display for RuntimeCapability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delivery(kind) => write!(f, "MODE {}", kind.phrase()),
            Self::Publishing(kind) => write!(f, "MODE {}", kind.label()),
            Self::Option { keywords } => f.write_str(keywords),
            Self::OptionChoice { keywords, choice } => write!(f, "{keywords} {choice}"),
            Self::Metadata { field } => write!(f, "metadata.{field}"),
            Self::Batch => f.write_str("BATCH"),
            Self::Envelope(kind) => f.write_str(kind.keywords()),
            Self::StalledPeer(key) => write!(f, "STALLED PEER {}", key.key()),
            Self::Property { name } => write!(f, "PROPERTIES {name}"),
        }
    }
}

#[cfg(test)]
#[path = "declaration_tests.rs"]
mod tests;
