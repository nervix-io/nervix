//! How one side of an entity keeps the transport headers of a message.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The header model an entity declares for each side: what a repeated header name
//!   keeps, the order in which names travel, how names compare, the exact types a value has and
//!   what the transport carries each as, and which literal `write_header` calls of an emitter the
//!   model makes meaningless.
//! - **Depends on.** The scalar types a transport carries, the standard library, strum and
//!   indexmap.
//! - **Must not know.** Any one connector, how a connector reads or writes its transport's
//!   headers, or the grammar that spells `read_header` and `write_header`.
//!
//! A connector keeps its transport's own header semantics; this module only states them. The
//! registry, `DESCRIBE`, the manuals and the capability reference read the one declaration each
//! entity makes, so none of them restates what a transport does with a header.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{
    borrow::Cow,
    collections::{BTreeMap, btree_map::Entry},
    fmt::{Display, Formatter},
};

use indexmap::IndexMap;
use meticulous::OptionExt as _;
use strum::{AsRefStr, EnumIter, EnumProperty};

use crate::PropertyType;

/// What a repeated header name keeps, and the order in which the names of one message travel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, EnumIter, EnumProperty)]
#[strum(serialize_all = "snake_case")]
pub enum HeaderLayout {
    /// Every header in the order it was written, a repeated name once for each of its values,
    /// such as the record headers of a Kafka message.
    #[strum(props(
        meaning = "every header in the order it was written; a repeated name keeps each of its \
                   values in its place"
    ))]
    OrderedRepeats,
    /// Every value of a name, kept together under that name in the order written, while the
    /// order of different names is not declared, such as the headers of a NATS message.
    #[strum(props(
        meaning = "every value of a name, together and in the order written; the order of \
                   different names is not declared"
    ))]
    RepeatsByName,
    /// One value per name, the last one written for it, while the order of names is not
    /// declared, such as the attributes of an SQS message.
    #[strum(props(
        meaning = "one value per name, the last one written for it; the order of names is not \
                   declared"
    ))]
    LastWins,
    /// One value per name, the last one written for it, with the names in ascending byte order,
    /// such as the field table of an AMQP message.
    #[strum(props(
        meaning = "one value per name, the last one written for it; names in ascending byte order"
    ))]
    SortedByName,
}

impl HeaderLayout {
    /// Whether a later write of a name replaces the value an earlier write gave it, so a message
    /// carries one value per name.
    pub const fn keeps_one_value_per_name(self) -> bool {
        match self {
            Self::OrderedRepeats | Self::RepeatsByName => false,
            Self::LastWins | Self::SortedByName => true,
        }
    }

    /// What this layout means for the headers of one message, as the manuals state it.
    pub fn meaning(self) -> &'static str {
        self.get_str("meaning")
            .assured("the strum property is declared on every variant of this enum")
    }
}

/// How the header names of one side of an entity compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, EnumIter, EnumProperty)]
#[strum(serialize_all = "snake_case")]
pub enum HeaderNames {
    /// A name keeps the letter case it was written in and matches only a name spelled the same.
    #[strum(props(meaning = "a name matches only a name spelled the same, letter case included"))]
    Exact,
    /// A name is recorded in ASCII lowercase, so names that differ only in letter case are one
    /// name, as HTTP field names are.
    #[strum(props(
        meaning = "a name is recorded in ASCII lowercase, so names that differ only in letter \
                   case are one name"
    ))]
    Lowercase,
}

impl HeaderNames {
    /// `name` as these names record it: as written, or in ASCII lowercase.
    pub fn recorded(self, name: &str) -> Cow<'_, str> {
        match self {
            Self::Exact => Cow::Borrowed(name),
            Self::Lowercase => Cow::Owned(name.to_ascii_lowercase()),
        }
    }

    /// What these names mean for a header name, as the manuals state it.
    pub fn meaning(self) -> &'static str {
        self.get_str("meaning")
            .assured("the strum property is declared on every variant of this enum")
    }
}

/// One exact type a header value may have, and the value type its transport carries it as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HeaderValueType {
    pub ty: PropertyType,
    /// The transport's own name for the type it carries a value of `ty` as, such as SQS's
    /// `Number`.
    pub carried_as: &'static str,
}

/// The exact types the values of one side's headers have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HeaderValues {
    /// Every value is a `STRING`, carried as the transport's text value.
    Text,
    /// Every value has exactly one of these types, which the transport carries as the value type
    /// each names, such as the string, number and binary attributes of an SQS message.
    Typed(&'static [HeaderValueType]),
}

impl HeaderValues {
    /// Every exact type a value may have, in declared order.
    pub fn types(self) -> Vec<PropertyType> {
        match self {
            Self::Text => vec![PropertyType::String],
            Self::Typed(types) => types.iter().map(|declared| declared.ty).collect(),
        }
    }

    /// The exact types a value may have, as a diagnostic lists them: `STRING`, or `STRING, U8 or
    /// BYTES`.
    pub fn listed(self) -> String {
        let types = self.types();
        let Some((last, leading)) = types.split_last() else {
            return String::new();
        };
        if leading.is_empty() {
            return last.to_string();
        }
        let mut names = Vec::with_capacity(leading.len());
        for ty in leading {
            names.push(ty.to_string());
        }
        format!("{} or {last}", names.join(", "))
    }

    /// What the transport carries each value type as, as the manuals and `DESCRIBE` state it:
    /// `STRING as String; U8, I64 as Number`. Text values have nothing to state, because their
    /// transport carries text.
    pub fn carried(self) -> Option<String> {
        let Self::Typed(types) = self else {
            return None;
        };
        // The types one transport value type carries are stated together, in the order the
        // declaration first names that value type.
        let mut carriers: IndexMap<&'static str, Vec<String>> = IndexMap::new();
        for declared in types {
            carriers
                .entry(declared.carried_as)
                .or_default()
                .push(declared.ty.to_string());
        }
        let mut carried = Vec::with_capacity(carriers.len());
        for (carried_as, types) in carriers {
            carried.push(format!("{} as {carried_as}", types.join(", ")));
        }
        Some(carried.join("; "))
    }
}

/// How one side of an entity keeps the transport headers of a message: what `read_header` and
/// `read_headers` find on a message an ingestor reads, or what the `write_header` calls of one
/// record become on the message an emitter publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HeaderModel {
    /// What a repeated name keeps, and the order in which names travel.
    pub layout: HeaderLayout,
    /// How names compare.
    pub names: HeaderNames,
    /// The exact types a value has.
    pub values: HeaderValues,
}

impl HeaderModel {
    /// A model whose names match only a name spelled the same and whose values are text.
    pub const fn exact(layout: HeaderLayout) -> Self {
        Self {
            layout,
            names: HeaderNames::Exact,
            values: HeaderValues::Text,
        }
    }

    /// A model whose names are recorded in ASCII lowercase and whose values are text.
    pub const fn lowercase(layout: HeaderLayout) -> Self {
        Self {
            layout,
            names: HeaderNames::Lowercase,
            values: HeaderValues::Text,
        }
    }

    /// This model, with values of exactly the declared types.
    pub const fn with_typed_values(self, types: &'static [HeaderValueType]) -> Self {
        Self {
            values: HeaderValues::Typed(types),
            ..self
        }
    }

    /// The first of an emitter's literal header writes, in written order, whose name an earlier
    /// literal write already wrote, where this model keeps one value per name.
    ///
    /// On such an emitter the later write replaces the earlier one on every record, so the earlier
    /// write can never reach a message. Names compare as this model records them, and a model
    /// that keeps every value of a name has no such write.
    pub fn replaced_write<'a>(
        &self,
        writes: impl IntoIterator<Item = LiteralHeaderWrite<'a>>,
    ) -> Option<ReplacedHeaderWrite<'a>> {
        if !self.layout.keeps_one_value_per_name() {
            return None;
        }
        let mut written: BTreeMap<Cow<'a, str>, LiteralHeaderWrite<'a>> = BTreeMap::new();
        for write in writes {
            let recorded = self.names.recorded(write.name);
            match written.entry(recorded) {
                Entry::Occupied(earlier) => {
                    return Some(ReplacedHeaderWrite {
                        earlier: *earlier.get(),
                        later: write,
                    });
                }
                Entry::Vacant(slot) => {
                    slot.insert(write);
                }
            }
        }
        None
    }
}

/// A model is written as its layout and its names, such as `ordered_repeats, exact names`, the
/// way `DESCRIBE` shows it, followed by what its transport carries each value type as where its
/// values are typed: `last_wins, exact names; STRING as String; BYTES as Binary`.
impl Display for HeaderModel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, {} names", self.layout.as_ref(), self.names.as_ref())?;
        match self.values.carried() {
            Some(carried) => write!(f, "; {carried}"),
            None => Ok(()),
        }
    }
}

/// One `write_header` call of an emitter whose name is a string literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiteralHeaderWrite<'a> {
    /// The call's place among the calls of the emitter's `INVOKE` clause, counted from 1.
    pub position: usize,
    /// The name as the statement writes it.
    pub name: &'a str,
}

/// Two literal header writes of one name on an emitter that keeps one value per name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplacedHeaderWrite<'a> {
    /// The write whose value the later one replaces.
    pub earlier: LiteralHeaderWrite<'a>,
    /// The write that replaces it.
    pub later: LiteralHeaderWrite<'a>,
}

#[cfg(test)]
#[path = "header_model_tests.rs"]
mod tests;
