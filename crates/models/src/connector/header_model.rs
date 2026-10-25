//! How one side of an entity keeps the transport headers of a message.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The header model an entity declares for each side: what a repeated header name
//!   keeps, the order in which names travel, how names compare, and which literal `write_header`
//!   calls of an emitter the model makes meaningless.
//! - **Depends on.** Nothing beyond the standard library and strum.
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

use meticulous::OptionExt as _;
use strum::{AsRefStr, EnumIter, EnumProperty};

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

/// How one side of an entity keeps the transport headers of a message: what `read_header` and
/// `read_headers` find on a message an ingestor reads, or what the `write_header` calls of one
/// record become on the message an emitter publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HeaderModel {
    /// What a repeated name keeps, and the order in which names travel.
    pub layout: HeaderLayout,
    /// How names compare.
    pub names: HeaderNames,
}

impl HeaderModel {
    /// A model whose names match only a name spelled the same.
    pub const fn exact(layout: HeaderLayout) -> Self {
        Self {
            layout,
            names: HeaderNames::Exact,
        }
    }

    /// A model whose names are recorded in ASCII lowercase.
    pub const fn lowercase(layout: HeaderLayout) -> Self {
        Self {
            layout,
            names: HeaderNames::Lowercase,
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
/// way `DESCRIBE` shows it.
impl Display for HeaderModel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, {} names", self.layout.as_ref(), self.names.as_ref())
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
