//! The allowances an ingestor's body declares with `RATE LIMIT`, and which entities and delivery
//! modes can honor them.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The `RATE LIMIT` clause as a body writes it, the ordered clauses of one body, the
//!   declaration of whether an entity's ingestors can hold their source back in each delivery mode,
//!   and the refusal a statement meets where they cannot.
//! - **Depends on.** The structured expression Model, the ingestor entity and delivery kinds, and
//!   the shared duration and byte-size text rules.
//! - **Must not know.** The allowance engine, its clock, or how a host holds a source back.
//!
//! A clause keeps its counts and its period as written, so `16MiB` reads back as `16MiB`; what
//! they count is read through [`RateQuantity::units`] and [`RateLimit::period`], which the registry
//! checks and the plan converts once.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{num::NonZeroU64, time::Duration};

use error_stack::{Report, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{Expression, IngestDeliveryKind, IngestorEntityKind, parse_duration_text};

/// A count as a `RATE LIMIT` clause writes its rate or its burst: a positive whole number such as
/// `100`, or a byte size such as `16MiB`, which counts its bytes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(transparent)]
pub struct RateQuantity(String);

/// Why the text of a `RATE LIMIT` count is not one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "reading a written count is a pure refusal of the text"
    )
)]
pub enum RateQuantityError {
    #[error("`{text}` is not a whole number or a byte size")]
    Unreadable { text: String },
    #[error("a RATE LIMIT count must be greater than 0")]
    Zero,
}

impl RateQuantity {
    /// The quantity `text` writes, which counts at least one unit.
    pub fn parse(text: &str) -> error_stack::Result<Self, RateQuantityError> {
        let quantity = Self(text.to_owned());
        quantity.units()?;
        Ok(quantity)
    }

    /// The quantity as it is written.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// How many units the quantity counts: the number, or the bytes a byte size names.
    pub fn units(&self) -> error_stack::Result<NonZeroU64, RateQuantityError> {
        let units = match self.0.parse::<ubyte::ByteUnit>() {
            Ok(units) => units.as_u64(),
            Err(_) => {
                return Err(Report::new(RateQuantityError::Unreadable {
                    text: self.0.clone(),
                }));
            }
        };
        NonZeroU64::new(units).ok_or_else(|| Report::new(RateQuantityError::Zero))
    }
}

/// One `RATE LIMIT <count> [OF <measure>] PER <period> BURST <burst>` clause: an allowance that
/// holds at most `burst` units and refills at `count` units every `period`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RateLimit {
    pub count: RateQuantity,
    /// What one payload costs in this allowance. Without one, every payload costs one unit.
    pub measure: Option<Expression>,
    /// The period the count refills over, as written.
    pub period: String,
    pub burst: RateQuantity,
}

/// Why the period of a `RATE LIMIT` clause is not one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "reading a written period is a pure refusal of the text"
    )
)]
pub enum RatePeriodError {
    #[error("a RATE LIMIT period must be a duration")]
    Unreadable,
    #[error("a RATE LIMIT period must be longer than 0")]
    Zero,
}

impl RateLimit {
    /// The period the count refills over, which is longer than zero.
    pub fn period(&self) -> error_stack::Result<Duration, RatePeriodError> {
        let period =
            parse_duration_text(&self.period).change_context(RatePeriodError::Unreadable)?;
        if period.is_zero() {
            return Err(Report::new(RatePeriodError::Zero));
        }
        Ok(period)
    }

    /// Whether every payload costs one unit of this allowance, so that the allowance alone bounds
    /// how many payloads a read may take.
    pub fn counts_payloads(&self) -> bool {
        self.measure.is_none()
    }
}

/// The `RATE LIMIT` clauses of one ingestor body, in the order it writes them. A payload is
/// admitted once every one of them can pay for it.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct RateLimits(Vec<RateLimit>);

/// The clauses of a body that writes none.
pub(crate) static NO_RATE_LIMITS: RateLimits = RateLimits::NONE;

impl RateLimits {
    /// The clauses of `input`, which reads the pair `Source`, for an alteration that replaces or
    /// drops them. Whether the delivery mode of the complete result can hold its source back is
    /// checked once every operation applied.
    pub(crate) fn altered<Source>(
        input: &mut crate::IngestorInput,
    ) -> error_stack::Result<&mut Self, crate::AlterIngestorError>
    where
        Source: crate::connector::AlteredIngestorEntity,
    {
        Source::rate_limits_of_mut(input).ok_or_else(|| {
            Report::new(crate::AlterIngestorError::RateLimitUnsupported { kind: Source::KIND })
        })
    }

    /// A body that writes no `RATE LIMIT`.
    pub const NONE: Self = Self(Vec::new());

    /// The clauses in the order written.
    pub fn clauses(&self) -> &[RateLimit] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<RateLimit>> for RateLimits {
    fn from(clauses: Vec<RateLimit>) -> Self {
        Self(clauses)
    }
}

/// Whether an entity's ingestors can hold their source back, which a `RATE LIMIT` needs: a held
/// source stops taking payloads while an allowance is spent and loses none of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitDeclaration {
    /// Its transport keeps what the host has not taken, in every delivery mode it declares.
    Held,
    /// Its transport keeps what the host has not taken only in these delivery modes. In the
    /// others it pushes every message the moment it arrives.
    HeldIn(&'static [IngestDeliveryKind]),
    /// Its transport, or its peers, push every message the moment it arrives, in every mode.
    Unheld,
    /// It reads on its `EVERY` cadence, which already decides how often it reads.
    Paced,
}

impl RateLimitDeclaration {
    /// Whether an ingestor of the entity in the delivery mode `delivery` can hold its source back.
    pub fn holds(self, delivery: IngestDeliveryKind) -> bool {
        match self {
            Self::Held => true,
            Self::HeldIn(modes) => modes.contains(&delivery),
            Self::Unheld | Self::Paced => false,
        }
    }

    /// Whether some delivery mode of the entity holds its source back, so that a statement of it
    /// may write a `RATE LIMIT` and an alteration may set one.
    pub fn holds_in_some_mode(self) -> bool {
        match self {
            Self::Held | Self::HeldIn(_) => true,
            Self::Unheld | Self::Paced => false,
        }
    }

    /// The refusal of a `RATE LIMIT` an ingestor of `entity` writes in the delivery mode
    /// `delivery`, where that mode cannot hold its source back.
    pub fn refusal(
        self,
        entity: IngestorEntityKind,
        delivery: IngestDeliveryKind,
    ) -> Option<RateLimitRefusal> {
        match self {
            Self::Paced => Some(RateLimitRefusal::Paced { entity }),
            Self::Held | Self::HeldIn(_) | Self::Unheld => {
                if self.holds(delivery) {
                    None
                } else {
                    Some(RateLimitRefusal::Unheld { entity, delivery })
                }
            }
        }
    }
}

/// A `RATE LIMIT` written where its ingestor cannot honor one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "checking a RATE LIMIT against its entity's declaration is a pure refusal"
    )
)]
pub enum RateLimitRefusal {
    #[error(
        "{entity} ingestors in MODE {} cannot hold their source back, so they take no RATE LIMIT",
        .delivery.phrase()
    )]
    Unheld {
        entity: IngestorEntityKind,
        delivery: IngestDeliveryKind,
    },
    #[error("{entity} ingestors read on their EVERY cadence, so they take no RATE LIMIT")]
    Paced { entity: IngestorEntityKind },
}
