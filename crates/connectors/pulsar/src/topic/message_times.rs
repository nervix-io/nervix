//! The times a Pulsar message carries from its record.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The event time a record's `TIMESTAMP` sets and the delivery time its `DELIVER AFTER`
//!   sets, each in the whole Unix milliseconds a Pulsar message counts, and why a record cannot
//!   carry them.
//! - **Depends on.** The connector contract's record and vocabulary timestamps.
//! - **Must not know.** The Pulsar driver, brokers, or how a message is published.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

use std::{num::NonZeroU64, time::Duration};

use nervix_connector::EmitterRecord;
use nervix_models::Timestamp;
use thiserror::Error;

use super::PULSAR;

/// The event time a Pulsar message carries, from the `TIMESTAMP` its record's envelope sets.
///
/// A timestamp counts nanoseconds and a message whole Unix milliseconds, so a message carries the
/// millisecond that contains it. The protocol's count is unsigned and reads zero as a message with
/// no event time, so the earliest event time is the Unix epoch's first millisecond.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PulsarEventTime {
    unix_millis: NonZeroU64,
}

impl TryFrom<Timestamp> for PulsarEventTime {
    type Error = PulsarMessageTimeError;

    fn try_from(timestamp: Timestamp) -> Result<Self, Self::Error> {
        let refused = PulsarMessageTimeError::EventTimeBeforeFirstMillisecond { timestamp };
        let Ok(unix_millis) = u64::try_from(timestamp.unix_millis()) else {
            return Err(refused);
        };
        let Some(unix_millis) = NonZeroU64::new(unix_millis) else {
            return Err(refused);
        };
        Ok(Self { unix_millis })
    }
}

/// The times one Pulsar message carries beside its payload, as its producer message holds them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PulsarMessageTimes {
    /// The `event_time`, absent where the record's envelope sets no `TIMESTAMP`.
    pub(super) event_time: Option<u64>,
    /// The `deliver_at_time`, absent where the record's envelope sets no `DELIVER AFTER`.
    pub(super) deliver_at_time: Option<i64>,
}

/// Why a record cannot carry the time its envelope sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the conversion returns a pure range validation refusal"
    )
)]
pub(super) enum PulsarMessageTimeError {
    /// The millisecond that contains the timestamp is not after the Unix epoch.
    #[error(
        "message TIMESTAMP {} precedes 1970-01-01T00:00:00.001Z, the earliest event time a \
         {PULSAR} message carries",
        .timestamp.to_rfc3339()
    )]
    EventTimeBeforeFirstMillisecond { timestamp: Timestamp },
    /// The delay leaves the range of a timestamp, which only a paced domain's logical time reaches.
    #[error(
        "DELIVER AFTER {deliver_after:?} past the execution time {} is later than any timestamp",
        .execution_time.to_rfc3339()
    )]
    DeliveryTimeOutOfRange {
        execution_time: Timestamp,
        deliver_after: Duration,
    },
}

impl PulsarMessageTimes {
    /// The times the message of `record` carries.
    ///
    /// They read only what the host retains with the record, its envelope and its batch's
    /// execution time, so every attempt to publish the record writes the same times.
    pub(super) fn of(record: &EmitterRecord) -> Result<Self, PulsarMessageTimeError> {
        let event_time = match record.envelope.timestamp {
            Some(timestamp) => Some(PulsarEventTime::try_from(timestamp)?.unix_millis.get()),
            None => None,
        };
        let deliver_at_time = match record.envelope.deliver_after {
            Some(deliver_after) => Some(Self::deliver_at(record.occurred_at, deliver_after)?),
            None => None,
        };
        Ok(Self {
            event_time,
            deliver_at_time,
        })
    }

    /// The Unix millisecond that contains `deliver_after` past `execution_time`. The field is
    /// signed, because a paced domain's logical time may precede the Unix epoch, and a broker
    /// delivers a message whose time has passed at once.
    fn deliver_at(
        execution_time: Timestamp,
        deliver_after: Duration,
    ) -> Result<i64, PulsarMessageTimeError> {
        let Ok(deliver_at) = execution_time.checked_add(deliver_after) else {
            return Err(PulsarMessageTimeError::DeliveryTimeOutOfRange {
                execution_time,
                deliver_after,
            });
        };
        Ok(deliver_at.unix_millis())
    }
}
