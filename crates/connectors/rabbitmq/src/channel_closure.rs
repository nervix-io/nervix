//! Why the broker closed a channel a RabbitMQ sink published on.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Listening to the events of the sink's connection, reading the one reason Lapin
//!   reports for each channel the broker closes, and recognizing in that reason the broker's
//!   refusal of a message body larger than its `max_message_size`.
//! - **Depends on.** Lapin's connection, events and errors, and Tokio's timer.
//! - **Must not know.** Records, the messages a write holds, publishing modes, outcomes, or the
//!   host.
//!
//! # One reason for each lost channel
//!
//! The broker names its reason when it closes a channel, and Lapin hands that reason only to what
//! was waiting on the channel at that moment: its pending publisher confirms and replies. A publish
//! or request that starts afterwards fails with the bare channel state instead. Which of the sink's
//! calls the reason reaches therefore depends on timing, so the sink never reads it from the call
//! that failed. Lapin also reports the reason once as an error event of the connection, whatever
//! was waiting, and the sink reads it there, exactly once for every channel it loses, so an event
//! it has not read yet always belongs to a channel it has not lost yet.
//!
//! A failed connection takes the channel with it and reports no reason of the channel's own, so a
//! channel lost with its connection is explained by the connection without waiting for an event,
//! and a failure that left the channel open was no loss at all. `CLOSE_REASON_BUDGET` bounds the
//! wait only for the one close that reports no reason: a close carrying the success reply code,
//! which RabbitMQ never sends a publisher.
//!
//! # The broker's size refusal
//!
//! RabbitMQ compares the body of each published message, and nothing written around it, with its
//! `max_message_size`. A larger body closes the channel with reply code 406 and the text
//! `PRECONDITION_FAILED - message size <size> is larger than configured max size <limit>`, or
//! `... larger than max size <limit>` when the limit is the broker's own 512 MiB ceiling.

use std::{pin::Pin, time::Duration};

use futures_util::{Stream, StreamExt as _};
use lapin::{
    Channel, Connection, ErrorKind, Event,
    protocol::{AMQPErrorKind, AMQPSoftError},
};

/// How long the sink waits for its connection to report why the broker closed a channel. Lapin
/// reports the reason as soon as it has answered the broker's close, so the wait normally ends at
/// once; the bound covers only a close that reports no reason.
const CLOSE_REASON_BUDGET: Duration = Duration::from_secs(5);

/// What every reply text of the broker's size refusal begins with, up to the refused size.
const SIZE_REFUSAL: &str = "PRECONDITION_FAILED - message size ";
/// What follows the refused size when the limit is the broker's configured `max_message_size`.
const CONFIGURED_LIMIT: &str = "is larger than configured max size ";
/// What follows the refused size when the limit is the broker's own ceiling.
const CEILING_LIMIT: &str = "is larger than max size ";

/// Why the broker closed a channel the sink published on.
#[derive(Debug)]
pub(crate) enum ChannelClosure {
    /// The broker refused a message whose body is larger than `limit` bytes, its
    /// `max_message_size`, and closed the channel the message arrived on. `reason` is the broker's
    /// own account of it.
    MessageTooLarge { limit: usize, reason: lapin::Error },
    /// The connection failed, or the broker closed the channel for a reason no single message
    /// explains.
    Unattributed(lapin::Error),
}

impl From<lapin::Error> for ChannelClosure {
    fn from(reason: lapin::Error) -> Self {
        if let ErrorKind::ProtocolError(error) = reason.kind()
            && let AMQPErrorKind::Soft(AMQPSoftError::PRECONDITIONFAILED) = error.kind()
            && let Some(limit) = Self::refused_limit(error.get_message().as_str())
        {
            return Self::MessageTooLarge { limit, reason };
        }
        Self::Unattributed(reason)
    }
}

impl ChannelClosure {
    /// The limit a reply text of the broker's size refusal names, and nothing for any other text.
    fn refused_limit(text: &str) -> Option<usize> {
        let refusal = text.strip_prefix(SIZE_REFUSAL)?;
        let (size, comparison) = refusal.split_once(' ')?;
        if size.parse::<usize>().is_err() {
            return None;
        }
        let limit = match comparison.strip_prefix(CONFIGURED_LIMIT) {
            Some(limit) => limit,
            None => comparison.strip_prefix(CEILING_LIMIT)?,
        };
        limit.parse().ok()
    }
}

/// The error events of the connection a sink publishes through.
pub(crate) struct ChannelClosures {
    events: Pin<Box<dyn Stream<Item = Event> + Send>>,
}

impl ChannelClosures {
    /// Starts listening to `connection`. Lapin reports an event only to a listener that exists when
    /// it happens, so this runs before the sink opens its first channel.
    pub(crate) fn listen(connection: &Connection) -> Self {
        Self {
            events: Box::pin(connection.events_listener()),
        }
    }

    /// Why `channel`, on which the sink just saw `observed` fail, is gone. `observed` stands for the
    /// reason when the channel is still open, when its connection failed, or when no reason
    /// arrives.
    pub(crate) async fn next(
        &mut self,
        connection: &Connection,
        channel: &Channel,
        observed: lapin::Error,
    ) -> ChannelClosure {
        if channel.status().connected() || !connection.status().connected() {
            return ChannelClosure::Unattributed(observed);
        }
        let reported = tokio::time::timeout(CLOSE_REASON_BUDGET, self.next_error()).await;
        match reported {
            Ok(Some(reason)) => ChannelClosure::from(reason),
            Ok(None) | Err(_) => ChannelClosure::Unattributed(observed),
        }
    }

    /// The next error the connection reports, skipping its other events, or nothing once the
    /// connection is gone.
    async fn next_error(&mut self) -> Option<lapin::Error> {
        loop {
            tokio::task::consume_budget().await;
            let event = self.events.next().await?;
            if let Event::Error(error) = event {
                return Some(error);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use lapin::protocol::{AMQPError, AMQPHardError};

    use super::*;

    fn protocol_error(kind: AMQPErrorKind, text: &str) -> lapin::Error {
        lapin::Error::from(ErrorKind::ProtocolError(AMQPError::new(kind, text.into())))
    }

    fn precondition_failed(text: &str) -> lapin::Error {
        protocol_error(AMQPErrorKind::Soft(AMQPSoftError::PRECONDITIONFAILED), text)
    }

    #[test]
    fn a_size_refusal_names_the_configured_limit() {
        let closure = ChannelClosure::from(precondition_failed(
            "PRECONDITION_FAILED - message size 1200090 is larger than configured max size 1048576",
        ));
        assert!(matches!(
            closure,
            ChannelClosure::MessageTooLarge {
                limit: 1_048_576,
                ..
            }
        ));
    }

    #[test]
    fn a_size_refusal_at_the_ceiling_names_the_ceiling() {
        let closure = ChannelClosure::from(precondition_failed(
            "PRECONDITION_FAILED - message size 536870913 is larger than max size 536870912",
        ));
        assert!(matches!(
            closure,
            ChannelClosure::MessageTooLarge {
                limit: 536_870_912,
                ..
            }
        ));
    }

    #[test]
    fn other_closes_are_not_attributed_to_a_message() {
        let other_precondition = precondition_failed(
            "PRECONDITION_FAILED - inequivalent arg 'durable' for queue 'notifications'",
        );
        let unparsable_limit = precondition_failed(
            "PRECONDITION_FAILED - message size 1200090 is larger than configured max size many",
        );
        let unparsable_size = precondition_failed(
            "PRECONDITION_FAILED - message size large is larger than configured max size 1048576",
        );
        let access_refused = protocol_error(
            AMQPErrorKind::Soft(AMQPSoftError::ACCESSREFUSED),
            "PRECONDITION_FAILED - message size 1200090 is larger than configured max size 1048576",
        );
        let connection_closed = protocol_error(
            AMQPErrorKind::Hard(AMQPHardError::INTERNALERROR),
            "INTERNAL_ERROR",
        );
        let lost = lapin::Error::from(ErrorKind::InvalidChannelState(
            lapin::ChannelState::Closed,
            "basic.publish",
        ));
        for error in [
            other_precondition,
            unparsable_limit,
            unparsable_size,
            access_refused,
            connection_closed,
            lost,
        ] {
            assert!(matches!(
                ChannelClosure::from(error),
                ChannelClosure::Unattributed(_)
            ));
        }
    }
}
