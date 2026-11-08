//! The AMQP Basic properties of one prepared message.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Exact conversion of a retained record envelope to AMQP properties and typed refusals.
//! - **Depends on.** Connector records, declared RabbitMQ property types and Lapin wire values.
//! - **Must not know.** Models, expression evaluation, graph ownership or host retry policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns AMQP property conversion")
)]

use error_stack::{Report, ResultExt as _};
use lapin::{BasicProperties, types::ShortString};
use nervix_connector::{EmitterRecord, PropertyValue};
use nervix_models::{EmitterEntityKind, PropertyType, RabbitMqEmitterEntity};
use thiserror::Error;

use super::{RabbitMqChannel, RabbitMqMessage};

#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "AMQP timestamps carry whole Unix seconds")
)]
const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// A record whose envelope AMQP cannot carry is refused before any transport write.
#[derive(Debug, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the publisher rejects this record before transport admission"
    )
)]
pub(super) enum RabbitMqPropertyError {
    #[error("TIMESTAMP must be a nonnegative whole Unix second")]
    TimestampUnrepresentable,
    #[error(
        "PROPERTIES key '{name}' has {bytes} UTF-8 bytes, exceeding the AMQP short-string limit"
    )]
    StringTooLong { name: &'static str, bytes: usize },
    #[error("PROPERTIES key '{name}' requires {expected}, received {actual}")]
    InvalidType {
        name: &'static str,
        expected: PropertyType,
        actual: PropertyType,
    },
    #[error("no AMQP property conversion is declared for '{name}'")]
    UnmappedProperty { name: &'static str },
}

impl RabbitMqMessage {
    /// Converts once when a record becomes a message; every channel attempt retains this result.
    pub(super) fn properties(
        record: &EmitterRecord,
    ) -> error_stack::Result<BasicProperties, RabbitMqPropertyError> {
        let mut properties = RabbitMqChannel::properties(&record.headers);
        if record.envelope.persistent {
            properties = properties.with_delivery_mode(2);
        }
        if let Some(timestamp) = record.envelope.timestamp {
            let nanos = timestamp.unix_nanos();
            if nanos < 0 || nanos % NANOS_PER_SECOND != 0 {
                return Err(Report::new(RabbitMqPropertyError::TimestampUnrepresentable));
            }
            let seconds = u64::try_from(nanos / NANOS_PER_SECOND)
                .change_context(RabbitMqPropertyError::TimestampUnrepresentable)?;
            properties = properties.with_timestamp(seconds);
        }
        // The host admits each of the ten declared keys at most once, bounding these lookups.
        let declared = EmitterEntityKind::RabbitMq(RabbitMqEmitterEntity::Queue).capabilities();
        for property in &record.envelope.properties {
            let name = property.name;
            let declaration = declared
                .property(name)
                .ok_or_else(|| Report::new(RabbitMqPropertyError::UnmappedProperty { name }))?;
            let actual = property.value.ty();
            if actual != declaration.ty {
                return Err(Report::new(RabbitMqPropertyError::InvalidType {
                    name,
                    expected: declaration.ty,
                    actual,
                }));
            }
            properties = match &property.value {
                PropertyValue::U8(value) if name == "priority" => properties.with_priority(*value),
                PropertyValue::String(value) => {
                    let value = ShortString::try_new(value).change_context(
                        RabbitMqPropertyError::StringTooLong {
                            name,
                            bytes: value.len(),
                        },
                    )?;
                    string_property(properties, name, value)?
                }
                _ => {
                    return Err(Report::new(RabbitMqPropertyError::UnmappedProperty {
                        name,
                    }));
                }
            };
        }
        Ok(properties)
    }
}

fn string_property(
    properties: BasicProperties,
    name: &'static str,
    value: ShortString,
) -> error_stack::Result<BasicProperties, RabbitMqPropertyError> {
    Ok(match name {
        "content_type" => properties.with_content_type(value),
        "content_encoding" => properties.with_content_encoding(value),
        "message_id" => properties.with_message_id(value),
        "correlation_id" => properties.with_correlation_id(value),
        "reply_to" => properties.with_reply_to(value),
        "expiration" => properties.with_expiration(value),
        "type" => properties.with_type(value),
        "app_id" => properties.with_app_id(value),
        "user_id" => properties.with_user_id(value),
        _ => {
            return Err(Report::new(RabbitMqPropertyError::UnmappedProperty {
                name,
            }));
        }
    })
}
