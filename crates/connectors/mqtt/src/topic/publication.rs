//! Layer: engines and infrastructure.
//! - **Owns.** Validated MQTT publication properties, their exact encoded size and driver options.
//! - **Depends on.** The shared record envelope, vocabulary topic names and MQTT driver values.
//! - **Must not know.** Models, expressions, runtime batches, retries, branches or graph state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns MQTT publication-property validation and wire projection"
    )
)]

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_connector::{PropertyValue, RecordProperty};
use nervix_models::MqttTopicName;
use rumqttc::{PublishOptions, PublishProperties};

use super::{MqttPublishingMode, MqttRecordError, MqttRecordResult};

/// The driver's property section after every field has passed its wire boundary.
pub(super) struct MqttPublishProperties {
    properties: Option<PublishProperties>,
}

impl MqttPublishProperties {
    pub(super) fn new(
        records: Vec<RecordProperty>,
        headers: Vec<(String, String)>,
    ) -> MqttRecordResult<Self> {
        let mut properties = PublishProperties::default();
        for record in records {
            let name = record.name;
            match (name, record.value) {
                ("content_type", PropertyValue::String(value)) => {
                    Self::validate_text(name, &value)?;
                    properties.content_type = Some(value);
                }
                ("message_expiry", PropertyValue::U32(value)) => {
                    properties.message_expiry_interval = Some(value);
                }
                ("response_topic", PropertyValue::String(value)) => {
                    Self::validate_text(name, &value)?;
                    let topic = MqttTopicName::try_from(value)
                        .change_context(MqttRecordError::ResponseTopic)?;
                    properties.response_topic = Some(topic.into());
                }
                ("correlation_data", PropertyValue::Bytes(value)) => {
                    Self::validate_length(name, value.len())?;
                    properties.correlation_data = Some(value.into());
                }
                (name, value) => {
                    return Err(Report::new(MqttRecordError::PropertyType {
                        name,
                        actual: value.ty(),
                    }));
                }
            }
        }
        for (name, value) in &headers {
            Self::validate_text("user property name", name)?;
            Self::validate_text("user property value", value)?;
        }
        properties.user_properties = headers;
        let properties = if properties == PublishProperties::default() {
            None
        } else {
            Some(properties)
        };
        Ok(Self { properties })
    }

    fn validate_length(field: &'static str, bytes: usize) -> MqttRecordResult<()> {
        u16::try_from(bytes).change_context(MqttRecordError::PropertyLength { field, bytes })?;
        Ok(())
    }

    fn validate_text(field: &'static str, value: &str) -> MqttRecordResult<()> {
        Self::validate_length(field, value.len())?;
        if value.contains('\0') {
            return Err(Report::new(MqttRecordError::PropertyNul { field }));
        }
        Ok(())
    }

    /// Property identifiers are one byte for these fields. MQTT strings and binary data carry a
    /// two-byte length, expiry a four-byte integer, and each user property two string lengths.
    /// The section's variable-length prefix belongs to the packet framing owner.
    pub(super) fn encoded_bytes(&self) -> usize {
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "these MQTT property identifiers occupy one byte followed by a two-byte \
                          field length"
            )
        )]
        const FIELD_FRAMING: usize = 3;
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "MQTT Message Expiry Interval occupies one property identifier byte and \
                          four integer bytes"
            )
        )]
        const EXPIRY_BYTES: usize = 5;
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "MQTT User Property occupies one identifier byte and two two-byte string \
                          lengths"
            )
        )]
        const USER_FRAMING: usize = 5;
        const IN_MEMORY: &str = "property bytes and their framing fit within the memory holding \
                                 their strings, vectors and field carriers";
        let Some(properties) = &self.properties else {
            return 0;
        };
        let mut bytes = 0_usize;
        for text in [&properties.content_type, &properties.response_topic]
            .into_iter()
            .flatten()
        {
            bytes = bytes.checked_add(FIELD_FRAMING).assured(IN_MEMORY);
            bytes = bytes.checked_add(text.len()).assured(IN_MEMORY);
        }
        if properties.message_expiry_interval.is_some() {
            bytes = bytes.checked_add(EXPIRY_BYTES).assured(IN_MEMORY);
        }
        if let Some(value) = &properties.correlation_data {
            bytes = bytes.checked_add(FIELD_FRAMING).assured(IN_MEMORY);
            bytes = bytes.checked_add(value.len()).assured(IN_MEMORY);
        }
        for (name, value) in &properties.user_properties {
            bytes = bytes.checked_add(USER_FRAMING).assured(IN_MEMORY);
            bytes = bytes.checked_add(name.len()).assured(IN_MEMORY);
            bytes = bytes.checked_add(value.len()).assured(IN_MEMORY);
        }
        bytes
    }

    pub(super) fn into_options(self, mode: MqttPublishingMode, retain: bool) -> PublishOptions {
        let options = mode.publish_options().retain(retain);
        match self.properties {
            Some(properties) => options.properties(properties),
            None => options,
        }
    }
}

#[cfg(test)]
mod tests;
