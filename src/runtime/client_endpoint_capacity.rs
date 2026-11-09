//! Node-local byte capacities for client producer and consumer attachments.
//!
//! Layer: data plane.
//! - **Owns.** The declared node credit capacities and their memory-relative defaults.
//! - **Depends on.** Typed setting declarations, cluster session defaults and byte quantities.
//! - **Must not know.** Sessions, transports, detection or attachment lifecycles.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns client endpoint byte admission settings")
)]

use std::num::NonZeroU64;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use thiserror::Error;
use ubyte::ByteUnit;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientEndpointOverrides {
    pub producer_node_bytes: Option<ByteUnit>,
    pub consumer_node_bytes: Option<ByteUnit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ClientEndpointCapacityError {
    #[error("{setting} must be greater than zero")]
    Empty { setting: &'static str },
    #[error("{setting} ({bytes}) exceeds the node memory limit ({memory})")]
    AboveMemory {
        setting: &'static str,
        bytes: ByteUnit,
        memory: ByteUnit,
    },
}

macro_rules! endpoint_capacity {
    ($definition:ident, $name:literal, $session:ty) => {
        pub struct $definition;

        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = ByteUnit;
            type Context = Option<NonZeroU64>;
            type Error = ClientEndpointCapacityError;

            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = Some(
                "one thirty-second of node memory, holding at least one default session window",
            );

            fn default_value(memory: &Self::Context) -> ByteUnit {
                match memory {
                    Some(memory) => ByteUnit::Byte((memory.get() / 32).max(<$session>::DEFAULT)),
                    None => ByteUnit::Mebibyte(128),
                }
            }

            fn validate(
                bytes: &ByteUnit,
                memory: &Self::Context,
            ) -> Result<(), Report<Self::Error>> {
                if bytes.as_u64() == 0 {
                    return Err(Report::new(ClientEndpointCapacityError::Empty {
                        setting: Self::NAME,
                    }));
                }
                if let Some(memory) = memory
                    && bytes.as_u64() > memory.get()
                {
                    return Err(Report::new(ClientEndpointCapacityError::AboveMemory {
                        setting: Self::NAME,
                        bytes: *bytes,
                        memory: ByteUnit::Byte(memory.get()),
                    }));
                }
                Ok(())
            }
        }
    };
}

endpoint_capacity!(
    ClientProducerNodeBytes,
    "client-producer-node-bytes",
    nervix_models::ClientProducerSessionBytes
);
endpoint_capacity!(
    ClientConsumerNodeBytes,
    "client-consumer-node-bytes",
    nervix_models::ClientConsumerSessionBytes
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientEndpointSettings {
    pub producer_node_bytes: Setting<ClientProducerNodeBytes>,
    pub consumer_node_bytes: Setting<ClientConsumerNodeBytes>,
}

impl ClientEndpointSettings {
    pub fn resolve(
        overrides: ClientEndpointOverrides,
        memory: Option<NonZeroU64>,
    ) -> Result<Self, Report<ClientEndpointCapacityError>> {
        Ok(Self {
            producer_node_bytes: Setting::resolve(overrides.producer_node_bytes, &memory)?,
            consumer_node_bytes: Setting::resolve(overrides.consumer_node_bytes, &memory)?,
        })
    }

    pub fn total_bytes(&self) -> Option<u64> {
        self.producer_node_bytes
            .value()
            .as_u64()
            .checked_add(self.consumer_node_bytes.value().as_u64())
    }
}

impl Default for ClientEndpointSettings {
    fn default() -> Self {
        Self::resolve(ClientEndpointOverrides::default(), None)
            .assured("the node credit defaults are positive")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_known_memory_capacity_derives_two_independent_session_windows() {
        let settings = ClientEndpointSettings::resolve(
            ClientEndpointOverrides::default(),
            NonZeroU64::new(ByteUnit::Gibibyte(1).as_u64()),
        )
        .expect("one GiB holds both client windows");
        assert_eq!(
            settings.producer_node_bytes.value().as_u64(),
            ByteUnit::Mebibyte(32).as_u64()
        );
        assert_eq!(
            settings.consumer_node_bytes.value().as_u64(),
            ByteUnit::Mebibyte(32).as_u64()
        );
        assert_eq!(
            settings.total_bytes(),
            Some(ByteUnit::Mebibyte(64).as_u64())
        );
    }

    #[test]
    fn an_explicit_capacity_is_preserved_and_cannot_exceed_memory() {
        let settings = ClientEndpointSettings::resolve(
            ClientEndpointOverrides {
                producer_node_bytes: Some(ByteUnit::Mebibyte(7)),
                ..Default::default()
            },
            NonZeroU64::new(ByteUnit::Gibibyte(1).as_u64()),
        )
        .expect("seven MiB is a positive local credit bound");
        assert_eq!(*settings.producer_node_bytes.value(), ByteUnit::Mebibyte(7));
        let error = ClientEndpointSettings::resolve(
            ClientEndpointOverrides {
                producer_node_bytes: Some(ByteUnit::Gibibyte(2)),
                ..Default::default()
            },
            NonZeroU64::new(ByteUnit::Gibibyte(1).as_u64()),
        )
        .expect_err("credit above memory cannot be admitted");
        assert!(matches!(
            error.current_context(),
            ClientEndpointCapacityError::AboveMemory { .. }
        ));
    }
}
