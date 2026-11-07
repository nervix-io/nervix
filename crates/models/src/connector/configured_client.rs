//! What every configured client Model is made of, whichever connector it configures.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** A client's pass-through configuration entry, the one resource version it mounts,
//!   its declared connection-pool bounds, the declaration every connector module writes its
//!   client Model with, and what every client of a connector declares: whether it is pooled, who
//!   reads its configuration keys and the options its statement writes.
//! - **Depends on.** Resource names and the connector kinds.
//! - **Must not know.** What a configuration key means to a driver, how a mount is resolved to
//!   files, or how a pool is sized at run time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU32;

use error_stack::Report;
use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ClientConfigSchema, ClientConfigSchemaError, ClientConfigSide, ClientName, ConnectorKind,
    OptionDeclaration, ResourceName,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientConfigEntry {
    pub key: String,
    pub value: String,
}

/// The one resource version mounted into a client's configuration namespace.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientResourceMount<Version = u64> {
    pub resource: ResourceName,
    pub version: Version,
}

/// Why a declared pair of connection-pool sizes is not a usable bound.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClientPoolBoundsError {
    #[error("minimum pool size {minimum} must not exceed maximum pool size {maximum}")]
    MinimumExceedsMaximum { minimum: u32, maximum: NonZeroU32 },
}

/// The connection-pool bounds a database client declares.
///
/// The two counts only mean anything together: a minimum above its maximum asks for a maintained
/// size the ceiling forbids. [`ClientPoolBounds::new`] is the one place that ordering is decided,
/// so every holder of a pair already knows its minimum is reachable. The serde and archived forms
/// are decoded through it too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Archive, RkyvSerialize)]
pub struct ClientPoolBounds {
    minimum: u32,
    maximum: NonZeroU32,
}

impl<'de> Deserialize<'de> for ClientPoolBounds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// The two counts as the serde form carries them, before they are paired.
        #[derive(Deserialize)]
        struct Counts {
            minimum: u32,
            maximum: NonZeroU32,
        }

        let Counts { minimum, maximum } = Counts::deserialize(deserializer)?;
        Self::new(minimum, maximum)
            .map_err(|report| <D::Error as serde::de::Error>::custom(report.current_context()))
    }
}

impl<D> RkyvDeserialize<ClientPoolBounds, D> for ArchivedClientPoolBounds
where
    D: rkyv::rancor::Fallible + ?Sized,
    D::Error: rkyv::rancor::Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<ClientPoolBounds, D::Error> {
        let minimum: u32 = self.minimum.deserialize(deserializer)?;
        let maximum: NonZeroU32 = self.maximum.deserialize(deserializer)?;
        ClientPoolBounds::new(minimum, maximum).map_err(|report| {
            <D::Error as rkyv::rancor::Source>::new(report.current_context().clone())
        })
    }
}

impl ClientPoolBounds {
    /// Pair a declared minimum with its maximum, rejecting a minimum the ceiling cannot reach.
    pub fn new(minimum: u32, maximum: NonZeroU32) -> Result<Self, Report<ClientPoolBoundsError>> {
        if minimum > maximum.get() {
            return Err(Report::new(ClientPoolBoundsError::MinimumExceedsMaximum {
                minimum,
                maximum,
            }));
        }
        Ok(Self { minimum, maximum })
    }

    /// The declared number of established, usable connections to keep available.
    pub const fn minimum(&self) -> u32 {
        self.minimum
    }

    /// The declared ceiling on managed connections, counting those being established or retired.
    pub const fn maximum(&self) -> NonZeroU32 {
        self.maximum
    }
}

/// Whether a client keeps a pool of connections, which its statement bounds with `POOL SIZE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientPooling {
    Pooled,
    Unpooled,
}

impl ClientPooling {
    /// Alternative configuration spellings that would compete with the declared POOL SIZE.
    pub const fn reserved_config_keys(self) -> &'static [&'static str] {
        match self {
            Self::Pooled => &[
                "maxpoolsize",
                "max_pool_size",
                "minpoolsize",
                "min_pool_size",
                "pool_max",
                "pool_min",
                "pool_size",
            ],
            Self::Unpooled => &[],
        }
    }

    /// Pool-capacity keys are reserved without regard to ASCII case, including URL parameters.
    pub fn reserves_config_key(self, key: &str) -> bool {
        // Seven fixed declaration entries bound this scan independently of configuration size.
        self.reserved_config_keys()
            .iter()
            .any(|reserved| reserved.eq_ignore_ascii_case(key))
    }
}

/// What every client of one connector declares, whatever its statement says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientDeclaration {
    pub pooling: ClientPooling,
    pub config: ClientConfigSchema,
    /// The options its statement writes after its kind, before its mount, in order.
    pub options: &'static [OptionDeclaration],
}

/// A borrowed view of the client fields shared by every configured connector kind.
pub struct ConfiguredClientRef<'a, Version> {
    pub connector: ConnectorKind,
    pub name: &'a ClientName,
    pub mount: Option<&'a ClientResourceMount<Version>>,
    pub config: &'a [ClientConfigEntry],
}

impl<Version> ConfiguredClientRef<'_, Version> {
    /// Refuses a key only the other side of this client's connector reads.
    pub fn validate_side(
        &self,
        side: ClientConfigSide,
    ) -> Result<(), Report<ClientConfigSchemaError>> {
        let declaration = self
            .connector
            .client()
            .assured("a configured client has a connector declaration");
        declaration.config.validate_side(self.config, side)
    }
}

/// Declares the client Model of one connector: its name, its mount and its configuration.
///
/// A pooled client carries its declared [`ClientPoolBounds`] as well, because connection capacity
/// belongs to the client itself rather than to the connector's string configuration. Every client
/// states the connector it configures as `CONNECTOR`, which is the one declaration the Model
/// union and canonical rendering read, and what it declares as `DECLARATION`: whether it is
/// pooled, which the Model's shape decides, and who reads its configuration keys.
macro_rules! configured_client {
    (
        $(#[$meta:meta])*
        $Connector:ident: unpooled, config $Owner:ident, $Client:ident => $Config:ident
    ) => {
        $(#[$meta])*
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::rkyv::Archive,
            ::rkyv::Serialize,
            ::rkyv::Deserialize,
        )]
        pub struct $Client<Version = u64> {
            pub name: $crate::ClientName,
            pub mount: Option<$crate::ClientResourceMount<Version>>,
            pub config: Vec<$crate::ClientConfigEntry>,
        }

        $crate::connector::configured_client::configured_client!(
            @connector $Connector: Unpooled, $Owner, $Client => $Config
        );
    };
    (
        $(#[$meta:meta])*
        $Connector:ident: pooled, config $Owner:ident, $Client:ident => $Config:ident
    ) => {
        $(#[$meta])*
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::rkyv::Archive,
            ::rkyv::Serialize,
            ::rkyv::Deserialize,
        )]
        pub struct $Client<Version = u64> {
            pub name: $crate::ClientName,
            pub pool: $crate::ClientPoolBounds,
            pub mount: Option<$crate::ClientResourceMount<Version>>,
            pub config: Vec<$crate::ClientConfigEntry>,
        }

        $crate::connector::configured_client::configured_client!(
            @connector $Connector: Pooled, $Owner, $Client => $Config
        );
    };
    (@connector $Connector:ident: $Pooling:ident, $Owner:ident, $Client:ident => $Config:ident) => {
        pub type $Config = $crate::ClientConfigEntry;

        impl<Version> $Client<Version> {
            /// The connector this client configures.
            pub const CONNECTOR: $crate::ConnectorKind = $crate::ConnectorKind::$Connector;

            /// What every client of this connector declares.
            pub const DECLARATION: $crate::ClientDeclaration = $crate::ClientDeclaration {
                pooling: $crate::ClientPooling::$Pooling,
                config: $Owner,
                options: &[],
            };
        }
    };
}

pub(crate) use configured_client;

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nonzero_ext::nonzero;

    use super::{ClientPoolBounds, ClientPoolBoundsError};

    /// The archived layout of pool bounds, so a test can archive a pair the constructor refuses.
    #[derive(rkyv::Archive, rkyv::Serialize)]
    struct ArchivedPoolBounds {
        minimum: u32,
        maximum: std::num::NonZeroU32,
    }

    #[test]
    fn decoders_refuse_a_minimum_above_the_maximum() {
        let json = r#"{"minimum":5,"maximum":3}"#;
        assert!(serde_json::from_str::<ClientPoolBounds>(json).is_err());
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&ArchivedPoolBounds {
            minimum: 5,
            maximum: nonzero!(3_u32),
        })
        .assured("the fields archive");
        assert!(rkyv::from_bytes::<ClientPoolBounds, rkyv::rancor::Error>(&archived).is_err());
    }

    #[test]
    fn pool_bounds_accept_a_minimum_up_to_the_maximum_and_reject_one_above_it() {
        let bounds = ClientPoolBounds::new(0, nonzero!(1u32)).expect("zero is below one");
        assert_eq!(bounds.minimum(), 0);
        assert_eq!(bounds.maximum(), nonzero!(1u32));

        let bounds = ClientPoolBounds::new(3, nonzero!(3u32)).expect("equal bounds are allowed");
        assert_eq!(bounds.minimum(), 3);
        assert_eq!(bounds.maximum(), nonzero!(3u32));

        let error = ClientPoolBounds::new(4, nonzero!(3u32)).expect_err("four exceeds three");
        assert_eq!(
            error.current_context(),
            &ClientPoolBoundsError::MinimumExceedsMaximum {
                minimum: 4,
                maximum: nonzero!(3u32),
            }
        );
    }
}
