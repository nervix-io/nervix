//! Runtime-independent TLS algorithm identities and ordered selections.
//!
//! Layer: primitives.
//! - **Owns.** Algorithm names and IANA identifiers, alias membership and bounded ordered selections.
//! - **Depends on.** Serialization, typed reports and declarative string conversions.
//! - **Must not know.** Models, settings, TLS implementations, listeners or running nodes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        primitives,
        reason = "owns self-contained TLS algorithm values and conversions"
    )
)]

use std::{fmt, str::FromStr};

use error_stack::Report;
use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum AlgorithmKind {
    #[strum(serialize = "cipher suite")]
    CipherSuite,
    #[strum(serialize = "key exchange group")]
    KeyExchangeGroup,
}

#[derive(Debug, Clone, Copy, strum::EnumString, strum::IntoStaticStr)]
#[strum(ascii_case_insensitive, serialize_all = "lowercase")]
pub enum AlgorithmAlias {
    Fips,
    Default,
}

impl AlgorithmAlias {
    pub const ALL: &'static [Self] = &[Self::Fips, Self::Default];
    pub fn name(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(outcome, reason = "algorithm selection returns pure value refusals")
)]
pub enum AlgorithmSelectionError {
    #[error("unknown {kind} '{item}'")]
    Unknown { kind: AlgorithmKind, item: String },
    #[error("the {kind} selection is empty")]
    Empty { kind: AlgorithmKind },
    #[error("the {kind} selection repeats '{item}'")]
    Duplicate {
        kind: AlgorithmKind,
        item: &'static str,
    },
    #[error("the {kind} selection contains a value after its end")]
    NonContiguous { kind: AlgorithmKind },
}

macro_rules! algorithms {
    ($algorithm:ident, $selection:ident, $kind:ident, $archived:ident;
     $($variant:ident = ($id:expr, $name:literal, $approved:literal, $preference:literal $(, $alias:literal)*)),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
                 Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
                 strum::EnumString, strum::IntoStaticStr)]
        #[strum(ascii_case_insensitive)]
        pub enum $algorithm {
            $(#[strum(serialize = $name, $(serialize = $alias,)* to_string = $name)]
              $variant,)+
        }

        impl $algorithm {
            /// Complete membership in alias preference order. The explicit ranks keep preference
            /// independent of enum discriminants and serialization identities.
            pub const ALL: &'static [Self] = &{
                let declared = [$(Self::$variant,)+];
                let mut ordered = declared;
                let mut occupied = [$( { let _ = Self::$variant; false },)+];
                let mut index = 0;
                while index < declared.len() {
                    let rank = match declared[index] { $(Self::$variant => $preference,)+ };
                    assert!(rank < declared.len(), "algorithm preference is within the catalog");
                    assert!(!occupied[rank], "algorithm preference ranks are unique");
                    occupied[rank] = true;
                    ordered[rank] = declared[index];
                    index += 1;
                }
                ordered
            };
            pub const ACCEPTED_NAMES: &'static [&'static str] = &[$($name, $($alias,)* )+];

            pub const fn iana(self) -> u16 {
                match self { $(Self::$variant => $id,)+ }
            }

            pub const fn is_fips_approved(self) -> bool {
                match self { $(Self::$variant => $approved,)+ }
            }

            pub fn name(self) -> &'static str {
                self.into()
            }
        }

        /// A nonempty unique ordered selection, bounded by the complete algorithm catalog.
        /// Unused array slots are private absent capacity, never public algorithm identities.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Archive, RkyvSerialize)]
        #[serde(transparent)]
        pub struct $selection([Option<$algorithm>; $algorithm::ALL.len()]);

        impl $selection {
            pub fn names() -> impl Iterator<Item = &'static str> {
                AlgorithmAlias::ALL.iter().copied().map(AlgorithmAlias::name)
                    .chain($algorithm::ACCEPTED_NAMES.iter().copied())
            }

            pub fn resolve(items: impl IntoIterator<Item = impl AsRef<str>>)
                -> Result<Self, Report<AlgorithmSelectionError>>
            {
                let mut selection = Self([None; $algorithm::ALL.len()]);
                for item in items {
                    let item = item.as_ref();
                    match item.parse::<AlgorithmAlias>() {
                        Ok(AlgorithmAlias::Fips) => {
                            for algorithm in $algorithm::ALL {
                                if algorithm.is_fips_approved() {
                                    selection.insert_unique(*algorithm);
                                }
                            }
                        },
                        Ok(AlgorithmAlias::Default) => {
                            for algorithm in $algorithm::ALL {
                                selection.insert_unique(*algorithm);
                            }
                        },
                        Err(_) => {
                            let algorithm = item.parse::<$algorithm>().map_err(|_| {
                                Report::new(AlgorithmSelectionError::Unknown {
                                    kind: AlgorithmKind::$kind,
                                    item: item.to_owned(),
                                })
                            })?;
                            selection.insert_unique(algorithm);
                        },
                    }
                }
                selection.validate()?;
                Ok(selection)
            }

            fn insert_unique(&mut self, algorithm: $algorithm) {
                // Both scans are bounded by the complete finite algorithm catalog.
                if self.0.contains(&Some(algorithm)) {
                    return;
                }
                let slot = self.0.iter_mut().find(|slot| slot.is_none())
                    .assured("each distinct catalog algorithm has its own capacity slot");
                *slot = Some(algorithm);
            }

            pub fn iter(&self) -> impl Iterator<Item = $algorithm> + '_ {
                self.0.iter().flatten().copied()
            }

            pub fn validate(&self) -> Result<(), Report<AlgorithmSelectionError>> {
                if self.0[0].is_none() {
                    return Err(Report::new(AlgorithmSelectionError::Empty { kind: AlgorithmKind::$kind }));
                }
                let mut ended = false;
                for (index, slot) in self.0.iter().enumerate() {
                    match slot {
                        None => ended = true,
                        Some(algorithm) => {
                            if ended {
                                return Err(Report::new(AlgorithmSelectionError::NonContiguous { kind: AlgorithmKind::$kind }));
                            }
                            // At most the complete catalog's entries precede this slot.
                            if self.0[..index].contains(slot) {
                                return Err(Report::new(AlgorithmSelectionError::Duplicate {
                                    kind: AlgorithmKind::$kind, item: algorithm.name(),
                                }));
                            }
                        }
                    }
                }
                Ok(())
            }
        }

        impl Default for $selection {
            fn default() -> Self {
                use meticulous::ResultExt as _;
                Self::resolve([AlgorithmAlias::Fips.name()]).assured("the fixed fips alias names a nonempty unique selection")
            }
        }

        impl FromStr for $selection {
            type Err = Report<AlgorithmSelectionError>;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                if value.trim().is_empty() {
                    return Self::resolve(std::iter::empty::<&str>());
                }
                Self::resolve(value.split(',').map(str::trim))
            }
        }

        impl fmt::Display for $selection {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut first = true;
                for algorithm in self.iter() {
                    if !first {
                        formatter.write_str(",")?;
                    }
                    formatter.write_str(algorithm.name())?;
                    first = false;
                }
                Ok(())
            }
        }

        impl<'de> Deserialize<'de> for $selection {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                use serde::de::Error as _;
                let selection = Self(Deserialize::deserialize(deserializer)?);
                selection.validate().map_err(|error| D::Error::custom(format_args!("{error:#}")))?;
                Ok(selection)
            }
        }

        impl<D> RkyvDeserialize<$selection, D> for $archived
        where
            D: rkyv::rancor::Fallible + ?Sized,
            D::Error: rkyv::rancor::Source,
        {
            fn deserialize(&self, deserializer: &mut D) -> Result<$selection, D::Error> {
                use rkyv::rancor::Source as _;
                let selection = $selection(RkyvDeserialize::deserialize(&self.0, deserializer)?);
                selection.validate().map_err(|error| D::Error::new(error.into_error()))?;
                Ok(selection)
            }
        }
    };
}

algorithms! { TlsCipherSuite, TlsCipherSuites, CipherSuite, ArchivedTlsCipherSuites;
    Aes256GcmSha384 = (0x1302, "TLS13_AES_256_GCM_SHA384", true, 0, "TLS_AES_256_GCM_SHA384"),
    Aes128GcmSha256 = (0x1301, "TLS13_AES_128_GCM_SHA256", true, 1, "TLS_AES_128_GCM_SHA256"),
    ChaCha20Poly1305Sha256 = (0x1303, "TLS13_CHACHA20_POLY1305_SHA256", false, 2, "TLS_CHACHA20_POLY1305_SHA256"),
    EcdheEcdsaAes256GcmSha384 = (0xc02c, "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", true, 3),
    EcdheEcdsaAes128GcmSha256 = (0xc02b, "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", true, 4),
    EcdheEcdsaChaCha20Poly1305Sha256 = (0xcca9, "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256", false, 5),
    EcdheRsaAes256GcmSha384 = (0xc030, "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", true, 6),
    EcdheRsaAes128GcmSha256 = (0xc02f, "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", true, 7),
    EcdheRsaChaCha20Poly1305Sha256 = (0xcca8, "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256", false, 8),
}

algorithms! { TlsKeyExchangeGroup, TlsKeyExchangeGroups, KeyExchangeGroup, ArchivedTlsKeyExchangeGroups;
    X25519 = (0x001d, "X25519", false, 1),
    Secp256r1 = (0x0017, "secp256r1", true, 2),
    Secp384r1 = (0x0018, "secp384r1", true, 3),
    X25519MlKem768 = (0x11ec, "X25519MLKEM768", true, 0),
    Secp256r1MlKem768 = (0x11eb, "secp256r1MLKEM768", true, 4),
    MlKem768 = (0x0201, "MLKEM768", true, 5),
    MlKem1024 = (0x0202, "MLKEM1024", true, 6),
}

impl TlsCipherSuite {
    pub const fn is_tls13(self) -> bool {
        matches!(
            self,
            Self::Aes256GcmSha384 | Self::Aes128GcmSha256 | Self::ChaCha20Poly1305Sha256
        )
    }
}

impl TlsKeyExchangeGroup {
    pub const fn supports_tls12(self) -> bool {
        matches!(self, Self::Secp256r1 | Self::Secp384r1 | Self::X25519)
    }
}

#[cfg(test)]
mod tests;
