//! The cluster's outward TLS algorithm settings and their joint validation.
//!
//! Layer: vocabulary.
//! - **Owns.** Setting classes/defaults, typed list construction and complete-policy validation.
//! - **Depends on.** The pure cryptography catalog and the setting/value vocabulary.
//! - **Must not know.** Rustls, listeners, connection lifetimes or consensus installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns semantic TLS parameter values")
)]

use error_stack::{Report, ResultExt as _};

use super::{ClusterParameterError, ClusterParameterKey, ClusterParameterValues};
use crate::{ClusterParameters, SettingDefinition};

macro_rules! tls_setting {
    ($definition:ident, $value:ident, $name:literal) => {
        pub struct $definition;

        impl $definition {
            pub fn names() -> impl Iterator<Item = &'static str> {
                nervix_crypto_policy::$value::names()
            }

            pub fn parse_names(
                items: impl IntoIterator<Item = impl AsRef<str>>,
            ) -> Result<nervix_crypto_policy::$value, Report<ClusterParameterError>> {
                nervix_crypto_policy::$value::resolve(items).change_context(
                    ClusterParameterError::TlsSelection {
                        parameter: ClusterParameterKey::$definition,
                    },
                )
            }
        }

        impl SettingDefinition for $definition {
            type Class = ClusterParameters;
            type Value = nervix_crypto_policy::$value;
            type Context = ();
            type Error = ClusterParameterError;

            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;

            fn default_value(_: &()) -> Self::Value {
                Self::Value::default()
            }

            fn validate(value: &Self::Value, _: &()) -> Result<(), Report<ClusterParameterError>> {
                value
                    .validate()
                    .change_context(ClusterParameterError::TlsSelection {
                        parameter: ClusterParameterKey::$definition,
                    })
            }
        }
    };
}

tls_setting!(TlsCipherSuites, TlsCipherSuites, "tls-cipher-suites");
tls_setting!(
    TlsKeyExchangeGroups,
    TlsKeyExchangeGroups,
    "tls-key-exchange-groups"
);

impl ClusterParameterValues {
    /// Validate the complete candidate, so changing both lists is one atomic decision.
    pub fn validate_tls(&self) -> Result<(), Report<ClusterParameterError>> {
        let suites = self.tls_cipher_suites();
        let groups = self.tls_key_exchange_groups();
        if !suites
            .iter()
            .any(nervix_crypto_policy::TlsCipherSuite::is_tls13)
        {
            return Err(Report::new(ClusterParameterError::NoTls13CipherSuite {
                suites,
            }));
        }
        if suites.iter().any(|suite| !suite.is_tls13())
            && !groups
                .iter()
                .any(nervix_crypto_policy::TlsKeyExchangeGroup::supports_tls12)
        {
            return Err(Report::new(
                ClusterParameterError::NoTls12KeyExchangeGroup { suites, groups },
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;
    use crate::{ClusterParameterChange, ClusterParameterUpdates};

    #[test]
    fn both_lists_change_atomically_without_validating_an_intermediate_pair() {
        let suites = TlsCipherSuites::parse_names(["TLS13_CHACHA20_POLY1305_SHA256"])
            .assured("the named TLS 1.3 suite exists");
        let groups = TlsKeyExchangeGroups::parse_names(["MLKEM768"])
            .assured("the named TLS 1.3 group exists");
        let current = ClusterParameterValues::default();
        for changes in [
            vec![
                ClusterParameterChange::TlsKeyExchangeGroups(groups),
                ClusterParameterChange::TlsCipherSuites(suites),
            ],
            vec![
                ClusterParameterChange::TlsCipherSuites(suites),
                ClusterParameterChange::TlsKeyExchangeGroups(groups),
            ],
        ] {
            let changes = ClusterParameterUpdates::try_from(changes)
                .assured("each parameter is assigned once");
            let candidate = current
                .changed(&changes)
                .assured("the complete pair supports TLS 1.3");
            assert_eq!(candidate.tls_cipher_suites(), suites);
            assert_eq!(candidate.tls_key_exchange_groups(), groups);
        }
    }

    #[test]
    fn tls13_and_a_usable_group_for_every_tls12_suite_are_required() {
        let current = ClusterParameterValues::default();
        let suites = TlsCipherSuites::parse_names(["TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"])
            .assured("the named TLS 1.2 suite exists");
        let changes =
            ClusterParameterUpdates::try_from(vec![ClusterParameterChange::TlsCipherSuites(
                suites,
            )])
            .assured("one assignment is unique");
        assert_eq!(
            current
                .changed(&changes)
                .expect_err("TLS 1.3 is required")
                .current_context(),
            &ClusterParameterError::NoTls13CipherSuite { suites }
        );
        let groups = TlsKeyExchangeGroups::parse_names(["MLKEM768"])
            .assured("the named TLS 1.3 group exists");
        let changes =
            ClusterParameterUpdates::try_from(vec![ClusterParameterChange::TlsKeyExchangeGroups(
                groups,
            )])
            .assured("one assignment is unique");
        assert_eq!(
            current
                .changed(&changes)
                .expect_err("the retained TLS 1.2 suites need a usable group")
                .current_context(),
            &ClusterParameterError::NoTls12KeyExchangeGroup {
                suites: current.tls_cipher_suites(),
                groups
            }
        );
    }

    #[test]
    fn both_current_record_decoders_validate_the_complete_pair() {
        let mut values = ClusterParameterValues::default();
        let groups = TlsKeyExchangeGroups::parse_names(["MLKEM768"])
            .assured("the individual group selection is valid");
        values.tls_key_exchange_groups =
            crate::cluster_parameters::ParameterSetting::new(groups, &())
                .assured("the individual setting has a nonempty unique selection");
        let json = serde_json::to_vec(&values)
            .assured("the current invalid pair serializes for the boundary test");
        assert!(serde_json::from_slice::<ClusterParameterValues>(&json).is_err());
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&values)
            .assured("the current invalid pair archives for the boundary test");
        assert!(rkyv::from_bytes::<ClusterParameterValues, rkyv::rancor::Error>(&bytes).is_err());
    }
}
