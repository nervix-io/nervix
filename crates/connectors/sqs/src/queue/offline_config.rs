//! Network-free validation of the configuration the host is preparing to commit.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The SQS client's offline configuration refusal, with its complete typed cause.
//! - **Depends on.** The connector contract, configuration entries and the SQS client both
//!   directions build.
//! - **Must not know.** Models, transaction admission, resource resolution or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the connector driver validation boundary")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies the client's resolved entries")
)]

use nervix_connector::OfflineClientConfigError;
use nervix_models::ClientConfigEntry;

use super::connection::SqsClientConfig;

/// Read the configuration as every connection reads it, its CA file included, without opening one
/// or resolving its host.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    match SqsClientConfig::parse(config) {
        Ok(_) => Ok(()),
        Err(refusal) => {
            let key = refusal.current_context().key;
            Err(refusal.change_context(OfflineClientConfigError::key(key)))
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_connector::{ClientConfigError, TlsClientConfigError};
    use tempfile::TempDir;

    use super::*;
    use crate::queue::{
        connection::{EmptyCaFile, InvalidSqsClientKey},
        connection_tests::ca_certificate_pem,
    };

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    /// The keys every SQS client names, with a CA file holding `ca_pem` when one is given.
    fn config(ca_pem: Option<&str>) -> (Vec<ClientConfigEntry>, TempDir) {
        let directory = tempfile::tempdir().expect("a temporary directory can be created");
        let mut entries = vec![
            entry("endpoint", "https://sqs.nervix.test:9325"),
            entry("region", "eu-west-1"),
            entry("access_key_id", "AKIDEXAMPLE"),
            entry("secret_access_key", "declared-secret"),
        ];
        if let Some(ca_pem) = ca_pem {
            let ca_file = directory.path().join("ca.pem");
            std::fs::write(&ca_file, ca_pem).expect("the CA file can be written");
            entries.push(entry("tls_ca_file", &ca_file.display().to_string()));
        }
        (entries, directory)
    }

    #[test]
    fn a_client_its_connections_can_use_needs_no_service() {
        let (entries, _directory) = config(None);
        validate_client_configuration(&entries).expect("a complete client is valid");
        let (entries, _directory) = config(Some(&ca_certificate_pem()));
        validate_client_configuration(&entries)
            .expect("a client trusting a CA certificate is valid");
    }

    /// The cause a CA file Smithy could not trust is refused with.
    #[derive(Debug, Clone, Copy)]
    enum Untrusted {
        /// A section that is not base64, which the PEM parser refuses.
        Unparsed,
        /// Base64 that is not a certificate, which the root store refuses.
        Unanchored,
        /// No certificate at all, which would trust no service.
        Empty,
    }

    impl Untrusted {
        fn content(self) -> &'static str {
            match self {
                Self::Unparsed => {
                    "-----BEGIN CERTIFICATE-----\n!this is not base64!\n-----END CERTIFICATE-----\n"
                }
                Self::Unanchored => {
                    "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"
                }
                Self::Empty => "no certificate is written here\n",
            }
        }

        fn is_the_cause_of(self, refusal: &error_stack::Report<OfflineClientConfigError>) -> bool {
            let parse = refusal.downcast_ref::<TlsClientConfigError>();
            match self {
                Self::Unparsed => {
                    matches!(parse, Some(TlsClientConfigError::ParseCaCertificate { .. }))
                }
                Self::Unanchored => {
                    matches!(parse, Some(TlsClientConfigError::AddCaCertificate { .. }))
                }
                Self::Empty => refusal.downcast_ref::<EmptyCaFile>().is_some(),
            }
        }
    }

    #[test]
    fn a_ca_file_without_a_trusted_certificate_is_refused_by_its_key() {
        for untrusted in [Untrusted::Unparsed, Untrusted::Unanchored, Untrusted::Empty] {
            let (entries, _directory) = config(Some(untrusted.content()));

            let refusal = validate_client_configuration(&entries)
                .expect_err("a CA file Smithy could not trust fails the client before commit");

            assert!(
                matches!(refusal.current_context(), OfflineClientConfigError::InvalidKey { key } if key == "tls_ca_file"),
                "{untrusted:?}: {refusal:?}"
            );
            assert_eq!(
                refusal.to_string(),
                "invalid client config key 'tls_ca_file'"
            );
            assert_eq!(
                refusal
                    .downcast_ref::<InvalidSqsClientKey>()
                    .map(|invalid| invalid.key),
                Some("tls_ca_file"),
                "{untrusted:?}"
            );
            assert!(
                untrusted.is_the_cause_of(&refusal),
                "{untrusted:?}: {refusal:?}"
            );
        }
    }

    #[test]
    fn a_missing_key_is_refused_by_its_name() {
        for key in ["endpoint", "region", "access_key_id", "secret_access_key"] {
            let (mut entries, _directory) = config(None);
            entries.retain(|entry| entry.key != key);

            let refusal = validate_client_configuration(&entries)
                .expect_err("an SQS client signs and addresses every request with these keys");

            assert!(
                matches!(refusal.current_context(), OfflineClientConfigError::InvalidKey { key: refused } if refused == key),
                "{key}: {refusal:?}"
            );
            assert!(
                matches!(
                    refusal.downcast_ref::<ClientConfigError>(),
                    Some(ClientConfigError::MissingRequired { key: missing, .. }) if missing == key
                ),
                "{key}: {refusal:?}"
            );
        }
    }
}
