//! Network-free validation of the configuration the host is preparing to commit.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The WebSocket client's offline refusal of a CA file that holds no certificate.
//! - **Depends on.** The connector contract, configuration entries and the PEM parser the shared
//!   rustls provider reads the same file with.
//! - **Must not know.** Models, transaction admission, resource resolution or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the connector driver validation boundary")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies the client's resolved entries")
)]

use error_stack::{Report, ResultExt as _};
use nervix_connector::{OfflineClientConfigError, client_tls_paths, read_tls_file};
use nervix_models::{ClientConfigEntry, ClientConfigKey};
use rustls_pki_types::{CertificateDer, pem::PemObject as _};

/// Reads the client's CA file without opening a connection. The source adds this file's
/// certificates to the default roots, so a file without one fails nothing when the source opens
/// and leaves a private CA untrusted. A section the parser cannot read still counts as a
/// certificate here: the source refuses it when it opens.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    let Some(ca_file) = client_tls_paths(config).ca_file else {
        return Ok(());
    };
    let key = ClientConfigKey::TLS_CA.name;
    let ca_pem = read_tls_file(&ca_file, "TLS CA certificate")
        .change_context(OfflineClientConfigError::key(key))?;
    if CertificateDer::pem_slice_iter(&ca_pem).next().is_none() {
        return Err(Report::new(OfflineClientConfigError::NoCertificate {
            key: key.to_string(),
        }));
    }
    Ok(())
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_connector::ClientConfigError;
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
    use tempfile::TempDir;

    use super::*;

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    /// A client of a secure endpoint that names a CA file holding `ca_pem`.
    fn client_trusting(ca_pem: &str) -> (Vec<ClientConfigEntry>, TempDir) {
        let directory = tempfile::tempdir().assured("a temporary directory can be created");
        let ca_file = directory.path().join("ca.pem");
        std::fs::write(&ca_file, ca_pem).assured("the fixture CA file can be written");
        let config = vec![
            entry("endpoint", "wss://websocket.nervix.test/events"),
            entry("tls_ca_file", &ca_file.display().to_string()),
        ];
        (config, directory)
    }

    fn private_key_pem() -> String {
        KeyPair::generate()
            .assured("a fixture key generates")
            .serialize_pem()
    }

    /// A CA certificate rustls takes as a trust anchor, in PEM.
    fn ca_certificate_pem() -> String {
        let key = KeyPair::generate().assured("a CA key generates");
        let mut params =
            CertificateParams::new(Vec::new()).assured("an empty SAN list is a valid request");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "nervix websocket test ca");
        params
            .self_signed(&key)
            .assured("a CA certificate generates")
            .pem()
    }

    #[test]
    fn a_client_without_a_ca_file_has_nothing_to_read() {
        validate_client_configuration(&[entry("endpoint", "wss://websocket.nervix.test/events")])
            .assured("the default roots verify a client that names no CA file");
    }

    #[test]
    fn a_ca_file_with_a_certificate_passes_beside_sections_the_source_skips() {
        let certificate = ca_certificate_pem();
        let key = private_key_pem();
        for ca_pem in [certificate.clone(), format!("a note\n{key}{certificate}")] {
            let (config, _directory) = client_trusting(&ca_pem);
            validate_client_configuration(&config).assured("the file holds a certificate");
        }
    }

    #[test]
    fn a_ca_file_without_a_certificate_is_refused_by_its_key() {
        let key = private_key_pem();
        for ca_pem in ["", "no certificate is written here\n", key.as_str()] {
            let (config, _directory) = client_trusting(ca_pem);
            let refusal = validate_client_configuration(&config)
                .err()
                .assured("a file without a certificate adds no trust root");
            assert!(matches!(
                refusal.current_context(),
                OfflineClientConfigError::NoCertificate { key } if key == "tls_ca_file"
            ));
            assert_eq!(
                refusal.to_string(),
                "client config key 'tls_ca_file' holds no certificate"
            );
        }
    }

    #[test]
    fn a_ca_file_it_cannot_read_keeps_its_cause_beneath_the_key() {
        let directory = tempfile::tempdir().assured("a temporary directory can be created");
        let missing = directory.path().join("missing.pem");
        let refusal =
            validate_client_configuration(&[entry("tls_ca_file", &missing.display().to_string())])
                .err()
                .assured("the file does not exist");
        assert!(matches!(
            refusal.current_context(),
            OfflineClientConfigError::InvalidKey { key } if key == "tls_ca_file"
        ));
        assert!(matches!(
            refusal.downcast_ref::<ClientConfigError>(),
            Some(ClientConfigError::ReadTlsFile { .. })
        ));
    }
}
