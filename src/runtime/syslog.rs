use std::sync::Arc as StdArc;

use ahash::HashSet;
use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use thiserror::Error;
use url::Url;

use super::{RustlsClientConfigSource, client_tls_paths, read_tls_file};

pub(super) const DEFAULT_MAX_MESSAGE_SIZE: usize = 131_072;
pub(super) const MAX_UDP_PAYLOAD_SIZE: usize = 65_507;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SyslogDirection {
    Ingest,
    Emit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SyslogProtocol {
    Udp,
    Tcp,
    Tls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SyslogFraming {
    OctetCounting,
    NonTransparent,
}

#[derive(Debug, Error)]
pub(super) enum SyslogConfigError {
    #[error("missing Syslog client config key '{key}'")]
    MissingKey { key: &'static str },
    #[error("missing Syslog TLS {direction} config key '{key}'")]
    MissingTlsKey {
        direction: &'static str,
        key: &'static str,
    },
    #[error("unknown Syslog client config key '{key}'")]
    UnknownKey { key: String },
    #[error("duplicate Syslog client config key '{key}'")]
    DuplicateKey { key: String },
    #[error(
        "invalid Syslog client config key 'protocol': expected udp, tcp, or tls, found '{value}'"
    )]
    Protocol { value: String },
    #[error(
        "invalid Syslog client config key 'framing': expected octet-counting or non-transparent, \
         found '{value}'"
    )]
    Framing { value: String },
    #[error("invalid Syslog client config key 'max_message_size' value '{value}': {source}")]
    MessageSize {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("invalid Syslog client config key 'max_message_size': value must be greater than zero")]
    ZeroMessageSize,
    #[error("invalid Syslog client config key 'addr' value '{value}': {source}")]
    AddressParse {
        value: String,
        #[source]
        source: url::ParseError,
    },
    #[error("invalid Syslog client config key 'addr' value '{value}': expected host:port")]
    AddressShape { value: String },
    #[error("invalid Syslog client config key 'addr' value '{value}': host is missing")]
    AddressHostMissing { value: String },
    #[error("invalid Syslog client config key 'addr' value '{value}': port is missing")]
    AddressPortMissing { value: String },
    #[error("invalid Syslog client config key 'framing': UDP does not use stream framing")]
    UdpFraming,
    #[error("invalid Syslog client config key 'framing': TLS requires octet-counting framing")]
    TlsFraming,
    #[error("Syslog TLS {direction} config key '{key}' requires '{required}'")]
    IdentityPair {
        direction: &'static str,
        key: &'static str,
        required: &'static str,
    },
    #[error("invalid Syslog client config key '{key}': TLS files require protocol=tls")]
    TlsWithPlainProtocol { key: &'static str },
    #[error("invalid Syslog TLS configuration: {reason}")]
    TlsMaterial { reason: String },
}

#[derive(Debug, Clone)]
pub(super) struct SyslogClientConfig {
    pub(super) protocol: SyslogProtocol,
    pub(super) addr: String,
    pub(super) server_name: String,
    pub(super) max_message_size: usize,
    pub(super) framing: SyslogFraming,
    entries: Vec<nervix_models::ClientConfigEntry>,
}

impl SyslogClientConfig {
    pub(super) fn parse(
        entries: &[nervix_models::ClientConfigEntry],
        direction: SyslogDirection,
    ) -> Result<Self, SyslogConfigError> {
        Self::validate_keys(entries)?;
        let protocol = Self::required_value(entries, "protocol")?;
        let protocol = match protocol.as_str() {
            "udp" => SyslogProtocol::Udp,
            "tcp" => SyslogProtocol::Tcp,
            "tls" => SyslogProtocol::Tls,
            value => {
                return Err(SyslogConfigError::Protocol {
                    value: value.to_string(),
                });
            }
        };
        let addr = Self::required_value(entries, "addr")?;
        let server_name = Self::validate_addr(&addr)?;
        let max_message_size = Self::optional_value(entries, "max_message_size")
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|source| SyslogConfigError::MessageSize {
                        value: value.to_string(),
                        source,
                    })
            })
            .transpose()?
            .unwrap_or(DEFAULT_MAX_MESSAGE_SIZE);
        if max_message_size == 0 {
            return Err(SyslogConfigError::ZeroMessageSize);
        }
        let explicit_framing = Self::optional_value(entries, "framing");
        let framing = match explicit_framing.unwrap_or("octet-counting") {
            "octet-counting" => SyslogFraming::OctetCounting,
            "non-transparent" => SyslogFraming::NonTransparent,
            value => {
                return Err(SyslogConfigError::Framing {
                    value: value.to_string(),
                });
            }
        };
        if protocol == SyslogProtocol::Udp && explicit_framing.is_some() {
            return Err(SyslogConfigError::UdpFraming);
        }
        if protocol == SyslogProtocol::Tls && framing == SyslogFraming::NonTransparent {
            return Err(SyslogConfigError::TlsFraming);
        }

        let tls = client_tls_paths(entries);
        match (protocol, direction) {
            (SyslogProtocol::Tls, SyslogDirection::Ingest) => {
                if tls.cert_file.is_none() {
                    return Err(SyslogConfigError::MissingTlsKey {
                        direction: "ingestor",
                        key: "tls_cert_file",
                    });
                }
                if tls.key_file.is_none() {
                    return Err(SyslogConfigError::MissingTlsKey {
                        direction: "ingestor",
                        key: "tls_key_file",
                    });
                }
            }
            (SyslogProtocol::Tls, SyslogDirection::Emit) => {
                match (tls.cert_file.as_ref(), tls.key_file.as_ref()) {
                    (Some(_), Some(_)) | (None, None) => {}
                    (Some(_), None) => {
                        return Err(SyslogConfigError::IdentityPair {
                            direction: "emitter",
                            key: "tls_cert_file",
                            required: "tls_key_file",
                        });
                    }
                    (None, Some(_)) => {
                        return Err(SyslogConfigError::IdentityPair {
                            direction: "emitter",
                            key: "tls_key_file",
                            required: "tls_cert_file",
                        });
                    }
                }
            }
            (SyslogProtocol::Udp | SyslogProtocol::Tcp, _) if !tls.is_empty() => {
                let key = if tls.ca_file.is_some() {
                    "tls_ca_file"
                } else if tls.cert_file.is_some() {
                    "tls_cert_file"
                } else {
                    "tls_key_file"
                };
                return Err(SyslogConfigError::TlsWithPlainProtocol { key });
            }
            (SyslogProtocol::Udp | SyslogProtocol::Tcp, _) => {}
        }

        Ok(Self {
            protocol,
            addr,
            server_name,
            max_message_size,
            framing,
            entries: entries.to_vec(),
        })
    }

    fn required_value(
        entries: &[nervix_models::ClientConfigEntry],
        key: &'static str,
    ) -> Result<String, SyslogConfigError> {
        Self::optional_value(entries, key)
            .map(str::to_string)
            .ok_or(SyslogConfigError::MissingKey { key })
    }

    fn optional_value<'a>(
        entries: &'a [nervix_models::ClientConfigEntry],
        key: &str,
    ) -> Option<&'a str> {
        entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| entry.value.as_str())
    }

    fn validate_keys(
        entries: &[nervix_models::ClientConfigEntry],
    ) -> Result<(), SyslogConfigError> {
        let known = [
            "protocol",
            "addr",
            "max_message_size",
            "framing",
            "tls_cert_file",
            "tls_key_file",
            "tls_ca_file",
        ];
        let mut seen = HashSet::default();
        for entry in entries {
            if !known.contains(&entry.key.as_str()) {
                return Err(SyslogConfigError::UnknownKey {
                    key: entry.key.clone(),
                });
            }
            if !seen.insert(entry.key.clone()) {
                return Err(SyslogConfigError::DuplicateKey {
                    key: entry.key.clone(),
                });
            }
        }
        Ok(())
    }

    fn validate_addr(addr: &str) -> Result<String, SyslogConfigError> {
        let parsed = Url::parse(&format!("syslog://{addr}")).map_err(|source| {
            SyslogConfigError::AddressParse {
                value: addr.to_string(),
                source,
            }
        })?;
        if !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.path() != ""
        {
            return Err(SyslogConfigError::AddressShape {
                value: addr.to_string(),
            });
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| SyslogConfigError::AddressHostMissing {
                value: addr.to_string(),
            })?;
        if parsed.port().is_none() {
            return Err(SyslogConfigError::AddressPortMissing {
                value: addr.to_string(),
            });
        }
        Ok(host.to_string())
    }

    pub(super) fn tls_client_config(
        &self,
    ) -> Result<StdArc<rustls::ClientConfig>, SyslogConfigError> {
        RustlsClientConfigSource::new(&self.entries)
            .build_with_default_roots()
            .map_err(|reason| SyslogConfigError::TlsMaterial { reason })
    }

    pub(super) fn tls_server_config(&self) -> Result<StdArc<ServerConfig>, SyslogConfigError> {
        nervix_interconnect::install_rustls_crypto_provider();
        let tls = client_tls_paths(&self.entries);
        let cert_file = tls
            .cert_file
            .as_ref()
            .ok_or(SyslogConfigError::MissingTlsKey {
                direction: "ingestor",
                key: "tls_cert_file",
            })?;
        let key_file = tls
            .key_file
            .as_ref()
            .ok_or(SyslogConfigError::MissingTlsKey {
                direction: "ingestor",
                key: "tls_key_file",
            })?;
        let cert_pem = read_tls_file(cert_file, "Syslog TLS server certificate")
            .map_err(|reason| SyslogConfigError::TlsMaterial { reason })?;
        let key_pem = read_tls_file(key_file, "Syslog TLS server private key")
            .map_err(|reason| SyslogConfigError::TlsMaterial { reason })?;
        let certs = CertificateDer::pem_slice_iter(&cert_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| SyslogConfigError::TlsMaterial {
                reason: format!(
                    "invalid Syslog client config key 'tls_cert_file' '{}': {error}",
                    cert_file.display()
                ),
            })?;
        if certs.is_empty() {
            return Err(SyslogConfigError::TlsMaterial {
                reason: format!(
                    "invalid Syslog client config key 'tls_cert_file' '{}': no certificates found",
                    cert_file.display()
                ),
            });
        }
        let key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(|error| {
            SyslogConfigError::TlsMaterial {
                reason: format!(
                    "invalid Syslog client config key 'tls_key_file' '{}': {error}",
                    key_file.display()
                ),
            }
        })?;
        let builder = ServerConfig::builder();
        let config = if let Some(ca_file) = tls.ca_file.as_ref() {
            let ca_pem = read_tls_file(ca_file, "Syslog TLS client CA certificate")
                .map_err(|reason| SyslogConfigError::TlsMaterial { reason })?;
            let mut roots = RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(&ca_pem) {
                let cert = cert.map_err(|error| SyslogConfigError::TlsMaterial {
                    reason: format!(
                        "invalid Syslog client config key 'tls_ca_file' '{}': {error}",
                        ca_file.display()
                    ),
                })?;
                roots
                    .add(cert)
                    .map_err(|error| SyslogConfigError::TlsMaterial {
                        reason: format!(
                            "invalid Syslog client config key 'tls_ca_file' '{}': {error}",
                            ca_file.display()
                        ),
                    })?;
            }
            let verifier = WebPkiClientVerifier::builder(StdArc::new(roots))
                .build()
                .map_err(|error| SyslogConfigError::TlsMaterial {
                    reason: format!(
                        "invalid Syslog client config key 'tls_ca_file' '{}': {error}",
                        ca_file.display()
                    ),
                })?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
        } else {
            builder.with_no_client_auth().with_single_cert(certs, key)
        }
        .map_err(|error| SyslogConfigError::TlsMaterial {
            reason: format!("invalid Syslog TLS server identity: {error}"),
        })?;
        Ok(StdArc::new(config))
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::ClientConfigEntry;

    use super::*;

    fn entries(values: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
        values
            .iter()
            .map(|(key, value)| ClientConfigEntry {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect()
    }

    #[test]
    fn parses_udp_defaults() {
        let parsed = SyslogClientConfig::parse(
            &entries(&[("protocol", "udp"), ("addr", "127.0.0.1:5514")]),
            SyslogDirection::Ingest,
        )
        .expect("UDP config should parse");
        assert_eq!(parsed.protocol, SyslogProtocol::Udp);
        assert_eq!(parsed.max_message_size, DEFAULT_MAX_MESSAGE_SIZE);
        assert_eq!(parsed.framing, SyslogFraming::OctetCounting);
    }

    #[test]
    fn validates_directional_tls_identity_and_framing() {
        let missing_identity = SyslogClientConfig::parse(
            &entries(&[("protocol", "tls"), ("addr", "localhost:6514")]),
            SyslogDirection::Ingest,
        )
        .expect_err("TLS ingestor identity is required");
        assert!(missing_identity.to_string().contains("tls_cert_file"));

        let half_identity = SyslogClientConfig::parse(
            &entries(&[
                ("protocol", "tls"),
                ("addr", "localhost:6514"),
                ("tls_cert_file", "/tmp/client.pem"),
            ]),
            SyslogDirection::Emit,
        )
        .expect_err("half a TLS client identity must fail");
        assert!(half_identity.to_string().contains("tls_key_file"));

        let tls_framing = SyslogClientConfig::parse(
            &entries(&[
                ("protocol", "tls"),
                ("addr", "localhost:6514"),
                ("framing", "non-transparent"),
            ]),
            SyslogDirection::Emit,
        )
        .expect_err("TLS non-transparent framing must fail");
        assert!(tls_framing.to_string().contains("framing"));
    }

    #[test]
    fn rejects_malformed_addresses_and_udp_framing() {
        let malformed = SyslogClientConfig::parse(
            &entries(&[("protocol", "tcp"), ("addr", "missing-port")]),
            SyslogDirection::Emit,
        )
        .expect_err("address without a port must fail");
        assert!(malformed.to_string().contains("addr"));

        let udp_framing = SyslogClientConfig::parse(
            &entries(&[
                ("protocol", "udp"),
                ("addr", "127.0.0.1:5514"),
                ("framing", "octet-counting"),
            ]),
            SyslogDirection::Emit,
        )
        .expect_err("UDP framing must fail");
        assert!(udp_framing.to_string().contains("framing"));

        let uppercase_protocol = SyslogClientConfig::parse(
            &entries(&[("protocol", "UDP"), ("addr", "127.0.0.1:5514")]),
            SyslogDirection::Emit,
        )
        .expect_err("protocol values use one lowercase shape");
        assert!(uppercase_protocol.to_string().contains("protocol"));
    }
}
