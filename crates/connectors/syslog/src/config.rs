//! Syslog client configuration shared by the ingestor and emitter transports.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Syslog transport, framing, size and TLS configuration interpretation, and the
//!   transports over which each deadline key bounds a wait; the client declaration states which
//!   side reads each key.
//! - **Depends on.** Connector TLS primitives, Syslog client entries, and rustls.
//! - **Must not know.** Runtime tasks, relays, branches, schedules, Models beyond typed client
//!   entries, or registry state.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::num::NonZeroUsize;

use ahash::HashSet;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_connector::{
    RustlsClientConfigProvider, client_tls_paths, install_rustls_crypto_provider, read_tls_file,
};
use nervix_models::{ClientConfigKey, CreateClientSyslog, DeadlineKey};
use nervix_primitives::sync::StdArc;
use nonzero_ext::nonzero;
use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use strum::IntoEnumIterator as _;
use thiserror::Error;
use url::{Host, Url};

pub const DEFAULT_MAX_MESSAGE_SIZE: NonZeroUsize = nonzero!(131_072usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyslogDirection {
    Ingest,
    Emit,
}

impl SyslogDirection {
    /// Whether a client used on this side reads `key`, as the client declaration states.
    fn reads(self, key: &ClientConfigKey) -> bool {
        match self {
            Self::Ingest => key.readers.include_ingestors(),
            Self::Emit => key.readers.include_emitters(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyslogProtocol {
    Udp,
    Tcp,
    Tls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyslogFraming {
    OctetCounting,
    NonTransparent,
}

/// The Syslog clients whose waits one deadline key bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum SyslogDeadlineScope {
    /// `connect_timeout_ms`: the connection an emitter opens, and the handshake a TLS listener
    /// completes with each connection it accepts.
    #[strum(to_string = "an emitter's connection or a TLS listener's handshake")]
    ConnectionOrTlsHandshake,
    /// `timeout_ms`: the frames an emitter writes.
    #[strum(to_string = "an emitter's frame writes")]
    EmitterWrites,
    /// `idle_timeout_ms`: the connections a TCP or TLS listener reads.
    #[strum(to_string = "a TCP or TLS listener's connections")]
    ListenerConnections,
}

impl SyslogDeadlineScope {
    /// The clients whose waits `key` bounds.
    fn of(key: DeadlineKey) -> Self {
        match key {
            DeadlineKey::Timeout => Self::EmitterWrites,
            DeadlineKey::ConnectTimeout => Self::ConnectionOrTlsHandshake,
            DeadlineKey::IdleTimeout => Self::ListenerConnections,
        }
    }

    /// Whether a client of `direction` that reads this scope's key performs its wait over
    /// `protocol`: a listener completes TLS handshakes only over TLS, and reads connections only
    /// over TCP or TLS.
    fn waits_over(self, direction: SyslogDirection, protocol: SyslogProtocol) -> bool {
        match (self, direction) {
            (Self::ConnectionOrTlsHandshake, SyslogDirection::Ingest) => {
                protocol == SyslogProtocol::Tls
            }
            (Self::ListenerConnections, _) => protocol != SyslogProtocol::Udp,
            (Self::ConnectionOrTlsHandshake, SyslogDirection::Emit) | (Self::EmitterWrites, _) => {
                true
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum SyslogConfigError {
    #[error("missing Syslog client config key '{key}'")]
    MissingKey { key: &'static str },
    #[error("missing Syslog TLS {direction} config key '{key}'")]
    MissingTlsKey {
        direction: &'static str,
        key: &'static str,
    },
    #[error("unknown Syslog client config key '{key}'")]
    UnknownKey { key: String },
    /// A deadline key on a client whose direction and protocol never perform the wait it bounds.
    #[error("Syslog client config key '{key}' applies only to {scope}")]
    InapplicableDeadlineKey {
        key: &'static str,
        scope: SyslogDeadlineScope,
    },
    /// A deadline key's value breaks the deadline rule, which the frames beneath name with the key.
    #[error("invalid Syslog client deadline")]
    InvalidDeadline,
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
    #[error("invalid Syslog client config key 'max_message_size' value '{value}'")]
    MessageSize {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("invalid Syslog client config key 'addr' value '{value}'")]
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
pub struct SyslogClientConfig {
    pub protocol: SyslogProtocol,
    pub addr: String,
    pub server_name: String,
    pub(crate) port: u16,
    pub max_message_size: NonZeroUsize,
    pub framing: SyslogFraming,
    entries: Vec<nervix_models::ClientConfigEntry>,
}

impl SyslogClientConfig {
    pub fn parse(
        entries: &[nervix_models::ClientConfigEntry],
        direction: SyslogDirection,
    ) -> error_stack::Result<Self, SyslogConfigError> {
        Self::validate_keys(entries)?;
        let protocol = Self::required_value(entries, "protocol")?;
        let protocol = match protocol.as_str() {
            "udp" => SyslogProtocol::Udp,
            "tcp" => SyslogProtocol::Tcp,
            "tls" => SyslogProtocol::Tls,
            value => {
                return Err(Report::new(SyslogConfigError::Protocol {
                    value: value.to_string(),
                }));
            }
        };
        let addr = Self::required_value(entries, "addr")?;
        let (server_name, port) = Self::validate_addr(&addr)?;
        let max_message_size = Self::optional_value(entries, "max_message_size")
            .map(|value| {
                value.parse::<NonZeroUsize>().map_err(|source| {
                    Report::new(SyslogConfigError::MessageSize {
                        value: value.to_string(),
                        source,
                    })
                })
            })
            .transpose()?
            .unwrap_or(DEFAULT_MAX_MESSAGE_SIZE);
        let explicit_framing = Self::optional_value(entries, "framing");
        let framing = match explicit_framing.unwrap_or("octet-counting") {
            "octet-counting" => SyslogFraming::OctetCounting,
            "non-transparent" => SyslogFraming::NonTransparent,
            value => {
                return Err(Report::new(SyslogConfigError::Framing {
                    value: value.to_string(),
                }));
            }
        };
        Self::validate_deadline_keys(entries, direction, protocol)?;
        if protocol == SyslogProtocol::Udp && explicit_framing.is_some() {
            return Err(Report::new(SyslogConfigError::UdpFraming));
        }
        if protocol == SyslogProtocol::Tls && framing == SyslogFraming::NonTransparent {
            return Err(Report::new(SyslogConfigError::TlsFraming));
        }

        let tls = client_tls_paths(entries);
        match (protocol, direction) {
            (SyslogProtocol::Tls, SyslogDirection::Ingest) => {
                if tls.cert_file.is_none() {
                    return Err(Report::new(SyslogConfigError::MissingTlsKey {
                        direction: "ingestor",
                        key: "tls_cert_file",
                    }));
                }
                if tls.key_file.is_none() {
                    return Err(Report::new(SyslogConfigError::MissingTlsKey {
                        direction: "ingestor",
                        key: "tls_key_file",
                    }));
                }
            }
            (SyslogProtocol::Tls, SyslogDirection::Emit) => {
                match (tls.cert_file.as_ref(), tls.key_file.as_ref()) {
                    (Some(_), Some(_)) | (None, None) => {}
                    (Some(_), None) => {
                        return Err(Report::new(SyslogConfigError::IdentityPair {
                            direction: "emitter",
                            key: "tls_cert_file",
                            required: "tls_key_file",
                        }));
                    }
                    (None, Some(_)) => {
                        return Err(Report::new(SyslogConfigError::IdentityPair {
                            direction: "emitter",
                            key: "tls_key_file",
                            required: "tls_cert_file",
                        }));
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
                return Err(Report::new(SyslogConfigError::TlsWithPlainProtocol { key }));
            }
            (SyslogProtocol::Udp | SyslogProtocol::Tcp, _) => {}
        }

        Ok(Self {
            protocol,
            addr,
            server_name,
            port,
            max_message_size,
            framing,
            entries: entries.to_vec(),
        })
    }

    fn required_value(
        entries: &[nervix_models::ClientConfigEntry],
        key: &'static str,
    ) -> error_stack::Result<String, SyslogConfigError> {
        match Self::optional_value(entries, key) {
            Some(value) => Ok(value.to_string()),
            None => Err(Report::new(SyslogConfigError::MissingKey { key })),
        }
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
    ) -> error_stack::Result<(), SyslogConfigError> {
        let schema = nervix_models::CreateClientSyslog::<u64>::DECLARATION.config;
        let mut seen = HashSet::default();
        for entry in entries {
            if schema.key(&entry.key).is_none() {
                return Err(Report::new(SyslogConfigError::UnknownKey {
                    key: entry.key.clone(),
                }));
            }
            if !seen.insert(entry.key.clone()) {
                return Err(Report::new(SyslogConfigError::DuplicateKey {
                    key: entry.key.clone(),
                }));
            }
        }
        Ok(())
    }

    /// Refuses a deadline key on a client of `direction` over `protocol` that never performs the
    /// wait the key bounds, so every key a client sets bounds something. The declaration states
    /// which side reads the key; the transport decides whether that side waits over `protocol`.
    fn validate_deadline_keys(
        entries: &[nervix_models::ClientConfigEntry],
        direction: SyslogDirection,
        protocol: SyslogProtocol,
    ) -> error_stack::Result<(), SyslogConfigError> {
        let schema = CreateClientSyslog::<u64>::DECLARATION.config;
        for key in DeadlineKey::iter() {
            if Self::optional_value(entries, key.key()).is_none() {
                continue;
            }
            let declared = schema
                .key(key.key())
                .verified("validate_keys refused every entry the client does not declare");
            let scope = SyslogDeadlineScope::of(key);
            if !direction.reads(declared) || !scope.waits_over(direction, protocol) {
                return Err(Report::new(SyslogConfigError::InapplicableDeadlineKey {
                    key: key.key(),
                    scope,
                }));
            }
        }
        Ok(())
    }

    fn validate_addr(addr: &str) -> error_stack::Result<(String, u16), SyslogConfigError> {
        let parsed = Url::parse(&format!("syslog://{addr}")).map_err(|source| {
            Report::new(SyslogConfigError::AddressParse {
                value: addr.to_string(),
                source,
            })
        })?;
        if !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.path() != ""
        {
            return Err(Report::new(SyslogConfigError::AddressShape {
                value: addr.to_string(),
            }));
        }
        let host = parsed.host().ok_or_else(|| {
            Report::new(SyslogConfigError::AddressHostMissing {
                value: addr.to_string(),
            })
        })?;
        let port = parsed.port().ok_or_else(|| {
            Report::new(SyslogConfigError::AddressPortMissing {
                value: addr.to_string(),
            })
        })?;
        let host = match host {
            Host::Domain(name) => name.to_string(),
            Host::Ipv4(address) => address.to_string(),
            Host::Ipv6(address) => address.to_string(),
        };
        Ok((host, port))
    }

    pub fn tls_client_config(
        &self,
    ) -> error_stack::Result<StdArc<rustls::ClientConfig>, SyslogConfigError> {
        {
            let mapping_input =
                RustlsClientConfigProvider::new(&self.entries).build_with_default_roots();
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let reason = error.to_string();
                    error.change_context(SyslogConfigError::TlsMaterial { reason })
                }),
            }
        }
    }

    pub fn tls_server_config(
        &self,
    ) -> error_stack::Result<StdArc<ServerConfig>, SyslogConfigError> {
        install_rustls_crypto_provider();
        let tls = client_tls_paths(&self.entries);
        let cert_file = tls.cert_file.as_ref().ok_or_else(|| {
            Report::new(SyslogConfigError::MissingTlsKey {
                direction: "ingestor",
                key: "tls_cert_file",
            })
        })?;
        let key_file = tls.key_file.as_ref().ok_or_else(|| {
            Report::new(SyslogConfigError::MissingTlsKey {
                direction: "ingestor",
                key: "tls_key_file",
            })
        })?;
        let cert_pem = {
            let mapping_input = read_tls_file(cert_file, "Syslog TLS server certificate");
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let reason = error.to_string();
                    error.change_context(SyslogConfigError::TlsMaterial { reason })
                }),
            }
        }?;
        let key_pem = {
            let mapping_input = read_tls_file(key_file, "Syslog TLS server private key");
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let reason = error.to_string();
                    error.change_context(SyslogConfigError::TlsMaterial { reason })
                }),
            }
        }?;
        let certs = {
            let mapping_input =
                CertificateDer::pem_slice_iter(&cert_pem).collect::<Result<Vec<_>, _>>();
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let reason = format!(
                        "invalid Syslog client config key 'tls_cert_file' '{}': {error}",
                        cert_file.display()
                    );
                    Report::new(error).change_context(SyslogConfigError::TlsMaterial { reason })
                }),
            }
        }?;
        if certs.is_empty() {
            return Err(Report::new(SyslogConfigError::TlsMaterial {
                reason: format!(
                    "invalid Syslog client config key 'tls_cert_file' '{}': no certificates found",
                    cert_file.display()
                ),
            }));
        }
        let key = {
            let mapping_input = PrivateKeyDer::from_pem_slice(&key_pem);
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let reason = format!(
                        "invalid Syslog client config key 'tls_key_file' '{}': {error}",
                        key_file.display()
                    );
                    Report::new(error).change_context(SyslogConfigError::TlsMaterial { reason })
                }),
            }
        }?;
        let builder = ServerConfig::builder();
        let config = {
            let mapping_input = if let Some(ca_file) = tls.ca_file.as_ref() {
                let ca_pem = {
                    let mapping_input = read_tls_file(ca_file, "Syslog TLS client CA certificate");
                    match mapping_input {
                        Ok(mapped_value) => Ok(mapped_value),
                        Err(error) => Err({
                            let reason = error.to_string();
                            error.change_context(SyslogConfigError::TlsMaterial { reason })
                        }),
                    }
                }?;
                let mut roots = RootCertStore::empty();
                for cert in CertificateDer::pem_slice_iter(&ca_pem) {
                    let cert = {
                        let mapping_input = cert;
                        match mapping_input {
                            Ok(mapped_value) => Ok(mapped_value),
                            Err(error) => Err({
                                let reason = format!(
                                    "invalid Syslog client config key 'tls_ca_file' '{}': {error}",
                                    ca_file.display()
                                );
                                Report::new(error)
                                    .change_context(SyslogConfigError::TlsMaterial { reason })
                            }),
                        }
                    }?;
                    {
                        let mapping_input = roots.add(cert);
                        match mapping_input {
                            Ok(mapped_value) => Ok(mapped_value),
                            Err(error) => Err({
                                let reason = format!(
                                    "invalid Syslog client config key 'tls_ca_file' '{}': {error}",
                                    ca_file.display()
                                );
                                Report::new(error)
                                    .change_context(SyslogConfigError::TlsMaterial { reason })
                            }),
                        }
                    }?;
                }
                let verifier = {
                    let mapping_input = WebPkiClientVerifier::builder(StdArc::new(roots)).build();
                    match mapping_input {
                        Ok(mapped_value) => Ok(mapped_value),
                        Err(error) => Err({
                            let reason = format!(
                                "invalid Syslog client config key 'tls_ca_file' '{}': {error}",
                                ca_file.display()
                            );
                            Report::new(error)
                                .change_context(SyslogConfigError::TlsMaterial { reason })
                        }),
                    }
                }?;
                builder
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(certs, key)
            } else {
                builder.with_no_client_auth().with_single_cert(certs, key)
            };
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let reason = format!("invalid Syslog TLS server identity: {error}");
                    Report::new(error).change_context(SyslogConfigError::TlsMaterial { reason })
                }),
            }
        }?;
        Ok(StdArc::new(config))
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_models::ClientConfigEntry;

    use super::*;

    type InvalidCase<'a> = (&'a [(&'a str, &'a str)], SyslogDirection, &'a str);

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

    /// A client's direction and transport.
    #[derive(Debug, Clone, Copy)]
    struct Client {
        direction: SyslogDirection,
        protocol: &'static str,
    }

    const fn client(direction: SyslogDirection, protocol: &'static str) -> Client {
        Client {
            direction,
            protocol,
        }
    }

    /// One deadline key, the clients that perform the wait it bounds, and the clients that do not.
    struct DeadlineCase {
        key: &'static str,
        scope: SyslogDeadlineScope,
        applies: &'static [Client],
        refused: &'static [Client],
    }

    const UDP_EMITTER: Client = client(SyslogDirection::Emit, "udp");
    const TCP_EMITTER: Client = client(SyslogDirection::Emit, "tcp");
    const TLS_EMITTER: Client = client(SyslogDirection::Emit, "tls");
    const UDP_LISTENER: Client = client(SyslogDirection::Ingest, "udp");
    const TCP_LISTENER: Client = client(SyslogDirection::Ingest, "tcp");
    const TLS_LISTENER: Client = client(SyslogDirection::Ingest, "tls");

    #[test]
    fn a_deadline_key_applies_only_to_a_client_that_performs_its_wait() {
        let cases = [
            DeadlineCase {
                key: "connect_timeout_ms",
                scope: SyslogDeadlineScope::ConnectionOrTlsHandshake,
                applies: &[UDP_EMITTER, TCP_EMITTER, TLS_EMITTER, TLS_LISTENER],
                refused: &[UDP_LISTENER, TCP_LISTENER],
            },
            DeadlineCase {
                key: "timeout_ms",
                scope: SyslogDeadlineScope::EmitterWrites,
                applies: &[UDP_EMITTER, TCP_EMITTER, TLS_EMITTER],
                refused: &[UDP_LISTENER, TCP_LISTENER, TLS_LISTENER],
            },
            DeadlineCase {
                key: "idle_timeout_ms",
                scope: SyslogDeadlineScope::ListenerConnections,
                applies: &[TCP_LISTENER, TLS_LISTENER],
                refused: &[UDP_LISTENER, UDP_EMITTER, TCP_EMITTER, TLS_EMITTER],
            },
        ];
        for case in cases {
            for performer in case.applies {
                let settings = deadline_client(performer.protocol, case.key);
                if let Err(error) = SyslogClientConfig::parse(&settings, performer.direction) {
                    panic!("{} bounds a wait of {performer:?}: {error}", case.key);
                }
            }
            for bystander in case.refused {
                let settings = deadline_client(bystander.protocol, case.key);
                let error = SyslogClientConfig::parse(&settings, bystander.direction)
                    .expect_err("the client performs no wait the key bounds");
                assert_eq!(
                    error.current_context().to_string(),
                    format!(
                        "Syslog client config key '{}' applies only to {}",
                        case.key, case.scope
                    ),
                    "{bystander:?}"
                );
                let SyslogConfigError::InapplicableDeadlineKey { key, scope } =
                    error.current_context()
                else {
                    panic!(
                        "{bystander:?} must refuse {} by its scope: {error:?}",
                        case.key
                    );
                };
                assert_eq!((*key, *scope), (case.key, case.scope), "{bystander:?}");
            }
        }
    }

    /// A client over `protocol` that sets the deadline `key`, with the TLS identity a TLS listener
    /// requires.
    fn deadline_client(protocol: &str, key: &str) -> Vec<ClientConfigEntry> {
        let mut settings = vec![
            ("protocol", protocol),
            ("addr", "127.0.0.1:6514"),
            (key, "1000"),
        ];
        if protocol == "tls" {
            settings.push(("tls_cert_file", "/tmp/server.pem"));
            settings.push(("tls_key_file", "/tmp/server.key"));
        }
        entries(&settings)
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

    #[test]
    fn invalid_client_settings_name_the_setting_that_failed() {
        let cases: &[InvalidCase<'_>] = &[
            (
                &[],
                SyslogDirection::Emit,
                "missing Syslog client config key 'protocol'",
            ),
            (
                &[("protocol", "tcp")],
                SyslogDirection::Emit,
                "config key 'addr'",
            ),
            (
                &[
                    ("protocol", "tcp"),
                    ("addr", "localhost:5514"),
                    ("extra", "x"),
                ],
                SyslogDirection::Emit,
                "unknown Syslog client config key 'extra'",
            ),
            (
                &[
                    ("protocol", "tcp"),
                    ("protocol", "udp"),
                    ("addr", "localhost:5514"),
                ],
                SyslogDirection::Emit,
                "duplicate Syslog client config key 'protocol'",
            ),
            (
                &[
                    ("protocol", "tcp"),
                    ("addr", "localhost:5514"),
                    ("max_message_size", "0"),
                ],
                SyslogDirection::Emit,
                "max_message_size",
            ),
            (
                &[
                    ("protocol", "tcp"),
                    ("addr", "localhost:5514"),
                    ("framing", "invalid"),
                ],
                SyslogDirection::Emit,
                "config key 'framing'",
            ),
            (
                &[("protocol", "tcp"), ("addr", "localhost:invalid")],
                SyslogDirection::Emit,
                "config key 'addr'",
            ),
            (
                &[("protocol", "tcp"), ("addr", "localhost:5514?query=1")],
                SyslogDirection::Emit,
                "config key 'addr'",
            ),
            (
                &[
                    ("protocol", "udp"),
                    ("addr", "localhost:5514"),
                    ("tls_ca_file", "/tmp/ca.pem"),
                ],
                SyslogDirection::Emit,
                "TLS files require protocol=tls",
            ),
            (
                &[
                    ("protocol", "tls"),
                    ("addr", "localhost:6514"),
                    ("tls_cert_file", "/tmp/cert.pem"),
                ],
                SyslogDirection::Ingest,
                "tls_key_file",
            ),
            (
                &[
                    ("protocol", "tls"),
                    ("addr", "localhost:6514"),
                    ("tls_key_file", "/tmp/key.pem"),
                ],
                SyslogDirection::Emit,
                "tls_cert_file",
            ),
        ];
        for (values, direction, expected) in cases {
            let error = SyslogClientConfig::parse(&entries(values), *direction)
                .expect_err("the invalid setting must be rejected");
            assert!(error.to_string().contains(*expected), "{error}");
        }
    }

    #[test]
    fn a_setting_that_does_not_parse_keeps_its_parser_error_beneath_it() {
        let error = SyslogClientConfig::parse(
            &entries(&[
                ("protocol", "tcp"),
                ("addr", "localhost:5514"),
                ("max_message_size", "many"),
            ]),
            SyslogDirection::Emit,
        )
        .expect_err("a message size must be a positive integer");

        assert!(matches!(
            error.current_context(),
            SyslogConfigError::MessageSize { value, .. } if value == "many"
        ));
        assert_eq!(
            format!("{error:#}"),
            "invalid Syslog client config key 'max_message_size' value 'many': invalid digit \
             found in string"
        );
    }

    #[test]
    fn syslog_tls_material_read_failures_keep_their_context() {
        let missing = "/definitely/missing/nervix-syslog-tls.pem";
        let emitter = SyslogClientConfig::parse(
            &entries(&[
                ("protocol", "tls"),
                ("addr", "localhost:6514"),
                ("tls_ca_file", missing),
            ]),
            SyslogDirection::Emit,
        )
        .expect("the emitter TLS shape is valid before reading its files");
        let error = emitter
            .tls_client_config()
            .expect_err("a missing emitter CA file must fail");
        assert!(matches!(
            error.current_context(),
            SyslogConfigError::TlsMaterial { .. }
        ));
        assert!(error.contains::<nervix_connector::ClientConfigError>());

        let ingestor = SyslogClientConfig::parse(
            &entries(&[
                ("protocol", "tls"),
                ("addr", "localhost:6514"),
                ("tls_cert_file", missing),
                ("tls_key_file", missing),
            ]),
            SyslogDirection::Ingest,
        )
        .expect("the ingestor TLS shape is valid before reading its files");
        let error = ingestor
            .tls_server_config()
            .expect_err("a missing server certificate must fail");
        assert!(matches!(
            error.current_context(),
            SyslogConfigError::TlsMaterial { .. }
        ));
        assert!(error.contains::<nervix_connector::ClientConfigError>());

        let root = tempfile::tempdir().expect("temporary Syslog TLS directory should open");
        let certificate = root.path().join("certificate.pem");
        std::fs::write(&certificate, b"certificate fixture")
            .expect("the Syslog certificate fixture should be writable");
        let certificate = certificate.to_string_lossy().into_owned();
        let ingestor = SyslogClientConfig::parse(
            &entries(&[
                ("protocol", "tls"),
                ("addr", "localhost:6514"),
                ("tls_cert_file", &certificate),
                ("tls_key_file", missing),
            ]),
            SyslogDirection::Ingest,
        )
        .expect("the ingestor TLS shape is valid before reading its files");
        let error = ingestor
            .tls_server_config()
            .expect_err("a missing server private key must fail");
        assert!(matches!(
            error.current_context(),
            SyslogConfigError::TlsMaterial { .. }
        ));
        assert!(error.contains::<nervix_connector::ClientConfigError>());
    }
}
