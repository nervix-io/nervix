//! What each side of a SYSLOG client reads from its configuration.
//!
//! Layer: vocabulary.
//! - **Owns.** The transport and framing a client selects, the address each side uses, the largest
//!   message a listener admits and the TLS files each side names, read once from rendered entries,
//!   and the rules that join a key to its side and its transport beyond what the key declarations
//!   state.
//! - **Depends on.** The client key declaration and the client address.
//! - **Must not know.** Name resolution, sockets, how TLS files are loaded or how frames are written.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{
    collections::BTreeMap, net::SocketAddr, num::NonZeroUsize, path::PathBuf, str::FromStr as _,
};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use strum::VariantNames as _;

use super::CLIENT_CONFIG;
use crate::{ClientConfigEntry, ClientConfigSchemaError, ClientConfigSide, HostPort};

pub(super) const ADDRESS_KEY: &str = "addr";
pub(super) const CONNECT_TIMEOUT_KEY: &str = "connect_timeout_ms";
pub(super) const FRAMING_KEY: &str = "framing";
pub(super) const MAX_MESSAGE_SIZE_KEY: &str = "max_message_size";
pub(super) const PROTOCOL_KEY: &str = "protocol";
pub(super) const TLS_CERT_KEY: &str = "tls_cert_file";
pub(super) const TLS_CLIENT_CA_KEY: &str = "tls_client_ca_file";
pub(super) const TLS_KEY_KEY: &str = "tls_key_file";

/// The protocols a TLS file or a TLS listener's handshake deadline means something over.
pub(super) const TLS_PROTOCOL: &[&str] = &["tls"];
/// The protocols that carry a byte stream, which alone have framing and idle connections.
pub(super) const STREAM_PROTOCOLS: &[&str] = &["tcp", "tls"];

/// What a listener's `addr` must be, so that binding it asks no resolver.
const LITERAL_ADDRESS: &str = "literal IP address and port";
/// The framing RFC 5425 fixes for TLS.
const TLS_FRAMING: &str = "octet-counting over tls";

/// The transport a client's `protocol` key selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::VariantNames)]
#[strum(serialize_all = "lowercase")]
pub(super) enum SyslogProtocol {
    Udp,
    Tcp,
    Tls,
}

/// The choices `protocol` declares.
pub(super) const SYSLOG_PROTOCOLS: &[&str] = SyslogProtocol::VARIANTS;

impl SyslogProtocol {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "a protocol outside the declared choices is the declaration's refusal, which \
                      validation reports with the key"
        )
    )]
    fn of_choice(value: &str) -> Result<Self, strum::ParseError> {
        Self::from_str(value)
    }
}

/// How a stream carries its frames, which `framing` selects: RFC 6587 octet counting, or
/// non-transparent framing, in which a trailer byte ends each frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::VariantNames)]
#[strum(serialize_all = "kebab-case")]
pub enum SyslogFraming {
    OctetCounting,
    NonTransparent,
}

/// The choices `framing` declares.
pub(super) const SYSLOG_FRAMINGS: &[&str] = SyslogFraming::VARIANTS;

impl SyslogFraming {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "a framing outside the declared choices is the declaration's refusal, which \
                      validation reports with the key"
        )
    )]
    fn of_choice(value: &str) -> Result<Self, strum::ParseError> {
        Self::from_str(value)
    }
}

/// The TLS files a listener's client names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyslogListenerTls {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    /// The authority a client certificate must chain to. Without it the listener requests none.
    pub client_ca_file: Option<PathBuf>,
}

/// The transport a listener serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyslogListenerTransport {
    Udp,
    /// `framing` holds every connection to the framing it names. Without it, each frame's first
    /// byte states its own framing.
    Tcp {
        framing: Option<SyslogFraming>,
    },
    /// RFC 5425 fixes octet counting.
    Tls(SyslogListenerTls),
}

impl SyslogListenerTransport {
    /// The framing every frame of a stream keeps to. Without one, each frame's first byte states
    /// its own framing. A datagram has no framing to keep.
    pub const fn stream_framing(&self) -> Option<SyslogFraming> {
        match self {
            Self::Udp => None,
            Self::Tcp { framing } => *framing,
            Self::Tls(_) => Some(SyslogFraming::OctetCounting),
        }
    }
}

/// What a SYSLOG listener reads from its client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyslogListenerClient {
    /// The socket address every node binds.
    pub bind: SocketAddr,
    pub max_message_size: NonZeroUsize,
    pub transport: SyslogListenerTransport,
}

/// The transport a collector writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyslogCollectorTransport {
    Udp,
    Tcp {
        framing: SyslogFraming,
    },
    /// RFC 5425 fixes octet counting.
    Tls,
}

/// What a SYSLOG collector reads from its client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyslogCollectorClient {
    /// Where the collector sends, which the node's resolver resolves when it is a name.
    pub target: HostPort,
    pub transport: SyslogCollectorTransport,
}

/// A client's entries by key.
struct SyslogEntries<'a> {
    values: BTreeMap<&'a str, &'a str>,
}

impl<'a> SyslogEntries<'a> {
    fn of(config: &'a [ClientConfigEntry]) -> Self {
        let values = config
            .iter()
            .map(|entry| (entry.key.as_str(), entry.value.as_str()))
            .collect();
        Self { values }
    }

    fn has(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    /// The value of `key`, unless it still holds a template. A value that holds one is judged once
    /// it is rendered.
    fn rendered(&self, key: &str) -> Option<&'a str> {
        let value = *self.values.get(key)?;
        if value.contains("{{") {
            return None;
        }
        Some(value)
    }

    /// The transport the client selects, when `protocol` is rendered and one of its choices. The
    /// declaration owns the refusal of any other value.
    fn protocol(&self) -> Option<SyslogProtocol> {
        let protocol = self.rendered(PROTOCOL_KEY)?;
        match SyslogProtocol::of_choice(protocol) {
            Ok(protocol) => Some(protocol),
            Err(_) => None,
        }
    }

    /// The framing the client selects, when `framing` is rendered and one of its choices.
    fn framing(&self) -> Option<SyslogFraming> {
        let framing = self.rendered(FRAMING_KEY)?;
        match SyslogFraming::of_choice(framing) {
            Ok(framing) => Some(framing),
            Err(_) => None,
        }
    }

    /// RFC 5425 leaves TLS one framing, whichever side reads the client.
    fn validate_framing(&self) -> Result<(), Report<ClientConfigSchemaError>> {
        let refused = self.protocol() == Some(SyslogProtocol::Tls)
            && self.framing() == Some(SyslogFraming::NonTransparent);
        if refused {
            return Err(Report::new(ClientConfigSchemaError::InvalidValue {
                key: FRAMING_KEY.to_string(),
                expected: TLS_FRAMING,
            }));
        }
        Ok(())
    }

    /// What a listener demands beyond the keys its side reads: an address it binds without a
    /// resolver, the identity a TLS listener presents, and a handshake for `connect_timeout_ms`
    /// to bound, since a listener opens no connection of its own.
    fn validate_listener(&self) -> Result<(), Report<ClientConfigSchemaError>> {
        self.validate_framing()?;
        if let Some(address) = self.rendered(ADDRESS_KEY) {
            Self::literal_address(address)?;
        }
        match self.protocol() {
            Some(SyslogProtocol::Tls) => {
                for key in [TLS_CERT_KEY, TLS_KEY_KEY] {
                    if !self.has(key) {
                        return Err(Report::new(ClientConfigSchemaError::MissingRequired {
                            key,
                        }));
                    }
                }
            }
            Some(SyslogProtocol::Udp | SyslogProtocol::Tcp) => {
                if self.has(CONNECT_TIMEOUT_KEY) {
                    return Err(Report::new(ClientConfigSchemaError::RequiresChoice {
                        key: CONNECT_TIMEOUT_KEY,
                        selector: PROTOCOL_KEY,
                        choices: TLS_PROTOCOL,
                    }));
                }
            }
            None => {}
        }
        Ok(())
    }

    fn literal_address(address: &str) -> Result<SocketAddr, Report<ClientConfigSchemaError>> {
        let refusal = || ClientConfigSchemaError::InvalidValue {
            key: ADDRESS_KEY.to_string(),
            expected: LITERAL_ADDRESS,
        };
        let address = HostPort::parse(address).change_context_lazy(refusal)?;
        match address.socket_addr() {
            Some(address) => Ok(address),
            None => Err(Report::new(refusal())),
        }
    }

    /// The value of a key the declaration requires of rendered entries.
    fn required(&self, key: &'static str) -> Result<&'a str, Report<ClientConfigSchemaError>> {
        match self.values.get(key) {
            Some(value) => Ok(*value),
            None => Err(Report::new(ClientConfigSchemaError::MissingRequired {
                key,
            })),
        }
    }

    fn file(&self, key: &str) -> Option<PathBuf> {
        let value = self.values.get(key)?;
        Some(PathBuf::from(value))
    }

    /// The largest message a listener admits: the key, or the default its declaration states.
    fn max_message_size(&self) -> Result<NonZeroUsize, Report<ClientConfigSchemaError>> {
        let size = self.required(MAX_MESSAGE_SIZE_KEY)?;
        size.parse::<NonZeroUsize>()
            .change_context_lazy(|| ClientConfigSchemaError::InvalidValue {
                key: MAX_MESSAGE_SIZE_KEY.to_string(),
                expected: "positive integer that fits this node's address space",
            })
    }
}

impl SyslogListenerClient {
    /// Reads a rendered client for a listener, refusing whatever a listener cannot use.
    pub fn read(config: &[ClientConfigEntry]) -> Result<Self, Report<ClientConfigSchemaError>> {
        CLIENT_CONFIG.validate_rendered(config)?;
        CLIENT_CONFIG.validate_side(config, ClientConfigSide::Ingestor)?;
        let effective = CLIENT_CONFIG.effective_entries(config);
        let entries = SyslogEntries::of(&effective);
        entries.validate_listener()?;
        let protocol = entries
            .protocol()
            .verified("validate_rendered held the required protocol to its declared choices");
        let transport = match protocol {
            SyslogProtocol::Udp => SyslogListenerTransport::Udp,
            SyslogProtocol::Tcp => SyslogListenerTransport::Tcp {
                framing: entries.framing(),
            },
            SyslogProtocol::Tls => SyslogListenerTransport::Tls(SyslogListenerTls {
                cert_file: entries
                    .file(TLS_CERT_KEY)
                    .verified("validate_listener requires a TLS listener's certificate"),
                key_file: entries
                    .file(TLS_KEY_KEY)
                    .verified("validate_listener requires a TLS listener's key"),
                client_ca_file: entries.file(TLS_CLIENT_CA_KEY),
            }),
        };
        Ok(Self {
            bind: SyslogEntries::literal_address(entries.required(ADDRESS_KEY)?)?,
            max_message_size: entries.max_message_size()?,
            transport,
        })
    }
}

impl SyslogCollectorClient {
    /// Reads a rendered client for a collector, refusing whatever a collector cannot use.
    pub fn read(config: &[ClientConfigEntry]) -> Result<Self, Report<ClientConfigSchemaError>> {
        CLIENT_CONFIG.validate_rendered(config)?;
        CLIENT_CONFIG.validate_side(config, ClientConfigSide::Emitter)?;
        let entries = SyslogEntries::of(config);
        entries.validate_framing()?;
        let protocol = entries
            .protocol()
            .verified("validate_rendered held the required protocol to its declared choices");
        let transport = match protocol {
            SyslogProtocol::Udp => SyslogCollectorTransport::Udp,
            SyslogProtocol::Tcp => {
                let framing = entries.framing();
                SyslogCollectorTransport::Tcp {
                    framing: framing.unwrap_or(SyslogFraming::OctetCounting),
                }
            }
            SyslogProtocol::Tls => SyslogCollectorTransport::Tls,
        };
        let target = HostPort::parse(entries.required(ADDRESS_KEY)?).change_context_lazy(|| {
            ClientConfigSchemaError::InvalidValue {
                key: ADDRESS_KEY.to_string(),
                expected: "host:port",
            }
        })?;
        Ok(Self { target, transport })
    }
}

/// Refuses what no side of a SYSLOG client can use, whichever entity binds it.
pub fn validate_syslog_client_config(
    config: &[ClientConfigEntry],
) -> Result<(), Report<ClientConfigSchemaError>> {
    SyslogEntries::of(config).validate_framing()
}

/// Refuses what a listener cannot use beyond the keys its side reads. A value that holds a template
/// is judged when the listener starts.
pub(super) fn validate_listener_client_config(
    config: &[ClientConfigEntry],
) -> Result<(), Report<ClientConfigSchemaError>> {
    SyslogEntries::of(config).validate_listener()
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
