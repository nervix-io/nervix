//! MQTT client configuration.
//!
//! Layer: vocabulary.
//! - **Owns.** The typed values of an MQTT client configuration that satisfies the declared key
//!   schema, with the MQTT rules the schema cannot state: a broker named by host and port, the TLS
//!   files of an `mqtts` connection, an identity, username and password MQTT can carry, a keep-alive
//!   of whole seconds MQTT's Two Byte Integer holds, and a publish admission bound no larger than
//!   MQTT's largest packet.
//! - **Depends on.** The shared client configuration schema and deadline contract, and the URL
//!   parser.
//! - **Must not know.** The MQTT driver, sockets, reading TLS files, rendering templates or runtime
//!   scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{
    collections::BTreeMap,
    fmt,
    num::{NonZeroU16, NonZeroU32},
    path::{Path, PathBuf},
};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use thiserror::Error;
use url::{Host, Url};

use super::{
    ADDR, CLIENT_CONFIG, CLIENT_ID, KEEP_ALIVE, MAX_PACKET_SIZE, MQTT_MAXIMUM_PACKET_BYTES,
    PASSWORD, USERNAME,
};
use crate::{CLIENT_CONFIG_REDACTION, ClientConfigEntry, ClientConfigKey, ClientDeadline};

/// The most bytes an MQTT UTF-8 string or binary data carries: MQTT writes its length as a Two Byte
/// Integer.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "MQTT 5 prefixes a UTF-8 Encoded String and Binary Data with a Two Byte Integer \
                  length"
    )
)]
const MQTT_STRING_MAX_BYTES: usize = 65_535;

/// A refusal names its key without printing a configured value. The schema's own refusal stays
/// beneath `Schema`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(outcome, reason = "pure MQTT client configuration validation")
)]
pub enum MqttClientConfigError {
    #[error("the configuration does not satisfy the client's declared keys")]
    Schema,
    #[error("'client_id' must not be empty")]
    EmptyClientId,
    #[error("'{key}' must be at most {maximum} bytes", maximum = MQTT_STRING_MAX_BYTES)]
    TooLong { key: &'static str },
    #[error("'{key}' must not contain the NUL character")]
    NulCharacter { key: &'static str },
    #[error(
        "'keep_alive_ms' must be whole seconds from 1 s through {maximum} s",
        maximum = u16::MAX
    )]
    KeepAlive,
    #[error("'max_packet_size' must be at most {maximum}")]
    PacketSizeAboveMaximum { maximum: u64 },
}

/// The broker an MQTT client connects to: the host and the port its `addr` names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MqttBroker {
    host: String,
    port: NonZeroU16,
}

impl MqttBroker {
    /// The host to resolve and dial, a domain name or an address literal, written without the
    /// brackets a URL puts around an IPv6 literal.
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> NonZeroU16 {
        self.port
    }
}

/// How an MQTT client's connection reaches its broker: plain TCP for `mqtt`, TLS for `mqtts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttTransport {
    Tcp,
    Tls(MqttTlsFiles),
}

impl MqttTransport {
    pub const fn uses_tls(&self) -> bool {
        match self {
            Self::Tcp => false,
            Self::Tls(_) => true,
        }
    }
}

/// The files an `mqtts` connection reads: the certificate authority its broker's certificate must
/// chain to, which has no other trust root, and the client's own certificate and key, where the
/// configuration presents one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttTlsFiles {
    certificate_authority: PathBuf,
    identity: Option<MqttTlsIdentity>,
}

impl MqttTlsFiles {
    pub fn certificate_authority(&self) -> &Path {
        &self.certificate_authority
    }

    pub fn identity(&self) -> Option<&MqttTlsIdentity> {
        self.identity.as_ref()
    }
}

/// The certificate and private key an `mqtts` client presents to its broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttTlsIdentity {
    certificate: PathBuf,
    private_key: PathBuf,
}

impl MqttTlsIdentity {
    pub fn certificate(&self) -> &Path {
        &self.certificate
    }

    pub fn private_key(&self) -> &Path {
        &self.private_key
    }
}

/// The password an MQTT client authenticates with. A debug rendering never shows its value.
#[derive(Clone, PartialEq, Eq)]
pub struct MqttPassword(String);

impl MqttPassword {
    /// The password, as the client writes it into its `CONNECT`.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for MqttPassword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("MqttPassword")
            .field(&CLIENT_CONFIG_REDACTION)
            .finish()
    }
}

/// The validated values of one MQTT client configuration, after the host rendered its templates.
///
/// `client_id` is a template: its `{{instance}}` placeholder renders the index of the ingestor
/// instance that connects, and an emitter connects as instance 0. Absent, each connection uses the
/// domain-qualified default identity of the entity it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttClientConfig {
    broker: MqttBroker,
    transport: MqttTransport,
    client_id: Option<String>,
    username: Option<String>,
    password: Option<MqttPassword>,
    keep_alive_seconds: NonZeroU16,
    max_packet_size: NonZeroU32,
}

impl MqttClientConfig {
    /// Holds `config` to the declared schema, then converts the effective values once.
    pub fn parse(config: &[ClientConfigEntry]) -> error_stack::Result<Self, MqttClientConfigError> {
        CLIENT_CONFIG
            .validate_rendered(config)
            .change_context(MqttClientConfigError::Schema)?;
        let effective = CLIENT_CONFIG.effective_entries(config);
        // The schema refused repeated keys above, so each key maps to its one value.
        let mut values = BTreeMap::new();
        for entry in &effective {
            values.insert(entry.key.as_str(), entry.value.as_str());
        }
        let address = values.get(ADDR.name).verified("the schema requires addr");
        let address = Url::parse(address).verified("the schema parsed addr as a URL");
        let broker = Self::broker_of(&address);
        let transport = Self::transport_of(&address, &values);
        let client_id = match values.get(CLIENT_ID.name) {
            Some(client_id) => Some(Self::identity_of(client_id)?),
            None => None,
        };
        let username = match values.get(USERNAME.name) {
            Some(username) => Some(Self::mqtt_string(USERNAME, username)?),
            None => None,
        };
        let password = match values.get(PASSWORD.name) {
            Some(password) => Some(Self::password_of(password)?),
            None => None,
        };
        let keep_alive = values
            .get(KEEP_ALIVE.name)
            .verified("effective entries fill the defaulted keep_alive_ms");
        let max_packet_size = values
            .get(MAX_PACKET_SIZE.name)
            .verified("effective entries fill the defaulted max_packet_size");
        Ok(Self {
            broker,
            transport,
            client_id,
            username,
            password,
            keep_alive_seconds: Self::keep_alive_of(keep_alive)?,
            max_packet_size: Self::admission_bound_of(max_packet_size)?,
        })
    }

    /// The host and port of `address`, which the schema admitted as naming only a host and a
    /// nonzero port.
    fn broker_of(address: &Url) -> MqttBroker {
        let host = address
            .host()
            .verified("the schema admits an addr only with a host");
        let host = match host {
            Host::Domain(domain) => domain.to_string(),
            Host::Ipv4(address) => address.to_string(),
            Host::Ipv6(address) => address.to_string(),
        };
        let port = address
            .port()
            .verified("the schema admits an addr only with a port");
        let port =
            NonZeroU16::new(port).verified("the schema admits an addr only with a nonzero port");
        MqttBroker { host, port }
    }

    /// The transport `address` asks for, with the files the schema requires of an `mqtts` address
    /// and refuses with an `mqtt` one.
    fn transport_of(address: &Url, values: &BTreeMap<&str, &str>) -> MqttTransport {
        if address.scheme() != "mqtts" {
            return MqttTransport::Tcp;
        }
        let certificate_authority = values
            .get(ClientConfigKey::TLS_CA.name)
            .verified("the schema requires tls_ca_file exactly while addr uses mqtts");
        let identity = match values.get(ClientConfigKey::TLS_CERT.name) {
            Some(certificate) => {
                let private_key = values
                    .get(ClientConfigKey::TLS_KEY.name)
                    .verified("the schema requires tls_key_file with tls_cert_file");
                Some(MqttTlsIdentity {
                    certificate: PathBuf::from(certificate),
                    private_key: PathBuf::from(private_key),
                })
            }
            // The schema refuses tls_key_file without tls_cert_file.
            None => None,
        };
        MqttTransport::Tls(MqttTlsFiles {
            certificate_authority: PathBuf::from(certificate_authority),
            identity,
        })
    }

    /// An identity a broker can hold a session for: a nonempty MQTT string.
    fn identity_of(value: &str) -> error_stack::Result<String, MqttClientConfigError> {
        if value.is_empty() {
            return Err(Report::new(MqttClientConfigError::EmptyClientId));
        }
        Self::mqtt_string(CLIENT_ID, value)
    }

    /// A value MQTT carries as a UTF-8 string, which holds at most 65,535 bytes and no NUL.
    fn mqtt_string(
        key: ClientConfigKey,
        value: &str,
    ) -> error_stack::Result<String, MqttClientConfigError> {
        if value.len() > MQTT_STRING_MAX_BYTES {
            return Err(Report::new(MqttClientConfigError::TooLong {
                key: key.name,
            }));
        }
        if value.contains('\0') {
            return Err(Report::new(MqttClientConfigError::NulCharacter {
                key: key.name,
            }));
        }
        Ok(value.to_string())
    }

    /// A password, which MQTT carries as binary data of at most 65,535 bytes.
    fn password_of(value: &str) -> error_stack::Result<MqttPassword, MqttClientConfigError> {
        if value.len() > MQTT_STRING_MAX_BYTES {
            return Err(Report::new(MqttClientConfigError::TooLong {
                key: PASSWORD.name,
            }));
        }
        Ok(MqttPassword(value.to_string()))
    }

    /// The keep-alive `value` sets, which MQTT carries in whole seconds as a Two Byte Integer. A
    /// keep-alive of zero would turn the mechanism off, which the schema's positive milliseconds
    /// already refuse.
    fn keep_alive_of(value: &str) -> error_stack::Result<NonZeroU16, MqttClientConfigError> {
        let interval = ClientDeadline::parse(value)
            .verified("the schema admitted keep_alive_ms as schedulable milliseconds")
            .duration();
        if interval.subsec_nanos() != 0 {
            return Err(Report::new(MqttClientConfigError::KeepAlive));
        }
        let seconds =
            u16::try_from(interval.as_secs()).change_context(MqttClientConfigError::KeepAlive)?;
        let seconds = NonZeroU16::new(seconds)
            .verified("a positive whole number of seconds is at least one second");
        Ok(seconds)
    }

    /// The admission bound `value` sets, at most the largest packet MQTT can frame.
    fn admission_bound_of(value: &str) -> error_stack::Result<NonZeroU32, MqttClientConfigError> {
        let bytes = value
            .parse::<u64>()
            .verified("the schema admitted max_packet_size as a positive integer");
        if bytes > MQTT_MAXIMUM_PACKET_BYTES {
            return Err(Report::new(MqttClientConfigError::PacketSizeAboveMaximum {
                maximum: MQTT_MAXIMUM_PACKET_BYTES,
            }));
        }
        let bytes = u32::try_from(bytes)
            .verified("MQTT's largest packet fits its Four Byte Integer, checked beside it");
        let bytes =
            NonZeroU32::new(bytes).verified("the schema admitted max_packet_size as positive");
        Ok(bytes)
    }

    pub fn broker(&self) -> &MqttBroker {
        &self.broker
    }

    pub fn transport(&self) -> &MqttTransport {
        &self.transport
    }

    /// The identity the configuration renders for this connection, where it declares one.
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub fn password(&self) -> Option<&MqttPassword> {
        self.password.as_ref()
    }

    /// The keep-alive, in seconds, every `CONNECT` asks the broker for. A broker that announces a
    /// Server Keep Alive in its `CONNACK` replaces it for that connection.
    pub fn keep_alive_seconds(&self) -> NonZeroU16 {
        self.keep_alive_seconds
    }

    /// The largest complete publish, in bytes, an ingestor admits, its topic, properties and MQTT
    /// framing included.
    pub fn max_packet_size(&self) -> NonZeroU32 {
        self.max_packet_size
    }
}
