//! Qualifies what each side of a SYSLOG client reads.
//!
//! Layer: test harness.
//! - **Owns.** The typed read of each side, the refusals that join a key to its side and its
//!   transport, and the round trip of every valid client through its entries.
//! - **Depends on.** The SYSLOG client declaration and readers.
//! - **Must not know.** Sockets, TLS loading or frames.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies the typed reads of a SYSLOG client")
)]

use meticulous::ResultExt as _;
use rstest::rstest;

use super::*;
use crate::{ClientConfigReaders, CreateClientSyslog};

fn entries(values: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
    values
        .iter()
        .map(|(key, value)| ClientConfigEntry {
            key: (*key).to_string(),
            value: (*value).to_string(),
        })
        .collect()
}

fn refusal<T: std::fmt::Debug>(
    read: Result<T, Report<ClientConfigSchemaError>>,
) -> ClientConfigSchemaError {
    read.expect_err("the client is refused")
        .current_context()
        .clone()
}

#[test]
fn a_listener_reads_its_bind_address_and_the_declared_default_size() {
    let client =
        SyslogListenerClient::read(&entries(&[("protocol", "udp"), ("addr", "127.0.0.1:5514")]))
            .expect("a listener reads a literal address");
    assert_eq!(
        client,
        SyslogListenerClient {
            bind: SocketAddr::from(([127, 0, 0, 1], 5514)),
            max_message_size: NonZeroUsize::new(131_072).assured("the default is positive"),
            transport: SyslogListenerTransport::Udp,
        }
    );
}

#[rstest]
#[case(None, None)]
#[case(Some("octet-counting"), Some(SyslogFraming::OctetCounting))]
#[case(Some("non-transparent"), Some(SyslogFraming::NonTransparent))]
fn a_tcp_listener_reads_the_framing_its_client_declares(
    #[case] written: Option<&str>,
    #[case] framing: Option<SyslogFraming>,
) {
    let mut values = vec![("protocol", "tcp"), ("addr", "[::1]:5514")];
    if let Some(written) = written {
        values.push(("framing", written));
    }
    let client = SyslogListenerClient::read(&entries(&values)).expect("the client is valid");
    assert_eq!(client.transport, SyslogListenerTransport::Tcp { framing });
    assert_eq!(
        client.bind,
        SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 5514))
    );
}

#[test]
fn a_tls_listener_reads_its_identity_and_the_authority_that_verifies_clients() {
    let client = SyslogListenerClient::read(&entries(&[
        ("protocol", "tls"),
        ("addr", "0.0.0.0:6514"),
        ("framing", "octet-counting"),
        ("tls_cert_file", "/mnt/server.crt"),
        ("tls_key_file", "/mnt/server.key"),
        ("tls_client_ca_file", "/mnt/clients-ca.crt"),
        ("connect_timeout_ms", "1000"),
        ("idle_timeout_ms", "1000"),
    ]))
    .expect("the client is valid");
    assert_eq!(
        client.transport,
        SyslogListenerTransport::Tls(SyslogListenerTls {
            cert_file: PathBuf::from("/mnt/server.crt"),
            key_file: PathBuf::from("/mnt/server.key"),
            client_ca_file: Some(PathBuf::from("/mnt/clients-ca.crt")),
        })
    );
}

#[rstest]
#[case("udp", None, SyslogCollectorTransport::Udp)]
#[case("tcp", None, SyslogCollectorTransport::Tcp { framing: SyslogFraming::OctetCounting })]
#[case(
    "tcp",
    Some("non-transparent"),
    SyslogCollectorTransport::Tcp { framing: SyslogFraming::NonTransparent }
)]
#[case("tls", Some("octet-counting"), SyslogCollectorTransport::Tls)]
fn a_collector_reads_its_target_and_transport(
    #[case] protocol: &str,
    #[case] framing: Option<&str>,
    #[case] transport: SyslogCollectorTransport,
) {
    let mut values = vec![("protocol", protocol), ("addr", "syslog.example.test:5514")];
    if let Some(framing) = framing {
        values.push(("framing", framing));
    }
    let client = SyslogCollectorClient::read(&entries(&values)).expect("the client is valid");
    assert_eq!(client.transport, transport);
    assert_eq!(client.target.host(), "syslog.example.test");
    assert_eq!(client.target.port(), 5514);
}

#[rstest]
#[case(&[("protocol", "tcp"), ("timeout_ms", "1000")], "timeout_ms")]
#[case(
    &[
        ("protocol", "tls"),
        ("tls_cert_file", "/mnt/server.crt"),
        ("tls_key_file", "/mnt/server.key"),
        ("tls_ca_file", "/mnt/ca.crt"),
    ],
    "tls_ca_file"
)]
fn a_listener_refuses_a_key_only_collectors_read(
    #[case] values: &[(&str, &str)],
    #[case] key: &'static str,
) {
    let mut config = entries(&[("addr", "127.0.0.1:6514")]);
    config.extend(entries(values));
    assert_eq!(
        refusal(SyslogListenerClient::read(&config)),
        ClientConfigSchemaError::ReadByOtherSide {
            key,
            readers: ClientConfigReaders::Emitters,
        }
    );
}

#[rstest]
#[case(("protocol", "tcp"), "max_message_size", "512")]
#[case(("protocol", "tcp"), "idle_timeout_ms", "1000")]
#[case(("protocol", "tls"), "tls_client_ca_file", "/mnt/ca.crt")]
fn a_collector_refuses_a_key_only_listeners_read(
    #[case] protocol: (&str, &str),
    #[case] key: &'static str,
    #[case] value: &str,
) {
    let config = entries(&[protocol, ("addr", "127.0.0.1:6514"), (key, value)]);
    assert_eq!(
        refusal(SyslogCollectorClient::read(&config)),
        ClientConfigSchemaError::ReadByOtherSide {
            key,
            readers: ClientConfigReaders::Ingestors,
        }
    );
}

#[rstest]
#[case("syslog.example.test:5514")]
#[case("localhost:5514")]
#[case("missing-port")]
fn a_listener_binds_only_a_literal_address(#[case] address: &str) {
    let refused =
        validate_listener_client_config(&entries(&[("protocol", "udp"), ("addr", address)]));
    assert_eq!(
        refusal(refused),
        ClientConfigSchemaError::InvalidValue {
            key: "addr".to_string(),
            expected: LITERAL_ADDRESS,
        }
    );
}

#[rstest]
#[case("udp")]
#[case("tcp")]
fn a_listener_without_a_handshake_refuses_connect_timeout_ms(#[case] protocol: &str) {
    let refused = validate_listener_client_config(&entries(&[
        ("protocol", protocol),
        ("addr", "127.0.0.1:5514"),
        ("connect_timeout_ms", "1000"),
    ]));
    assert_eq!(
        refusal(refused),
        ClientConfigSchemaError::RequiresChoice {
            key: "connect_timeout_ms",
            selector: "protocol",
            choices: &["tls"],
        }
    );
}

#[rstest]
#[case(&[("tls_key_file", "/mnt/server.key")], "tls_cert_file")]
#[case(&[("tls_cert_file", "/mnt/server.crt")], "tls_key_file")]
#[case(&[], "tls_cert_file")]
fn a_tls_listener_requires_its_identity(
    #[case] identity: &[(&str, &str)],
    #[case] missing: &'static str,
) {
    let mut values = vec![("protocol", "tls"), ("addr", "127.0.0.1:6514")];
    values.extend_from_slice(identity);
    let refused = validate_listener_client_config(&entries(&values));
    assert_eq!(
        refusal(refused),
        ClientConfigSchemaError::MissingRequired { key: missing }
    );
}

/// The side and transport of one client.
#[derive(Debug, Clone, Copy)]
struct Client {
    side: ClientConfigSide,
    protocol: &'static str,
}

const fn client(side: ClientConfigSide, protocol: &'static str) -> Client {
    Client { side, protocol }
}

const UDP_COLLECTOR: Client = client(ClientConfigSide::Emitter, "udp");
const TCP_COLLECTOR: Client = client(ClientConfigSide::Emitter, "tcp");
const TLS_COLLECTOR: Client = client(ClientConfigSide::Emitter, "tls");
const UDP_LISTENER: Client = client(ClientConfigSide::Ingestor, "udp");
const TCP_LISTENER: Client = client(ClientConfigSide::Ingestor, "tcp");
const TLS_LISTENER: Client = client(ClientConfigSide::Ingestor, "tls");

/// Reads a client of `client`'s side that sets the deadline `key`, with the TLS identity a TLS
/// listener requires.
fn read_deadline_client(client: Client, key: &str) -> Result<(), Report<ClientConfigSchemaError>> {
    let mut values = vec![
        ("protocol", client.protocol),
        ("addr", "127.0.0.1:6514"),
        (key, "1000"),
    ];
    if client.protocol == "tls" {
        values.push(("tls_cert_file", "/mnt/server.crt"));
        values.push(("tls_key_file", "/mnt/server.key"));
    }
    let config = entries(&values);
    match client.side {
        ClientConfigSide::Ingestor => SyslogListenerClient::read(&config).map(|_| ()),
        ClientConfigSide::Emitter => SyslogCollectorClient::read(&config).map(|_| ()),
    }
}

/// `connect_timeout_ms` bounds a collector's connection and a TLS listener's handshake,
/// `timeout_ms` a collector's frame writes, and `idle_timeout_ms` a TCP or TLS listener's
/// connections. Every other client refuses the key, naming it and the clients it applies to.
#[rstest]
#[case(
    "connect_timeout_ms",
    &[UDP_COLLECTOR, TCP_COLLECTOR, TLS_COLLECTOR, TLS_LISTENER],
    &[
        (UDP_LISTENER, "client config key 'connect_timeout_ms' requires 'protocol' to be tls"),
        (TCP_LISTENER, "client config key 'connect_timeout_ms' requires 'protocol' to be tls"),
    ],
)]
#[case(
    "timeout_ms",
    &[UDP_COLLECTOR, TCP_COLLECTOR, TLS_COLLECTOR],
    &[
        (UDP_LISTENER, "client config key 'timeout_ms' is read only by emitters"),
        (TCP_LISTENER, "client config key 'timeout_ms' is read only by emitters"),
        (TLS_LISTENER, "client config key 'timeout_ms' is read only by emitters"),
    ],
)]
#[case(
    "idle_timeout_ms",
    &[TCP_LISTENER, TLS_LISTENER],
    &[
        (UDP_LISTENER, "client config key 'idle_timeout_ms' requires 'protocol' to be tcp or tls"),
        (UDP_COLLECTOR, "client config key 'idle_timeout_ms' requires 'protocol' to be tcp or tls"),
        (TCP_COLLECTOR, "client config key 'idle_timeout_ms' is read only by ingestors"),
        (TLS_COLLECTOR, "client config key 'idle_timeout_ms' is read only by ingestors"),
    ],
)]
fn a_deadline_key_applies_only_to_a_client_that_performs_its_wait(
    #[case] key: &str,
    #[case] performers: &[Client],
    #[case] bystanders: &[(Client, &str)],
) {
    for performer in performers {
        if let Err(error) = read_deadline_client(*performer, key) {
            panic!("{key} bounds a wait of {performer:?}: {error:#}");
        }
    }
    for (bystander, refusal) in bystanders {
        let error = read_deadline_client(*bystander, key)
            .expect_err("the client performs no wait the key bounds");
        assert_eq!(
            error.current_context().to_string(),
            *refusal,
            "{bystander:?}"
        );
    }
}

#[test]
fn tls_keeps_octet_counting_on_every_side() {
    let config = entries(&[
        ("protocol", "tls"),
        ("addr", "127.0.0.1:6514"),
        ("framing", "non-transparent"),
    ]);
    let refused = ClientConfigSchemaError::InvalidValue {
        key: "framing".to_string(),
        expected: TLS_FRAMING,
    };
    assert_eq!(refusal(validate_syslog_client_config(&config)), refused);
    assert_eq!(refusal(SyslogCollectorClient::read(&config)), refused);
}

#[test]
fn a_template_waits_for_rendering_before_it_is_judged() {
    let declared = entries(&[
        ("protocol", "{{ transport }}"),
        ("addr", "{{ bind }}"),
        ("connect_timeout_ms", "1000"),
    ]);
    validate_listener_client_config(&declared).expect("nothing rendered can be judged yet");
    validate_syslog_client_config(&declared).expect("nothing rendered can be judged yet");
}

#[test]
fn the_declaration_spells_every_transport_and_framing_this_module_reads() {
    let schema = CreateClientSyslog::<u64>::DECLARATION.config;
    for protocol in SYSLOG_PROTOCOLS {
        schema
            .key(PROTOCOL_KEY)
            .assured("the client declares its protocol")
            .validate(protocol)
            .expect("every transport is a declared choice");
        assert!(SyslogProtocol::from_str(protocol).is_ok(), "{protocol}");
    }
    for framing in SYSLOG_FRAMINGS {
        schema
            .key(FRAMING_KEY)
            .assured("the client declares its framing")
            .validate(framing)
            .expect("every framing is a declared choice");
        assert!(SyslogFraming::from_str(framing).is_ok(), "{framing}");
    }
}

/// The entries that declare a valid listener client, in the keys and spellings a statement writes.
fn listener_entries(client: &SyslogListenerClient) -> Vec<ClientConfigEntry> {
    let bind = client.bind.to_string();
    let size = client.max_message_size.to_string();
    let mut values: Vec<(&str, String)> = vec![("addr", bind), ("max_message_size", size)];
    match &client.transport {
        SyslogListenerTransport::Udp => values.push(("protocol", "udp".to_string())),
        SyslogListenerTransport::Tcp { framing } => {
            values.push(("protocol", "tcp".to_string()));
            if let Some(framing) = framing {
                values.push(("framing", framing_spelling(*framing)));
            }
        }
        SyslogListenerTransport::Tls(tls) => {
            values.push(("protocol", "tls".to_string()));
            values.push(("tls_cert_file", tls.cert_file.display().to_string()));
            values.push(("tls_key_file", tls.key_file.display().to_string()));
            if let Some(authority) = &tls.client_ca_file {
                values.push(("tls_client_ca_file", authority.display().to_string()));
            }
        }
    }
    values
        .into_iter()
        .map(|(key, value)| ClientConfigEntry {
            key: key.to_string(),
            value,
        })
        .collect()
}

/// The entries that declare a valid collector client.
fn collector_entries(client: &SyslogCollectorClient) -> Vec<ClientConfigEntry> {
    let address = format!(
        "collector-{}.example.test:{}",
        client.target.port(),
        client.target.port()
    );
    let mut values = vec![("addr", address)];
    match client.transport {
        SyslogCollectorTransport::Udp => values.push(("protocol", "udp".to_string())),
        SyslogCollectorTransport::Tcp { framing } => {
            values.push(("protocol", "tcp".to_string()));
            values.push(("framing", framing_spelling(framing)));
        }
        SyslogCollectorTransport::Tls => values.push(("protocol", "tls".to_string())),
    }
    values
        .into_iter()
        .map(|(key, value)| ClientConfigEntry {
            key: key.to_string(),
            value,
        })
        .collect()
}

fn framing_spelling(framing: SyslogFraming) -> String {
    match framing {
        SyslogFraming::OctetCounting => SYSLOG_FRAMINGS[0].to_string(),
        SyslogFraming::NonTransparent => SYSLOG_FRAMINGS[1].to_string(),
    }
}

/// Picks one framing, or none, from a generated word.
fn drawn_framing(word: u16) -> Option<SyslogFraming> {
    match word % 3 {
        0 => None,
        1 => Some(SyslogFraming::OctetCounting),
        _ => Some(SyslogFraming::NonTransparent),
    }
}

fn drawn_listener(input: [u16; 5]) -> SyslogListenerClient {
    let transport = match input[0] % 3 {
        0 => SyslogListenerTransport::Udp,
        1 => SyslogListenerTransport::Tcp {
            framing: drawn_framing(input[1]),
        },
        _ => SyslogListenerTransport::Tls(SyslogListenerTls {
            cert_file: PathBuf::from(format!("/mnt/{}.crt", input[1])),
            key_file: PathBuf::from(format!("/mnt/{}.key", input[2])),
            client_ca_file: match input[3] % 2 {
                0 => None,
                _ => Some(PathBuf::from(format!("/mnt/{}-ca.crt", input[3]))),
            },
        }),
    };
    let ip = [0, 0, 0, 0, 0, 0, input[2], input[3]];
    let bind = match input[1] % 2 {
        0 => SocketAddr::from((ip, input[4])),
        _ => SocketAddr::from((
            [127, 0, 0, u8::try_from(input[2] % 256).assured("below 256")],
            input[4],
        )),
    };
    SyslogListenerClient {
        bind,
        max_message_size: NonZeroUsize::MIN
            .checked_add(usize::from(input[3]))
            .assured("a generated word and one fit every supported target"),
        transport,
    }
}

fn drawn_collector(input: [u16; 5]) -> SyslogCollectorClient {
    let transport = match input[0] % 3 {
        0 => SyslogCollectorTransport::Udp,
        1 => SyslogCollectorTransport::Tcp {
            framing: drawn_framing(input[1]).unwrap_or(SyslogFraming::OctetCounting),
        },
        _ => SyslogCollectorTransport::Tls,
    };
    let address = format!("collector-{0}.example.test:{0}", input[4]);
    SyslogCollectorClient {
        target: HostPort::parse(&address).assured("a generated port and its host are an address"),
        transport,
    }
}

/// Every valid client of either side reads back complete through the entries that declare it, and
/// a key only the other side reads refuses the client by name.
#[test]
fn bolero_each_side_reads_back_every_valid_client_and_refuses_the_other_sides_keys() {
    bolero::check!().with_type::<[u16; 5]>().for_each(|input| {
        let listener = drawn_listener(*input);
        let mut config = listener_entries(&listener);
        assert_eq!(
            SyslogListenerClient::read(&config).assured("a drawn listener client is valid"),
            listener
        );
        config.push(ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "1000".to_string(),
        });
        assert_eq!(
            refusal(SyslogListenerClient::read(&config)),
            ClientConfigSchemaError::ReadByOtherSide {
                key: "timeout_ms",
                readers: ClientConfigReaders::Emitters,
            }
        );

        let collector = drawn_collector(*input);
        let mut config = collector_entries(&collector);
        assert_eq!(
            SyslogCollectorClient::read(&config).assured("a drawn collector client is valid"),
            collector
        );
        config.push(ClientConfigEntry {
            key: "max_message_size".to_string(),
            value: "512".to_string(),
        });
        assert_eq!(
            refusal(SyslogCollectorClient::read(&config)),
            ClientConfigSchemaError::ReadByOtherSide {
                key: "max_message_size",
                readers: ClientConfigReaders::Ingestors,
            }
        );
    });
}
