//! The one Pulsar client against a scripted broker: what it authenticates with, how much of a
//! broker's certificate it confirms, and which retries it leaves to the host.
//!
//! Layer: test harness.
//! - **Owns.** Client entries, a loopback broker that answers the commands a test scripts, and the
//!   count of attempts it saw.
//! - **Depends on.** The production client connection and the driver's wire codec.
//! - **Must not know.** Ingestors, emitters, topics or a real broker.

use std::{future::Future, time::Duration};

use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::TlsClientConfigError;
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    task,
    time::Instant,
};
use nervix_recovery::Discarded as _;
use pulsar::message::{
    Codec as PulsarCodec, Message as PulsarFrame,
    proto::{
        BaseCommand, CommandConnect, CommandConnected, CommandError,
        CommandPartitionedTopicMetadataResponse, base_command::Type as CommandType,
        command_partitioned_topic_metadata_response::LookupType as PartitionLookupType,
    },
};
use tokio_util::codec::Framed;

use super::*;

const TOPIC: &str = "persistent://public/default/events";

fn entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

/// The method and data `config` connects with.
fn credential(config: &[ClientConfigEntry]) -> Option<(String, Vec<u8>)> {
    let authentication =
        PulsarClientConnection::new(config, None, &Default::default()).authentication()?;
    Some((authentication.name, authentication.data))
}

fn trust(config: &[ClientConfigEntry]) -> ServerCertificateTrust {
    PulsarClientConnection::new(config, None, &Default::default())
        .server_certificate_trust()
        .assured("each fixture holds readable booleans")
}

/// A loopback broker that hands every connection it accepts to `serve`, counting them.
struct ScriptedBroker {
    service_url: String,
    connections: Arc<AtomicUsize>,
}

impl ScriptedBroker {
    async fn start<Serve, Serving>(serve: Serve) -> Self
    where
        Serve: Fn(Framed<TcpStream, PulsarCodec>) -> Serving + Send + 'static,
        Serving: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .assured("a loopback port is available");
        let address = listener
            .local_addr()
            .assured("a bound listener has an address");
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        task::spawn(async move {
            loop {
                task::consume_budget().await;
                let Ok((stream, _peer)) = listener.accept().await else {
                    return;
                };
                accepted.fetch_add(1, Ordering::Relaxed);
                task::spawn(serve(Framed::new(stream, PulsarCodec)));
            }
        });
        Self {
            service_url: format!("pulsar://{address}"),
            connections,
        }
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::Relaxed)
    }
}

fn answer(command: BaseCommand) -> PulsarFrame {
    PulsarFrame {
        command,
        payload: None,
    }
}

fn connected() -> PulsarFrame {
    answer(BaseCommand {
        r#type: i32::from(CommandType::Connected),
        connected: Some(CommandConnected {
            server_version: "scripted".to_string(),
            protocol_version: Some(12),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn refused(error: PulsarServerError, reason: &str) -> PulsarFrame {
    answer(BaseCommand {
        r#type: i32::from(CommandType::Error),
        error: Some(CommandError {
            request_id: u64::MAX,
            error: i32::from(error),
            message: reason.to_string(),
        }),
        ..Default::default()
    })
}

/// The `CONNECT` a client opened its connection with.
async fn read_connect(frames: &mut Framed<TcpStream, PulsarCodec>) -> Option<CommandConnect> {
    frames.next().await?.ok()?.command.connect
}

#[test]
fn a_token_authenticates_under_the_token_method() {
    assert_eq!(
        credential(&[entry("auth_token", "issued-token")]),
        Some(("token".to_string(), b"issued-token".to_vec()))
    );
}

#[test]
fn a_certificate_alone_authenticates_under_the_tls_method_with_no_data() {
    assert_eq!(
        credential(&[
            entry("tls_cert_file", "client.pem"),
            entry("tls_key_file", "client-key.pem"),
        ]),
        Some(("tls".to_string(), Vec::new()))
    );
}

/// The certificate stays in the TLS session for a broker that requires one, and the token names
/// the role.
#[test]
fn a_token_beside_a_certificate_names_the_role() {
    assert_eq!(
        credential(&[
            entry("auth_token", "issued-token"),
            entry("tls_cert_file", "client.pem"),
            entry("tls_key_file", "client-key.pem"),
        ]),
        Some(("token".to_string(), b"issued-token".to_vec()))
    );
}

#[test]
fn a_client_with_neither_presents_no_credential() {
    assert_eq!(credential(&[entry("tls_ca_file", "ca.pem")]), None);
}

#[test]
fn the_tls_keys_choose_how_much_of_a_brokers_certificate_is_confirmed() {
    assert_eq!(trust(&[]), ServerCertificateTrust::NamedAndAnchored);
    assert_eq!(
        trust(&[
            entry("tls_allow_insecure_connection", "false"),
            entry("tls_hostname_verification_enabled", "true"),
        ]),
        ServerCertificateTrust::NamedAndAnchored
    );
    assert_eq!(
        trust(&[entry("tls_hostname_verification_enabled", "false")]),
        ServerCertificateTrust::Anchored
    );
    assert_eq!(
        trust(&[entry("tls_allow_insecure_connection", "true")]),
        ServerCertificateTrust::Unconfirmed
    );
    // Accepting any certificate already accepts one for any name.
    assert_eq!(
        trust(&[
            entry("tls_allow_insecure_connection", "true"),
            entry("tls_hostname_verification_enabled", "true"),
        ]),
        ServerCertificateTrust::Unconfirmed
    );
}

#[nervix_primitives::test]
async fn a_tls_key_that_is_no_boolean_is_the_clients_configuration_failure() {
    let config = [
        entry("addr", "pulsar+ssl://127.0.0.1:1"),
        entry("tls_allow_insecure_connection", "sometimes"),
    ];
    let Err(failure) = PulsarClientConnection::new(&config, None, &Default::default())
        .open()
        .await
    else {
        panic!("a client whose key cannot be read must not connect");
    };
    assert!(matches!(
        failure.current_context(),
        PulsarClientError::Configuration
    ));
}

/// The typed cause stays beneath the failure, so a report names the file that could not be read.
#[nervix_primitives::test]
async fn a_certificate_file_that_cannot_be_read_is_the_clients_configuration_failure() {
    let config = [
        entry("addr", "pulsar+ssl://127.0.0.1:1"),
        entry("tls_cert_file", "/nonexistent/client.pem"),
        entry("tls_key_file", "/nonexistent/client-key.pem"),
    ];
    let Err(failure) = PulsarClientConnection::new(&config, None, &Default::default())
        .open()
        .await
    else {
        panic!("a client whose identity cannot be read must not connect");
    };
    assert!(matches!(
        failure.current_context(),
        PulsarClientError::Configuration
    ));
    assert!(matches!(
        failure.downcast_ref::<TlsClientConfigError>(),
        Some(TlsClientConfigError::ReadClientIdentity)
    ));
}

#[test]
fn both_directions_leave_every_retry_to_the_host() {
    let policy = Default::default();
    let without_timeout = PulsarClientConnection::new(&[], None, &policy);
    let with_timeout = PulsarClientConnection::new(&[], Some(Duration::from_secs(7)), &policy);
    assert_eq!(PulsarClientConnection::connection_retries().max_retries, 0);
    for connection in [&without_timeout, &with_timeout] {
        let operations =
            PulsarClientConnection::operation_retry_options(connection.operation_timeout);
        assert_eq!(operations.max_retries, Some(0));
        assert!(!operations.allow_retry(0));
    }
    assert_eq!(
        PulsarClientConnection::operation_retry_options(without_timeout.operation_timeout)
            .operation_timeout,
        OperationRetryOptions::default().operation_timeout
    );
    assert_eq!(
        PulsarClientConnection::operation_retry_options(with_timeout.operation_timeout)
            .operation_timeout,
        Duration::from_secs(7)
    );
}

/// The refusal reaches the caller as the broker answered it, after the one attempt the client
/// makes, and the token it refused went out under the token method.
#[nervix_primitives::test]
async fn a_broker_that_refuses_the_connection_is_reported_with_its_answer() {
    let (presented_tx, mut presented_rx) = mpsc::unbounded_channel();
    let broker = ScriptedBroker::start(move |mut frames| {
        let presented_tx = presented_tx.clone();
        async move {
            presented_tx
                .send(read_connect(&mut frames).await)
                .discarded("the test that reads this frame is still waiting for it");
            frames
                .send(refused(
                    PulsarServerError::AuthenticationError,
                    "Failed to authenticate",
                ))
                .await
                .discarded("a client that left stops reading the refusal");
        }
    })
    .await;
    let config = [
        entry("addr", &broker.service_url),
        entry("auth_token", "refused-token"),
    ];

    let Err(failure) = PulsarClientConnection::new(&config, None, &Default::default())
        .open()
        .await
    else {
        panic!("the broker refused the connection");
    };

    match failure.current_context() {
        PulsarClientError::Refused { refusal } => {
            assert_eq!(refusal.error, Some(PulsarServerError::AuthenticationError));
            assert_eq!(refusal.reason.as_deref(), Some("Failed to authenticate"));
        }
        other => panic!("expected the broker's refusal, got {other:?}"),
    }
    assert_eq!(
        failure.current_context().to_string(),
        "the PULSAR broker refused the connection with AuthenticationError: Failed to authenticate"
    );
    let connect = presented_rx
        .recv()
        .await
        .assured("the broker read the connection before it refused it")
        .assured("the client opened with a CONNECT frame");
    assert_eq!(connect.auth_method_name.as_deref(), Some("token"));
    assert_eq!(connect.auth_data.as_deref(), Some(&b"refused-token"[..]));
    assert_eq!(broker.connections(), 1);
}

/// A refused connection is one the driver would retry twelve times, sleeping about 41 seconds in
/// all between them. The paused clock counts those sleeps exactly, and one attempt can spend at
/// most its own 10-second connection timeout.
#[nervix_primitives::test(start_paused = true)]
async fn a_connection_the_address_refuses_is_attempted_once() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("a loopback port is available");
    let address = listener
        .local_addr()
        .assured("a bound listener has an address");
    drop(listener);
    let config = [entry("addr", &format!("pulsar://{address}"))];
    let started = Instant::now();

    let Err(failure) = PulsarClientConnection::new(&config, None, &Default::default())
        .open()
        .await
    else {
        panic!("nothing listens at the address");
    };

    assert!(started.elapsed() <= ConnectionRetryOptions::default().connection_timeout);
    assert!(matches!(
        failure.current_context(),
        PulsarClientError::NotOpened
    ));
    assert!(failure.downcast_ref::<PulsarError>().is_some());
}

/// `ServiceNotReady` is the answer the driver would otherwise ask again for, every five seconds and
/// without limit. This broker answers only the first lookup so, and every later one, so a client
/// that asked again would get its answer instead of the failure.
#[nervix_primitives::test]
async fn an_operation_the_broker_is_not_ready_for_is_attempted_once() {
    let lookups = Arc::new(AtomicUsize::new(0));
    let seen = lookups.clone();
    let broker = ScriptedBroker::start(move |mut frames| {
        let seen = seen.clone();
        async move {
            let _accepted = read_connect(&mut frames).await;
            frames
                .send(connected())
                .await
                .discarded("a client that left stops reading the answer");
            while let Some(Ok(frame)) = frames.next().await {
                task::consume_budget().await;
                let Some(lookup) = frame.command.partition_metadata else {
                    continue;
                };
                let earlier = seen.fetch_add(1, Ordering::Relaxed);
                let response = match earlier {
                    0 => CommandPartitionedTopicMetadataResponse {
                        request_id: lookup.request_id,
                        response: Some(i32::from(PartitionLookupType::Failed)),
                        error: Some(i32::from(PulsarServerError::ServiceNotReady)),
                        message: Some("the namespace is loading".to_string()),
                        ..Default::default()
                    },
                    _ => CommandPartitionedTopicMetadataResponse {
                        request_id: lookup.request_id,
                        response: Some(i32::from(PartitionLookupType::Success)),
                        partitions: Some(0),
                        ..Default::default()
                    },
                };
                let answered = answer(BaseCommand {
                    r#type: i32::from(CommandType::PartitionedMetadataResponse),
                    partition_metadata_response: Some(response),
                    ..Default::default()
                });
                frames
                    .send(answered)
                    .await
                    .discarded("a client that left stops reading the answer");
            }
        }
    })
    .await;
    let config = [entry("addr", &broker.service_url)];

    let client = PulsarClientConnection::new(&config, None, &Default::default())
        .open()
        .await
        .assured("the broker accepts this client");
    let lookup = client.lookup_partitioned_topic_number(TOPIC).await;

    assert!(lookup.is_err(), "the broker is not ready: {lookup:?}");
    assert_eq!(lookups.load(Ordering::Relaxed), 1);
}
