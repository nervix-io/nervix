//! MQTT source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The MQTT client and event loop each source instance subscribes through, its
//!   session and quality-of-service settings, the shared subscription every instance joins, the
//!   client identity each instance connects with, manual acknowledgement of publishes, and the
//!   local replay of publishes the host rejected.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   Tokio, `rumqttc` and `url`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use std::{
    collections::VecDeque,
    num::{NonZeroU64, NonZeroUsize},
};

use arch_into::ArchInto as _;
use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, NoIngestHeaders, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceResult, SourceResume,
    client_config_value, client_tls_paths, optional_client_config_value, read_tls_file,
};
use nervix_models::{ClientConfigEntry, MqttQos, MqttSession};
use rumqttc::{
    AckMode, AsyncClient, BrokerSessionResumePolicy, Event, EventLoop, Incoming, MqttOptions,
    Publish, QoS, SessionMode, SubscribeReasonCode, TlsConfiguration, Transport as MqttTransport,
};
use thiserror::Error;
use tokio::time::{Instant, sleep_until};
use url::{Host, Url};

const MQTT: &str = "mqtt";
/// The template a multi-instance client identity must contain, so that every instance connects
/// under its own identity.
const MQTT_INSTANCE_PLACEHOLDER: &str = "{{instance}}";

/// Why an MQTT source could not build its client, subscribe, receive, or acknowledge.
#[derive(Debug, Error)]
pub enum MqttSourceError {
    #[error("invalid MQTT client configuration")]
    ClientConfig,
    #[error("MQTT TLS requires client config key 'tls_ca_file'")]
    MissingTlsCa,
    #[error("MQTT TLS client authentication requires both 'tls_cert_file' and 'tls_key_file'")]
    IncompleteTlsIdentity,
    #[error("failed to build MQTT client")]
    BuildClient,
    #[error("invalid MQTT service address")]
    InvalidAddress,
    #[error("unsupported MQTT service address scheme '{scheme}'")]
    UnsupportedScheme { scheme: String },
    #[error("MQTT service address has no host")]
    MissingHost,
    #[error("MQTT service address has no port")]
    MissingPort,
    #[error("instance {instance} has no MQTT client configuration")]
    MissingInstance { instance: u64 },
    #[error("failed to subscribe MQTT source")]
    Subscribe,
    #[error("the MQTT broker refused the subscription")]
    SubscriptionRefused,
    #[error("failed to receive an MQTT message")]
    Receive,
    #[error("the MQTT connection is closed")]
    Disconnected,
    #[error("failed to acknowledge an MQTT message")]
    Acknowledge,
    #[error("MQTT batch timeout exceeds the monotonic clock range")]
    BatchDeadline,
}

type MqttSourceResult<T> = Result<T, Report<MqttSourceError>>;

/// Why the declared client identity cannot serve every instance of a source.
///
/// A source with this conflict stays declared but never subscribes: each instance reports it as
/// the reason it cannot resume until the declaration changes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MqttClientIdError {
    #[error(
        "MQTT client_id is required for {instances} instances and must contain '{{{{instance}}}}'"
    )]
    MissingTemplate { instances: NonZeroU64 },
    #[error(
        "MQTT client_id '{client_id}' is shared by {instances} instances; use {{{{instance}}}} in \
         client_id for multi-instance MQTT ingestors"
    )]
    SharedIdentity {
        client_id: String,
        instances: NonZeroU64,
    },
}

#[derive(Debug, PartialEq, Eq)]
struct MqttSourceAddr {
    host: String,
    port: u16,
    tls: bool,
}

/// What one MQTT source subscribes with.
pub struct MqttSourceSettings {
    /// The client entries every instance connects with, rendered for that instance.
    pub instances: Vec<Vec<ClientConfigEntry>>,
    /// Why the declared client identity cannot serve every instance, if it cannot.
    pub client_id_conflict: Option<MqttClientIdError>,
    /// The client identity an instance connects with when its entries declare none.
    pub default_client_id: String,
    /// The shared subscription group every instance joins, so instances never duplicate messages.
    pub share_group: String,
    pub topic: String,
    pub session: MqttSession,
    pub qos: MqttQos,
    /// Whether the source acknowledges each publish itself once the host accepted it, rather
    /// than the client acknowledging on receipt.
    pub manual_acks: bool,
}

/// One MQTT source's subscription settings and the client entries of each of its instances.
#[derive(Clone)]
pub struct MqttSourcePlan {
    instances: Vec<Vec<ClientConfigEntry>>,
    default_client_id: String,
    subscribe_filter: String,
    session: MqttSession,
    qos: QoS,
    manual_acks: bool,
    client_id_conflict: Option<MqttClientIdError>,
}

/// One instance's client and the event loop that carries its session.
struct MqttConnection {
    client: AsyncClient,
    eventloop: EventLoop,
}

impl MqttSourcePlan {
    pub fn new(settings: MqttSourceSettings) -> Self {
        let MqttSourceSettings {
            instances,
            client_id_conflict,
            default_client_id,
            share_group,
            topic,
            session,
            qos,
            manual_acks,
        } = settings;
        Self {
            instances,
            default_client_id,
            subscribe_filter: format!("$share/{share_group}/{topic}"),
            session,
            qos: match qos {
                MqttQos::AtMostOnce => QoS::AtMostOnce,
                MqttQos::AtLeastOnce => QoS::AtLeastOnce,
            },
            manual_acks,
            client_id_conflict,
        }
    }

    /// Several instances must each connect under their own identity, so a declaration shared by
    /// `instances` has to carry the instance template; one instance may use any identity or the
    /// default.
    pub fn client_id_conflict(
        declared: &[ClientConfigEntry],
        instances: NonZeroU64,
    ) -> Option<MqttClientIdError> {
        if instances == NonZeroU64::MIN {
            return None;
        }
        let Some(client_id) = optional_client_config_value(declared, "client_id") else {
            return Some(MqttClientIdError::MissingTemplate { instances });
        };
        if client_id.contains(MQTT_INSTANCE_PLACEHOLDER) {
            return None;
        }
        Some(MqttClientIdError::SharedIdentity {
            client_id: client_id.to_string(),
            instances,
        })
    }

    fn client(&self, instance_index: u64) -> MqttSourceResult<MqttConnection> {
        let Some(instance) = self.instances.get(instance_index.arch_into()) else {
            return Err(Report::new(MqttSourceError::MissingInstance {
                instance: instance_index,
            }));
        };
        let (client, eventloop) = Self::client_from_config(
            instance,
            &self.default_client_id,
            self.session,
            self.manual_acks,
        )?;
        Ok(MqttConnection { client, eventloop })
    }

    fn client_from_config(
        config: &[ClientConfigEntry],
        default_client_id: &str,
        session: MqttSession,
        manual_acks: bool,
    ) -> MqttSourceResult<(AsyncClient, EventLoop)> {
        let addr = client_config_value(config, "addr", "MQTT")
            .change_context(MqttSourceError::ClientConfig)?;
        let client_id = match optional_client_config_value(config, "client_id") {
            Some(client_id) => client_id.to_owned(),
            None => default_client_id.to_string(),
        };

        let mqtt_addr = Self::parse_addr(&addr)?;
        let mut options = MqttOptions::new(client_id, (mqtt_addr.host, mqtt_addr.port));
        options.set_session_mode(match session {
            MqttSession::Clean => SessionMode::Clean,
            MqttSession::Persistent => SessionMode::Persistent,
        });
        if session == MqttSession::Persistent {
            // Nervix deliberately keeps in-flight messages and ACK state in memory. After an
            // owner change, accept the broker's retained session and let it redeliver pending
            // QoS messages instead of requiring a local rumqttc protocol checkpoint.
            options.set_broker_session_resume_policy(BrokerSessionResumePolicy::AllowBrokerOnly);
        }
        options.set_ack_mode(if manual_acks {
            AckMode::Manual
        } else {
            AckMode::Automatic
        });
        if mqtt_addr.tls {
            let tls = client_tls_paths(config);
            let ca = if let Some(ca_file) = tls.ca_file.as_ref() {
                read_tls_file(ca_file, "TLS CA certificate")
                    .change_context(MqttSourceError::ClientConfig)?
            } else {
                return Err(Report::new(MqttSourceError::MissingTlsCa));
            };
            let client_auth = match (&tls.cert_file, &tls.key_file) {
                (Some(cert_file), Some(key_file)) => Some((
                    read_tls_file(cert_file, "TLS certificate")
                        .change_context(MqttSourceError::ClientConfig)?,
                    read_tls_file(key_file, "TLS private key")
                        .change_context(MqttSourceError::ClientConfig)?,
                )),
                (None, None) => None,
                _ => {
                    return Err(Report::new(MqttSourceError::IncompleteTlsIdentity));
                }
            };
            options.set_transport(MqttTransport::Tls(TlsConfiguration::Simple {
                ca,
                alpn: None,
                client_auth,
            }));
        }
        AsyncClient::builder(options)
            .capacity(1024)
            .try_build()
            .map_err(|error| {
                Report::new(MqttSourceError::BuildClient).attach_printable(error.to_string())
            })
    }

    fn parse_addr(addr: &str) -> MqttSourceResult<MqttSourceAddr> {
        let url = Url::parse(addr).map_err(|source| {
            Report::new(MqttSourceError::InvalidAddress).attach_printable(source.to_string())
        })?;
        let tls = if url.scheme() == "mqtt" {
            false
        } else if url.scheme() == "mqtts" {
            true
        } else {
            return Err(Report::new(MqttSourceError::UnsupportedScheme {
                scheme: url.scheme().to_string(),
            }));
        };
        let host = match url.host() {
            Some(Host::Domain(domain)) => domain.to_string(),
            Some(Host::Ipv4(address)) => address.to_string(),
            Some(Host::Ipv6(address)) => address.to_string(),
            None => String::new(),
        };
        if host.is_empty() {
            return Err(Report::new(MqttSourceError::MissingHost));
        }
        let Some(port) = url.port() else {
            return Err(Report::new(MqttSourceError::MissingPort));
        };
        Ok(MqttSourceAddr { host, port, tls })
    }

    /// Drives the event loop until the broker confirms the subscription.
    ///
    /// A broker that resumes a persistent session already holds the subscription and cannot
    /// allocate a new SUBSCRIBE packet identifier, so its CONNACK is the readiness boundary and
    /// queued QoS messages follow immediately.
    async fn establish_subscription(
        &self,
        connection: &mut MqttConnection,
    ) -> MqttSourceResult<()> {
        loop {
            tokio::task::consume_budget().await;
            match connection.eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                    if connack.session_present {
                        return Ok(());
                    }
                    connection
                        .client
                        .subscribe(self.subscribe_filter.as_str(), self.qos)
                        .await
                        .map_err(|error| {
                            Report::new(MqttSourceError::Subscribe)
                                .attach_printable(error.to_string())
                        })?;
                }
                Ok(Event::Incoming(Incoming::SubAck(suback))) => {
                    let granted = suback.return_codes.iter().all(|code| {
                        let SubscribeReasonCode::Success(_) = code else {
                            return false;
                        };
                        true
                    });
                    if granted {
                        return Ok(());
                    }
                    return Err(Report::new(MqttSourceError::SubscriptionRefused)
                        .attach_printable(format!("{suback:?}")));
                }
                Ok(Event::Incoming(_) | Event::Outgoing(_) | Event::Auth(_)) => {}
                Err(error) => {
                    return Err(
                        Report::new(MqttSourceError::Subscribe).attach_printable(error.to_string())
                    );
                }
            }
        }
    }
}

/// The publish one acknowledgement settles, kept whole because a rejected publish is delivered
/// again from here and an acknowledged one is confirmed by its packet identifier.
#[derive(Debug, Clone)]
pub struct MqttSourcePosition {
    publish: Publish,
}

/// One received publish. MQTT carries no headers into the ingest namespace.
pub struct MqttSourceMessage {
    position: MqttSourcePosition,
    headers: NoIngestHeaders,
}

impl MqttSourceMessage {
    fn new(publish: Publish) -> Self {
        Self {
            position: MqttSourcePosition { publish },
            headers: NoIngestHeaders,
        }
    }
}

impl SourceMessage for MqttSourceMessage {
    type Position = MqttSourcePosition;

    fn payload(&self) -> &[u8] {
        &self.position.publish.payload
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.headers
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::Headers {
            headers: &self.headers,
        }
    }
}

/// One MQTT source instance: its connection, absent while suspended or reconnecting, and the
/// publishes the host rejected that it delivers again before polling the broker.
pub struct MqttSource {
    plan: MqttSourcePlan,
    instance_index: u64,
    connection: Option<MqttConnection>,
    replay: VecDeque<Publish>,
}

impl MqttSource {
    /// Drops the connection, and with it every rejected publish, whose packet identifiers belong
    /// to the session that ends here. A persistent session delivers them again on resume.
    fn disconnect(&mut self) {
        self.connection = None;
        self.replay.clear();
    }

    async fn next_publish(connection: &mut MqttConnection) -> MqttSourceResult<Publish> {
        loop {
            tokio::task::consume_budget().await;
            match connection.eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(publish))) => return Ok(publish),
                Ok(Event::Incoming(_) | Event::Outgoing(_) | Event::Auth(_)) => {}
                Err(error) => {
                    return Err(
                        Report::new(MqttSourceError::Receive).attach_printable(error.to_string())
                    );
                }
            }
        }
    }

    fn replayed_batch(&mut self, max_messages: NonZeroUsize) -> Option<Vec<MqttSourceMessage>> {
        let first = self.replay.pop_front()?;
        let mut messages = Vec::with_capacity(max_messages.get());
        messages.push(MqttSourceMessage::new(first));
        while messages.len() < max_messages.get() {
            let Some(publish) = self.replay.pop_front() else {
                break;
            };
            messages.push(MqttSourceMessage::new(publish));
        }
        Some(messages)
    }
}

#[async_trait]
impl SourceConnector for MqttSource {
    type Plan = MqttSourcePlan;

    async fn open(plan: &Self::Plan, instance_index: u64) -> SourceResult<Self> {
        Ok(Self {
            plan: plan.clone(),
            instance_index,
            connection: None,
            replay: VecDeque::new(),
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.connection.is_none()
    }

    /// Suspension disconnects while the broker retains a persistent session, so what arrives
    /// meanwhile waits in the broker's offline queue.
    async fn suspend(&mut self) -> SourceResult<()> {
        self.disconnect();
        Ok(())
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if let Some(conflict) = self.plan.client_id_conflict.as_ref() {
            return Err(Report::new(conflict.clone())
                .change_context(SourceError::Resume { connector: MQTT }));
        }
        if self.connection.is_none() {
            let mut connection = self
                .plan
                .client(self.instance_index)
                .change_context(SourceError::Resume { connector: MQTT })?;
            self.plan
                .establish_subscription(&mut connection)
                .await
                .change_context(SourceError::Resume { connector: MQTT })?;
            self.connection = Some(connection);
        }
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.disconnect();
        Ok(())
    }
}

#[async_trait]
impl BrokerSourceConnector for MqttSource {
    type Message = MqttSourceMessage;
    type Position = MqttSourcePosition;

    async fn next_batch(
        &mut self,
        request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        if let Some(messages) = self.replayed_batch(request.max_messages) {
            return Ok(SourceBatch::Messages(messages));
        }
        let Some(connection) = self.connection.as_mut() else {
            return Ok(SourceBatch::ResumeRequired);
        };
        let first = match Self::next_publish(connection).await {
            Ok(publish) => publish,
            Err(error) => {
                self.disconnect();
                return Err(error.change_context(SourceError::Read { connector: MQTT }));
            }
        };
        let mut messages = Vec::with_capacity(request.max_messages.get());
        messages.push(MqttSourceMessage::new(first));
        if request.max_messages == NonZeroUsize::MIN {
            return Ok(SourceBatch::Messages(messages));
        }
        let Some(batch_timeout) = request.batch_timeout else {
            return Ok(SourceBatch::Messages(messages));
        };
        let Some(deadline) = Instant::now().checked_add(batch_timeout) else {
            return Err(Report::new(MqttSourceError::BatchDeadline)
                .change_context(SourceError::Read { connector: MQTT }));
        };
        let mut failure = None;
        while messages.len() < request.max_messages.get() {
            tokio::task::consume_budget().await;
            tokio::select! {
                _ = sleep_until(deadline) => break,
                next = Self::next_publish(connection) => {
                    match next {
                        Ok(publish) => messages.push(MqttSourceMessage::new(publish)),
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    }
                }
            }
        }
        if let Some(error) = failure {
            self.disconnect();
            return Err(error.change_context(SourceError::Read { connector: MQTT }));
        }
        Ok(SourceBatch::Messages(messages))
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        if !self.plan.manual_acks {
            return Ok(());
        }
        let Some(connection) = self.connection.as_ref() else {
            return Err(Report::new(MqttSourceError::Disconnected)
                .change_context(SourceError::Acknowledge { connector: MQTT }));
        };
        for position in positions {
            tokio::task::consume_budget().await;
            connection
                .client
                .ack(&position.publish)
                .await
                .map_err(|error| {
                    Report::new(MqttSourceError::Acknowledge).attach_printable(error.to_string())
                })
                .change_context(SourceError::Acknowledge { connector: MQTT })?;
        }
        Ok(())
    }

    /// The broker delivers a QoS 1 publish again only when the session reconnects, so a rejected
    /// publish is delivered again from here, ahead of anything the broker sends next.
    async fn reject(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        for position in positions.iter().rev() {
            tokio::task::consume_budget().await;
            self.replay.push_front(position.publish.clone());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nonzero_ext::nonzero;

    use super::*;

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn config_with_client_id(client_id: &str) -> Vec<ClientConfigEntry> {
        vec![entry("client_id", client_id)]
    }

    fn build_client(config: &[ClientConfigEntry]) -> MqttSourceResult<(AsyncClient, EventLoop)> {
        MqttSourcePlan::client_from_config(config, "default-client", MqttSession::Clean, false)
    }

    fn build_client_error(
        config: &[ClientConfigEntry],
        expectation: &str,
    ) -> Report<MqttSourceError> {
        match build_client(config) {
            Ok(_) => panic!("{expectation}"),
            Err(error) => error,
        }
    }

    #[test]
    fn persistent_sessions_allow_broker_only_resume() {
        let config = [
            entry("addr", "mqtt://127.0.0.1:1883"),
            entry("client_id", "persistent-client"),
        ];
        let (_, eventloop) =
            MqttSourcePlan::client_from_config(&config, "fallback", MqttSession::Persistent, true)
                .expect("persistent MQTT client must be valid");

        assert_eq!(
            eventloop.options.broker_session_resume_policy(),
            BrokerSessionResumePolicy::AllowBrokerOnly
        );
    }

    #[test]
    fn multi_instance_client_id_requires_instance_template() {
        assert_eq!(
            MqttSourcePlan::client_id_conflict(
                &config_with_client_id("fixed-client"),
                nonzero!(2u64)
            ),
            Some(MqttClientIdError::SharedIdentity {
                client_id: "fixed-client".to_string(),
                instances: nonzero!(2u64),
            })
        );
        assert_eq!(
            MqttSourcePlan::client_id_conflict(&[], nonzero!(2u64)),
            Some(MqttClientIdError::MissingTemplate {
                instances: nonzero!(2u64),
            })
        );
        assert_eq!(
            MqttSourcePlan::client_id_conflict(
                &config_with_client_id("templated-{{instance}}"),
                nonzero!(2u64)
            ),
            None
        );
        assert_eq!(
            MqttSourcePlan::client_id_conflict(&[], nonzero!(1u64)),
            None
        );
    }

    #[test]
    fn a_shared_client_id_names_the_conflict_as_the_resume_failure() {
        let plan = MqttSourcePlan::new(MqttSourceSettings {
            instances: vec![Vec::new(), Vec::new()],
            client_id_conflict: MqttSourcePlan::client_id_conflict(
                &config_with_client_id("fixed-client"),
                nonzero!(2u64),
            ),
            default_client_id: "fallback".to_string(),
            share_group: "default~mqtt_notifications".to_string(),
            topic: "notifications".to_string(),
            session: MqttSession::Persistent,
            qos: MqttQos::AtLeastOnce,
            manual_acks: true,
        });
        assert_eq!(
            plan.subscribe_filter,
            "$share/default~mqtt_notifications/notifications"
        );
        assert_eq!(
            plan.client_id_conflict.as_ref().map(ToString::to_string),
            Some(
                "MQTT client_id 'fixed-client' is shared by 2 instances; use {{instance}} in \
                 client_id for multi-instance MQTT ingestors"
                    .to_string()
            )
        );
    }

    #[test]
    fn parse_addr_handles_valid_and_invalid_inputs() {
        assert_eq!(
            MqttSourcePlan::parse_addr("mqtt://user:pass@broker.example.com:1883/topic")
                .expect("must parse"),
            MqttSourceAddr {
                host: "broker.example.com".to_string(),
                port: 1883,
                tls: false,
            }
        );
        assert_eq!(
            MqttSourcePlan::parse_addr("mqtts://broker.example.com:8883").expect("must parse"),
            MqttSourceAddr {
                host: "broker.example.com".to_string(),
                port: 8883,
                tls: true,
            }
        );
        assert_eq!(
            MqttSourcePlan::parse_addr("mqtt://[2001:db8::1]:1883/topic").expect("must parse"),
            MqttSourceAddr {
                host: "2001:db8::1".to_string(),
                port: 1883,
                tls: false,
            }
        );
        assert_eq!(
            MqttSourcePlan::parse_addr("mqtt://broker.example.com:1883?keep_alive=30")
                .expect("must parse"),
            MqttSourceAddr {
                host: "broker.example.com".to_string(),
                port: 1883,
                tls: false,
            }
        );
        assert!(MqttSourcePlan::parse_addr("http://broker.example.com:1883").is_err());
        assert!(MqttSourcePlan::parse_addr("mqtt://broker.example.com").is_err());
        assert!(MqttSourcePlan::parse_addr("mqtt://:1883").is_err());
    }

    #[test]
    fn client_builder_uses_configured_or_default_client_id() {
        build_client(&[entry("addr", "mqtt://broker.example.com:1883")])
            .expect("must build client from default id");
        build_client(&[
            entry("addr", "mqtt://broker.example.com:1883"),
            entry("client_id", "explicit-client"),
        ])
        .expect("must build client from explicit id");
    }

    #[test]
    fn client_builder_requires_addr() {
        let error = build_client_error(&[], "missing mqtt addr");
        assert!(matches!(
            error.current_context(),
            MqttSourceError::ClientConfig
        ));
        assert!(
            matches!(
                error.downcast_ref::<nervix_connector::ClientConfigError>(),
                Some(nervix_connector::ClientConfigError::MissingRequired {
                    connector: "MQTT",
                    key,
                }) if key == "addr"
            ),
            "missing MQTT address should preserve its typed client-config cause"
        );
    }

    #[test]
    fn client_reports_typed_address_and_tls_configuration_errors() {
        let error = build_client_error(
            &[entry("addr", "not a URL")],
            "an invalid MQTT address must fail",
        );
        assert!(matches!(
            error.current_context(),
            MqttSourceError::InvalidAddress
        ));

        let error = build_client_error(
            &[entry("addr", "mqtts://localhost:8883")],
            "MQTTS requires an explicit CA file",
        );
        assert!(matches!(
            error.current_context(),
            MqttSourceError::MissingTlsCa
        ));

        let root = tempfile::tempdir().expect("temporary MQTT TLS directory should open");
        let ca = root.path().join("ca.pem");
        let cert = root.path().join("client.pem");
        std::fs::write(&ca, b"test CA").expect("the MQTT CA fixture should be writable");
        std::fs::write(&cert, b"test certificate")
            .expect("the MQTT certificate fixture should be writable");
        let error = build_client_error(
            &[
                entry("addr", "mqtts://localhost:8883"),
                entry("tls_ca_file", &ca.to_string_lossy()),
                entry("tls_cert_file", &cert.to_string_lossy()),
            ],
            "a client certificate without a key must fail",
        );
        assert!(matches!(
            error.current_context(),
            MqttSourceError::IncompleteTlsIdentity
        ));
    }

    #[tokio::test]
    async fn rejected_publishes_replay_in_order_before_the_broker_is_polled() {
        let plan = MqttSourcePlan::new(MqttSourceSettings {
            instances: vec![vec![entry("addr", "mqtt://127.0.0.1:1")]],
            client_id_conflict: None,
            default_client_id: "client".to_string(),
            share_group: "default~ingestor".to_string(),
            topic: "events".to_string(),
            session: MqttSession::Persistent,
            qos: MqttQos::AtLeastOnce,
            manual_acks: true,
        });
        let mut source = MqttSource::open(&plan, 0)
            .await
            .expect("an MQTT source opens without connecting");
        let first = MqttSourcePosition {
            publish: Publish::new("events", QoS::AtLeastOnce, b"1".to_vec(), None),
        };
        let second = MqttSourcePosition {
            publish: Publish::new("events", QoS::AtLeastOnce, b"2".to_vec(), None),
        };
        source
            .reject(&[first, second])
            .await
            .expect("rejecting queues the replay");

        let batch = source
            .next_batch(SourceBatchRequest {
                max_messages: nonzero!(2usize),
                batch_timeout: None,
            })
            .await
            .expect("the replay is delivered without a connection");
        let SourceBatch::Messages(messages) = batch else {
            panic!("the replay must be delivered as messages");
        };
        assert_eq!(
            messages
                .iter()
                .map(|message| message.payload().to_vec())
                .collect::<Vec<_>>(),
            vec![b"1".to_vec(), b"2".to_vec()]
        );
        assert!(source.replay.is_empty());
    }
}
