//! MQTT source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The MQTT client and event loop each source instance subscribes through, its
//!   session and quality-of-service settings, the shared subscription every instance joins, the
//!   client identity each instance connects with, the batches a read gathers and how a read ends
//!   when the host cancels it, manual acknowledgement of publishes, and the local replay of
//!   publishes the host rejected, and retirement of obsolete broker sessions.
//! - **Depends on.** The connector contract, the crate's broker connection through the node DNS
//!   resolver, typed client configuration entries, `error-stack`, Tokio, `rumqttc` and `url`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or Plexus.

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

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use arch_into::ArchInto as _;
use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, NoIngestHeaders, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceReadCancellation,
    SourceResult, SourceResume, client_config_value, client_tls_paths,
    optional_client_config_value, read_tls_file,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, ConnectorKind, MqttQos, MqttSession};
use nervix_primitives::time::{Instant, timeout, timeout_at};
use rumqttc::{
    AckMode, AsyncClient, BrokerSessionResumePolicy, DisconnectProperties, DisconnectReasonCode,
    Event, EventLoop, Incoming, MqttOptions, Outgoing, Publish, QoS, SessionMode,
    SubscribeReasonCode, TlsConfiguration, Transport as MqttTransport, UnsubAckReason,
};
use thiserror::Error;
use url::{Host, Url};

use crate::connection::{MqttConnectionError, MqttDialer};

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
    #[error("failed to connect MQTT source to its broker")]
    Connect,
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
    #[error("failed to unsubscribe the retired {connector} input", connector = ConnectorKind::Mqtt)]
    Unsubscribe,
    #[error("the {connector} broker refused to unsubscribe the retired input: {reasons:?}", connector = ConnectorKind::Mqtt)]
    UnsubscribeRefused { reasons: Vec<UnsubAckReason> },
    #[error("failed to disconnect the retired {connector} session", connector = ConnectorKind::Mqtt)]
    Disconnect,
    #[error("{connector} session cleanup exceeded its connection budget of {budget:?}", connector = ConnectorKind::Mqtt)]
    RetirementDeadline { budget: Duration },
    #[error("failed to retire {connector} source instance {instance}", connector = ConnectorKind::Mqtt)]
    RetireInstance { instance: u64 },
}

type MqttSourceResult<T> = Result<T, Report<MqttSourceError>>;

/// Why the declared client identity cannot serve every instance of a source.
///
/// A source with this conflict stays declared but never subscribes: each instance reports it as
/// the reason it cannot resume until the declaration changes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MqttClientIdError {
    #[error(
        "MQTT client_id '{client_id}' is shared by {instances} instances; use {{{{instance}}}} in \
         client_id for multi-instance MQTT ingestors"
    )]
    SharedIdentity {
        client_id: String,
        instances: NonZeroU64,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MqttSourceAddr {
    host: String,
    port: u16,
    tls: bool,
}

/// A broker's session namespace and the client identity within it.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MqttSessionIdentity {
    broker: MqttSourceAddr,
    client_id: String,
}

/// The validated broker identities of a source plan, with one representative per broker session.
pub struct MqttSourceSessions<'a> {
    plan: &'a MqttSourcePlan,
    identities: BTreeMap<MqttSessionIdentity, u64>,
}

impl MqttSourceSessions<'_> {
    /// The sessions this plan uses that the other plan does not keep with the same subscription.
    /// The instance indices are unique and ordered, including when a conflicting declaration
    /// repeats an explicit client identity.
    pub fn retiring_instances(&self, replacement: Option<&Self>) -> Vec<u64> {
        let retained = match replacement {
            Some(replacement)
                if self.plan.subscribe_filter == replacement.plan.subscribe_filter
                    && self.plan.session == replacement.plan.session
                    && self.plan.qos == replacement.plan.qos =>
            {
                Some(&replacement.identities)
            }
            Some(_) | None => None,
        };
        let mut instances = BTreeSet::new();
        for (identity, instance) in &self.identities {
            if let Some(retained) = retained
                && retained.contains_key(identity)
            {
                continue;
            }
            instances.insert(*instance);
        }
        instances.into_iter().collect()
    }
}

/// What one MQTT source subscribes with.
pub struct MqttSourceSettings {
    /// The client entries every instance connects with, rendered for that instance.
    pub instances: Vec<Vec<ClientConfigEntry>>,
    /// Why the declared client identity cannot serve every instance, if it cannot.
    pub client_id_conflict: Option<MqttClientIdError>,
    /// The default identity for one instance; multiple instances append their instance index.
    pub default_client_id: String,
    /// The shared subscription group every instance joins, so instances never duplicate messages.
    pub share_group: String,
    pub topic: String,
    pub session: MqttSession,
    pub qos: MqttQos,
    /// Whether the source acknowledges each publish itself once the host accepted it, rather
    /// than the client acknowledging on receipt.
    pub manual_acks: bool,
    /// The node resolver every instance resolves its broker host through when it connects.
    pub dns: DnsResolver,
}

/// One MQTT source's subscription settings, the client entries of each of its instances, and the
/// node resolver they connect through.
#[derive(Clone)]
pub struct MqttSourcePlan {
    instances: Vec<Vec<ClientConfigEntry>>,
    default_client_id: String,
    subscribe_filter: String,
    session: MqttSession,
    qos: QoS,
    manual_acks: bool,
    client_id_conflict: Option<MqttClientIdError>,
    dns: DnsResolver,
}

/// One instance's client and the event loop that carries its session.
struct MqttConnection {
    client: AsyncClient,
    eventloop: EventLoop,
}

/// The publishes a source instance reads from its broker connection: the event loop of its
/// client, and a fake event loop in the checks of how a read meets the host's cancellation.
///
/// A read stops waiting for the next publish when the host cancels it or its batch timeout
/// passes. The connection keeps every publish it already read for its next poll: the client
/// queues each packet it reads before it writes a reply, and its next poll returns a queued
/// publish first.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "each steady poll of a source's read takes its publishes through its connection"
    )
)]
trait PublishRead: Send {
    /// Polls the connection until it yields its next publish.
    fn next_publish(&mut self) -> impl Future<Output = MqttSourceResult<Publish>> + Send;
}

impl PublishRead for MqttConnection {
    fn next_publish(&mut self) -> impl Future<Output = MqttSourceResult<Publish>> + Send {
        async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                match self.eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => return Ok(publish),
                    Ok(Event::Incoming(_) | Event::Outgoing(_) | Event::Auth(_)) => {}
                    Err(error) => {
                        return Err(MqttConnectionError::report(error)
                            .change_context(MqttSourceError::Receive));
                    }
                }
            }
        }
    }
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
            dns,
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
            dns,
        }
    }

    /// Several instances need distinct identities. An explicit shared identity contains the
    /// instance template; an absent identity uses the instance-qualified default.
    pub fn client_id_conflict(
        declared: &[ClientConfigEntry],
        instances: NonZeroU64,
    ) -> Option<MqttClientIdError> {
        if instances == NonZeroU64::MIN {
            return None;
        }
        let client_id = optional_client_config_value(declared, "client_id")?;
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
        let default_client_id = self.default_instance_client_id(instance_index);
        let (client, eventloop) = Self::client_from_config(
            instance,
            &default_client_id,
            self.session,
            self.manual_acks,
            self.dns.clone(),
        )?;
        Ok(MqttConnection { client, eventloop })
    }

    fn default_instance_client_id(&self, instance_index: u64) -> String {
        if self.instances.len() == 1 {
            self.default_client_id.clone()
        } else {
            format!("{}~{instance_index}", self.default_client_id)
        }
    }

    fn session_identity(&self, instance_index: u64) -> MqttSourceResult<MqttSessionIdentity> {
        let Some(instance) = self.instances.get(instance_index.arch_into()) else {
            return Err(Report::new(MqttSourceError::MissingInstance {
                instance: instance_index,
            }));
        };
        let address = client_config_value(instance, "addr", ConnectorKind::Mqtt.into())
            .change_context(MqttSourceError::ClientConfig)?;
        let client_id = match optional_client_config_value(instance, "client_id") {
            Some(client_id) => client_id.to_owned(),
            None => self.default_instance_client_id(instance_index),
        };
        Ok(MqttSessionIdentity {
            broker: Self::parse_addr(&address)?,
            client_id,
        })
    }

    /// Validates the external identity of every configured instance once before comparing inputs.
    pub fn sessions(&self) -> error_stack::Result<MqttSourceSessions<'_>, MqttSourceError> {
        let mut identities = BTreeMap::new();
        for (instance_index, _) in (0_u64..).zip(&self.instances) {
            let identity = self.session_identity(instance_index)?;
            identities.entry(identity).or_insert(instance_index);
        }
        Ok(MqttSourceSessions {
            plan: self,
            identities,
        })
    }

    /// Releases one retired input after the host stopped its execution. A clean connection also
    /// clears a session left by an already-stopped or suspended source, without restoring any
    /// in-memory protocol state. Its ordinary connection budget bounds the entire cleanup.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "the host retires an obsolete input after its execution stopped"
        )
    )]
    pub async fn retire(&self, instance_index: u64) -> error_stack::Result<(), MqttSourceError> {
        let retirement = async {
            let mut connection = self.client(instance_index)?;
            connection
                .eventloop
                .options
                .set_session_mode(SessionMode::Clean);
            let budget = connection.eventloop.options.connect_timeout();
            match timeout(budget, self.retire_connection(&mut connection)).await {
                Ok(result) => result,
                Err(_) => Err(Report::new(MqttSourceError::RetirementDeadline { budget })),
            }
        };
        retirement
            .await
            .change_context(MqttSourceError::RetireInstance {
                instance: instance_index,
            })
    }

    async fn retire_connection(&self, connection: &mut MqttConnection) -> MqttSourceResult<()> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let event = connection.eventloop.poll().await.map_err(|error| {
                MqttConnectionError::report(error).change_context(MqttSourceError::Connect)
            })?;
            if let Event::Incoming(Incoming::ConnAck(_)) = event {
                break;
            }
        }
        connection
            .client
            .unsubscribe(self.subscribe_filter.as_str())
            .await
            .map_err(|error| Report::new(error).change_context(MqttSourceError::Unsubscribe))?;
        let unsubscribe = Self::confirm_unsubscribe(connection).await;
        // Even a refused unsubscribe must not keep this session alive. The clean connection's
        // zero expiry already makes dropping it terminal if the peer stops answering.
        connection
            .client
            .disconnect_now_with_properties(
                DisconnectReasonCode::NormalDisconnection,
                DisconnectProperties {
                    session_expiry_interval: Some(0),
                    reason_string: None,
                    user_properties: Vec::new(),
                    server_reference: None,
                },
            )
            .await
            .map_err(|error| Report::new(error).change_context(MqttSourceError::Disconnect))?;
        loop {
            nervix_primitives::task::consume_budget().await;
            let event = connection.eventloop.poll().await.map_err(|error| {
                MqttConnectionError::report(error).change_context(MqttSourceError::Disconnect)
            })?;
            if let Event::Outgoing(Outgoing::Disconnect) = event {
                return unsubscribe;
            }
        }
    }

    async fn confirm_unsubscribe(connection: &mut MqttConnection) -> MqttSourceResult<()> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let event = connection.eventloop.poll().await.map_err(|error| {
                MqttConnectionError::report(error).change_context(MqttSourceError::Unsubscribe)
            })?;
            if let Event::Incoming(Incoming::UnsubAck(answer)) = event {
                if answer.reasons.iter().all(|reason| {
                    matches!(
                        reason,
                        UnsubAckReason::Success | UnsubAckReason::NoSubscriptionExisted
                    )
                }) {
                    return Ok(());
                }
                return Err(Report::new(MqttSourceError::UnsubscribeRefused {
                    reasons: answer.reasons,
                }));
            }
        }
    }

    fn client_from_config(
        config: &[ClientConfigEntry],
        default_client_id: &str,
        session: MqttSession,
        manual_acks: bool,
        dns: DnsResolver,
    ) -> MqttSourceResult<(AsyncClient, EventLoop)> {
        let addr = client_config_value(config, "addr", ConnectorKind::Mqtt.into())
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
        MqttDialer::install(&mut options, dns);
        AsyncClient::builder(options)
            .capacity(1024)
            .try_build()
            .map_err(|error| Report::new(error).change_context(MqttSourceError::BuildClient))
    }

    fn parse_addr(addr: &str) -> MqttSourceResult<MqttSourceAddr> {
        let url = Url::parse(addr).map_err(|source| {
            Report::new(source).change_context(MqttSourceError::InvalidAddress)
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
            nervix_primitives::task::consume_budget().await;
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
                        MqttConnectionError::report(error).change_context(MqttSourceError::Connect)
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

/// How gathering the publishes that follow a batch's first ended, when it did not fail.
enum Gathered {
    /// The batch holds as many publishes as the host asked for.
    Full,
    /// The host cancelled the read while it waited for the next publish.
    Cancelled,
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

    /// Reads one batch from `publishes`: its first publish, and under a batch timeout the
    /// publishes that follow it, until the batch holds `request.max_messages` or the timeout,
    /// counted from the first publish, passes.
    ///
    /// The read observes the host's cancellation whenever it waits for a publish, so a read the
    /// host cancels ends at once with every publish it gathered, rather than once its batch is
    /// full or its timeout passes.
    async fn read_batch<P: PublishRead>(
        publishes: &mut P,
        request: SourceBatchRequest,
        cancellation: &SourceReadCancellation,
    ) -> MqttSourceResult<SourceBatch<MqttSourceMessage>> {
        let Some(first) = cancellation
            .unless_cancelled(publishes.next_publish())
            .await
        else {
            return Ok(SourceBatch::Interrupted {
                gathered: Vec::new(),
            });
        };
        let first = first?;
        let mut messages = Vec::with_capacity(request.max_messages.get());
        messages.push(MqttSourceMessage::new(first));
        if request.max_messages == NonZeroUsize::MIN {
            return Ok(SourceBatch::Messages(messages));
        }
        let Some(batch_timeout) = request.batch_timeout else {
            return Ok(SourceBatch::Messages(messages));
        };
        let Some(deadline) = Instant::now().checked_add(batch_timeout) else {
            return Err(Report::new(MqttSourceError::BatchDeadline));
        };
        let gathering = Self::gather(publishes, &mut messages, request.max_messages, cancellation);
        let gathered = timeout_at(deadline, gathering).await;
        match gathered {
            Ok(Ok(Gathered::Full)) => Ok(SourceBatch::Messages(messages)),
            Ok(Ok(Gathered::Cancelled)) => Ok(SourceBatch::Interrupted { gathered: messages }),
            Ok(Err(error)) => Err(error),
            // The batch timeout passed: the batch holds every publish it gathered until then.
            Err(_batch_timeout) => Ok(SourceBatch::Messages(messages)),
        }
    }

    /// Takes the publishes that follow a batch's first into `messages`, until the batch holds
    /// `max_messages` or the host cancels the read while it waits for the next publish.
    async fn gather<P: PublishRead>(
        publishes: &mut P,
        messages: &mut Vec<MqttSourceMessage>,
        max_messages: NonZeroUsize,
        cancellation: &SourceReadCancellation,
    ) -> MqttSourceResult<Gathered> {
        while messages.len() < max_messages.get() {
            nervix_primitives::task::consume_budget().await;
            let Some(next) = cancellation
                .unless_cancelled(publishes.next_publish())
                .await
            else {
                return Ok(Gathered::Cancelled);
            };
            let publish = next?;
            messages.push(MqttSourceMessage::new(publish));
        }
        Ok(Gathered::Full)
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
        cancellation: &SourceReadCancellation,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        if let Some(messages) = self.replayed_batch(request.max_messages) {
            return Ok(SourceBatch::Messages(messages));
        }
        let Some(connection) = self.connection.as_mut() else {
            return Ok(SourceBatch::ResumeRequired);
        };
        match Self::read_batch(connection, request, cancellation).await {
            Ok(batch) => Ok(batch),
            Err(error) => {
                // A read that loses its connection drops it, and with it every publish the read
                // took, which a persistent session delivers again once the source resumes.
                if let MqttSourceError::Receive = error.current_context() {
                    self.disconnect();
                }
                Err(error.change_context(SourceError::Read { connector: MQTT }))
            }
        }
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
            nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::task::consume_budget().await;
            self.replay.push_front(position.publish.clone());
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_dns::{DnsLookupError, DnsLookupFailure};
    use nonzero_ext::nonzero;

    use super::*;
    use crate::test_fixtures::DnsFixture;

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn config_with_client_id(client_id: &str) -> Vec<ClientConfigEntry> {
        vec![entry("client_id", client_id)]
    }

    fn retiring_instances(plan: &MqttSourcePlan, replacement: Option<&MqttSourcePlan>) -> Vec<u64> {
        let sessions = plan
            .sessions()
            .assured("the source fixture has valid session identities");
        let replacement = replacement.map(|replacement| {
            replacement
                .sessions()
                .assured("the replacement fixture has valid session identities")
        });
        sessions.retiring_instances(replacement.as_ref())
    }

    async fn build_client(
        config: &[ClientConfigEntry],
    ) -> MqttSourceResult<(AsyncClient, EventLoop)> {
        let fixture = DnsFixture::start().await;
        MqttSourcePlan::client_from_config(
            config,
            "default-client",
            MqttSession::Clean,
            false,
            fixture.dns.clone(),
        )
    }

    async fn build_client_error(
        config: &[ClientConfigEntry],
        expectation: &str,
    ) -> Report<MqttSourceError> {
        match build_client(config).await {
            Ok(_) => panic!("{expectation}"),
            Err(error) => error,
        }
    }

    /// The settings of one source instance subscribing to `events` through `addr` and `dns`.
    fn settings(addr: &str, session: MqttSession, dns: DnsResolver) -> MqttSourceSettings {
        MqttSourceSettings {
            instances: vec![vec![entry("addr", addr)]],
            client_id_conflict: None,
            default_client_id: "client".to_string(),
            share_group: "default~ingestor".to_string(),
            topic: "events".to_string(),
            session,
            qos: MqttQos::AtLeastOnce,
            manual_acks: true,
            dns,
        }
    }

    #[nervix_primitives::test]
    async fn persistent_sessions_allow_broker_only_resume() {
        let config = [
            entry("addr", "mqtt://127.0.0.1:1883"),
            entry("client_id", "persistent-client"),
        ];
        let fixture = DnsFixture::start().await;
        let (_, eventloop) = MqttSourcePlan::client_from_config(
            &config,
            "fallback",
            MqttSession::Persistent,
            true,
            fixture.dns.clone(),
        )
        .expect("persistent MQTT client must be valid");

        assert_eq!(
            eventloop.options.broker_session_resume_policy(),
            BrokerSessionResumePolicy::AllowBrokerOnly
        );
    }

    #[test]
    fn explicit_multi_instance_client_id_uses_instance_template() {
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
            None
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

    #[nervix_primitives::test]
    async fn retirement_keeps_matching_session_identities_and_releases_obsolete_ones() {
        let fixture = DnsFixture::start().await;
        let mut declared = settings(
            "mqtt://127.0.0.1:1883",
            MqttSession::Persistent,
            fixture.dns.clone(),
        );
        declared.default_client_id = "domain~events".to_string();
        let singleton = MqttSourcePlan::new(declared);
        assert_eq!(
            singleton
                .client(0)
                .assured("the source plan is valid")
                .eventloop
                .options
                .client_id(),
            "domain~events"
        );
        assert_eq!(retiring_instances(&singleton, None), vec![0]);
        assert!(retiring_instances(&singleton, Some(&singleton)).is_empty());

        let mut parallel = singleton.clone();
        parallel.instances.push(parallel.instances[0].clone());
        assert_eq!(
            parallel
                .client(0)
                .assured("the source plan is valid")
                .eventloop
                .options
                .client_id(),
            "domain~events~0"
        );
        assert_eq!(
            parallel
                .client(1)
                .assured("the source plan is valid")
                .eventloop
                .options
                .client_id(),
            "domain~events~1"
        );
        assert_eq!(retiring_instances(&singleton, Some(&parallel)), vec![0]);

        let mut expanded = parallel.clone();
        expanded.instances.push(expanded.instances[0].clone());
        assert!(retiring_instances(&parallel, Some(&expanded)).is_empty());
        assert_eq!(retiring_instances(&expanded, Some(&parallel)), vec![2]);

        let mut different_topic = parallel.clone();
        different_topic.subscribe_filter = "$share/domain~events/replacement".to_string();
        assert_eq!(
            retiring_instances(&parallel, Some(&different_topic)),
            vec![0, 1]
        );

        let mut explicit = singleton.clone();
        explicit.instances[0].push(entry("client_id", "provided-0"));
        let mut explicit_expanded = explicit.clone();
        explicit_expanded.instances.push(vec![
            entry("addr", "mqtt://127.0.0.1:1883"),
            entry("client_id", "provided-1"),
        ]);
        assert!(retiring_instances(&explicit, Some(&explicit_expanded)).is_empty());
        assert_eq!(
            retiring_instances(&explicit_expanded, Some(&explicit)),
            vec![1]
        );
        let mut repeated_identity = explicit.clone();
        repeated_identity
            .instances
            .push(repeated_identity.instances[0].clone());
        assert_eq!(retiring_instances(&repeated_identity, None), vec![0]);
    }

    #[nervix_primitives::test]
    async fn retirement_unsubscribes_and_sends_expiry_zero_even_when_unsubscribe_is_refused() {
        use crate::scripted_broker::ScriptedBroker;

        let fixture = DnsFixture::start().await;
        for reason in [0x00, 0x11, 0x87] {
            nervix_primitives::task::consume_budget().await;
            let broker = ScriptedBroker::start().await;
            let plan = MqttSourcePlan::new(settings(
                &broker.addr(),
                MqttSession::Persistent,
                fixture.dns.clone(),
            ));
            let observe = async {
                let mut connection = broker.accept().await;
                let (packet_type, body) = connection.read_packet().await;
                assert_eq!(packet_type, 10, "retirement sends UNSUBSCRIBE");
                assert_eq!(body[2], 0, "the unsubscribe carries no properties");
                let filter_length = usize::from(u16::from_be_bytes([body[3], body[4]]));
                assert_eq!(filter_length, plan.subscribe_filter.len());
                assert_eq!(&body[5..], plan.subscribe_filter.as_bytes());
                connection
                    .write(&[0xb0, 4, body[0], body[1], 0, reason])
                    .await;
                let (packet_type, body) = connection.read_packet().await;
                assert_eq!(packet_type, 14, "retirement sends DISCONNECT");
                assert_eq!(
                    body,
                    [0, 5, 0x11, 0, 0, 0, 0],
                    "DISCONNECT declares session expiry zero"
                );
            };
            let (result, ()) = timeout(Duration::from_secs(30), async {
                tokio::join!(plan.retire(0), observe)
            })
            .await
            .assured("the scripted exchange completes within its observation budget");
            if reason == 0x87 {
                let Err(error) = result else {
                    panic!("the broker's refusal must be reported");
                };
                assert!(matches!(
                    error.current_context(),
                    MqttSourceError::RetireInstance { instance: 0 }
                ));
                assert!(error.frames().any(|frame| matches!(frame.downcast_ref::<MqttSourceError>(), Some(MqttSourceError::UnsubscribeRefused { reasons }) if reasons == &[UnsubAckReason::NotAuthorized])));
            } else {
                result.assured("the broker released the subscription");
            }
        }
    }

    #[nervix_primitives::test]
    async fn retirement_of_a_silent_broker_ends_at_its_connection_budget() {
        use nervix_primitives::sync::CancellationToken;

        use crate::scripted_broker::ScriptedBroker;

        let fixture = DnsFixture::start().await;
        let broker = ScriptedBroker::start().await;
        let plan = MqttSourcePlan::new(settings(
            &broker.addr(),
            MqttSession::Persistent,
            fixture.dns.clone(),
        ));
        let budget = plan
            .client(0)
            .assured("the source plan is valid")
            .eventloop
            .options
            .connect_timeout();
        let finished = CancellationToken::new();
        let observe = async {
            let _connection = broker.accept_without_reply().await;
            finished.cancelled().await;
        };
        let retire = async {
            let result = plan.retire(0).await;
            finished.cancel();
            result
        };
        let started = Instant::now();
        let (result, ()) = timeout(Duration::from_secs(30), async {
            tokio::join!(retire, observe)
        })
        .await
        .assured("the cleanup finishes within its observation budget");
        let Err(error) = result else {
            panic!("a broker that never answers cannot complete session cleanup");
        };
        assert!(started.elapsed() >= budget);
        let cleanup_timeout = error.frames().any(|frame| matches!(frame.downcast_ref::<MqttSourceError>(), Some(MqttSourceError::RetirementDeadline { budget: actual }) if *actual == budget));
        let connect_timeout = error.frames().any(|frame| {
            matches!(
                frame.downcast_ref::<MqttConnectionError>(),
                Some(MqttConnectionError::Driver(
                    rumqttc::ConnectionError::Timeout(_)
                ))
            )
        });
        assert!(
            cleanup_timeout || connect_timeout,
            "the report preserves its timeout owner: {error:#}"
        );
    }

    #[nervix_primitives::test]
    async fn a_shared_client_id_names_the_conflict_as_the_resume_failure() {
        let fixture = DnsFixture::start().await;
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
            dns: fixture.dns.clone(),
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

    #[nervix_primitives::test]
    async fn client_builder_uses_configured_or_default_client_id() {
        build_client(&[entry("addr", "mqtt://broker.example.com:1883")])
            .await
            .expect("must build client from default id");
        build_client(&[
            entry("addr", "mqtt://broker.example.com:1883"),
            entry("client_id", "explicit-client"),
        ])
        .await
        .expect("must build client from explicit id");
    }

    #[nervix_primitives::test]
    async fn client_builder_requires_addr() {
        let error = build_client_error(&[], "missing mqtt addr").await;
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

    #[nervix_primitives::test]
    async fn client_reports_typed_address_and_tls_configuration_errors() {
        let error = build_client_error(
            &[entry("addr", "not a URL")],
            "an invalid MQTT address must fail",
        )
        .await;
        assert!(matches!(
            error.current_context(),
            MqttSourceError::InvalidAddress
        ));

        let error = build_client_error(
            &[entry("addr", "mqtts://localhost:8883")],
            "MQTTS requires an explicit CA file",
        )
        .await;
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
        )
        .await;
        assert!(matches!(
            error.current_context(),
            MqttSourceError::IncompleteTlsIdentity
        ));
    }

    #[nervix_primitives::test]
    async fn rejected_publishes_replay_in_order_before_the_broker_is_polled() {
        let fixture = DnsFixture::start().await;
        let plan = MqttSourcePlan::new(settings(
            "mqtt://127.0.0.1:1",
            MqttSession::Persistent,
            fixture.dns.clone(),
        ));
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
            .next_batch(
                SourceBatchRequest {
                    max_messages: nonzero!(2usize),
                    batch_timeout: None,
                },
                &SourceReadCancellation::new(),
            )
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

    #[nervix_primitives::test]
    async fn a_broker_whose_name_does_not_resolve_fails_the_resume_with_the_lookup() {
        let fixture = DnsFixture::start().await;
        fixture.deny("missing.nervix.test");
        let plan = MqttSourcePlan::new(settings(
            "mqtt://missing.nervix.test:1883",
            MqttSession::Clean,
            fixture.dns.clone(),
        ));
        let mut source = MqttSource::open(&plan, 0)
            .await
            .expect("an MQTT source opens without connecting");

        let resumed = source.resume().await;
        let Err(error) = resumed else {
            panic!("a broker whose name does not exist cannot be reached");
        };

        assert!(matches!(
            error.current_context(),
            SourceError::Resume { connector: MQTT }
        ));
        assert!(matches!(
            error.downcast_ref::<MqttSourceError>(),
            Some(MqttSourceError::Connect)
        ));
        assert!(
            matches!(
                error.downcast_ref::<MqttConnectionError>(),
                Some(MqttConnectionError::Resolve {
                    host,
                    failure: DnsLookupFailure::NameNotFound,
                }) if host == "missing.nervix.test"
            ),
            "{error:?}"
        );
        // DESCRIBE INGESTOR shows the deepest context, the resolver's own lookup error.
        let lookup = error
            .downcast_ref::<DnsLookupError>()
            .assured("the resolver's error is the deepest context");
        assert_eq!(
            lookup.to_string(),
            "resolving 'missing.nervix.test' failed: the name does not exist"
        );
        assert!(source.needs_resume());
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "source_read_tests.rs"]
mod read_tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "source_shuttle_tests.rs"]
mod shuttle_tests;
