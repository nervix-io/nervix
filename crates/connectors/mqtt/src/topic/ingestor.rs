//! MQTT ingestor transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The MQTT client and event loop each ingestor instance subscribes through, its
//!   session and quality-of-service settings, the Maximum Packet Size and the Receive Maximum every
//!   connection advertises and local admission of bounded publishes, the shared subscription every
//!   instance joins, the client identity each instance connects with, the batches a read gathers
//!   from its connection's read-ahead and how a read ends when the host cancels it, manual
//!   acknowledgement of publishes and its confirmation once written, the local replay of publishes
//!   the host rejected, and retirement of obsolete broker sessions.
//! - **Depends on.** The connector contract, the crate's broker connection through the node DNS
//!   resolver, the driver task that keeps each connection's event loop polled, typed client
//!   configuration entries, `error-stack`, Tokio, `rumqttc` and `url`.
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

mod driver;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    num::{NonZeroU16, NonZeroU64, NonZeroUsize},
    time::Duration,
};

use arch_into::ArchInto as _;
use async_trait::async_trait;
use driver::MqttConnection;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector, IngestMessageHeaders, IngestMetadataRow, IngestorBatch,
    IngestorBatchRequest, IngestorConnector, IngestorError, IngestorMessage,
    IngestorReadCancellation, IngestorResult, IngestorResume, NoIngestHeaders, client_config_value,
    client_tls_paths, optional_client_config_value, read_tls_file,
};
use nervix_dns::DnsResolver;
use nervix_models::{
    ClientConfigEntry, ConnectorKind, MQTT_MAXIMUM_PACKET_BYTES, MQTT_SOURCE_READ_AHEAD_HEADROOM,
    MqttQos, MqttSession,
};
use nervix_primitives::time::{Instant, timeout, timeout_at};
use rumqttc::{
    AckMode, AsyncClient, BrokerSessionResumePolicy, ConnectionError, DisconnectProperties,
    DisconnectReasonCode, Event, EventLoop, Incoming, ManualAck, MqttOptions, Outgoing, PubAck,
    PubRec, Publish, QoS, SessionMode, SubscribeReasonCode, TlsConfiguration,
    Transport as MqttTransport, UnsubAckReason,
};
use thiserror::Error;
use url::{Host, Url};

use crate::topic::connection::{MqttConnectionError, MqttDialer};

/// Admission releases or copies a packet's frame bytes before the client decodes another packet.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "classifying every decoded packet before decoding the next bounds retained raw \
                  frames to one"
    )
)]
const DECODED_PACKET_BATCH: usize = 1;

const MQTT: &str = "mqtt";
/// The template a multi-instance client identity must contain, so that every instance connects
/// under its own identity.
const MQTT_INSTANCE_PLACEHOLDER: &str = "{{instance}}";

/// Why an MQTT ingestor could not build its client, subscribe, receive, or acknowledge.
#[derive(Debug, Error)]
pub enum MqttTopicIngestorError {
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
    #[error(
        "the {connector} connection stopped reading with its read-ahead of {publishes} publishes \
         full, so it writes no acknowledgement until the source reads again",
        connector = ConnectorKind::Mqtt
    )]
    ReadAheadFull { publishes: NonZeroUsize },
    #[error(
        "the {connector} connection did not write the acknowledgements within its keep-alive of \
         {keep_alive:?}",
        connector = ConnectorKind::Mqtt
    )]
    AcknowledgementDeadline { keep_alive: Duration },
    #[error("the task driving the {connector} connection failed", connector = ConnectorKind::Mqtt)]
    Driver,
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

type MqttTopicIngestorResult<T> = Result<T, Report<MqttTopicIngestorError>>;

impl MqttTopicIngestorError {
    /// The typed cause of a connection the client's event loop ended with `error`.
    fn lost_connection(error: ConnectionError) -> Report<Self> {
        MqttConnectionError::report(error).change_context(Self::Receive)
    }

    /// Whether a read that failed with this error lost its connection.
    fn ends_connection(&self) -> bool {
        matches!(
            self,
            Self::Receive
                | Self::Driver
                | Self::Disconnected
                | Self::Acknowledge
                | Self::ReadAheadFull { .. }
                | Self::AcknowledgementDeadline { .. }
        )
    }
}

/// Why the declared client identity cannot serve every instance of an ingestor.
///
/// An ingestor with this conflict stays declared but never subscribes: each instance reports it as
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
struct MqttTopicIngestorAddr {
    host: String,
    port: u16,
    tls: bool,
}

/// A broker's session namespace and the client identity within it.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MqttSessionIdentity {
    broker: MqttTopicIngestorAddr,
    client_id: String,
}

/// The validated broker identities of an ingestor plan, with one representative per broker session.
pub struct MqttTopicIngestorSessions<'a> {
    plan: &'a MqttTopicIngestorPlan,
    identities: BTreeMap<MqttSessionIdentity, u64>,
}

impl MqttTopicIngestorSessions<'_> {
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

/// What one MQTT ingestor subscribes with.
pub struct MqttTopicIngestorSettings {
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
    /// How many publishes may wait on their acknowledgements at once, where the ingestor
    /// acknowledges each publish itself once the host accepted it. Absent, the client acknowledges
    /// each publish on receipt.
    pub acknowledgement_window: Option<NonZeroU64>,
    /// The node resolver every instance resolves its broker host through when it connects.
    pub dns: DnsResolver,
    /// Host-owned, pre-resolved counter for QoS 0 publishes that do not fit ACK-mode read-ahead.
    pub read_ahead_dropped: prometheus::IntCounter,
}

/// Who acknowledges an ingestor's publishes, and how many of them may wait on their acknowledgements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IngestorAcknowledgement {
    /// The client acknowledges each publish on receipt.
    OnReceipt,
    /// The ingestor acknowledges each publish once the host accepted it. Every connection
    /// advertises `receive_maximum`, the window and its headroom, as its Receive Maximum, so the
    /// broker sends no further publish at quality of service 1 or 2 while that many wait on their
    /// acknowledgements.
    Manual { receive_maximum: NonZeroU16 },
}

impl IngestorAcknowledgement {
    /// The acknowledgement of an ingestor whose publishes may wait on their acknowledgements `window`
    /// at a time, or that acknowledges none itself.
    fn of(window: Option<NonZeroU64>) -> Self {
        let Some(window) = window else {
            return Self::OnReceipt;
        };
        // MQTT carries the Receive Maximum as a Two Byte Integer, so a wider one is advertised as
        // the widest one the protocol holds: the broker sends no more unacknowledged publishes.
        let Some(admitted) = window.checked_add(MQTT_SOURCE_READ_AHEAD_HEADROOM) else {
            return Self::Manual {
                receive_maximum: NonZeroU16::MAX,
            };
        };
        let receive_maximum = match NonZeroU16::try_from(admitted) {
            Ok(receive_maximum) => receive_maximum,
            Err(_) => NonZeroU16::MAX,
        };
        Self::Manual { receive_maximum }
    }

    fn ack_mode(self) -> AckMode {
        match self {
            Self::OnReceipt => AckMode::Automatic,
            Self::Manual { .. } => AckMode::Manual,
        }
    }

    /// How many requests the client queues for its event loop: the most a connection queues before
    /// its event loop takes them. A subscription, an unsubscription and a disconnection each queue
    /// one and poll until it is written, and an acknowledgement queues one for each publish of its
    /// batch, which holds no more of them than the Receive Maximum admits.
    fn request_capacity(self) -> NonZeroUsize {
        match self {
            Self::OnReceipt => NonZeroUsize::MIN,
            Self::Manual { receive_maximum } => NonZeroUsize::from(receive_maximum),
        }
    }

    /// Every acknowledged outcome the Receive Maximum admits, plus separate QoS 0 headroom.
    /// Automatic acknowledgement keeps only the headroom and its existing backpressure.
    fn read_ahead(self) -> NonZeroUsize {
        let headroom = usize::try_from(MQTT_SOURCE_READ_AHEAD_HEADROOM)
            .assured("MQTT's read-ahead headroom fits every supported native target");
        let headroom = NonZeroUsize::new(headroom)
            .assured("MQTT's declared read-ahead headroom is nonzero");
        match self {
            Self::OnReceipt => headroom,
            Self::Manual { receive_maximum } => {
                let acknowledged = usize::from(receive_maximum.get());
                let capacity = acknowledged
                    .checked_add(headroom.get())
                    .assured("the supported native targets hold MQTT's 65,535 slots plus 64");
                NonZeroUsize::new(capacity)
                    .assured("the acknowledged window and its headroom are both nonzero")
            }
        }
    }
}

/// One MQTT ingestor's subscription settings, the client entries of each of its instances, and the
/// node resolver they connect through.
#[derive(Clone)]
pub struct MqttTopicIngestorPlan {
    instances: Vec<Vec<ClientConfigEntry>>,
    default_client_id: String,
    subscribe_filter: String,
    session: MqttSession,
    qos: QoS,
    acknowledgement: IngestorAcknowledgement,
    client_id_conflict: Option<MqttClientIdError>,
    dns: DnsResolver,
    read_ahead_dropped: prometheus::IntCounter,
}

/// One instance's client and the event loop that carries its session, before a driver owns the
/// event loop: while the ingestor establishes its subscription, and while retirement releases the
/// session.
struct UnstartedConnection {
    client: AsyncClient,
    eventloop: EventLoop,
}

/// The publishes an ingestor instance reads from its broker connection: the read-ahead its
/// connection's driver hands them over through, and a fake connection in the checks of how a read
/// meets the host's cancellation.
///
/// A read stops waiting for the next publish when the host cancels it or its batch timeout
/// passes. The connection keeps every publish it already handed over for its next read.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "each steady poll of a source's read takes its publishes through its connection"
    )
)]
trait PublishRead: Send {
    /// Waits until the connection yields its next publish.
    fn next_publish(
        &mut self,
    ) -> impl Future<Output = MqttTopicIngestorResult<PublishReadOutcome>> + Send;

    /// Takes a following publish, or ends the ordinary prefix before acknowledging a discard.
    fn gather_publish(
        &mut self,
    ) -> impl Future<Output = MqttTopicIngestorResult<GatherPublishOutcome>> + Send;
}

/// An admitted publish or one size rejection whose acknowledgement is already written.
enum PublishReadOutcome {
    Message(Box<Publish>),
    Oversized,
}

/// A following publish, or a boundary whose discard remains in the connection for the next read.
enum GatherPublishOutcome {
    Message(Box<Publish>),
    Boundary,
}

impl MqttTopicIngestorPlan {
    pub fn new(settings: MqttTopicIngestorSettings) -> Self {
        let MqttTopicIngestorSettings {
            instances,
            client_id_conflict,
            default_client_id,
            share_group,
            topic,
            session,
            qos,
            acknowledgement_window,
            dns,
            read_ahead_dropped,
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
            acknowledgement: IngestorAcknowledgement::of(acknowledgement_window),
            client_id_conflict,
            dns,
            read_ahead_dropped,
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

    fn client(&self, instance_index: u64) -> MqttTopicIngestorResult<UnstartedConnection> {
        let Some(instance) = self.instances.get(instance_index.arch_into()) else {
            return Err(Report::new(MqttTopicIngestorError::MissingInstance {
                instance: instance_index,
            }));
        };
        let default_client_id = self.default_instance_client_id(instance_index);
        let (client, eventloop) = Self::client_from_config(
            instance,
            &default_client_id,
            self.session,
            self.acknowledgement,
            self.dns.clone(),
        )?;
        Ok(UnstartedConnection { client, eventloop })
    }

    fn default_instance_client_id(&self, instance_index: u64) -> String {
        if self.instances.len() == 1 {
            self.default_client_id.clone()
        } else {
            format!("{}~{instance_index}", self.default_client_id)
        }
    }

    fn session_identity(
        &self,
        instance_index: u64,
    ) -> MqttTopicIngestorResult<MqttSessionIdentity> {
        let Some(instance) = self.instances.get(instance_index.arch_into()) else {
            return Err(Report::new(MqttTopicIngestorError::MissingInstance {
                instance: instance_index,
            }));
        };
        let address = client_config_value(instance, "addr", ConnectorKind::Mqtt.into())
            .change_context(MqttTopicIngestorError::ClientConfig)?;
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
    pub fn sessions(
        &self,
    ) -> error_stack::Result<MqttTopicIngestorSessions<'_>, MqttTopicIngestorError> {
        let mut identities = BTreeMap::new();
        for (instance_index, _) in (0_u64..).zip(&self.instances) {
            let identity = self.session_identity(instance_index)?;
            identities.entry(identity).or_insert(instance_index);
        }
        Ok(MqttTopicIngestorSessions {
            plan: self,
            identities,
        })
    }

    /// Releases one retired input after the host stopped its execution. A clean connection also
    /// clears a session left by an already-stopped or suspended ingestor, without restoring any
    /// in-memory protocol state. Its ordinary connection budget bounds the entire cleanup.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "the host retires an obsolete input after its execution stopped"
        )
    )]
    pub async fn retire(
        &self,
        instance_index: u64,
    ) -> error_stack::Result<(), MqttTopicIngestorError> {
        let retirement = async {
            let mut connection = self.client(instance_index)?;
            connection
                .eventloop
                .options
                .set_session_mode(SessionMode::Clean);
            let budget = connection.eventloop.options.connect_timeout();
            match timeout(budget, self.retire_connection(&mut connection)).await {
                Ok(result) => result,
                Err(_) => Err(Report::new(MqttTopicIngestorError::RetirementDeadline {
                    budget,
                })),
            }
        };
        retirement
            .await
            .change_context(MqttTopicIngestorError::RetireInstance {
                instance: instance_index,
            })
    }

    async fn retire_connection(
        &self,
        connection: &mut UnstartedConnection,
    ) -> MqttTopicIngestorResult<()> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let event = connection.eventloop.poll().await.map_err(|error| {
                MqttConnectionError::report(error).change_context(MqttTopicIngestorError::Connect)
            })?;
            if let Event::Incoming(Incoming::ConnAck(_)) = event {
                break;
            }
        }
        connection
            .client
            .unsubscribe(self.subscribe_filter.as_str())
            .await
            .map_err(|error| {
                Report::new(error).change_context(MqttTopicIngestorError::Unsubscribe)
            })?;
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
            .map_err(|error| {
                Report::new(error).change_context(MqttTopicIngestorError::Disconnect)
            })?;
        loop {
            nervix_primitives::task::consume_budget().await;
            let event = connection.eventloop.poll().await.map_err(|error| {
                MqttConnectionError::report(error)
                    .change_context(MqttTopicIngestorError::Disconnect)
            })?;
            if let Event::Outgoing(Outgoing::Disconnect) = event {
                return unsubscribe;
            }
        }
    }

    async fn confirm_unsubscribe(
        connection: &mut UnstartedConnection,
    ) -> MqttTopicIngestorResult<()> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let event = connection.eventloop.poll().await.map_err(|error| {
                MqttConnectionError::report(error)
                    .change_context(MqttTopicIngestorError::Unsubscribe)
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
                return Err(Report::new(MqttTopicIngestorError::UnsubscribeRefused {
                    reasons: answer.reasons,
                }));
            }
        }
    }

    fn client_from_config(
        config: &[ClientConfigEntry],
        default_client_id: &str,
        session: MqttSession,
        acknowledgement: IngestorAcknowledgement,
        dns: DnsResolver,
    ) -> MqttTopicIngestorResult<(AsyncClient, EventLoop)> {
        let addr = client_config_value(config, "addr", ConnectorKind::Mqtt.into())
            .change_context(MqttTopicIngestorError::ClientConfig)?;
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
        // Receive every legal packet so local admission can acknowledge oversized publishes and
        // release their broker inflight slots. Classify one decoded packet before reading another.
        let maximum_packet_size = u32::try_from(MQTT_MAXIMUM_PACKET_BYTES)
            .assured("the vocabulary bounds the declared size by MQTT's Four Byte Integer");
        options.set_max_packet_size(Some(maximum_packet_size));
        options.set_read_batch_size(DECODED_PACKET_BATCH);
        if session == MqttSession::Persistent {
            // Nervix deliberately keeps in-flight messages and ACK state in memory. After an
            // owner change, accept the broker's retained session and let it redeliver pending
            // QoS messages instead of requiring a local rumqttc protocol checkpoint.
            options.set_broker_session_resume_policy(BrokerSessionResumePolicy::AllowBrokerOnly);
        }
        options.set_ack_mode(acknowledgement.ack_mode());
        if let IngestorAcknowledgement::Manual { receive_maximum } = acknowledgement {
            // The broker sends no further publish that asks for an acknowledgement while this many
            // wait on theirs, so neither the read-ahead nor the client's requests fill with them.
            options.set_receive_maximum(Some(receive_maximum.get()));
        }
        if mqtt_addr.tls {
            let tls = client_tls_paths(config);
            let ca = if let Some(ca_file) = tls.ca_file.as_ref() {
                read_tls_file(ca_file, "TLS CA certificate")
                    .change_context(MqttTopicIngestorError::ClientConfig)?
            } else {
                return Err(Report::new(MqttTopicIngestorError::MissingTlsCa));
            };
            let client_auth = match (&tls.cert_file, &tls.key_file) {
                (Some(cert_file), Some(key_file)) => Some((
                    read_tls_file(cert_file, "TLS certificate")
                        .change_context(MqttTopicIngestorError::ClientConfig)?,
                    read_tls_file(key_file, "TLS private key")
                        .change_context(MqttTopicIngestorError::ClientConfig)?,
                )),
                (None, None) => None,
                _ => {
                    return Err(Report::new(MqttTopicIngestorError::IncompleteTlsIdentity));
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
            .capacity(acknowledgement.request_capacity().get())
            .try_build()
            .map_err(|error| Report::new(error).change_context(MqttTopicIngestorError::BuildClient))
    }

    fn parse_addr(addr: &str) -> MqttTopicIngestorResult<MqttTopicIngestorAddr> {
        let url = Url::parse(addr).map_err(|source| {
            Report::new(source).change_context(MqttTopicIngestorError::InvalidAddress)
        })?;
        let tls = if url.scheme() == "mqtt" {
            false
        } else if url.scheme() == "mqtts" {
            true
        } else {
            return Err(Report::new(MqttTopicIngestorError::UnsupportedScheme {
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
            return Err(Report::new(MqttTopicIngestorError::MissingHost));
        }
        let Some(port) = url.port() else {
            return Err(Report::new(MqttTopicIngestorError::MissingPort));
        };
        Ok(MqttTopicIngestorAddr { host, port, tls })
    }

    /// Drives the event loop until the broker confirms the subscription.
    ///
    /// A broker that resumes a persistent session already holds the subscription and cannot
    /// allocate a new SUBSCRIBE packet identifier, so its CONNACK is the readiness boundary and
    /// queued QoS messages follow immediately.
    async fn establish_subscription(
        &self,
        connection: &mut UnstartedConnection,
    ) -> MqttTopicIngestorResult<()> {
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
                            Report::new(MqttTopicIngestorError::Subscribe)
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
                    return Err(Report::new(MqttTopicIngestorError::SubscriptionRefused)
                        .attach_printable(format!("{suback:?}")));
                }
                Ok(Event::Incoming(_) | Event::Outgoing(_) | Event::Auth(_)) => {}
                Err(error) => {
                    return Err(MqttConnectionError::report(error)
                        .change_context(MqttTopicIngestorError::Connect));
                }
            }
        }
    }
}

/// The publish one acknowledgement settles, kept whole because a rejected publish is delivered
/// again from here and an acknowledged one is confirmed by its packet identifier.
#[derive(Debug, Clone)]
pub struct MqttTopicIngestorPosition {
    publish: Publish,
}

impl MqttTopicIngestorPosition {
    /// The publish this position settles.
    fn publish(&self) -> &Publish {
        &self.publish
    }
}

/// Prepares an acknowledgement without retaining any topic, payload or properties.
fn acknowledgement(publish: &Publish) -> Option<ManualAck> {
    match publish.qos {
        QoS::AtMostOnce => None,
        QoS::AtLeastOnce => Some(ManualAck::PubAck(PubAck::new(publish.pkid, None))),
        QoS::ExactlyOnce => Some(ManualAck::PubRec(PubRec::new(publish.pkid, None))),
    }
}

/// One received publish. MQTT carries no headers into the ingest namespace.
pub struct MqttTopicIngestorMessage {
    position: MqttTopicIngestorPosition,
    headers: NoIngestHeaders,
}

impl MqttTopicIngestorMessage {
    fn new(publish: Publish) -> Self {
        Self {
            position: MqttTopicIngestorPosition { publish },
            headers: NoIngestHeaders,
        }
    }
}

impl IngestorMessage for MqttTopicIngestorMessage {
    type Position = MqttTopicIngestorPosition;

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
    /// The batch is full or the following discard ends its ordinary prefix.
    Ready,
    /// The host cancelled the read while it waited for the next publish.
    Cancelled,
}

/// One MQTT ingestor instance: its connection, absent while suspended or reconnecting, and the
/// publishes the host rejected that it delivers again before reading its connection.
pub struct MqttTopicIngestor {
    plan: MqttTopicIngestorPlan,
    instance_index: u64,
    connection: Option<MqttConnection>,
    replay: VecDeque<Publish>,
}

impl MqttTopicIngestor {
    /// Drops the connection, which ends its driver and the broker connection, and with it every
    /// rejected publish, whose packet identifiers belong to the session that ends here. A persistent
    /// session delivers them again on resume.
    fn disconnect(&mut self) {
        self.connection = None;
        self.replay.clear();
    }

    /// Reads one batch from `publishes`: its first publish, and under a batch timeout the
    /// publishes that follow it, until the batch holds `request.max_messages` or the timeout,
    /// counted from the first publish, passes. A discard ends the ordinary prefix before its
    /// acknowledgement starts, preserving MQTT acknowledgement order.
    ///
    /// The read observes the host's cancellation whenever it waits for a publish, so a read the
    /// host cancels ends at once with every publish it gathered, rather than once its batch is
    /// full or its timeout passes.
    async fn read_batch<P: PublishRead>(
        publishes: &mut P,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> MqttTopicIngestorResult<IngestorBatch<MqttTopicIngestorMessage>> {
        let Some(first) = cancellation
            .unless_cancelled(publishes.next_publish())
            .await
        else {
            return Ok(IngestorBatch::Interrupted {
                gathered: Vec::new(),
            });
        };
        let first = match first? {
            PublishReadOutcome::Message(publish) => *publish,
            PublishReadOutcome::Oversized => {
                return Ok(IngestorBatch::Oversized {
                    payloads: NonZeroUsize::MIN,
                });
            }
        };
        let mut messages = Vec::with_capacity(request.max_messages.get());
        messages.push(MqttTopicIngestorMessage::new(first));
        if request.max_messages == NonZeroUsize::MIN {
            return Ok(IngestorBatch::Messages(messages));
        }
        let Some(batch_timeout) = request.batch_timeout else {
            return Ok(IngestorBatch::Messages(messages));
        };
        let Some(deadline) = Instant::now().checked_add(batch_timeout) else {
            return Err(Report::new(MqttTopicIngestorError::BatchDeadline));
        };
        let gathering = Self::gather(publishes, &mut messages, request.max_messages, cancellation);
        let gathered = timeout_at(deadline, gathering).await;
        match gathered {
            Ok(Ok(Gathered::Ready)) => Ok(IngestorBatch::Messages(messages)),
            Ok(Ok(Gathered::Cancelled)) => Ok(IngestorBatch::Interrupted { gathered: messages }),
            Ok(Err(error)) => Err(error),
            // The batch timeout passed: the batch holds every publish it gathered until then.
            Err(_batch_timeout) => Ok(IngestorBatch::Messages(messages)),
        }
    }

    /// Takes the publishes that follow a batch's first into `messages`, until the batch holds
    /// `max_messages` or the host cancels the read while it waits for the next publish.
    async fn gather<P: PublishRead>(
        publishes: &mut P,
        messages: &mut Vec<MqttTopicIngestorMessage>,
        max_messages: NonZeroUsize,
        cancellation: &IngestorReadCancellation,
    ) -> MqttTopicIngestorResult<Gathered> {
        while messages.len() < max_messages.get() {
            nervix_primitives::task::consume_budget().await;
            let Some(next) = cancellation
                .unless_cancelled(publishes.gather_publish())
                .await
            else {
                return Ok(Gathered::Cancelled);
            };
            match next? {
                GatherPublishOutcome::Message(publish) => {
                    messages.push(MqttTopicIngestorMessage::new(*publish))
                }
                GatherPublishOutcome::Boundary => return Ok(Gathered::Ready),
            }
        }
        Ok(Gathered::Ready)
    }

    fn replayed_batch(
        &mut self,
        max_messages: NonZeroUsize,
    ) -> Option<Vec<MqttTopicIngestorMessage>> {
        let first = self.replay.pop_front()?;
        let mut messages = Vec::with_capacity(max_messages.get());
        messages.push(MqttTopicIngestorMessage::new(first));
        while messages.len() < max_messages.get() {
            let Some(publish) = self.replay.pop_front() else {
                break;
            };
            messages.push(MqttTopicIngestorMessage::new(publish));
        }
        Some(messages)
    }
}

#[async_trait]
impl IngestorConnector for MqttTopicIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Mqtt(nervix_models::MqttIngestorEntity::Topic);

    type Plan = MqttTopicIngestorPlan;

    async fn open(plan: &Self::Plan, instance_index: u64) -> IngestorResult<Self> {
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
    async fn suspend(&mut self) -> IngestorResult<()> {
        self.disconnect();
        Ok(())
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        if let Some(conflict) = self.plan.client_id_conflict.as_ref() {
            return Err(Report::new(conflict.clone())
                .change_context(IngestorError::Resume { connector: MQTT }));
        }
        if self.connection.is_none() {
            let mut connection = self
                .plan
                .client(self.instance_index)
                .change_context(IngestorError::Resume { connector: MQTT })?;
            self.plan
                .establish_subscription(&mut connection)
                .await
                .change_context(IngestorError::Resume { connector: MQTT })?;
            let UnstartedConnection { client, eventloop } = connection;
            // The CONNACK has replaced the client's own keep-alive with the broker's, if it
            // announced one, and a keep-alive of zero is none.
            let keep_alive = eventloop.options.keep_alive();
            let keep_alive = if keep_alive.is_zero() {
                None
            } else {
                Some(keep_alive)
            };
            self.connection = Some(MqttConnection::start(
                client,
                eventloop,
                self.plan.acknowledgement,
                keep_alive,
                self.plan.read_ahead_dropped.clone(),
            ));
        }
        Ok(IngestorResume::Ready)
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.disconnect();
        Ok(())
    }
}

#[async_trait]
impl BrokerIngestorConnector for MqttTopicIngestor {
    type Message = MqttTopicIngestorMessage;
    type Position = MqttTopicIngestorPosition;

    async fn next_batch(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        if let Some(messages) = self.replayed_batch(request.max_messages) {
            return Ok(IngestorBatch::Messages(messages));
        }
        let Some(connection) = self.connection.as_mut() else {
            return Ok(IngestorBatch::ResumeRequired);
        };
        match Self::read_batch(connection, request, cancellation).await {
            Ok(batch) => Ok(batch),
            Err(error) => {
                // A read that loses its connection drops it, and with it every publish the read
                // took, which a persistent session delivers again once the ingestor resumes.
                if error.current_context().ends_connection() {
                    self.disconnect();
                }
                Err(error.change_context(IngestorError::Read { connector: MQTT }))
            }
        }
    }

    /// Returns once the client's event loop has written every `PUBACK` the positions ask for to the
    /// connection. The broker answers none, so nothing more can be confirmed.
    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let IngestorAcknowledgement::Manual { .. } = self.plan.acknowledgement else {
            return Ok(());
        };
        let Some(connection) = self.connection.as_mut() else {
            return Err(Report::new(MqttTopicIngestorError::Disconnected)
                .change_context(IngestorError::Acknowledge { connector: MQTT }));
        };
        let Err(error) = connection.acknowledge(positions).await else {
            return Ok(());
        };
        // An acknowledgement left unwritten waits where nothing writes it, so the connection is
        // dropped with it: the broker delivers again what it never saw acknowledged once the
        // ingestor resumes, and a later acknowledgement can never repeat one written late.
        self.disconnect();
        Err(error.change_context(IngestorError::Acknowledge { connector: MQTT }))
    }

    /// The broker delivers a QoS 1 publish again only when the session reconnects, so a rejected
    /// publish is delivered again from here, ahead of anything the broker sends next.
    ///
    /// A publish whose connection ended belongs to a session the broker delivers again once the
    /// ingestor resumes, and no other connection can reject it, so its rejection fails.
    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let ended = match self.connection.as_ref() {
            Some(connection) => connection.has_ended(),
            None => true,
        };
        if ended {
            self.disconnect();
            return Err(Report::new(MqttTopicIngestorError::Disconnected)
                .change_context(IngestorError::Reject { connector: MQTT }));
        }
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
    use nervix_dns::{DnsLookupError, DnsLookupFailure};
    use nonzero_ext::nonzero;

    use super::{metrics_fixture::IngestorMetricsFixture, *};
    use crate::topic::test_fixtures::DnsFixture;

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn config_with_client_id(client_id: &str) -> Vec<ClientConfigEntry> {
        vec![entry("client_id", client_id)]
    }

    fn retiring_instances(
        plan: &MqttTopicIngestorPlan,
        replacement: Option<&MqttTopicIngestorPlan>,
    ) -> Vec<u64> {
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
    ) -> MqttTopicIngestorResult<(AsyncClient, EventLoop)> {
        let fixture = DnsFixture::start().await;
        MqttTopicIngestorPlan::client_from_config(
            config,
            "default-client",
            MqttSession::Clean,
            IngestorAcknowledgement::OnReceipt,
            fixture.dns.clone(),
        )
    }

    async fn build_client_error(
        config: &[ClientConfigEntry],
        expectation: &str,
    ) -> Report<MqttTopicIngestorError> {
        match build_client(config).await {
            Ok(_) => panic!("{expectation}"),
            Err(error) => error,
        }
    }

    /// The settings of one ingestor instance subscribing to `events` through `addr` and `dns`.
    fn settings(addr: &str, session: MqttSession, dns: DnsResolver) -> MqttTopicIngestorSettings {
        MqttTopicIngestorSettings {
            instances: vec![vec![entry("addr", addr)]],
            client_id_conflict: None,
            default_client_id: "client".to_string(),
            share_group: "default~ingestor".to_string(),
            topic: "events".to_string(),
            session,
            qos: MqttQos::AtLeastOnce,
            acknowledgement_window: Some(nonzero!(1u64)),
            dns,
            read_ahead_dropped: IngestorMetricsFixture::new().counter(),
        }
    }

    #[nervix_primitives::test]
    async fn persistent_sessions_allow_broker_only_resume() {
        let config = [
            entry("addr", "mqtt://127.0.0.1:1883"),
            entry("client_id", "persistent-client"),
        ];
        let fixture = DnsFixture::start().await;
        let (_, eventloop) = MqttTopicIngestorPlan::client_from_config(
            &config,
            "fallback",
            MqttSession::Persistent,
            IngestorAcknowledgement::of(Some(nonzero!(1u64))),
            fixture.dns.clone(),
        )
        .expect("persistent MQTT client must be valid");

        assert_eq!(
            eventloop.options.broker_session_resume_policy(),
            BrokerSessionResumePolicy::AllowBrokerOnly
        );
    }

    #[nervix_primitives::test]
    async fn every_connection_receives_protocol_sized_packets_before_source_admission() {
        let config = [entry("addr", "mqtt://127.0.0.1:1883")];
        for session in [MqttSession::Clean, MqttSession::Persistent] {
            nervix_primitives::task::consume_budget().await;
            let fixture = DnsFixture::start().await;
            let (_, eventloop) = MqttTopicIngestorPlan::client_from_config(
                &config,
                "bounded",
                session,
                IngestorAcknowledgement::of(Some(nonzero!(1u64))),
                fixture.dns.clone(),
            )
            .expect("the bounded MQTT client is valid");

            assert_eq!(
                eventloop.options.max_packet_size().map(u64::from),
                Some(268_435_460),
                "a {session:?} session receives the full MQTT Remaining Length and fixed header"
            );
            assert_eq!(eventloop.options.read_batch_size(), 1);
        }
    }

    #[nervix_primitives::test]
    async fn an_acknowledging_connection_advertises_its_window_and_headroom_as_receive_maximum() {
        let config = [entry("addr", "mqtt://127.0.0.1:1883")];
        let fixture = DnsFixture::start().await;
        for (window, advertised) in [
            (nonzero!(1u64), 65),
            (nonzero!(2_000u64), 2_064),
            (nonzero!(65_471u64), u16::MAX),
            (nonzero!(65_472u64), u16::MAX),
            (NonZeroU64::MAX, u16::MAX),
        ] {
            nervix_primitives::task::consume_budget().await;
            let (_, eventloop) = MqttTopicIngestorPlan::client_from_config(
                &config,
                "windowed",
                MqttSession::Persistent,
                IngestorAcknowledgement::of(Some(window)),
                fixture.dns.clone(),
            )
            .expect("an acknowledging MQTT client is valid");

            assert_eq!(
                eventloop.options.receive_maximum(),
                Some(advertised),
                "a window of {window} is advertised with its headroom, as far as MQTT can carry"
            );
        }
        let (_, eventloop) = MqttTopicIngestorPlan::client_from_config(
            &config,
            "on-receipt",
            MqttSession::Clean,
            IngestorAcknowledgement::OnReceipt,
            fixture.dns.clone(),
        )
        .expect("a client that acknowledges on receipt is valid");
        assert_eq!(
            eventloop.options.receive_maximum(),
            None,
            "a client that acknowledges every publish on receipt advertises no window"
        );
    }

    #[test]
    fn read_ahead_reserves_the_receive_maximum_and_separate_qos_zero_headroom() {
        let on_receipt = IngestorAcknowledgement::OnReceipt;
        assert_eq!(on_receipt.read_ahead().get(), 64);
        assert_eq!(
            on_receipt.request_capacity(),
            NonZeroUsize::MIN,
            "a connection that acknowledges on receipt queues one request at a time"
        );

        let sequential = IngestorAcknowledgement::of(Some(NonZeroU64::MIN));
        assert_eq!(
            sequential,
            IngestorAcknowledgement::Manual {
                receive_maximum: nonzero!(65u16)
            }
        );
        assert_eq!(sequential.read_ahead().get(), 129);
        assert_eq!(sequential.request_capacity().get(), 65);

        let parallel = IngestorAcknowledgement::of(Some(nonzero!(2_000u64)));
        assert_eq!(
            parallel,
            IngestorAcknowledgement::Manual {
                receive_maximum: nonzero!(2_064u16)
            }
        );
        assert_eq!(parallel.read_ahead().get(), 2_128);
        assert_eq!(parallel.request_capacity().get(), 2_064);

        let wide = IngestorAcknowledgement::of(Some(nonzero!(70_000u64)));
        assert_eq!(
            wide,
            IngestorAcknowledgement::Manual {
                receive_maximum: NonZeroU16::MAX
            }
        );
        assert_eq!(wide.read_ahead().get(), 65_599);
        assert_eq!(wide.request_capacity().get(), usize::from(u16::MAX));
    }

    #[test]
    fn explicit_multi_instance_client_id_uses_instance_template() {
        assert_eq!(
            MqttTopicIngestorPlan::client_id_conflict(
                &config_with_client_id("fixed-client"),
                nonzero!(2u64)
            ),
            Some(MqttClientIdError::SharedIdentity {
                client_id: "fixed-client".to_string(),
                instances: nonzero!(2u64),
            })
        );
        assert_eq!(
            MqttTopicIngestorPlan::client_id_conflict(&[], nonzero!(2u64)),
            None
        );
        assert_eq!(
            MqttTopicIngestorPlan::client_id_conflict(
                &config_with_client_id("templated-{{instance}}"),
                nonzero!(2u64)
            ),
            None
        );
        assert_eq!(
            MqttTopicIngestorPlan::client_id_conflict(&[], nonzero!(1u64)),
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
        let singleton = MqttTopicIngestorPlan::new(declared);
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
        use crate::topic::scripted_broker::ScriptedBroker;

        let fixture = DnsFixture::start().await;
        for reason in [0x00, 0x11, 0x87] {
            nervix_primitives::task::consume_budget().await;
            let broker = ScriptedBroker::start().await;
            let plan = MqttTopicIngestorPlan::new(settings(
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
                    MqttTopicIngestorError::RetireInstance { instance: 0 }
                ));
                assert!(error.frames().any(|frame| matches!(frame.downcast_ref::<MqttTopicIngestorError>(), Some(MqttTopicIngestorError::UnsubscribeRefused { reasons }) if reasons == &[UnsubAckReason::NotAuthorized])));
            } else {
                result.assured("the broker released the subscription");
            }
        }
    }

    #[nervix_primitives::test]
    async fn retirement_of_a_silent_broker_ends_at_its_connection_budget() {
        use nervix_primitives::sync::CancellationToken;

        use crate::topic::scripted_broker::ScriptedBroker;

        let fixture = DnsFixture::start().await;
        let broker = ScriptedBroker::start().await;
        let plan = MqttTopicIngestorPlan::new(settings(
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
        let cleanup_timeout = error.frames().any(|frame| matches!(frame.downcast_ref::<MqttTopicIngestorError>(), Some(MqttTopicIngestorError::RetirementDeadline { budget: actual }) if *actual == budget));
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
        let plan = MqttTopicIngestorPlan::new(MqttTopicIngestorSettings {
            instances: vec![Vec::new(), Vec::new()],
            client_id_conflict: MqttTopicIngestorPlan::client_id_conflict(
                &config_with_client_id("fixed-client"),
                nonzero!(2u64),
            ),
            default_client_id: "fallback".to_string(),
            share_group: "default~mqtt_notifications".to_string(),
            topic: "notifications".to_string(),
            session: MqttSession::Persistent,
            qos: MqttQos::AtLeastOnce,
            acknowledgement_window: Some(nonzero!(1u64)),
            dns: fixture.dns.clone(),
            read_ahead_dropped: IngestorMetricsFixture::new().counter(),
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
            MqttTopicIngestorPlan::parse_addr("mqtt://user:pass@broker.example.com:1883/topic")
                .expect("must parse"),
            MqttTopicIngestorAddr {
                host: "broker.example.com".to_string(),
                port: 1883,
                tls: false,
            }
        );
        assert_eq!(
            MqttTopicIngestorPlan::parse_addr("mqtts://broker.example.com:8883")
                .expect("must parse"),
            MqttTopicIngestorAddr {
                host: "broker.example.com".to_string(),
                port: 8883,
                tls: true,
            }
        );
        assert_eq!(
            MqttTopicIngestorPlan::parse_addr("mqtt://[2001:db8::1]:1883/topic")
                .expect("must parse"),
            MqttTopicIngestorAddr {
                host: "2001:db8::1".to_string(),
                port: 1883,
                tls: false,
            }
        );
        assert_eq!(
            MqttTopicIngestorPlan::parse_addr("mqtt://broker.example.com:1883?keep_alive=30")
                .expect("must parse"),
            MqttTopicIngestorAddr {
                host: "broker.example.com".to_string(),
                port: 1883,
                tls: false,
            }
        );
        assert!(MqttTopicIngestorPlan::parse_addr("http://broker.example.com:1883").is_err());
        assert!(MqttTopicIngestorPlan::parse_addr("mqtt://broker.example.com").is_err());
        assert!(MqttTopicIngestorPlan::parse_addr("mqtt://:1883").is_err());
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
            MqttTopicIngestorError::ClientConfig
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
            MqttTopicIngestorError::InvalidAddress
        ));

        let error = build_client_error(
            &[entry("addr", "mqtts://localhost:8883")],
            "MQTTS requires an explicit CA file",
        )
        .await;
        assert!(matches!(
            error.current_context(),
            MqttTopicIngestorError::MissingTlsCa
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
            MqttTopicIngestorError::IncompleteTlsIdentity
        ));
    }

    #[nervix_primitives::test]
    async fn rejected_publishes_replay_in_order_before_the_connection_is_read() {
        use crate::topic::scripted_broker::ScriptedBroker;

        let fixture = DnsFixture::start().await;
        let broker = ScriptedBroker::start().await;
        let plan = MqttTopicIngestorPlan::new(settings(
            &broker.addr(),
            MqttSession::Persistent,
            fixture.dns.clone(),
        ));
        let mut source = MqttTopicIngestor::open(&plan, 0)
            .await
            .expect("an MQTT source opens without connecting");
        let grant = async {
            let mut connection = broker.accept().await;
            connection.grant_subscription().await;
            connection
        };
        let (resumed, mut connection) = tokio::join!(source.resume(), grant);
        resumed.expect("the scripted broker granted the subscription");
        // The connection reads this publish into its read-ahead, behind the rejected ones.
        connection.write_publish(3, b"3").await;
        let first = MqttTopicIngestorPosition {
            publish: Publish::new("events", QoS::AtLeastOnce, b"1".to_vec(), None),
        };
        let second = MqttTopicIngestorPosition {
            publish: Publish::new("events", QoS::AtLeastOnce, b"2".to_vec(), None),
        };
        source
            .reject(&[first, second])
            .await
            .expect("rejecting on a live connection queues the replay");

        let cancellation = IngestorReadCancellation::new();
        let replayed = timeout(
            Duration::from_secs(30),
            source.next_batch(
                IngestorBatchRequest {
                    max_messages: nonzero!(2usize),
                    batch_timeout: None,
                },
                &cancellation,
            ),
        )
        .await
        .expect("the replay is delivered without waiting for the connection")
        .expect("the replay is delivered before the connection is read");
        let read = timeout(
            Duration::from_secs(30),
            source.next_batch(
                IngestorBatchRequest {
                    max_messages: NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &cancellation,
            ),
        )
        .await
        .expect("the client received the publish within its observation budget")
        .expect("the scripted broker keeps the connection open");

        let mut payloads = Vec::new();
        for batch in [replayed, read] {
            let IngestorBatch::Messages(messages) = batch else {
                panic!("an uncancelled read is delivered as messages");
            };
            for message in &messages {
                payloads.push(message.payload().to_vec());
            }
        }
        assert_eq!(
            payloads,
            [b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
            "the rejected publishes come back in order, ahead of what the connection read"
        );
        assert!(source.replay.is_empty());
    }

    #[nervix_primitives::test]
    async fn a_broker_whose_name_does_not_resolve_fails_the_resume_with_the_lookup() {
        let fixture = DnsFixture::start().await;
        fixture.deny("missing.nervix.test");
        let plan = MqttTopicIngestorPlan::new(settings(
            "mqtt://missing.nervix.test:1883",
            MqttSession::Clean,
            fixture.dns.clone(),
        ));
        let mut source = MqttTopicIngestor::open(&plan, 0)
            .await
            .expect("an MQTT source opens without connecting");

        let resumed = source.resume().await;
        let Err(error) = resumed else {
            panic!("a broker whose name does not exist cannot be reached");
        };

        assert!(matches!(
            error.current_context(),
            IngestorError::Resume { connector: MQTT }
        ));
        assert!(matches!(
            error.downcast_ref::<MqttTopicIngestorError>(),
            Some(MqttTopicIngestorError::Connect)
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
#[path = "ingestor_read_tests.rs"]
mod read_tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "ingestor_shuttle_tests.rs"]
mod shuttle_tests;

#[cfg(test)]
#[path = "ingestor/metrics_fixture.rs"]
mod metrics_fixture;
