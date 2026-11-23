//! The ingestor composition root, and the one start path every ingestor takes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Starting an ingestor: preparing its quiescence, refusing a second start, compiling
//!   its dependencies, and mapping its input plan to the connector that runs a transport or to the
//!   client ingestor the node hosts, which is the only place an input plan's kind selects anything.
//! - **Depends on.** Ingestor start plans, the host ingestor launcher, every ingestor connector, and
//!   the endpoint and client ingestors the server keeps.
//! - **Must not know.** Plexus parsing, registry validation, or placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "ingestor installation and intake withdrawal bind one exact connector lifetime"
    )
)]

use super::{ingestor_start::RetiringIngestorInput, *};

mod connector;
pub(in crate::runtime) mod endpoint;
pub(in crate::runtime) mod http;
pub(in crate::runtime) mod kafka;
pub(in crate::runtime) mod mqtt;
pub(in crate::runtime) mod nats;
pub(in crate::runtime) mod prometheus;
pub(in crate::runtime) mod pulsar;
pub(in crate::runtime) mod rabbitmq;
pub(in crate::runtime) mod redis;
pub(in crate::runtime) mod sqs;
pub(in crate::runtime) mod syslog;
pub(in crate::runtime) mod websockets;
pub(in crate::runtime) mod zeromq;

/// Why this node could not start an ingestor.
///
/// Every failure names the ingestor, or the domain and codec it binds. A failure to initialize the
/// ingestor's connector is [`IngestorStartError::Initialize`], with the cause beneath it: a
/// [`IngestorConnectorStartError`], or the report of the client configuration, connector plan, ingestor
/// instance or domain cadence that failed.
#[derive(Debug, Error)]
pub(crate) enum IngestorStartError {
    #[error("ingestor '{ingestor}' in domain '{domain}'{suffix} is already running", suffix = .unit.label_suffix())]
    AlreadyRunning {
        domain: DomainName,
        ingestor: IngestorName,
        unit: Unit,
    },
    #[error(
        "the execution of domain '{domain}' is not installed while starting ingestor '{ingestor}'"
    )]
    DomainExecutionUnavailable {
        domain: DomainName,
        ingestor: IngestorName,
    },
    #[error("codec '{codec}' in domain '{domain}' is not instantiated")]
    CodecNotInstantiated {
        domain: DomainName,
        codec: CodecName,
    },
    /// The ingestor's programs or routes did not bind; the binding failure is the cause beneath.
    #[error("failed to bind an ingestor or reingestor of domain '{domain}'")]
    Bind { domain: DomainName },
    #[error("failed to initialize ingestor '{ingestor}' in domain '{domain}'")]
    Initialize {
        domain: DomainName,
        ingestor: IngestorName,
    },
}

/// Why an ingestor's client configuration could not resolve or its connector could not be composed
/// or opened on this node. Startup wraps it in [`IngestorStartError::Initialize`]; retirement
/// wraps it in [`RuntimeError::RetireIngestorEntity`].
#[derive(Debug, Error)]
pub(crate) enum IngestorConnectorStartError {
    #[error("failed to resolve the ingestor's client configuration")]
    ClientConfiguration,
    #[error("the node DNS resolver is not installed")]
    NodeDnsUnavailable,
    #[error("missing signaling protocol '{protocol}'")]
    SignalingProtocolMissing { protocol: SignalingProtocolName },
    #[error("endpoint '{endpoint}' is not instantiated")]
    EndpointNotInstantiated { endpoint: EndpointName },
    #[error("endpoint '{endpoint}' requires a signaling protocol for MODE ACK")]
    EndpointAcknowledgementProtocolMissing { endpoint: EndpointName },
    #[error("endpoint '{endpoint}' cannot encode its acknowledgement in the signaling wire format")]
    EndpointAcknowledgementEncoding { endpoint: EndpointName },
    #[error("Kafka DOMAIN offsets are not authoritative on this node")]
    KafkaDomainOffsetsNotAuthoritative,
    #[error("invalid {setting} '{value}'")]
    InvalidDuration {
        setting: DeliverySetting,
        value: String,
        #[source]
        source: nervix_models::DurationTextError,
    },
}

/// A duration an ingestor's delivery mode declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(crate) enum DeliverySetting {
    #[strum(serialize = "ack timeout")]
    AckTimeout,
    #[strum(serialize = "batch timeout")]
    BatchTimeout,
    #[strum(serialize = "retry backoff")]
    RetryBackoff,
    #[strum(serialize = "retry max backoff")]
    RetryMaxBackoff,
}

impl RetiringIngestorInput {
    /// The MQTT topic input the replacement reads, absent when it reads none.
    fn mqtt_replacement(&self) -> Option<&MqttTopicIngestorStartPlan> {
        let IngestorInputPlan::Transport(input) = &self.replacement.as_deref()?.input else {
            return None;
        };
        let IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(source)) = &input.source
        else {
            return None;
        };
        Some(source)
    }

    /// The Pulsar topic input the replacement reads, absent when it reads none.
    fn pulsar_replacement(&self) -> Option<&PulsarTopicIngestorStartPlan> {
        let IngestorInputPlan::Transport(input) = &self.replacement.as_deref()?.input else {
            return None;
        };
        let IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(source)) = &input.source
        else {
            return None;
        };
        Some(source)
    }
}

impl Runtime {
    /// Applies external retirement after the previous execution stopped, in one step where
    /// nothing puts the previous input back, as a domain rebuild. Cleanup is best-effort; a
    /// replacement prepares its own changed sessions before its new input can become ready.
    pub(super) async fn retire_ingestor_input(
        &self,
        retirement: RetiringIngestorInput,
    ) -> error_stack::Result<(), RuntimeError> {
        let IngestorInputPlan::Transport(input) = &retirement.previous.input else {
            return Ok(());
        };
        match &input.source {
            IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(source)) => {
                source
                    .retire_before(
                        self,
                        &retirement.previous.ingestor,
                        &retirement.units,
                        retirement.mqtt_replacement(),
                    )
                    .await
            }
            IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(source)) => {
                self.retire_pulsar_input(source, &retirement).await;
                Ok(())
            }
            IngestorConnectorPlan::Http(HttpIngestorStartPlan::Endpoint(_))
            | IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Endpoint(_)) => {
                self.retire_endpoint_input(&retirement);
                Ok(())
            }
            IngestorConnectorPlan::Http(HttpIngestorStartPlan::Request(_))
            | IngestorConnectorPlan::Kafka(KafkaIngestorStartPlan::Topic(_))
            | IngestorConnectorPlan::Nats(NatsIngestorStartPlan::Subject(_))
            | IngestorConnectorPlan::Nats(NatsIngestorStartPlan::Stream(_))
            | IngestorConnectorPlan::RabbitMq(RabbitMqIngestorStartPlan::Queue(_))
            | IngestorConnectorPlan::Redis(RedisIngestorStartPlan::PubSub(_))
            | IngestorConnectorPlan::Prometheus(PrometheusIngestorStartPlan::Query(_))
            | IngestorConnectorPlan::ZeroMq(ZeroMqIngestorStartPlan::Pull(_))
            | IngestorConnectorPlan::Sqs(SqsIngestorStartPlan::Queue(_))
            | IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Connection(_))
            | IngestorConnectorPlan::Syslog(SyslogIngestorStartPlan::Listener(_)) => Ok(()),
        }
    }

    /// Prepares the replacement of a retiring input while the swap that replaces it can still be
    /// undone. It touches nothing the previous input keeps at its broker, so a failed swap starts
    /// the previous ingestor again on everything it left there: only an MQTT replacement's changed
    /// sessions at a broker session the previous input never connects start clean. The first
    /// failure fails the swap.
    pub(super) async fn prepare_ingestor_replacement(
        &self,
        retirement: &RetiringIngestorInput,
    ) -> error_stack::Result<(), RuntimeError> {
        let IngestorInputPlan::Transport(input) = &retirement.previous.input else {
            return Ok(());
        };
        let IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(source)) = &input.source
        else {
            return Ok(());
        };
        source
            .prepare_replacement(
                self,
                &retirement.previous.ingestor,
                &retirement.units,
                retirement.mqtt_replacement(),
            )
            .await
    }

    /// Retires what a replaced input kept at its broker once the swap that replaced it can no
    /// longer be undone, before its replacement starts. Nothing puts the previous input back any
    /// more, so every failure is reported. An MQTT replacement's instances whose sessions this
    /// cleaned owe them no retirement before they first connect.
    pub(super) async fn retire_replaced_ingestor_input(&self, retirement: RetiringIngestorInput) {
        let IngestorInputPlan::Transport(input) = &retirement.previous.input else {
            return;
        };
        match &input.source {
            IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(source)) => {
                let ingestor = &retirement.previous.ingestor;
                let clean = source
                    .retire_after_swap(
                        self,
                        ingestor,
                        &retirement.units,
                        retirement.mqtt_replacement(),
                    )
                    .await;
                self.complete_mqtt_session_retirements(&ingestor.domain, &ingestor.name, &clean);
            }
            IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(source)) => {
                self.retire_pulsar_input(source, &retirement).await;
            }
            IngestorConnectorPlan::Http(HttpIngestorStartPlan::Endpoint(_))
            | IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Endpoint(_)) => {
                self.retire_endpoint_input(&retirement);
            }
            IngestorConnectorPlan::Http(HttpIngestorStartPlan::Request(_))
            | IngestorConnectorPlan::Kafka(KafkaIngestorStartPlan::Topic(_))
            | IngestorConnectorPlan::Nats(NatsIngestorStartPlan::Subject(_))
            | IngestorConnectorPlan::Nats(NatsIngestorStartPlan::Stream(_))
            | IngestorConnectorPlan::RabbitMq(RabbitMqIngestorStartPlan::Queue(_))
            | IngestorConnectorPlan::Redis(RedisIngestorStartPlan::PubSub(_))
            | IngestorConnectorPlan::Prometheus(PrometheusIngestorStartPlan::Query(_))
            | IngestorConnectorPlan::ZeroMq(ZeroMqIngestorStartPlan::Pull(_))
            | IngestorConnectorPlan::Sqs(SqsIngestorStartPlan::Queue(_))
            | IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Connection(_))
            | IngestorConnectorPlan::Syslog(SyslogIngestorStartPlan::Listener(_)) => {}
        }
    }

    /// Retires, once the swap of `domain` from `previous` to `desired` can no longer be undone and
    /// before that revision publishes and its ingestors start, what each ingestor among `entities`
    /// that `retiring` holds kept at its broker. It first records the retirements the MQTT
    /// ingestors starting on this node owe sessions a preceding input left with another
    /// subscription, and last forgets those `desired` leaves to no instance on this node, a
    /// reassigned ingestor's included.
    pub(super) async fn retire_swapped_ingestor_inputs(
        &self,
        domain: &DomainName,
        previous: Option<&ExecutionRevision>,
        desired: &ExecutionRevision,
        entities: &[NodeRef],
        #[cfg_attr(
            nervix_lint,
            nervix::identity(
                node,
                reason = "identifies the node applying this execution revision"
            )
        )]
        local_node_id: Option<&ClusterNodeName>,
        retiring: &mut BTreeMap<IngestorName, RetiringIngestorInput>,
    ) {
        for entity in entities {
            nervix_primitives::task::consume_budget().await;
            if entity.kind != ModelKind::Ingestor {
                continue;
            }
            let name = IngestorName::from(&entity.identifier);
            if let Some(previous) = previous {
                self.owe_mqtt_session_retirements(domain, &name, previous, desired, local_node_id);
            }
            let Some(retirement) = retiring.remove(&name) else {
                continue;
            };
            Box::pin(self.retire_replaced_ingestor_input(retirement)).await;
        }
        self.keep_mqtt_session_retirements_for(domain, desired, local_node_id);
    }

    /// Removes an owned Pulsar subscription the replacement does not read, best-effort, reporting
    /// a failure.
    async fn retire_pulsar_input(
        &self,
        source: &PulsarTopicIngestorStartPlan,
        retirement: &RetiringIngestorInput,
    ) {
        if let Err(error) = source
            .retire_before(
                self,
                &retirement.previous.ingestor,
                &retirement.units,
                retirement.pulsar_replacement(),
            )
            .await
        {
            self.events().report_error(format!("{error:#}"));
        }
    }

    /// Ends the quiescence an endpoint input's ending units kept.
    fn retire_endpoint_input(&self, retirement: &RetiringIngestorInput) {
        let ingestor = &retirement.previous.ingestor;
        for unit in &retirement.units {
            self.remove_ingestor_quiescence(&ingestor.domain, &ingestor.name, *unit);
        }
    }

    /// Starts one ingestor. Every ingestor takes this path.
    ///
    /// Everything that can fail comes first: the ingestor's dependencies compile and its connector
    /// plan composes into opened connector instances. Only then does the host register the ingestor
    /// and start its tasks, so an ingestor that cannot start leaves nothing running behind it.
    pub(in crate::runtime) async fn start_ingestor(
        &self,
        plan: &IngestorStartPlan,
    ) -> error_stack::Result<(), IngestorStartError> {
        let IngestorStartPlan {
            ingestor,
            input,
            suspended,
        } = plan;
        let quiesce = self.prepare_ingestor_quiescence(&ingestor.domain, ingestor);
        if self.inner.ingestors.contains_key(&ingestor.runtime_key()) {
            return Err(Report::new(IngestorStartError::AlreadyRunning {
                domain: ingestor.domain.clone(),
                ingestor: ingestor.name.clone(),
                unit: ingestor.unit,
            }));
        }

        if *suspended {
            self.prepare_ingestor_readiness(&ingestor.domain, &ingestor.name, ingestor.unit);
            self.record_ingestor_transient_error(
                &ingestor.domain,
                &ingestor.name,
                ingestor.unit,
                "waiting for a coherent external partition topology",
            );
            let (shutdown, _) = watch::channel(false);
            self.inner.ingestors.insert(
                ingestor.runtime_key(),
                IngestorRuntime {
                    shutdown,
                    branched: Vec::new(),
                    tasks: Vec::new(),
                    domain_offsets: None,
                },
            );
            return Ok(());
        }
        let BoundIngestor {
            input,
            dependencies,
        } = self.ingestor_dependencies(ingestor, input).await?;
        let (codec, entity) = match input {
            BoundIngestorInput::Transport { codec, source } => (codec, source),
            BoundIngestorInput::Client { plan, generation } => {
                self.host_client_ingestor(ingestor, &plan, generation, quiesce, dependencies);
                return Ok(());
            }
        };
        let connector = match entity {
            IngestorConnectorPlan::Http(HttpIngestorStartPlan::Request(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Kafka(KafkaIngestorStartPlan::Topic(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Nats(NatsIngestorStartPlan::Subject(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Nats(NatsIngestorStartPlan::Stream(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::RabbitMq(RabbitMqIngestorStartPlan::Queue(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Redis(RedisIngestorStartPlan::PubSub(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Prometheus(PrometheusIngestorStartPlan::Query(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::ZeroMq(ZeroMqIngestorStartPlan::Pull(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Sqs(SqsIngestorStartPlan::Queue(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Http(HttpIngestorStartPlan::Endpoint(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Endpoint(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Connection(plan)) => {
                plan.compose(self, ingestor).await?
            }
            IngestorConnectorPlan::Syslog(SyslogIngestorStartPlan::Listener(plan)) => {
                plan.compose(self, ingestor).await?
            }
        };
        self.host_ingestor(ingestor, quiesce, dependencies, codec, connector);
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_models::{
        ClientPoolBounds, CreateClientMqtt, CreateClientRedis, CreateClientSqs, CreateClientZeroMq,
        HttpIngestor, HttpRequestIngestor, MqttIngestMode, MqttIngestor, MqttQos, MqttSession,
        MqttTopicIngestor, PrometheusIngestor, PrometheusQueryIngestor, RabbitMqIngestMode,
        RabbitMqIngestor, RabbitMqQueueIngestor, RedisIngestor, RedisPubSubIngestMode,
        RedisPubSubIngestor, SqsIngestMode, SqsIngestor, SqsQueueIngestor, ZeroMqIngestMode,
        ZeroMqIngestor, ZeroMqPullIngestor,
    };

    use super::*;
    use crate::runtime::resources::RuntimeResourceError;

    fn named<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: std::fmt::Debug,
    {
        T::try_from(value.to_string()).assured("the fixture name is valid")
    }

    /// The entrypoint plans of a domain whose ingestor `orders_ingestor` reads `entity` through
    /// `client_model` with a JSON codec into the relay `events`.
    fn planned_transport_ingestor(
        domain: &DomainName,
        entity: IngestorEntity,
        client_model: Model,
    ) -> EntrypointPlans {
        planned_entrypoints_for_test(domain, transport_ingestor_models(entity, client_model))
    }

    /// The models of a domain whose ingestor `orders_ingestor` reads `entity` through
    /// `client_model` with a JSON codec into the relay `events`.
    fn transport_ingestor_models(entity: IngestorEntity, client_model: Model) -> Vec<Model> {
        let ingestor = CreateIngestor {
            labels: Default::default(),
            name: named("orders_ingestor"),
            output_routes: with_inherit_all(nervix_models::ProcessorOutputs::single(named(
                "events",
            )))
            .with_flush_policy(FlushPolicy::Immediate)
            .with_branch(OutputBranch::Unbranched),
            input: nervix_models::IngestorInput::Transport(nervix_models::TransportIngestorInput {
                source: entity,
                codec: named("json"),
            }),
            timestamp_source: None,
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        };
        vec![
            Model::Schema(nervix_models::CreateSchema {
                labels: Default::default(),
                name: named("payload"),
                fields: vec![nervix_models::SchemaField {
                    name: named("value"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                }],
            }),
            Model::WireJsonSchema(nervix_models::CreateJsonWireSchema {
                labels: Default::default(),
                name: named("payload_wire"),
                strictness: Default::default(),
                fields: vec![nervix_models::WireSchemaField {
                    name: named("value"),
                    ty: nervix_models::JsonType::String,
                    optional: false,
                }],
            }),
            Model::Codec(nervix_models::CreateCodec {
                labels: Default::default(),
                name: named("json"),
                wire_format: nervix_models::CodecWireFormat::Json {
                    wire_schema: named("payload_wire"),
                },
                schema: named("payload"),
                encoding_rules: Vec::new(),
            }),
            Model::Relay(CreateRelay {
                labels: Default::default(),
                name: named("events"),
                schema: named("payload"),
                buffer: nonzero_ext::nonzero!(2usize),
                branching: nervix_models::RelayBranching::unbranched(),
                materialized_state: None,
            }),
            client_model,
            Model::Ingestor(ingestor),
        ]
    }

    /// The MQTT topic input of an ingestor reading `topic` through `session` at `qos`.
    fn planned_mqtt_input(
        topic: &str,
        session: MqttSession,
        qos: MqttQos,
    ) -> MqttTopicIngestorStartPlan {
        let client_name: ClientName = named("broker");
        let plans = planned_transport_ingestor(
            &named("sessions"),
            IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor {
                client: client_name.clone(),
                topic: nervix_models::MqttTopicFilter::parse(topic).expect("valid topic filter"),
                instances: nervix_models::IngestorInstances::ONE,
                session,
                qos,
                mode: MqttIngestMode::NoAck,
                rate_limits: nervix_models::RateLimits::NONE,
                quiesce: IngestQuiesceMode::Drop,
            })),
            Model::ClientMqtt(CreateClientMqtt {
                labels: Default::default(),
                name: client_name,
                mount: None,
                config: Vec::new(),
            }),
        );
        let plan = plans
            .ingestor(&named("orders_ingestor"), Unit::FIRST_INSTANCE)
            .assured("the fixture plans its one ingestor instance");
        let IngestorInputPlan::Transport(transport) = &plan.input else {
            panic!("the fixture ingestor reads a transport");
        };
        let IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(source)) = &transport.source
        else {
            panic!("the fixture ingestor reads an MQTT topic");
        };
        source.clone()
    }

    #[nervix_primitives::test]
    async fn a_replacement_retires_a_shared_session_first_only_where_it_may_hold_another_subscription()
     {
        let previous = planned_mqtt_input("events", MqttSession::Persistent, MqttQos::Qos1);
        let retopiced = planned_mqtt_input("orders", MqttSession::Persistent, MqttQos::Qos1);
        let requalified = planned_mqtt_input("events", MqttSession::Persistent, MqttQos::Qos2);
        let clean = planned_mqtt_input("orders", MqttSession::Clean, MqttQos::Qos1);
        let clean_previous = planned_mqtt_input("events", MqttSession::Clean, MqttQos::Qos1);

        // A persistent session the preceding input subscribed under another topic or QoS may still
        // hold that subscription.
        assert!(retopiced.may_find_subscription_of(&previous));
        assert!(requalified.may_find_subscription_of(&previous));
        // The same subscription keeps its session, a clean replacement discards any session as it
        // connects, and a clean preceding input leaves none behind.
        assert!(!previous.may_find_subscription_of(&previous));
        assert!(!clean.may_find_subscription_of(&previous));
        assert!(!retopiced.may_find_subscription_of(&clean_previous));
    }

    /// The revision of `domain` whose ingestor `orders_ingestor` reads `topic` through a persistent
    /// session at QoS 1 with two instances, each executing on the node `owners` names for it, or a
    /// revision without that ingestor when there is no topic.
    fn mqtt_revision(
        domain: &DomainName,
        topic: Option<&str>,
        owners: [&str; 2],
    ) -> Arc<ExecutionRevision> {
        let client_name: ClientName = named("broker");
        let client = Model::ClientMqtt(CreateClientMqtt {
            labels: Default::default(),
            name: client_name.clone(),
            mount: None,
            config: Vec::new(),
        });
        let entity = IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor {
            client: client_name,
            topic: nervix_models::MqttTopicFilter::parse(topic.unwrap_or("unread"))
                .expect("valid topic filter"),
            instances: nervix_models::IngestorInstances::Count(nonzero_ext::nonzero!(2u64)),
            session: MqttSession::Persistent,
            qos: MqttQos::Qos1,
            mode: MqttIngestMode::NoAck,
            rate_limits: nervix_models::RateLimits::NONE,
            quiesce: IngestQuiesceMode::Drop,
        }));
        let mut nodes = Vec::new();
        for model in transport_ingestor_models(entity, client) {
            let mut node = scheduled_model(model);
            if node.identity().kind == ModelKind::Ingestor {
                if topic.is_none() {
                    continue;
                }
                for (index, owner) in (0_u64..).zip(owners) {
                    let unit = Unit::try_instance(index)
                        .assured("the fixture index fits the execution-unit range");
                    let scheduled = node
                        .units
                        .get_mut(&unit)
                        .expect("the fixture schedules both instances");
                    scheduled.primary_node = Some(named(owner));
                    scheduled.assigned_nodes = vec![named(owner)];
                }
            }
            nodes.push(node);
        }
        test_execution_revision(domain, nodes)
    }

    #[nervix_primitives::test]
    async fn a_changed_topic_owes_a_retirement_only_for_the_instances_this_node_runs() {
        let runtime = Runtime::with_executor(Executor::default());
        let domain = named("sessions");
        let name = named("orders_ingestor");
        let here: ClusterNodeName = named("node-1");
        let previous = mqtt_revision(&domain, Some("events"), ["node-1", "node-2"]);
        let desired = mqtt_revision(&domain, Some("orders"), ["node-1", "node-2"]);

        // The same subscription leaves the sessions nothing to retire.
        runtime.owe_mqtt_session_retirements(&domain, &name, &previous, &previous, Some(&here));
        assert!(
            runtime
                .outstanding_mqtt_session_retirement(&domain, &name, 0)
                .is_none()
        );

        // Instance 0 runs here and owes its session a retirement; instance 1 owes one on node-2.
        runtime.owe_mqtt_session_retirements(&domain, &name, &previous, &desired, Some(&here));
        let retirement = runtime
            .outstanding_mqtt_session_retirement(&domain, &name, 0)
            .expect("instance 0 owes its session a retirement here");
        assert!(
            runtime
                .outstanding_mqtt_session_retirement(&domain, &name, 1)
                .is_none()
        );

        // The instance completes the retirement it was handed, so the next one owes nothing.
        retirement.complete();
        assert!(
            runtime
                .outstanding_mqtt_session_retirement(&domain, &name, 0)
                .is_none()
        );
    }

    #[nervix_primitives::test]
    async fn an_owed_retirement_is_forgotten_once_no_instance_on_this_node_can_owe_it() {
        let runtime = Runtime::with_executor(Executor::default());
        let domain = named("sessions");
        let name = named("orders_ingestor");
        let here: ClusterNodeName = named("node-1");
        let previous = mqtt_revision(&domain, Some("events"), ["node-1", "node-1"]);
        let desired = mqtt_revision(&domain, Some("orders"), ["node-1", "node-1"]);
        runtime.owe_mqtt_session_retirements(&domain, &name, &previous, &desired, Some(&here));

        // A revision that keeps both instances here keeps both retirements.
        runtime.keep_mqtt_session_retirements_for(&domain, &desired, Some(&here));
        for instance in [0, 1] {
            assert!(
                runtime
                    .outstanding_mqtt_session_retirement(&domain, &name, instance)
                    .is_some()
            );
        }

        // Instance 1 now runs on node-2: its session was retired before it went, or node-2 owes it.
        let moved = mqtt_revision(&domain, Some("orders"), ["node-1", "node-2"]);
        runtime.keep_mqtt_session_retirements_for(&domain, &moved, Some(&here));
        assert!(
            runtime
                .outstanding_mqtt_session_retirement(&domain, &name, 1)
                .is_none()
        );

        // A completed retirement goes, and with it the ingestor's last one here.
        runtime
            .outstanding_mqtt_session_retirement(&domain, &name, 0)
            .expect("instance 0 still owes its session a retirement here")
            .complete();
        runtime.keep_mqtt_session_retirements_for(&domain, &moved, Some(&here));
        assert!(runtime.inner.mqtt_session_retirements.is_empty());

        // A revision that no longer declares the ingestor forgets what it owed.
        runtime.owe_mqtt_session_retirements(&domain, &name, &previous, &desired, Some(&here));
        let removed = mqtt_revision(&domain, None, ["node-1", "node-1"]);
        runtime.keep_mqtt_session_retirements_for(&domain, &removed, Some(&here));
        assert!(runtime.inner.mqtt_session_retirements.is_empty());
    }

    #[nervix_primitives::test]
    async fn a_removed_domain_forgets_only_its_own_owed_retirements() {
        let runtime = Runtime::with_executor(Executor::default());
        let name = named("orders_ingestor");
        let here: ClusterNodeName = named("node-1");
        let removed = named("removed");
        let kept = named("kept");
        for domain in [&removed, &kept] {
            let previous = mqtt_revision(domain, Some("events"), ["node-1", "node-1"]);
            let desired = mqtt_revision(domain, Some("orders"), ["node-1", "node-1"]);
            runtime.owe_mqtt_session_retirements(domain, &name, &previous, &desired, Some(&here));
        }

        runtime.clear_mqtt_session_retirements(&removed);

        assert!(
            runtime
                .outstanding_mqtt_session_retirement(&removed, &name, 0)
                .is_none()
        );
        assert!(
            runtime
                .outstanding_mqtt_session_retirement(&kept, &name, 0)
                .is_some()
        );
    }

    #[nervix_primitives::test]
    async fn a_broker_unit_opens_its_global_index_independently() {
        use nervix_connector_fake::{FakeIngestor, FakeIngestorPlan, IngestorObservations};
        use nervix_primitives::sync::blocking::Mutex;
        let domain = named("instances");
        let plans = planned_transport_ingestor(
            &domain,
            IngestorEntity::Kafka(nervix_models::KafkaIngestor::Topic(
                nervix_models::KafkaTopicIngestor {
                    client: named("broker"),
                    topic: named("events"),
                    offset_mode: nervix_models::KafkaOffsetMode::Domain,
                    instances: nervix_models::IngestorInstances::Count(nonzero_ext::nonzero!(3u64)),
                    mode: nervix_models::KafkaIngestMode::NoAck,
                    rate_limits: nervix_models::RateLimits::NONE,
                    quiesce: IngestQuiesceMode::Suspend,
                },
            )),
            Model::ClientKafka(nervix_models::CreateClientKafka {
                labels: Default::default(),
                name: named("broker"),
                mount: None,
                config: vec![],
            }),
        );
        let runtime = Runtime::with_executor(Executor::default());
        for index in [2, 0] {
            let connector = FakeIngestorPlan::new(
                [(
                    index,
                    Ok(FakeIngestor::empty(Arc::new(Mutex::new(
                        IngestorObservations::default(),
                    )))),
                )]
                .into_iter()
                .collect(),
            );
            let plan = plans
                .ingestor(
                    &named("orders_ingestor"),
                    Unit::try_instance(index)
                        .assured("the fixture index fits the execution-unit range"),
                )
                .expect("each global unit has its plan");
            let start = connector::BrokerIngestorStart {
                connector,
                instances: nonzero_ext::nonzero!(3u64),
                acknowledgement: nervix_models::IngestAcknowledgement::Unacknowledged,
                buffered_intake: true,
                flush_each_intake: true,
                client_mounts: Vec::new(),
            };
            // Only the selected global slot exists, even when index 2 is the first local unit.
            start
                .open::<FakeIngestor>(&runtime, &plan.ingestor)
                .await
                .expect("only the selected global index opens");
        }
    }

    #[nervix_primitives::test]
    async fn an_ingestor_without_its_domain_execution_names_the_missing_execution() {
        let runtime = Runtime::default();
        let domain: DomainName = named("sales");
        let client_name: ClientName = named("upstream");
        let plans = planned_transport_ingestor(
            &domain,
            IngestorEntity::Http(HttpIngestor::Request(HttpRequestIngestor {
                client: client_name.clone(),
                method: "GET".to_owned(),
                headers: Vec::new(),
                max_body_size: None,
                every: "1s"
                    .parse()
                    .assured("the fixture cadence is a positive duration"),
                mode: nervix_models::HttpRequestIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            })),
            Model::ClientHttp(CreateClientHttp {
                labels: Default::default(),
                name: client_name,
                mount: None,
                config: Vec::new(),
            }),
        );
        let plan = plans
            .ingestors()
            .find(|plan| plan.ingestor.name == named::<IngestorName>("orders_ingestor"))
            .assured("the fixture schedules the ingestor named orders_ingestor");

        let report = runtime
            .start_ingestor(plan)
            .await
            .expect_err("an ingestor binds its dependencies from its domain's execution");

        assert!(
            matches!(
                report.current_context(),
                IngestorStartError::DomainExecutionUnavailable { domain: failed, ingestor }
                    if *failed == domain && ingestor.as_str() == "orders_ingestor"
            ),
            "{report:?}"
        );
        let message = "the execution of domain 'sales' is not installed while starting ingestor \
                       'orders_ingestor'";
        assert_eq!(format!("{report:#}"), message);
    }

    #[test]
    fn ingestor_start_failures_keep_their_diagnostic_text() {
        let domain: DomainName = named("sales");
        let ingestor: IngestorName = named("orders_ingestor");
        let initialize = || IngestorStartError::Initialize {
            domain: domain.clone(),
            ingestor: ingestor.clone(),
        };
        let cases = [
            (
                Report::new(IngestorStartError::AlreadyRunning {
                    domain: domain.clone(),
                    ingestor: ingestor.clone(),
                    unit: Unit::try_instance(2)
                        .assured("the fixture index fits the execution-unit range"),
                }),
                "ingestor 'orders_ingestor' in domain 'sales' instance 2 is already running",
            ),
            (
                Report::new(IngestorStartError::CodecNotInstantiated {
                    domain: domain.clone(),
                    codec: named("json"),
                }),
                "codec 'json' in domain 'sales' is not instantiated",
            ),
            (
                Report::new(IngestorStartError::Bind {
                    domain: domain.clone(),
                }),
                "failed to bind an ingestor or reingestor of domain 'sales'",
            ),
            (
                Report::new(IngestorConnectorStartError::SignalingProtocolMissing {
                    protocol: named("handshake"),
                })
                .change_context(initialize()),
                "failed to initialize ingestor 'orders_ingestor' in domain 'sales': missing \
                 signaling protocol 'handshake'",
            ),
            (
                Report::new(IngestorConnectorStartError::EndpointNotInstantiated {
                    endpoint: named("events_endpoint"),
                })
                .change_context(initialize()),
                "failed to initialize ingestor 'orders_ingestor' in domain 'sales': endpoint \
                 'events_endpoint' is not instantiated",
            ),
            (
                Report::new(IngestorConnectorStartError::KafkaDomainOffsetsNotAuthoritative)
                    .change_context(initialize()),
                "failed to initialize ingestor 'orders_ingestor' in domain 'sales': Kafka DOMAIN \
                 offsets are not authoritative on this node",
            ),
        ];
        for (report, message) in cases {
            assert_eq!(format!("{report:#}"), message);
        }
    }

    #[nervix_primitives::test]
    async fn ingestors_that_resolve_names_report_missing_node_dns_as_start_failure() {
        let runtime = Runtime::default();
        let domain: DomainName = named("sales");
        let client_name: ClientName = named("upstream");
        let cadence: nervix_models::DomainClockPeriod = "1s"
            .parse()
            .assured("the fixture cadence is a positive duration");
        for (source, client_model) in [
            (
                IngestorEntity::Http(HttpIngestor::Request(HttpRequestIngestor {
                    client: client_name.clone(),
                    method: "GET".to_owned(),
                    headers: Vec::new(),
                    max_body_size: None,
                    every: cadence,
                    mode: nervix_models::HttpRequestIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Suspend,
                })),
                Model::ClientHttp(CreateClientHttp {
                    labels: Default::default(),
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestorEntity::Prometheus(PrometheusIngestor::Query(PrometheusQueryIngestor {
                    client: client_name.clone(),
                    query: "up".to_string(),
                    max_body_size: None,
                    every: cadence,
                    mode: nervix_models::PrometheusIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Suspend,
                })),
                Model::ClientPrometheus(CreateClientPrometheus {
                    labels: Default::default(),
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestorEntity::RabbitMq(RabbitMqIngestor::Queue(RabbitMqQueueIngestor {
                    client: client_name.clone(),
                    queue: named("events"),
                    instances: nervix_models::IngestorInstances::ONE,
                    mode: RabbitMqIngestMode::AckSequential {
                        timeout: "5s".to_string(),
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    rate_limits: nervix_models::RateLimits::NONE,
                    quiesce: IngestQuiesceMode::Suspend,
                })),
                Model::ClientRabbitMq(CreateClientRabbitMq {
                    labels: Default::default(),
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestorEntity::Sqs(SqsIngestor::Queue(SqsQueueIngestor {
                    client: client_name.clone(),
                    queue: named("events"),
                    instances: nervix_models::IngestorInstances::ONE,
                    mode: SqsIngestMode::AckSequential {
                        timeout: "5s".to_string(),
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    rate_limits: nervix_models::RateLimits::NONE,
                    quiesce: IngestQuiesceMode::Suspend,
                })),
                Model::ClientSqs(CreateClientSqs {
                    labels: Default::default(),
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestorEntity::Redis(RedisIngestor::PubSub(RedisPubSubIngestor {
                    client: client_name.clone(),
                    subscription: nervix_models::RedisSubscriptionTarget::Channel(named("events")),
                    mode: RedisPubSubIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Drop,
                })),
                Model::ClientRedis(CreateClientRedis {
                    labels: Default::default(),
                    name: client_name.clone(),
                    pool: ClientPoolBounds::new(0, nonzero_ext::nonzero!(1u32))
                        .assured("zero is below one"),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor {
                    client: client_name.clone(),
                    topic: nervix_models::MqttTopicFilter::parse("events")
                        .expect("valid topic filter"),
                    instances: nervix_models::IngestorInstances::ONE,
                    session: MqttSession::Clean,
                    qos: MqttQos::Qos0,
                    mode: MqttIngestMode::NoAck,
                    rate_limits: nervix_models::RateLimits::NONE,
                    quiesce: IngestQuiesceMode::Drop,
                })),
                Model::ClientMqtt(CreateClientMqtt {
                    labels: Default::default(),
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
        ] {
            let plans = planned_transport_ingestor(&domain, source, client_model);
            let plan = plans
                .ingestors()
                .find(|plan| plan.ingestor.name == named::<IngestorName>("orders_ingestor"))
                .assured("the fixture schedules the ingestor named orders_ingestor");
            let IngestorInputPlan::Transport(transport) = &plan.input else {
                panic!("the fixture ingestor reads a transport");
            };
            let result = match transport.source.clone() {
                IngestorConnectorPlan::Http(HttpIngestorStartPlan::Request(source)) => {
                    source.compose(&runtime, &plan.ingestor).await
                }
                IngestorConnectorPlan::Prometheus(PrometheusIngestorStartPlan::Query(source)) => {
                    source.compose(&runtime, &plan.ingestor).await
                }
                IngestorConnectorPlan::RabbitMq(RabbitMqIngestorStartPlan::Queue(source)) => {
                    source.compose(&runtime, &plan.ingestor).await
                }
                IngestorConnectorPlan::Redis(RedisIngestorStartPlan::PubSub(source)) => {
                    source.compose(&runtime, &plan.ingestor).await
                }
                IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(source)) => {
                    source.compose(&runtime, &plan.ingestor).await
                }
                IngestorConnectorPlan::Sqs(SqsIngestorStartPlan::Queue(source)) => {
                    source.compose(&runtime, &plan.ingestor).await
                }
                _ => panic!("the fixture only includes ingestors that resolve names"),
            };
            let Err(report) = result else {
                panic!(
                    "an ingestor that resolves names must fail without node DNS before it opens"
                );
            };
            let IngestorStartError::Initialize { domain, ingestor } = report.current_context()
            else {
                panic!("the failure must name the ingestor that could not start: {report:?}");
            };
            assert_eq!(domain.as_str(), "sales");
            assert_eq!(ingestor.as_str(), "orders_ingestor");
            assert!(
                matches!(
                    report.downcast_ref::<IngestorConnectorStartError>(),
                    Some(IngestorConnectorStartError::NodeDnsUnavailable)
                ),
                "the missing resolver must be the typed cause: {report:?}"
            );
            assert_eq!(
                format!("{report:#}"),
                "failed to initialize ingestor 'orders_ingestor' in domain 'sales': the node DNS \
                 resolver is not installed"
            );
        }
    }

    /// The ZeroMQ client `upstream`, reading `addr` with the optional resource mount.
    fn zeromq_client(mount: Option<ClientResourceMount>, addr: &str) -> CreateClientZeroMq {
        CreateClientZeroMq {
            labels: Default::default(),
            name: named("upstream"),
            mount,
            config: vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: addr.to_string(),
            }],
        }
    }

    /// Composes the ZeroMQ ingestor `ingestor` of domain `sales`, which reads `client`.
    async fn compose_zeromq_ingestor(
        runtime: &Runtime,
        client: CreateClientZeroMq,
    ) -> error_stack::Result<connector::IngestorStart, IngestorStartError> {
        let domain: DomainName = named("sales");
        let pull = IngestorEntity::ZeroMq(ZeroMqIngestor::Pull(ZeroMqPullIngestor {
            client: client.name.clone(),
            mode: ZeroMqIngestMode::NoAck,
            rate_limits: nervix_models::RateLimits::NONE,
            quiesce: IngestQuiesceMode::Suspend,
        }));
        let plans = planned_transport_ingestor(&domain, pull, Model::ClientZeroMq(client));
        let plan = plans
            .ingestors()
            .find(|plan| plan.ingestor.name == named::<IngestorName>("orders_ingestor"))
            .assured("the fixture schedules the ingestor named orders_ingestor");
        let IngestorInputPlan::Transport(transport) = &plan.input else {
            panic!("the fixture ingestor reads a transport");
        };
        let IngestorConnectorPlan::ZeroMq(ZeroMqIngestorStartPlan::Pull(zeromq)) =
            transport.source.clone()
        else {
            panic!("the fixture ingestor reads a ZeroMQ client");
        };
        zeromq.compose(runtime, &plan.ingestor).await
    }

    #[nervix_primitives::test]
    async fn a_zeromq_ingestor_resolves_its_client_configuration_before_it_opens() {
        let runtime = Runtime::default();
        let feed = ClientResourceMount {
            resource: named("feed"),
            version: 1,
        };
        let cases = [
            (
                zeromq_client(None, "ipc://{{ feed }}/ingest.sock"),
                concat!(
                    "client configuration entry 'addr' has an invalid template: ",
                    "failed to render client config template for key 'addr': ",
                    "render error: not found in this scope",
                ),
            ),
            (
                zeromq_client(Some(feed), "ipc://{{ feed }}/ingest.sock"),
                "client resource 'feed' in domain 'sales' requires a resource store",
            ),
        ];
        for (client, cause) in cases {
            let Err(report) = compose_zeromq_ingestor(&runtime, client).await else {
                panic!("a ZeroMQ ingestor must resolve its client configuration before it opens");
            };
            assert!(
                matches!(
                    report.current_context(),
                    IngestorStartError::Initialize { domain, ingestor }
                        if domain.as_str() == "sales" && ingestor.as_str() == "orders_ingestor"
                ),
                "the failure must name the ingestor that could not start: {report:?}"
            );
            assert!(
                report.contains::<RuntimeResourceError>(),
                "the client configuration failure must be the typed cause: {report:?}"
            );
            assert_eq!(
                format!("{report:#}"),
                format!(
                    "failed to initialize ingestor 'orders_ingestor' in domain 'sales': {cause}"
                )
            );
        }
    }

    #[nervix_primitives::test]
    async fn a_zeromq_ingestor_renders_its_address_over_a_mount_it_keeps() {
        let store_root = tempfile::tempdir().expect("resource store tempdir");
        let content_root = tempfile::tempdir().expect("resource content tempdir");
        std::fs::write(content_root.path().join("feed.txt"), "feed")
            .expect("resource file should be written");
        let store = ResourceStore::open(store_root.path(), Executor::default())
            .expect("resource store should open");
        store
            .install_from_directory(
                ResourceId::new(named("sales"), named("feed"), 1),
                content_root.path(),
                named("node-1"),
                Timestamp::from_unix_nanos(0),
            )
            .await
            .expect("resource version should install");
        let runtime = Runtime::default();
        runtime.attach_resource_store(StdArc::new(store));
        let feed = ClientResourceMount {
            resource: named("feed"),
            version: 1,
        };

        let composed = compose_zeromq_ingestor(
            &runtime,
            zeromq_client(Some(feed), "ipc://{{ feed }}/ingest.sock"),
        )
        .await;

        let start = match composed {
            Ok(start) => start,
            Err(report) => panic!("a ZeroMQ source over an installed mount composes: {report:?}"),
        };
        assert_eq!(
            start.client_mounts.len(),
            1,
            "the source keeps its client mount for as long as its instance runs"
        );
    }
}
