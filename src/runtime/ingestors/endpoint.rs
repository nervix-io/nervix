//! The endpoint ingestor: the one request-scoped ingestor, which the node's own HTTP listener feeds.
//!
//! Layer: data plane.
//!
//! - **Owns.** Resolving the routes an endpoint ingestor receives requests on, and binding them to
//!   the host's request intake for exactly as long as the ingestor runs.
//! - **Depends on.** The connector ingestor contract, the domain's instantiated endpoint routes, and
//!   the host's endpoint binding table.
//! - **Must not know.** Request admission, the endpoint buffer, rejection with a retry delay, or
//!   dispatch, all of which the host owns; Plexus parsing, registry validation, or placement
//!   computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use async_trait::async_trait;
use error_stack::ResultExt as _;
use nervix_connector::{
    IngestorAckPolicy, IngestorConnector, IngestorHost, IngestorHostServices as _, IngestorPlan,
    IngestorResult,
};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{IngestorInstance, IngestorStart, RuntimeIngestorHost, run_request_ingestor},
};

/// The routes one endpoint ingestor receives requests on.
pub(in crate::runtime) struct EndpointIntakePlan {
    runtime: Runtime,
    routes: Vec<HttpRouteKey>,
    mode: EndpointRequestPlan,
}

/// An endpoint intake: the binding between the routes it receives requests on and the
/// host's request intake.
///
/// A request is admitted and dispatched by the host on the request path, so the ingestor reads
/// nothing itself. Starting it binds its routes; closing it, or dropping it when its task ends any
/// other way, unbinds them, so a stopped ingestor's routes never keep receiving requests.
pub(in crate::runtime) struct EndpointIntake {
    runtime: Runtime,
    routes: Vec<HttpRouteKey>,
    mode: EndpointRequestPlan,
    /// The ingestor whose intake the routes are bound to, while they are bound.
    bound: Option<EndpointBinding<EndpointIngestBinding>>,
}

macro_rules! endpoint_entity {
    ($name:ident, $connector:ident, $entity:ident) => {
        pub(in crate::runtime) struct $name(EndpointIntake);
        #[async_trait]
        impl IngestorConnector for $name {
            const KIND: nervix_models::IngestorEntityKind =
                nervix_models::IngestorEntityKind::$connector(nervix_models::$entity::Endpoint);
            type Plan = EndpointIntakePlan;
            async fn open(plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
                Ok(Self(EndpointIntake {
                    runtime: plan.runtime.clone(),
                    routes: plan.routes.clone(),
                    mode: plan.mode.clone(),
                    bound: None,
                }))
            }
            async fn close(&mut self) -> IngestorResult<()> {
                self.0.unbind();
                Ok(())
            }
        }
        impl IngestorInstance for $name {
            fn start(
                mut self: Box<Self>,
                host: RuntimeIngestorHost,
                shutdown: watch::Receiver<bool>,
            ) -> BoxFuture<'static, ()> {
                let request = EndpointRequestPolicy::from(&self.0.mode);
                self.0.bind(host.request_intake(request));
                host.mark_ready();
                Box::pin(run_request_ingestor(
                    *self,
                    IngestorHost::new(host),
                    shutdown,
                ))
            }
        }
    };
}
endpoint_entity!(HttpEndpointIngestor, Http, HttpIngestorEntity);
endpoint_entity!(
    WebsocketsEndpointIngestor,
    Websockets,
    WebsocketsIngestorEntity
);
const _: () = assert!(matches!(
    HttpEndpointIngestor::KIND,
    nervix_models::IngestorEntityKind::Http(nervix_models::HttpIngestorEntity::Endpoint)
));
const _: () = assert!(matches!(
    WebsocketsEndpointIngestor::KIND,
    nervix_models::IngestorEntityKind::Websockets(
        nervix_models::WebsocketsIngestorEntity::Endpoint
    )
));

impl EndpointIntake {
    /// Binds every route to `intake`, so the requests arriving on it enter the host.
    fn bind(&mut self, intake: EndpointIngestBinding) {
        let identity = DomainNodeRef::node_in(
            intake.domain.clone(),
            ModelKind::Ingestor,
            intake.ingestor.clone(),
        );
        self.bound = Some(self.runtime.inner.endpoint_intake_routes.bind(
            identity,
            intake,
            &self.routes,
        ));
    }

    fn unbind(&mut self) {
        let Some(binding) = self.bound.take() else {
            return;
        };
        self.runtime
            .inner
            .endpoint_intake_routes
            .unbind(&binding, &self.routes);
    }
}

impl Drop for EndpointIntake {
    fn drop(&mut self) {
        self.unbind();
    }
}

impl EndpointIntakeStartPlan {
    /// Resolves the routes the endpoint publishes, which its ingestor binds when the host starts it.
    async fn compose<C>(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError>
    where
        C: IngestorConnector<Plan = EndpointIntakePlan> + IngestorInstance,
    {
        let EndpointIntakeStartPlan { endpoint, mode } = self;
        let route = {
            let Some(execution) = runtime.inner.executions.get(&ingestor.domain) else {
                return Err(Report::new(
                    IngestorStartError::DomainExecutionUnavailable {
                        domain: ingestor.domain.clone(),
                        ingestor: ingestor.name.clone(),
                    },
                ));
            };
            execution.endpoint_routes.get(&endpoint).cloned()
        };
        let Some(route) = route else {
            return Err(ingestor.connector_start_failure(
                IngestorConnectorStartError::EndpointNotInstantiated { endpoint },
            ));
        };
        if let EndpointRequestPlan::Ack { retry_after, .. } = &mode
            && route.kind.endpoint_type() == EndpointType::Websockets
        {
            let Some(protocol) = route.kind.signaling_protocol() else {
                return Err(ingestor.connector_start_failure(
                    IngestorConnectorStartError::EndpointAcknowledgementProtocolMissing {
                        endpoint,
                    },
                ));
            };
            for outcome in EndpointRequestOutcome::replies() {
                let reply = EndpointRequestReply {
                    outcome,
                    retry_after: *retry_after,
                };
                protocol
                    .encode_acknowledgement(reply.value())
                    .change_context_lazy(|| {
                        IngestorConnectorStartError::EndpointAcknowledgementEncoding {
                            endpoint: endpoint.clone(),
                        }
                    })
                    .change_context_lazy(|| ingestor.initialize_failure())?;
            }
        }
        let routes = route
            .hostnames
            .iter()
            .map(|host| HttpRouteKey {
                host: host.clone(),
                path: route.path.clone(),
            })
            .collect();

        let acknowledgement = IngestorAckPolicy::None;
        let plan = IngestorPlan {
            connector: EndpointIntakePlan {
                runtime: runtime.clone(),
                routes,
                mode,
            },
            capabilities: ingestor
                .ingestor_capabilities(NonZeroU64::MIN, acknowledgement.support()),
            acknowledgement,
        };
        let connector = C::open(&plan.connector, 0)
            .await
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let instance: Box<dyn IngestorInstance> = Box::new(connector);
        Ok(IngestorStart {
            instance,
            companions: Vec::new(),
            // Requests never pass through the ingestor loop's intake: the request path admits
            // them. What the endpoint buffer retained replays one request per ingest group.
            buffered_intake: false,
            flush_each_intake: true,
            // The request path answers a body the extension workers cannot unfold now with a
            // refusal its sender retries, so no request reaches the loop's live intake.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: Vec::new(),
            connector_label: C::KIND.connector().keyword().to_ascii_lowercase(),
            domain_offsets: None,
        })
    }
}

impl HttpEndpointIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        self.0
            .compose::<HttpEndpointIngestor>(runtime, ingestor)
            .await
    }
}
impl WebsocketsEndpointIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        self.0
            .compose::<WebsocketsEndpointIngestor>(runtime, ingestor)
            .await
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
pub(super) mod tests {
    use std::collections::BTreeMap;

    use futures_util::FutureExt as _;
    use nervix_connector::{IngestMetadataRow, NoIngestHeaders};
    use nervix_execution::CpuClass;
    use nervix_models::{
        ClusterNodeName, ClusterSchedule, CodecJaqFormat, CodecJaqTransformations, CodecName,
        CodecWireFormat, CreateCodec, CreateEndpoint, CreateJsonWireSchema, CreateRelay,
        CreateSchema, CreateVhost, DomainConfig, DomainPace, DomainState, DomainStatus,
        EndpointIngestMode, EndpointName, HttpEndpointIngestor, JsonType, OutputBranch,
        ParseAsType, ProcessorOutputs, RelayBranching, RelayName, SchemaField, SchemaName,
        VhostName, WireSchemaField, WireSchemaName,
    };
    use nervix_primitives::sync::atomic::{AtomicUsize, Ordering};
    use nonzero_ext::nonzero;

    use super::*;
    use crate::{
        metrics::{EndpointIntakeOutcome, IngestorEndpointMetrics},
        runtime::{
            endpoint::{EndpointAdmission, EndpointDispatchOutcome},
            endpoint_acknowledgement::{EndpointAdmissionRefusal, EndpointInputRefusal},
        },
    };

    /// The request children the fixture's node registered for its endpoint ingestor `ingestor`.
    fn endpoint_requests(
        runtime: &Runtime,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> IngestorEndpointMetrics {
        runtime
            .inner
            .metrics
            .register_ingestor_reads(
                domain,
                ingestor,
                Unit::SINGLE,
                None,
                nervix_models::IngestorEntityKind::Http(
                    nervix_models::HttpIngestorEntity::Endpoint,
                ),
            )
            .endpoint_requests()
            .assured("an endpoint ingestor's unit registers its request children")
    }

    /// How many requests an endpoint ingestor counted under each outcome.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct Counted {
        accepted: u64,
        busy: u64,
        refused: u64,
        not_admitted: u64,
        decode_rejected: u64,
        processing_failed: u64,
    }

    impl Counted {
        fn of(requests: &IngestorEndpointMetrics) -> Self {
            Self {
                accepted: requests.total(EndpointIntakeOutcome::Accepted),
                busy: requests.total(EndpointIntakeOutcome::Busy),
                refused: requests.total(EndpointIntakeOutcome::Refused),
                not_admitted: requests.total(EndpointIntakeOutcome::NotAdmitted),
                decode_rejected: requests.total(EndpointIntakeOutcome::DecodeRejected),
                processing_failed: requests.total(EndpointIntakeOutcome::ProcessingFailed),
            }
        }
    }

    /// The JSON codec of the fixture's `event_wire` wire schema.
    fn event_wire_format() -> CodecWireFormat {
        CodecWireFormat::Json {
            wire_schema: named::<WireSchemaName>("event_wire"),
        }
    }

    /// The quiesce that refuses requests while an ingestor is quiesced, naming a one-second retry.
    fn reject_quiesce() -> IngestQuiesceMode {
        IngestQuiesceMode::Reject {
            retry_after: "1s".to_string(),
        }
    }

    /// A running domain whose one endpoint ingestor reads `/events` on `edge.example.com` through
    /// its `event_wire` JSON wire schema.
    async fn runtime_with_endpoint_ingestor(
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> Runtime {
        start_endpoint_ingestor(Runtime::default(), domain, ingestor, event_wire_format()).await
    }

    /// One endpoint ingestor a fixture starts: its name, its mode and its quiesce.
    struct EndpointIngestorFixture<'a> {
        name: &'a IngestorName,
        mode: EndpointIngestMode,
        quiesce: IngestQuiesceMode,
    }

    /// Starts, on `runtime`, a running domain whose one endpoint ingestor reads `/events` on
    /// `edge.example.com` through a codec of `wire_format`.
    pub(in crate::runtime::ingestors) async fn start_endpoint_ingestor(
        runtime: Runtime,
        domain: &DomainName,
        ingestor: &IngestorName,
        wire_format: CodecWireFormat,
    ) -> Runtime {
        start_endpoint_ingestors(
            runtime,
            domain,
            &[EndpointIngestorFixture {
                name: ingestor,
                mode: EndpointIngestMode::NoAck,
                quiesce: IngestQuiesceMode::EndpointBuffer {
                    max_size: "1MiB".to_string(),
                },
            }],
            wire_format,
        )
        .await
    }

    /// Starts, on `runtime`, a running domain whose endpoint ingestors `ingestors` all read `/events`
    /// on `edge.example.com` through one codec of `wire_format`, in the order given.
    async fn start_endpoint_ingestors(
        runtime: Runtime,
        domain: &DomainName,
        ingestors: &[EndpointIngestorFixture<'_>],
        wire_format: CodecWireFormat,
    ) -> Runtime {
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                },
                status: DomainStatus::Running,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
                stop_cause: None,
            },
        )]));

        let schema = named::<SchemaName>("event");
        let wire_schema = named::<WireSchemaName>("event_wire");
        let codec = named::<CodecName>("event_json");
        let relay = named::<RelayName>("events");
        let vhost = named::<VhostName>("edge");
        let endpoint = named::<EndpointName>("event_ingress");
        let mut models = vec![
            scheduled_model(Model::Schema(CreateSchema {
                name: schema.clone(),
                fields: vec![SchemaField {
                    name: named("user_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            scheduled_model(Model::WireJsonSchema(CreateJsonWireSchema {
                name: wire_schema,
                strictness: Default::default(),
                fields: vec![WireSchemaField {
                    name: named("user_id"),
                    ty: JsonType::Integer,
                    optional: false,
                }],
            })),
            scheduled_model(Model::Codec(CreateCodec {
                name: codec.clone(),
                wire_format,
                schema: schema.clone(),
                encoding_rules: Vec::new(),
            })),
            scheduled_model(Model::Relay(CreateRelay {
                name: relay.clone(),
                schema,
                buffer: nonzero!(2usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            })),
            scheduled_model(Model::Vhost(CreateVhost {
                name: vhost.clone(),
                hostnames: vec!["edge.example.com".to_string()],
                tls: None,
            })),
            scheduled_model(Model::Endpoint(CreateEndpoint {
                name: endpoint.clone(),
                on_vhost: vhost,
                path: "/events".to_string(),
                kind: nervix_models::EndpointKind::Http,
            })),
        ];
        for fixture in ingestors {
            models.push(scheduled_model(Model::Ingestor(CreateIngestor {
                name: fixture.name.clone(),
                output_routes: with_inherit_all(ProcessorOutputs::single(relay.clone()))
                    .with_flush_policy(FlushPolicy::Immediate)
                    .with_branch(OutputBranch::Unbranched),
                input: nervix_models::IngestorInput::Transport(
                    nervix_models::TransportIngestorInput {
                        source: IngestorEntity::from(HttpEndpointIngestor {
                            endpoint: endpoint.clone(),
                            mode: fixture.mode.clone(),
                            quiesce: fixture.quiesce.clone(),
                        }),
                        codec: codec.clone(),
                    },
                ),
                timestamp_source: None,
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: None,
            })));
        }
        runtime
            .apply_cluster_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &ClusterSchedule::from_iter([DomainSchedule::new(
                    domain.clone(),
                    models,
                    Vec::new(),
                )]),
            )
            .await
            .expect("the endpoint ingestors' schedule applies");
        runtime
    }

    #[nervix_primitives::test]
    async fn endpoint_requests_reuse_bound_routes() {
        struct RouteReferences {
            routes: Arc<BoundIngestorRoutes>,
            observed: AtomicUsize,
        }

        impl nervix_connector::IngestMessageHeaders for RouteReferences {
            fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {
                self.observed
                    .store(Arc::strong_count(&self.routes), Ordering::Relaxed);
            }
        }

        let domain = domain("endpoint_reuse");
        let ingestor = named::<IngestorName>("event_ingestor");
        let runtime = runtime_with_endpoint_ingestor(&domain, &ingestor).await;
        let route = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("the fixture installs its endpoint route");
        let routes = route.bindings()[0]
            .intake()
            .as_deref()
            .assured("the fixture starts its endpoint ingestor")
            .output_routes
            .clone();
        let probe = RouteReferences {
            routes,
            observed: AtomicUsize::new(0),
        };
        let references = Arc::strong_count(&probe.routes);
        for _ in 0..4 {
            let outcome = route
                .dispatch(
                    &runtime,
                    br#"{"user_id":7}"#,
                    IngestMetadataRow::headers(&probe, &nervix_connector::NoIngestMetadata(0)),
                    &CancellationToken::new(),
                )
                .await;
            assert!(outcome.is_accepted());
            assert_eq!(
                probe.observed.load(Ordering::Relaxed),
                references,
                "request routing must borrow the prepared routes without cloning the intake"
            );
        }
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }

    #[cfg(feature = "benchmarks")]
    #[nervix_primitives::test]
    #[ignore = "same-host routing and allocation measurement"]
    async fn endpoint_routing_cost() {
        let domain = domain("endpoint_cost");
        let ingestor = named::<IngestorName>("event_ingestor");
        let runtime = runtime_with_endpoint_ingestor(&domain, &ingestor).await;
        let allocated = tikv_jemalloc_ctl::thread::allocatedp::read()
            .assured("the server test allocator uses jemalloc");
        for sample in 0..5 {
            let before = allocated.get();
            let started = nervix_primitives::time::Instant::now();
            for _ in 0..10_000 {
                nervix_primitives::task::consume_budget().await;
                let route = runtime
                    .resolve_endpoint("edge.example.com", "/events")
                    .into_route()
                    .assured("the benchmark route remains installed");
                std::hint::black_box(route.endpoint_type());
                std::hint::black_box(route.refusal());
            }
            println!(
                "endpoint-routing sample={sample} ns_per_request={} allocated_bytes_per_request={}",
                started.elapsed().as_nanos() / 10_000,
                allocated
                    .get()
                    .checked_sub(before)
                    .assured("the sample allocation counter does not wrap")
                    / 10_000
            );
        }
        let retained = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("the benchmark route remains installed");
        for sample in 0..5 {
            let before = allocated.get();
            let started = nervix_primitives::time::Instant::now();
            for _ in 0..10_000 {
                nervix_primitives::task::consume_budget().await;
                std::hint::black_box(retained.refusal());
            }
            println!(
                "endpoint-retained sample={sample} ns_per_frame={} allocated_bytes_per_frame={}",
                started.elapsed().as_nanos() / 10_000,
                allocated
                    .get()
                    .checked_sub(before)
                    .assured("the sample allocation counter does not wrap")
                    / 10_000
            );
        }
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }

    /// A body its codec refuses is accepted and consumed as a decode rejection, which the
    /// ingestor's `ON GENERAL ERROR LOG` reports with the codec's whole chain, counts and describes.
    #[nervix_primitives::test]
    async fn a_body_its_codec_refuses_is_consumed_as_a_decode_rejection() {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_ingestor");
        let runtime = runtime_with_endpoint_ingestor(&domain, &ingestor).await;
        let mut events = runtime.events().subscribe();

        let route = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("the fixture installs its endpoint route");
        let outcome = route
            .dispatch(
                &runtime,
                br#"{"user_id":"seven"}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(0),
                ),
                &CancellationToken::new(),
            )
            .await;

        assert!(outcome.is_accepted());
        let RuntimeEvent::Error(message) = events
            .recv()
            .await
            .expect("the refused body is reported to the node's observers");
        let described = runtime
            .describe_local_ingestor(
                &domain,
                &ingestor,
                nervix_models::Unit::SINGLE,
                nervix_models::IngestorEntityKind::Http(
                    nervix_models::HttpIngestorEntity::Endpoint,
                ),
            )
            .expect("the fixture's ingestor describes itself");
        let rejection = described
            .last_decode_rejection
            .assured("the refused body is the ingestor's last decode rejection");
        assert_eq!(
            message,
            format!(
                "ingestor 'event_ingestor' general error in domain 'default': decode rejection \
                 {rejection}"
            )
        );
        assert!(
            rejection.to_string().ends_with(
                ": codec 'event_json' refused the payload: codec 'event_json' failed to parse \
                 field 'user_id': expected Integer, found a JSON string"
            ),
            "the rejection renders the codec's whole chain: {rejection}"
        );
        assert_eq!(described.decode_rejections_total, 1);
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }

    /// The header values of a request that are not UTF-8 count on the counter the route's bound
    /// ingestor resolved when it started, and a request with none counts nothing.
    #[nervix_primitives::test]
    async fn undecodable_header_values_count_on_the_bound_ingestor() {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_ingestor");
        let runtime = runtime_with_endpoint_ingestor(&domain, &ingestor).await;
        let route = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("the fixture installs its endpoint route");
        let dispatcher = runtime.inner.remote_dispatcher.load();
        let dropped = runtime.inner.metrics.ingestor_header_values_dropped(
            &domain,
            &ingestor,
            Unit::SINGLE,
            dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
            nervix_models::IngestorEntityKind::Http(nervix_models::HttpIngestorEntity::Endpoint),
        );

        route.count_dropped_header_values(0);
        assert_eq!(dropped.get(), 0);
        route.count_dropped_header_values(2);
        assert_eq!(dropped.get(), 2);
        route.count_dropped_header_values(1);
        assert_eq!(dropped.get(), 3);
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }

    #[nervix_primitives::test]
    async fn a_running_ingestor_refuses_a_second_start() {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_ingestor");
        let runtime = runtime_with_endpoint_ingestor(&domain, &ingestor).await;
        let plan = runtime
            .inner
            .executions
            .get(&domain)
            .expect("the running domain has an execution")
            .revision
            .entrypoints
            .ingestor(&ingestor, Unit::SINGLE)
            .cloned()
            .expect("the execution plans the endpoint ingestor");

        let report = runtime
            .start_ingestor(&plan)
            .await
            .expect_err("the ingestor already runs on this node");

        assert!(matches!(
            report.current_context(),
            IngestorStartError::AlreadyRunning { .. }
        ));
        assert_eq!(
            format!("{report:#}"),
            "ingestor 'event_ingestor' in domain 'default' is already running"
        );
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }

    #[nervix_primitives::test]
    async fn endpoint_ingestor_binds_its_routes_while_it_runs() {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_ingestor");
        let runtime = runtime_with_endpoint_ingestor(&domain, &ingestor).await;
        let route = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("a running endpoint ingestor publishes its route");
        let runtime_key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());

        // The routes are bound before the start returns, so a request that arrives right after it
        // is admitted, and the ingestor counts as ready at once.
        let bound = route
            .bindings()
            .iter()
            .map(|binding| binding.identity().clone())
            .collect::<Vec<_>>();
        assert_eq!(bound, vec![runtime_key]);
        let describe = runtime
            .describe_local_ingestor(
                &domain,
                &ingestor,
                nervix_models::Unit::SINGLE,
                nervix_models::IngestorEntityKind::Http(
                    nervix_models::HttpIngestorEntity::Endpoint,
                ),
            )
            .expect("describe should represent the running ingestor");
        assert!(describe.running);
        assert!(describe.ready);
        let outcome = route
            .dispatch(
                &runtime,
                br#"{"user_id":7}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(0),
                ),
                &CancellationToken::new(),
            )
            .await;
        assert!(outcome.is_accepted());

        runtime
            .stop_ingestor(&domain, &ingestor)
            .await
            .expect("the running endpoint ingestor stops");

        let current = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("ingestor stop retains the configured endpoint");
        assert!(current.bindings().is_empty());
        assert!(route.bindings()[0].intake().is_none());
        assert_eq!(
            route.refusal(),
            Some(EndpointDispatchOutcome::NoAck(EndpointAdmission {
                accepted: 0,
                rejected: 1,
                retry_after: None,
            })),
            "a retained route refuses a request its only intake can no longer take"
        );
        let outcome = route
            .dispatch(
                &runtime,
                br#"{"user_id":8}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(0),
                ),
                &CancellationToken::new(),
            )
            .await;
        assert!(!outcome.is_accepted());
    }

    /// A JSON codec whose `ON INGESTION` transformation passes each payload through, so every
    /// payload is unfolded on the node's extension workers.
    pub(in crate::runtime::ingestors) fn unfolding_wire_format() -> CodecWireFormat {
        CodecWireFormat::JaqNative {
            format: CodecJaqFormat::Json,
            transformations: CodecJaqTransformations {
                on_ingestion: Some(".".to_string()),
                on_emitting: None,
                on_emitting_batch: None,
            },
        }
    }

    #[nervix_primitives::test]
    async fn endpoint_rejects_a_body_whose_unfolding_the_node_cannot_take_now() {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_ingestor");
        let executor = single_worker_executor();
        let runtime = start_endpoint_ingestor(
            Runtime::with_executor(executor.clone()),
            &domain,
            &ingestor,
            unfolding_wire_format(),
        )
        .await;

        let route = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("the fixture installs its endpoint route");

        // Nothing judged the body, so its sender is told to send it again, without a delay the
        // node could not promise.
        let filled = FilledCpuClass::fill(&executor, CpuClass::Extension).await;
        let refused = route
            .dispatch(
                &runtime,
                br#"{"user_id":7}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(0),
                ),
                &CancellationToken::new(),
            )
            .await;
        assert_eq!(
            refused,
            EndpointDispatchOutcome::NoAck(EndpointAdmission {
                accepted: 0,
                rejected: 1,
                retry_after: None,
            })
        );
        let requests = endpoint_requests(&runtime, &domain, &ingestor);
        assert_eq!(
            Counted::of(&requests),
            Counted {
                not_admitted: 1,
                ..Counted::default()
            }
        );
        assert_eq!(
            runtime
                .inner
                .metrics
                .ingestor_unfolding_refused(
                    &domain,
                    &ingestor,
                    Unit::SINGLE,
                    None,
                    nervix_models::IngestorEntityKind::Http(
                        nervix_models::HttpIngestorEntity::Endpoint
                    ),
                )
                .get(),
            1
        );

        filled.release().await;
        let accepted = route
            .dispatch(
                &runtime,
                br#"{"user_id":7}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(0),
                ),
                &CancellationToken::new(),
            )
            .await;
        assert_eq!(
            accepted,
            EndpointDispatchOutcome::NoAck(EndpointAdmission {
                accepted: 1,
                rejected: 0,
                retry_after: None,
            })
        );
        assert_eq!(
            Counted::of(&requests),
            Counted {
                accepted: 1,
                not_admitted: 1,
                ..Counted::default()
            }
        );
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }

    /// A `MODE NO ACK` ingestor counts every request once under the outcome it reached: one it
    /// dispatched or its endpoint buffer retained as accepted and one its codec refused as a
    /// decode rejection. Every outcome is registered at zero, and `DESCRIBE INGESTOR` reports the
    /// same counts under `source:`.
    #[nervix_primitives::test]
    async fn an_unacknowledged_endpoint_counts_each_request_once_by_its_outcome() {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_ingestor");
        let runtime = runtime_with_endpoint_ingestor(&domain, &ingestor).await;
        let route = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("the fixture installs its endpoint route");
        let requests = endpoint_requests(&runtime, &domain, &ingestor);
        assert_eq!(Counted::of(&requests), Counted::default());

        assert_eq!(route.refusal(), None);
        let dispatched = route
            .dispatch(
                &runtime,
                br#"{"user_id":7}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(br#"{"user_id":7}"#.len()),
                ),
                &CancellationToken::new(),
            )
            .await;
        assert!(dispatched.is_accepted());
        let undecodable = route
            .dispatch(
                &runtime,
                br#"{"user_id":"seven"}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(br#"{"user_id":"seven"}"#.len()),
                ),
                &CancellationToken::new(),
            )
            .await;
        assert!(
            undecodable.is_accepted(),
            "a body its codec refuses is consumed, so sending it again would not help"
        );
        let quiesce = runtime
            .ingestor_quiesce_control(&domain, &ingestor, Unit::SINGLE)
            .assured("a running ingestor holds its quiesce control");
        quiesce.engage(IngestorQuiesceCause::EntityHold);
        assert_eq!(
            route.refusal(),
            None,
            "the endpoint buffer takes requests while the ingestor is quiesced"
        );
        let buffered = route
            .dispatch(
                &runtime,
                br#"{"user_id":8}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(br#"{"user_id":8}"#.len()),
                ),
                &CancellationToken::new(),
            )
            .await;
        assert!(buffered.is_accepted());
        assert_eq!(quiesce.counters().buffered_records, 1);
        assert_eq!(
            Counted::of(&requests),
            Counted {
                accepted: 2,
                decode_rejected: 1,
                ..Counted::default()
            }
        );

        let described = runtime
            .describe_local_ingestor(
                &domain,
                &ingestor,
                Unit::SINGLE,
                nervix_models::IngestorEntityKind::Http(
                    nervix_models::HttpIngestorEntity::Endpoint,
                ),
            )
            .expect("the fixture's ingestor describes itself");
        let mut lines = Vec::new();
        for line in &described.source_metrics {
            if line.starts_with("nervix_ingestor_endpoint_requests_total{") {
                lines.push(line.as_str());
            }
        }
        assert_eq!(lines.len(), 6, "one line for every outcome: {lines:?}");
        assert!(
            lines[0]
                .contains(r#",connector="HTTP",entity="ENDPOINT",outcome="accepted"} total=2 "#),
            "{lines:?}"
        );
        assert!(
            lines[4].contains(r#",outcome="decode_rejected"} total=1 "#),
            "{lines:?}"
        );
        quiesce.release(IngestorQuiesceCause::EntityHold);
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }

    /// An ingestor whose quiesce refuses the requests of a route it shares with an available
    /// ingestor counts each of them once, as does its quiesce's rejected counter. The route refuses
    /// a request before reading it only when every ingestor refuses it, and each ingestor counts
    /// that refusal once.
    #[nervix_primitives::test]
    async fn a_quiesced_ingestor_counts_each_request_of_a_shared_route_once() {
        let domain = domain("default");
        let held = named::<IngestorName>("held");
        let open = named::<IngestorName>("open");
        let runtime = start_endpoint_ingestors(
            Runtime::default(),
            &domain,
            &[
                EndpointIngestorFixture {
                    name: &held,
                    mode: EndpointIngestMode::NoAck,
                    quiesce: reject_quiesce(),
                },
                EndpointIngestorFixture {
                    name: &open,
                    mode: EndpointIngestMode::NoAck,
                    quiesce: reject_quiesce(),
                },
            ],
            event_wire_format(),
        )
        .await;
        let route = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("the fixture installs its endpoint route");
        assert_eq!(route.bindings().len(), 2);
        let held_requests = endpoint_requests(&runtime, &domain, &held);
        let open_requests = endpoint_requests(&runtime, &domain, &open);
        let held_quiesce = runtime
            .ingestor_quiesce_control(&domain, &held, Unit::SINGLE)
            .assured("a running ingestor holds its quiesce control");
        let open_quiesce = runtime
            .ingestor_quiesce_control(&domain, &open, Unit::SINGLE)
            .assured("a running ingestor holds its quiesce control");

        held_quiesce.engage(IngestorQuiesceCause::EntityHold);
        assert_eq!(
            route.refusal(),
            None,
            "the open ingestor takes the request, so dispatch counts both ingestors' outcomes"
        );
        let shared = route
            .dispatch(
                &runtime,
                br#"{"user_id":7}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(br#"{"user_id":7}"#.len()),
                ),
                &CancellationToken::new(),
            )
            .await;
        assert_eq!(
            shared,
            EndpointDispatchOutcome::NoAck(EndpointAdmission {
                accepted: 1,
                rejected: 1,
                retry_after: None,
            })
        );
        assert_eq!(
            Counted::of(&held_requests),
            Counted {
                refused: 1,
                ..Counted::default()
            }
        );
        assert_eq!(held_quiesce.counters().rejected_total, 1);
        assert_eq!(
            Counted::of(&open_requests),
            Counted {
                accepted: 1,
                ..Counted::default()
            }
        );

        open_quiesce.engage(IngestorQuiesceCause::EntityHold);
        assert_eq!(
            route.refusal(),
            Some(EndpointDispatchOutcome::NoAck(EndpointAdmission {
                accepted: 0,
                rejected: 2,
                retry_after: Some(Duration::from_secs(1)),
            }))
        );
        assert_eq!(
            Counted::of(&held_requests),
            Counted {
                refused: 2,
                ..Counted::default()
            }
        );
        assert_eq!(held_quiesce.counters().rejected_total, 2);
        assert_eq!(
            Counted::of(&open_requests),
            Counted {
                accepted: 1,
                refused: 1,
                ..Counted::default()
            }
        );
        assert_eq!(open_quiesce.counters().rejected_total, 1);
        held_quiesce.release(IngestorQuiesceCause::EntityHold);
        open_quiesce.release(IngestorQuiesceCause::EntityHold);
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }

    /// A `MODE ACK` ingestor counts a request refused for a full response window as busy, one the
    /// extension workers could not take as not admitted beside its unfolding refusal, one its codec
    /// refused as a decode rejection and one refused while it is quiesced as refused.
    #[nervix_primitives::test]
    async fn an_acknowledged_endpoint_counts_each_request_once_by_its_outcome() {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_ingestor");
        let executor = single_worker_executor();
        let runtime = start_endpoint_ingestors(
            Runtime::with_executor(executor.clone()),
            &domain,
            &[EndpointIngestorFixture {
                name: &ingestor,
                mode: EndpointIngestMode::Ack {
                    max: nonzero!(1u16),
                    timeout: "30s".to_string(),
                },
                quiesce: reject_quiesce(),
            }],
            unfolding_wire_format(),
        )
        .await;
        let route = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("the fixture installs its endpoint route");
        let requests = endpoint_requests(&runtime, &domain, &ingestor);
        let unfolding_refused = runtime.inner.metrics.ingestor_unfolding_refused(
            &domain,
            &ingestor,
            Unit::SINGLE,
            None,
            nervix_models::IngestorEntityKind::Http(nervix_models::HttpIngestorEntity::Endpoint),
        );

        {
            let lease = route.bindings()[0].intake();
            let binding = lease
                .as_deref()
                .assured("the fixture starts its endpoint ingestor");
            let EndpointRequestPolicy::Ack(window) = &binding.request else {
                panic!("the fixture's ingestor answers each request once acknowledged");
            };
            let _slot = window
                .try_admit()
                .assured("the idle window has its only slot free");
            assert_eq!(
                route.refusal(),
                Some(EndpointDispatchOutcome::Ack(EndpointRequestReply {
                    outcome: EndpointRequestOutcome::NotAdmitted(
                        EndpointAdmissionRefusal::WindowFull
                    ),
                    retry_after: Duration::from_secs(1),
                }))
            );
            // A request that passed the pre-read check while a slot was free finds the window full
            // at dispatch, which counts it there instead.
            let raced = route
                .dispatch(
                    &runtime,
                    br#"{"user_id":7}"#,
                    IngestMetadataRow::headers(
                        &NoIngestHeaders,
                        &nervix_connector::NoIngestMetadata(br#"{"user_id":7}"#.len()),
                    ),
                    &CancellationToken::new(),
                )
                .await;
            let Some(raced) = raced.acknowledgement() else {
                panic!("an acknowledged ingestor answers the request with its outcome");
            };
            assert_eq!(
                raced.outcome,
                EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::WindowFull)
            );
        }
        assert_eq!(
            Counted::of(&requests),
            Counted {
                busy: 2,
                ..Counted::default()
            }
        );

        let filled = FilledCpuClass::fill(&executor, CpuClass::Extension).await;
        let unfolding = route
            .dispatch(
                &runtime,
                br#"{"user_id":7}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(br#"{"user_id":7}"#.len()),
                ),
                &CancellationToken::new(),
            )
            .await;
        filled.release().await;
        let Some(unfolding) = unfolding.acknowledgement() else {
            panic!("an acknowledged ingestor answers the request with its outcome");
        };
        assert_eq!(
            unfolding.outcome,
            EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::UnfoldingRefused)
        );
        assert_eq!(unfolding_refused.get(), 1);

        let undecodable = route
            .dispatch(
                &runtime,
                br#"{"user_id":"seven"}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(br#"{"user_id":"seven"}"#.len()),
                ),
                &CancellationToken::new(),
            )
            .await;
        let Some(undecodable) = undecodable.acknowledgement() else {
            panic!("an acknowledged ingestor answers the request with its outcome");
        };
        assert_eq!(
            undecodable.outcome,
            EndpointRequestOutcome::Refused(EndpointInputRefusal::Codec)
        );

        let quiesce = runtime
            .ingestor_quiesce_control(&domain, &ingestor, Unit::SINGLE)
            .assured("a running ingestor holds its quiesce control");
        quiesce.engage(IngestorQuiesceCause::EntityHold);
        let Some(EndpointDispatchOutcome::Ack(suspended)) = route.refusal() else {
            panic!("a quiesced acknowledged ingestor refuses the request before it is read");
        };
        assert_eq!(
            suspended.outcome,
            EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::Suspended)
        );
        assert_eq!(quiesce.counters().rejected_total, 1);
        // A request that passed the pre-read check before the quiesce is refused at dispatch,
        // and counted there once.
        let raced = route
            .dispatch(
                &runtime,
                br#"{"user_id":7}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(br#"{"user_id":7}"#.len()),
                ),
                &CancellationToken::new(),
            )
            .await;
        let Some(raced) = raced.acknowledgement() else {
            panic!("an acknowledged ingestor answers the request with its outcome");
        };
        assert_eq!(
            raced.outcome,
            EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::Suspended)
        );
        assert_eq!(quiesce.counters().rejected_total, 2);
        assert_eq!(
            Counted::of(&requests),
            Counted {
                busy: 2,
                refused: 2,
                not_admitted: 1,
                decode_rejected: 1,
                ..Counted::default()
            }
        );
        quiesce.release(IngestorQuiesceCause::EntityHold);
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }

    /// A body its codec refuses as it drains after resume leaves the buffer as a decode rejection.
    #[nervix_primitives::test]
    async fn a_retained_body_its_codec_refuses_leaves_the_buffer_as_a_decode_rejection() {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_ingestor");
        let runtime = runtime_with_endpoint_ingestor(&domain, &ingestor).await;
        let mut errors = runtime.events().subscribe();
        let quiesce = runtime
            .ingestor_quiesce_control(&domain, &ingestor, nervix_models::Unit::SINGLE)
            .assured("a running ingestor holds its quiesce control");
        let route = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("the fixture installs its endpoint route");

        quiesce.engage(IngestorQuiesceCause::EntityHold);
        let outcome = route
            .dispatch(
                &runtime,
                br#"{"user_id":"seven"}"#,
                IngestMetadataRow::headers(
                    &NoIngestHeaders,
                    &nervix_connector::NoIngestMetadata(0),
                ),
                &CancellationToken::new(),
            )
            .await;
        assert!(
            outcome.is_accepted(),
            "the quiesced ingestor retains the body"
        );
        assert_eq!(quiesce.counters().buffered_records, 1);
        quiesce.release(IngestorQuiesceCause::EntityHold);

        let RuntimeEvent::Error(message) =
            nervix_primitives::time::timeout(Duration::from_secs(10), errors.recv())
                .await
                .expect("the drain reports the body its codec refused")
                .expect("the node keeps its observers while it runs");
        assert!(
            message.starts_with(
                "ingestor 'event_ingestor' general error in domain 'default': decode rejection "
            ),
            "the drain reports a decode rejection: {message}"
        );
        assert!(
            message.contains(
                "codec 'event_json' failed to parse field 'user_id': expected Integer, found a \
                 JSON string"
            ),
            "the report names the codec's own failure: {message}"
        );
        assert_eq!(
            quiesce.counters(),
            IngestorQuiesceCounters::default(),
            "the rejected body leaves the buffer"
        );
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }

    /// A body `BUFFER` retained was already answered 202, so when the extension workers cannot
    /// take its unfolding as it drains after resume, it waits for them in the buffer, still counted
    /// there, instead of being reported and lost. Once they have room it is delivered, in order
    /// with the bodies retained after it.
    #[nervix_primitives::test]
    async fn a_retained_body_waits_for_the_extension_workers_when_it_drains() {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_ingestor");
        let executor = single_worker_executor();
        let runtime = Runtime::with_executor(executor.clone());
        // The fixture places its relay on node-1, so the runtime joins as node-1 to own it and hand
        // what the ingestor delivers to the relay's subscribers.
        attach_loopback_cluster(
            &runtime,
            &ClusterNodeName::parse("node-1").expect("valid name"),
        )
        .await;
        let runtime =
            start_endpoint_ingestor(runtime, &domain, &ingestor, unfolding_wire_format()).await;
        let subscriber = RelaySubscriptionDefinition::new(
            Arc::new(compile_schema(&CreateSchema {
                name: named("event"),
                fields: vec![SchemaField {
                    name: named("user_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            ResolvedBranching::unbranched(),
        );
        let mut delivered = runtime
            .subscribe_stream(&domain, &named("events"), &subscriber)
            .await
            .expect("the fixture's relay accepts a subscriber that describes its rows");
        let mut errors = runtime.events().subscribe();
        let quiesce = runtime
            .ingestor_quiesce_control(&domain, &ingestor, nervix_models::Unit::SINGLE)
            .assured("a running ingestor holds its quiesce control");
        let route = runtime
            .resolve_endpoint("edge.example.com", "/events")
            .into_route()
            .assured("the fixture installs its endpoint route");

        quiesce.engage(IngestorQuiesceCause::EntityHold);
        for user_id in 1..=3 {
            let body = format!(r#"{{"user_id":{user_id}}}"#);
            let outcome = route
                .dispatch(
                    &runtime,
                    body.as_bytes(),
                    IngestMetadataRow::headers(
                        &NoIngestHeaders,
                        &nervix_connector::NoIngestMetadata(0),
                    ),
                    &CancellationToken::new(),
                )
                .await;
            assert!(
                outcome.is_accepted(),
                "the quiesced ingestor retains body {user_id}"
            );
        }
        let retained = quiesce.counters();
        assert_eq!(retained.buffered_records, 3);

        let filled = FilledCpuClass::fill(&executor, CpuClass::Extension).await;
        quiesce.release(IngestorQuiesceCause::EntityHold);
        // The drain reaches the first body's unfolding, which charges the payload's memory before
        // it asks the extension class for a place.
        nervix_primitives::time::timeout(Duration::from_secs(10), async {
            loop {
                let snapshot = executor.snapshot();
                if snapshot.extension_cpu.refused > 0 || snapshot.relay_memory.reserved_bytes > 0 {
                    return;
                }
                nervix_primitives::task::yield_now().await;
            }
        })
        .await
        .expect("the resumed ingestor drains its buffer into the saturated extension class");
        assert_eq!(
            quiesce.counters(),
            retained,
            "a body waiting for the extension workers stays retained with its bytes"
        );
        assert!(
            errors.recv().now_or_never().is_none(),
            "a body waiting for the extension workers is not reported as an ingestor error"
        );

        filled.release().await;
        // The relay may carry several bodies in one batch, so the rows are read across batches.
        let mut user_ids = Vec::new();
        while user_ids.len() < 3 {
            nervix_primitives::task::consume_budget().await;
            let batch = nervix_primitives::time::timeout(Duration::from_secs(10), delivered.recv())
                .await
                .expect("a retained body is delivered once the extension workers have room")
                .expect("the relay keeps its subscriber while the ingestor runs");
            for row in 0..batch.batch.batch().num_rows() {
                user_ids.push(
                    batch
                        .batch
                        .value(row, "user_id")
                        .expect("a delivered row reads its own field"),
                );
            }
        }
        assert_eq!(
            user_ids,
            vec![
                Some(RuntimeValue::I64(1)),
                Some(RuntimeValue::I64(2)),
                Some(RuntimeValue::I64(3)),
            ],
            "every retained body is delivered once, in the order it arrived"
        );
        assert_eq!(quiesce.counters().buffered_records, 0);
        assert_eq!(quiesce.counters().buffered_bytes, 0);
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }
}
