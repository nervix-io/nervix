//! The endpoint source: the one request-scoped source, which the node's own HTTP listener feeds.
//!
//! Layer: data plane.
//!
//! - **Owns.** Resolving the routes an endpoint ingestor receives requests on, and binding them to
//!   the host's request intake for exactly as long as the source runs.
//! - **Depends on.** The connector source contract, the domain's instantiated endpoint routes, and
//!   the host's endpoint binding table.
//! - **Must not know.** Request admission, the endpoint buffer, rejection with a retry delay, or
//!   dispatch, all of which the host owns; NSPL parsing, registry validation, or placement
//!   computation.

use async_trait::async_trait;
use nervix_connector::{
    SourceAckPolicy, SourceConnector, SourceHost, SourceHostServices as _, SourcePlan, SourceResult,
};

use super::{
    super::*,
    source::{RuntimeSourceHost, SourceInstance, SourceStart, run_request_source},
};

/// The routes one endpoint ingestor receives requests on.
pub(in crate::runtime) struct EndpointSourcePlan {
    runtime: Runtime,
    routes: Vec<HttpRouteKey>,
}

/// An endpoint ingestor's source: the binding between the routes it receives requests on and the
/// host's request intake.
///
/// A request is admitted and dispatched by the host on the request path, so the source reads
/// nothing itself. Starting it binds its routes; closing it, or dropping it when its task ends any
/// other way, unbinds them, so a stopped ingestor's routes never keep receiving requests.
pub(in crate::runtime) struct EndpointSource {
    runtime: Runtime,
    routes: Vec<HttpRouteKey>,
    /// The ingestor whose intake the routes are bound to, while they are bound.
    bound: Option<DomainNodeRef>,
}

#[async_trait]
impl SourceConnector for EndpointSource {
    type Plan = EndpointSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        Ok(Self {
            runtime: plan.runtime.clone(),
            routes: plan.routes.clone(),
            bound: None,
        })
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.unbind();
        Ok(())
    }
}

impl EndpointSource {
    /// Binds every route to `intake`, so the requests arriving on it enter the host.
    fn bind(&mut self, intake: EndpointIngestBinding) {
        for route in &self.routes {
            self.runtime
                .inner
                .endpoint_bindings
                .entry(route.clone())
                .or_default()
                .push(intake.clone());
        }
        self.bound = Some(intake.runtime_key);
    }

    fn unbind(&mut self) {
        let Some(runtime_key) = self.bound.take() else {
            return;
        };
        let bindings = &self.runtime.inner.endpoint_bindings;
        for route in &self.routes {
            if let Some(mut bound) = bindings.get_mut(route) {
                bound.retain(|binding| binding.runtime_key != runtime_key);
            }
            // Removing only an emptied entry, under the same shard lock that checks it, keeps a
            // binding another ingestor added to the route in the meantime.
            bindings.remove_if(route, |_, bound| bound.is_empty());
        }
    }
}

impl Drop for EndpointSource {
    fn drop(&mut self) {
        self.unbind();
    }
}

impl SourceInstance for EndpointSource {
    fn start(
        mut self: Box<Self>,
        host: RuntimeSourceHost,
        shutdown: watch::Receiver<bool>,
    ) -> BoxFuture<'static, ()> {
        // Requests may arrive as soon as the ingestor's start returns, so the routes are bound
        // here rather than when the source loop first runs.
        self.bind(host.request_intake());
        host.mark_ready();
        Box::pin(run_request_source(*self, SourceHost::new(host), shutdown))
    }
}

impl EndpointIngestorStartPlan {
    /// Resolves the routes the endpoint publishes, which its source binds when the host starts it.
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> Result<SourceStart, RuntimeError> {
        let EndpointIngestorStartPlan { endpoint, mode: _ } = self;
        let route = {
            let Some(execution) = runtime.inner.executions.get(&ingestor.domain) else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: ingestor.domain.as_str().to_string(),
                    reason: "domain execution is not instantiated".to_string(),
                });
            };
            execution.endpoint_routes.get(&endpoint).cloned()
        };
        let Some(route) = route else {
            return Err(ingestor.start_failure(format!(
                "endpoint '{}' is not instantiated",
                endpoint.as_str()
            )));
        };
        let routes = route
            .hostnames
            .iter()
            .map(|host| HttpRouteKey {
                host: host.clone(),
                path: route.path.clone(),
            })
            .collect();

        let acknowledgement = SourceAckPolicy::None;
        let plan = SourcePlan {
            connector: EndpointSourcePlan {
                runtime: runtime.clone(),
                routes,
            },
            capabilities: ingestor.source_capabilities(NonZeroU64::MIN, acknowledgement.support()),
            acknowledgement,
        };
        let source = EndpointSource::open(&plan.connector, 0)
            .await
            .map_err(|error| ingestor.start_failure(format!("{error:#}")))?;
        let instance: Box<dyn SourceInstance> = Box::new(source);
        Ok(SourceStart {
            instances: vec![instance],
            companions: Vec::new(),
            // Requests never pass through the source loop's intake: the request path admits
            // them. What the endpoint buffer retained replays one request per ingest group.
            buffered_intake: false,
            flush_each_intake: true,
            client_mounts: Vec::new(),
            connector_label: "endpoint",
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_connector::NoIngestHeaders;
    use nervix_models::{
        ClusterNodeName, ClusterSchedule, CodecName, CodecWireFormat, CreateCodec, CreateEndpoint,
        CreateJsonWireSchema, CreateRelay, CreateSchema, CreateVhost, DomainConfig, DomainPace,
        DomainState, DomainStatus, EndpointIngestMode, EndpointName, JsonType, OutputBranch,
        ParseAsType, ProcessorOutputs, RelayBranching, RelayName, SchemaField, SchemaName,
        VhostName, WireSchemaField, WireSchemaName,
    };
    use nonzero_ext::nonzero;

    use super::*;

    /// A running domain whose one endpoint ingestor reads `/events` on `edge.example.com`.
    async fn runtime_with_endpoint_ingestor(
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> Runtime {
        let runtime = Runtime::default();
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: DomainStatus::Running,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            },
        )]));

        let schema = named::<SchemaName>("event");
        let wire_schema = named::<WireSchemaName>("event_wire");
        let codec = named::<CodecName>("event_json");
        let relay = named::<RelayName>("events");
        let vhost = named::<VhostName>("edge");
        let endpoint = named::<EndpointName>("event_ingress");
        runtime
            .apply_cluster_schedule(
                &ClusterNodeName::parse("node-1").expect("valid name"),
                &ClusterSchedule::from_iter([DomainSchedule::new(
                    domain.clone(),
                    vec![
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
                            name: wire_schema.clone(),
                            strictness: Default::default(),
                            fields: vec![WireSchemaField {
                                name: named("user_id"),
                                ty: JsonType::Integer,
                                optional: false,
                            }],
                        })),
                        scheduled_model(Model::Codec(CreateCodec {
                            name: codec.clone(),
                            wire_format: CodecWireFormat::Json { wire_schema },
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
                            endpoint_type: EndpointType::Http,
                            signaling_protocol: None,
                        })),
                        scheduled_model(Model::Ingestor(CreateIngestor {
                            name: ingestor.clone(),
                            output_routes: with_inherit_all(ProcessorOutputs::single(relay))
                                .with_flush_policy(FlushPolicy::Immediate)
                                .with_branch(OutputBranch::Unbranched),
                            decode_using_codec: codec,
                            timestamp_source: None,
                            source: IngestSource::Endpoint {
                                endpoint,
                                mode: EndpointIngestMode::NoAckSequential,
                                quiesce: IngestQuiesceMode::EndpointBuffer {
                                    max_size: "1MiB".to_string(),
                                },
                            },
                            general_error_policy: GeneralErrorPolicy::Log,
                            filter_where: None,
                        })),
                    ],
                    Vec::new(),
                )]),
            )
            .await
            .expect("the endpoint ingestor's schedule applies");
        runtime
    }

    #[tokio::test]
    async fn endpoint_source_binds_its_routes_while_its_ingestor_runs() {
        let domain = domain("default");
        let ingestor = named::<IngestorName>("event_source");
        let runtime = runtime_with_endpoint_ingestor(&domain, &ingestor).await;
        let route = HttpRouteKey {
            host: "edge.example.com".to_string(),
            path: "/events".to_string(),
        };
        let runtime_key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());

        // The routes are bound before the start returns, so a request that arrives right after it
        // is admitted, and the source counts as ready at once.
        let bindings = runtime
            .inner
            .endpoint_bindings
            .get(&route)
            .expect("a running endpoint ingestor binds its route");
        let bound = bindings
            .iter()
            .map(|binding| binding.runtime_key.clone())
            .collect::<Vec<_>>();
        drop(bindings);
        assert_eq!(bound, vec![runtime_key]);
        let describe = runtime
            .describe_local_ingestor(&domain, &ingestor)
            .expect("describe should represent the running ingestor");
        assert!(describe.running);
        assert!(describe.ready);
        let outcome = runtime
            .dispatch_http_payload(
                "edge.example.com",
                "/events",
                br#"{"user_id":7}"#,
                &NoIngestHeaders,
            )
            .await;
        assert!(outcome.is_accepted());

        runtime
            .stop_ingestor(&domain, &ingestor)
            .await
            .expect("the running endpoint ingestor stops");

        assert!(
            !runtime.inner.endpoint_bindings.contains_key(&route),
            "a stopped endpoint ingestor must leave no binding on its route"
        );
        let outcome = runtime
            .dispatch_http_payload(
                "edge.example.com",
                "/events",
                br#"{"user_id":8}"#,
                &NoIngestHeaders,
            )
            .await;
        assert!(!outcome.is_accepted());
    }
}
