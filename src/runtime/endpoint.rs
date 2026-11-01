//! HTTP endpoint ingestion boundary: the request path of the endpoint source.
//!
//! Layer: data plane.
//! - **Owns.** Resolving a request's host and path to the endpoint routes and the ingestors bound
//!   to them, the metadata a request or a WebSocket frame hands those ingestors, request admission,
//!   the endpoint buffer, rejection with a retry delay, per-request dispatch, and response
//!   disposition.
//! - **Depends on.** The request intakes endpoint sources bind, Arrow decoding and actual-UTC
//!   observation.
//! - **Must not know.** When a route is bound or unbound, which the endpoint source owns; Plexus
//!   parsing, placement policy or consensus storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{borrow::Cow, net::SocketAddr};

use nervix_connector::{
    IngestMessageMetadata, IngestMetadataRow, IngestMetadataValues, physical_time::actual_utc_now,
};
use nervix_connector_websockets::CompiledSignalingProtocol;
use nervix_models::WebsocketsFrameType;

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct HttpRouteKey {
    pub(super) host: String,
    pub(super) path: String,
}

#[derive(Debug, Clone)]
pub(super) struct EndpointRoute {
    pub(super) path: String,
    pub(super) hostnames: Vec<String>,
    pub(super) endpoint_type: EndpointType,
    pub(super) signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
}

/// One instantiated endpoint route as inbound HTTP and WebSocket routing sees it, without the
/// configuration a request never consults.
#[derive(Debug, Clone)]
pub(super) struct RoutedEndpoint {
    pub(super) endpoint_type: EndpointType,
    pub(super) signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
}

/// The intake one ingestor admits an endpoint's requests through, bound to every route the
/// endpoint publishes while the ingestor's endpoint source runs.
pub(crate) struct EndpointIngestBinding {
    pub(super) request: EndpointRequestPolicy,
    pub(super) handles: IngestTaskHandles,
    pub(super) quiesce: Arc<IngestorQuiesceControl>,
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) timestamp_source: Option<IngestTimestampSource>,
    pub(super) output_routes: Arc<BoundIngestorRoutes>,
    pub(super) filter_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) codec: Arc<CompiledCodec>,
    pub(super) metrics: MessageMetricsHandle,
    pub(super) branched_senders: HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    /// Consumes a body the ingestor's codec refuses.
    pub(super) decode_rejections: DecodeRejections,
    /// Counts the header values of the requests and WebSocket upgrades routed to the ingestor that
    /// are not UTF-8, which it does not read.
    pub(super) header_values_dropped: prometheus::IntCounter,
}

/// The metadata an `HTTP ENDPOINT` ingestor declares, as one admitted request carries it.
///
/// It hands its values over in the order the entity declares them: the method, path and query the
/// request named, the address of the peer that sent it and the host it was routed by.
pub(crate) struct HttpEndpointRequestMetadata<'a> {
    method: &'a str,
    path: &'a str,
    query: Option<&'a str>,
    remote_addr: SocketAddr,
    host: Cow<'a, str>,
}

impl<'a> HttpEndpointRequestMetadata<'a> {
    /// The metadata of a request that `remote_addr` sent with `method` for `path` and `query`,
    /// whose `Host` value is read as routing reads it: in ASCII lowercase and without its port.
    pub(crate) fn new(
        method: &'a str,
        path: &'a str,
        query: Option<&'a str>,
        remote_addr: SocketAddr,
        host: &'a str,
    ) -> Self {
        Self {
            method,
            path,
            query,
            remote_addr,
            host: normalize_http_host(host),
        }
    }
}

impl IngestMessageMetadata for HttpEndpointRequestMetadata<'_> {
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
        values.string(self.method);
        values.string(self.path);
        match self.query {
            Some(query) => values.string(query),
            None => values.absent(),
        }
        values.rendered(&self.remote_addr);
        values.string(&self.host);
    }
}

/// The metadata a `WEBSOCKETS ENDPOINT` ingestor declares, as one data frame carries it.
///
/// It hands its values over in the order the entity declares them: the frame's kind, the address of
/// the peer that opened its connection and the path the connection's upgrade request named.
pub(crate) struct WebsocketsEndpointFrameMetadata<'a> {
    pub(crate) frame_type: WebsocketsFrameType,
    pub(crate) remote_addr: SocketAddr,
    pub(crate) path: &'a str,
}

impl IngestMessageMetadata for WebsocketsEndpointFrameMetadata<'_> {
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
        values.string(self.frame_type.as_ref());
        values.rendered(&self.remote_addr);
        values.string(self.path);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct EndpointAdmission {
    pub(in crate::runtime) accepted: usize,
    pub(in crate::runtime) rejected: usize,
    pub(crate) retry_after: Option<Duration>,
}

impl EndpointAdmission {
    pub(crate) fn is_accepted(self) -> bool {
        self.accepted > 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointDispatchOutcome {
    NoAck(EndpointAdmission),
    Ack(EndpointRequestReply),
}

impl EndpointDispatchOutcome {
    pub(crate) fn is_accepted(self) -> bool {
        match self {
            Self::NoAck(admission) => admission.is_accepted(),
            Self::Ack(reply) => reply.outcome == EndpointRequestOutcome::Completed,
        }
    }

    pub(crate) fn acknowledgement(self) -> Option<EndpointRequestReply> {
        match self {
            Self::NoAck(_) => None,
            Self::Ack(reply) => Some(reply),
        }
    }

    pub(crate) fn retry_after(self) -> Option<Duration> {
        match self {
            Self::NoAck(admission) => admission.retry_after,
            Self::Ack(reply) => Some(reply.retry_after),
        }
    }
}

/// What became of one payload a binding dispatched.
enum EndpointBindingDispatch {
    /// The binding took the payload: it dispatched it, or reported why its codec rejected it.
    Taken,
    /// The node's bounded execution could not take the payload's unfolding now. Nothing judged
    /// the payload, so its sender is told to send it again.
    NotAdmitted,
}

pub(crate) type ResolvedEndpointRoute = Arc<EndpointIntakeRoute<EndpointIngestBinding>>;

pub(super) fn normalize_http_host(host: &str) -> Cow<'_, str> {
    let host = host.split(':').next().unwrap_or(host).trim();
    if host.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Cow::Owned(host.to_ascii_lowercase())
    } else {
        Cow::Borrowed(host)
    }
}

impl Runtime {
    pub(crate) fn resolve_endpoint(
        &self,
        host: &str,
        path: &str,
    ) -> EndpointResolution<ResolvedEndpointRoute> {
        self.inner.endpoint_intake_routes.resolve(host, path)
    }

    pub(in crate::runtime) async fn signaling_protocol(
        &self,
        domain: &DomainName,
        signaling_protocol: &SignalingProtocolName,
    ) -> Option<Arc<CompiledSignalingProtocol>> {
        let routing = self.domain_routing(domain)?;
        routing
            .load()
            .signaling_protocols
            .get(signaling_protocol)
            .cloned()
    }
}

impl EndpointIntakeRoute<EndpointIngestBinding> {
    /// Refuses unavailable intake before HTTP reads a body or upgrades a WebSocket. Actual
    /// dispatch still acquires its own permit, so racing requests cannot exceed the window.
    pub(crate) fn refusal(&self) -> Option<EndpointDispatchOutcome> {
        let mut outcome = EndpointAdmission::default();
        let mut acknowledgement = None;
        let mut retry_after = Vec::new();
        for lifetime in self.bindings() {
            let lease = lifetime.intake();
            let Some(binding) = lease.as_deref() else {
                outcome.rejected = outcome
                    .rejected
                    .checked_add(1)
                    .assured("endpoint bindings have in-memory owners");
                retry_after.push(None);
                continue;
            };
            let availability = binding.quiesce.endpoint_admission();
            if availability.is_ok() && binding.request.has_capacity() {
                outcome.accepted = outcome
                    .accepted
                    .checked_add(1)
                    .assured("endpoint bindings have in-memory owners");
                continue;
            }
            outcome.rejected = outcome
                .rejected
                .checked_add(1)
                .assured("endpoint bindings have in-memory owners");
            let mut delay = match availability {
                Ok(()) => None,
                Err(delay) => delay,
            };
            if let EndpointRequestPolicy::Ack(window) = &binding.request {
                let refusal = if availability.is_err() {
                    nervix_models::ClientSubmissionRefusal::Suspended
                } else {
                    nervix_models::ClientSubmissionRefusal::Busy
                };
                let reply = EndpointRequestReply {
                    outcome: EndpointRequestOutcome::NotAdmitted(refusal),
                    retry_after: window.retry_after,
                };
                acknowledgement.get_or_insert(reply);
                delay = Some(window.retry_after);
            }
            retry_after.push(delay);
        }
        if outcome.accepted > 0 {
            return None;
        }
        if !retry_after.is_empty() && retry_after.iter().all(Option::is_some) {
            outcome.retry_after = retry_after.into_iter().flatten().max();
        }
        Some(match acknowledgement {
            Some(reply) => EndpointDispatchOutcome::Ack(reply),
            None => EndpointDispatchOutcome::NoAck(outcome),
        })
    }

    /// Dispatches one request body or WebSocket frame to every ingestor bound to this route, each
    /// reading `metadata`: the values its entity declares and the request's headers.
    pub(crate) async fn dispatch(
        &self,
        runtime: &Runtime,
        payload: &[u8],
        metadata: IngestMetadataRow<'_>,
        shutdown: &CancellationToken,
    ) -> EndpointDispatchOutcome {
        let protocol = match self.endpoint_type() {
            EndpointType::Http => "http",
            EndpointType::Websockets => "websocket",
        };
        let mut dispatches = Vec::with_capacity(self.bindings().len());
        for lifetime in self.bindings() {
            dispatches.push(
                runtime.dispatch_endpoint_intake(lifetime, payload, metadata, shutdown, protocol),
            );
        }
        dispatch_endpoint_bindings(dispatches).await
    }

    /// Counts, for every ingestor bound to this route, the `dropped` header values of one request or
    /// WebSocket upgrade that are not UTF-8, which none of them reads.
    pub(crate) fn count_dropped_header_values(&self, dropped: usize) {
        if dropped == 0 {
            return;
        }
        for lifetime in self.bindings() {
            let lease = lifetime.intake();
            if let Some(binding) = lease.as_deref() {
                binding.header_values_dropped.inc_by(dropped.arch_into());
            }
        }
    }
}

/// Poll every binding together: one downstream consumer may wait for another binding's batch
/// before acknowledging its own. Preserve binding order when selecting the first failure.
pub(super) async fn dispatch_endpoint_bindings<I>(dispatches: I) -> EndpointDispatchOutcome
where
    I: IntoIterator,
    I::Item: std::future::Future<Output = EndpointDispatchOutcome>,
{
    let mut outcome = EndpointAdmission::default();
    let mut acknowledgement: Option<EndpointRequestReply> = None;
    let mut retry_after = Vec::new();
    for dispatched in futures_util::future::join_all(dispatches).await {
        match dispatched {
            EndpointDispatchOutcome::Ack(reply) => {
                if matches!(reply.outcome, EndpointRequestOutcome::NotAdmitted(_)) {
                    outcome.rejected = outcome
                        .rejected
                        .checked_add(1)
                        .assured("endpoint bindings have in-memory owners");
                } else {
                    outcome.accepted = outcome
                        .accepted
                        .checked_add(1)
                        .assured("endpoint bindings have in-memory owners");
                }
                match &mut acknowledgement {
                    None => acknowledgement = Some(reply),
                    Some(current) if current.outcome == EndpointRequestOutcome::Completed => {
                        *current = reply;
                    }
                    Some(_) => {}
                }
            }
            EndpointDispatchOutcome::NoAck(admission) => {
                outcome.accepted = outcome
                    .accepted
                    .checked_add(admission.accepted)
                    .assured("endpoint bindings have in-memory owners");
                outcome.rejected = outcome
                    .rejected
                    .checked_add(admission.rejected)
                    .assured("endpoint bindings have in-memory owners");
                if admission.rejected > 0 {
                    retry_after.push(admission.retry_after);
                }
            }
        }
    }
    if outcome.accepted == 0 && !retry_after.is_empty() && retry_after.iter().all(Option::is_some) {
        outcome.retry_after = retry_after.into_iter().flatten().max();
    }
    match acknowledgement {
        Some(mut reply) => {
            // A multi-binding request may already have entered another ingestor. It must
            // never tell its sender that no input was admitted in that case.
            if outcome.accepted > 0
                && (matches!(reply.outcome, EndpointRequestOutcome::NotAdmitted(_))
                    || (outcome.rejected > 0 && reply.outcome == EndpointRequestOutcome::Completed))
            {
                reply.outcome = EndpointRequestOutcome::ProcessingFailed(
                    nervix_models::ClientProcessingFailure::Rejected,
                );
            }
            EndpointDispatchOutcome::Ack(reply)
        }
        None => EndpointDispatchOutcome::NoAck(outcome),
    }
}

impl Runtime {
    async fn dispatch_endpoint_intake(
        &self,
        lifetime: &endpoint_route_table::EndpointBinding<EndpointIngestBinding>,
        payload: &[u8],
        metadata: IngestMetadataRow<'_>,
        shutdown: &CancellationToken,
        protocol: &str,
    ) -> EndpointDispatchOutcome {
        let lease = lifetime.intake();
        let Some(binding) = lease.as_deref() else {
            return EndpointDispatchOutcome::NoAck(EndpointAdmission {
                rejected: 1,
                ..EndpointAdmission::default()
            });
        };
        if let EndpointRequestPolicy::Ack(window) = &binding.request {
            let outcome = if binding.quiesce.endpoint_admission().is_err() {
                EndpointRequestOutcome::NotAdmitted(
                    nervix_models::ClientSubmissionRefusal::Suspended,
                )
            } else {
                window
                    .run_until(
                        async {
                            nervix_primitives::select! {
                                () = lifetime.ended() => {},
                                () = binding.quiesce.wait_until_intake_stops() => {},
                                () = shutdown.cancelled() => {},
                            }
                        },
                        nervix_primitives::time::sleep(window.timeout),
                        self.dispatch_acknowledged_endpoint_binding(
                            binding, payload, metadata, protocol,
                        ),
                    )
                    .await
            };
            return EndpointDispatchOutcome::Ack(EndpointRequestReply {
                outcome,
                retry_after: window.retry_after,
            });
        }
        // A binding may buffer this request until it resumes, so its copy of the
        // request's metadata and headers is taken here and only here.
        let payload = BufferedIngestPayload::new(
            payload,
            BufferedIngestMetadata::retain(&metadata),
            actual_utc_now(),
        );
        let admission = match binding.quiesce.intake(0, payload, true) {
            IngestorQuiesceIntake::Dispatch(payload) => {
                match self
                    .dispatch_endpoint_binding(binding, payload, protocol)
                    .await
                {
                    EndpointBindingDispatch::Taken => EndpointAdmission {
                        accepted: 1,
                        ..EndpointAdmission::default()
                    },
                    EndpointBindingDispatch::NotAdmitted => EndpointAdmission {
                        rejected: 1,
                        ..EndpointAdmission::default()
                    },
                }
            }
            IngestorQuiesceIntake::Buffered => EndpointAdmission {
                accepted: 1,
                ..EndpointAdmission::default()
            },
            IngestorQuiesceIntake::Dropped => EndpointAdmission {
                rejected: 1,
                ..EndpointAdmission::default()
            },
            IngestorQuiesceIntake::Rejected { retry_after } => EndpointAdmission {
                rejected: 1,
                retry_after,
                ..EndpointAdmission::default()
            },
        };
        EndpointDispatchOutcome::NoAck(admission)
    }
}

impl Runtime {
    async fn dispatch_acknowledged_endpoint_binding(
        &self,
        binding: &EndpointIngestBinding,
        payload: &[u8],
        metadata: IngestMetadataRow<'_>,
        protocol: &str,
    ) -> EndpointRequestOutcome {
        let mut collector = IngestRouteCollector::new(
            binding.output_routes.metadata.clone(),
            1,
            binding.metrics.clone(),
        );
        let (root, mut completion) = binding.handles.tracked_root();
        let observed_at = actual_utc_now();
        match collector
            .decode_payload(
                self.executor(),
                QueueAdmission::RefuseWhenFull,
                &binding.codec,
                payload,
            )
            .await
        {
            Ok(()) => {}
            Err(PayloadDecodeFailure::NotAdmitted(report)) => {
                debug!(
                    domain = binding.domain.as_str(), ingestor = binding.ingestor.as_str(),
                    error = %format_args!("{report:#}"), protocol,
                    "the node could not take an acknowledged endpoint message now"
                );
                root.ack_success();
                return EndpointRequestOutcome::NotAdmitted(
                    nervix_models::ClientSubmissionRefusal::Busy,
                );
            }
            Err(PayloadDecodeFailure::Codec(report)) => {
                root.no_ack("codec refused the endpoint payload");
                let rejection =
                    DecodeRejection::new(&binding.codec, &metadata, &report, observed_at);
                binding.decode_rejections.consume(
                    self,
                    &binding.domain,
                    &binding.ingestor,
                    rejection,
                );
                return EndpointRequestOutcome::Refused;
            }
        }
        let routed_ack = if binding.branched_senders.is_empty() {
            root.clone()
        } else {
            root.attached()
        };
        let metadata = [metadata];
        let dispatch = self
            .dispatch_ingested_records(IngestGroupDispatch {
                handles: &binding.handles,
                collector: &mut collector,
                domain: &binding.domain,
                ingestor: &binding.ingestor,
                timestamp_source: binding.timestamp_source.as_ref(),
                output_routes: &binding.output_routes,
                filter_where: binding.filter_where.as_ref(),
                metadata: &metadata,
                ingested_at: observed_at,
                acks: vec![routed_ack],
            })
            .await;
        let flush = self
            .flush_ingest_collector(
                &binding.domain,
                &binding.ingestor,
                &binding.branched_senders,
                &mut collector,
            )
            .await;
        root.ack_success();
        if let Err(error) = dispatch.and(flush) {
            self.inner.events.report_error(format!(
                "failed to dispatch {protocol} message for ingestor '{}' in domain '{}': {error:#}",
                binding.ingestor.as_str(),
                binding.domain.as_str(),
            ));
            return EndpointRequestOutcome::ProcessingFailed(
                nervix_models::ClientProcessingFailure::Rejected,
            );
        }
        if collector.input_refused() {
            return EndpointRequestOutcome::Refused;
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            match completion.wait_for_progress().await {
                AckProgress::Alive => {}
                AckProgress::Complete(AckOutcome::Ack) => return EndpointRequestOutcome::Completed,
                AckProgress::Complete(AckOutcome::NoAck(_)) => {
                    return EndpointRequestOutcome::ProcessingFailed(
                        nervix_models::ClientProcessingFailure::Rejected,
                    );
                }
            }
        }
    }

    async fn dispatch_endpoint_binding(
        &self,
        binding: &EndpointIngestBinding,
        payload: BufferedIngestPayload,
        protocol: &str,
    ) -> EndpointBindingDispatch {
        // One request is one group, so its builders are sized for the single payload this binding
        // decodes; a payload that unfolds into several messages grows them.
        let mut collector = IngestRouteCollector::new(
            binding.output_routes.metadata.clone(),
            1,
            binding.metrics.clone(),
        );
        // The sender is answered, so a body the extension workers have no room for is refused
        // rather than held.
        let decoded = collector
            .decode_payload(
                self.executor(),
                QueueAdmission::RefuseWhenFull,
                &binding.codec,
                payload.payload(),
            )
            .await;
        match decoded {
            Ok(()) => {
                let metadata = [payload.first_metadata_row()];
                let dispatch_result = self
                    .dispatch_ingested_records(IngestGroupDispatch {
                        handles: &binding.handles,
                        collector: &mut collector,
                        domain: &binding.domain,
                        ingestor: &binding.ingestor,
                        timestamp_source: binding.timestamp_source.as_ref(),
                        output_routes: &binding.output_routes,
                        filter_where: binding.filter_where.as_ref(),
                        metadata: &metadata,
                        ingested_at: payload.observed_at(),
                        acks: vec![AckSet::empty()],
                    })
                    .await;
                let flush_result = self
                    .flush_ingest_collector(
                        &binding.domain,
                        &binding.ingestor,
                        &binding.branched_senders,
                        &mut collector,
                    )
                    .await;
                if let Err(error) = dispatch_result.and(flush_result) {
                    let reason = format!("{error:#}");
                    self.inner.events.report_error(format!(
                        "failed to dispatch {protocol} message for ingestor '{}' in domain '{}': \
                         {reason}",
                        binding.ingestor.as_str(),
                        binding.domain.as_str(),
                    ));
                    warn!(
                        domain = binding.domain.as_str(),
                        ingestor = binding.ingestor.as_str(),
                        error = %reason,
                        protocol,
                        "failed to dispatch endpoint message"
                    );
                }
            }
            Err(PayloadDecodeFailure::NotAdmitted(error)) => {
                debug!(
                    domain = binding.domain.as_str(),
                    ingestor = binding.ingestor.as_str(),
                    error = %format_args!("{error:#}"),
                    protocol,
                    "the node could not take an endpoint message now"
                );
                return EndpointBindingDispatch::NotAdmitted;
            }
            Err(PayloadDecodeFailure::Codec(report)) => {
                // Sending the body again would only repeat the refusal, so the endpoint accepts it
                // and consumes it as a decode rejection.
                let rejection = DecodeRejection::new(
                    &binding.codec,
                    &payload.first_metadata_row(),
                    &report,
                    payload.observed_at(),
                );
                binding.decode_rejections.consume(
                    self,
                    &binding.domain,
                    &binding.ingestor,
                    rejection,
                );
            }
        }
        EndpointBindingDispatch::Taken
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use super::*;

    #[test]
    fn normalize_http_host_strips_port_and_normalizes_case() {
        assert_eq!(normalize_http_host(" Example.COM:8080 "), "example.com");
        assert_eq!(normalize_http_host("api.example.com"), "api.example.com");
        assert!(matches!(
            normalize_http_host(" api.example.com:8080 "),
            Cow::Borrowed("api.example.com")
        ));
    }
}

#[cfg(test)]
#[path = "endpoint_metadata_tests.rs"]
mod metadata_tests;
