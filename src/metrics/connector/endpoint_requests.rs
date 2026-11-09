//! Retained request outcomes of the ingestor units that answer endpoint requests.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The endpoint request family, its bounded `outcome` children for every unit of an
//!   entity that reads an endpoint, and their inspection lines.
//! - **Depends on.** The entity declaration's reference kind, the shared counter with wall rates and
//!   Prometheus.
//! - **Must not know.** HTTP or WebSocket framing, request admission, quiesce, decoding or
//!   acknowledgement policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the bounded endpoint request family")
)]

use meticulous::ResultExt as _;
use nervix_models::IngestorEntityKind;
use nervix_primitives::sync::Arc;
use prometheus::{IntCounterVec, Opts, Registry};
use strum::{AsRefStr, EnumIter, IntoEnumIterator as _};

use super::ingestor_reads::ReadCounter;

const REQUESTS: &str = "nervix_ingestor_endpoint_requests_total";

/// What became of one request or WebSocket data frame at one endpoint ingestor, as its counter's
/// bounded `outcome` label names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum EndpointIntakeOutcome {
    /// The ingestor took it in: it dispatched it into its graph or its endpoint buffer retained
    /// it, or its acknowledgement completed.
    Accepted,
    /// Every slot of the ingestor's response window was taken.
    Busy,
    /// The ingestor's quiesce refused new intake, or one of its routes refused the decoded input.
    Refused,
    /// The node's extension workers had no room to unfold it.
    NotAdmitted,
    /// The ingestor's codec refused it, and the ingestor consumed it as a decode rejection.
    DecodeRejected,
    /// The ingestor admitted it without completing it: its dispatch failed, or its acknowledgement
    /// failed, timed out or was interrupted.
    ProcessingFailed,
}

/// The family the endpoint request children of every endpoint ingestor unit belong to.
#[derive(Debug)]
pub(super) struct EndpointRequestFamily {
    requests: IntCounterVec,
}

impl EndpointRequestFamily {
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "registers the endpoint request family once")
    )]
    pub(super) fn register(registry: &Registry, labels: &[&str]) -> Self {
        let mut outcome_labels = labels.to_vec();
        outcome_labels.push("outcome");
        let requests = IntCounterVec::new(
            Opts::new(
                REQUESTS,
                "Requests and WebSocket data frames an endpoint ingestor answered, by outcome.",
            ),
            &outcome_labels,
        )
        .assured("fixed metric names and label names satisfy Prometheus");
        registry
            .register(Box::new(requests.clone()))
            .assured("the endpoint request family is registered once");
        Self { requests }
    }

    /// Whether the units of `kind` answer endpoint requests: an entity whose body names an
    /// endpoint does, and no other.
    fn answers_requests(kind: IngestorEntityKind) -> bool {
        kind.capabilities().reference.endpoint_type().is_some()
    }

    /// Resolves at zero every outcome child of a unit of `kind`, with `identity` its identity label
    /// values in the order the family declares them, where `kind` answers endpoint requests.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "resolves one unit's complete child set when the unit is registered"
        )
    )]
    pub(super) fn unit(
        &self,
        kind: IngestorEntityKind,
        identity: &[&str],
    ) -> Option<IngestorEndpointMetrics> {
        if !Self::answers_requests(kind) {
            return None;
        }
        Some(IngestorEndpointMetrics {
            series: Arc::new(EndpointRequestSeries {
                accepted: self.child(identity, EndpointIntakeOutcome::Accepted),
                busy: self.child(identity, EndpointIntakeOutcome::Busy),
                refused: self.child(identity, EndpointIntakeOutcome::Refused),
                not_admitted: self.child(identity, EndpointIntakeOutcome::NotAdmitted),
                decode_rejected: self.child(identity, EndpointIntakeOutcome::DecodeRejected),
                processing_failed: self.child(identity, EndpointIntakeOutcome::ProcessingFailed),
            }),
        })
    }

    /// The child for `outcome` of the unit whose identity label values are `identity`.
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "resolves one bounded child at registration")
    )]
    fn child(&self, identity: &[&str], outcome: EndpointIntakeOutcome) -> ReadCounter {
        let mut labels = identity.to_vec();
        labels.push(outcome.as_ref());
        ReadCounter::retained(self.requests.with_label_values(&labels))
    }

    /// Withdraws every outcome child of a retired unit of `kind`, with `identity` its identity
    /// label values, where `kind` answers endpoint requests.
    #[cfg_attr(
        nervix_lint,
        nervix::context(lifecycle, reason = "withdraws only a retired unit's children")
    )]
    pub(super) fn withdraw(&self, kind: IngestorEntityKind, identity: &[&str]) {
        if !Self::answers_requests(kind) {
            return;
        }
        for outcome in EndpointIntakeOutcome::iter() {
            let mut labels = identity.to_vec();
            labels.push(outcome.as_ref());
            self.requests
                .remove_label_values(&labels)
                .assured("registration installs every outcome child of an endpoint unit");
        }
    }
}

/// The retained endpoint request children of one unit, which every task generation of the unit,
/// the endpoint bindings it hands out and its inspection share.
#[derive(Debug, Clone)]
pub(crate) struct IngestorEndpointMetrics {
    series: Arc<EndpointRequestSeries>,
}

/// One child for each outcome a request can reach.
#[derive(Debug)]
struct EndpointRequestSeries {
    accepted: ReadCounter,
    busy: ReadCounter,
    refused: ReadCounter,
    not_admitted: ReadCounter,
    decode_rejected: ReadCounter,
    processing_failed: ReadCounter,
}

impl EndpointRequestSeries {
    fn of(&self, outcome: EndpointIntakeOutcome) -> &ReadCounter {
        match outcome {
            EndpointIntakeOutcome::Accepted => &self.accepted,
            EndpointIntakeOutcome::Busy => &self.busy,
            EndpointIntakeOutcome::Refused => &self.refused,
            EndpointIntakeOutcome::NotAdmitted => &self.not_admitted,
            EndpointIntakeOutcome::DecodeRejected => &self.decode_rejected,
            EndpointIntakeOutcome::ProcessingFailed => &self.processing_failed,
        }
    }
}

impl IngestorEndpointMetrics {
    /// Counts one request or frame that reached `outcome`.
    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "selects and increments one retained child")
    )]
    pub(crate) fn record(&self, outcome: EndpointIntakeOutcome) {
        self.series.of(outcome).record(1);
    }

    /// Appends one line for every outcome under `selector`, the unit's identity label selector.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "renders retained counters and their rate summaries"
        )
    )]
    pub(super) fn describe_lines(&self, selector: &str, lines: &mut Vec<String>) {
        for outcome in EndpointIntakeOutcome::iter() {
            lines.push(self.series.of(outcome).describe(
                REQUESTS,
                &format!("{selector},outcome=\"{}\"", outcome.as_ref()),
            ));
        }
    }

    /// The count of `outcome` so far.
    #[cfg(test)]
    pub(crate) fn total(&self, outcome: EndpointIntakeOutcome) -> u64 {
        self.series.of(outcome).total()
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies the production endpoint request owner")
)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::{
        ClusterNodeName, DomainName, DomainNodeRef, DomainNodeUnitRef, HttpIngestorEntity,
        IngestorName, KafkaIngestorEntity, ModelKind, Unit, WebsocketsIngestorEntity,
    };

    use super::*;
    use crate::metrics::RuntimeMetrics;

    /// The values of every `nervix_ingestor_endpoint_requests_total` child the registry gathers,
    /// by their `ingestor` and `outcome` labels, after asserting each child's complete label set.
    fn gathered(metrics: &RuntimeMetrics) -> BTreeMap<(String, String), f64> {
        let mut children = BTreeMap::new();
        for family in metrics.series.prometheus.registry.gather() {
            if family.name() != REQUESTS {
                continue;
            }
            for sample in family.get_metric() {
                let mut labels = BTreeMap::new();
                for pair in sample.get_label() {
                    labels.insert(pair.name().to_string(), pair.value().to_string());
                }
                assert_eq!(
                    labels.keys().map(String::as_str).collect::<Vec<_>>(),
                    [
                        "connector",
                        "domain",
                        "entity",
                        "ingestor",
                        "instance",
                        "outcome",
                        "physical_node_id"
                    ]
                );
                assert_eq!(labels["entity"], "ENDPOINT");
                assert_eq!(labels["physical_node_id"], "node-1");
                children.insert(
                    (labels["ingestor"].clone(), labels["outcome"].clone()),
                    sample.get_counter().value(),
                );
            }
        }
        children
    }

    #[test]
    fn endpoint_units_alone_register_every_outcome_at_zero_and_retire_them() {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("endpoint_requests").expect("valid domain");
        let node = ClusterNodeName::parse("node-1").expect("valid node");
        let http = IngestorName::parse("http_in").expect("valid ingestor");
        let websockets = IngestorName::parse("ws_in").expect("valid ingestor");
        let kafka = IngestorName::parse("kafka_in").expect("valid ingestor");
        let poll = IngestorName::parse("poll_in").expect("valid ingestor");
        let http_requests = metrics
            .register_ingestor_reads(
                &domain,
                &http,
                Unit::SINGLE,
                Some(&node),
                IngestorEntityKind::Http(HttpIngestorEntity::Endpoint),
            )
            .endpoint_requests()
            .expect("an HTTP ENDPOINT unit answers requests");
        let websockets_requests = metrics
            .register_ingestor_reads(
                &domain,
                &websockets,
                Unit::SINGLE,
                Some(&node),
                IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint),
            )
            .endpoint_requests()
            .expect("a WEBSOCKETS ENDPOINT unit answers frames");
        assert!(
            metrics
                .register_ingestor_reads(
                    &domain,
                    &kafka,
                    Unit::SINGLE,
                    Some(&node),
                    IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic),
                )
                .endpoint_requests()
                .is_none(),
            "an ingestor that reads no endpoint has no request children"
        );
        assert!(
            metrics
                .register_ingestor_reads(
                    &domain,
                    &poll,
                    Unit::SINGLE,
                    Some(&node),
                    IngestorEntityKind::Http(HttpIngestorEntity::Request),
                )
                .endpoint_requests()
                .is_none(),
            "an HTTP REQUEST poll reads no endpoint"
        );

        let children = gathered(&metrics);
        assert_eq!(children.len(), 12, "six outcomes for each endpoint unit");
        assert!(children.values().all(|value| *value == 0.0));

        http_requests.record(EndpointIntakeOutcome::Accepted);
        http_requests.record(EndpointIntakeOutcome::Accepted);
        http_requests.record(EndpointIntakeOutcome::Busy);
        websockets_requests.record(EndpointIntakeOutcome::DecodeRejected);
        let children = gathered(&metrics);
        assert_eq!(
            children[&("http_in".to_string(), "accepted".to_string())],
            2.0
        );
        assert_eq!(children[&("http_in".to_string(), "busy".to_string())], 1.0);
        assert_eq!(
            children[&("ws_in".to_string(), "decode_rejected".to_string())],
            1.0
        );
        assert_eq!(http_requests.total(EndpointIntakeOutcome::Accepted), 2);

        let unit = |ingestor: &IngestorName| {
            DomainNodeUnitRef::new(
                DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor),
                Unit::SINGLE,
            )
        };
        let lines = metrics.describe_ingestor_interactions(&unit(&http));
        assert_eq!(
            lines.len(),
            31,
            "seven read lines, six request lines and eighteen acknowledgement and lifecycle lines"
        );
        assert!(lines[7].starts_with(
            "nervix_ingestor_endpoint_requests_total{domain=\"endpoint_requests\",ingestor=\"\
             http_in\",physical_node_id=\"node-1\",instance=\"-\",connector=\"HTTP\",entity=\"\
             ENDPOINT\",outcome=\"accepted\"} total=2 "
        ));
        assert!(lines[8].contains("outcome=\"busy\"} total=1 "));
        assert!(
            lines[13].starts_with("nervix_ingestor_ack_outcomes_total{"),
            "the request lines precede the acknowledgement and lifecycle lines"
        );
        assert_eq!(
            metrics.describe_ingestor_interactions(&unit(&kafka)).len(),
            25,
            "an ingestor that reads no endpoint describes no request lines"
        );

        metrics.remove_ingestor_metrics(&domain, &http, Some(&node));
        let children = gathered(&metrics);
        assert_eq!(
            children.len(),
            6,
            "only the retired unit's children are withdrawn"
        );
        assert_eq!(
            children[&("ws_in".to_string(), "decode_rejected".to_string())],
            1.0
        );
    }
}
