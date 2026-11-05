//! The series a node records the traffic of its client listeners into: the sessions it serves,
//! the requests they decode and how those requests end, the transfer parts of replies larger than
//! one frame, and the authentication attempts its listeners judge.
//!
//! Layer: engines and infrastructure, with the same edge inside it the parent module declares.
//!
//! - **Owns.** The client traffic metric families, the names of the dimensions they are labelled
//!   by, and resolving one child of a family for the label values its caller gives.
//! - **Depends on.** Metrics registration.
//! - **Must not know.** Which transport, request, rejection, stage or outcome a label value names,
//!   or who presented credentials. Every label value comes from a vocabulary its caller owns and
//!   fixes at compile time, so no client can widen a series, and the exporting node is the node
//!   the traffic reached.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the infrastructure boundary declared above")
)]

use meticulous::ResultExt as _;
use prometheus::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};

/// The client listener a series counts the traffic of: `grpc` or `console`.
const TRANSPORT: &str = "transport";

const TRANSPORT_LABELS: &[&str] = &[TRANSPORT];
const REQUEST_LABELS: &[&str] = &[TRANSPORT, "request"];
const REJECTION_LABELS: &[&str] = &[TRANSPORT, "rejection"];
const CANCELLATION_LABELS: &[&str] = &[TRANSPORT, "stage"];
const AUTHENTICATION_LABELS: &[&str] = &[TRANSPORT, "outcome"];

/// The client traffic families of one node's registry.
#[derive(Debug, Clone)]
pub(crate) struct ClientTrafficMetrics {
    sessions: IntGaugeVec,
    sessions_opened_total: IntCounterVec,
    requests_total: IntCounterVec,
    rejections_total: IntCounterVec,
    cancellations_total: IntCounterVec,
    transfer_parts_total: IntCounterVec,
    authentications_total: IntCounterVec,
}

impl ClientTrafficMetrics {
    pub(super) fn register(registry: &Registry) -> Self {
        let metrics = Self {
            sessions: gauge(
                "sessions",
                "Client sessions this node serves now.",
                TRANSPORT_LABELS,
            ),
            sessions_opened_total: counter(
                "sessions_opened_total",
                "Client sessions this node has served.",
                TRANSPORT_LABELS,
            ),
            requests_total: counter(
                "session_requests_total",
                "Requests the client sessions this node serves decoded, by kind.",
                REQUEST_LABELS,
            ),
            rejections_total: counter(
                "session_rejections_total",
                "Requests the client sessions this node serves answered with a rejection, by \
                 reason.",
                REJECTION_LABELS,
            ),
            cancellations_total: counter(
                "session_cancellations_total",
                "Requests a cancellation ended, by whether it found them before or after their \
                 admission.",
                CANCELLATION_LABELS,
            ),
            transfer_parts_total: counter(
                "session_transfer_parts_total",
                "Transfer parts the client sessions this node serves queued for replies larger \
                 than one frame.",
                TRANSPORT_LABELS,
            ),
            authentications_total: counter(
                "client_authentications_total",
                "Calls this node's client listeners authenticated or refused, by outcome.",
                AUTHENTICATION_LABELS,
            ),
        };
        registry
            .register(Box::new(metrics.sessions.clone()))
            .assured("client traffic metrics are registered once with distinct fixed names");
        let counters = [
            metrics.sessions_opened_total.clone(),
            metrics.requests_total.clone(),
            metrics.rejections_total.clone(),
            metrics.cancellations_total.clone(),
            metrics.transfer_parts_total.clone(),
            metrics.authentications_total.clone(),
        ];
        for counter in counters {
            registry
                .register(Box::new(counter))
                .assured("client traffic metrics are registered once with distinct fixed names");
        }
        metrics
    }

    /// The sessions `transport` carries to this node now.
    pub(crate) fn sessions(&self, transport: &'static str) -> IntGauge {
        self.sessions.with_label_values(&[transport])
    }

    /// The sessions `transport` has carried to this node.
    pub(crate) fn sessions_opened(&self, transport: &'static str) -> IntCounter {
        self.sessions_opened_total.with_label_values(&[transport])
    }

    /// The requests of kind `request` that sessions on `transport` decoded.
    pub(crate) fn requests(&self, transport: &'static str, request: &'static str) -> IntCounter {
        self.requests_total.with_label_values(&[transport, request])
    }

    /// The requests sessions on `transport` answered with a `rejection`.
    pub(crate) fn rejections(
        &self,
        transport: &'static str,
        rejection: &'static str,
    ) -> IntCounter {
        self.rejections_total
            .with_label_values(&[transport, rejection])
    }

    /// The requests of sessions on `transport` a cancellation ended at `stage`.
    pub(crate) fn cancellations(&self, transport: &'static str, stage: &'static str) -> IntCounter {
        self.cancellations_total
            .with_label_values(&[transport, stage])
    }

    /// The transfer parts sessions on `transport` queued.
    pub(crate) fn transfer_parts(&self, transport: &'static str) -> IntCounter {
        self.transfer_parts_total.with_label_values(&[transport])
    }

    /// The calls on `transport` whose authentication ended with `outcome`.
    pub(crate) fn authentications(
        &self,
        transport: &'static str,
        outcome: &'static str,
    ) -> IntCounter {
        self.authentications_total
            .with_label_values(&[transport, outcome])
    }
}

fn gauge(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help).namespace("nervix"), labels).assured(
        "the metric name, help text and label names are constants that satisfy Prometheus naming \
         rules",
    )
}

fn counter(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help).namespace("nervix"), labels).assured(
        "the metric name, help text and label names are constants that satisfy Prometheus naming \
         rules",
    )
}

#[cfg(test)]
mod tests {
    use prometheus::{Encoder as _, TextEncoder};

    use super::*;

    fn exposition(registry: &Registry) -> String {
        let mut text = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut text)
            .expect("the registry's families encode as text");
        String::from_utf8(text).expect("the Prometheus text format is UTF-8")
    }

    #[test]
    fn every_family_is_exported_with_the_children_its_caller_resolved() {
        let registry = Registry::new();
        let metrics = ClientTrafficMetrics::register(&registry);

        metrics.sessions("grpc").set(2);
        metrics.sessions_opened("grpc").inc();
        metrics.requests("console", "list_domains").inc();
        metrics.rejections("grpc", "invalid_request").inc();
        metrics.cancellations("grpc", "before_admission").inc();
        metrics.transfer_parts("console").inc_by(3);
        metrics.authentications("grpc", "rejected").inc();

        let text = exposition(&registry);
        for line in [
            "nervix_sessions{transport=\"grpc\"} 2",
            "nervix_sessions_opened_total{transport=\"grpc\"} 1",
            "nervix_session_requests_total{request=\"list_domains\",transport=\"console\"} 1",
            "nervix_session_rejections_total{rejection=\"invalid_request\",transport=\"grpc\"} 1",
            "nervix_session_cancellations_total{stage=\"before_admission\",transport=\"grpc\"} 1",
            "nervix_session_transfer_parts_total{transport=\"console\"} 3",
            "nervix_client_authentications_total{outcome=\"rejected\",transport=\"grpc\"} 1",
        ] {
            assert!(
                text.lines().any(|exported| exported == line),
                "{line} is missing from the exposition:\n{text}"
            );
        }
    }
}
