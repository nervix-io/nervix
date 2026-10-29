//! MQTT ingestor overflow counters for production-owner checks.
//!
//! Layer: test harness.
//! - **Owns.** One unregistered counter per check and its observation.
//! - **Depends on.** The metric handle the host supplies to an ingestor.
//! - **Must not know.** The runtime's registry, graph or metric labels.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;
use prometheus::IntCounter;

pub(crate) struct IngestorMetricsFixture {
    read_ahead_dropped: IntCounter,
}

impl IngestorMetricsFixture {
    pub(crate) fn new() -> Self {
        Self {
            read_ahead_dropped: IntCounter::new(
                "mqtt_read_ahead_dropped_payloads_total",
                "QoS 0 packet outcomes refused by ACK-mode read-ahead.",
            )
            .assured("the counter's fixed name and help satisfy Prometheus naming rules"),
        }
    }

    pub(crate) fn counter(&self) -> IntCounter {
        self.read_ahead_dropped.clone()
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.read_ahead_dropped.get()
    }
}
