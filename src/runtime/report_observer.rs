//! Layer: test harness.
//! Owns: observing the structured fields emitted by a runtime reporting boundary.
//! May depend on: tracing subscribers, the captured-logs idle dispatcher and the primitive
//! boundary's channels.
//! Must not know: global tracing configuration, task scheduling or connector drivers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{collections::BTreeMap, fmt, future::Future};

use meticulous::ResultExt as _;
use nervix_primitives::sync::mpsc;
use tracing::{Dispatch, Event, Subscriber, field::Visit, instrument::WithSubscriber};
use tracing_subscriber::{Layer, layer::SubscriberExt as _};

use crate::captured_logs::IdleDispatch;

#[derive(Debug, Default)]
pub(crate) struct ReportLogRecord {
    pub(crate) fields: BTreeMap<String, String>,
}

impl Visit for ReportLogRecord {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().into(), format!("{value:?}"));
    }
}

struct ReportLogLayer {
    sender: mpsc::UnboundedSender<ReportLogRecord>,
}

impl<S: Subscriber> Layer<S> for ReportLogLayer {
    fn on_event(&self, event: &Event<'_>, _context: tracing_subscriber::layer::Context<'_, S>) {
        let mut record = ReportLogRecord::default();
        event.record(&mut record);
        self.sender
            .send(record)
            .assured("the observer keeps its receiver while its subscriber is installed");
    }
}

pub(crate) struct ReportLogObserver {
    dispatch: Dispatch,
    records: mpsc::UnboundedReceiver<ReportLogRecord>,
}

impl ReportLogObserver {
    /// An observer whose dispatcher is created after the idle dispatcher is registered, so it is
    /// never `tracing`'s only registered dispatcher.
    pub(crate) fn new() -> Self {
        IdleDispatch::register();
        let (sender, records) = mpsc::unbounded_channel();
        let subscriber = tracing_subscriber::registry().with(ReportLogLayer { sender });
        Self {
            dispatch: Dispatch::new(subscriber),
            records,
        }
    }

    pub(crate) async fn observe<T>(&self, future: impl Future<Output = T>) -> T {
        future.with_subscriber(self.dispatch.clone()).await
    }

    pub(crate) fn next(&mut self, message: &str) -> ReportLogRecord {
        while let Ok(record) = self.records.try_recv() {
            if record
                .fields
                .get("message")
                .is_some_and(|value| value == message)
            {
                return record;
            }
        }
        panic!("the observed boundary did not emit {message:?}")
    }
}

mod tests {
    use meticulous::ResultExt as _;
    use nervix_execution::{CpuClass, Executor, MemoryClass};

    use super::ReportLogObserver;

    /// The one boundary this test reports, from a callsite that no other test reaches.
    fn report_the_boundary() {
        tracing::warn!(outcome = "refused", "the boundary this observer expects");
    }

    #[nervix_primitives::test]
    async fn an_observer_records_a_callsite_that_a_thread_observing_nothing_reached_first() {
        let mut logs = ReportLogObserver::new();
        let executor = Executor::default();
        let charge = executor
            .try_reserve(MemoryClass::Relay, 1)
            .assured("a fresh executor admits a one-byte charge");
        // An extension worker, which observes nothing, reaches the callsite first, while this
        // observer is the only dispatcher that wants it.
        executor
            .run_cpu(CpuClass::Extension, charge, |_charge, _cancellation| {
                report_the_boundary();
            })
            .await
            .assured("a fresh executor runs one job");
        logs.observe(async { report_the_boundary() }).await;
        let record = logs.next("the boundary this observer expects");
        assert_eq!(
            record.fields.get("outcome").map(String::as_str),
            Some("\"refused\"")
        );
    }
}
