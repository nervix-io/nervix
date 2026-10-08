//! Layer: test harness.
//! Owns: observing the structured fields emitted by a runtime reporting boundary.
//! May depend on: tracing subscribers and the primitive boundary's channels.
//! Must not know: global tracing configuration, task scheduling or connector drivers.

use std::{collections::BTreeMap, fmt, future::Future};

use meticulous::ResultExt as _;
use nervix_primitives::sync::mpsc;
use tracing::{Dispatch, Event, Subscriber, field::Visit, instrument::WithSubscriber};
use tracing_subscriber::{Layer, layer::SubscriberExt as _};

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
    pub(crate) fn new() -> Self {
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
