//! The logs a unit test captures from the code it runs.
//!
//! Layer: test support, outside the layer order.
//!
//! - **Owns.** Collecting what a scoped `tracing` subscriber writes, so a test can assert on what
//!   the code under test reported, and the idle dispatcher that keeps every live capture consulted
//!   when a thread first reaches a callsite.
//! - **Depends on.** `tracing`, its subscribers and the primitive boundary's real one-time
//!   initialization.
//! - **Must not know.** What any code under test logs, or why.
//!
//! `tracing` decides whether a callsite is enabled when a thread first reaches it, and keeps the
//! answer. While at most one dispatcher is registered, it asks only that thread's own default
//! dispatcher, so a thread that captures nothing keeps "never" for a callsite that a live capture
//! on another thread wants, and the capture misses every later event of that callsite. Creating
//! logs registers the idle dispatcher first, once per test process. It wants nothing and stays
//! registered, so a capture is never the only registered dispatcher: `tracing` then asks every
//! live dispatcher, the capture among them, and the capture keeps each callsite it wants enabled.

use std::io;

use nervix_primitives::{
    sync::{StdArc, blocking::Mutex},
    unmodeled::sync::OnceLock,
};
use tracing::{Dispatch, level_filters::LevelFilter};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt as _};

/// The idle dispatcher, created on first use and held for the rest of the test process, since
/// `tracing` keeps only a weak reference to a registered dispatcher.
static IDLE_DISPATCH: OnceLock<Dispatch> = OnceLock::new();

/// A registered dispatcher that wants no event, so that no capture is `tracing`'s only one.
///
/// Its subscriber turns every level off: it enables no callsite, and its level hint leaves the
/// global maximum level to the captures.
pub(crate) struct IdleDispatch;

impl IdleDispatch {
    /// Registers the idle dispatcher unless this test process already has. A capture registers it
    /// before it creates its own dispatcher.
    pub(crate) fn register() {
        IDLE_DISPATCH
            .get_or_init(|| Dispatch::new(tracing_subscriber::registry().with(LevelFilter::OFF)));
    }
}

/// Collects everything a scoped subscriber writes so a test can assert on what was reported.
#[derive(Clone)]
pub(crate) struct CapturedLogs(StdArc<Mutex<Vec<u8>>>);

impl Default for CapturedLogs {
    /// Empty logs. The idle dispatcher is registered first, so the subscriber that writes into
    /// these logs is never `tracing`'s only registered dispatcher.
    fn default() -> Self {
        IdleDispatch::register();
        Self(StdArc::default())
    }
}

impl CapturedLogs {
    /// A subscriber that writes what it records into these logs, without terminal colours.
    pub(crate) fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + 'static {
        tracing_subscriber::fmt()
            .with_writer(self.clone())
            .with_ansi(false)
            .finish()
    }

    /// Runs `body` under a subscriber that captures what it logs, and returns what it logged.
    pub(crate) fn of(body: impl FnOnce()) -> String {
        let logs = Self::default();
        tracing::subscriber::with_default(logs.subscriber(), body);
        logs.contents()
    }

    pub(crate) fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().clone()).into_owned()
    }
}

impl io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

mod tests {
    use meticulous::ResultExt as _;
    use nervix_execution::{CpuClass, Executor, MemoryClass};

    use super::CapturedLogs;

    /// The one event this test reports, from a callsite that no other test reaches.
    fn report_the_refusal() {
        tracing::error!("the refusal this capture expects");
    }

    #[nervix_primitives::test]
    async fn a_capture_records_a_callsite_that_a_thread_capturing_nothing_reached_first() {
        let logs = CapturedLogs::default();
        let dispatch = tracing::Dispatch::new(logs.subscriber());
        let executor = Executor::default();
        let charge = executor
            .try_reserve(MemoryClass::Relay, 1)
            .assured("a fresh executor admits a one-byte charge");
        // An extension worker, which captures nothing, reaches the callsite first, while this
        // capture is the only dispatcher that wants it.
        executor
            .run_cpu(CpuClass::Extension, charge, |_charge, _cancellation| {
                report_the_refusal();
            })
            .await
            .assured("a fresh executor runs one job");
        tracing::dispatcher::with_default(&dispatch, report_the_refusal);
        let captured = logs.contents();
        assert!(
            captured.contains("the refusal this capture expects"),
            "a capture records a callsite that another thread reached first: {captured:?}"
        );
    }
}
