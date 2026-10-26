//! The logs a unit test captures from the code it runs.
//!
//! Layer: test support, outside the layer order.
//!
//! - **Owns.** Collecting what a scoped `tracing` subscriber writes, so a test can assert on what
//!   the code under test reported.
//! - **Depends on.** `tracing` and its formatting subscriber.
//! - **Must not know.** What any code under test logs, or why.
//!
//! A callsite that first logs while no subscriber is interested can cache itself as disabled, which
//! hides it from a test that captures it concurrently, so a test module that asserts on what some
//! code logs runs its other calls of that code under a capturing subscriber too.

use std::io;

use nervix_primitives::sync::{StdArc, blocking::Mutex};
use tracing_subscriber::fmt::MakeWriter;

/// Collects everything a scoped subscriber writes so a test can assert on what was reported.
#[derive(Clone, Default)]
pub(crate) struct CapturedLogs(StdArc<Mutex<Vec<u8>>>);

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
