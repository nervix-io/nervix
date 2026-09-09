//! The recovery class: the readings a caller may give a failure it has already survived.
//!
//! Layer: primitives.
//!
//! - **Owns.** The readings a recovered failure is allowed to have, and what each one owes the
//!   operator: a reason that makes the discard correct, a report where this call is the only
//!   witness, and, for a channel send, which absent receiver the send found.
//! - **Depends on.** The standard library and `tracing`.
//! - **Must not know.** What failed, which channel carried it, or which subsystem it belongs to.
//!   Every method is handed the caller's own name for what it was doing.
//!
//! Every fallible operation belongs to one of three classes. A broken internal contract panics,
//! and `meticulous` owns that class: `assured`, `verified` and `todo` each name the guarantee that
//! makes the failure impossible. An expected runtime failure becomes a typed error returned to a
//! caller that can decide. This crate owns the third class — an unexpected but non-critical
//! failure with a defined recovery — because that class is the one `let _ = …` erases. A discarded
//! result states no class at all: it cannot be told apart from a failure nobody considered.
//!
//! A channel send is the largest family in that class and the one whose readings differ most. The
//! send itself fails for exactly one reason, that no receiver is left, but that fact means three
//! unrelated things: the node is stopping and the receiver stopped with it, the peer withdrew and
//! nothing is waiting for the value, or the receiver was held for as long as the sender and its
//! absence is a defect. [`NoReceiver`] carries the first two. The third is the panic class, so it
//! is spelled with `meticulous` rather than here — a receiver that cannot be gone is a guarantee,
//! and inventing a recovery for it would hide the broken invariant instead of stating it.

use std::fmt::Display;

use tracing::debug;

/// A value dropped on purpose, together with the reason that makes dropping it correct.
///
/// The reason is part of the call, exactly as it is for `meticulous`. Write the guarantee that
/// makes the outcome uninteresting — where the failure is already recorded, or why the absent case
/// is ordinary — and not a restatement of the operation being performed.
pub trait Discarded {
    /// Drop this outcome because `because` says nothing is owed for it.
    fn discarded(self, because: &str);
}

impl<T, E> Discarded for Result<T, E> {
    /// Nothing is logged. `because` names a record that already exists, so reporting here would
    /// state the same recovery twice.
    fn discarded(self, because: &str) {
        drop((self, because));
    }
}

impl<T> Discarded for Option<T> {
    /// Nothing is logged, for the same reason the `Result` implementation reports nothing.
    fn discarded(self, because: &str) {
        drop((self, because));
    }
}

/// A failure the caller recovers from and is the only witness of.
pub trait Reported {
    /// Report the failure at `debug` under `operation`, the caller's name for what it attempted.
    ///
    /// `debug` is the level for a recovery that succeeded: the node kept serving, so the record
    /// exists for whoever is reading the failure back rather than for whoever is watching the node
    /// run. A failure that changes what the node offers is a lifecycle transition and belongs in
    /// its own `info` or `warn` at the site that knows which transition it is.
    fn reported(self, operation: &str);
}

impl<T, E: Display> Reported for Result<T, E> {
    fn reported(self, operation: &str) {
        if let Err(error) = self {
            debug!(%error, operation, "recovered from a failed operation");
        }
    }
}

/// What a channel send that found no receiver means.
pub trait NoReceiver {
    /// The receiver is gone because the node, domain, or task it belonged to is stopping.
    ///
    /// The value had nowhere left to go and nothing is owed for it: whatever the receiver would
    /// have done, it is no longer being done at all. `receiver` names what stopped.
    fn means_shutdown(self, receiver: &str);

    /// The peer withdrew before the value arrived, so nothing is waiting for it.
    ///
    /// A requester that timed out, a session that disconnected, and an observer that unsubscribed
    /// all reach here. The send is the last place that fact exists, so it is reported at `debug`;
    /// the value is dropped because its only consumer asked to stop consuming. `receiver` names
    /// the peer.
    fn means_peer_left(self, receiver: &str);
}

impl<T, E> NoReceiver for Result<T, E> {
    fn means_shutdown(self, receiver: &str) {
        if self.is_err() {
            debug!(
                receiver,
                "dropped a value whose receiver stopped with the node"
            );
        }
    }

    fn means_peer_left(self, receiver: &str) {
        if self.is_err() {
            debug!(
                receiver,
                "dropped a value whose peer stopped waiting for it"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    /// Collects everything a scoped subscriber writes so a test can assert on what was reported.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn contents(&self) -> String {
            let bytes = match self.0.lock() {
                Ok(bytes) => bytes.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            };
            String::from_utf8_lossy(&bytes).into_owned()
        }
    }

    impl io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            match self.0.lock() {
                Ok(mut bytes) => bytes.extend_from_slice(buf),
                Err(poisoned) => poisoned.into_inner().extend_from_slice(buf),
            }
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

    fn capturing(body: impl FnOnce()) -> String {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, body);
        logs.contents()
    }

    #[test]
    fn a_discarded_outcome_is_never_reported() {
        let logs = capturing(|| {
            Result::<(), &str>::Err("gone").discarded("the caller already returned the failure");
            Option::<()>::None.discarded("an entity with no observations has no series");
        });

        assert_eq!(logs, "", "a stated discard owes no record, got: {logs}");
    }

    #[test]
    fn a_reported_failure_names_the_operation_and_the_error() {
        let logs = capturing(|| {
            Result::<(), &str>::Err("the store rejected the write").reported("persist offsets");
        });

        assert!(
            logs.contains("persist offsets") && logs.contains("the store rejected the write"),
            "a report must carry the operation and the error, got: {logs}"
        );
    }

    #[test]
    fn a_successful_operation_is_not_reported() {
        let logs = capturing(|| {
            Result::<(), &str>::Ok(()).reported("persist offsets");
            Result::<(), &str>::Ok(()).means_shutdown("branch entrypoint");
            Result::<(), &str>::Ok(()).means_peer_left("describe relay");
        });

        assert_eq!(
            logs, "",
            "nothing failed, so nothing is reported, got: {logs}"
        );
    }

    #[test]
    fn the_two_absent_receivers_are_told_apart() {
        let stopping = capturing(|| {
            Result::<(), &str>::Err("closed").means_shutdown("branch entrypoint");
        });
        let withdrawn = capturing(|| {
            Result::<(), &str>::Err("closed").means_peer_left("describe relay");
        });

        assert!(
            stopping.contains("stopped with the node") && stopping.contains("branch entrypoint"),
            "a shutdown send must say the receiver stopped with the node, got: {stopping}"
        );
        assert!(
            withdrawn.contains("stopped waiting") && withdrawn.contains("describe relay"),
            "a withdrawn peer must say it stopped waiting, got: {withdrawn}"
        );
    }
}
