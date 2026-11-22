//! The node's runtime event bus.
//!
//! Layer: data plane.
//!
//! - **Owns.** The bounded broadcast that carries a recovered connector failure to the sessions
//!   attached to this node and, through the node's fan-out task, to every peer.
//! - **Depends on.** Nothing but the message each report carries.
//! - **Must not know.** Which subsystem recovered, or how.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

/// How many runtime events the bus holds for a receiver that has fallen behind. A receiver that
/// exceeds it is told how many it missed rather than being left to believe it saw everything.

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    Error(RuntimeErrorEvent),
}

/// A failure the node recovered from: its display text, and, when it reports an error of a graph
/// node, that error's class and node as typed fields that readers route by instead of the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeErrorEvent {
    pub message: String,
    /// The class and producing node of the error the event reports, absent for a failure that is
    /// no classified error, such as a connector failure the emitter recovered from.
    pub error: Option<ReportedError>,
}

/// The node's runtime event bus, and the one way a connector failure becomes observable.
///
/// A failure that arrives here has already been recovered from: the connector reconnects, retries,
/// or hands the message to its route's error policy, and the node keeps serving. What is left is
/// to make that recovery visible, which is why publishing goes through [`Self::report_error`]
/// rather than through the sender directly. Dropping the send result at each call site would leave
/// the recovery silent, and a recovery nobody can observe is indistinguishable from data loss.
#[derive(Clone)]
pub(crate) struct RuntimeEvents {
    sender: broadcast::Sender<RuntimeEvent>,
}

impl RuntimeEvents {
    pub(in crate::runtime) fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Report a failure the node recovered from. Callers name the entity and domain in `message`,
    /// because this bus carries the report to readers that have no other way to tell them apart.
    ///
    /// The event reaches the sessions attached to this node and, through the fan-out task the node
    /// starts with, every peer. That task holds its subscription for as long as the node serves, so
    /// a send that finds no receiver means the node is still starting or has already torn the task
    /// down. Nothing is left to observe the event in that window, so it is logged at `warn`
    /// instead. A delivered event is traced at `debug`, because the observers are the report and
    /// many of these failures are per-message.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the event message conversion")
    )]
    pub(crate) fn report_error(&self, message: impl Into<String>) {
        self.publish(RuntimeErrorEvent {
            message: message.into(),
            error: None,
        });
    }

    /// Report an error of a graph node the node recovered from: its text, as
    /// [`Self::report_error`] takes it, and the error's class and producing node beside it.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies the event message conversion")
    )]
    pub(crate) fn report_node_error(&self, message: impl Into<String>, error: ReportedError) {
        self.publish(RuntimeErrorEvent {
            message: message.into(),
            error: Some(error),
        });
    }

    fn publish(&self, event: RuntimeErrorEvent) {
        debug!(error = %event.message, "reported runtime error to observers");
        if let Err(broadcast::error::SendError(RuntimeEvent::Error(event))) =
            self.sender.send(RuntimeEvent::Error(event))
        {
            warn!(error = %event.message, "runtime error raised while no observer is attached");
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.sender.subscribe()
    }
}
