//! The node's runtime event bus.
//!
//! Layer: data plane.
//!
//! - **Owns.** The bounded broadcast that carries a recovered connector failure to the sessions
//!   attached to this node and, through the node's fan-out task, to every peer.
//! - **Depends on.** Nothing but the message each report carries.
//! - **Must not know.** Which subsystem recovered, or how.

use super::*;

/// How many runtime events the bus holds for a receiver that has fallen behind. A receiver that
/// exceeds it is told how many it missed rather than being left to believe it saw everything.
const RUNTIME_EVENT_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    Error(String),
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
    pub(in crate::runtime) fn new() -> Self {
        let (sender, _) = broadcast::channel(RUNTIME_EVENT_CAPACITY);
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
    pub(crate) fn report_error(&self, message: impl Into<String>) {
        let message = message.into();
        debug!(error = %message, "reported runtime error to observers");
        if let Err(broadcast::error::SendError(RuntimeEvent::Error(message))) =
            self.sender.send(RuntimeEvent::Error(message))
        {
            warn!(error = %message, "runtime error raised while no observer is attached");
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.sender.subscribe()
    }
}
